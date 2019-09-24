from tasks.us.census.tiger import ShorelineClip
from tasks.targets import PostgresTarget
from luigi import IntParameter, Parameter, Task, WrapperTask
from tasks.meta import current_session
from lib.logger import get_logger
from lib.tileutils import tile2bounds
from lib.geo import bboxes_intersect
import json
import os
import time
import asyncio
import concurrent.futures
import backoff
import csv

LOGGER = get_logger(__name__)
GEONAME_COLUMN = 'geoname'

Z_RANGES = [
    range(0, 8),    # 0-7
    range(8, 10),   # 8-9
    range(10, 11),  # 10
    range(11, 12),  # 11
    range(12, 13),  # 12
    range(13, 14),  # 13
    range(14, 15),  # 14
]

class ConfigFile():
    def _get_config_data(self, config_file):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with (open('{}/conf/{}'.format(dir_path, config_file))) as f:
            return json.load(f)


class TilesTempTable(Task, ConfigFile):
    zoom_level = IntParameter()
    geography = Parameter()
    config_file = Parameter()

    def __init__(self, *args, **kwargs):
        super(TilesTempTable, self).__init__(*args, **kwargs)
        self.config_data = self._get_config_data(self.config_file)

    def run(self):
        session = current_session()
        config_data = self._get_config_data(self.config_file)
        for config in config_data["tables"]:
            self._create_table(session, config)
            tiles = self._calculate_tiles(config)
            self._store_tiles(session, tiles, config)

    def _calculate_tiles(self, config):
        tiles = []
        for x in range(0, (pow(2, self.zoom_level) + 1)):
            for y in range(0, (pow(2, self.zoom_level) + 1)):
                if self._tile_in_bboxes(self.zoom_level, x, y, config['bboxes']):
                    tiles.append([x, y, self.zoom_level])
        return tiles

    def _tile_in_bboxes(self, zoom, x, y, bboxes):
        rect1 = tile2bounds(zoom, x, y)
        for bbox in bboxes:
            rect2 = [bbox['xmin'], bbox['ymin'], bbox['xmax'], bbox['ymax']]
            if bboxes_intersect(rect1, rect2):
                return True

        return False

    def _store_tiles(self, session, tiles, config):
        for tile in tiles:
            query = '''
                    WITH data as (
                        SELECT {x} x, {y} y, {z} z, cdb_observatory.OBS_GetTileBounds({z},{x},{y}) bounds
                    )
                    INSERT INTO \"{schema}\".\"{table}\" (x,y,z,bounds,envelope,ext)
                    SELECT x, y, z, bounds bounds,
                           ST_MakeEnvelope(bounds[1], bounds[2], bounds[3], bounds[4], 4326) envelope,
                           ST_MakeBox2D(ST_Point(bounds[1], bounds[2]), ST_Point(bounds[3], bounds[4])) ext
                    FROM data
                    '''.format(schema=self.schema, table=self._get_table_name(config),
                               x=tile[0], y=tile[1], z=tile[2])
            session.execute(query)
        session.commit()

    def _create_table(self, session, config):
        session.execute('CREATE SCHEMA IF NOT EXISTS \"{}\"'.format(self.schema))
        sql_table = '''CREATE TABLE IF NOT EXISTS \"{schema}\".\"{table}\"(
                       x INTEGER NOT NULL,
                       y INTEGER NOT NULL,
                       z INTEGER NOT NULL,
                       bounds Numeric[],
                       envelope Geometry,
                       ext Geometry,
                       CONSTRAINT {table}_pk PRIMARY KEY (z,x,y)
                    )'''.format(schema=self.schema,
                                table=self._get_table_name(config))
        session.execute(sql_table)
        session.commit()

    @property
    def schema(self):
        return 'tiler_tmp'

    def output(self):
        targets = []
        for config in self.config_data["tables"]:
            targets.append(PostgresTarget(self.schema,
                                          self._get_table_name(config)))
        return targets

    def _get_table_name(self, config):
        return "{table}_tiles_temp_{geo}_{zoom}".format(table=config['table'], geo=self.geography, zoom=self.zoom_level)


class SimpleTilerDOXYZTableTask(Task, ConfigFile):
    zoom_level = IntParameter()
    geography = Parameter()
    config_file = Parameter()
    geom_tablename_suffix = Parameter(default=None, significant=False)

    table_postfix = None
    mc_geography_level = None

    def __init__(self, *args, **kwargs):
        super(SimpleTilerDOXYZTableTask, self).__init__(*args, **kwargs)
        self.columns = self._get_columns()
        self.mc_geography_level = self.geography
        self.config_data = self._get_config_data(self.config_file)

    def requires(self):
        return TilesTempTable(zoom_level=self.zoom_level,
                              geography=self.geography,
                              config_file=self.config_file)

    def _get_country(self):
        return self.config_data["country"]

    def _get_simplification_tolerance(self):
        return self.config_data["geolevels"][self.geography].get("simplification", 0)

    def get_geography_name(self):
        return self.config_data["geolevels"][self.geography]["geography"]

    def get_columns_ids(self):
        columns_ids = []

        for column in self._get_columns():
            if column['id'] == GEONAME_COLUMN:
                column['id'] = self.config_data["geolevels"][self.geography][GEONAME_COLUMN]
            if self.geography not in column.get('exception_levels', []):
                columns_ids.append(column['id'])

        return columns_ids

    def _get_columns(self):
        config_data = self._get_config_data(self.config_file)
        for config in config_data["tables"]:
            if config['table'] == self.output().tablename:
                return config['columns']

    def _get_table_column_names(self):
        columns = []
        for column in self._get_columns():
            columns.append(column.get('column_alias', column['column_name']))
        return columns

    def _get_table_columns(self):
        columns = []
        for column in self._get_columns():
            nullable = '' if column['nullable'] else 'NOT NULL'
            columns.append("{} {} {}".format(column.get('column_alias', column['column_name']),
                                             column['type'],
                                             nullable))
        return columns

    def _create_table(self):
        session = current_session()

        output = self.output()
        LOGGER.info('Creating schema "{schema}" if needed'.format(schema=output.schema))
        session.execute('CREATE SCHEMA IF NOT EXISTS tiler')

        LOGGER.info('Creating table {table} if needed'.format(table=output.table))
        query = '''
                CREATE TABLE IF NOT EXISTS "{schema}".{table}(
                    x INTEGER NOT NULL,
                    y INTEGER NOT NULL,
                    z INTEGER NOT NULL,
                    geoid VARCHAR NOT NULL,
                    mvt_geometry Geometry,
                    area_ratio NUMERIC,
                    area NUMERIC,
                    {cols}
                )
                PARTITION BY LIST (z)
                '''.format(schema=output.schema,
                           table=output.tablename,
                           cols=", ".join(self._get_table_columns()))
        session.execute(query)

        z_range = next(r for r in Z_RANGES if self.zoom_level in r)
        zs = ['{}'.format(z) for z in z_range]
        column_names = self._get_table_column_names()

        query = '''
                CREATE TABLE IF NOT EXISTS "{schema}".{table}_{z}
                PARTITION OF "{schema}".{table} ({col_names})
                FOR VALUES IN ({values})
                WITH (
                    autovacuum_enabled = false,
                    toast.autovacuum_enabled = false
                )
                '''.format(schema=output.schema,
                           table=output.tablename,
                           z=z_range[0],
                           col_names=', '.join(column_names),
                           values=','.join(zs))
        session.execute(query)

        drop_index_query = '''
                DROP INDEX IF EXISTS "{schema}".{table}_{z}_idx
                '''.format(schema=output.schema,
                           table=output.tablename,
                           z=z_range[0])
        session.execute(drop_index_query)

        session.execute('''
            ALTER TABLE "{schema}".{table}_{z}
            ALTER COLUMN mvt_geometry
            SET STORAGE EXTERNAL
        '''.format(schema=output.schema,
                   table=output.tablename,
                   z=z_range[0]))
        session.commit()

    def _create_index(self):
        session = current_session()
        output = self.output()

        z_range = next(r for r in Z_RANGES if self.zoom_level in r)
        index_query = '''
                      CREATE UNIQUE INDEX IF NOT EXISTS {table}_{z}_idx
                      ON "{schema}".{table}_{z}
                      (z, x, y, geoid)
                      '''.format(schema=output.schema,
                                 table=output.tablename,
                                 z=z_range[0])
        session.execute(index_query)


    def _set_normal_parameters(self):
        session = current_session()
        output = self.output()

        z_range = next(r for r in Z_RANGES if self.zoom_level in r)
        autovacuum_query = '''
                      ALTER TABLE "{schema}".{table}_{z}
                      SET (autovacuum_enabled = true,
                          toast.autovacuum_enabled = true)
                      '''.format(schema=output.schema,
                                 table=output.tablename,
                                 z=z_range[0])
        session.execute(autovacuum_query)


    def _insert_data(self):
        session = current_session()

        columns = self._get_columns()

        available_columns = [column for column in columns
                             if self.geography not in
                             column.get('exception_levels', [])]
        LOGGER.debug('Columns: {}'.format(columns))
        LOGGER.debug('Available columns: {}'.format(available_columns))

        out_columns = [x.get('column_alias', x['column_name']) for x in available_columns]
        in_columns = ["q.{name} as {alias}".format(
                        name=x['column_name'], type=x['type'], alias=x.get('column_alias', x['column_name'])
                      ) for x in available_columns]
        in_columns_ids = ["'{}'".format(x) for x in self.get_columns_ids()]
        obs_getmcdomvt_types = ["{name} {type}".format(
            name=column['column_name'], type=column['type']
        ) for column in available_columns]
        LOGGER.info('Inserting data into {table}'.format(table=self.output().table))
        query = '''
                INSERT INTO "{schema}".{table} (x, y, z, geoid,
                                                area_ratio, area, mvt_geometry,
                                                {out_columns})
                SELECT q.x, q.y, q.zoom z, q.id as geoid,
                       q.area_ratio as area_ratio,
                       q.area as area, q.mvtgeom,
                       {in_columns}
                  FROM (
                    SELECT * FROM cdb_observatory.OBS_GetMCDOMVT(
                        {zoom_level},
                        '{geography}',
                        ARRAY[{in_columns_ids}]::TEXT[],
                        ARRAY[]::TEXT[],
                        '{country}',
                        ARRAY[]::TEXT[],
                        ARRAY[]::TEXT[],
                        {simplification_tolerance},
                        '{table_postfix}',
                        {mc_geography_level},
                        False, -- area_normalized
                        True, -- use_meta_cache
                        4096, -- extent
                        256, -- buf
                        True, -- clip_geom
                        {geom_tablename_suffix} -- geom_tablename_suffix
                    ) AS result(x integer, y integer, zoom integer, mvtgeom geometry,
                                id text, {obs_getmcdomvt_types},
                                area_ratio float, area float)
                    ) q
                '''.format(schema=self.output().schema,
                           table=self.output().tablename,
                           zoom_level=self.zoom_level,
                           geography=self.get_geography_name(),
                           out_columns=','.join(out_columns),
                           in_columns=','.join(in_columns),
                           in_columns_ids=','.join(in_columns_ids),
                           country=self._get_country(),
                           obs_getmcdomvt_types=','.join(obs_getmcdomvt_types),
                           simplification_tolerance=self._get_simplification_tolerance(),
                           table_postfix=self.table_postfix if self.table_postfix is not None else '',
                           mc_geography_level="'{}'".format(self.mc_geography_level)
                                              if self.mc_geography_level is not None else 'NULL',
                           geom_tablename_suffix="'{}'".format(self.geom_tablename_suffix)
                                              if self.geom_tablename_suffix is not None else 'NULL')

        session.execute(query)
        session.commit()

    def run(self):
        self._create_table()
        self._insert_data()
        self._create_index()
        self._set_normal_parameters()

    def output(self):
        config_data = self._get_config_data(self.config_file)
        return PostgresTarget('tiler', 'xyz_{country}_do_geoms'.format(country=config_data["country"]))

    def complete(self):
        session = current_session()
        exists = False

        try:
            query = '''
                    SELECT 1 FROM "{schema}".{table}
                    WHERE z = {zoom_level}
                    LIMIT 1
                    '''.format(schema=self.output().schema,
                               table=self.output().tablename,
                               zoom_level=self.zoom_level,)
            exists = session.execute(query).fetchone() is not None
            if exists:
                LOGGER.warn('The zoom level is already loaded')
        except Exception:
            LOGGER.info('Error checking output. Maybe the table doesn\'t exist')

        return exists


class AllSimpleDOXYZTables(WrapperTask, ConfigFile):
    config_file = Parameter()

    def requires(self):
        self.config_data = self._get_config_data(self.config_file)
        return [
            SimpleTilerDOXYZTableTask(zoom_level=zoom,
                                      geography=level,
                                      config_file=self.config_file)
            for level, info in self.config_data["geolevels"].items()
            for zoom in info["zoomlevels"]]
