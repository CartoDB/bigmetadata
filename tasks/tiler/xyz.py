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
                    '''.format(schema=config['schema'], table=self._get_table_name(config),
                               x=tile[0], y=tile[1], z=tile[2])
            session.execute(query)
        session.commit()

    def _create_table(self, session, config):
        session.execute('CREATE SCHEMA IF NOT EXISTS \"{}\"'.format(config['schema']))
        sql_table = '''CREATE TABLE IF NOT EXISTS \"{schema}\".\"{table}\"(
                       x INTEGER NOT NULL,
                       y INTEGER NOT NULL,
                       z INTEGER NOT NULL,
                       bounds Numeric[],
                       envelope Geometry,
                       ext Geometry,
                       CONSTRAINT {table}_pk PRIMARY KEY (x,y,z)
                    )'''.format(schema=config['schema'],
                                table=self._get_table_name(config))
        session.execute(sql_table)
        session.commit()

    def output(self):
        targets = []
        for config in self.config_data["tables"]:
            targets.append(PostgresTarget(config['schema'],
                                          self._get_table_name(config)))
        return targets

    def _get_table_name(self, config):
        return "{table}_tiles_temp_{geo}_{zoom}".format(table=config['table'], geo=self.geography, zoom=self.zoom_level)


class SimpleTilerDOXYZTableTask(Task, ConfigFile):
    zoom_level = IntParameter()
    geography = Parameter()
    config_file = Parameter()

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
                column['id'] = self.config_data["geolevels"][self.geography]["geoname"]
            columns_ids.append(column['id'])

        return columns_ids

    def _get_columns(self):
        config_data = self._get_config_data(self.config_file)
        for config in config_data["tables"]:
            if config['table'] == self.output().tablename:
                return config['columns']

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

        LOGGER.info('Creating schema "{schema}" if needed'.format(schema=self.output().schema))
        session.execute('CREATE SCHEMA IF NOT EXISTS tiler')

        LOGGER.info('Creating table {table} if needed'.format(table=self.output().table))
        query = '''
                CREATE TABLE IF NOT EXISTS "{schema}".{table}(
                    x INTEGER NOT NULL,
                    y INTEGER NOT NULL,
                    z INTEGER NOT NULL,
                    geoid VARCHAR NOT NULL,
                    mvt_geometry Geometry,
                    area_ratio NUMERIC,
                    area NUMERIC,
                    {cols},
                    CONSTRAINT {schema}_{table}_pk PRIMARY KEY (x,y,z,geoid)
                )
                '''.format(schema=self.output().schema,
                           table=self.output().tablename,
                           cols=", ".join(self._get_table_columns()))

        session.execute(query)
        session.commit()

    def _insert_data(self):
        session = current_session()

        columns = self._get_columns()
        out_columns = [x.get('column_alias', x['column_name']) for x in columns]
        in_columns = ["((aa).mvtdata::json->>'{name}')::{type} as {alias}".format(
                        name=x['column_name'], type=x['type'], alias=x.get('column_alias', x['column_name'])
                      ) for x in columns]
        in_columns_ids = ["'{}'".format(x) for x in self.get_columns_ids()]

        LOGGER.info('Inserting data into {table}'.format(table=self.output().table))
        query = '''
                INSERT INTO "{schema}".{table} (x, y, z, geoid,
                                                area_ratio, area, mvt_geometry,
                                                {out_columns})
                SELECT (aa).x, (aa).y, (aa).zoom z, (aa).mvtdata::json->>'id'::text as geoid,
                       ((aa).mvtdata::json->>'area_ratio')::numeric as area_ratio,
                       ((aa).mvtdata::json->>'area')::numeric as area, (aa).mvtgeom,
                       {in_columns}
                  FROM (
                    SELECT cdb_observatory.OBS_GetMCDOMVT({zoom_level}, '{geography}',
                    ARRAY[{in_columns_ids}]::TEXT[],
                    ARRAY[]::TEXT[], '{country}', ARRAY[]::TEXT[], ARRAY[]::TEXT[],
                    {simplification_tolerance}, '{table_postfix}', {mc_geography_level}) aa
                    ) q;
                '''.format(schema=self.output().schema,
                           table=self.output().tablename,
                           zoom_level=self.zoom_level,
                           geography=self.get_geography_name(),
                           out_columns=','.join(out_columns),
                           in_columns=','.join(in_columns),
                           in_columns_ids=','.join(in_columns_ids),
                           country=self._get_country(),
                           simplification_tolerance=self._get_simplification_tolerance(),
                           table_postfix=self.table_postfix if self.table_postfix is not None else '',
                           mc_geography_level="'{}'".format(self.mc_geography_level)
                                              if self.mc_geography_level is not None else 'NULL')

        session.execute(query)
        session.commit()

    def run(self):
        self._create_table()
        self._insert_data()

    def output(self):
        config_data = self._get_config_data(self.config_file)
        return PostgresTarget('tiler', 'xyz_{country}_do_geoms'.format(country=config_data["country"]))

    def complete(self):
        session = current_session()
        count = 0

        try:
            query = '''
                    SELECT count(*) FROM "{schema}".{table} WHERE z = {zoom_level}
                    '''.format(schema=self.output().schema,
                               table=self.output().tablename,
                               zoom_level=self.zoom_level,)
            count = session.execute(query).fetchone()[0]
            if count > 0:
                LOGGER.warn('The zoom level is already loaded')
        except Exception:
            LOGGER.info('Error checking output. Maybe the table doesn\'t exist')

        return count > 0


class AllSimpleDOXYZTables(WrapperTask, ConfigFile):
    config_file = Parameter()

    def requires(self):
        self.config_data = self._get_config_data(self.config_file)
        for level, info in self.config_data["geolevels"].items():
            for zoom in info["zoomlevels"]:
                yield SimpleTilerDOXYZTableTask(zoom_level=zoom,
                                                geography=level,
                                                config_file=self.config_file)
