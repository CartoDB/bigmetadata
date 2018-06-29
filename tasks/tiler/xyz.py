from tasks.us.census.tiger import ShorelineClip
from tasks.targets import PostgresTarget
from luigi import IntParameter, Parameter, WrapperTask, Task
from tasks.meta import current_session, async_pool
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


class TilerXYZTableTask(Task):

    zoom_level = IntParameter()
    geography = Parameter()
    month = Parameter()

    def __init__(self, *args, **kwargs):
        super(TilerXYZTableTask, self).__init__(*args, **kwargs)
        self.config_data = self._get_config_data()
        if not os.path.exists('tmp/tiler'):
            os.makedirs('tmp/tiler')
        self._csv_filename = 'tmp/tiler/tile_{}.csv'.format(self.task_id)

    def get_config_file(self):
        raise NotImplementedError('Config file must be implemented by the child class')

    def get_geography_level(self, level):
        raise NotImplementedError('Geography levels file must be implemented by the child class')

    def run(self):
        config_data = self._get_config_data()
        for table_config in config_data:
            table_schema = self._get_table_schema(table_config)
            table_bboxes = table_config['bboxes']
            self._create_schema_and_table(table_schema)
            self._generate_and_insert_tiles(self.zoom_level, table_schema, table_config, table_bboxes)

    def _get_table_name(self, table_schema):
        return table_schema['table_name'] + '_' + self.month

    def _get_mc_dates(self):
        session = current_session()
        query = '''
                SELECT cdb_observatory.OBS_GetMCDates('{schema}', '{geo_level}', '{month}');
                '''.format(schema='us.mastercard', geo_level=self.geography, month=self.month)
        return session.execute(query).fetchone()[0]

    def _create_schema_and_table(self, table_schema):
        session = current_session()
        session.execute('CREATE SCHEMA IF NOT EXISTS tiler')
        cols_schema = []
        table_name = "{}.{}".format(table_schema['schema'], self._get_table_name(table_schema))
        for _, cols in table_schema['columns'].items():
            cols_schema += cols
        sql_table = '''CREATE TABLE IF NOT EXISTS {}(
                x INTEGER NOT NULL,
                y INTEGER NOT NULL,
                z INTEGER NOT NULL,
                mvt_geometry Geometry NOT NULL,
                geoid VARCHAR NOT NULL,
                area_ratio NUMERIC,
                area NUMERIC,
                {},
                CONSTRAINT xyzusall_pk PRIMARY KEY (x,y,z,geoid)
            )'''.format(table_name, ", ".join(cols_schema))
        session.execute(sql_table)
        session.commit()

    def _get_config_data(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with (open('{}/conf/{}'.format(dir_path, self.get_config_file()))) as f:
            return json.load(f)

    def _get_table_schema(self, table_config):
        mc_dates = [date.replace('-', '') for date in self._get_mc_dates()]
        columns = {}

        columns['do'] = []
        for do_column in table_config['columns']['do']:
            nullable = '' if do_column['nullable'] else 'NOT NULL'
            columns['do'].append("{} {} {}".format(do_column['column_name'], do_column['type'], nullable))

        columns['mc'] = []
        for mc_date in mc_dates:
            for mc_category in table_config['mc_categories']:
                for mc_column in table_config['columns']['mc']:
                    nullable = '' if mc_column['nullable'] else 'NOT NULL'
                    columns['mc'].append("{} {} {}".format('_'.join([mc_column['column_name'],
                                                                     mc_category['id'],
                                                                     mc_date]),
                                                           mc_column['type'], nullable))
        return {"schema": table_config['schema'], "table_name": table_config['table'], "columns": columns}

    def _tile_in_bboxes(self, zoom, x, y, bboxes):
        rect1 = tile2bounds(zoom, x, y)
        for bbox in bboxes:
            rect2 = [bbox['xmin'], bbox['ymin'], bbox['xmax'], bbox['ymax']]
            if bboxes_intersect(rect1, rect2):
                return True

        return False

    def _generate_and_insert_tiles(self, zoom, table_schema, table_config, bboxes_config):
        do_columns = [column['id'] for column in table_config['columns']['do']]
        mc_columns = [column['id'] for column in table_config['columns']['mc']]
        mc_categories = [category['id'] for category in table_config['mc_categories']]
        mc_dates = mc_dates = [date.replace('-', '') for date in self._get_mc_dates()]

        recordset = ["mvtdata->>'id' as id"]
        recordset.append("(mvtdata->>'area_ratio')::numeric as area_ratio")
        recordset.append("(mvtdata->>'area')::numeric as area")

        recordset += ["(mvtdata->>'{name}')::{type} as {name}".format(name=column['column_name'].lower(),
                                                                      type=column['type'])
                      for column in table_config['columns']['do']]

        for mc_date in mc_dates:
            for mc_category in table_config['mc_categories']:
                recordset += ["(mvtdata->>'{name}')::{type} as {name}".format(name='_'.join([column['column_name'],
                                                                                             mc_category['id'],
                                                                                             mc_date]).lower(),
                                                                              type=column['type'])
                              for column in table_config['columns']['mc']]

        tiles = []
        tile_start = time.time()
        for x in range(0, (pow(2, zoom) + 1)):
            for y in range(0, (pow(2, zoom) + 1)):
                if self._tile_in_bboxes(zoom, x, y, bboxes_config):
                    tiles.append([x, y, zoom])
        tile_end = time.time()
        LOGGER.info("Tiles to be processed: {} tiles. Calculated in {} seconds".format(len(tiles), (tile_end - tile_start)))
        sql_start = time.time()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            exceptions = loop.run_until_complete(self._generate_tiles(tiles, recordset,
                                                                      do_columns, mc_columns, mc_categories))
            if exceptions:
                LOGGER.warning("Exception/s found processing tiles: {}".format("\n".join([str(e) for e in exceptions if e is not None])))
        finally:
            loop.close()
        sql_end = time.time()
        LOGGER.info("Generation tiles time was {} seconds".format(sql_end - sql_start))
        self._insert_tiles(table_schema)

    @backoff.on_exception(backoff.expo,
                          (asyncio.TimeoutError,
                           concurrent.futures._base.TimeoutError),
                          max_time=600)
    async def _generate_tiles(self, tiles, recordset, do_columns, mc_columns, mc_categories):
        mc_dates = self._get_mc_dates()
        with open(self._csv_filename, 'w+') as csvfile:
            db_pool = await async_pool()
            csvwriter = csv.writer(csvfile)
            geography_level = self.get_geography_level(self.geography)
            executed_tiles = [self._generate_tile(db_pool, csvwriter, tile, geography_level, recordset,
                                                  do_columns, mc_columns, mc_categories, mc_dates)
                              for tile in tiles]
            exceptions = await asyncio.gather(*executed_tiles, return_exceptions=True)
            return exceptions

    @backoff.on_exception(backoff.expo,
                          (asyncio.TimeoutError,
                           concurrent.futures._base.TimeoutError),
                          max_time=600)
    async def _generate_tile(self, db_pool, csvwriter, tile, geography, recordset,
                             do_columns, mc_columns, mc_categories, mc_dates):
        sql_tile = '''
            SELECT {x}, {y}, {z}, ST_CollectionExtract(ST_MakeValid(mvtgeom), 3) mvtgeom, {recordset}
            FROM cdb_observatory.OBS_GetMCDOMVT({z},{x},{y},'{geography_level}',
                                                ARRAY['{docols}']::TEXT[],
                                                ARRAY['{mccols}']::TEXT[],
                                                ARRAY['{mccategories}']::TEXT[],
                                                ARRAY['{mcdates}']::TEXT[])
            WHERE mvtgeom IS NOT NULL;
            '''.format(x=tile[0], y=tile[1], z=tile[2], geography_level=geography,
                       recordset=", ".join(recordset), docols="', '".join(do_columns),
                       mccols="', '".join(mc_columns),
                       mccategories="', '".join(mc_categories),
                       mcdates="','".join(mc_dates))
        conn = None
        try:
            conn = await db_pool.acquire()
            tile_start = time.time()
            records = await conn.fetch(sql_tile)
            tile_end = time.time()
            LOGGER.debug('Generated tile [{}/{}/{}] in {} seconds'.format(tile[0], tile[1], tile[2], (tile_end - tile_start)))
            for record in records:
                csvwriter.writerow(tuple(record))
            else:
                LOGGER.debug('Tile [{}/{}/{}] without data'.format(tile[0], tile[1], tile[2], (tile_end - tile_start)))
        except BaseException as e:
            LOGGER.error('Tile [{}/{}/{}] returned exception {}'.format(tile[0], tile[1], tile[2], e))
            raise e
        finally:
            await db_pool.release(conn)

    def _insert_tiles(self, table_schema):
        table_name = '{}.{}'.format(table_schema['schema'], self._get_table_name(table_schema))
        copy_start = time.time()
        session = current_session()
        with open(self._csv_filename, 'rb') as f:
            cursor = session.connection().connection.cursor()
            cursor.copy_from(f, table_name, sep=',', null='')
            session.commit()
        copy_end = time.time()
        LOGGER.info("Copy tiles time was {} seconds".format(copy_end - copy_start))

    def output(self):
        targets = []
        for table_config in self.config_data:
            table_schema = self._get_table_schema(table_config)
            targets.append(PostgresTarget(table_schema['schema'],
                                          self._get_table_name(table_schema),
                                          where='z = {}'.format(self.zoom_level)))
        return targets
