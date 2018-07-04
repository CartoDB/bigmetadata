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

    def __init__(self, *args, **kwargs):
        super(TilerXYZTableTask, self).__init__(*args, **kwargs)
        self.config_data = self._get_config_data()

    def get_config_file(self):
        raise NotImplementedError('Config file must be implemented by the child class')

    def get_geography_level(self, level):
        raise NotImplementedError('Geography levels file must be implemented by the child class')

    def get_columns(self, config, shard_value=None):
        raise NotImplementedError('Get columns function must be implemented by the child class')

    def get_table_columns(self, config, shard_value=None):
        raise NotImplementedError('Get Table columns function must be implemented by the child class')

    def get_tile_query(self, config, tile, geography, shard_value=None):
        raise NotImplementedError('Get tile query function must be implemented by the child class')


    def run(self):
        config_data = self._get_config_data()
        for config in config_data:
            table_bboxes = config['bboxes']
            tables_data = self._get_table_names(config)
            for table_data in tables_data:
                self._create_schema_and_table(table_data, config)
                self._generate_csv_tiles(self.zoom_level, table_data, config, table_bboxes)
                self._insert_tiles(table_data)

    def _get_table_names(self,config):
        table_names = []
        if config['sharded']:
            for value in config['sharding']['values']:
                table_names.append({"schema": config['schema'], "table": "{}_{}".format(config['table'], value), "value": value})
        else:
            table_names.append({"schema": config['schema'], "table": config['table'], "value": None})

        return table_names

    def _create_schema_and_table(self, table_data, config):
        session = current_session()
        session.execute('CREATE SCHEMA IF NOT EXISTS tiler')
        cols_schema = self.get_table_columns(config, table_data['value'])
        sql_table = '''CREATE TABLE IF NOT EXISTS {table}(
                       x INTEGER NOT NULL,
                       y INTEGER NOT NULL,
                       z INTEGER NOT NULL,
                       mvt_geometry Geometry,
                       geoid VARCHAR NOT NULL,
                       area_ratio NUMERIC,
                       area NUMERIC,
                       {cols},
                       CONSTRAINT {table_name}_pk PRIMARY KEY (x,y,z,geoid)
                    )'''.format(table=self._get_table_name(table_data),
                                table_name=table_data['table'],
                                cols=", ".join(cols_schema))
        session.execute(sql_table)
        session.commit()

    def _get_config_data(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with (open('{}/conf/{}'.format(dir_path, self.get_config_file()))) as f:
            return json.load(f)

    def _tile_in_bboxes(self, zoom, x, y, bboxes):
        rect1 = tile2bounds(zoom, x, y)
        for bbox in bboxes:
            rect2 = [bbox['xmin'], bbox['ymin'], bbox['xmax'], bbox['ymax']]
            if bboxes_intersect(rect1, rect2):
                return True

        return False

    def _generate_csv_tiles(self, zoom, table_data, config, bboxes_config):
        tiles = []
        tile_start = time.time()
        for x in range(0, (pow(2, zoom) + 1)):
            for y in range(0, (pow(2, zoom) + 1)):
                if self._tile_in_bboxes(zoom, x, y, bboxes_config):
                    tiles.append([x, y, zoom])
        tile_end = time.time()
        LOGGER.info("Tiles to be processed: {}. Calculated in {} seconds".format(len(tiles), (tile_end - tile_start)))
        sql_start = time.time()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            exceptions = loop.run_until_complete(self._generate_tiles(tiles, config, table_data))
            exceptions =  [e for e in exceptions if e is not None]
            if exceptions:
                LOGGER.warning("Exception/s found processing tiles: {}".format("\n".join([str(e) for e in exceptions if e is not None])))
        finally:
            loop.close()
        sql_end = time.time()
        LOGGER.info("Generated tiles it took {} seconds".format(sql_end - sql_start))

    @backoff.on_exception(backoff.expo,
                          (asyncio.TimeoutError,
                           concurrent.futures._base.TimeoutError),
                          max_time=600)
    async def _generate_tiles(self, tiles, config, table_data):
        with open(self._get_csv_filename(table_data), 'w+') as csvfile:
            db_pool = await async_pool()
            columns = self.get_columns(config, table_data['value'])
            csvwriter = csv.writer(csvfile)
            headers = ['x', 'y', 'z', 'mvt_geometry', 'geoid', 'area_ratio', 'area']
            headers += [column['column_name'].lower() for column in columns]
            csvwriter.writerow(headers)
            geography_level = self.get_geography_level(self.geography)
            executed_tiles = [self._generate_tile(db_pool, csvwriter, tile, geography_level, config, table_data['value'])
                              for tile in tiles]
            exceptions = await asyncio.gather(*executed_tiles, return_exceptions=True)
            return exceptions

    @backoff.on_exception(backoff.expo,
                          (asyncio.TimeoutError,
                           concurrent.futures._base.TimeoutError),
                          max_time=600)
    async def _generate_tile(self, db_pool, csvwriter, tile, geography, config, shard_value=None):
        tile_query = self.get_tile_query(config, tile, geography, shard_value)
        conn = None
        try:
            conn = await db_pool.acquire()
            tile_start = time.time()
            records = await conn.fetch(tile_query)
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

    def _insert_tiles(self, table_data):
        table_name = self._get_table_name(table_data)
        copy_start = time.time()
        session = current_session()
        with open(self._get_csv_filename(table_data), 'rb') as f:
            cursor = session.connection().connection.cursor()
            sql_copy = """
                        COPY {table} FROM STDIN WITH CSV HEADER DELIMITER AS ','
                       """.format(table=table_name)
            cursor.copy_expert(sql=sql_copy, file=f)
            session.commit()
            cursor.close()
        copy_end = time.time()
        LOGGER.info("Copy tiles for table {} took {} seconds".format(table_data['table'],copy_end - copy_start))

    def output(self):
        targets = []
        for config in self.config_data:
            tables_data = self._get_table_names(config)
            for table_data in tables_data:
                targets.append(PostgresTarget(table_data['schema'],
                                              table_data['table'],
                                              where='z = {}'.format(self.zoom_level)))
        return targets

    def _get_table_name(self, table_data):
        return "\"{schema}\".\"{table}\"".format(schema=table_data['schema'], table=table_data['table'])

    def _get_csv_filename(self, table_data):
        if not os.path.exists('tmp/tiler'):
            os.makedirs('tmp/tiler')
        return "tmp/tiler/tiler_{table_name}_{geography}.csv".format(table_name=table_data['table'],
                                                                     geography=self.geography)
