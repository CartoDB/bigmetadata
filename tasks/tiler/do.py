from tasks.us.census.tiger import ShorelineClip
from tasks.targets import PostgresTarget
from luigi import IntParameter, Parameter, WrapperTask, Task
from tasks.meta import current_session, async_pool
from lib.logger import get_logger
from lib.tileutils import tile2bounds
import json
import os
import time
import asyncio
import concurrent.futures
import backoff
import csv

LOGGER = get_logger(__name__)

GEOGRAPHY_LEVELS = {
    'state': 'us.census.tiger.state',
    'county': 'us.census.tiger.county',
    'census_tract': 'us.census.tiger.census_tract',
    'block_group': 'us.census.tiger.block_group',
    'block': 'us.census.tiger.block'
}


# Check if two bounding boxes intersect
# rects are assumed to be [xmin, ymin, xmax, ymax]
def bboxes_intersect(rect1, rect2):
    return not (rect2[0] > rect1[2]
                or rect2[2] < rect1[0]
                or rect2[3] < rect1[1]
                or rect2[1] > rect1[3])


class XYZUSTables(Task):

    zoom_level = IntParameter()
    geography = Parameter()

    def __init__(self, *args, **kwargs):
        super(XYZUSTables, self).__init__(*args, **kwargs)
        self.config_data = self._get_config_data()
        if not os.path.exists('tmp/tiler'):
            os.makedirs('tmp/tiler')
        self._csv_filename = 'tmp/tiler/tile_{}.csv'.format(self.task_id)

    def requires(self):
        return {
            'shorelineclip': ShorelineClip(geography=self.geography, year='2015')
        }

    def run(self):
        config_data = self._get_config_data()
        for table_config in config_data:
            table_schema = self._get_table_schema(table_config)
            table_bboxes = table_config['bboxes']
            self._create_schema_and_table(table_schema)
            self._generate_and_insert_tiles(self.zoom_level, table_schema, table_config['columns'], table_bboxes)

    def _create_schema_and_table(self, table_schema):
        session = current_session()
        session.execute('CREATE SCHEMA IF NOT EXISTS tiler')
        cols_schema = []
        table_name = "{}.{}".format(table_schema['schema'], table_schema['table_name'])
        for _, cols in table_schema['columns'].items():
            cols_schema += cols
        sql_table = '''CREATE TABLE IF NOT EXISTS {}(
                x INTEGER NOT NULL,
                y INTEGER NOT NULL,
                z INTEGER NOT NULL,
                mvt_geometry Geometry NOT NULL,
                geoid VARCHAR NOT NULL,
                area_ratio NUMERIC,
                {},
                CONSTRAINT xyzusall_pk PRIMARY KEY (x,y,z,geoid)
            )'''.format(table_name, ", ".join(cols_schema))
        session.execute(sql_table)
        session.commit()

    def _get_config_data(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with (open('{}/us_all.json'.format(dir_path))) as f:
            return json.load(f)

    def _get_table_schema(self, table_config):
        columns = {}
        for dataset, col_data in table_config['columns'].items():
            columns[dataset] = []
            for column in col_data:
                nullable = '' if column['nullable'] else 'NOT NULL'
                columns[dataset].append("{} {} {}".format(column['column_name'], column['type'], nullable))
        return {"schema": table_config['schema'], "table_name": table_config['table'], "columns": columns}

    def _tile_in_bboxes(self, zoom, x, y, bboxes):
        rect1 = tile2bounds(zoom, x, y)
        for bbox in bboxes:
            rect2 = [bbox['xmin'], bbox['ymin'], bbox['xmax'], bbox['ymax']]
            if bboxes_intersect(rect1, rect2):
                return True

        return False

    def _generate_and_insert_tiles(self, zoom, table_schema, columns_config, bboxes_config):
        table_name = "{}.{}".format(table_schema['schema'], table_schema['table_name'])
        do_columns = [column['id'] for column in columns_config['do']]
        mc_columns = [column['id'] for column in columns_config['mc']]
        recordset = ["mvtdata->>'id' as id"]
        recordset.append("(mvtdata->>'area_ratio')::numeric as area_ratio")
        for _, columns in columns_config.items():
            recordset += ["(mvtdata->>'{}')::{} as {}".format(column['column_name'], column['type'], column['column_name']) for column in columns]
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
            exceptions = loop.run_until_complete(self._generate_tiles(tiles, recordset, do_columns, mc_columns))
            LOGGER.info("Exception/s found processing tiles: {}".format(exceptions))
        finally:
            loop.close()
        sql_end = time.time()
        LOGGER.info("Generation tiles time was {} seconds".format(sql_end - sql_start))
        self._insert_tiles(table_name)

    @backoff.on_exception(backoff.expo,
                          (asyncio.TimeoutError,
                          concurrent.futures._base.TimeoutError),
                          max_time=600)
    async def _generate_tiles(self, tiles, recordset, do_columns, mc_columns):
        with open(self._csv_filename, 'w+') as csvfile:
            db_pool = await async_pool()
            csvwriter = csv.writer(csvfile)
            geography_level = GEOGRAPHY_LEVELS[self.geography]
            executed_tiles = [self._generate_tile(db_pool, csvwriter, tile, geography_level, recordset, do_columns, mc_columns) for tile in tiles]
            exceptions = await asyncio.gather(*executed_tiles, return_exceptions=True)
            return exceptions

    @backoff.on_exception(backoff.expo,
                          (asyncio.TimeoutError,
                          concurrent.futures._base.TimeoutError),
                          max_time=600)
    async def _generate_tile(self, db_pool, csvwriter, tile, geography, recordset, do_columns, mc_columns):
        sql_tile = '''
            SELECT {x}, {y}, {z}, ST_CollectionExtract(ST_MakeValid(mvtgeom), 3) mvtgeom, {recordset}
            FROM cdb_observatory.OBS_GetMCDOMVT({z},{x},{y},'{geography_level}',ARRAY['{docols}']::TEXT[],ARRAY['{mccols}']::TEXT[])
            WHERE mvtgeom IS NOT NULL;
            '''.format(x=tile[0], y=tile[1], z=tile[2], geography_level=geography,
                       recordset=", ".join(recordset), docols="', '".join(do_columns),
                       mccols="', '".join(mc_columns))
        conn = None
        try:
            conn = await db_pool.acquire()
            tile_start = time.time()
            records = await conn.fetch(sql_tile)
            tile_end = time.time()
            LOGGER.debug('Generated tile {},{},{} in {} seconds'.format(tile[0], tile[1], tile[2], (tile_end - tile_start)))
            for record in records:
                csvwriter.writerow(tuple(record))
            else:
                LOGGER.debug('Tile {},{},{} without data'.format(tile[0], tile[1], tile[2], (tile_end - tile_start)))
        except BaseException as e:
            LOGGER.error('Tile {},{},{} returned exception {}'.format(tile[0],tile[1],tile[2],e))
            raise e
        finally:
            await db_pool.release(conn)

    def _insert_tiles(self, table_name):
        copy_start = time.time()
        session = current_session()
        with open(self._csv_filename,'rb') as f:
            cursor = session.connection().connection.cursor()
            cursor.copy_from(f, table_name, sep=',', null='')
            session.commit()
        copy_end = time.time()
        LOGGER.info("Copy tiles time was {} seconds".format(copy_end - copy_start))

    def output(self):
        targets = []
        for table_config in self.config_data:
            table_schema = self._get_table_schema(table_config)
            targets.append(PostgresTarget(table_schema['schema'], table_schema['table_name'], where='z = {}'.format(self.zoom_level)))
        return targets


class AllXYZTables(WrapperTask):

    def requires(self):
        for zoom in range(0, 15):
            yield XYZUSTables(zoom_level=zoom, geography=self._get_geography_level(zoom))

    def _get_geography_level(self, zoom):
        if zoom >= 0 and zoom <= 4:
            return 'state'
        elif zoom >= 5 and zoom <= 8:
            return 'county'
        elif zoom >= 9 and zoom <= 11:
            return 'census_tract'
        elif zoom >= 12 and zoom <= 13:
            return 'block_group'
        elif zoom == 14:
            return 'block'
