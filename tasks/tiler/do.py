from tasks.us.census.tiger import ShorelineClip
from luigi import IntParameter, Parameter, WrapperTask, Task
from tasks.meta import current_session
from lib.logger import get_logger
import quadkey
import json
import os

LOGGER = get_logger(__name__)

class XYZUSTables(Task):

    zoom_level = IntParameter()
    geography = Parameter()

    def requires(self):
        return {
            'shorelineclip': ShorelineClip(geography=self.geography, year='2015')
        }

    def run(self):
        config_data = self._get_config_data()
        for table_config in config_data:
            table_schema = self._get_table_schema(table_config)
            self._create_schema_and_table(table_schema)
            self._generate_tiles(self.zoom_level, table_schema['table_name'], table_config['columns'])

    def _create_schema_and_table(self, table_schema):
            session = current_session()
            session.execute('CREATE SCHEMA IF NOT EXISTS tiler')
            # TODO Do this dynamically
            columns = table_schema['columns']['do'] + table_schema['columns']['mastercard']
            sql_table = '''CREATE TABLE IF NOT EXISTS {}(
                    x INTEGER NOT NULL,
                    y INTEGER NOT NULL,
                    z INTEGER NOT NULL,
                    quadint NUMERIC NOT NULL,
                    mvt_geometry Geometry NOT NULL,
                    geoid VARCHAR NOT NULL,
                    area_ratio NUMERIC,
                    {},
                    CONSTRAINT xyzusall_pk PRIMARY KEY (quadint,geoid)
                )'''.format(table_schema['table_name'], ", ".join(columns))
            session.execute(sql_table)
            session.commit()

    def _get_config_data(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with (open('{}/us_all.json'.format(dir_path))) as f:
            return json.load(f)

    def _get_table_schema(self, table_config):
        table_name = "{}.{}".format(table_config['schema'], table_config['table'])
        columns = {}
        for dataset, col_data in table_config['columns'].items():
            columns[dataset] = []
            for column in col_data:
                nullable = '' if column['nullable'] else 'NOT NULL'
                columns[dataset].append("{} {} {}".format(column['column_name'], column['type'], nullable))
        return {"table_name": table_name, "columns": columns}

    def _generate_tiles(self, zoom, table_name, columns_config):
        session = current_session()
        do_columns = [column['id'] for column in columns_config['do']]
        mc_columns = [column['id'] for column in columns_config['mastercard']]
        recordset = ["mvtdata->>'id' as id"]
        recordset.append("(mvtdata->>'area_ratio')::numeric as area_ratio")
        for dataset, columns in columns_config.items():
            recordset += ["(mvtdata->>'{}')::{} as {}".format(column['column_name'], column['type'], column['column_name']) for column in columns]
        for x in range(0,(pow(zoom,2) + 1)):
            for y in range(0,(pow(zoom,2) + 1)):
                quadint = quadkey.xyz2quadint(x,y,zoom)
                geography = self._get_geography_level(zoom)
                # TODO store it and make a COPY which is faster
                sql_tile = '''
                    INSERT INTO {table}
                    (SELECT {x}, {y}, {z}, {quadint}, mvtgeom, {recordset}
                    FROM cdb_observatory.OBS_GetMCDOMVT({x},{y},{z},'{geography}',ARRAY['{docols}']::TEXT[],ARRAY['{mccols}']::TEXT[]));'''.format(table = table_name, x = x, y = y, z = zoom, quadint = quadint, geography = geography, recordset = ", ".join(recordset), docols = "', '".join(do_columns), mccols = "', '".join(mc_columns))
                LOGGER.info(sql_tile)
                session.execute(sql_tile)
            session.commit()


    def _get_geography_level(self, zoom):
        if zoom >= 0 and zoom <= 4:
            return 'us.census.tiger.state'
        elif zoom >= 5 and zoom <= 8:
            return 'us.census.tiger.county'
        elif zoom >= 9 and zoom <= 11:
            return 'us.census.tiger.census_tract'
        elif zoom >= 12 and zoom <= 13:
            return 'us.census.tiger.block_group'
        elif zoom == 14:
            return 'us.census.tiger.block'


class AllXYZTables(WrapperTask):

    def requires(self):
        for zoom in rage(1,2):
            yield XYZUSTables(zoom_level=zoom, geography='state')
