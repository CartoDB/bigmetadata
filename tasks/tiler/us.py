from tasks.us.census.tiger import ShorelineClip
from tasks.targets import PostgresTarget
from tasks.tiler.xyz import TilerXYZTableTask, TilesTempTable, SimpleTilerDOXYZTableTask
from luigi import WrapperTask
from lib.logger import get_logger
from tasks.meta import current_session

LOGGER = get_logger(__name__)

GEOGRAPHY_LEVELS = {
    'state': 'us.census.tiger.state',
    'county': 'us.census.tiger.county',
    'census_tract': 'us.census.tiger.census_tract',
    'block_group': 'us.census.tiger.block_group',
    'block': 'us.census.tiger.block'
}

GEONAME_COLUMN = 'geoname'
GEOGRAPHY_NAME_COLUMNS = {
    'state': 'us.census.tiger.state_geoname',
    'county': 'us.census.tiger.county_geoname',
    'census_tract': 'us.census.tiger.census_tract_geoname',
    'block_group': 'us.census.tiger.block_group_geoname',
    'block': 'us.census.tiger.block_geoname',
}


class XYZTables(TilerXYZTableTask):

    def __init__(self, *args, **kwargs):
        super(XYZTables, self).__init__(*args, **kwargs)

    def requires(self):
        return {
            'shorelineclip': ShorelineClip(geography=self.geography, year='2015'),
            'tiles': TilesTempTable(geography=self.geography,
                                    zoom_level=self.zoom_level,
                                    config_file=self.get_config_file())
        }

    def get_config_file(self):
        return 'us_all.json'

    def get_geography_level(self, level):
        return GEOGRAPHY_LEVELS[level]

    def get_table_columns(self, config, shard_value=None):
        columns = []
        for column in self.get_columns(config, shard_value):
            nullable = '' if column['nullable'] else 'NOT NULL'
            columns.append("{} {} {}".format(column.get('column_alias', column['column_name']),
                                             column['type'],
                                             nullable))
        return columns

    def get_columns(self, config, shard_value=None):
        columns = []
        # TODO Make possible to define columns prefix as in the JSON
        if config['table'] == 'xyz_us_mc':
            mc_dates = [date.replace('/', '') for date in self._get_mc_dates(shard_value)]
            for mc_date in mc_dates:
                for mc_category in config['mc_categories']:
                    for column in config['columns']:
                        column_name = "{}".format('_'.join([column['column_name'],
                                                            mc_category['id'],
                                                            mc_date]))
                        columns.append({"id": column['id'], "column_name": column_name,
                                        "type": column['type'], "nullable": ['nullable']})
        else:
            for column in config['columns']:
                if column['id'] == GEONAME_COLUMN:
                    column['id'] = GEOGRAPHY_NAME_COLUMNS[self.geography]
                columns.append(column)

        return columns

    def _get_recordset(self, config, shard_value=None):
        columns = self.get_columns(config, shard_value)
        recordset = ["mvtdata->>'id' as id"]
        recordset.append("(mvtdata->>'area_ratio')::numeric as area_ratio")
        recordset.append("(mvtdata->>'area')::numeric as area")
        for column in columns:
            recordset.append("(mvtdata->>'{name}')::{type} as {alias}".format(name=column['column_name'].lower(),
                                                                              type=column['type'],
                                                                              alias=column.get('column_alias',
                                                                                               column['column_name'])))

        return recordset

    def get_tile_query(self, config, tile, geography, shard_value=None):
        columns = [column['id'] for column in config['columns']]
        recordset = self._get_recordset(config, shard_value)
        if config['table'] == 'xyz_us_mc':
            mc_categories = [category['id'] for category in config['mc_categories']]
            mc_dates = self._get_mc_dates(shard_value)
            return '''
                SELECT {x} x, {y} y, {z} z, NULL as mvtgeom, {recordset}
                FROM cdb_observatory.OBS_GetMCDOMVT({z},{x},{y},'{geography_level}',
                                                    ARRAY[]::TEXT[],
                                                    ARRAY['{cols}']::TEXT[],
                                                    ARRAY['{mccategories}']::TEXT[],
                                                    ARRAY['{mcdates}']::TEXT[])
                WHERE mvtgeom IS NOT NULL;
                '''.format(x=tile[0], y=tile[1], z=tile[2], geography_level=geography,
                           recordset=", ".join(recordset),
                           cols="', '".join(columns),
                           mccategories="', '".join(mc_categories),
                           mcdates="','".join(mc_dates))
        else:
            return '''
                SELECT {x} x, {y} y, {z} z, ST_CollectionExtract(ST_MakeValid(mvtgeom), 3) mvtgeom, {recordset}
                FROM cdb_observatory.OBS_GetMCDOMVT({z},{x},{y},'{geography_level}',
                                                    ARRAY['{cols}']::TEXT[],
                                                    ARRAY[]::TEXT[],
                                                    ARRAY[]::TEXT[],
                                                    ARRAY[]::TEXT[])
                WHERE mvtgeom IS NOT NULL;
                '''.format(x=tile[0], y=tile[1], z=tile[2], geography_level=geography,
                           recordset=", ".join(recordset),
                           cols="', '".join(columns))

    def _get_mc_dates(self, month="02"):
        session = current_session()
        query = '''
                SELECT cdb_observatory.OBS_GetMCDates('{schema}', '{geo_level}', '{month}');
                '''.format(schema='us.mastercard', geo_level=self.geography, month=month)
        return session.execute(query).fetchone()[0]


class AllUSXYZTables(WrapperTask):

    def requires(self):
        for zoom in range(0, 15):
            yield XYZTables(zoom_level=zoom, geography=self._get_geography_level(zoom))

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


class SimpleDOXYZTables(SimpleTilerDOXYZTableTask):
    country = 'us'

    def __init__(self, *args, **kwargs):
        super(SimpleDOXYZTables, self).__init__(*args, **kwargs)

    def get_config_file(self):
        return 'us_all.json'

    def get_geography_name(self):
        return GEOGRAPHY_LEVELS[self.geography]

    def get_columns_ids(self):
        columns_ids = []

        for column in self._get_columns():
            if column['id'] == GEONAME_COLUMN:
                column['id'] = GEOGRAPHY_NAME_COLUMNS[self.geography]
            columns_ids.append(column['id'])

        return columns_ids

    def output(self):
        return PostgresTarget('tiler', 'xyz_us_do_geoms')


class AllSimpleDOXYZTables(WrapperTask):

    def requires(self):
        for zoom in range(0, 15):
            yield SimpleDOXYZTables(zoom_level=zoom, geography=self._get_geography_level(zoom))

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
