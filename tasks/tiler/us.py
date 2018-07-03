from tasks.us.census.tiger import ShorelineClip
from tasks.tiler.xyz import TilerXYZTableTask
from luigi import WrapperTask
from lib.logger import get_logger
from tasks.meta import current_session, async_pool

LOGGER = get_logger(__name__)

GEOGRAPHY_LEVELS = {
    'state': 'us.census.tiger.state',
    'county': 'us.census.tiger.county',
    'census_tract': 'us.census.tiger.census_tract',
    'block_group': 'us.census.tiger.block_group',
    'block': 'us.census.tiger.block'
}


class XYZTables(TilerXYZTableTask):

    def __init__(self, *args, **kwargs):
        super(XYZTables, self).__init__(*args, **kwargs)

    def requires(self):
        return {
            'shorelineclip': ShorelineClip(geography=self.geography, year='2015')
        }

    def get_config_file(self):
        return 'us_all.json'

    def get_geography_level(self, level):
        return GEOGRAPHY_LEVELS[level]

    def get_table_columns(self, config):
        columns = []
        for column in self.get_columns(config):
            nullable = '' if column['nullable'] else 'NOT NULL'
            columns.append("{} {} {}".format(column['column_name'], column['type'], nullable))
        return columns

    def get_columns(self, config):
        columns = []
        # TODO Make possible to define columns prefix as in the JSON
        if config['table'] == 'xyz_us_mc':
            mc_dates = [date.replace('-', '') for date in self._get_mc_dates()]
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
                columns.append(column)

        return columns

    def get_recordset(self, config):
        columns = self.get_columns(config)
        recordset = ["mvtdata->>'id' as id"]
        recordset.append("(mvtdata->>'area_ratio')::numeric as area_ratio")
        recordset.append("(mvtdata->>'area')::numeric as area")
        if table_config['table'] == 'xyz_us_mc':
            mc_categories = [category['id'] for category in table_config['mc_categories']]
            mc_dates = mc_dates = [date.replace('-', '') for date in self._get_mc_dates()]
            for mc_date in mc_dates:
                for mc_category in table_config['mc_categories']:
                    recordset += ["(mvtdata->>'{name}')::{type} as {name}".format(name='_'.join([column['column_name'],
                                                                                                 mc_category['id'],
                                                                                                 mc_date]).lower(),
                                                                                  type=column['type'])
                                  for column in columns]
        else:
            recordset += ["(mvtdata->>'{name}')::{type} as {name}".format(name=column['column_name'].lower(),
                                                                          type=column['type'])
                          for column in columns]

        return recordset

    def get_tile_query(self, config):
        columns = [column['id'] for column in self.get_columns(config)]
        if config['table'] == 'xyz_us_mc':
            mc_categories = [category['id'] for category in config['mc_categories']]
            mc_dates = mc_dates = [date.replace('-', '') for date in self._get_mc_dates()]
            return '''
                SELECT {x}, {y}, {z}, NULL as mvtgeom, {recordset}
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
                SELECT {x}, {y}, {z}, ST_CollectionExtract(ST_MakeValid(mvtgeom), 3) mvtgeom, {recordset}
                FROM cdb_observatory.OBS_GetMCDOMVT({z},{x},{y},'{geography_level}',
                                                    ARRAY['{cols}']::TEXT[],
                                                    ARRAY[]::TEXT[],
                                                    ARRAY[]::TEXT[],
                                                    ARRAY[]::TEXT[])
                WHERE mvtgeom IS NOT NULL;
                '''.format(x=tile[0], y=tile[1], z=tile[2], geography_level=geography,
                        recordset=", ".join(recordset),
                        cols="', '".join(columns),
                        mccategories="', '".join(mc_categories),
                        mcdates="','".join(mc_dates))

    def _get_mc_dates(self, month="02"):
        session = current_session()
        query = '''
                SELECT cdb_observatory.OBS_GetMCDates('{schema}', '{geo_level}', '{month}');
                '''.format(schema='us.mastercard', geo_level=self.geography, month=month)
        return session.execute(query).fetchone()[0]


class AllUSXYZTables(WrapperTask):

    def requires(self):
        for zoom in range(0, 1):
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
