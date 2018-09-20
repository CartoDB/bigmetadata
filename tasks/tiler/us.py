from tasks.tiler.xyz import SimpleTilerDOXYZTableTask
from luigi import WrapperTask
from lib.logger import get_logger

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


class SimpleDOXYZTables(SimpleTilerDOXYZTableTask):
    country = 'us'

    def __init__(self, *args, **kwargs):
        super(SimpleDOXYZTables, self).__init__(*args, **kwargs)

    def get_geography_name(self):
        return GEOGRAPHY_LEVELS[self.geography]

    def get_columns_ids(self):
        columns_ids = []

        for column in self._get_columns():
            if column['id'] == GEONAME_COLUMN:
                column['id'] = GEOGRAPHY_NAME_COLUMNS[self.geography]
            columns_ids.append(column['id'])

        return columns_ids


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
