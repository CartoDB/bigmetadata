from tasks.targets import PostgresTarget
from tasks.tiler.xyz import SimpleTilerDOXYZTableTask
from luigi import WrapperTask
from lib.logger import get_logger

LOGGER = get_logger(__name__)

GEOGRAPHY_LEVELS = {
    'province': 'ca.statcan.geo.pr_',
    'census_division': 'ca.statcan.geo.cd_',
    'forward_sortation_area': 'ca.statcan.geo.fsa',
    'dissemination_area': 'ca.statcan.geo.da_',
}

GEONAME_COLUMN = 'geoname'
GEOGRAPHY_NAME_COLUMNS = {
    'province': 'ca.statcan.geo.pr__name',
    'census_division': 'ca.statcan.geo.cd__name',
    'forward_sortation_area': 'ca.statcan.geo.fsa_name',
    'dissemination_area': 'ca.statcan.geo.da__name',
}


class SimpleDOXYZTables(SimpleTilerDOXYZTableTask):
    country = 'ca'

    def __init__(self, *args, **kwargs):
        super(SimpleDOXYZTables, self).__init__(*args, **kwargs)
        self.mc_geography_level = self.geography

    def get_config_file(self):
        return 'ca_all.json'

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
        return PostgresTarget('tiler', 'xyz_ca_do_geoms')


class AllSimpleDOXYZTables(WrapperTask):

    def requires(self):
        for zoom in range(0, 15):
            yield SimpleDOXYZTables(zoom_level=zoom, geography=self._get_geography_level(zoom))

    def _get_geography_level(self, zoom):
        if zoom >= 0 and zoom <= 4:
            return 'province'
        elif zoom >= 5 and zoom <= 8:
            return 'census_division'
        elif zoom >= 9 and zoom <= 11:
            return 'forward_sortation_area'
        elif zoom >= 12 and zoom <= 14:
            return 'dissemination_area'
