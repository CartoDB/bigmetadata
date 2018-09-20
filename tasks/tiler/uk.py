from tasks.targets import PostgresTarget
from tasks.tiler.xyz import SimpleTilerDOXYZTableTask
from luigi import WrapperTask
from lib.logger import get_logger

LOGGER = get_logger(__name__)

GEOGRAPHY_LEVELS = {
    'postcode_area': 'uk.datashare.pa_geo',
    'postcode_district': 'uk.odl.pd_geo',
    'postcode_sector': 'uk.odl.ps_geo',
}

GEONAME_COLUMN = 'geoname'
GEOGRAPHY_NAME_COLUMNS = {
    'postcode_area': 'uk.datashare.pa_name',
    'postcode_district': 'uk.odl.pd_id',
    'postcode_sector': 'uk.odl.ps_id',
}


class SimpleDOXYZTables(SimpleTilerDOXYZTableTask):
    country = 'uk'

    def __init__(self, *args, **kwargs):
        super(SimpleDOXYZTables, self).__init__(*args, **kwargs)
        self.mc_geography_level = self.geography

    def get_config_file(self):
        return 'uk_all.json'

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
        return PostgresTarget('tiler', 'xyz_uk_do_geoms')


class AllSimpleDOXYZTables(WrapperTask):

    def requires(self):
        for zoom in range(0, 15):
            yield SimpleDOXYZTables(zoom_level=zoom, geography=self._get_geography_level(zoom))

    def _get_geography_level(self, zoom):
        if zoom >= 0 and zoom <= 5:
            return 'postcode_area'
        elif zoom >= 6 and zoom <= 9:
            return 'postcode_district'
        elif zoom >= 10 and zoom <= 14:
            return 'postcode_sector'
