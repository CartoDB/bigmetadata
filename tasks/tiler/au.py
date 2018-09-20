from tasks.tiler.xyz import SimpleTilerDOXYZTableTask
from luigi import WrapperTask
from lib.logger import get_logger

LOGGER = get_logger(__name__)

GEOGRAPHY_LEVELS = {
    'state': 'au.geo.STE',
    'sa4': 'au.geo.SA4',
    'sa3': 'au.geo.SA3',
    'sa2': 'au.geo.SA2',
    'sa1': 'au.geo.SA1',
    'mesh_block': 'au.geo.MB',
}

GEONAME_COLUMN = 'geoname'
GEOGRAPHY_NAME_COLUMNS = {
    'state': 'au.geo.STE_name',
    'sa4': 'au.geo.SA4_name',
    'sa3': 'au.geo.SA3_name',
    'sa2': 'au.geo.SA2_name',
    'sa1': 'au.geo.SA1_name',
    'mesh_block': 'au.geo.MB_name',
}


class SimpleDOXYZTables(SimpleTilerDOXYZTableTask):
    country = 'au'

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
        elif zoom >= 5 and zoom <= 7:
            return 'sa4'
        elif zoom >= 8 and zoom <= 9:
            return 'sa3'
        elif zoom >= 10 and zoom <= 11:
            return 'sa2'
        elif zoom >= 12 and zoom <= 13:
            return 'sa1'
        elif zoom == 14:
            return 'mesh_block'
