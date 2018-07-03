from tasks.us.census.tiger import ShorelineClip
from tasks.tiler.xyz import TilerXYZTableTask
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


class AllUSXYZTables(WrapperTask):

    def requires(self):
        for zoom in range(0, 5):
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
