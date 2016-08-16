from luigi import Task, Parameter, WrapperTask

from tasks.util import (DownloadUnzipTask, shell, Shp2TempTableTask,
                        ColumnsTask, TableTask, TempTableTask)
from tasks.meta import GEOM_REF, OBSColumn, current_session
# from tasks.mx.inegi_columns import DemographicColumns
from tasks.tags import SectionTags, SubsectionTags, UnitTags

from collections import OrderedDict


GEOGRAPHIES = (
    'ct_',
    'pr_',
    'cd_',
    'cds_',
    'cma_',
)


GEOGRAPHY_NAMES = {
    'ct_': 'Census Tracts',
    'pr_': 'Canada, provinces and territories',
    'cd_': 'Census divisions',
    'csd': 'Census subdivisions',
    'cma': 'Census metropolitan areas and census agglomerations',
}

GEOGRAPHY_DESCS = {
    'ct_': '',
    'pr_': '',
    'cd_': '',
    'csd': '',
    'cma': '',
}

GEOGRAPHY_CODES = {
    'ct_': 401,
    'pr_': 101,
    'cd_': 701,
    'csd': 301,
    'cma': 201,
}


# http://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/bound-limit-2011-eng.cfm
# 2011 Boundary Files
class DownloadGeography(DownloadUnzipTask):

    geog_lvl = Parameter(default='pr_')

    URL = 'http://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/g{geog_lvl}000b11a_e.zip'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(
            output=self.output().path, url=self.URL.format(geog_lvl=self.geog_lvl)
        ))


class ImportGeography(Shp2TempTableTask):
    '''
    Import geographies into postgres by geography level
    '''

    geog_lvl = Parameter(default='pr_')

    def requires(self):
        return DownloadGeography(geog_lvl=self.geog_lvl)

    def input_shp(self):
        cmd = 'ls {input}/*.shp'.format(
            input=self.input().path
        )
        for shp in shell(cmd).strip().split('\n'):
            yield shp


class GeographyColumns(ColumnsTask):

    geog_lvl = Parameter(default='pr_')

    weights = {
        'ct_': 5,
        'pr_': 4,
        'cd_': 4,
        'cds_': 4,
        'cma_': 4,
    }

    def version(self):
        return 4

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
        }

    def columns(self):
        sections = self.input()['sections']
        subsections = self.input()['subsections']
        geom = OBSColumn(
            id=self.geog_lvl,
            type='Geometry',
            name=GEOGRAPHY_NAMES[self.geog_lvl],
            description=GEOGRAPHY_DESCS[self.geog_lvl],
            weight=self.weights[self.geog_lvl],
            tags=[sections['ca'], subsections['boundary']],
        )
        geom_id = OBSColumn(
            type='Text',
            weight=0,
            targets={geom: GEOM_REF},
        )
        return OrderedDict([
            ('geom_id', geom_id),   # cvegeo
            ('the_geom', geom),     # the_geom
        ])


class Geography(TableTask):
    '''
    '''

    geog_lvl = Parameter(default='pr_')

    def version(self):
        return 2

    def requires(self):
        return {
            'data': ImportGeography(geog_lvl=self.geog_lvl),
            'columns': GeographyColumns(geog_lvl=self.geog_lvl)
        }

    def timespan(self):
        return 2011

    def columns(self):
        return self.input()['columns']

    def populate(self):
        session = current_session()
        session.execute('INSERT INTO {output} '
                        'SELECT pruid as geom_id, '
                        '       wkb_geometry as geom '
                        'FROM {input} '.format(
                            output=self.output().table,
                            input=self.input()['data'].table))


class AllGeographies(WrapperTask):

    def requires(self):
        for geog_lvl in GEOGRAPHIES:
            yield Geography(geog_lvl=geog_lvl)
