from luigi import Task, Parameter, WrapperTask

from tasks.util import (DownloadUnzipTask, shell, Shp2TempTableTask,
                        ColumnsTask, TableTask, TempTableTask)
from tasks.meta import GEOM_REF, OBSColumn, current_session
# from tasks.mx.inegi_columns import DemographicColumns
from tasks.tags import SectionTags, SubsectionTags, UnitTags

from collections import OrderedDict


GEOGRAPHIES = (
    'ct',
    'pr',
)


GEOGRAPHY_NAMES = {
    'ct': 'Census Tracts',
    'pr': 'Provinces/Territories',
}

GEOGRAPHY_DESCS = {
    'ct': '',
    'pr': '',
}


# http://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/bound-limit-2011-eng.cfm
# 2011 Boundary Files
class DownloadGeography(DownloadUnzipTask):

    geog_lvl = Parameter()

    URL = 'http://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/g{geog_lvl}_000b11a_e.zip'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(
            output=self.output().path, url=self.URL.format(geog_lvl=self.geog_lvl)
        ))


class ImportGeography(Shp2TempTableTask):
    '''
    Import geographies into postgres by geography level
    '''

    geog_lvl = Parameter()

    def requires(self):
        return DownloadGeography(geog_lvl=self.geog_lvl)

    def input_shp(self):
        cmd = 'ls {input}/*.shp'.format(
            input=self.input().path
        )
        for shp in shell(cmd).strip().split('\n'):
            yield shp


class GeographyColumns(ColumnsTask):

    geog_lvl = Parameter()

    weights = {
        'ct': 5,
        'pr': 4,
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

    geog_lvl = Parameter()

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
