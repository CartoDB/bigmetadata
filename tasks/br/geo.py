from luigi import Task, Parameter, WrapperTask

from tasks.util import (DownloadUnzipTask, shell, Shp2TempTableTask,
                        ColumnsTask, TableTask)
from tasks.meta import GEOM_REF, OBSColumn, current_session
from tasks.tags import SectionTags, SubsectionTags
from abc import ABCMeta
from collections import OrderedDict


GEO_M = 'municipios'
GEO_D = 'distritos'
GEO_S = 'subdistritos'
GEO_I = 'setores_censitarios'

GEOGRAPHIES = (
    GEO_M,
    GEO_D,
    GEO_S,
    GEO_I,
)

# English names
GEOGRAPHY_NAMES = {
    GEO_M: 'Counties',
    GEO_D: 'Districts',
    GEO_S: 'Subdistricts',
    GEO_I: 'Census tracts',
}

GEOGRAPHY_DESCS = {
    GEO_D: '',
    GEO_M: '',
    GEO_I: '',
    GEO_S: '',
}

GEOGRAPHY_CODES = {
    GEO_M: 'cd_geocodm',   # eg: 1200203
    GEO_D: 'cd_geocodd',   # eg: 120020305
    GEO_S: 'cd_geocods',   # eg: 12002030500
    GEO_I: 'cd_geocodi',   # eg: 120020305000030
}

# 27 Federative Units
STATES = (
    'ac',
    'al',
    'am',
    'ap',
    'ba',
    'ce',
    'df',
    'es',
    'go',
    'ma',
    'mg',
    'ms',
    'mt',
    'pa',
    'pb',
    'pe',
    'pi',
    'pr',
    'rj',
    'rn',
    'ro',
    'rr',
    'rs',
    'sc',
    'se',
    'sp',
    'to'
)

class BaseParams:
    __metaclass__ = ABCMeta

    resolution = Parameter(default=GEO_I)
    state = Parameter(default='ac')


class DownloadGeography(BaseParams, DownloadUnzipTask):

    URL = 'ftp://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2010/setores_censitarios_shp/{state}/{state}_{resolution}.zip'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(
            output=self.output().path, url=self.URL.format(state=self.state, resolution=self.resolution)
        ))


class ImportGeography(BaseParams, Shp2TempTableTask):

    def requires(self):
        return DownloadGeography(resolution=self.resolution, state=self.state)

    def input_shp(self):
        cmd = 'ls {input}/*.shp'.format(
            input=self.input().path
        )
        for shp in shell(cmd).strip().split('\n'):
            yield shp


class GeographyColumns(BaseParams, ColumnsTask):

    weights = {
        GEO_M: 4,
        GEO_D: 3,
        GEO_S: 2,
        GEO_I: 1,
    }

    def version(self):
        return 1

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
        }

    def columns(self):
        sections = self.input()['sections']
        subsections = self.input()['subsections']
        geom = OBSColumn(
            id=self.resolution,
            type='Geometry',
            name=GEOGRAPHY_NAMES[self.resolution],
            description=GEOGRAPHY_DESCS[self.resolution],
            weight=self.weights[self.resolution],
            tags=[sections['br'], subsections['boundary']],
        )
        geom_id = OBSColumn(
            type='Text',
            weight=0,
            targets={geom: GEOM_REF},
        )
        return OrderedDict([
            ('geom_id', geom_id),   #
            ('the_geom', geom),     # the_geom
        ])


class Geography(BaseParams, TableTask):

    def version(self):
        return 1

    def requires(self):
        return {
            'data': ImportGeography(resolution=self.resolution, state=self.state),
            'columns': GeographyColumns(resolution=self.resolution)
        }

    def timespan(self):
        return 2010

    def columns(self):
        return self.input()['columns']

    def populate(self):
        session = current_session()
        session.execute('INSERT INTO {output} '
                        'SELECT {code} as geom_id, '
                        '       wkb_geometry as geom '
                        'FROM {input} '.format(
                            output=self.output().table,
                            code=GEOGRAPHY_CODES[self.resolution],
                            input=self.input()['data'].table))


class AllGeographies(BaseParams, WrapperTask):

    def requires(self):
        for resolution in GEOGRAPHIES:
            yield Geography(resolution=resolution, state=self.state)


class AllStates(BaseParams, WrapperTask):

    def requires(self):
        for state in STATES:
            yield Geography(resolution=self.resolution, state=state)


class AllGeographiesAllStates(WrapperTask):

    def requires(self):
        for resolution in GEOGRAPHIES:
            for state in STATES:
                yield Geography(resolution=resolution, state=state)

