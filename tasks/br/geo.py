from luigi import Task, Parameter, WrapperTask

from tasks.util import (DownloadUnzipTask, shell, Shp2TempTableTask,
                        ColumnsTask, TableTask)
from tasks.meta import GEOM_REF, OBSColumn, current_session
from tasks.tags import SectionTags, SubsectionTags, BoundaryTags
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

DATA_STATES = (
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
    'sp_capital',
    'sp_exceto_a_capital',
    'to'
)

class BaseParams:
    __metaclass__ = ABCMeta

    resolution = Parameter(default=GEO_I)
    state = Parameter(default='ac')


class DownloadGeography(BaseParams, DownloadUnzipTask):

    PATH = 'ftp://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2010/setores_censitarios_shp/{state}/'

    FILENAME = '{state}_{resolution}.zip'

    def download(self):

        path = self.PATH.format(state=self.state)

        res = self.resolution
        if self.state == 'go': #exception here for go
            res = res.replace('_', '%20_')

        filename = self.FILENAME.format(state=self.state, resolution=res)

        shell('wget -O {output}.zip {url}'.format(
            output=self.output().path,
            url=path + filename
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


class ImportAllStates(BaseParams, WrapperTask):

    def requires(self):
        for state in STATES:
            yield ImportGeography(state=state)


class ImportAllGeographies(BaseParams, WrapperTask):

    def requires(self):
        for resolution in GEOGRAPHIES:
            yield ImportGeography(resolution=resolution)


class GeographyColumns(ColumnsTask):

    resolution = Parameter()

    weights = {
        GEO_M: 4,
        GEO_D: 3,
        GEO_S: 2,
        GEO_I: 1,
    }

    def version(self):
        return 3

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'boundary': BoundaryTags(),
        }

    def columns(self):
        sections = self.input()['sections']
        subsections = self.input()['subsections']
        boundary_type = self.input()['boundary']
        geom = OBSColumn(
            id=self.resolution,
            type='Geometry',
            name=GEOGRAPHY_NAMES[self.resolution],
            description=GEOGRAPHY_DESCS[self.resolution],
            weight=self.weights[self.resolution],
            tags=[sections['br'], subsections['boundary'],boundary_type['interpolation_boundary'],
                  boundary_type['cartographic_boundary']],
        )
        geom_id = OBSColumn(
            id=self.resolution + '_id',
            type='Text',
            weight=0,
            targets={geom: GEOM_REF},
        )
        return OrderedDict([
            ('geom_id', geom_id),   #
            ('the_geom', geom),     # the_geom
        ])


class Geography(TableTask):

    resolution = Parameter()

    def version(self):
        return 2

    def requires(self):
        import_data = {}
        for state in STATES:
            import_data[state] = ImportGeography(state=state, resolution=self.resolution)
        return {
            'data': import_data,
            'columns': GeographyColumns(resolution=self.resolution)
        }

    def timespan(self):
        return 2010

    def columns(self):
        return self.input()['columns']

    def populate(self):
        session = current_session()
        for _, input_ in self.input()['data'].iteritems():
            intable = input_.table
            session.execute('INSERT INTO {output} '
                            'SELECT {code} as geom_id, '
                            '       wkb_geometry as geom '
                            'FROM {input} '.format(
                                output=self.output().table,
                                code=GEOGRAPHY_CODES[self.resolution],
                                input=intable))


class AllGeographies(BaseParams, WrapperTask):

    def requires(self):
        for resolution in GEOGRAPHIES:
            yield Geography(resolution=resolution)
