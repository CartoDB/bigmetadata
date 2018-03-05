from luigi import Task, Parameter, WrapperTask

from lib.timespan import get_timespan

from tasks.base_tasks import (ColumnsTask, DownloadUnzipTask, Shp2TempTableTask, SimplifiedTempTableTask, TableTask,
                              RepoFile)
from tasks.util import shell
from tasks.meta import GEOM_REF, GEOM_NAME, OBSColumn, current_session
from tasks.tags import SectionTags, SubsectionTags, BoundaryTags
from abc import ABCMeta
from collections import OrderedDict
from shutil import copyfile


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
    GEO_M: 'cd_geocodm',   # Counties eg: 1200203
    GEO_D: 'cd_geocodd',   # Districts, eg: 120020305
    GEO_S: 'cd_geocods',   # Subdistricts, eg: 12002030500
    GEO_I: 'cd_geocodi',   # Census tracts, eg: 120020305000030
}

GEOGRAPHY_PROPERNAMES = {
    GEO_M: 'nm_municip',            # Counties eg: 2700102 "Gua Branca"
    GEO_D: 'nm_distrit',            # Districts, eg: 270010205 "Gua Branca"
    GEO_S: 'nm_subdist',            # Subdistricts, eg: 27001020500 ""
    GEO_I: ['nm_micro','nm_meso'],  # Census tracts, eg: 270010205000012 "Serrana do Serto Alagoano", "Serto Alagoana"
}

REGION_TYPE = {
    'nm_micro': 'microregion',
    'nm_meso': 'mesoregion',
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


class BaseParams(metaclass=ABCMeta):
    resolution = Parameter(default=GEO_I)
    state = Parameter(default='ac')


class DownloadGeography(BaseParams, DownloadUnzipTask):

    PATH = 'ftp://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2010/setores_censitarios_shp/{state}/'

    FILENAME = '{state}_{resolution}.zip'

    def version(self):
        return 1

    def requires(self):
        path = self.PATH.format(state=self.state)

        res = self.resolution
        if self.state == 'go':  # exception here for go
            res = res.replace('_', '%20_')

        filename = self.FILENAME.format(state=self.state, resolution=res)

        url = path + filename

        return RepoFile(resource_id=self.task_id,
                        version=self.version(),
                        url=url)

    def download(self):
        self.output().makedirs()
        copyfile(self.input().path, '{output}.zip'.format(output=self.output().path))


class ImportGeography(BaseParams, Shp2TempTableTask):

    def requires(self):
        return DownloadGeography(resolution=self.resolution, state=self.state)

    def input_shp(self):
        cmd = 'ls {input}/*.shp'.format(
            input=self.input().path
        )
        for shp in shell(cmd).strip().split('\n'):
            yield shp


class SimplifiedImportGeography(BaseParams, SimplifiedTempTableTask):
    def get_table_id(self):
        # Removing the hash and the State
        # For example the table_id for 'br.geo.importgeography_setores_censitar_ac_b5dd2616fb'
        # will be 'br.geo.importgeography_setores_censitar'
        return '.'.join([self.input().schema, '_'.join(self.input().tablename.split('_')[:-2])])

    def requires(self):
        return ImportGeography(state=self.state, resolution=self.resolution)


class ImportAllStates(BaseParams, WrapperTask):

    def requires(self):
        for state in STATES:
            yield SimplifiedImportGeography(state=state)


class ImportAllGeographies(BaseParams, WrapperTask):

    def requires(self):
        for resolution in GEOGRAPHIES:
            yield SimplifiedImportGeography(resolution=resolution)


class GeographyColumns(ColumnsTask):

    resolution = Parameter()

    weights = {
        GEO_M: 4,
        GEO_D: 3,
        GEO_S: 2,
        GEO_I: 1,
    }

    def version(self):
        return 5

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'boundary': BoundaryTags(),
        }

    def columns(self):
        sections = self.input()['sections']
        subsections = self.input()['subsections']
        cols = OrderedDict()
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

        if self.resolution != 'setores_censitarios':
            geom_name = OBSColumn(
                id = self.resolution + '_name',
                name="Name of the {}".format(GEOGRAPHY_NAMES[self.resolution]),
                type='Text',
                weight=1,
                tags=[sections['br'], subsections['names']],
                targets={geom: GEOM_NAME}
            )
            cols['{}'.format(GEOGRAPHY_PROPERNAMES[self.resolution])] = geom_name
        else:
            for region in GEOGRAPHY_PROPERNAMES[GEO_I]:
                cols['{}'.format(region)] = OBSColumn(
                    id=self.resolution + region + '_name',
                    name="Name of the {}".format(REGION_TYPE[region]),
                    type='Text',
                    weight=1,
                    tags=[sections['br'], subsections['names']],
                    targets={geom: GEOM_NAME}
                )
        cols['{}'.format(GEOGRAPHY_CODES[self.resolution])] = geom_id
        cols['wkb_geometry'] = geom

        return cols


class Geography(TableTask):

    resolution = Parameter()

    def version(self):
        return 5

    def requires(self):
        import_data = {}
        for state in STATES:
            import_data[state] = SimplifiedImportGeography(state=state, resolution=self.resolution)
        return {
            'data': import_data,
            'columns': GeographyColumns(resolution=self.resolution)
        }

    def table_timespan(self):
        return get_timespan('2010')

    def columns(self):
        return self.input()['columns']

    def populate(self):
        session = current_session()
        for _, input_ in self.input()['data'].items():
            intable = input_.table
            column_targets = self.columns()
            out_colnames = list(column_targets.keys())
            in_columns = ['"{}"::{}'.format(colname, ct.get(session).type)
                          for colname, ct in column_targets.items()]
            session.execute('INSERT INTO {output} ({out_colnames}) '
                            'SELECT {in_columns} '
                            'FROM {input} '.format(
                                output=self.output().table,
                                out_colnames=', '.join(out_colnames),
                                in_columns=', '.join(in_columns),
                                input=intable))


class AllGeographies(BaseParams, WrapperTask):

    def requires(self):
        for resolution in GEOGRAPHIES:
            yield Geography(resolution=resolution)
