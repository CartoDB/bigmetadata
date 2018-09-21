from luigi import Parameter, WrapperTask

from lib.timespan import get_timespan

from tasks.base_tasks import (ColumnsTask, RepoFileUnzipTask, GeoFile2TempTableTask, TableTask, SimplifiedTempTableTask)
from tasks.util import shell, copyfile
from tasks.meta import GEOM_REF, GEOM_NAME, OBSTable, OBSColumn, current_session
from tasks.tags import SectionTags, SubsectionTags, BoundaryTags
from tasks.ca.statcan.license import LicenseTags, SourceTags

from collections import OrderedDict


GEO_CT = 'ct_'
GEO_PR = 'pr_'
GEO_CD = 'cd_'
GEO_CSD = 'csd'
GEO_CMA = 'cma'
GEO_DA = 'da_'
GEO_FSA = 'fsa'

GEOGRAPHIES = (
    GEO_CT,
    GEO_PR,
    GEO_CD,
    GEO_CSD,
    GEO_CMA,
    GEO_DA,
    GEO_FSA,
)

GEOGRAPHY_NAMES = {
    GEO_CT: 'Census tracts',
    GEO_PR: 'Canada, provinces and territories',
    GEO_CD: 'Census divisions',
    GEO_CSD: 'Census subdivisions',
    GEO_CMA: 'Census metropolitan areas and census agglomerations',
    GEO_DA: 'Census dissemination areas',
    GEO_FSA: 'Forward sortation areas',
}

GEOGRAPHY_DESCS = {
    GEO_CT: '',
    GEO_PR: '',
    GEO_CD: '',
    GEO_CSD: '',
    GEO_CMA: '',
    GEO_DA: '',
    GEO_FSA: '',
}

GEOGRAPHY_PROPERNAMES = {
    GEO_CT: 'CTNAME',
    GEO_PR: 'PRNAME',
    GEO_CD: 'CDNAME',
    GEO_CSD: 'CSDNAME',
    GEO_CMA: 'CMANAME',
    GEO_DA: 'DAUID',  # DA has no proper name
    GEO_FSA: 'PRNAME',
}

GEOGRAPHY_CODES = {
    GEO_CT: 401,
    GEO_PR: 101,
    GEO_CD: 701,
    GEO_CSD: 301,
    GEO_CMA: 201,
    GEO_DA: 1501,
    GEO_FSA: 1601,
}

GEOGRAPHY_TAGS = {
    GEO_CT: ['cartographic_boundary'],
    GEO_PR: ['cartographic_boundary', 'interpolation_boundary'],
    GEO_CD: ['cartographic_boundary', 'interpolation_boundary'],
    GEO_CSD: ['cartographic_boundary', 'interpolation_boundary'],
    GEO_CMA: ['cartographic_boundary'],
    GEO_DA: ['cartographic_boundary', 'interpolation_boundary'],
    GEO_FSA: ['cartographic_boundary', 'interpolation_boundary'],
}

GEOGRAPHY_FORMAT = {
    GEO_CT: 'a',  # SHP
    GEO_PR: 'a',  # SHP
    GEO_CD: 'a',  # SHP
    GEO_CSD: 'a',  # SHP
    GEO_CMA: 'a',  # SHP
    GEO_DA: 'a',  # SHP
    GEO_FSA: 'g',  # GML
}

GEOGRAPHY_UID = {
    GEO_CT: 'ctuid',
    GEO_PR: 'pruid',
    GEO_CD: 'cduid',
    GEO_CSD: 'csduid',
    GEO_CMA: 'cmauid',
    GEO_DA: 'dauid',
    GEO_FSA: 'cfsauid',
}


# http://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/bound-limit-2011-eng.cfm
# 2011 Boundary Files
class DownloadGeography(RepoFileUnzipTask):

    resolution = Parameter(default=GEO_PR)
    year = Parameter(default="2011")

    URL = 'http://www12.statcan.gc.ca/census-recensement/{year}/geo/bound-limit/files-fichiers/g{resolution}000b11{format}_e.zip'

    def get_url(self):
        return self.URL.format(year=self.year,
                               resolution=self.resolution,
                               format=GEOGRAPHY_FORMAT[self.resolution])


class ImportGeography(GeoFile2TempTableTask):
    '''
    Import geographies into postgres by geography level
    '''

    resolution = Parameter(default=GEO_PR)
    extension = Parameter(default='shp', significant=False)

    def requires(self):
        return DownloadGeography(resolution=self.resolution)

    def input_files(self):
        cmd = 'ls {input}/*.{extension}'.format(
            input=self.input().path,
            extension=self.extension
        )
        for geofile in shell(cmd).strip().split('\n'):
            yield geofile


class SimplifiedImportGeography(SimplifiedTempTableTask):
    resolution = Parameter(default=GEO_PR)

    def requires(self):
        extension = 'gml' if self.resolution == GEO_FSA else 'shp'
        return ImportGeography(resolution=self.resolution, extension=extension)


class GeographyColumns(ColumnsTask):

    resolution = Parameter(default=GEO_PR)

    weights = {
        GEO_PR: 1,
        GEO_CD: 2,
        GEO_CMA: 3,
        GEO_CSD: 4,
        GEO_CT: 5,
        GEO_DA: 6,
        GEO_FSA: 7,
    }

    def version(self):
        return 15

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'boundary': BoundaryTags(),
            'license': LicenseTags(),
            'source': SourceTags(),
        }

    def columns(self):
        input_ = self.input()
        sections = input_['sections']
        subsections = input_['subsections']
        boundary_type = input_['boundary']
        data_license = input_['license']['statcan-license']
        data_source = input_['source']['statcan-census-2011']
        geom = OBSColumn(
            id=self.resolution,
            type='Geometry',
            name=GEOGRAPHY_NAMES[self.resolution],
            description=GEOGRAPHY_DESCS[self.resolution],
            weight=self.weights[self.resolution],
            tags=[sections['ca'], subsections['boundary'], data_license, data_source],
        )
        geom_id = OBSColumn(
            id=self.resolution + '_id',
            type='Text',
            weight=0,
            targets={geom: GEOM_REF},
        )
        geom_name = OBSColumn(
            id=self.resolution + '_name',
            type='Text',
            weight=1,
            name='Name of ' + GEOGRAPHY_NAMES[self.resolution],
            targets={geom: GEOM_NAME},
            tags=[sections['ca'], subsections['names'], data_license, data_source]
        )
        geom.tags.extend(boundary_type[i] for i in GEOGRAPHY_TAGS[self.resolution])

        return OrderedDict([
            ('geom_name', geom_name),
            ('geom_id', geom_id),   # cvegeo
            ('the_geom', geom),     # the_geom
        ])


class Geography(TableTask):
    '''
    '''

    resolution = Parameter(default=GEO_PR)

    def version(self):
        return 10

    def requires(self):
        return {
            'data': SimplifiedImportGeography(resolution=self.resolution),
            'columns': GeographyColumns(resolution=self.resolution)
        }

    def table_timespan(self):
        return get_timespan('2011')

    # TODO: https://github.com/CartoDB/bigmetadata/issues/435
    def targets(self):
        return {
            OBSTable(id='.'.join([self.schema(), self.name()])): GEOM_REF,
        }

    def columns(self):
        return self.input()['columns']

    def populate(self):
        session = current_session()
        session.execute('INSERT INTO {output} '
                        'SELECT {name} as geom_name, '
                        '       {code} as geom_id, '
                        '       wkb_geometry as geom '
                        'FROM {input} '.format(
                            name=GEOGRAPHY_PROPERNAMES[self.resolution],
                            output=self.output().table,
                            code=GEOGRAPHY_UID[self.resolution],
                            input=self.input()['data'].table))


class AllGeographies(WrapperTask):

    def requires(self):
        for resolution in GEOGRAPHIES:
            yield Geography(resolution=resolution)


class AllGeographyColumns(WrapperTask):

    def requires(self):
        for resolution in GEOGRAPHIES:
            yield GeographyColumns(resolution=resolution)
