from luigi import Parameter, IntParameter, WrapperTask

from lib.logger import get_logger
from lib.timespan import get_timespan

from tasks.base_tasks import (ColumnsTask, RepoFileUnzipTask, GeoFile2TempTableTask, TableTask, SimplifiedTempTableTask)
from tasks.util import shell, copyfile
from tasks.meta import GEOM_REF, GEOM_NAME, OBSTable, OBSColumn, current_session
from tasks.tags import SectionTags, SubsectionTags, BoundaryTags
from tasks.ca.statcan.license import LicenseTags, SourceTags

from collections import OrderedDict


LOGGER = get_logger(__name__)

GEO_CT = 'ct_'
GEO_PR = 'pr_'
GEO_CD = 'cd_'
GEO_CSD = 'csd'
GEO_CMA = 'cma'
GEO_DA = 'da_'
GEO_FSA = 'fsa'
GEO_DB = 'db_'

GEOGRAPHIES = (
    GEO_CT,
    GEO_PR,
    GEO_CD,
    GEO_CSD,
    GEO_CMA,
    GEO_DA,
    GEO_FSA,
    GEO_DB,
)

SIMPLIFIED_GEOGRAPHIES = (
    GEO_FSA,
    GEO_DA,
)

GEOGRAPHY_NAMES = {
    GEO_CT: 'Census tracts',
    GEO_PR: 'Canada, provinces and territories',
    GEO_CD: 'Census divisions',
    GEO_CSD: 'Census subdivisions',
    GEO_CMA: 'Census metropolitan areas and census agglomerations',
    GEO_DA: 'Census dissemination areas',
    GEO_FSA: 'Forward sortation areas',
    GEO_DB: 'Census dissemination blocks',
}

GEOGRAPHY_DESCS = {
    GEO_CT: '',
    GEO_PR: '',
    GEO_CD: '',
    GEO_CSD: '',
    GEO_CMA: '',
    GEO_DA: '',
    GEO_FSA: '',
    GEO_DB: '',
}

GEOGRAPHY_PROPERNAMES = {
    GEO_CT: 'CTNAME',
    GEO_PR: 'PRNAME',
    GEO_CD: 'CDNAME',
    GEO_CSD: 'CSDNAME',
    GEO_CMA: 'CMANAME',
    GEO_DA: 'DAUID',  # DA has no proper name
    GEO_FSA: 'PRNAME',
    GEO_DB: 'DBUID',  # DB has no proper name
}

GEOGRAPHY_TAGS = {
    GEO_CT: ['cartographic_boundary'],
    GEO_PR: ['cartographic_boundary', 'interpolation_boundary'],
    GEO_CD: ['cartographic_boundary', 'interpolation_boundary'],
    GEO_CSD: ['cartographic_boundary', 'interpolation_boundary'],
    GEO_CMA: ['cartographic_boundary'],
    GEO_DA: ['cartographic_boundary', 'interpolation_boundary'],
    GEO_FSA: ['cartographic_boundary', 'interpolation_boundary'],
    GEO_DB: ['cartographic_boundary', 'interpolation_boundary'],
}

GEOGRAPHY_FORMAT = {
    GEO_CT: 'a',  # SHP
    GEO_PR: 'a',  # SHP
    GEO_CD: 'a',  # SHP
    GEO_CSD: 'a',  # SHP
    GEO_CMA: 'a',  # SHP
    GEO_DA: 'a',  # SHP
    GEO_FSA: 'g',  # GML
    GEO_DB: 'a',  # SHPL
}

GEOGRAPHY_EXTENSION = {
    'a': 'shp',
    'g': 'gml',
}

GEOGRAPHY_UID = {
    GEO_CT: 'ctuid',
    GEO_PR: 'pruid',
    GEO_CD: 'cduid',
    GEO_CSD: 'csduid',
    GEO_CMA: 'cmauid',
    GEO_DA: 'dauid',
    GEO_FSA: 'cfsauid',
    GEO_DB: 'dbuid',
}

YEAR_URL = {
    # http://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/bound-limit-2011-eng.cfm
    2011: 'http://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/g{resolution}000b11{format}_e.zip',
    # https://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/bound-limit-2016-eng.cfm
    2016: 'http://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/2016/l{resolution}000b16a_e.zip',
}


# 2011 Boundary Files
class DownloadGeography(RepoFileUnzipTask):

    resolution = Parameter(default=GEO_PR)
    year = IntParameter()

    def get_url(self):
        url = YEAR_URL[self.year]
        return url.format(resolution=self.resolution,
                          format=GEOGRAPHY_FORMAT[self.resolution])


class ImportGeography(GeoFile2TempTableTask):
    '''
    Import geographies into postgres by geography level
    '''

    resolution = Parameter(default=GEO_PR)
    year = IntParameter()
    extension = Parameter(default='shp', significant=False)

    def requires(self):
        return DownloadGeography(resolution=self.resolution, year=self.year)

    def input_files(self):
        cmd = 'ls {input}/*.{extension}'.format(
            input=self.input().path,
            extension=self.extension
        )
        for geofile in shell(cmd).strip().split('\n'):
            yield geofile


class SimplifiedImportGeography(SimplifiedTempTableTask):
    resolution = Parameter(default=GEO_PR)
    year = IntParameter()

    def requires(self):
        extension = GEOGRAPHY_EXTENSION[GEOGRAPHY_FORMAT[self.resolution]] if self.year == 2011 else 'shp'
        return ImportGeography(resolution=self.resolution, year=self.year, extension=extension)


# Task for additional simplifications
class SimplifiedGeography(SimplifiedTempTableTask):
    resolution = Parameter()
    # Suffix will be appended to table id and output table
    suffix = Parameter()
    year = IntParameter()

    def get_table_id(self):
        return "ca.statcan.geo.geography_{resolution}__{year}{suffix}".format(
            resolution=self.resolution,
            year=self.year,
            suffix=self.suffix
        )

    def get_suffix(self):
        return self.suffix

    def requires(self):
        return Geography(resolution=self.resolution, year=self.year)


class GeographyColumns(ColumnsTask):

    resolution = Parameter(default=GEO_PR)
    year = IntParameter()

    weights = {
        GEO_PR: 1,
        GEO_CD: 2,
        GEO_CMA: 3,
        GEO_CSD: 4,
        GEO_CT: 5,
        GEO_DA: 6,
        GEO_FSA: 7,
        GEO_DB: 8,
    }

    def version(self):
        return 14

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
            id='{resolution}_{year}'.format(resolution=self.resolution, year=self.year),
            type='Geometry',
            name=GEOGRAPHY_NAMES[self.resolution],
            description=GEOGRAPHY_DESCS[self.resolution],
            weight=self.weights[self.resolution],
            tags=[sections['ca'], subsections['boundary'], data_license, data_source],
        )
        geom_id = OBSColumn(
            id='{resolution}_id_{year}'.format(resolution=self.resolution, year=self.year),
            type='Text',
            weight=0,
            targets={geom: GEOM_REF},
        )
        geom_name = OBSColumn(
            id='{resolution}_name_{year}'.format(resolution=self.resolution, year=self.year),
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

    resolution = Parameter(default=GEO_PR)
    year = IntParameter()

    def version(self):
        return 10

    def requires(self):
        return {
            'data': SimplifiedImportGeography(resolution=self.resolution, year=self.year),
            'columns': GeographyColumns(resolution=self.resolution, year=self.year)
        }

    def table_timespan(self):
        return get_timespan(self.year)

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
    year = IntParameter()

    def requires(self):
        return [Geography(resolution=resolution, year=self.year)
                for resolution in GEOGRAPHIES]


class AllSimplifiedGeographies(WrapperTask):
    year = IntParameter()

    def requires(self):
        return [SimplifiedGeography(resolution=resolution,
                                    year=self.year,
                                    suffix='_oversimpl')
                for resolution in SIMPLIFIED_GEOGRAPHIES]


class AllGeographyColumns(WrapperTask):
    year = IntParameter()

    def requires(self):
        for resolution in GEOGRAPHIES:
            yield GeographyColumns(resolution=resolution, year=self.year)
