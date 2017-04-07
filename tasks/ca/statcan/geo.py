from luigi import Parameter, WrapperTask

from tasks.util import (DownloadUnzipTask, shell, Shp2TempTableTask,
                        ColumnsTask, TableTask)
from tasks.meta import GEOM_REF, OBSColumn, current_session
from tasks.tags import SectionTags, SubsectionTags, BoundaryTags

from collections import OrderedDict


GEO_CT = 'ct_'
GEO_PR = 'pr_'
GEO_CD = 'cd_'
GEO_CSD = 'csd'
GEO_CMA = 'cma'

GEOGRAPHIES = (
    GEO_CT,
    GEO_PR,
    GEO_CD,
    GEO_CSD,
    GEO_CMA,
)


GEOGRAPHY_NAMES = {
    GEO_CT: 'Census Tracts',
    GEO_PR: 'Canada, provinces and territories',
    GEO_CD: 'Census divisions',
    GEO_CSD: 'Census subdivisions',
    GEO_CMA: 'Census metropolitan areas and census agglomerations',
}

GEOGRAPHY_DESCS = {
    GEO_CT: '',
    GEO_PR: '',
    GEO_CD: '',
    GEO_CSD: '',
    GEO_CMA: '',
}

GEOGRAPHY_CODES = {
    GEO_CT: 401,
    GEO_PR: 101,
    GEO_CD: 701,
    GEO_CSD: 301,
    GEO_CMA: 201,
}

GEOGRAPHY_TAGS = {
    GEO_CT: ['cartographic_boundary', 'interpolation_boundary'],
    GEO_PR: ['cartographic_boundary', 'interpolation_boundary'],
    GEO_CD: ['cartographic_boundary', 'interpolation_boundary'],
    GEO_CSD: ['cartographic_boundary', 'interpolation_boundary'],
    GEO_CMA: [],
}



# http://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/bound-limit-2011-eng.cfm
# 2011 Boundary Files
class DownloadGeography(DownloadUnzipTask):

    resolution = Parameter(default=GEO_PR)

    URL = 'http://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/g{resolution}000b11a_e.zip'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(
            output=self.output().path, url=self.URL.format(resolution=self.resolution)
        ))


class ImportGeography(Shp2TempTableTask):
    '''
    Import geographies into postgres by geography level
    '''

    resolution = Parameter(default=GEO_PR)

    def requires(self):
        return DownloadGeography(resolution=self.resolution)

    def input_shp(self):
        cmd = 'ls {input}/*.shp'.format(
            input=self.input().path
        )
        for shp in shell(cmd).strip().split('\n'):
            yield shp


class GeographyColumns(ColumnsTask):

    resolution = Parameter(default=GEO_PR)

    weights = {
        GEO_CT: 5,
        GEO_PR: 4,
        GEO_CD: 3,
        GEO_CSD: 2,
        GEO_CMA: 1,
    }

    def version(self):
        return 8

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'boundary': BoundaryTags()
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
            tags=[sections['ca'], subsections['boundary']],
        )
        geom_id = OBSColumn(
            type='Text',
            weight=0,
            targets={geom: GEOM_REF},
        )
        geom.tags.extend(boundary_type[i] for i in GEOGRAPHY_TAGS[self.resolution])
        return OrderedDict([
            ('geom_id', geom_id),   # cvegeo
            ('the_geom', geom),     # the_geom
        ])


class Geography(TableTask):
    '''
    '''

    resolution = Parameter(default=GEO_PR)

    def version(self):
        return 2

    def requires(self):
        return {
            'data': ImportGeography(resolution=self.resolution),
            'columns': GeographyColumns(resolution=self.resolution)
        }

    def timespan(self):
        return 2011

    def columns(self):
        return self.input()['columns']

    def populate(self):
        session = current_session()
        session.execute('INSERT INTO {output} '
                        'SELECT {code}uid as geom_id, '
                        '       wkb_geometry as geom '
                        'FROM {input} '.format(
                            output=self.output().table,
                            code=self.resolution.replace('_', ''),
                            input=self.input()['data'].table))


class AllGeographies(WrapperTask):

    def requires(self):
        for resolution in GEOGRAPHIES:
            yield Geography(resolution=resolution)
