from luigi import Parameter, WrapperTask

from tasks.util import (DownloadUnzipTask, shell, Shp2TempTableTask,
                        ColumnsTask, TableTask)
from tasks.meta import GEOM_REF, OBSColumn, current_session
from tasks.tags import SectionTags, SubsectionTags
from abc import ABCMeta
from collections import OrderedDict


GEO_STE = 'STE'
GEO_SA4 = 'SA4'
GEO_SA3 = 'SA3'
GEO_SA2 = 'SA2'
GEO_SA1 = 'SA1'
GEO_GCCSA = 'GCCSA'
GEO_LGA = 'LGA'
GEO_SLA = 'SLA'
GEO_SSC = 'SSC'
GEO_POA = 'POA'
GEO_CED = 'CED'
GEO_SED = 'SED'
GEO_SOS = 'SOS'
GEO_SOSR = 'SOSR'
GEO_UCL = 'UCL'
GEO_SUA = 'SUA'
GEO_RA = 'RA'

GEOGRAPHIES = (
    GEO_STE,
    GEO_SA4,
    GEO_SA3,
    GEO_SA2,
    GEO_SA1,
    GEO_GCCSA,
    GEO_LGA,
    GEO_SLA,
    GEO_SSC,
    GEO_POA,
    GEO_CED,
    GEO_SED,
    GEO_SOS,
    GEO_SOSR,
    GEO_UCL,
    GEO_SUA,
    GEO_RA
)


GEOGRAPHY_NAMES = {
    GEO_STE: 'State/Territory',
    GEO_SA4: 'Statistical Area Level 4',
    GEO_SA3: 'Statistical Area Level 3',
    GEO_SA2: 'Statistical Area Level 2',
    GEO_SA1: 'Statistical Area Level 1',
    GEO_GCCSA: 'Greater Capital City Statistical Areas',
    GEO_LGA: 'Local Government Areas',
    GEO_SLA: 'Statistical Local Areas',
    GEO_SSC: 'State Suburbs',
    GEO_POA: 'Postal Areas',
    GEO_CED: 'Commonwealth Electoral Divisions',
    GEO_SED: 'State Electoral Divisions',
    GEO_SOS: 'Section of State',
    GEO_SOSR: 'Section of State Ranges',
    GEO_UCL: 'Urban Centres and Localities',
    GEO_SUA: 'Significant Urban Areas',
    GEO_RA: 'Remoteness Areas',
}

class BaseParams:
    __metaclass__ = ABCMeta

    year = Parameter(default='2011')
    resolution = Parameter(default=GEO_STE)


class DownloadGeography(BaseParams, DownloadUnzipTask):

    URL = 'http://www.censusdata.abs.gov.au/CensusOutput/copsubdatapacks.nsf/All%20docs%20by%20catNo/Boundaries_{year}_{resolution}/\$File/{year}_{resolution}_shape.zip'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(
            output=self.output().path, url=self.URL.format(resolution=self.resolution, year=self.year)
        ))


class ImportGeography(BaseParams, Shp2TempTableTask):
    '''
    Import geographies into postgres by geography level
    '''

    def requires(self):
        return DownloadGeography(resolution=self.resolution, year=self.year)

    def input_shp(self):
        cmd = 'ls {input}/*.shp'.format(
            input=self.input().path
        )
        for shp in shell(cmd).strip().split('\n'):
            yield shp


class GeographyColumns(BaseParams, ColumnsTask):

    weights = {
        GEO_STE: 17,
        GEO_SA4: 16,
        GEO_SA3: 15,
        GEO_SA2: 14,
        GEO_SA1: 13,
        GEO_GCCSA: 12,
        GEO_LGA: 11,
        GEO_SLA: 10,
        GEO_SSC: 9,
        GEO_POA: 8,
        GEO_CED: 7,
        GEO_SED: 6,
        GEO_SOS: 5,
        GEO_SOSR: 4,
        GEO_UCL: 3,
        GEO_SUA: 2,
        GEO_RA: 1
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
            description='',
            weight=self.weights[self.resolution],
            tags=[sections['au'], subsections['boundary']],
        )
        geom_id = OBSColumn(
            type='Text',
            id=self.resolution + '_id',
            weight=0,
            targets={geom: GEOM_REF},
        )
        return OrderedDict([
            ('geom_id', geom_id),   # cvegeo
            ('the_geom', geom),     # the_geom
        ])


class Geography(BaseParams, TableTask):

    def version(self):
        return 1

    def requires(self):
        return {
            'data': ImportGeography(resolution=self.resolution, year=self.year),
            'columns': GeographyColumns(resolution=self.resolution)
        }

    def timespan(self):
        return self.year

    def columns(self):
        return self.input()['columns']

    def populate(self):
        session = current_session()
        session.execute('INSERT INTO {output} '
                        'SELECT ogc_fid as geom_id, '
                        '       wkb_geometry as the_geom '
                        'FROM {input} '.format(
                            output=self.output().table,
                            code=self.resolution.replace('_', ''),
                            input=self.input()['data'].table))


class AllGeographies(WrapperTask):

    def requires(self):
        for resolution in GEOGRAPHIES:
            yield Geography(resolution=resolution)
