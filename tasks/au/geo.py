from luigi import Parameter, WrapperTask

from tasks.util import (DownloadUnzipTask, shell, Shp2TempTableTask,
                        ColumnsTask, TableTask)
from tasks.meta import GEOM_REF, OBSColumn, current_session
from tasks.tags import SectionTags, SubsectionTags, BoundaryTags
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


GEOGRAPHY = {
    GEO_STE: {'name': 'State/Territory', 'weight': 17, 'region_col': 'STATE_CODE'},
    GEO_SA4: {'name': 'Statistical Area Level 4', 'weight': 16, 'region_col': 'SA4_CODE'},
    GEO_SA3: {'name': 'Statistical Area Level 3', 'weight': 15, 'region_col': 'SA3_CODE'},
    GEO_SA2: {'name': 'Statistical Area Level 2', 'weight': 14, 'region_col': 'SA2_MAIN'},
    GEO_SA1: {'name': 'Statistical Area Level 1', 'weight': 13, 'region_col': 'SA1_7DIGIT'},
    GEO_GCCSA: {'name': 'Greater Capital City Statistical Areas', 'weight': 12, 'region_col': 'GCCSA_CODE'},
    GEO_LGA: {'name': 'Local Government Areas', 'weight': 11, 'region_col': 'LGA_CODE'},
    GEO_SLA: {'name': 'Statistical Local Areas', 'weight': 10, 'region_col': 'SLA_MAIN'},
    GEO_SSC: {'name': 'State Suburbs', 'weight': 9, 'region_col': 'SSC_CODE'},
    GEO_POA: {'name': 'Postal Areas', 'weight': 8, 'region_col': 'POA_CODE'},
    GEO_CED: {'name': 'Commonwealth Electoral Divisions', 'weight': 7, 'region_col': 'CED_CODE'},
    GEO_SED: {'name': 'State Electoral Divisions', 'weight': 6, 'region_col': 'SED_CODE'},
    GEO_SOS: {'name': 'Section of State', 'weight': 5, 'region_col': 'SOS_CODE'},
    GEO_SOSR: {'name': 'Section of State Ranges', 'weight': 4, 'region_col': 'SOSR_CODE'},
    GEO_UCL: {'name': 'Urban Centres and Localities', 'weight': 3, 'region_col': 'UCL_CODE'},
    GEO_SUA: {'name': 'Significant Urban Areas', 'weight': 2, 'region_col': 'SUA_CODE'},
    GEO_RA: {'name': 'Remoteness Areas', 'weight': 1, 'region_col': 'RA_CODE'},
}


class DownloadGeography(DownloadUnzipTask):

    year = Parameter()
    resolution = Parameter()

    URL = 'http://www.censusdata.abs.gov.au/CensusOutput/copsubdatapacks.nsf/All%20docs%20by%20catNo/Boundaries_{year}_{resolution}/\$File/{year}_{resolution}_shape.zip'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(
            output=self.output().path, url=self.URL.format(resolution=self.resolution, year=self.year)
        ))


class ImportGeography(Shp2TempTableTask):
    '''
    Import geographies into postgres by geography level
    '''

    year = Parameter()
    resolution = Parameter()

    def requires(self):
        return DownloadGeography(resolution=self.resolution, year=self.year)

    def input_shp(self):
        cmd = 'ls {input}/*.shp'.format(
            input=self.input().path
        )
        for shp in shell(cmd).strip().split('\n'):
            yield shp


class GeographyColumns(ColumnsTask):

    resolution = Parameter()

    def version(self):
        return 2

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
            name=GEOGRAPHY[self.resolution]['name'],
            description='',
            weight=GEOGRAPHY[self.resolution]['weight'],
            tags=[sections['au'], subsections['boundary']],
        )
        geom_id = OBSColumn(
            type='Text',
            id=self.resolution + '_id',
            weight=0,
            targets={geom: GEOM_REF},
        )

        cartographic_boundaries = [GEO_LGA, GEO_POA, GEO_CED, GEO_SED, GEO_SSC,
                                   GEO_SA1, GEO_SA2, GEO_SA3, GEO_SA4,
                                   GEO_STE, GEO_GCCSA, GEO_UCL,
                                   GEO_SOS, GEO_SOSR, GEO_SUA, GEO_RA]
        interpolated_boundaries = [GEO_SA1, GEO_SA2, GEO_SA3, GEO_SA4,
                                   GEO_STE, GEO_GCCSA, GEO_UCL,
                                   GEO_SOS, GEO_SOSR, GEO_SUA, GEO_RA]


        cols =  OrderedDict([
            ('geom_id', geom_id),
            ('the_geom', geom),
        ])

        for colname, col in cols.iteritems():
            if col.id in interpolated_boundaries:
                col.tags.append(boundary_type['interpolation_boundary'])
            if col.id in cartographic_boundaries:
                col.tags.append(boundary_type['cartographic_boundary'])
        return cols

class Geography(TableTask):

    year = Parameter()
    resolution = Parameter()

    def version(self):
        return 2

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
                        'SELECT {region_col} as geom_id, '
                        '       wkb_geometry as the_geom '
                        'FROM {input} '.format(
                            region_col=GEOGRAPHY[self.resolution]['region_col'],
                            output=self.output().table,
                            input=self.input()['data'].table))


class AllGeographies(WrapperTask):

    year = Parameter()

    def requires(self):
        for resolution in GEOGRAPHIES:
            yield Geography(resolution=resolution, year=self.year)
