from luigi import Parameter, WrapperTask

from tasks.util import (DownloadUnzipTask, shell, Shp2TempTableTask,
                        ColumnsTask, TableTask, TagsTask)
from tasks.meta import GEOM_REF, GEOM_NAME, OBSColumn, current_session, OBSTag
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
    GEO_STE: {'name': 'State/Territory', 'weight': 17, 'region_col': 'STATE_CODE', 'proper_name': 'STATE_NAME'},
    GEO_SA4: {'name': 'Statistical Area Level 4', 'weight': 16, 'region_col': 'SA4_CODE', 'proper_name': 'SA4_NAME'},
    GEO_SA3: {'name': 'Statistical Area Level 3', 'weight': 15, 'region_col': 'SA3_CODE', 'proper_name': 'SA3_NAME'},
    GEO_SA2: {'name': 'Statistical Area Level 2', 'weight': 14, 'region_col': 'SA2_MAIN', 'proper_name': 'SA2_NAME'},
    GEO_SA1: {'name': 'Statistical Area Level 1', 'weight': 13, 'region_col': 'SA1_7DIGIT', 'proper_name': 'STATE_NAME'},
    GEO_GCCSA: {'name': 'Greater Capital City Statistical Areas', 'weight': 12, 'region_col': 'GCCSA_CODE', 'proper_name': 'GCCSA_NAME'},
    GEO_LGA: {'name': 'Local Government Areas', 'weight': 11, 'region_col': 'LGA_CODE', 'proper_name': 'LGA_NAME'},
    GEO_SLA: {'name': 'Statistical Local Areas', 'weight': 10, 'region_col': 'SLA_MAIN', 'proper_name': 'SLA_NAME'},
    GEO_SSC: {'name': 'State Suburbs', 'weight': 9, 'region_col': 'SSC_CODE', 'proper_name': 'SSC_NAME'},
    GEO_POA: {'name': 'Postal Areas', 'weight': 8, 'region_col': 'POA_CODE', 'proper_name': 'POA_NAME'},
    GEO_CED: {'name': 'Commonwealth Electoral Divisions', 'weight': 7, 'region_col': 'CED_CODE', 'proper_name': 'CED_NAME'},
    GEO_SED: {'name': 'State Electoral Divisions', 'weight': 6, 'region_col': 'SED_CODE', 'proper_name': 'SED_NAME'},
    GEO_SOS: {'name': 'Section of State', 'weight': 5, 'region_col': 'SOS_CODE', 'proper_name': 'SOS_NAME'},
    GEO_SOSR: {'name': 'Section of State Ranges', 'weight': 4, 'region_col': 'SOSR_CODE', 'proper_name': 'SOSR_NAME'},
    GEO_UCL: {'name': 'Urban Centres and Localities', 'weight': 3, 'region_col': 'UCL_CODE', 'proper_name': 'UCL_NAME'},
    GEO_SUA: {'name': 'Significant Urban Areas', 'weight': 2, 'region_col': 'SUA_CODE', 'proper_name': 'SUA_NAME'},
    GEO_RA: {'name': 'Remoteness Areas', 'weight': 1, 'region_col': 'RA_CODE', 'proper_name': 'RA_NAME'},
}

class SourceTags(TagsTask):

    def tags(self):
        return [
            OBSTag(id='au-census',
                   name='Australian Bureau of Statistics (ABS)',
                   type='source',
                   description=u'The `Australian Bureau of Statistics <http://abs.gov.au/websitedbs/censushome.nsf/home/datapacks>`')
        ]


class LicenseTags(TagsTask):

    def tags(self):
        return [
            OBSTag(id='au-datapacks-license',
                   name='Creative Commons Attribution 2.5 Australia licence',
                   type='license',
                   description=u'DataPacks is licenced under a `Creative Commons Attribution 2.5 Australia licence <https://creativecommons.org/licenses/by/2.5/au/>`_')
        ]


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
            'source': SourceTags(),
            'license': LicenseTags(),
            'boundary': BoundaryTags(),
        }

    def columns(self):
        input_ = self.input()
        sections = input_['sections']
        subsections = input_['subsections']
        source = input_['source']['au-census']
        license = input_['license']['au-datapacks-license']
        boundary_type = input_['boundary']

        geom = OBSColumn(
            id=self.resolution,
            type='Geometry',
            name=GEOGRAPHY[self.resolution]['name'],
            description='',
            weight=GEOGRAPHY[self.resolution]['weight'],
            tags=[source, license, sections['au'], subsections['boundary']],
        )
        geom_id = OBSColumn(
            type='Text',
            id=self.resolution + '_id',
            weight=0,
            targets={geom: GEOM_REF},
        )
        geom_name = OBSColumn(
            type='Text',
            name= 'Proper name of {}'.format(GEOGRAPHY[self.resolution]['name']),
            id=self.resolution + '_name',
            description='',
            weight=1,
            targets={geom: GEOM_NAME},
            tags=[source, license, sections['au'], subsections['names']],
        )


        cartographic_boundaries = [GEO_LGA, GEO_POA, GEO_CED, GEO_SED, GEO_SSC,
                                   GEO_SA1, GEO_SA2, GEO_SA3, GEO_SA4,
                                   GEO_STE, GEO_GCCSA, GEO_UCL,
                                   GEO_SOS, GEO_SOSR, GEO_SUA, GEO_RA]
        interpolated_boundaries = [GEO_SA1, GEO_SA2, GEO_SA3, GEO_SA4,
                                   GEO_STE, GEO_GCCSA, GEO_UCL,
                                   GEO_SOS, GEO_SOSR, GEO_SUA, GEO_RA]


        cols = OrderedDict([
            ('geom_name', geom_name),
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
        return 3

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
                        'SELECT {geom_name} as geom_name, '
                        '       {region_col} as geom_id, '
                        '       wkb_geometry as the_geom '
                        'FROM {input} '.format(
                            geom_name=GEOGRAPHY[self.resolution]['proper_name'],
                            region_col=GEOGRAPHY[self.resolution]['region_col'],
                            output=self.output().table,
                            input=self.input()['data'].table))


class AllGeographies(WrapperTask):

    year = Parameter()

    def requires(self):
        for resolution in GEOGRAPHIES:
            yield Geography(resolution=resolution, year=self.year)
