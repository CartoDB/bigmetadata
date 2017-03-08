# https://data.cdrc.ac.uk/dataset/cdrc-2011-oac-geodata-pack-uk

from luigi import Task, Parameter, LocalTarget

from tasks.util import (TableTask, TagsTask, ColumnsTask, classpath, shell,
                        DownloadUnzipTask, Shp2TempTableTask, MetaWrapper)
from tasks.meta import GEOM_REF, OBSColumn, OBSTag, current_session
from tasks.tags import SectionTags, SubsectionTags, UnitTags, LicenseTags, BoundaryTags

from collections import OrderedDict
import os


class OpenDemographicsLicenseTags(TagsTask):

    def tags(self):
        return [OBSTag(id='opengeodemographics-license',
                       name='Open Geodemographics license',
                       type='license',
                       description='Free to download and reuse, even for '
                                   'commercial sector applications.  More '
                                   'information `here <http://www.opengeodemographics.com/index.php#why-section>`_'
                      )]


class SourceTags(TagsTask):

    def version(self):
        return 1

    def tags(self):
        return[
            OBSTag(id='cdrc-source',
                    name= 'Consumer Data Research Centre',
                    type='source',
                    description='The 2011 Area Classification for Output Areas (2011 OAC) is a UK geodemographic classification produced as a collaboration between the Office for National Statistics and University College London. For further information regarding the 2011 OAC please visit: http://www.ons.gov.uk/ons/guide-method/geography/products/area-classifications/ns-area-classifications/ns-2011-area-classifications/index.html or http://www.opengeodemographics.com. CDRC 2011 OAC Geodata Pack by the ESRC Consumer Data Research Centre; Contains National Statistics data Crown copyright and database right 2015; Contains Ordnance Survey data Crown copyright and database right 2015')
                    ]

class DownloadOutputAreas(DownloadUnzipTask):

    URL = 'https://data.cdrc.ac.uk/dataset/68771b14-72aa-4ad7-99f3-0b8d1124cb1b/resource/8fff55da-6235-459c-b66d-017577b060d3/download/output-area-classification.zip'

    def download(self):
        shell('wget --header=\'Cookie: auth_tkt="96a4778a0e3366127d4a47cf19a9c7d65751e5a9talos!userid_type:unicode"; auth_tkt="96a4778a0e3366127d4a47cf19a9c7d65751e5a9talos!userid_type:unicode";\' -O {output}.zip {url}'.format(
            output=self.output().path,
            url=self.URL))


class ImportOutputAreas(Shp2TempTableTask):

    def requires(self):
        return DownloadOutputAreas()

    def input_shp(self):
        return os.path.join(self.input().path, 'Output Area Classification',
                            'Shapefiles', '2011_OAC.shp')


class OutputAreaColumns(ColumnsTask):

    def version(self):
        return 4

    def requires(self):
        return {
            'subsections': SubsectionTags(),
            'sections': SectionTags(),
            'source': SourceTags(),
            'license': LicenseTags(),
            'boundary': BoundaryTags(),
        }

    def columns(self):
        input_ = self.input()
        license = input_['license']['uk_ogl']
        source = input_['source']['cdrc-source']
        boundary_type = input_['boundary']
        geom = OBSColumn(
            type='Geometry',
            name='Census Output Areas',
            description='The smallest unit for which census data are published '
                        'in the UK.  They contain at least 40 households and '
                        '100 persons, the target size being 125 households. '
                        'They were built up from postcode blocks after the '
                        'census data were available, with the intention of '
                        'standardising population sizes, geographical shape '
                        'and social homogeneity (in terms of dwelling types '
                        'and housing tenure). The OAs generated in 2001 were '
                        'retained as far as possible for the publication of '
                        'outputs from the 2011 Census (less than 3% were '
                        'changed). -`Wikipedia <https://en.wikipedia.org/'
                        'wiki/ONS_coding_system#Geography_of_the_UK_Census>`_',
            weight=8,
            tags=[input_['subsections']['boundary'], input_['sections']['uk'], source, license,
                  boundary_type['cartographic_boundary'], boundary_type['interpolation_boundary']]
        )
        geomref = OBSColumn(
            type='Text',
            name='Census Output Area ID',
            weight=0,
            targets={geom: GEOM_REF}
        )

        return OrderedDict([
            ('the_geom', geom),
            ('oa_sa', geomref)
        ])

class OutputAreas(TableTask):

    def requires(self):
        return {
            'geom_columns': OutputAreaColumns(),
            'data': ImportOutputAreas(),
        }

    def version(self):
        return 7

    def timespan(self):
        return 2011

    def columns(self):
        input_ = self.input()
        cols = OrderedDict()
        cols.update(input_['geom_columns'])
        return cols

    def populate(self):
        session = current_session()
        session.execute('INSERT INTO {output} '
                        'SELECT ST_MakeValid(wkb_geometry), oa_sa '
                        'FROM {input}'.format(
                            output=self.output().table,
                            input=self.input()['data'].table,
                        ))


class OutputAreaClassificationColumns(ColumnsTask):

    sprgrp_mapping = OrderedDict([
        ('Rural Residents', '1'),
        ('Cosmopolitans', '2'),
        ('Ethnicity Central', '3'),
        ('Multicultural Metropolitans', '4'),
        ('Urbanites', '5'),
        ('Suburbanites', '6'),
        ('Constrained City Dwellers', '7'),
        ('Hard-Pressed Living', '8'),
    ])

    grp_mapping = OrderedDict([
        ('Farming Communities', '1a'),
        ('Rural Tenants', '1b'),
        ('Ageing Rural Dwellers', '1c'),
        ('Students Around Campus', '2a'),
        ('Inner-City Students', '2b'),
        ('Comfortable Cosmopolitans', '2c'),
        ('Aspiring and Affluent', '2d'),
        ('Ethnic Family Life', '3a'),
        ('Endeavouring Ethnic Mix', '3b'),
        ('Ethnic Dynamics', '3c'),
        ('Aspirational Techies', '3d'),
        ('Rented Family Living', '4a'),
        ('Challenged Asian Terraces', '4b'),
        ('Asian Traits', '4c'),
        ('Urban Professionals and Families', '5a'),
        ('Ageing Urban Living', '5b'),
        ('Suburban Achievers', '6a'),
        ('Semi-Detached Suburbia', '6b'),
        ('Challenged Diversity', '7a'),
        ('Constrained Flat Dwellers', '7b'),
        ('White Communities', '7c'),
        ('Ageing City Dwellers', '7d'),
        ('Industrious Communities', '8a'),
        ('Challenged Terraced Workers', '8b'),
        ('Hard-Pressed Ageing Workers', '8c'),
        ('Migration and Churn', '8d'),
    ])

    subgrp_mapping = OrderedDict([
        ('Rural Workers and Families', '1a1'),
        ('Established Farming Communities', '1a2'),
        ('Agricultural Communities', '1a3'),
        ('Older Farming Communities', '1a4'),
        ('Rural Life', '1b1'),
        ('Rural White-Collar Workers', '1b2'),
        ('Ageing Rural Flat Tenants', '1b3'),
        ('Rural Employment and Retirees', '1c1'),
        ('Renting Rural Retirement', '1c2'),
        ('Detached Rural Retirement', '1c3'),
        ('Student Communal Living', '2a1'),
        ('Student Digs', '2a2'),
        ('Students and Professionals', '2a3'),
        ('Students and Commuters', '2b1'),
        ('Multicultural Student Neighbourhoods', '2b2'),
        ('Migrant Families', '2c1'),
        ('Migrant Commuters', '2c2'),
        ('Professional Service Cosmopolitans', '2c3'),
        ('Urban Cultural Mix', '2d1'),
        ('Highly-Qualified Quaternary Workers', '2d2'),
        ('EU White-Collar Workers', '2d3'),
        ('Established Renting Families', '3a1'),
        ('Young Families and Students', '3a2'),
        ('Striving Service Workers', '3b1'),
        ('Bangladeshi Mixed Employment', '3b2'),
        ('Multi-Ethnic Professional Service Workers', '3b3'),
        ('Constrained Neighbourhoods', '3c1'),
        ('Constrained Commuters', '3c2'),
        ('New EU Tech Workers', '3d1'),
        ('Established Tech Workers', '3d2'),
        ('Old EU Tech Workers', '3d3'),
        ('Social Renting Young Families', '4a1'),
        ('Private Renting New Arrivals', '4a2'),
        ('Commuters with Young Families', '4a3'),
        ('Asian Terraces and Flats', '4b1'),
        ('Pakistani Communities', '4b2'),
        ('Achieving Minorities', '4c1'),
        ('Multicultural New Arrivals', '4c2'),
        ('Inner City Ethnic Mix', '4c3'),
        ('White Professionals', '5a1'),
        ('Multi-Ethnic Professionals with Families', '5a2'),
        ('Families in Terraces and Flats', '5a3'),
        ('Delayed Retirement', '5b1'),
        ('Communal Retirement', '5b2'),
        ('Self-Sufficient Retirement', '5b3'),
        ('Indian Tech Achievers', '6a1'),
        ('Comfortable Suburbia', '6a2'),
        ('Detached Retirement Living', '6a3'),
        ('Ageing in Suburbia', '6a4'),
        ('Multi-Ethnic Suburbia', '6b1'),
        ('White Suburban Communities', '6b2'),
        ('Semi-Detached Ageing', '6b3'),
        ('Older Workers and Retirement', '6b4'),
        ('Transitional Eastern European Neighbourhoods', '7a1'),
        ('Hampered Aspiration', '7a2'),
        ('Multi-Ethnic Hardship', '7a3'),
        ('Eastern European Communities', '7b1'),
        ('Deprived Neighbourhoods', '7b2'),
        ('Endeavouring Flat Dwellers', '7b3'),
        ('Challenged Transitionaries', '7c1'),
        ('Constrained Young Families', '7c2'),
        ('Outer City Hardship', '7c3'),
        ('Ageing Communities and Families', '7d1'),
        ('Retired Independent City Dwellers', '7d2'),
        ('Retired Communal City Dwellers', '7d3'),
        ('Retired City Hardship', '7d4'),
        ('Industrious Transitions', '8a1'),
        ('Industrious Hardship', '8a2'),
        ('Deprived Blue-Collar Terraces', '8b1'),
        ('Hard-Pressed Rented Terraces', '8b2'),
        ('Ageing Industrious Workers', '8c1'),
        ('Ageing Rural Industry Workers', '8c2'),
        ('Renting Hard-Pressed Workers', '8c3'),
        ('Young Hard-Pressed Families', '8d1'),
        ('Hard-Pressed Ethnic Mix', '8d2'),
        ('Hard-Pressed European Settlers', '8d3'),
    ])

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'units': UnitTags(),
            'license': OpenDemographicsLicenseTags(),
            'source': SourceTags(),
        }

    def version(self):
        return 4

    def columns(self):
        input_ = self.input()
        uk = input_['sections']['uk']
        segments = input_['subsections']['segments']
        gen_cats = lambda d: OrderedDict([
            (catname, {'description': '', 'details': {}}) for catname in d.keys()
        ])
        segmentation = input_['units']['segmentation']
        license = input_['license']['opengeodemographics-license']
        source = input_['source']['cdrc-source']
        return OrderedDict([
            ('sprgrp', OBSColumn(
                type='Text',
                weight=1,
                name='Supergroup Area Classification',
                description='The 2011 Area Classification for Output Areas '
                '(2011 OAC) is a UK geodemographic classification produced as '
                'a collaboration between the Office for National Statistics and '
                'University College London. Visit `here '
                '<http://www.ons.gov.uk/ons/guide-method/geography/products/'
                'area-classifications/ns-area-classifications/ns-2011-area-'
                'classifications/index.html>`_ or `here '
                '<http://www.opengeodemographics.com>`_ for further '
                'information regarding the 2011 OAC. ',
                extra={'categories': gen_cats(self.sprgrp_mapping)},
                tags=[uk, segments, segmentation, license, source],
            )),
            ('grp', OBSColumn(
                type='Text',
                weight=3,
                name='Group Area Classification',
                description='The 2011 Area Classification for Output Areas '
                '(2011 OAC) is a UK geodemographic classification produced as '
                'a collaboration between the Office for National Statistics and '
                'University College London. Visit `here '
                '<http://www.ons.gov.uk/ons/guide-method/geography/products/'
                'area-classifications/ns-area-classifications/ns-2011-area-'
                'classifications/index.html>`_ or `here '
                '<http://www.opengeodemographics.com>`_ for further '
                'information regarding the 2011 OAC. ',
                extra={'categories': gen_cats(self.grp_mapping)},
                tags=[uk, segments, segmentation, license, source],
            )),
            ('subgrp', OBSColumn(
                type='Text',
                weight=5,
                name='Subgroup Area Classification',
                description='The 2011 Area Classification for Output Areas '
                '(2011 OAC) is a UK geodemographic classification produced as '
                'a collaboration between the Office for National Statistics and '
                'University College London. Visit `here '
                '<http://www.ons.gov.uk/ons/guide-method/geography/products/'
                'area-classifications/ns-area-classifications/ns-2011-area-'
                'classifications/index.html>`_ or `here '
                '<http://www.opengeodemographics.com>`_ for further '
                'information regarding the 2011 OAC. ',
                extra={'categories': gen_cats(self.subgrp_mapping)},
                tags=[uk, segments, segmentation, license, source],
            )),
        ])


class OutputAreaClassifications(TableTask):

    def timespan(self):
        return 2011

    def requires(self):
        return {
            'geom_columns': OutputAreaColumns(),
            'segment_columns': OutputAreaClassificationColumns(),
            'data': ImportOutputAreas(),
        }

    def columns(self):
        input_ = self.input()
        cols = OrderedDict()
        cols['oa_sa'] = input_['geom_columns']['oa_sa']
        cols.update(input_['segment_columns'])
        return cols

    def populate(self):
        session = current_session()
        oacc = OutputAreaClassificationColumns
        sprgrp_case = ' '.join([
            "WHEN '{}' THEN '{}'".format(c[1], c[0]) for c in oacc.sprgrp_mapping.items()
        ])
        grp_case = ' '.join([
            "WHEN '{}' THEN '{}'".format(c[1], c[0]) for c in oacc.grp_mapping.items()
        ])
        subgrp_case = ' '.join([
            "WHEN '{}' THEN '{}'".format(c[1], c[0]) for c in oacc.subgrp_mapping.items()
        ])
        session.execute('INSERT INTO {output} '
                        'SELECT oa_sa, '
                        'CASE sprgrp {sprgrp_case} END sprgrp, '
                        'CASE grp {grp_case} END grp, '
                        'CASE subgrp {subgrp_case} END subgrp '
                        'FROM {input}'.format(
                            output=self.output().table,
                            input=self.input()['data'].table,
                            sprgrp_case=sprgrp_case,
                            grp_case=grp_case,
                            subgrp_case=subgrp_case,
                        ))

class CDRCMetaWrapper(MetaWrapper):
    def tables(self):
        yield OutputAreaClassifications()
        yield OutputAreas()
