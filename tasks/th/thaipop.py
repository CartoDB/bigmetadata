'''
Bigmetadata tasks
tasks to download and create metadata
'''

from tasks.tags import SubsectionTags, SectionTags
from tasks.meta import (GEOM_REF, current_session, GEOM_NAME, OBSColumn)
from tasks.util import ColumnsTask, TableTask, Carto2TempTableTask
from collections import OrderedDict

class ImportThai(Carto2TempTableTask):

    subdomain = 'solutions'
    table = 'thai_districts'

class ThaiColumns(ColumnsTask):

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
        }

    def version(self):
        return 1

    def columns(self):
        inputs = self.input()
        age_gender = inputs['subsections']['age_gender']
        boundaries = inputs['subsections']['boundary']
        thailand = inputs['sections']['global']

        the_geom = OBSColumn(
            name='District',
            description='Districts in Thailand, also known as amphoes, are'
            'adminstrative regions analogous to counties that make up the provinces.'
            'There are 878 amphoes in Thailand and'
            '50 urban districts of Bangkok known as khets.',
            type='Geometry',
            weight=5,
            tags=[thailand, boundaries],
        )
        pop = OBSColumn(
            name='Population in 2010',
            type='Numeric',
            aggregate='sum',
            weight=5,
            tags=[thailand, age_gender],
        )
        name = OBSColumn(
            name='Name of District',
            type='Text',
            weight=5,
            targets={the_geom: GEOM_NAME},
        )

        return OrderedDict([
            ('the_geom', the_geom),
            ('pop', pop),
            ('name', name),
        ])

class ThaiDistricts(TableTask):

    def requires(self):
        return {
            'meta': ThaiColumns(),
            'data': ImportThai(),
        }

    def version(self):
        return 2

    def timespan(self):
        return '2010'

    def columns(self):
        return self.input()['meta']

    def populate(self):
        session = current_session()
        session.execute(' INSERT INTO {output} '
                        ' SELECT the_geom, pop2010, name_2 '
                        ' FROM {input} '.format(
                            output=self.output().table,
                            input=self.input()['data'].table
                        ))
