from tasks.meta import OBSTag
from tasks.util import TagsTask


class UnitTags(TagsTask):

    def version(self):
        return 1

    def tags(self):
        return [
            OBSTag(id='people',
                   name='People',
                   type='unit',
                   description=''),
            OBSTag(id='households',
                   name='Households',
                   type='unit',
                   description=''),
            OBSTag(id='housing_units',
                   name='Housing Units',
                   type='unit',
                   description=''),
            OBSTag(id='money',
                   name='Money',
                   type='unit',
                   description=''),
            OBSTag(id='ratio',
                   name='Ratio',
                   type='unit',
                   description=''),
            OBSTag(id='years',
                   name='Years',
                   type='unit',
                   description=''),
        ]


class SectionTags(TagsTask):

    def version(self):
        return 1

    def tags(self):
        return [
            OBSTag(id='global',
                   name='Global',
                   type='section',
                   description=''),
            OBSTag(id='united_states',
                   name='United States',
                   type='section',
                   description=''),
            OBSTag(id='spain',
                   name='Spain',
                   type='section',
                   description=''),
        ]


class SubsectionTags(TagsTask):

    def version(self):
        return 15

    def tags(self):
        return [
            OBSTag(id='housing',
                   name='Housing',
                   type='subsection',
                   description='What type of housing exists and how do people live in it?'),
            OBSTag(id='income',
                   name='Income',
                   type='subsection',
                   description='''
How much people earn.

.. cartofigure:: 5a2f4cc8-e189-11e5-8327-0e5db1731f59
  :width: 100 %
  :alt: Median Household Income
  :align: center
  :target: https://observatory.cartodb.com/viz/5a2f4cc8-e189-11e5-8327-0e5db1731f59/embed_map

  Median household income in the United States according to the American
  Community Survey, 5-year estimate from 2014.

'''),
            OBSTag(id='education',
                   name='Education',
                   type='subsection',
                   description='Educational attainment and enrollment.'
                  ),
            OBSTag(id='employment',
                   name='Employment',
                   type='subsection',
                   description='How people are employed.'
                  ),
            OBSTag(id='families',
                   name="Families",
                   type='subsection',
                   description='Familial arrangements of people and households.'),
            OBSTag(id='language',
                   name="Language",
                   type='subsection',
                   description='What languages people speak.'),
            OBSTag(id='age_gender',
                   name='Age and Gender',
                   type='subsection',
                   description='Population breakdowns by age and gender.'
                  ),
            OBSTag(id='nationality',
                   name='Nationality',
                   type='subsection',
                   description='''Population breakdowns by nationality and place of birth.''',
                  ),
            OBSTag(id='race_ethnicity',
                   name='Race and Ethnicity',
                   type='subsection',
                   description='''Population breakdowns by race and ethnicity.

.. cartofigure:: 4def78b4-f6c6-11e5-ac8d-0e31c9be1b51
  :width: 100 %
  :alt: Median Household Income
  :align: center
  :target: https://observatory.cartodb.com/viz/4def78b4-f6c6-11e5-ac8d-0e31c9be1b51/embed_map

  Percent of the population which is white in every United States county.

'''),
            OBSTag(id='transportation',
                   name='Transportation',
                   type='subsection',
                   description='How do people move from place to place?'),
            OBSTag(id='boundary',
                   name='Boundaries',
                   type='subsection',
                   description='Political, administrative, and census-based boundaries.'),
        ]
