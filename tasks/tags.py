from tasks.meta import OBSTag
from tasks.util import TagsTask


class CategoryTags(TagsTask):

    def version(self):
        return 7

    def tags(self):
        return [
            OBSTag(id='denominator',
                   name='Denominator',
                   type='category',
                   description='Use these to provide a baseline for comparison between different areas.'),
            OBSTag(id='population',
                   name='Population',
                   type='category',
                   description=''),
            OBSTag(id='housing',
                   name='Housing',
                   type='category',
                   description='What type of housing exists and how do people live in it?'),
            OBSTag(id='income_education_employment',
                   name='Income, Education and Employment',
                   type='category',
                   description='''
How much money people earn, what jobs they hold or are available in different
areas, and the educational attainment or current enrollment of the population.

.. figure:: ../../_static/img/GenerateStaticImage_5a2f4cc8_e189_11_125dd21a55.png
  :scale: 100 %
  :alt: Median Household Income
  :align: center
  :target: https://observatory.cartodb.com/viz/5a2f4cc8-e189-11e5-8327-0e5db1731f59/embed_map

  Median household income in the United States according to the American
  Community Survey, 5-year estimate from 2013.

'''
                  ),
            OBSTag(id='language',
                   name="Language",
                   type='category',
                   description='What languages do people speak?'),
            OBSTag(id='race_age_gender',
                   name='Race, Age and Gender',
                   type='category',
                   description='''Basic demographic breakdowns.

.. figure:: ../../_static/img/GenerateStaticImage_4def78b4_f6c6_11_9e6824668c.png
  :scale: 100 %
  :alt: Median Household Income
  :align: center
  :target: https://observatory.cartodb.com/viz/4def78b4-f6c6-11e5-ac8d-0e31c9be1b51/embed_map

  Percent of the population which is white in every United States county.

'''),
            OBSTag(id='transportation',
                   name='Transportation',
                   type='category',
                   description='How do people move from place to place?'),
            OBSTag(id='boundary',
                   name='Boundaries',
                   type='category',
                   description='Use these to provide regions for sound comparison and analysis.'),
        ]
