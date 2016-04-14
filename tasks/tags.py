from tasks.meta import OBSTag
from tasks.util import TagsTask


class CategoryTags(TagsTask):

    def version(self):
        return 1

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
                   description=''),
            OBSTag(id='language',
                   name="Language",
                   type='category',
                   description='What languages do people speak?'),
            OBSTag(id='race_age_gender',
                   name='Race, Age and Gender',
                   type='category',
                   description='Basic demographic breakdowns.'),
            OBSTag(id='transportation',
                   name='Transportation',
                   type='category',
                   description='How do people move from place to place?'),
            OBSTag(id='boundary',
                   name='Boundaries',
                   type='category',
                   description='Use these to provide regions for sound comparison and analysis.'),
        ]
