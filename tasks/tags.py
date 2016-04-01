from tasks.meta import BMDTag
from tasks.util import TagsTask


class Tags(TagsTask):

    def tags(self):
        return [
            BMDTag(id='denominator',
                   name='Denominator',
                   type='catalog',
                   description='Use these to provide a baseline for comparison between different areas.'),
            BMDTag(id='population',
                   name='Population',
                   type='catalog',
                   description=''),
            BMDTag(id='housing',
                   name='Housing',
                   type='catalog',
                   description='What type of housing exists and how do people live in it?'),
            BMDTag(id='income_education_employment',
                   name='Income, Education and Employment',
                   type='catalog',
                   description=''),
            BMDTag(id='language',
                   name="Language",
                   type='catalog',
                   description='What languages do people speak?'),
            BMDTag(id='race_age_gender',
                   name='Race, Age and Gender',
                   type='catalog',
                   description='Basic demographic breakdowns.'),
            BMDTag(id='transportation',
                   name='Transportation',
                   type='catalog',
                   description='How do people move from place to place?'),
            BMDTag(id='boundary',
                   name='Boundaries',
                   type='catalog',
                   description='Use these to provide regions for sound comparison and analysis.'),
        ]
