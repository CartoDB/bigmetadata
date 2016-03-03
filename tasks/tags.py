from tasks.meta import BMDTag
from tasks.util import TagsTask


class Tags(TagsTask):

    def tags(self):
        return [
            BMDTag(id='denominator',
                   name='Denominator',
                   description='Use these to provide a baseline for comparison between different areas.'),
            BMDTag(id='population',
                   name='Population',
                   description=''),
            BMDTag(id='housing',
                   name='Housing',
                   description='What type of housing exists and how do people live in it?'),
            BMDTag(id='income_education_employment',
                   name='Income, Education and Employment',
                   description=''),
            BMDTag(id='language',
                   name="Language",
                   description='What languages do people speak?'),
            BMDTag(id='race_age_gender',
                   name='Race, Age and Gender',
                   description='Basic demographic breakdowns.'),
            BMDTag(id='transportation',
                   name='Transportation',
                   description='How do people move from place to place?'),
            BMDTag(id='boundary',
                   name='Boundaries',
                   description='Use these to provide regions for sound comparison and analysis.'),
        ]
        
