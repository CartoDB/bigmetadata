from tasks.meta import BMDTag


class Tags():
    denominator = BMDTag(
        id='denominator',
        name='Denominator',
        description='Use these to provide a baseline for comparison between different areas.')
    population = BMDTag(
        id='population',
        name='Population',
        description='')
    housing = BMDTag(
        id='housing',
        name='Housing',
        description='What type of housing exists and how do people live in it?')
    income_education_employment = BMDTag(
        id='income_education_employment',
        name='Income, Education and Employment',
        description='')
    language = BMDTag(
        id='language',
        name="Language",
        description='What languages do people speak?')
    race_age_gender = BMDTag(
        id='race_age_gender',
        name='Race, Age and Gender',
        description='Basic demographic breakdowns.')
    transportation = BMDTag(
        id='transportation',
        name='Transportation',
        description='How do people move from place to place?')
    boundary = BMDTag(
        id='boundary',
        name='Boundaries',
        description='Use these to provide regions for sound comparison and analysis.')
