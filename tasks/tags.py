from tasks.meta import OBSTag
from tasks.util import TagsTask


class LicenseTags(TagsTask):

    def version(self):
        return 1

    def tags(self):
        return [
            OBSTag(id='cc-by-4',
                   name='CC-BY 4.0',
                   description=''),
        ]


class UnitTags(TagsTask):

    def version(self):
        return 3

    def tags(self):
        return [
            OBSTag(id='index',
                   name='Index',
                   type='unit',
                   description=''),
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
            OBSTag(id='telephones',
                   name='Telephones',
                   type='unit',
                   description=''),
            OBSTag(id='vehicles',
                   name='Vehicles',
                   type='unit',
                   description=''),
            OBSTag(id='businesses',
                   name='Businesses',
                   type='unit',
                   description=''),
        ]


class SectionTags(TagsTask):

    def version(self):
        return 2

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
            OBSTag(id='uk',
                   name='United Kingdom',
                   type='section',
                   description=''),
        ]


class SubsectionTags(TagsTask):

    def version(self):
        return 17

    def tags(self):
        return [
            OBSTag(id='housing',
                   name='Housing',
                   type='subsection',
                   description='What type of housing exists and how do people live in it?'),
            OBSTag(id='income',
                   name='Income',
                   type='subsection',
                   description='''How much people earn.'''),
            OBSTag(id='education',
                   name='Education',
                   type='subsection',
                   description='Educational attainment and enrollment.'),
            OBSTag(id='employment',
                   name='Employment',
                   type='subsection',
                   description='How people are employed.'),
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
                   description='Population breakdowns by age and gender.'),
            OBSTag(id='nationality',
                   name='Nationality',
                   type='subsection',
                   description='''Population breakdowns by nationality and place of birth.''',),
            OBSTag(id='race_ethnicity',
                   name='Race and Ethnicity',
                   type='subsection',
                   description='''Population breakdowns by race and ethnicity. '''),
            OBSTag(id='transportation',
                   name='Transportation',
                   type='subsection',
                   description='How do people move from place to place?'),
            OBSTag(id='boundary',
                   name='Boundaries',
                   type='subsection',
                   description='Political, administrative, and census-based boundaries.'),
            OBSTag(id='religion',
                   name='Religion',
                   type='subsection',
                   description='Breakdowns of the population by religion.'),
            OBSTag(id='health',
                   name='Health',
                   type='subsection',
                   description='Breakdowns of the population by health'),
            OBSTag(id='commerce_economy',
                   name='Commerce & Economy',
                   type='subsection',
                   description='Broader measures of economic and commercial activity.'),
        ]
