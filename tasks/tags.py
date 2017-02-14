from tasks.meta import OBSTag
from tasks.util import TagsTask


class LicenseTags(TagsTask):

    def version(self):
        return 3

    def tags(self):
        return [
            OBSTag(id='cc-by-4',
                   name='CC-BY 4.0',
                   type='license',
                   description=''),
            OBSTag(id='no-restrictions',
                   name='Unrestricted',
                   type='license',
                   description='You may do whatever you want with this information'
                  ),
            OBSTag(id='uk_ogl',
                    name='UK Open Government Licence (OGL)',
                    type='license',
                    description='The Licensor grants you a worldwide, royalty-free, perpetual, non-exclusive licence to use the Information subject to the conditions `here <http://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/>`_.'),
            OBSTag(id='ine_property',
                    name='National Statistics Institute (INE) Property',
                    type='license',
                    description='http://www.ine.es/ss/Satellite?L=1&c=Page&cid=1254735849170&p=1254735849170&pagename=Ayuda%2FINELayout#'),
            OBSTag(id='zillow',
                    name='Zillow Group Data',
                    type='license',
                    description='http://www.zillow.com/research/data/. All data accessed and downloaded from this page is free for public use by consumers, media, analysts, academics etc., consistent with our published Terms of Use. Proper and clear attribution of all data to Zillow is required.'),
            OBSTag(id='eurostat_copyright',
                   name='Eurostat Copyright',
                   type='license',
                   description='''
            Eurostat has a policy of encouraging free re-use of its data, both for
            non-commercial and commercial purposes. All statistical
            data, metadata, content of web pages or other dissemination
            tools, official publications and other documents published
            on its website, with the exceptions listed below, can be
            reused without any payment or written licence provided that:
            the source is indicated as Eurostat; when re-use involves
            modifications to the data or text, this must be stated
            clearly to the end user of the information. `Full copyright
            notice
            <http://ec.europa.eu/eurostat/about/our-partners/copyright>`_.''')
        ]


class UnitTags(TagsTask):

    def version(self):
        return 10

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
            OBSTag(id='minutes',
                   name='Minutes',
                   type='unit',
                   description=''),
            OBSTag(id='days',
                   name='Days',
                   type='unit',
                   description=''),
           OBSTag(id='nights',
                  name='Nights',
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
            OBSTag(id='education_level',
                   name='Level of education',
                   type='unit',
                   description=''),
            OBSTag(id='segmentation',
                   name='Segmentation',
                   type='unit',
                   description=''),
            OBSTag(id='rooms',
                   name='Rooms',
                   type='unit',
                   description=''),
            OBSTag(id='beds',
                   name='Beds',
                   type='unit',
                   description=''),
            OBSTag(id='family_unit',
                   name='Families',
                   type='unit',
                   description=''),
            OBSTag(id='hours',
                   name='Hours',
                   type='unit',
                   description=''),
            OBSTag(id='crimes',
                   name='Crimes',
                   type='unit',
                   description=''),
            OBSTag(id='tonnes',
                   name='Metric Tonnes',
                   type='unit',
                   description=''),
            OBSTag(id='kilometres',
                   name='Kilometres',
                   type='unit',
                   description=''),
            OBSTag(id='inches',
                   name='Inches',
                   type='unit',
                   description=''),
            OBSTag(id='km2',
                   name='Kilometers squared',
                   type='unit',
                   description=''),
        ]


class SectionTags(TagsTask):

    def version(self):
        return 5

    def tags(self):
        return [
            OBSTag(id='global',
                   name='Global',
                   type='section',
                   description=''),
            OBSTag(id='eu',
                   name='European Union',
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
            OBSTag(id='mx',
                   name='Mexico',
                   type='section',
                   description=''),
            OBSTag(id='uk',
                   name='United Kingdom',
                   type='section',
                   description=''),
            OBSTag(id='fr',
                   name='France',
                   type='section',
                   description=''),
            OBSTag(id='ca',
                   name='Canada',
                   type='section',
                   description=''),
            OBSTag(id='th',
                   name='Thailand',
                   type='section',
                   description=''),
            OBSTag(id='fr',
                   name='France',
                   type='section',
                   description=''),
            OBSTag(id='br',
                   name='Brazil',
                   type='section',
                   description='')
        ]


class SubsectionTags(TagsTask):

    def version(self):
        return 21

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
            OBSTag(id='migration',
                   name='Migration',
                   type='subsection',
                   description='Patterns of migration.'),
            OBSTag(id='health',
                   name='Health',
                   type='subsection',
                   description='Breakdowns of the population by health.'),
            OBSTag(id='commerce_economy',
                   name='Commerce & Economy',
                   type='subsection',
                   description='Broader measures of economic and commercial activity.'),
            OBSTag(id='business_demography',
                   name='Business Demography',
                   type='subsection',
                   description = 'Measures of enterprise growth and decline.'),
            OBSTag(id='segments',
                   name='Population segments',
                   type='subsection',
                   description='Segmentations of the population.'),
            OBSTag(id='society',
                   name='Society',
                   type='subsection',
                   description='Measures of society quality of life'),
            OBSTag(id='energy',
                   name='Energy',
                   type='subsection',
                   description='Measures related to energy production and consumption.'),
            OBSTag(id='environmental',
                   name='Environmental',
                   type='subsection',
                   description='Attributes of the natural environment'),
            OBSTag(id='roads',
                   name='Roads',
                   type='subsection',
                   description=''),
            OBSTag(id='poi',
                   name='Points of Interest',
                   type='subsection',
                   description=''),
        ]

class BoundaryTags(TagsTask):
    def version(self):
        return 1

    def tags(self):
        return [
            OBSTag(id='interpolation_boundary',
                   name='Interpolation Boundary',
                   type='boundary_type',
                   description='Boundary appropriate for interpolation calculation'),
            OBSTag(id='cartographic_boundary',
                   name='Cartographic Boundary',
                   type='boundary_type',
                   description='Boundary appropriate for cartographic use'),
            ]
