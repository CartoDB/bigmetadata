from tasks.meta import OBSColumn, DENOMINATOR
from tasks.util import ColumnsTask
from tasks.tags import SectionTags, SubsectionTags, UnitTags

from collections import OrderedDict

class NHSColumns(ColumnsTask):

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'unittags': UnitTags(),
        }

    def version(self):
        return 3

    def columns(self):
        input_ = self.input()

        subsections = input_['subsections']

        unit_people = input_['unittags']['people']
        unit_housing = input_['unittags']['housing_units']
        unit_household = input_['unittags']['households']
        unit_years = input_['unittags']['years']
        unit_ratio = input_['unittags']['ratio']
        unit_education = input_['unittags']['education_level']

        ca = input_['sections']['ca']

        t001c001 = OBSColumn(
            id='t001c001',
            name='Total population in private households by Aboriginal identity',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={},)

        t001c002 = OBSColumn(
            id='t001c002',
            name='Aboriginal identity',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t001c001: DENOMINATOR },)

        t001c003 = OBSColumn(
            id='t001c003',
            name='First Nations (North American Indian) single identity',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t001c002: DENOMINATOR },)

        t001c004 = OBSColumn(
            id='t001c004',
            name='M tis single identity',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t001c002: DENOMINATOR },)

        t001c005 = OBSColumn(
            id='t001c005',
            name='Inuk (Inuit) single identity',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t001c002: DENOMINATOR },)

        t001c006 = OBSColumn(
            id='t001c006',
            name='Multiple Aboriginal identities',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t001c002: DENOMINATOR },)

        t001c007 = OBSColumn(
            id='t001c007',
            name='Aboriginal identities not included elsewhere',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t001c002: DENOMINATOR },)

        t001c008 = OBSColumn(
            id='t001c008',
            name='Non-Aboriginal identity',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t001c001: DENOMINATOR },)

        t001c009 = OBSColumn(
            id='t001c009',
            name='Total population in private households by Registered or Treaty Indian status',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={},)

        t001c010 = OBSColumn(
            id='t001c010',
            name='Registered or Treaty Indian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t001c009: DENOMINATOR },)

        t001c011 = OBSColumn(
            id='t001c011',
            name='Not a Registered or Treaty Indian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t001c009: DENOMINATOR },)

        t001c012 = OBSColumn(
            id='t001c012',
            name='Total population in private households by Aboriginal ancestry',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={},)

        t001c013 = OBSColumn(
            id='t001c013',
            name='Aboriginal ancestry',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t001c012: DENOMINATOR },)

        t001c014 = OBSColumn(
            id='t001c014',
            name='First Nations (North American Indian) Aboriginal ancestry',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t001c013: DENOMINATOR },)

        t001c015 = OBSColumn(
            id='t001c015',
            name='M tis ancestry',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t001c013: DENOMINATOR },)

        t001c016 = OBSColumn(
            id='t001c016',
            name='Inuit ancestry',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t001c013: DENOMINATOR },)

        t001c017 = OBSColumn(
            id='t001c017',
            name='Non-Aboriginal ancestry only',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t001c012: DENOMINATOR },)

        t002c001 = OBSColumn(
            id='t002c001',
            name='Total immigrant population in private households by age at immigration',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={},)

        t002c002 = OBSColumn(
            id='t002c002',
            name='Under 5 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t002c001: DENOMINATOR },)

        t002c003 = OBSColumn(
            id='t002c003',
            name='5 to 14 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t002c001: DENOMINATOR },)

        t002c004 = OBSColumn(
            id='t002c004',
            name='15 to 24 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t002c001: DENOMINATOR },)

        t002c005 = OBSColumn(
            id='t002c005',
            name='25 to 44 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t002c001: DENOMINATOR },)

        t002c006 = OBSColumn(
            id='t002c006',
            name='45 years and over',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t002c001: DENOMINATOR },)

        t003c001 = OBSColumn(
            id='t003c001',
            name='Total population in private households by citizenship',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={},)

        t003c002 = OBSColumn(
            id='t003c002',
            name='Canadian citizens',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t003c001: DENOMINATOR },)

        t003c003 = OBSColumn(
            id='t003c003',
            name='Canadian citizens aged under 18',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t003c002: DENOMINATOR },)

        t003c004 = OBSColumn(
            id='t003c004',
            name='Canadian citizens aged 18 and over',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t003c002: DENOMINATOR },)

        t003c005 = OBSColumn(
            id='t003c005',
            name='Not Canadian citizens',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t003c001: DENOMINATOR },)

        t004c001 = OBSColumn(
            id='t004c001',
            name='Total labour force aged 15 years and over by class of worker',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={},)

        t004c002 = OBSColumn(
            id='t004c002',
            name='Class of worker - not applicable',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t004c001: DENOMINATOR },)

        t004c003 = OBSColumn(
            id='t004c003',
            name='All classes of worker',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t004c001: DENOMINATOR },)

        t004c004 = OBSColumn(
            id='t004c004',
            name='Employee',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t004c003: DENOMINATOR },)

        t004c005 = OBSColumn(
            id='t004c005',
            name='Self-employed',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t004c003: DENOMINATOR },)

        t005c001 = OBSColumn(
            id='t005c001',
            name='Total population aged 15 years and over by highest certificate, diploma or degree',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={},)

        t005c002 = OBSColumn(
            id='t005c002',
            name='No certificate, diploma or degree',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c001: DENOMINATOR },)

        t005c003 = OBSColumn(
            id='t005c003',
            name='High school diploma or equivalent',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c001: DENOMINATOR },)

        t005c004 = OBSColumn(
            id='t005c004',
            name='Postsecondary certificate, diploma or degree',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c001: DENOMINATOR },)

        t005c005 = OBSColumn(
            id='t005c005',
            name='Apprenticeship or trades certificate or diploma',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c004: DENOMINATOR },)

        t005c006 = OBSColumn(
            id='t005c006',
            name='College, CEGEP or other non-university certificate or diploma',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c004: DENOMINATOR },)

        t005c007 = OBSColumn(
            id='t005c007',
            name='University certificate or diploma below bachelor level',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c004: DENOMINATOR },)

        t005c008 = OBSColumn(
            id='t005c008',
            name='University certificate, diploma or degree at bachelor level or above',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c004: DENOMINATOR },)

        t005c009 = OBSColumn(
            id='t005c009',
            name='Bachelor\'s degree',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c008: DENOMINATOR },)

        t005c010 = OBSColumn(
            id='t005c010',
            name='University certificate, diploma or degree above bachelor level',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c008: DENOMINATOR },)

        t005c011 = OBSColumn(
            id='t005c011',
            name='Total population aged 25 to 64 years by highest certificate, diploma or degree',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={},)

        t005c012 = OBSColumn(
            id='t005c012',
            name='No certificate, diploma or degree',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c011: DENOMINATOR },)

        t005c013 = OBSColumn(
            id='t005c013',
            name='High school diploma or equivalent',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c011: DENOMINATOR },)

        t005c014 = OBSColumn(
            id='t005c014',
            name='Postsecondary certificate, diploma or degree',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c011: DENOMINATOR },)

        t005c015 = OBSColumn(
            id='t005c015',
            name='Apprenticeship or trades certificate or diploma',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c014: DENOMINATOR },)

        t005c016 = OBSColumn(
            id='t005c016',
            name='College, CEGEP or other non-university certificate or diploma',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c014: DENOMINATOR },)

        t005c017 = OBSColumn(
            id='t005c017',
            name='University certificate or diploma below bachelor level',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c014: DENOMINATOR },)

        t005c018 = OBSColumn(
            id='t005c018',
            name='University certificate, diploma or degree at bachelor level or above',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c014: DENOMINATOR },)

        t005c019 = OBSColumn(
            id='t005c019',
            name='Bachelor\'s degree',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c018: DENOMINATOR },)

        t005c020 = OBSColumn(
            id='t005c020',
            name='University certificate, diploma or degree above bachelor level',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c018: DENOMINATOR },)

        t005c021 = OBSColumn(
            id='t005c021',
            name='Total population aged 15 years and over by major field of study - Classification of Instructional Programs (CIP) 2011',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={},)

        t005c022 = OBSColumn(
            id='t005c022',
            name='No postsecondary certificate, diploma or degree',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c021: DENOMINATOR },)

        t005c023 = OBSColumn(
            id='t005c023',
            name='Education',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c021: DENOMINATOR },)

        t005c024 = OBSColumn(
            id='t005c024',
            name='Visual and performing arts, and communications technologies',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c021: DENOMINATOR },)

        t005c025 = OBSColumn(
            id='t005c025',
            name='Humanities',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c021: DENOMINATOR },)

        t005c026 = OBSColumn(
            id='t005c026',
            name='Social and behavioural sciences and law',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c021: DENOMINATOR },)

        t005c027 = OBSColumn(
            id='t005c027',
            name='Business, management and public administration',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c021: DENOMINATOR },)

        t005c028 = OBSColumn(
            id='t005c028',
            name='Physical and life sciences and technologies',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c021: DENOMINATOR },)

        t005c029 = OBSColumn(
            id='t005c029',
            name='Mathematics, computer and information sciences',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c021: DENOMINATOR },)

        t005c030 = OBSColumn(
            id='t005c030',
            name='Architecture, engineering, and related technologies',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c021: DENOMINATOR },)

        t005c031 = OBSColumn(
            id='t005c031',
            name='Agriculture, natural resources and conservation',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c021: DENOMINATOR },)

        t005c032 = OBSColumn(
            id='t005c032',
            name='Health and related fields',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c021: DENOMINATOR },)

        t005c033 = OBSColumn(
            id='t005c033',
            name='Personal, protective and transportation services',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c021: DENOMINATOR },)

        t005c034 = OBSColumn(
            id='t005c034',
            name='Other fields of study',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c021: DENOMINATOR },)

        t005c035 = OBSColumn(
            id='t005c035',
            name='Total population aged 15 years and over by location of study compared with province or territory of residence',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={},)

        t005c036 = OBSColumn(
            id='t005c036',
            name='No postsecondary certificate, diploma or degree',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c035: DENOMINATOR },)

        t005c037 = OBSColumn(
            id='t005c037',
            name='With postsecondary certificate, diploma or degree',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c035: DENOMINATOR },)

        t005c038 = OBSColumn(
            id='t005c038',
            name='Location of study inside Canada',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c037: DENOMINATOR },)

        t005c039 = OBSColumn(
            id='t005c039',
            name='Same as province or territory of residence',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c038: DENOMINATOR },)

        t005c040 = OBSColumn(
            id='t005c040',
            name='Another province or territory',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c038: DENOMINATOR },)

        t005c041 = OBSColumn(
            id='t005c041',
            name='Location of study outside Canada',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t005c037: DENOMINATOR },)

        t006c001 = OBSColumn(
            id='t006c001',
            name='Total population in private households by ethnic origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={},)

        t006c002 = OBSColumn(
            id='t006c002',
            name='North American Aboriginal origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c001: DENOMINATOR },)

        t006c003 = OBSColumn(
            id='t006c003',
            name='First Nations (North American Indian)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c002: DENOMINATOR },)

        t006c004 = OBSColumn(
            id='t006c004',
            name='Inuit',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c002: DENOMINATOR },)

        t006c005 = OBSColumn(
            id='t006c005',
            name='M tis',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c002: DENOMINATOR },)

        t006c006 = OBSColumn(
            id='t006c006',
            name='Other North American origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c001: DENOMINATOR },)

        t006c007 = OBSColumn(
            id='t006c007',
            name='Acadian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c006: DENOMINATOR },)

        t006c008 = OBSColumn(
            id='t006c008',
            name='American',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c006: DENOMINATOR },)

        t006c009 = OBSColumn(
            id='t006c009',
            name='Canadian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c006: DENOMINATOR },)

        t006c010 = OBSColumn(
            id='t006c010',
            name='New Brunswicker',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c006: DENOMINATOR },)

        t006c011 = OBSColumn(
            id='t006c011',
            name='Newfoundlander',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c006: DENOMINATOR },)

        t006c012 = OBSColumn(
            id='t006c012',
            name='Nova Scotian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c006: DENOMINATOR },)

        t006c013 = OBSColumn(
            id='t006c013',
            name='Ontarian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c006: DENOMINATOR },)

        t006c014 = OBSColumn(
            id='t006c014',
            name='Qu b cois',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c006: DENOMINATOR },)

        t006c015 = OBSColumn(
            id='t006c015',
            name='Other North American origins, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c006: DENOMINATOR },)

        t006c016 = OBSColumn(
            id='t006c016',
            name='European origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c001: DENOMINATOR },)

        t006c017 = OBSColumn(
            id='t006c017',
            name='British Isles origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c016: DENOMINATOR },)

        t006c018 = OBSColumn(
            id='t006c018',
            name='Channel Islander',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c017: DENOMINATOR },)

        t006c019 = OBSColumn(
            id='t006c019',
            name='Cornish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c017: DENOMINATOR },)

        t006c020 = OBSColumn(
            id='t006c020',
            name='English',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c017: DENOMINATOR },)

        t006c021 = OBSColumn(
            id='t006c021',
            name='Irish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c017: DENOMINATOR },)

        t006c022 = OBSColumn(
            id='t006c022',
            name='Manx',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c017: DENOMINATOR },)

        t006c023 = OBSColumn(
            id='t006c023',
            name='Scottish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c017: DENOMINATOR },)

        t006c024 = OBSColumn(
            id='t006c024',
            name='Welsh',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c017: DENOMINATOR },)

        t006c025 = OBSColumn(
            id='t006c025',
            name='British Isles origins, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c017: DENOMINATOR },)

        t006c026 = OBSColumn(
            id='t006c026',
            name='French origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c016: DENOMINATOR },)

        t006c027 = OBSColumn(
            id='t006c027',
            name='Alsatian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c026: DENOMINATOR },)

        t006c028 = OBSColumn(
            id='t006c028',
            name='Breton',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c026: DENOMINATOR },)

        t006c029 = OBSColumn(
            id='t006c029',
            name='French',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c026: DENOMINATOR },)

        t006c030 = OBSColumn(
            id='t006c030',
            name='Western European origins (except French origins)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c016: DENOMINATOR },)

        t006c031 = OBSColumn(
            id='t006c031',
            name='Austrian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c030: DENOMINATOR },)

        t006c032 = OBSColumn(
            id='t006c032',
            name='Belgian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c030: DENOMINATOR },)

        t006c033 = OBSColumn(
            id='t006c033',
            name='Dutch',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c030: DENOMINATOR },)

        t006c034 = OBSColumn(
            id='t006c034',
            name='Flemish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c030: DENOMINATOR },)

        t006c035 = OBSColumn(
            id='t006c035',
            name='Frisian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c030: DENOMINATOR },)

        t006c036 = OBSColumn(
            id='t006c036',
            name='German',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c030: DENOMINATOR },)

        t006c037 = OBSColumn(
            id='t006c037',
            name='Luxembourger',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c030: DENOMINATOR },)

        t006c038 = OBSColumn(
            id='t006c038',
            name='Swiss',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c030: DENOMINATOR },)

        t006c039 = OBSColumn(
            id='t006c039',
            name='Western European origins, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c030: DENOMINATOR },)

        t006c040 = OBSColumn(
            id='t006c040',
            name='Northern European origins (except British Isles origins)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c016: DENOMINATOR },)

        t006c041 = OBSColumn(
            id='t006c041',
            name='Danish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c040: DENOMINATOR },)

        t006c042 = OBSColumn(
            id='t006c042',
            name='Finnish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c040: DENOMINATOR },)

        t006c043 = OBSColumn(
            id='t006c043',
            name='Icelandic',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c040: DENOMINATOR },)

        t006c044 = OBSColumn(
            id='t006c044',
            name='Norwegian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c040: DENOMINATOR },)

        t006c045 = OBSColumn(
            id='t006c045',
            name='Swedish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c040: DENOMINATOR },)

        t006c046 = OBSColumn(
            id='t006c046',
            name='Northern European origins, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c040: DENOMINATOR },)

        t006c047 = OBSColumn(
            id='t006c047',
            name='Eastern European origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c016: DENOMINATOR },)

        t006c048 = OBSColumn(
            id='t006c048',
            name='Bulgarian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c047: DENOMINATOR },)

        t006c049 = OBSColumn(
            id='t006c049',
            name='Byelorussian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c047: DENOMINATOR },)

        t006c050 = OBSColumn(
            id='t006c050',
            name='Czech',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c047: DENOMINATOR },)

        t006c051 = OBSColumn(
            id='t006c051',
            name='Czechoslovakian, n.o.s.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c047: DENOMINATOR },)

        t006c052 = OBSColumn(
            id='t006c052',
            name='Estonian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c047: DENOMINATOR },)

        t006c053 = OBSColumn(
            id='t006c053',
            name='Hungarian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c047: DENOMINATOR },)

        t006c054 = OBSColumn(
            id='t006c054',
            name='Latvian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c047: DENOMINATOR },)

        t006c055 = OBSColumn(
            id='t006c055',
            name='Lithuanian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c047: DENOMINATOR },)

        t006c056 = OBSColumn(
            id='t006c056',
            name='Moldovan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c047: DENOMINATOR },)

        t006c057 = OBSColumn(
            id='t006c057',
            name='Polish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c047: DENOMINATOR },)

        t006c058 = OBSColumn(
            id='t006c058',
            name='Romanian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c047: DENOMINATOR },)

        t006c059 = OBSColumn(
            id='t006c059',
            name='Russian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c047: DENOMINATOR },)

        t006c060 = OBSColumn(
            id='t006c060',
            name='Slovak',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c047: DENOMINATOR },)

        t006c061 = OBSColumn(
            id='t006c061',
            name='Ukrainian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c047: DENOMINATOR },)

        t006c062 = OBSColumn(
            id='t006c062',
            name='Eastern European origins, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c047: DENOMINATOR },)

        t006c063 = OBSColumn(
            id='t006c063',
            name='Southern European origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c016: DENOMINATOR },)

        t006c064 = OBSColumn(
            id='t006c064',
            name='Albanian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c063: DENOMINATOR },)

        t006c065 = OBSColumn(
            id='t006c065',
            name='Bosnian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c063: DENOMINATOR },)

        t006c066 = OBSColumn(
            id='t006c066',
            name='Croatian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c063: DENOMINATOR },)

        t006c067 = OBSColumn(
            id='t006c067',
            name='Cypriot',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c063: DENOMINATOR },)

        t006c068 = OBSColumn(
            id='t006c068',
            name='Greek',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c063: DENOMINATOR },)

        t006c069 = OBSColumn(
            id='t006c069',
            name='Italian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c063: DENOMINATOR },)

        t006c070 = OBSColumn(
            id='t006c070',
            name='Kosovar',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c063: DENOMINATOR },)

        t006c071 = OBSColumn(
            id='t006c071',
            name='Macedonian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c063: DENOMINATOR },)

        t006c072 = OBSColumn(
            id='t006c072',
            name='Maltese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c063: DENOMINATOR },)

        t006c073 = OBSColumn(
            id='t006c073',
            name='Montenegrin',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c063: DENOMINATOR },)

        t006c074 = OBSColumn(
            id='t006c074',
            name='Portuguese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c063: DENOMINATOR },)

        t006c075 = OBSColumn(
            id='t006c075',
            name='Serbian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c063: DENOMINATOR },)

        t006c076 = OBSColumn(
            id='t006c076',
            name='Sicilian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c063: DENOMINATOR },)

        t006c077 = OBSColumn(
            id='t006c077',
            name='Slovenian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c063: DENOMINATOR },)

        t006c078 = OBSColumn(
            id='t006c078',
            name='Spanish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c063: DENOMINATOR },)

        t006c079 = OBSColumn(
            id='t006c079',
            name='Yugoslavian, n.o.s.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c063: DENOMINATOR },)

        t006c080 = OBSColumn(
            id='t006c080',
            name='Southern European origins, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c063: DENOMINATOR },)

        t006c081 = OBSColumn(
            id='t006c081',
            name='Other European origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c016: DENOMINATOR },)

        t006c082 = OBSColumn(
            id='t006c082',
            name='Basque',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c081: DENOMINATOR },)

        t006c083 = OBSColumn(
            id='t006c083',
            name='Jewish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c081: DENOMINATOR },)

        t006c084 = OBSColumn(
            id='t006c084',
            name='Roma (Gypsy)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c081: DENOMINATOR },)

        t006c085 = OBSColumn(
            id='t006c085',
            name='Slavic, n.o.s.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c081: DENOMINATOR },)

        t006c086 = OBSColumn(
            id='t006c086',
            name='Other European origins, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c081: DENOMINATOR },)

        t006c087 = OBSColumn(
            id='t006c087',
            name='Caribbean origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c001: DENOMINATOR },)

        t006c088 = OBSColumn(
            id='t006c088',
            name='Antiguan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c087: DENOMINATOR },)

        t006c089 = OBSColumn(
            id='t006c089',
            name='Bahamian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c087: DENOMINATOR },)

        t006c090 = OBSColumn(
            id='t006c090',
            name='Barbadian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c087: DENOMINATOR },)

        t006c091 = OBSColumn(
            id='t006c091',
            name='Bermudan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c087: DENOMINATOR },)

        t006c092 = OBSColumn(
            id='t006c092',
            name='Carib',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c087: DENOMINATOR },)

        t006c093 = OBSColumn(
            id='t006c093',
            name='Cuban',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c087: DENOMINATOR },)

        t006c094 = OBSColumn(
            id='t006c094',
            name='Dominican',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c087: DENOMINATOR },)

        t006c095 = OBSColumn(
            id='t006c095',
            name='Grenadian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c087: DENOMINATOR },)

        t006c096 = OBSColumn(
            id='t006c096',
            name='Haitian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c087: DENOMINATOR },)

        t006c097 = OBSColumn(
            id='t006c097',
            name='Jamaican',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c087: DENOMINATOR },)

        t006c098 = OBSColumn(
            id='t006c098',
            name='Kittitian/Nevisian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c087: DENOMINATOR },)

        t006c099 = OBSColumn(
            id='t006c099',
            name='Martinican',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c087: DENOMINATOR },)

        t006c100 = OBSColumn(
            id='t006c100',
            name='Montserratan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c087: DENOMINATOR },)

        t006c101 = OBSColumn(
            id='t006c101',
            name='Puerto Rican',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c087: DENOMINATOR },)

        t006c102 = OBSColumn(
            id='t006c102',
            name='St. Lucian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c087: DENOMINATOR },)

        t006c103 = OBSColumn(
            id='t006c103',
            name='Trinidadian/Tobagonian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c087: DENOMINATOR },)

        t006c104 = OBSColumn(
            id='t006c104',
            name='Vincentian/Grenadinian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c087: DENOMINATOR },)

        t006c105 = OBSColumn(
            id='t006c105',
            name='West Indian, n.o.s.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c087: DENOMINATOR },)

        t006c106 = OBSColumn(
            id='t006c106',
            name='Caribbean origins, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c087: DENOMINATOR },)

        t006c107 = OBSColumn(
            id='t006c107',
            name='Latin, Central and South American origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c001: DENOMINATOR },)

        t006c108 = OBSColumn(
            id='t006c108',
            name='Aboriginal from Central/South America (except Maya)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c109 = OBSColumn(
            id='t006c109',
            name='Argentinian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c110 = OBSColumn(
            id='t006c110',
            name='Belizean',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c111 = OBSColumn(
            id='t006c111',
            name='Bolivian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c112 = OBSColumn(
            id='t006c112',
            name='Brazilian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c113 = OBSColumn(
            id='t006c113',
            name='Chilean',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c114 = OBSColumn(
            id='t006c114',
            name='Colombian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c115 = OBSColumn(
            id='t006c115',
            name='Costa Rican',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c116 = OBSColumn(
            id='t006c116',
            name='Ecuadorian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c117 = OBSColumn(
            id='t006c117',
            name='Guatemalan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c118 = OBSColumn(
            id='t006c118',
            name='Guyanese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c119 = OBSColumn(
            id='t006c119',
            name='Hispanic',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c120 = OBSColumn(
            id='t006c120',
            name='Honduran',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c121 = OBSColumn(
            id='t006c121',
            name='Maya',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c122 = OBSColumn(
            id='t006c122',
            name='Mexican',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c123 = OBSColumn(
            id='t006c123',
            name='Nicaraguan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c124 = OBSColumn(
            id='t006c124',
            name='Panamanian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c125 = OBSColumn(
            id='t006c125',
            name='Paraguayan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c126 = OBSColumn(
            id='t006c126',
            name='Peruvian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c127 = OBSColumn(
            id='t006c127',
            name='Salvadorean',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c128 = OBSColumn(
            id='t006c128',
            name='Uruguayan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c129 = OBSColumn(
            id='t006c129',
            name='Venezuelan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c130 = OBSColumn(
            id='t006c130',
            name='Latin, Central and South American origins, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c107: DENOMINATOR },)

        t006c131 = OBSColumn(
            id='t006c131',
            name='African origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c001: DENOMINATOR },)

        t006c132 = OBSColumn(
            id='t006c132',
            name='Central and West African origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c131: DENOMINATOR },)

        t006c133 = OBSColumn(
            id='t006c133',
            name='Akan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c134 = OBSColumn(
            id='t006c134',
            name='Angolan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c135 = OBSColumn(
            id='t006c135',
            name='Ashanti',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c136 = OBSColumn(
            id='t006c136',
            name='Beninese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c137 = OBSColumn(
            id='t006c137',
            name='Burkinabe',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c138 = OBSColumn(
            id='t006c138',
            name='Cameroonian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c139 = OBSColumn(
            id='t006c139',
            name='Chadian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c140 = OBSColumn(
            id='t006c140',
            name='Congolese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c141 = OBSColumn(
            id='t006c141',
            name='Gabonese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c142 = OBSColumn(
            id='t006c142',
            name='Gambian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c143 = OBSColumn(
            id='t006c143',
            name='Ghanaian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c144 = OBSColumn(
            id='t006c144',
            name='Guinean',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c145 = OBSColumn(
            id='t006c145',
            name='Ibo',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c146 = OBSColumn(
            id='t006c146',
            name='Ivorian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c147 = OBSColumn(
            id='t006c147',
            name='Liberian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c148 = OBSColumn(
            id='t006c148',
            name='Malian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c149 = OBSColumn(
            id='t006c149',
            name='Nigerian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c150 = OBSColumn(
            id='t006c150',
            name='Peulh',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c151 = OBSColumn(
            id='t006c151',
            name='Senegalese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c152 = OBSColumn(
            id='t006c152',
            name='Sierra Leonean',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c153 = OBSColumn(
            id='t006c153',
            name='Togolese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c154 = OBSColumn(
            id='t006c154',
            name='Yoruba',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c155 = OBSColumn(
            id='t006c155',
            name='Central and West African origins, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c132: DENOMINATOR },)

        t006c156 = OBSColumn(
            id='t006c156',
            name='North African origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c131: DENOMINATOR },)

        t006c157 = OBSColumn(
            id='t006c157',
            name='Algerian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c156: DENOMINATOR },)

        t006c158 = OBSColumn(
            id='t006c158',
            name='Berber',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c156: DENOMINATOR },)

        t006c159 = OBSColumn(
            id='t006c159',
            name='Coptic',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c156: DENOMINATOR },)

        t006c160 = OBSColumn(
            id='t006c160',
            name='Dinka',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c156: DENOMINATOR },)

        t006c161 = OBSColumn(
            id='t006c161',
            name='Egyptian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c156: DENOMINATOR },)

        t006c162 = OBSColumn(
            id='t006c162',
            name='Libyan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c156: DENOMINATOR },)

        t006c163 = OBSColumn(
            id='t006c163',
            name='Maure',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c156: DENOMINATOR },)

        t006c164 = OBSColumn(
            id='t006c164',
            name='Moroccan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c156: DENOMINATOR },)

        t006c165 = OBSColumn(
            id='t006c165',
            name='Sudanese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c156: DENOMINATOR },)

        t006c166 = OBSColumn(
            id='t006c166',
            name='Tunisian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c156: DENOMINATOR },)

        t006c167 = OBSColumn(
            id='t006c167',
            name='North African origins, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c156: DENOMINATOR },)

        t006c168 = OBSColumn(
            id='t006c168',
            name='Southern and East African origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c131: DENOMINATOR },)

        t006c169 = OBSColumn(
            id='t006c169',
            name='Afrikaner',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c170 = OBSColumn(
            id='t006c170',
            name='Amhara',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c171 = OBSColumn(
            id='t006c171',
            name='Bantu, n.o.s.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c172 = OBSColumn(
            id='t006c172',
            name='Burundian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c173 = OBSColumn(
            id='t006c173',
            name='Eritrean',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c174 = OBSColumn(
            id='t006c174',
            name='Ethiopian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c175 = OBSColumn(
            id='t006c175',
            name='Harari',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c176 = OBSColumn(
            id='t006c176',
            name='Kenyan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c177 = OBSColumn(
            id='t006c177',
            name='Malagasy',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c178 = OBSColumn(
            id='t006c178',
            name='Mauritian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c179 = OBSColumn(
            id='t006c179',
            name='Oromo',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c180 = OBSColumn(
            id='t006c180',
            name='Rwandan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c181 = OBSColumn(
            id='t006c181',
            name='Seychellois',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c182 = OBSColumn(
            id='t006c182',
            name='Somali',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c183 = OBSColumn(
            id='t006c183',
            name='South African',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c184 = OBSColumn(
            id='t006c184',
            name='Tanzanian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c185 = OBSColumn(
            id='t006c185',
            name='Tigrian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c186 = OBSColumn(
            id='t006c186',
            name='Ugandan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c187 = OBSColumn(
            id='t006c187',
            name='Zambian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c188 = OBSColumn(
            id='t006c188',
            name='Zimbabwean',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c189 = OBSColumn(
            id='t006c189',
            name='Zulu',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c190 = OBSColumn(
            id='t006c190',
            name='Southern and East African origins, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c168: DENOMINATOR },)

        t006c191 = OBSColumn(
            id='t006c191',
            name='Other African origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c131: DENOMINATOR },)

        t006c192 = OBSColumn(
            id='t006c192',
            name='Black, n.o.s.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c191: DENOMINATOR },)

        t006c193 = OBSColumn(
            id='t006c193',
            name='Other African origins, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c191: DENOMINATOR },)

        t006c194 = OBSColumn(
            id='t006c194',
            name='Asian origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c001: DENOMINATOR },)

        t006c195 = OBSColumn(
            id='t006c195',
            name='West Central Asian and Middle Eastern origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c194: DENOMINATOR },)

        t006c196 = OBSColumn(
            id='t006c196',
            name='Afghan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c197 = OBSColumn(
            id='t006c197',
            name='Arab, n.o.s.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c198 = OBSColumn(
            id='t006c198',
            name='Armenian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c199 = OBSColumn(
            id='t006c199',
            name='Assyrian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c200 = OBSColumn(
            id='t006c200',
            name='Azerbaijani',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c201 = OBSColumn(
            id='t006c201',
            name='Georgian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c202 = OBSColumn(
            id='t006c202',
            name='Iranian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c203 = OBSColumn(
            id='t006c203',
            name='Iraqi',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c204 = OBSColumn(
            id='t006c204',
            name='Israeli',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c205 = OBSColumn(
            id='t006c205',
            name='Jordanian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c206 = OBSColumn(
            id='t006c206',
            name='Kazakh',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c207 = OBSColumn(
            id='t006c207',
            name='Kurd',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c208 = OBSColumn(
            id='t006c208',
            name='Kuwaiti',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c209 = OBSColumn(
            id='t006c209',
            name='Lebanese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c210 = OBSColumn(
            id='t006c210',
            name='Palestinian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c211 = OBSColumn(
            id='t006c211',
            name='Pashtun',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c212 = OBSColumn(
            id='t006c212',
            name='Saudi Arabian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c213 = OBSColumn(
            id='t006c213',
            name='Syrian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c214 = OBSColumn(
            id='t006c214',
            name='Tajik',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c215 = OBSColumn(
            id='t006c215',
            name='Tatar',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c216 = OBSColumn(
            id='t006c216',
            name='Turk',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c217 = OBSColumn(
            id='t006c217',
            name='Uighur',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c218 = OBSColumn(
            id='t006c218',
            name='Uzbek',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c219 = OBSColumn(
            id='t006c219',
            name='Yemeni',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c220 = OBSColumn(
            id='t006c220',
            name='West Central Asian and Middle Eastern origins, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c195: DENOMINATOR },)

        t006c221 = OBSColumn(
            id='t006c221',
            name='South Asian origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c194: DENOMINATOR },)

        t006c222 = OBSColumn(
            id='t006c222',
            name='Bangladeshi',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c221: DENOMINATOR },)

        t006c223 = OBSColumn(
            id='t006c223',
            name='Bengali',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c221: DENOMINATOR },)

        t006c224 = OBSColumn(
            id='t006c224',
            name='East Indian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c221: DENOMINATOR },)

        t006c225 = OBSColumn(
            id='t006c225',
            name='Goan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c221: DENOMINATOR },)

        t006c226 = OBSColumn(
            id='t006c226',
            name='Gujarati',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c221: DENOMINATOR },)

        t006c227 = OBSColumn(
            id='t006c227',
            name='Kashmiri',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c221: DENOMINATOR },)

        t006c228 = OBSColumn(
            id='t006c228',
            name='Nepali',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c221: DENOMINATOR },)

        t006c229 = OBSColumn(
            id='t006c229',
            name='Pakistani',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c221: DENOMINATOR },)

        t006c230 = OBSColumn(
            id='t006c230',
            name='Punjabi',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c221: DENOMINATOR },)

        t006c231 = OBSColumn(
            id='t006c231',
            name='Sinhalese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c221: DENOMINATOR },)

        t006c232 = OBSColumn(
            id='t006c232',
            name='Sri Lankan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c221: DENOMINATOR },)

        t006c233 = OBSColumn(
            id='t006c233',
            name='Tamil',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c221: DENOMINATOR },)

        t006c234 = OBSColumn(
            id='t006c234',
            name='South Asian origins, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c221: DENOMINATOR },)

        t006c235 = OBSColumn(
            id='t006c235',
            name='East and Southeast Asian origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c194: DENOMINATOR },)

        t006c236 = OBSColumn(
            id='t006c236',
            name='Burmese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c235: DENOMINATOR },)

        t006c237 = OBSColumn(
            id='t006c237',
            name='Cambodian (Khmer)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c235: DENOMINATOR },)

        t006c238 = OBSColumn(
            id='t006c238',
            name='Chinese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c235: DENOMINATOR },)

        t006c239 = OBSColumn(
            id='t006c239',
            name='Filipino',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c235: DENOMINATOR },)

        t006c240 = OBSColumn(
            id='t006c240',
            name='Hmong',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c235: DENOMINATOR },)

        t006c241 = OBSColumn(
            id='t006c241',
            name='Indonesian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c235: DENOMINATOR },)

        t006c242 = OBSColumn(
            id='t006c242',
            name='Japanese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c235: DENOMINATOR },)

        t006c243 = OBSColumn(
            id='t006c243',
            name='Korean',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c235: DENOMINATOR },)

        t006c244 = OBSColumn(
            id='t006c244',
            name='Laotian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c235: DENOMINATOR },)

        t006c245 = OBSColumn(
            id='t006c245',
            name='Malaysian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c235: DENOMINATOR },)

        t006c246 = OBSColumn(
            id='t006c246',
            name='Mongolian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c235: DENOMINATOR },)

        t006c247 = OBSColumn(
            id='t006c247',
            name='Singaporean',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c235: DENOMINATOR },)

        t006c248 = OBSColumn(
            id='t006c248',
            name='Taiwanese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c235: DENOMINATOR },)

        t006c249 = OBSColumn(
            id='t006c249',
            name='Thai',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c235: DENOMINATOR },)

        t006c250 = OBSColumn(
            id='t006c250',
            name='Tibetan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c235: DENOMINATOR },)

        t006c251 = OBSColumn(
            id='t006c251',
            name='Vietnamese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c235: DENOMINATOR },)

        t006c252 = OBSColumn(
            id='t006c252',
            name='East and Southeast Asian origins, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c235: DENOMINATOR },)

        t006c253 = OBSColumn(
            id='t006c253',
            name='Other Asian origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c194: DENOMINATOR },)

        t006c254 = OBSColumn(
            id='t006c254',
            name='Other Asian origins, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c253: DENOMINATOR },)

        t006c255 = OBSColumn(
            id='t006c255',
            name='Oceania origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c001: DENOMINATOR },)

        t006c256 = OBSColumn(
            id='t006c256',
            name='Australian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c255: DENOMINATOR },)

        t006c257 = OBSColumn(
            id='t006c257',
            name='New Zealander',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c255: DENOMINATOR },)

        t006c258 = OBSColumn(
            id='t006c258',
            name='Pacific Islands origins',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c255: DENOMINATOR },)

        t006c259 = OBSColumn(
            id='t006c259',
            name='Fijian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c258: DENOMINATOR },)

        t006c260 = OBSColumn(
            id='t006c260',
            name='Hawaiian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c258: DENOMINATOR },)

        t006c261 = OBSColumn(
            id='t006c261',
            name='Maori',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c258: DENOMINATOR },)

        t006c262 = OBSColumn(
            id='t006c262',
            name='Polynesian, n.o.s.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c258: DENOMINATOR },)

        t006c263 = OBSColumn(
            id='t006c263',
            name='Samoan',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c258: DENOMINATOR },)

        t006c264 = OBSColumn(
            id='t006c264',
            name='Pacific Islands origins, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t006c258: DENOMINATOR },)

        t007c001 = OBSColumn(
            id='t007c001',
            name='Total labour force population aged 15 years and over by full-time or part-time weeks worked in 2010',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={},)

        t007c002 = OBSColumn(
            id='t007c002',
            name='Did not work in 2010',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t007c001: DENOMINATOR },)

        t007c003 = OBSColumn(
            id='t007c003',
            name='Worked in 2010',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t007c001: DENOMINATOR },)

        t007c004 = OBSColumn(
            id='t007c004',
            name='Worked full-time in 2010',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t007c003: DENOMINATOR },)

        t007c005 = OBSColumn(
            id='t007c005',
            name='Worked part-time in 2010',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t007c003: DENOMINATOR },)

        t008c001 = OBSColumn(
            id='t008c001',
            name='Total population in private households by generation status',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={},)

        t008c002 = OBSColumn(
            id='t008c002',
            name='First generation',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t008c001: DENOMINATOR },)

        t008c003 = OBSColumn(
            id='t008c003',
            name='Second generation',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t008c001: DENOMINATOR },)

        t008c004 = OBSColumn(
            id='t008c004',
            name='Third generation or more',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t008c001: DENOMINATOR },)

        t009c001 = OBSColumn(
            id='t009c001',
            name='Total number of private households by tenure',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={},)

        t009c002 = OBSColumn(
            id='t009c002',
            name='Owner',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t009c001: DENOMINATOR },)

        t009c003 = OBSColumn(
            id='t009c003',
            name='Renter',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t009c001: DENOMINATOR },)

        t009c004 = OBSColumn(
            id='t009c004',
            name='Band housing',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t009c001: DENOMINATOR },)

        t009c005 = OBSColumn(
            id='t009c005',
            name='Total number of private households by condominium status',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={},)

        t009c006 = OBSColumn(
            id='t009c006',
            name='Part of a condominium development',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t009c005: DENOMINATOR },)

        t009c007 = OBSColumn(
            id='t009c007',
            name='Not part of a condominium development',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t009c005: DENOMINATOR },)

        t009c008 = OBSColumn(
            id='t009c008',
            name='Total number of private households by number of household maintainers',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={},)

        t009c009 = OBSColumn(
            id='t009c009',
            name='1 household maintainer',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t009c008: DENOMINATOR },)

        t009c010 = OBSColumn(
            id='t009c010',
            name='2 household maintainers',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t009c008: DENOMINATOR },)

        t009c011 = OBSColumn(
            id='t009c011',
            name='3 or more household maintainers',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t009c008: DENOMINATOR },)

        t009c012 = OBSColumn(
            id='t009c012',
            name='Total number of private households by age group of primary household maintainers',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={},)

        t009c013 = OBSColumn(
            id='t009c013',
            name='Under 25 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t009c012: DENOMINATOR },)

        t009c014 = OBSColumn(
            id='t009c014',
            name='25 to 34 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t009c012: DENOMINATOR },)

        t009c015 = OBSColumn(
            id='t009c015',
            name='35 to 44 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t009c012: DENOMINATOR },)

        t009c016 = OBSColumn(
            id='t009c016',
            name='45 to 54 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t009c012: DENOMINATOR },)

        t009c017 = OBSColumn(
            id='t009c017',
            name='55 to 64 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t009c012: DENOMINATOR },)

        t009c018 = OBSColumn(
            id='t009c018',
            name='65 to 74 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t009c012: DENOMINATOR },)

        t009c019 = OBSColumn(
            id='t009c019',
            name='75 years and over',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t009c012: DENOMINATOR },)

        t009c020 = OBSColumn(
            id='t009c020',
            name='Total number of private households by number of persons per room',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={},)

        t009c021 = OBSColumn(
            id='t009c021',
            name='One person or fewer per room',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t009c020: DENOMINATOR },)

        t009c022 = OBSColumn(
            id='t009c022',
            name='More than one person per room',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t009c020: DENOMINATOR },)

        t009c023 = OBSColumn(
            id='t009c023',
            name='Total number of private households by housing suitability',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={},)

        t009c024 = OBSColumn(
            id='t009c024',
            name='Suitable',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t009c023: DENOMINATOR },)

        t009c025 = OBSColumn(
            id='t009c025',
            name='Not suitable',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t009c023: DENOMINATOR },)

        t010c001 = OBSColumn(
            id='t010c001',
            name='Total population in private households by immigrant status and period of immigration',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={},)

        t010c002 = OBSColumn(
            id='t010c002',
            name='Non-immigrants',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t010c001: DENOMINATOR },)

        t010c003 = OBSColumn(
            id='t010c003',
            name='Immigrants',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t010c001: DENOMINATOR },)

        t010c004 = OBSColumn(
            id='t010c004',
            name='Before 1971',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t010c003: DENOMINATOR },)

        t010c005 = OBSColumn(
            id='t010c005',
            name='1971 to 1980',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t010c003: DENOMINATOR },)

        t010c006 = OBSColumn(
            id='t010c006',
            name='1981 to 1990',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t010c003: DENOMINATOR },)

        t010c007 = OBSColumn(
            id='t010c007',
            name='1991 to 2000',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t010c003: DENOMINATOR },)

        t010c008 = OBSColumn(
            id='t010c008',
            name='2001 to 2011',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t010c003: DENOMINATOR },)

        t010c009 = OBSColumn(
            id='t010c009',
            name='2001 to 2005',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t010c008: DENOMINATOR },)

        t010c010 = OBSColumn(
            id='t010c010',
            name='2006 to 2011',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t010c008: DENOMINATOR },)

        t010c011 = OBSColumn(
            id='t010c011',
            name='Non-permanent residents',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
        targets={ t010c001: DENOMINATOR },)

        return OrderedDict([
            ('t001c001', t001c001),
            ('t001c002', t001c002),
            ('t001c003', t001c003),
            ('t001c004', t001c004),
            ('t001c005', t001c005),
            ('t001c006', t001c006),
            ('t001c007', t001c007),
            ('t001c008', t001c008),
            ('t001c009', t001c009),
            ('t001c010', t001c010),
            ('t001c011', t001c011),
            ('t001c012', t001c012),
            ('t001c013', t001c013),
            ('t001c014', t001c014),
            ('t001c015', t001c015),
            ('t001c016', t001c016),
            ('t001c017', t001c017),
            ('t002c001', t002c001),
            ('t002c002', t002c002),
            ('t002c003', t002c003),
            ('t002c004', t002c004),
            ('t002c005', t002c005),
            ('t002c006', t002c006),
            ('t003c001', t003c001),
            ('t003c002', t003c002),
            ('t003c003', t003c003),
            ('t003c004', t003c004),
            ('t003c005', t003c005),
            ('t004c001', t004c001),
            ('t004c002', t004c002),
            ('t004c003', t004c003),
            ('t004c004', t004c004),
            ('t004c005', t004c005),
            ('t005c001', t005c001),
            ('t005c002', t005c002),
            ('t005c003', t005c003),
            ('t005c004', t005c004),
            ('t005c005', t005c005),
            ('t005c006', t005c006),
            ('t005c007', t005c007),
            ('t005c008', t005c008),
            ('t005c009', t005c009),
            ('t005c010', t005c010),
            ('t005c011', t005c011),
            ('t005c012', t005c012),
            ('t005c013', t005c013),
            ('t005c014', t005c014),
            ('t005c015', t005c015),
            ('t005c016', t005c016),
            ('t005c017', t005c017),
            ('t005c018', t005c018),
            ('t005c019', t005c019),
            ('t005c020', t005c020),
            ('t005c021', t005c021),
            ('t005c022', t005c022),
            ('t005c023', t005c023),
            ('t005c024', t005c024),
            ('t005c025', t005c025),
            ('t005c026', t005c026),
            ('t005c027', t005c027),
            ('t005c028', t005c028),
            ('t005c029', t005c029),
            ('t005c030', t005c030),
            ('t005c031', t005c031),
            ('t005c032', t005c032),
            ('t005c033', t005c033),
            ('t005c034', t005c034),
            ('t005c035', t005c035),
            ('t005c036', t005c036),
            ('t005c037', t005c037),
            ('t005c038', t005c038),
            ('t005c039', t005c039),
            ('t005c040', t005c040),
            ('t005c041', t005c041),
            ('t006c001', t006c001),
            ('t006c002', t006c002),
            ('t006c003', t006c003),
            ('t006c004', t006c004),
            ('t006c005', t006c005),
            ('t006c006', t006c006),
            ('t006c007', t006c007),
            ('t006c008', t006c008),
            ('t006c009', t006c009),
            ('t006c010', t006c010),
            ('t006c011', t006c011),
            ('t006c012', t006c012),
            ('t006c013', t006c013),
            ('t006c014', t006c014),
            ('t006c015', t006c015),
            ('t006c016', t006c016),
            ('t006c017', t006c017),
            ('t006c018', t006c018),
            ('t006c019', t006c019),
            ('t006c020', t006c020),
            ('t006c021', t006c021),
            ('t006c022', t006c022),
            ('t006c023', t006c023),
            ('t006c024', t006c024),
            ('t006c025', t006c025),
            ('t006c026', t006c026),
            ('t006c027', t006c027),
            ('t006c028', t006c028),
            ('t006c029', t006c029),
            ('t006c030', t006c030),
            ('t006c031', t006c031),
            ('t006c032', t006c032),
            ('t006c033', t006c033),
            ('t006c034', t006c034),
            ('t006c035', t006c035),
            ('t006c036', t006c036),
            ('t006c037', t006c037),
            ('t006c038', t006c038),
            ('t006c039', t006c039),
            ('t006c040', t006c040),
            ('t006c041', t006c041),
            ('t006c042', t006c042),
            ('t006c043', t006c043),
            ('t006c044', t006c044),
            ('t006c045', t006c045),
            ('t006c046', t006c046),
            ('t006c047', t006c047),
            ('t006c048', t006c048),
            ('t006c049', t006c049),
            ('t006c050', t006c050),
            ('t006c051', t006c051),
            ('t006c052', t006c052),
            ('t006c053', t006c053),
            ('t006c054', t006c054),
            ('t006c055', t006c055),
            ('t006c056', t006c056),
            ('t006c057', t006c057),
            ('t006c058', t006c058),
            ('t006c059', t006c059),
            ('t006c060', t006c060),
            ('t006c061', t006c061),
            ('t006c062', t006c062),
            ('t006c063', t006c063),
            ('t006c064', t006c064),
            ('t006c065', t006c065),
            ('t006c066', t006c066),
            ('t006c067', t006c067),
            ('t006c068', t006c068),
            ('t006c069', t006c069),
            ('t006c070', t006c070),
            ('t006c071', t006c071),
            ('t006c072', t006c072),
            ('t006c073', t006c073),
            ('t006c074', t006c074),
            ('t006c075', t006c075),
            ('t006c076', t006c076),
            ('t006c077', t006c077),
            ('t006c078', t006c078),
            ('t006c079', t006c079),
            ('t006c080', t006c080),
            ('t006c081', t006c081),
            ('t006c082', t006c082),
            ('t006c083', t006c083),
            ('t006c084', t006c084),
            ('t006c085', t006c085),
            ('t006c086', t006c086),
            ('t006c087', t006c087),
            ('t006c088', t006c088),
            ('t006c089', t006c089),
            ('t006c090', t006c090),
            ('t006c091', t006c091),
            ('t006c092', t006c092),
            ('t006c093', t006c093),
            ('t006c094', t006c094),
            ('t006c095', t006c095),
            ('t006c096', t006c096),
            ('t006c097', t006c097),
            ('t006c098', t006c098),
            ('t006c099', t006c099),
            ('t006c100', t006c100),
            ('t006c101', t006c101),
            ('t006c102', t006c102),
            ('t006c103', t006c103),
            ('t006c104', t006c104),
            ('t006c105', t006c105),
            ('t006c106', t006c106),
            ('t006c107', t006c107),
            ('t006c108', t006c108),
            ('t006c109', t006c109),
            ('t006c110', t006c110),
            ('t006c111', t006c111),
            ('t006c112', t006c112),
            ('t006c113', t006c113),
            ('t006c114', t006c114),
            ('t006c115', t006c115),
            ('t006c116', t006c116),
            ('t006c117', t006c117),
            ('t006c118', t006c118),
            ('t006c119', t006c119),
            ('t006c120', t006c120),
            ('t006c121', t006c121),
            ('t006c122', t006c122),
            ('t006c123', t006c123),
            ('t006c124', t006c124),
            ('t006c125', t006c125),
            ('t006c126', t006c126),
            ('t006c127', t006c127),
            ('t006c128', t006c128),
            ('t006c129', t006c129),
            ('t006c130', t006c130),
            ('t006c131', t006c131),
            ('t006c132', t006c132),
            ('t006c133', t006c133),
            ('t006c134', t006c134),
            ('t006c135', t006c135),
            ('t006c136', t006c136),
            ('t006c137', t006c137),
            ('t006c138', t006c138),
            ('t006c139', t006c139),
            ('t006c140', t006c140),
            ('t006c141', t006c141),
            ('t006c142', t006c142),
            ('t006c143', t006c143),
            ('t006c144', t006c144),
            ('t006c145', t006c145),
            ('t006c146', t006c146),
            ('t006c147', t006c147),
            ('t006c148', t006c148),
            ('t006c149', t006c149),
            ('t006c150', t006c150),
            ('t006c151', t006c151),
            ('t006c152', t006c152),
            ('t006c153', t006c153),
            ('t006c154', t006c154),
            ('t006c155', t006c155),
            ('t006c156', t006c156),
            ('t006c157', t006c157),
            ('t006c158', t006c158),
            ('t006c159', t006c159),
            ('t006c160', t006c160),
            ('t006c161', t006c161),
            ('t006c162', t006c162),
            ('t006c163', t006c163),
            ('t006c164', t006c164),
            ('t006c165', t006c165),
            ('t006c166', t006c166),
            ('t006c167', t006c167),
            ('t006c168', t006c168),
            ('t006c169', t006c169),
            ('t006c170', t006c170),
            ('t006c171', t006c171),
            ('t006c172', t006c172),
            ('t006c173', t006c173),
            ('t006c174', t006c174),
            ('t006c175', t006c175),
            ('t006c176', t006c176),
            ('t006c177', t006c177),
            ('t006c178', t006c178),
            ('t006c179', t006c179),
            ('t006c180', t006c180),
            ('t006c181', t006c181),
            ('t006c182', t006c182),
            ('t006c183', t006c183),
            ('t006c184', t006c184),
            ('t006c185', t006c185),
            ('t006c186', t006c186),
            ('t006c187', t006c187),
            ('t006c188', t006c188),
            ('t006c189', t006c189),
            ('t006c190', t006c190),
            ('t006c191', t006c191),
            ('t006c192', t006c192),
            ('t006c193', t006c193),
            ('t006c194', t006c194),
            ('t006c195', t006c195),
            ('t006c196', t006c196),
            ('t006c197', t006c197),
            ('t006c198', t006c198),
            ('t006c199', t006c199),
            ('t006c200', t006c200),
            ('t006c201', t006c201),
            ('t006c202', t006c202),
            ('t006c203', t006c203),
            ('t006c204', t006c204),
            ('t006c205', t006c205),
            ('t006c206', t006c206),
            ('t006c207', t006c207),
            ('t006c208', t006c208),
            ('t006c209', t006c209),
            ('t006c210', t006c210),
            ('t006c211', t006c211),
            ('t006c212', t006c212),
            ('t006c213', t006c213),
            ('t006c214', t006c214),
            ('t006c215', t006c215),
            ('t006c216', t006c216),
            ('t006c217', t006c217),
            ('t006c218', t006c218),
            ('t006c219', t006c219),
            ('t006c220', t006c220),
            ('t006c221', t006c221),
            ('t006c222', t006c222),
            ('t006c223', t006c223),
            ('t006c224', t006c224),
            ('t006c225', t006c225),
            ('t006c226', t006c226),
            ('t006c227', t006c227),
            ('t006c228', t006c228),
            ('t006c229', t006c229),
            ('t006c230', t006c230),
            ('t006c231', t006c231),
            ('t006c232', t006c232),
            ('t006c233', t006c233),
            ('t006c234', t006c234),
            ('t006c235', t006c235),
            ('t006c236', t006c236),
            ('t006c237', t006c237),
            ('t006c238', t006c238),
            ('t006c239', t006c239),
            ('t006c240', t006c240),
            ('t006c241', t006c241),
            ('t006c242', t006c242),
            ('t006c243', t006c243),
            ('t006c244', t006c244),
            ('t006c245', t006c245),
            ('t006c246', t006c246),
            ('t006c247', t006c247),
            ('t006c248', t006c248),
            ('t006c249', t006c249),
            ('t006c250', t006c250),
            ('t006c251', t006c251),
            ('t006c252', t006c252),
            ('t006c253', t006c253),
            ('t006c254', t006c254),
            ('t006c255', t006c255),
            ('t006c256', t006c256),
            ('t006c257', t006c257),
            ('t006c258', t006c258),
            ('t006c259', t006c259),
            ('t006c260', t006c260),
            ('t006c261', t006c261),
            ('t006c262', t006c262),
            ('t006c263', t006c263),
            ('t006c264', t006c264),
            ('t007c001', t007c001),
            ('t007c002', t007c002),
            ('t007c003', t007c003),
            ('t007c004', t007c004),
            ('t007c005', t007c005),
            ('t008c001', t008c001),
            ('t008c002', t008c002),
            ('t008c003', t008c003),
            ('t008c004', t008c004),
            ('t009c001', t009c001),
            ('t009c002', t009c002),
            ('t009c003', t009c003),
            ('t009c004', t009c004),
            ('t009c005', t009c005),
            ('t009c006', t009c006),
            ('t009c007', t009c007),
            ('t009c008', t009c008),
            ('t009c009', t009c009),
            ('t009c010', t009c010),
            ('t009c011', t009c011),
            ('t009c012', t009c012),
            ('t009c013', t009c013),
            ('t009c014', t009c014),
            ('t009c015', t009c015),
            ('t009c016', t009c016),
            ('t009c017', t009c017),
            ('t009c018', t009c018),
            ('t009c019', t009c019),
            ('t009c020', t009c020),
            ('t009c021', t009c021),
            ('t009c022', t009c022),
            ('t009c023', t009c023),
            ('t009c024', t009c024),
            ('t009c025', t009c025),
            ('t010c001', t010c001),
            ('t010c002', t010c002),
            ('t010c003', t010c003),
            ('t010c004', t010c004),
            ('t010c005', t010c005),
            ('t010c006', t010c006),
            ('t010c007', t010c007),
            ('t010c008', t010c008),
            ('t010c009', t010c009),
            ('t010c010', t010c010),
            ('t010c011', t010c011),
        ])
