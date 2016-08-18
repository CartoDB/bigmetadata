from tasks.meta import OBSColumn, DENOMINATOR
from tasks.util import ColumnsTask
from tasks.tags import SectionTags, SubsectionTags, UnitTags

from collections import OrderedDict

class CensusColumns(ColumnsTask):

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'units': UnitTags(),
        }

    def version(self):
        return 1

    def columns(self):
        input_ = self.input()

        subsections = input_['subsections']

        unit_people = input_['units']['people']
        unit_housing = input_['units']['housing_units']
        unit_household = input_['units']['households']
        unit_years = input_['units']['years']
        unit_ratio = input_['units']['ratio']
        unit_education = input_['units']['education_level']

        ca = input_['sections']['ca']

        t001c001 = OBSColumn(
            id='t001c001',
            name='Total population by age groups',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t001c002 = OBSColumn(
            id='t001c002',
            name='0 to 4 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001: DENOMINATOR },)

        t001c003 = OBSColumn(
            id='t001c003',
            name='5 to 9 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001: DENOMINATOR },)

        t001c004 = OBSColumn(
            id='t001c004',
            name='10 to 14 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001: DENOMINATOR },)

        t001c005 = OBSColumn(
            id='t001c005',
            name='15 to 19 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001: DENOMINATOR },)

        t001c006 = OBSColumn(
            id='t001c006',
            name='15 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c005: DENOMINATOR },)

        t001c007 = OBSColumn(
            id='t001c007',
            name='16 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c005: DENOMINATOR },)

        t001c008 = OBSColumn(
            id='t001c008',
            name='17 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c005: DENOMINATOR },)

        t001c009 = OBSColumn(
            id='t001c009',
            name='18 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c005: DENOMINATOR },)

        t001c010 = OBSColumn(
            id='t001c010',
            name='19 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c005: DENOMINATOR },)

        t001c011 = OBSColumn(
            id='t001c011',
            name='20 to 24 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001: DENOMINATOR },)

        t001c012 = OBSColumn(
            id='t001c012',
            name='25 to 29 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001: DENOMINATOR },)

        t001c013 = OBSColumn(
            id='t001c013',
            name='30 to 34 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001: DENOMINATOR },)

        t001c014 = OBSColumn(
            id='t001c014',
            name='35 to 39 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001: DENOMINATOR },)

        t001c015 = OBSColumn(
            id='t001c015',
            name='40 to 44 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001: DENOMINATOR },)

        t001c016 = OBSColumn(
            id='t001c016',
            name='45 to 49 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001: DENOMINATOR },)

        t001c017 = OBSColumn(
            id='t001c017',
            name='50 to 54 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001: DENOMINATOR },)

        t001c018 = OBSColumn(
            id='t001c018',
            name='55 to 59 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001: DENOMINATOR },)

        t001c019 = OBSColumn(
            id='t001c019',
            name='60 to 64 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001: DENOMINATOR },)

        t001c020 = OBSColumn(
            id='t001c020',
            name='65 to 69 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001: DENOMINATOR },)

        t001c021 = OBSColumn(
            id='t001c021',
            name='70 to 74 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001: DENOMINATOR },)

        t001c022 = OBSColumn(
            id='t001c022',
            name='75 to 79 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001: DENOMINATOR },)

        t001c023 = OBSColumn(
            id='t001c023',
            name='80 to 84 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001: DENOMINATOR },)

        t001c024 = OBSColumn(
            id='t001c024',
            name='85 years and over',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001: DENOMINATOR },)

        t001c025 = OBSColumn(
            id='t001c025',
            name='Median age of the population',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t001c026 = OBSColumn(
            id='t001c026',
            name='% of the population aged 15 and over',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t002c001 = OBSColumn(
            id='t002c001',
            name='Detailed language spoken most often at home - Total population excluding institutional residents',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t002c002 = OBSColumn(
            id='t002c002',
            name='Single responses',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c001: DENOMINATOR },)

        t002c003 = OBSColumn(
            id='t002c003',
            name='English',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c002: DENOMINATOR },)

        t002c004 = OBSColumn(
            id='t002c004',
            name='French',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c002: DENOMINATOR },)

        t002c005 = OBSColumn(
            id='t002c005',
            name='Non-official languages',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c002: DENOMINATOR },)

        t002c006 = OBSColumn(
            id='t002c006',
            name='Selected Aboriginal languages',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c005: DENOMINATOR },)

        t002c007 = OBSColumn(
            id='t002c007',
            name='Atikamekw',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c006: DENOMINATOR },)

        t002c008 = OBSColumn(
            id='t002c008',
            name='Cree, n.o.s.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c006: DENOMINATOR },)

        t002c009 = OBSColumn(
            id='t002c009',
            name='Dene',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c006: DENOMINATOR },)

        t002c010 = OBSColumn(
            id='t002c010',
            name='Innu/Montagnais',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c006: DENOMINATOR },)

        t002c011 = OBSColumn(
            id='t002c011',
            name='Inuktitut',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c006: DENOMINATOR },)

        t002c012 = OBSColumn(
            id='t002c012',
            name='Mi\'kmaq',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c006: DENOMINATOR },)

        t002c013 = OBSColumn(
            id='t002c013',
            name='Ojibway',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c006: DENOMINATOR },)

        t002c014 = OBSColumn(
            id='t002c014',
            name='Oji-Cree',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c006: DENOMINATOR },)

        t002c015 = OBSColumn(
            id='t002c015',
            name='Stoney',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c006: DENOMINATOR },)

        t002c016 = OBSColumn(
            id='t002c016',
            name='Selected non-Aboriginal languages',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c005: DENOMINATOR },)

        t002c017 = OBSColumn(
            id='t002c017',
            name='African languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c018 = OBSColumn(
            id='t002c018',
            name='Afrikaans',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c019 = OBSColumn(
            id='t002c019',
            name='Akan (Twi)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c020 = OBSColumn(
            id='t002c020',
            name='Albanian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c021 = OBSColumn(
            id='t002c021',
            name='Amharic',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c022 = OBSColumn(
            id='t002c022',
            name='Arabic',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c023 = OBSColumn(
            id='t002c023',
            name='Armenian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c024 = OBSColumn(
            id='t002c024',
            name='Bantu languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c025 = OBSColumn(
            id='t002c025',
            name='Bengali',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c026 = OBSColumn(
            id='t002c026',
            name='Berber languages (Kabyle)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c027 = OBSColumn(
            id='t002c027',
            name='Bisayan languages',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c028 = OBSColumn(
            id='t002c028',
            name='Bosnian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c029 = OBSColumn(
            id='t002c029',
            name='Bulgarian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c030 = OBSColumn(
            id='t002c030',
            name='Burmese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c031 = OBSColumn(
            id='t002c031',
            name='Cantonese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c032 = OBSColumn(
            id='t002c032',
            name='Chinese, n.o.s.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c033 = OBSColumn(
            id='t002c033',
            name='Creoles',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c034 = OBSColumn(
            id='t002c034',
            name='Croatian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c035 = OBSColumn(
            id='t002c035',
            name='Czech',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c036 = OBSColumn(
            id='t002c036',
            name='Danish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c037 = OBSColumn(
            id='t002c037',
            name='Dutch',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c038 = OBSColumn(
            id='t002c038',
            name='Estonian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c039 = OBSColumn(
            id='t002c039',
            name='Finnish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c040 = OBSColumn(
            id='t002c040',
            name='Flemish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c041 = OBSColumn(
            id='t002c041',
            name='Fukien',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c042 = OBSColumn(
            id='t002c042',
            name='German',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c043 = OBSColumn(
            id='t002c043',
            name='Greek',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c044 = OBSColumn(
            id='t002c044',
            name='Gujarati',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c045 = OBSColumn(
            id='t002c045',
            name='Hakka',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c046 = OBSColumn(
            id='t002c046',
            name='Hebrew',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c047 = OBSColumn(
            id='t002c047',
            name='Hindi',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c048 = OBSColumn(
            id='t002c048',
            name='Hungarian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c049 = OBSColumn(
            id='t002c049',
            name='Ilocano',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c050 = OBSColumn(
            id='t002c050',
            name='Indo-Iranian languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c051 = OBSColumn(
            id='t002c051',
            name='Italian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c052 = OBSColumn(
            id='t002c052',
            name='Japanese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c053 = OBSColumn(
            id='t002c053',
            name='Khmer (Cambodian)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c054 = OBSColumn(
            id='t002c054',
            name='Korean',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c055 = OBSColumn(
            id='t002c055',
            name='Kurdish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c056 = OBSColumn(
            id='t002c056',
            name='Lao',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c057 = OBSColumn(
            id='t002c057',
            name='Latvian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c058 = OBSColumn(
            id='t002c058',
            name='Lingala',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c059 = OBSColumn(
            id='t002c059',
            name='Lithuanian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c060 = OBSColumn(
            id='t002c060',
            name='Macedonian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c061 = OBSColumn(
            id='t002c061',
            name='Malay',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c062 = OBSColumn(
            id='t002c062',
            name='Malayalam',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c063 = OBSColumn(
            id='t002c063',
            name='Maltese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c064 = OBSColumn(
            id='t002c064',
            name='Mandarin',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c065 = OBSColumn(
            id='t002c065',
            name='Marathi',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c066 = OBSColumn(
            id='t002c066',
            name='Nepali',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c067 = OBSColumn(
            id='t002c067',
            name='Niger-Congo languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c068 = OBSColumn(
            id='t002c068',
            name='Norwegian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c069 = OBSColumn(
            id='t002c069',
            name='Oromo',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c070 = OBSColumn(
            id='t002c070',
            name='Panjabi (Punjabi)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c071 = OBSColumn(
            id='t002c071',
            name='Pashto',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c072 = OBSColumn(
            id='t002c072',
            name='Persian (Farsi)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c073 = OBSColumn(
            id='t002c073',
            name='Polish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c074 = OBSColumn(
            id='t002c074',
            name='Portuguese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c075 = OBSColumn(
            id='t002c075',
            name='Romanian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c076 = OBSColumn(
            id='t002c076',
            name='Rundi (Kirundi)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c077 = OBSColumn(
            id='t002c077',
            name='Russian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c078 = OBSColumn(
            id='t002c078',
            name='Rwanda (Kinyarwanda)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c079 = OBSColumn(
            id='t002c079',
            name='Semitic languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c080 = OBSColumn(
            id='t002c080',
            name='Serbian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c081 = OBSColumn(
            id='t002c081',
            name='Serbo-Croatian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c082 = OBSColumn(
            id='t002c082',
            name='Shanghainese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c083 = OBSColumn(
            id='t002c083',
            name='Sign languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c084 = OBSColumn(
            id='t002c084',
            name='Sindhi',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c085 = OBSColumn(
            id='t002c085',
            name='Sinhala (Sinhalese)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c086 = OBSColumn(
            id='t002c086',
            name='Sino-Tibetan languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c087 = OBSColumn(
            id='t002c087',
            name='Slavic languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c088 = OBSColumn(
            id='t002c088',
            name='Slovak',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c089 = OBSColumn(
            id='t002c089',
            name='Slovenian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c090 = OBSColumn(
            id='t002c090',
            name='Somali',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c091 = OBSColumn(
            id='t002c091',
            name='Spanish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c092 = OBSColumn(
            id='t002c092',
            name='Swahili',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c093 = OBSColumn(
            id='t002c093',
            name='Swedish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c094 = OBSColumn(
            id='t002c094',
            name='Tagalog (Pilipino, Filipino)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c095 = OBSColumn(
            id='t002c095',
            name='Taiwanese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c096 = OBSColumn(
            id='t002c096',
            name='Tamil',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c097 = OBSColumn(
            id='t002c097',
            name='Telugu',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c098 = OBSColumn(
            id='t002c098',
            name='Thai',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c099 = OBSColumn(
            id='t002c099',
            name='Tibetan languages',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c100 = OBSColumn(
            id='t002c100',
            name='Tigrigna',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c101 = OBSColumn(
            id='t002c101',
            name='Turkish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c102 = OBSColumn(
            id='t002c102',
            name='Ukrainian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c103 = OBSColumn(
            id='t002c103',
            name='Urdu',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c104 = OBSColumn(
            id='t002c104',
            name='Vietnamese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c105 = OBSColumn(
            id='t002c105',
            name='Yiddish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c016: DENOMINATOR },)

        t002c106 = OBSColumn(
            id='t002c106',
            name='Other languages',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c005: DENOMINATOR },)

        t002c107 = OBSColumn(
            id='t002c107',
            name='Multiple responses',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c001: DENOMINATOR },)

        t002c108 = OBSColumn(
            id='t002c108',
            name='English and French',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c107: DENOMINATOR },)

        t002c109 = OBSColumn(
            id='t002c109',
            name='English and non-official language',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c107: DENOMINATOR },)

        t002c110 = OBSColumn(
            id='t002c110',
            name='French and non-official language',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c107: DENOMINATOR },)

        t002c111 = OBSColumn(
            id='t002c111',
            name='English, French and non-official language',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t002c107: DENOMINATOR },)

        t003c001 = OBSColumn(
            id='t003c001',
            name='Detailed mother tongue - Total population excluding institutional residents',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t003c002 = OBSColumn(
            id='t003c002',
            name='Single responses',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c001: DENOMINATOR },)

        t003c003 = OBSColumn(
            id='t003c003',
            name='English',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c002: DENOMINATOR },)

        t003c004 = OBSColumn(
            id='t003c004',
            name='French',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c002: DENOMINATOR },)

        t003c005 = OBSColumn(
            id='t003c005',
            name='Non-official languages',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c002: DENOMINATOR },)

        t003c006 = OBSColumn(
            id='t003c006',
            name='Selected Aboriginal languages',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c005: DENOMINATOR },)

        t003c007 = OBSColumn(
            id='t003c007',
            name='Atikamekw',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c006: DENOMINATOR },)

        t003c008 = OBSColumn(
            id='t003c008',
            name='Cree, n.o.s.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c006: DENOMINATOR },)

        t003c009 = OBSColumn(
            id='t003c009',
            name='Dene',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c006: DENOMINATOR },)

        t003c010 = OBSColumn(
            id='t003c010',
            name='Innu/Montagnais',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c006: DENOMINATOR },)

        t003c011 = OBSColumn(
            id='t003c011',
            name='Inuktitut',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c006: DENOMINATOR },)

        t003c012 = OBSColumn(
            id='t003c012',
            name='Mi\'kmaq',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c006: DENOMINATOR },)

        t003c013 = OBSColumn(
            id='t003c013',
            name='Ojibway',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c006: DENOMINATOR },)

        t003c014 = OBSColumn(
            id='t003c014',
            name='Oji-Cree',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c006: DENOMINATOR },)

        t003c015 = OBSColumn(
            id='t003c015',
            name='Stoney',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c006: DENOMINATOR },)

        t003c016 = OBSColumn(
            id='t003c016',
            name='Selected non-Aboriginal languages',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c005: DENOMINATOR },)

        t003c017 = OBSColumn(
            id='t003c017',
            name='African languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c018 = OBSColumn(
            id='t003c018',
            name='Afrikaans',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c019 = OBSColumn(
            id='t003c019',
            name='Akan (Twi)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c020 = OBSColumn(
            id='t003c020',
            name='Albanian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c021 = OBSColumn(
            id='t003c021',
            name='Amharic',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c022 = OBSColumn(
            id='t003c022',
            name='Arabic',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c023 = OBSColumn(
            id='t003c023',
            name='Armenian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c024 = OBSColumn(
            id='t003c024',
            name='Bantu languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c025 = OBSColumn(
            id='t003c025',
            name='Bengali',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c026 = OBSColumn(
            id='t003c026',
            name='Berber languages (Kabyle)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c027 = OBSColumn(
            id='t003c027',
            name='Bisayan languages',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c028 = OBSColumn(
            id='t003c028',
            name='Bosnian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c029 = OBSColumn(
            id='t003c029',
            name='Bulgarian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c030 = OBSColumn(
            id='t003c030',
            name='Burmese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c031 = OBSColumn(
            id='t003c031',
            name='Cantonese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c032 = OBSColumn(
            id='t003c032',
            name='Chinese, n.o.s.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c033 = OBSColumn(
            id='t003c033',
            name='Creoles',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c034 = OBSColumn(
            id='t003c034',
            name='Croatian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c035 = OBSColumn(
            id='t003c035',
            name='Czech',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c036 = OBSColumn(
            id='t003c036',
            name='Danish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c037 = OBSColumn(
            id='t003c037',
            name='Dutch',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c038 = OBSColumn(
            id='t003c038',
            name='Estonian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c039 = OBSColumn(
            id='t003c039',
            name='Finnish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c040 = OBSColumn(
            id='t003c040',
            name='Flemish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c041 = OBSColumn(
            id='t003c041',
            name='Fukien',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c042 = OBSColumn(
            id='t003c042',
            name='German',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c043 = OBSColumn(
            id='t003c043',
            name='Greek',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c044 = OBSColumn(
            id='t003c044',
            name='Gujarati',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c045 = OBSColumn(
            id='t003c045',
            name='Hakka',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c046 = OBSColumn(
            id='t003c046',
            name='Hebrew',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c047 = OBSColumn(
            id='t003c047',
            name='Hindi',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c048 = OBSColumn(
            id='t003c048',
            name='Hungarian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c049 = OBSColumn(
            id='t003c049',
            name='Ilocano',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c050 = OBSColumn(
            id='t003c050',
            name='Indo-Iranian languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c051 = OBSColumn(
            id='t003c051',
            name='Italian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c052 = OBSColumn(
            id='t003c052',
            name='Japanese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c053 = OBSColumn(
            id='t003c053',
            name='Khmer (Cambodian)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c054 = OBSColumn(
            id='t003c054',
            name='Korean',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c055 = OBSColumn(
            id='t003c055',
            name='Kurdish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c056 = OBSColumn(
            id='t003c056',
            name='Lao',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c057 = OBSColumn(
            id='t003c057',
            name='Latvian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c058 = OBSColumn(
            id='t003c058',
            name='Lingala',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c059 = OBSColumn(
            id='t003c059',
            name='Lithuanian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c060 = OBSColumn(
            id='t003c060',
            name='Macedonian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c061 = OBSColumn(
            id='t003c061',
            name='Malay',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c062 = OBSColumn(
            id='t003c062',
            name='Malayalam',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c063 = OBSColumn(
            id='t003c063',
            name='Maltese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c064 = OBSColumn(
            id='t003c064',
            name='Mandarin',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c065 = OBSColumn(
            id='t003c065',
            name='Marathi',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c066 = OBSColumn(
            id='t003c066',
            name='Nepali',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c067 = OBSColumn(
            id='t003c067',
            name='Niger-Congo languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c068 = OBSColumn(
            id='t003c068',
            name='Norwegian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c069 = OBSColumn(
            id='t003c069',
            name='Oromo',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c070 = OBSColumn(
            id='t003c070',
            name='Panjabi (Punjabi)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c071 = OBSColumn(
            id='t003c071',
            name='Pashto',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c072 = OBSColumn(
            id='t003c072',
            name='Persian (Farsi)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c073 = OBSColumn(
            id='t003c073',
            name='Polish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c074 = OBSColumn(
            id='t003c074',
            name='Portuguese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c075 = OBSColumn(
            id='t003c075',
            name='Romanian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c076 = OBSColumn(
            id='t003c076',
            name='Rundi (Kirundi)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c077 = OBSColumn(
            id='t003c077',
            name='Russian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c078 = OBSColumn(
            id='t003c078',
            name='Rwanda (Kinyarwanda)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c079 = OBSColumn(
            id='t003c079',
            name='Semitic languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c080 = OBSColumn(
            id='t003c080',
            name='Serbian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c081 = OBSColumn(
            id='t003c081',
            name='Serbo-Croatian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c082 = OBSColumn(
            id='t003c082',
            name='Shanghainese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c083 = OBSColumn(
            id='t003c083',
            name='Sign languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c084 = OBSColumn(
            id='t003c084',
            name='Sindhi',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c085 = OBSColumn(
            id='t003c085',
            name='Sinhala (Sinhalese)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c086 = OBSColumn(
            id='t003c086',
            name='Sino-Tibetan languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c087 = OBSColumn(
            id='t003c087',
            name='Slavic languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c088 = OBSColumn(
            id='t003c088',
            name='Slovak',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c089 = OBSColumn(
            id='t003c089',
            name='Slovenian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c090 = OBSColumn(
            id='t003c090',
            name='Somali',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c091 = OBSColumn(
            id='t003c091',
            name='Spanish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c092 = OBSColumn(
            id='t003c092',
            name='Swahili',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c093 = OBSColumn(
            id='t003c093',
            name='Swedish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c094 = OBSColumn(
            id='t003c094',
            name='Tagalog (Pilipino, Filipino)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c095 = OBSColumn(
            id='t003c095',
            name='Taiwanese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c096 = OBSColumn(
            id='t003c096',
            name='Tamil',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c097 = OBSColumn(
            id='t003c097',
            name='Telugu',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c098 = OBSColumn(
            id='t003c098',
            name='Thai',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c099 = OBSColumn(
            id='t003c099',
            name='Tibetan languages',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c100 = OBSColumn(
            id='t003c100',
            name='Tigrigna',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c101 = OBSColumn(
            id='t003c101',
            name='Turkish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c102 = OBSColumn(
            id='t003c102',
            name='Ukrainian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c103 = OBSColumn(
            id='t003c103',
            name='Urdu',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c104 = OBSColumn(
            id='t003c104',
            name='Vietnamese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c105 = OBSColumn(
            id='t003c105',
            name='Yiddish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c016: DENOMINATOR },)

        t003c106 = OBSColumn(
            id='t003c106',
            name='Other languages',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c005: DENOMINATOR },)

        t003c107 = OBSColumn(
            id='t003c107',
            name='Multiple responses',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c001: DENOMINATOR },)

        t003c108 = OBSColumn(
            id='t003c108',
            name='English and French',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c107: DENOMINATOR },)

        t003c109 = OBSColumn(
            id='t003c109',
            name='English and non-official language',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c107: DENOMINATOR },)

        t003c110 = OBSColumn(
            id='t003c110',
            name='French and non-official language',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c107: DENOMINATOR },)

        t003c111 = OBSColumn(
            id='t003c111',
            name='English, French and non-official language',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t003c107: DENOMINATOR },)

        t004c001 = OBSColumn(
            id='t004c001',
            name='Detailed other language spoken regularly at home - Total population excluding institutional residents',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t004c002 = OBSColumn(
            id='t004c002',
            name='None',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c001: DENOMINATOR },)

        t004c003 = OBSColumn(
            id='t004c003',
            name='Single responses',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c001: DENOMINATOR },)

        t004c004 = OBSColumn(
            id='t004c004',
            name='English',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c003: DENOMINATOR },)

        t004c005 = OBSColumn(
            id='t004c005',
            name='French',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c003: DENOMINATOR },)

        t004c006 = OBSColumn(
            id='t004c006',
            name='Non-official languages',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c003: DENOMINATOR },)

        t004c007 = OBSColumn(
            id='t004c007',
            name='Selected Aboriginal languages',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c006: DENOMINATOR },)

        t004c008 = OBSColumn(
            id='t004c008',
            name='Atikamekw',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c007: DENOMINATOR },)

        t004c009 = OBSColumn(
            id='t004c009',
            name='Cree, n.o.s.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c007: DENOMINATOR },)

        t004c010 = OBSColumn(
            id='t004c010',
            name='Dene',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c007: DENOMINATOR },)

        t004c011 = OBSColumn(
            id='t004c011',
            name='Innu/Montagnais',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c007: DENOMINATOR },)

        t004c012 = OBSColumn(
            id='t004c012',
            name='Inuktitut',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c007: DENOMINATOR },)

        t004c013 = OBSColumn(
            id='t004c013',
            name='Mi\'kmaq',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c007: DENOMINATOR },)

        t004c014 = OBSColumn(
            id='t004c014',
            name='Ojibway',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c007: DENOMINATOR },)

        t004c015 = OBSColumn(
            id='t004c015',
            name='Oji-Cree',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c007: DENOMINATOR },)

        t004c016 = OBSColumn(
            id='t004c016',
            name='Stoney',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c007: DENOMINATOR },)

        t004c017 = OBSColumn(
            id='t004c017',
            name='Selected non-Aboriginal languages',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c006: DENOMINATOR },)

        t004c018 = OBSColumn(
            id='t004c018',
            name='African languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c019 = OBSColumn(
            id='t004c019',
            name='Afrikaans',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c020 = OBSColumn(
            id='t004c020',
            name='Akan (Twi)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c021 = OBSColumn(
            id='t004c021',
            name='Albanian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c022 = OBSColumn(
            id='t004c022',
            name='Amharic',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c023 = OBSColumn(
            id='t004c023',
            name='Arabic',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c024 = OBSColumn(
            id='t004c024',
            name='Armenian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c025 = OBSColumn(
            id='t004c025',
            name='Bantu languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c026 = OBSColumn(
            id='t004c026',
            name='Bengali',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c027 = OBSColumn(
            id='t004c027',
            name='Berber languages (Kabyle)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c028 = OBSColumn(
            id='t004c028',
            name='Bisayan languages',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c029 = OBSColumn(
            id='t004c029',
            name='Bosnian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c030 = OBSColumn(
            id='t004c030',
            name='Bulgarian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c031 = OBSColumn(
            id='t004c031',
            name='Burmese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c032 = OBSColumn(
            id='t004c032',
            name='Cantonese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c033 = OBSColumn(
            id='t004c033',
            name='Chinese, n.o.s.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c034 = OBSColumn(
            id='t004c034',
            name='Creoles',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c035 = OBSColumn(
            id='t004c035',
            name='Croatian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c036 = OBSColumn(
            id='t004c036',
            name='Czech',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c037 = OBSColumn(
            id='t004c037',
            name='Danish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c038 = OBSColumn(
            id='t004c038',
            name='Dutch',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c039 = OBSColumn(
            id='t004c039',
            name='Estonian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c040 = OBSColumn(
            id='t004c040',
            name='Finnish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c041 = OBSColumn(
            id='t004c041',
            name='Flemish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c042 = OBSColumn(
            id='t004c042',
            name='Fukien',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c043 = OBSColumn(
            id='t004c043',
            name='German',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c044 = OBSColumn(
            id='t004c044',
            name='Greek',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c045 = OBSColumn(
            id='t004c045',
            name='Gujarati',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c046 = OBSColumn(
            id='t004c046',
            name='Hakka',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c047 = OBSColumn(
            id='t004c047',
            name='Hebrew',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c048 = OBSColumn(
            id='t004c048',
            name='Hindi',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c049 = OBSColumn(
            id='t004c049',
            name='Hungarian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c050 = OBSColumn(
            id='t004c050',
            name='Ilocano',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c051 = OBSColumn(
            id='t004c051',
            name='Indo-Iranian languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c052 = OBSColumn(
            id='t004c052',
            name='Italian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c053 = OBSColumn(
            id='t004c053',
            name='Japanese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c054 = OBSColumn(
            id='t004c054',
            name='Khmer (Cambodian)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c055 = OBSColumn(
            id='t004c055',
            name='Korean',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c056 = OBSColumn(
            id='t004c056',
            name='Kurdish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c057 = OBSColumn(
            id='t004c057',
            name='Lao',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c058 = OBSColumn(
            id='t004c058',
            name='Latvian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c059 = OBSColumn(
            id='t004c059',
            name='Lingala',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c060 = OBSColumn(
            id='t004c060',
            name='Lithuanian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c061 = OBSColumn(
            id='t004c061',
            name='Macedonian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c062 = OBSColumn(
            id='t004c062',
            name='Malay',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c063 = OBSColumn(
            id='t004c063',
            name='Malayalam',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c064 = OBSColumn(
            id='t004c064',
            name='Maltese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c065 = OBSColumn(
            id='t004c065',
            name='Mandarin',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c066 = OBSColumn(
            id='t004c066',
            name='Marathi',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c067 = OBSColumn(
            id='t004c067',
            name='Nepali',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c068 = OBSColumn(
            id='t004c068',
            name='Niger-Congo languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c069 = OBSColumn(
            id='t004c069',
            name='Norwegian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c070 = OBSColumn(
            id='t004c070',
            name='Oromo',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c071 = OBSColumn(
            id='t004c071',
            name='Panjabi (Punjabi)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c072 = OBSColumn(
            id='t004c072',
            name='Pashto',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c073 = OBSColumn(
            id='t004c073',
            name='Persian (Farsi)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c074 = OBSColumn(
            id='t004c074',
            name='Polish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c075 = OBSColumn(
            id='t004c075',
            name='Portuguese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c076 = OBSColumn(
            id='t004c076',
            name='Romanian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c077 = OBSColumn(
            id='t004c077',
            name='Rundi (Kirundi)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c078 = OBSColumn(
            id='t004c078',
            name='Russian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c079 = OBSColumn(
            id='t004c079',
            name='Rwanda (Kinyarwanda)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c080 = OBSColumn(
            id='t004c080',
            name='Semitic languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c081 = OBSColumn(
            id='t004c081',
            name='Serbian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c082 = OBSColumn(
            id='t004c082',
            name='Serbo-Croatian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c083 = OBSColumn(
            id='t004c083',
            name='Shanghainese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c084 = OBSColumn(
            id='t004c084',
            name='Sign languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c085 = OBSColumn(
            id='t004c085',
            name='Sindhi',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c086 = OBSColumn(
            id='t004c086',
            name='Sinhala (Sinhalese)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c087 = OBSColumn(
            id='t004c087',
            name='Sino-Tibetan languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c088 = OBSColumn(
            id='t004c088',
            name='Slavic languages, n.i.e.',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c089 = OBSColumn(
            id='t004c089',
            name='Slovak',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c090 = OBSColumn(
            id='t004c090',
            name='Slovenian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c091 = OBSColumn(
            id='t004c091',
            name='Somali',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c092 = OBSColumn(
            id='t004c092',
            name='Spanish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c093 = OBSColumn(
            id='t004c093',
            name='Swahili',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c094 = OBSColumn(
            id='t004c094',
            name='Swedish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c095 = OBSColumn(
            id='t004c095',
            name='Tagalog (Pilipino, Filipino)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c096 = OBSColumn(
            id='t004c096',
            name='Taiwanese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c097 = OBSColumn(
            id='t004c097',
            name='Tamil',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c098 = OBSColumn(
            id='t004c098',
            name='Telugu',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c099 = OBSColumn(
            id='t004c099',
            name='Thai',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c100 = OBSColumn(
            id='t004c100',
            name='Tibetan languages',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c101 = OBSColumn(
            id='t004c101',
            name='Tigrigna',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c102 = OBSColumn(
            id='t004c102',
            name='Turkish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c103 = OBSColumn(
            id='t004c103',
            name='Ukrainian',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c104 = OBSColumn(
            id='t004c104',
            name='Urdu',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c105 = OBSColumn(
            id='t004c105',
            name='Vietnamese',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c106 = OBSColumn(
            id='t004c106',
            name='Yiddish',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c017: DENOMINATOR },)

        t004c107 = OBSColumn(
            id='t004c107',
            name='Other languages',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c006: DENOMINATOR },)

        t004c108 = OBSColumn(
            id='t004c108',
            name='Multiple responses',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c001: DENOMINATOR },)

        t004c109 = OBSColumn(
            id='t004c109',
            name='English and French',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c108: DENOMINATOR },)

        t004c110 = OBSColumn(
            id='t004c110',
            name='English and non-official language',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c108: DENOMINATOR },)

        t004c111 = OBSColumn(
            id='t004c111',
            name='French and non-official language',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c108: DENOMINATOR },)

        t004c112 = OBSColumn(
            id='t004c112',
            name='English, French and non-official language',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t004c108: DENOMINATOR },)

        t005c001 = OBSColumn(
            id='t005c001',
            name='Total number of census families in private households',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t005c002 = OBSColumn(
            id='t005c002',
            name='Size of census family: 2 persons',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c001: DENOMINATOR },)

        t005c003 = OBSColumn(
            id='t005c003',
            name='Size of census family: 3 persons',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c001: DENOMINATOR },)

        t005c004 = OBSColumn(
            id='t005c004',
            name='Size of census family: 4 persons',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c001: DENOMINATOR },)

        t005c005 = OBSColumn(
            id='t005c005',
            name='Size of census family: 5 or more persons',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c001: DENOMINATOR },)

        t005c006 = OBSColumn(
            id='t005c006',
            name='Total number of census families in private households',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t005c007 = OBSColumn(
            id='t005c007',
            name='Total couple families by family structure and number of children',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c006: DENOMINATOR },)

        t005c008 = OBSColumn(
            id='t005c008',
            name='Married couples',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c007: DENOMINATOR },)

        t005c009 = OBSColumn(
            id='t005c009',
            name='Without children at home',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c008: DENOMINATOR },)

        t005c010 = OBSColumn(
            id='t005c010',
            name='With children at home',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c008: DENOMINATOR },)

        t005c011 = OBSColumn(
            id='t005c011',
            name='1 child',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c010: DENOMINATOR },)

        t005c012 = OBSColumn(
            id='t005c012',
            name='2 children',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c010: DENOMINATOR },)

        t005c013 = OBSColumn(
            id='t005c013',
            name='3 or more children',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c010: DENOMINATOR },)

        t005c014 = OBSColumn(
            id='t005c014',
            name='Common-law couples',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c007: DENOMINATOR },)

        t005c015 = OBSColumn(
            id='t005c015',
            name='Without children at home',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c014: DENOMINATOR },)

        t005c016 = OBSColumn(
            id='t005c016',
            name='With children at home',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c014: DENOMINATOR },)

        t005c017 = OBSColumn(
            id='t005c017',
            name='1 child',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c016: DENOMINATOR },)

        t005c018 = OBSColumn(
            id='t005c018',
            name='2 children',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c016: DENOMINATOR },)

        t005c019 = OBSColumn(
            id='t005c019',
            name='3 or more children',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c016: DENOMINATOR },)

        t005c020 = OBSColumn(
            id='t005c020',
            name='Total lone-parent families by sex of parent and number of children',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c006: DENOMINATOR },)

        t005c021 = OBSColumn(
            id='t005c021',
            name='Female parent',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c020: DENOMINATOR },)

        t005c022 = OBSColumn(
            id='t005c022',
            name='1 child',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c021: DENOMINATOR },)

        t005c023 = OBSColumn(
            id='t005c023',
            name='2 children',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c021: DENOMINATOR },)

        t005c024 = OBSColumn(
            id='t005c024',
            name='3 or more children',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c021: DENOMINATOR },)

        t005c025 = OBSColumn(
            id='t005c025',
            name='Male parent',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c020: DENOMINATOR },)

        t005c026 = OBSColumn(
            id='t005c026',
            name='1 child',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c025: DENOMINATOR },)

        t005c027 = OBSColumn(
            id='t005c027',
            name='2 children',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c025: DENOMINATOR },)

        t005c028 = OBSColumn(
            id='t005c028',
            name='3 or more children',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c025: DENOMINATOR },)

        t005c029 = OBSColumn(
            id='t005c029',
            name='Total children in census families in private households',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t005c030 = OBSColumn(
            id='t005c030',
            name='Under six years of age',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c029: DENOMINATOR },)

        t005c031 = OBSColumn(
            id='t005c031',
            name='6 to 14 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c029: DENOMINATOR },)

        t005c032 = OBSColumn(
            id='t005c032',
            name='15 to 17 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c029: DENOMINATOR },)

        t005c033 = OBSColumn(
            id='t005c033',
            name='18 to 24 years',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c029: DENOMINATOR },)

        t005c034 = OBSColumn(
            id='t005c034',
            name='25 years and over',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t005c029: DENOMINATOR },)

        t005c035 = OBSColumn(
            id='t005c035',
            name='Average number of children at home per census family',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t006c001 = OBSColumn(
            id='t006c001',
            name='First official language spoken - Total population excluding institutional residents',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t006c002 = OBSColumn(
            id='t006c002',
            name='English',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t006c001: DENOMINATOR },)

        t006c003 = OBSColumn(
            id='t006c003',
            name='French',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t006c001: DENOMINATOR },)

        t006c004 = OBSColumn(
            id='t006c004',
            name='English and French',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t006c001: DENOMINATOR },)

        t006c005 = OBSColumn(
            id='t006c005',
            name='Neither English nor French',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t006c001: DENOMINATOR },)

        t006c006 = OBSColumn(
            id='t006c006',
            name='Official language minority (number)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t006c007 = OBSColumn(
            id='t006c007',
            name='Official language minority (percentage)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t007c001 = OBSColumn(
            id='t007c001',
            name='Total number of persons in private households',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t007c002 = OBSColumn(
            id='t007c002',
            name='Number of persons not in census families',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c001: DENOMINATOR },)

        t007c003 = OBSColumn(
            id='t007c003',
            name='Living with relatives',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c002: DENOMINATOR },)

        t007c004 = OBSColumn(
            id='t007c004',
            name='Living with non-relatives only',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c002: DENOMINATOR },)

        t007c005 = OBSColumn(
            id='t007c005',
            name='Living alone',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c002: DENOMINATOR },)

        t007c006 = OBSColumn(
            id='t007c006',
            name='Number of census family persons',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c001: DENOMINATOR },)

        t007c007 = OBSColumn(
            id='t007c007',
            name='Average number of persons per census family',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t007c008 = OBSColumn(
            id='t007c008',
            name='Total number of persons aged 65 years and over in private households',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t007c009 = OBSColumn(
            id='t007c009',
            name='Number of persons not in census families aged 65 years and over',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c008: DENOMINATOR },)

        t007c010 = OBSColumn(
            id='t007c010',
            name='Living with relatives',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c009: DENOMINATOR },)

        t007c011 = OBSColumn(
            id='t007c011',
            name='Living with non-relatives only',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c009: DENOMINATOR },)

        t007c012 = OBSColumn(
            id='t007c012',
            name='Living alone',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c009: DENOMINATOR },)

        t007c013 = OBSColumn(
            id='t007c013',
            name='Number of census family persons aged 65 years and over',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c008: DENOMINATOR },)

        t007c014 = OBSColumn(
            id='t007c014',
            name='Total number of private households by household type',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t007c015 = OBSColumn(
            id='t007c015',
            name='Census-family households',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c014: DENOMINATOR },)

        t007c016 = OBSColumn(
            id='t007c016',
            name='One-family-only households',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c015: DENOMINATOR },)

        t007c017 = OBSColumn(
            id='t007c017',
            name='Couple-family households',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c016: DENOMINATOR },)

        t007c018 = OBSColumn(
            id='t007c018',
            name='Without children',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c017: DENOMINATOR },)

        t007c019 = OBSColumn(
            id='t007c019',
            name='With children',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c017: DENOMINATOR },)

        t007c020 = OBSColumn(
            id='t007c020',
            name='Lone-parent-family households',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c016: DENOMINATOR },)

        t007c021 = OBSColumn(
            id='t007c021',
            name='Other family households',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c015: DENOMINATOR },)

        t007c022 = OBSColumn(
            id='t007c022',
            name='One-family households with persons not in a census family',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c021: DENOMINATOR },)

        t007c023 = OBSColumn(
            id='t007c023',
            name='Couple-family households',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c022: DENOMINATOR },)

        t007c024 = OBSColumn(
            id='t007c024',
            name='Without children',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c023: DENOMINATOR },)

        t007c025 = OBSColumn(
            id='t007c025',
            name='With children',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c023: DENOMINATOR },)

        t007c026 = OBSColumn(
            id='t007c026',
            name='Lone-parent-family households',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c022: DENOMINATOR },)

        t007c027 = OBSColumn(
            id='t007c027',
            name='Two-or-more-family households',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c021: DENOMINATOR },)

        t007c028 = OBSColumn(
            id='t007c028',
            name='Non-census-family households',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c014: DENOMINATOR },)

        t007c029 = OBSColumn(
            id='t007c029',
            name='One-person households',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c028: DENOMINATOR },)

        t007c030 = OBSColumn(
            id='t007c030',
            name='Two-or-more-person households',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c028: DENOMINATOR },)

        t007c031 = OBSColumn(
            id='t007c031',
            name='Total number of occupied private dwellings by structural type of dwelling',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t007c032 = OBSColumn(
            id='t007c032',
            name='Single-detached house',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c031: DENOMINATOR },)

        t007c033 = OBSColumn(
            id='t007c033',
            name='Apartment, building that has five or more storeys',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c031: DENOMINATOR },)

        t007c034 = OBSColumn(
            id='t007c034',
            name='Movable dwelling',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c031: DENOMINATOR },)

        t007c035 = OBSColumn(
            id='t007c035',
            name='Other dwelling',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c031: DENOMINATOR },)

        t007c036 = OBSColumn(
            id='t007c036',
            name='Semi-detached house',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c035: DENOMINATOR },)

        t007c037 = OBSColumn(
            id='t007c037',
            name='Row house',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c035: DENOMINATOR },)

        t007c038 = OBSColumn(
            id='t007c038',
            name='Apartment, duplex',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c035: DENOMINATOR },)

        t007c039 = OBSColumn(
            id='t007c039',
            name='Apartment, building that has fewer than five storeys',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c035: DENOMINATOR },)

        t007c040 = OBSColumn(
            id='t007c040',
            name='Other single-attached house',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c035: DENOMINATOR },)

        t007c041 = OBSColumn(
            id='t007c041',
            name='Total number of private households by household size',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t007c042 = OBSColumn(
            id='t007c042',
            name='1 person',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c041: DENOMINATOR },)

        t007c043 = OBSColumn(
            id='t007c043',
            name='2 persons',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c041: DENOMINATOR },)

        t007c044 = OBSColumn(
            id='t007c044',
            name='3 persons',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c041: DENOMINATOR },)

        t007c045 = OBSColumn(
            id='t007c045',
            name='4 persons',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c041: DENOMINATOR },)

        t007c046 = OBSColumn(
            id='t007c046',
            name='5 persons',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c041: DENOMINATOR },)

        t007c047 = OBSColumn(
            id='t007c047',
            name='6 or more persons',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t007c041: DENOMINATOR },)

        t007c048 = OBSColumn(
            id='t007c048',
            name='Number of persons in private households',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t007c049 = OBSColumn(
            id='t007c049',
            name='Average number of persons in private households',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t008c001 = OBSColumn(
            id='t008c001',
            name='Knowledge of official languages - Total population excluding institutional residents',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t008c002 = OBSColumn(
            id='t008c002',
            name='English only',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t008c001: DENOMINATOR },)

        t008c003 = OBSColumn(
            id='t008c003',
            name='French only',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t008c001: DENOMINATOR },)

        t008c004 = OBSColumn(
            id='t008c004',
            name='English and French',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t008c001: DENOMINATOR },)

        t008c005 = OBSColumn(
            id='t008c005',
            name='Neither English nor French',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t008c001: DENOMINATOR },)

        t009c001 = OBSColumn(
            id='t009c001',
            name='Total population 15 years and over by marital status',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t009c002 = OBSColumn(
            id='t009c002',
            name='Married or living with a common-law partner',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t009c001: DENOMINATOR },)

        t009c003 = OBSColumn(
            id='t009c003',
            name='Married (and not separated)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t009c002: DENOMINATOR },)

        t009c004 = OBSColumn(
            id='t009c004',
            name='Living common law',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t009c002: DENOMINATOR },)

        t009c005 = OBSColumn(
            id='t009c005',
            name='Not married and not living with a common-law partner',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t009c001: DENOMINATOR },)

        t009c006 = OBSColumn(
            id='t009c006',
            name='Single (never legally married)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t009c005: DENOMINATOR },)

        t009c007 = OBSColumn(
            id='t009c007',
            name='Separated',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t009c005: DENOMINATOR },)

        t009c008 = OBSColumn(
            id='t009c008',
            name='Divorced',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t009c005: DENOMINATOR },)

        t009c009 = OBSColumn(
            id='t009c009',
            name='Widowed',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t009c005: DENOMINATOR },)

        t010c001 = OBSColumn(
            id='t010c001',
            name='Population in 2011',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t010c002 = OBSColumn(
            id='t010c002',
            name='Population in 2006',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t010c003 = OBSColumn(
            id='t010c003',
            name='2006 to 2011 population change (%)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t010c004 = OBSColumn(
            id='t010c004',
            name='Total private dwellings',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t010c005 = OBSColumn(
            id='t010c005',
            name='Private dwellings occupied by usual residents',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t010c006 = OBSColumn(
            id='t010c006',
            name='Population density per square kilometre',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t010c007 = OBSColumn(
            id='t010c007',
            name='Land area (square km)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

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
            ('t001c018', t001c018),
            ('t001c019', t001c019),
            ('t001c020', t001c020),
            ('t001c021', t001c021),
            ('t001c022', t001c022),
            ('t001c023', t001c023),
            ('t001c024', t001c024),
            ('t001c025', t001c025),
            ('t001c026', t001c026),
            ('t002c001', t002c001),
            ('t002c002', t002c002),
            ('t002c003', t002c003),
            ('t002c004', t002c004),
            ('t002c005', t002c005),
            ('t002c006', t002c006),
            ('t002c007', t002c007),
            ('t002c008', t002c008),
            ('t002c009', t002c009),
            ('t002c010', t002c010),
            ('t002c011', t002c011),
            ('t002c012', t002c012),
            ('t002c013', t002c013),
            ('t002c014', t002c014),
            ('t002c015', t002c015),
            ('t002c016', t002c016),
            ('t002c017', t002c017),
            ('t002c018', t002c018),
            ('t002c019', t002c019),
            ('t002c020', t002c020),
            ('t002c021', t002c021),
            ('t002c022', t002c022),
            ('t002c023', t002c023),
            ('t002c024', t002c024),
            ('t002c025', t002c025),
            ('t002c026', t002c026),
            ('t002c027', t002c027),
            ('t002c028', t002c028),
            ('t002c029', t002c029),
            ('t002c030', t002c030),
            ('t002c031', t002c031),
            ('t002c032', t002c032),
            ('t002c033', t002c033),
            ('t002c034', t002c034),
            ('t002c035', t002c035),
            ('t002c036', t002c036),
            ('t002c037', t002c037),
            ('t002c038', t002c038),
            ('t002c039', t002c039),
            ('t002c040', t002c040),
            ('t002c041', t002c041),
            ('t002c042', t002c042),
            ('t002c043', t002c043),
            ('t002c044', t002c044),
            ('t002c045', t002c045),
            ('t002c046', t002c046),
            ('t002c047', t002c047),
            ('t002c048', t002c048),
            ('t002c049', t002c049),
            ('t002c050', t002c050),
            ('t002c051', t002c051),
            ('t002c052', t002c052),
            ('t002c053', t002c053),
            ('t002c054', t002c054),
            ('t002c055', t002c055),
            ('t002c056', t002c056),
            ('t002c057', t002c057),
            ('t002c058', t002c058),
            ('t002c059', t002c059),
            ('t002c060', t002c060),
            ('t002c061', t002c061),
            ('t002c062', t002c062),
            ('t002c063', t002c063),
            ('t002c064', t002c064),
            ('t002c065', t002c065),
            ('t002c066', t002c066),
            ('t002c067', t002c067),
            ('t002c068', t002c068),
            ('t002c069', t002c069),
            ('t002c070', t002c070),
            ('t002c071', t002c071),
            ('t002c072', t002c072),
            ('t002c073', t002c073),
            ('t002c074', t002c074),
            ('t002c075', t002c075),
            ('t002c076', t002c076),
            ('t002c077', t002c077),
            ('t002c078', t002c078),
            ('t002c079', t002c079),
            ('t002c080', t002c080),
            ('t002c081', t002c081),
            ('t002c082', t002c082),
            ('t002c083', t002c083),
            ('t002c084', t002c084),
            ('t002c085', t002c085),
            ('t002c086', t002c086),
            ('t002c087', t002c087),
            ('t002c088', t002c088),
            ('t002c089', t002c089),
            ('t002c090', t002c090),
            ('t002c091', t002c091),
            ('t002c092', t002c092),
            ('t002c093', t002c093),
            ('t002c094', t002c094),
            ('t002c095', t002c095),
            ('t002c096', t002c096),
            ('t002c097', t002c097),
            ('t002c098', t002c098),
            ('t002c099', t002c099),
            ('t002c100', t002c100),
            ('t002c101', t002c101),
            ('t002c102', t002c102),
            ('t002c103', t002c103),
            ('t002c104', t002c104),
            ('t002c105', t002c105),
            ('t002c106', t002c106),
            ('t002c107', t002c107),
            ('t002c108', t002c108),
            ('t002c109', t002c109),
            ('t002c110', t002c110),
            ('t002c111', t002c111),
            ('t003c001', t003c001),
            ('t003c002', t003c002),
            ('t003c003', t003c003),
            ('t003c004', t003c004),
            ('t003c005', t003c005),
            ('t003c006', t003c006),
            ('t003c007', t003c007),
            ('t003c008', t003c008),
            ('t003c009', t003c009),
            ('t003c010', t003c010),
            ('t003c011', t003c011),
            ('t003c012', t003c012),
            ('t003c013', t003c013),
            ('t003c014', t003c014),
            ('t003c015', t003c015),
            ('t003c016', t003c016),
            ('t003c017', t003c017),
            ('t003c018', t003c018),
            ('t003c019', t003c019),
            ('t003c020', t003c020),
            ('t003c021', t003c021),
            ('t003c022', t003c022),
            ('t003c023', t003c023),
            ('t003c024', t003c024),
            ('t003c025', t003c025),
            ('t003c026', t003c026),
            ('t003c027', t003c027),
            ('t003c028', t003c028),
            ('t003c029', t003c029),
            ('t003c030', t003c030),
            ('t003c031', t003c031),
            ('t003c032', t003c032),
            ('t003c033', t003c033),
            ('t003c034', t003c034),
            ('t003c035', t003c035),
            ('t003c036', t003c036),
            ('t003c037', t003c037),
            ('t003c038', t003c038),
            ('t003c039', t003c039),
            ('t003c040', t003c040),
            ('t003c041', t003c041),
            ('t003c042', t003c042),
            ('t003c043', t003c043),
            ('t003c044', t003c044),
            ('t003c045', t003c045),
            ('t003c046', t003c046),
            ('t003c047', t003c047),
            ('t003c048', t003c048),
            ('t003c049', t003c049),
            ('t003c050', t003c050),
            ('t003c051', t003c051),
            ('t003c052', t003c052),
            ('t003c053', t003c053),
            ('t003c054', t003c054),
            ('t003c055', t003c055),
            ('t003c056', t003c056),
            ('t003c057', t003c057),
            ('t003c058', t003c058),
            ('t003c059', t003c059),
            ('t003c060', t003c060),
            ('t003c061', t003c061),
            ('t003c062', t003c062),
            ('t003c063', t003c063),
            ('t003c064', t003c064),
            ('t003c065', t003c065),
            ('t003c066', t003c066),
            ('t003c067', t003c067),
            ('t003c068', t003c068),
            ('t003c069', t003c069),
            ('t003c070', t003c070),
            ('t003c071', t003c071),
            ('t003c072', t003c072),
            ('t003c073', t003c073),
            ('t003c074', t003c074),
            ('t003c075', t003c075),
            ('t003c076', t003c076),
            ('t003c077', t003c077),
            ('t003c078', t003c078),
            ('t003c079', t003c079),
            ('t003c080', t003c080),
            ('t003c081', t003c081),
            ('t003c082', t003c082),
            ('t003c083', t003c083),
            ('t003c084', t003c084),
            ('t003c085', t003c085),
            ('t003c086', t003c086),
            ('t003c087', t003c087),
            ('t003c088', t003c088),
            ('t003c089', t003c089),
            ('t003c090', t003c090),
            ('t003c091', t003c091),
            ('t003c092', t003c092),
            ('t003c093', t003c093),
            ('t003c094', t003c094),
            ('t003c095', t003c095),
            ('t003c096', t003c096),
            ('t003c097', t003c097),
            ('t003c098', t003c098),
            ('t003c099', t003c099),
            ('t003c100', t003c100),
            ('t003c101', t003c101),
            ('t003c102', t003c102),
            ('t003c103', t003c103),
            ('t003c104', t003c104),
            ('t003c105', t003c105),
            ('t003c106', t003c106),
            ('t003c107', t003c107),
            ('t003c108', t003c108),
            ('t003c109', t003c109),
            ('t003c110', t003c110),
            ('t003c111', t003c111),
            ('t004c001', t004c001),
            ('t004c002', t004c002),
            ('t004c003', t004c003),
            ('t004c004', t004c004),
            ('t004c005', t004c005),
            ('t004c006', t004c006),
            ('t004c007', t004c007),
            ('t004c008', t004c008),
            ('t004c009', t004c009),
            ('t004c010', t004c010),
            ('t004c011', t004c011),
            ('t004c012', t004c012),
            ('t004c013', t004c013),
            ('t004c014', t004c014),
            ('t004c015', t004c015),
            ('t004c016', t004c016),
            ('t004c017', t004c017),
            ('t004c018', t004c018),
            ('t004c019', t004c019),
            ('t004c020', t004c020),
            ('t004c021', t004c021),
            ('t004c022', t004c022),
            ('t004c023', t004c023),
            ('t004c024', t004c024),
            ('t004c025', t004c025),
            ('t004c026', t004c026),
            ('t004c027', t004c027),
            ('t004c028', t004c028),
            ('t004c029', t004c029),
            ('t004c030', t004c030),
            ('t004c031', t004c031),
            ('t004c032', t004c032),
            ('t004c033', t004c033),
            ('t004c034', t004c034),
            ('t004c035', t004c035),
            ('t004c036', t004c036),
            ('t004c037', t004c037),
            ('t004c038', t004c038),
            ('t004c039', t004c039),
            ('t004c040', t004c040),
            ('t004c041', t004c041),
            ('t004c042', t004c042),
            ('t004c043', t004c043),
            ('t004c044', t004c044),
            ('t004c045', t004c045),
            ('t004c046', t004c046),
            ('t004c047', t004c047),
            ('t004c048', t004c048),
            ('t004c049', t004c049),
            ('t004c050', t004c050),
            ('t004c051', t004c051),
            ('t004c052', t004c052),
            ('t004c053', t004c053),
            ('t004c054', t004c054),
            ('t004c055', t004c055),
            ('t004c056', t004c056),
            ('t004c057', t004c057),
            ('t004c058', t004c058),
            ('t004c059', t004c059),
            ('t004c060', t004c060),
            ('t004c061', t004c061),
            ('t004c062', t004c062),
            ('t004c063', t004c063),
            ('t004c064', t004c064),
            ('t004c065', t004c065),
            ('t004c066', t004c066),
            ('t004c067', t004c067),
            ('t004c068', t004c068),
            ('t004c069', t004c069),
            ('t004c070', t004c070),
            ('t004c071', t004c071),
            ('t004c072', t004c072),
            ('t004c073', t004c073),
            ('t004c074', t004c074),
            ('t004c075', t004c075),
            ('t004c076', t004c076),
            ('t004c077', t004c077),
            ('t004c078', t004c078),
            ('t004c079', t004c079),
            ('t004c080', t004c080),
            ('t004c081', t004c081),
            ('t004c082', t004c082),
            ('t004c083', t004c083),
            ('t004c084', t004c084),
            ('t004c085', t004c085),
            ('t004c086', t004c086),
            ('t004c087', t004c087),
            ('t004c088', t004c088),
            ('t004c089', t004c089),
            ('t004c090', t004c090),
            ('t004c091', t004c091),
            ('t004c092', t004c092),
            ('t004c093', t004c093),
            ('t004c094', t004c094),
            ('t004c095', t004c095),
            ('t004c096', t004c096),
            ('t004c097', t004c097),
            ('t004c098', t004c098),
            ('t004c099', t004c099),
            ('t004c100', t004c100),
            ('t004c101', t004c101),
            ('t004c102', t004c102),
            ('t004c103', t004c103),
            ('t004c104', t004c104),
            ('t004c105', t004c105),
            ('t004c106', t004c106),
            ('t004c107', t004c107),
            ('t004c108', t004c108),
            ('t004c109', t004c109),
            ('t004c110', t004c110),
            ('t004c111', t004c111),
            ('t004c112', t004c112),
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
            ('t006c001', t006c001),
            ('t006c002', t006c002),
            ('t006c003', t006c003),
            ('t006c004', t006c004),
            ('t006c005', t006c005),
            ('t006c006', t006c006),
            ('t006c007', t006c007),
            ('t007c001', t007c001),
            ('t007c002', t007c002),
            ('t007c003', t007c003),
            ('t007c004', t007c004),
            ('t007c005', t007c005),
            ('t007c006', t007c006),
            ('t007c007', t007c007),
            ('t007c008', t007c008),
            ('t007c009', t007c009),
            ('t007c010', t007c010),
            ('t007c011', t007c011),
            ('t007c012', t007c012),
            ('t007c013', t007c013),
            ('t007c014', t007c014),
            ('t007c015', t007c015),
            ('t007c016', t007c016),
            ('t007c017', t007c017),
            ('t007c018', t007c018),
            ('t007c019', t007c019),
            ('t007c020', t007c020),
            ('t007c021', t007c021),
            ('t007c022', t007c022),
            ('t007c023', t007c023),
            ('t007c024', t007c024),
            ('t007c025', t007c025),
            ('t007c026', t007c026),
            ('t007c027', t007c027),
            ('t007c028', t007c028),
            ('t007c029', t007c029),
            ('t007c030', t007c030),
            ('t007c031', t007c031),
            ('t007c032', t007c032),
            ('t007c033', t007c033),
            ('t007c034', t007c034),
            ('t007c035', t007c035),
            ('t007c036', t007c036),
            ('t007c037', t007c037),
            ('t007c038', t007c038),
            ('t007c039', t007c039),
            ('t007c040', t007c040),
            ('t007c041', t007c041),
            ('t007c042', t007c042),
            ('t007c043', t007c043),
            ('t007c044', t007c044),
            ('t007c045', t007c045),
            ('t007c046', t007c046),
            ('t007c047', t007c047),
            ('t007c048', t007c048),
            ('t007c049', t007c049),
            ('t008c001', t008c001),
            ('t008c002', t008c002),
            ('t008c003', t008c003),
            ('t008c004', t008c004),
            ('t008c005', t008c005),
            ('t009c001', t009c001),
            ('t009c002', t009c002),
            ('t009c003', t009c003),
            ('t009c004', t009c004),
            ('t009c005', t009c005),
            ('t009c006', t009c006),
            ('t009c007', t009c007),
            ('t009c008', t009c008),
            ('t009c009', t009c009),
            ('t010c001', t010c001),
            ('t010c002', t010c002),
            ('t010c003', t010c003),
            ('t010c004', t010c004),
            ('t010c005', t010c005),
            ('t010c006', t010c006),
            ('t010c007', t010c007),
        ])

