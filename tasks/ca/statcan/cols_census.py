from tasks.meta import OBSColumn, DENOMINATOR, UNIVERSE
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
        return 3

    def columns(self):
        input_ = self.input()

        subsections = input_['subsections']

        unit_people = input_['units']['people']
        unit_housing = input_['units']['housing_units']
        unit_households = input_['units']['households']
        unit_years = input_['units']['years']
        unit_ratio = input_['units']['ratio']
        unit_education = input_['units']['education_level']

        ca = input_['sections']['ca']

        t001c001_t = OBSColumn(
            id='t001c001_t',
            name='Total population (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t001c001_m = OBSColumn(
            id='t001c001_m',
            name='Total population (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t001c001_f = OBSColumn(
            id='t001c001_f',
            name='Total population (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t001c002_t = OBSColumn(
            id='t001c002_t',
            name='Population - 0 to 4 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c002_m = OBSColumn(
            id='t001c002_m',
            name='Population - 0 to 4 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c002_f = OBSColumn(
            id='t001c002_f',
            name='Population - 0 to 4 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c003_t = OBSColumn(
            id='t001c003_t',
            name='Population - 5 to 9 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c003_m = OBSColumn(
            id='t001c003_m',
            name='Population - 5 to 9 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c003_f = OBSColumn(
            id='t001c003_f',
            name='Population - 5 to 9 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c004_t = OBSColumn(
            id='t001c004_t',
            name='Population - 10 to 14 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c004_m = OBSColumn(
            id='t001c004_m',
            name='Population - 10 to 14 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c004_f = OBSColumn(
            id='t001c004_f',
            name='Population - 10 to 14 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c005_t = OBSColumn(
            id='t001c005_t',
            name='Population - 15 to 19 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c005_m = OBSColumn(
            id='t001c005_m',
            name='Population - 15 to 19 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c005_f = OBSColumn(
            id='t001c005_f',
            name='Population - 15 to 19 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c006_t = OBSColumn(
            id='t001c006_t',
            name='Population - 15 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c005_t: DENOMINATOR },)

        t001c006_m = OBSColumn(
            id='t001c006_m',
            name='Population - 15 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c005_m: DENOMINATOR },)

        t001c006_f = OBSColumn(
            id='t001c006_f',
            name='Population - 15 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c005_f: DENOMINATOR },)

        t001c007_t = OBSColumn(
            id='t001c007_t',
            name='Population - 16 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c005_t: DENOMINATOR },)

        t001c007_m = OBSColumn(
            id='t001c007_m',
            name='Population - 16 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c005_m: DENOMINATOR },)

        t001c007_f = OBSColumn(
            id='t001c007_f',
            name='Population - 16 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c005_f: DENOMINATOR },)

        t001c008_t = OBSColumn(
            id='t001c008_t',
            name='Population - 17 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c005_t: DENOMINATOR },)

        t001c008_m = OBSColumn(
            id='t001c008_m',
            name='Population - 17 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c005_m: DENOMINATOR },)

        t001c008_f = OBSColumn(
            id='t001c008_f',
            name='Population - 17 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c005_f: DENOMINATOR },)

        t001c009_t = OBSColumn(
            id='t001c009_t',
            name='Population - 18 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c005_t: DENOMINATOR },)

        t001c009_m = OBSColumn(
            id='t001c009_m',
            name='Population - 18 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c005_m: DENOMINATOR },)

        t001c009_f = OBSColumn(
            id='t001c009_f',
            name='Population - 18 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c005_f: DENOMINATOR },)

        t001c010_t = OBSColumn(
            id='t001c010_t',
            name='Population - 19 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c005_t: DENOMINATOR },)

        t001c010_m = OBSColumn(
            id='t001c010_m',
            name='Population - 19 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c005_m: DENOMINATOR },)

        t001c010_f = OBSColumn(
            id='t001c010_f',
            name='Population - 19 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c005_f: DENOMINATOR },)

        t001c011_t = OBSColumn(
            id='t001c011_t',
            name='Population - 20 to 24 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c011_m = OBSColumn(
            id='t001c011_m',
            name='Population - 20 to 24 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c011_f = OBSColumn(
            id='t001c011_f',
            name='Population - 20 to 24 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c012_t = OBSColumn(
            id='t001c012_t',
            name='Population - 25 to 29 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c012_m = OBSColumn(
            id='t001c012_m',
            name='Population - 25 to 29 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c012_f = OBSColumn(
            id='t001c012_f',
            name='Population - 25 to 29 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c013_t = OBSColumn(
            id='t001c013_t',
            name='Population - 30 to 34 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c013_m = OBSColumn(
            id='t001c013_m',
            name='Population - 30 to 34 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c013_f = OBSColumn(
            id='t001c013_f',
            name='Population - 30 to 34 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c014_t = OBSColumn(
            id='t001c014_t',
            name='Population - 35 to 39 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c014_m = OBSColumn(
            id='t001c014_m',
            name='Population - 35 to 39 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c014_f = OBSColumn(
            id='t001c014_f',
            name='Population - 35 to 39 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c015_t = OBSColumn(
            id='t001c015_t',
            name='Population - 40 to 44 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c015_m = OBSColumn(
            id='t001c015_m',
            name='Population - 40 to 44 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c015_f = OBSColumn(
            id='t001c015_f',
            name='Population - 40 to 44 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c016_t = OBSColumn(
            id='t001c016_t',
            name='Population - 45 to 49 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c016_m = OBSColumn(
            id='t001c016_m',
            name='Population - 45 to 49 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c016_f = OBSColumn(
            id='t001c016_f',
            name='Population - 45 to 49 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c017_t = OBSColumn(
            id='t001c017_t',
            name='Population - 50 to 54 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c017_m = OBSColumn(
            id='t001c017_m',
            name='Population - 50 to 54 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c017_f = OBSColumn(
            id='t001c017_f',
            name='Population - 50 to 54 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c018_t = OBSColumn(
            id='t001c018_t',
            name='Population - 55 to 59 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c018_m = OBSColumn(
            id='t001c018_m',
            name='Population - 55 to 59 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c018_f = OBSColumn(
            id='t001c018_f',
            name='Population - 55 to 59 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c019_t = OBSColumn(
            id='t001c019_t',
            name='Population - 60 to 64 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c019_m = OBSColumn(
            id='t001c019_m',
            name='Population - 60 to 64 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c019_f = OBSColumn(
            id='t001c019_f',
            name='Population - 60 to 64 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c020_t = OBSColumn(
            id='t001c020_t',
            name='Population - 65 to 69 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c020_m = OBSColumn(
            id='t001c020_m',
            name='Population - 65 to 69 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c020_f = OBSColumn(
            id='t001c020_f',
            name='Population - 65 to 69 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c021_t = OBSColumn(
            id='t001c021_t',
            name='Population - 70 to 74 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c021_m = OBSColumn(
            id='t001c021_m',
            name='Population - 70 to 74 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c021_f = OBSColumn(
            id='t001c021_f',
            name='Population - 70 to 74 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c022_t = OBSColumn(
            id='t001c022_t',
            name='Population - 75 to 79 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c022_m = OBSColumn(
            id='t001c022_m',
            name='Population - 75 to 79 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c022_f = OBSColumn(
            id='t001c022_f',
            name='Population - 75 to 79 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c023_t = OBSColumn(
            id='t001c023_t',
            name='Population - 80 to 84 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c023_m = OBSColumn(
            id='t001c023_m',
            name='Population - 80 to 84 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c023_f = OBSColumn(
            id='t001c023_f',
            name='Population - 80 to 84 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c024_t = OBSColumn(
            id='t001c024_t',
            name='Population - 85 years and over (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c024_m = OBSColumn(
            id='t001c024_m',
            name='Population - 85 years and over (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c024_f = OBSColumn(
            id='t001c024_f',
            name='Population - 85 years and over (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c025_t = OBSColumn(
            id='t001c025_t',
            name='Median age of the population (total)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_years, subsections['age_gender']],
            targets={},)

        t001c025_m = OBSColumn(
            id='t001c025_m',
            name='Median age of the population (male)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_years, subsections['age_gender']],
            targets={},)

        t001c025_f = OBSColumn(
            id='t001c025_f',
            name='Median age of the population (female)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_years, subsections['age_gender']],
            targets={},)

        t001c026_t = OBSColumn(
            id='t001c026_t',
            name='% of the population aged 15 and over (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['age_gender']],
            targets={},)

        t001c026_m = OBSColumn(
            id='t001c026_m',
            name='% of the population aged 15 and over (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['age_gender']],
            targets={},)

        t001c026_f = OBSColumn(
            id='t001c026_f',
            name='% of the population aged 15 and over (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['age_gender']],
            targets={},)

        t002c001_t = OBSColumn(
            id='t002c001_t',
            name='Total population excluding institutional residents (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t002c001_m = OBSColumn(
            id='t002c001_m',
            name='Total population excluding institutional residents (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t002c001_f = OBSColumn(
            id='t002c001_f',
            name='Total population excluding institutional residents (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['age_gender']],
            targets={},)

        t002c002_t = OBSColumn(
            id='t002c002_t',
            name='Language spoken most often at home - Single responses (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_t: DENOMINATOR },)

        t002c002_m = OBSColumn(
            id='t002c002_m',
            name='Language spoken most often at home - Single responses (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_m: DENOMINATOR },)

        t002c002_f = OBSColumn(
            id='t002c002_f',
            name='Language spoken most often at home - Single responses (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_f: DENOMINATOR },)

        t002c003_t = OBSColumn(
            id='t002c003_t',
            name='English (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c002_t: DENOMINATOR },)

        t002c003_m = OBSColumn(
            id='t002c003_m',
            name='English (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c002_m: DENOMINATOR },)

        t002c003_f = OBSColumn(
            id='t002c003_f',
            name='English (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c002_f: DENOMINATOR },)

        t002c004_t = OBSColumn(
            id='t002c004_t',
            name='French (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c002_t: DENOMINATOR },)

        t002c004_m = OBSColumn(
            id='t002c004_m',
            name='French (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c002_m: DENOMINATOR },)

        t002c004_f = OBSColumn(
            id='t002c004_f',
            name='French (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c002_f: DENOMINATOR },)

        t002c005_t = OBSColumn(
            id='t002c005_t',
            name='Non-official languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c002_t: DENOMINATOR },)

        t002c005_m = OBSColumn(
            id='t002c005_m',
            name='Non-official languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c002_m: DENOMINATOR },)

        t002c005_f = OBSColumn(
            id='t002c005_f',
            name='Non-official languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c002_f: DENOMINATOR },)

        t002c006_t = OBSColumn(
            id='t002c006_t',
            name='Selected Aboriginal languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c005_t: DENOMINATOR },)

        t002c006_m = OBSColumn(
            id='t002c006_m',
            name='Selected Aboriginal languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c005_m: DENOMINATOR },)

        t002c006_f = OBSColumn(
            id='t002c006_f',
            name='Selected Aboriginal languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c005_f: DENOMINATOR },)

        t002c007_t = OBSColumn(
            id='t002c007_t',
            name='Atikamekw (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_t: DENOMINATOR },)

        t002c007_m = OBSColumn(
            id='t002c007_m',
            name='Atikamekw (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_m: DENOMINATOR },)

        t002c007_f = OBSColumn(
            id='t002c007_f',
            name='Atikamekw (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_f: DENOMINATOR },)

        t002c008_t = OBSColumn(
            id='t002c008_t',
            name='Cree, n.o.s. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_t: DENOMINATOR },)

        t002c008_m = OBSColumn(
            id='t002c008_m',
            name='Cree, n.o.s. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_m: DENOMINATOR },)

        t002c008_f = OBSColumn(
            id='t002c008_f',
            name='Cree, n.o.s. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_f: DENOMINATOR },)

        t002c009_t = OBSColumn(
            id='t002c009_t',
            name='Dene (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_t: DENOMINATOR },)

        t002c009_m = OBSColumn(
            id='t002c009_m',
            name='Dene (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_m: DENOMINATOR },)

        t002c009_f = OBSColumn(
            id='t002c009_f',
            name='Dene (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_f: DENOMINATOR },)

        t002c010_t = OBSColumn(
            id='t002c010_t',
            name='Innu/Montagnais (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_t: DENOMINATOR },)

        t002c010_m = OBSColumn(
            id='t002c010_m',
            name='Innu/Montagnais (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_m: DENOMINATOR },)

        t002c010_f = OBSColumn(
            id='t002c010_f',
            name='Innu/Montagnais (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_f: DENOMINATOR },)

        t002c011_t = OBSColumn(
            id='t002c011_t',
            name='Inuktitut (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_t: DENOMINATOR },)

        t002c011_m = OBSColumn(
            id='t002c011_m',
            name='Inuktitut (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_m: DENOMINATOR },)

        t002c011_f = OBSColumn(
            id='t002c011_f',
            name='Inuktitut (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_f: DENOMINATOR },)

        t002c012_t = OBSColumn(
            id='t002c012_t',
            name='Mi\'kmaq (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_t: DENOMINATOR },)

        t002c012_m = OBSColumn(
            id='t002c012_m',
            name='Mi\'kmaq (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_m: DENOMINATOR },)

        t002c012_f = OBSColumn(
            id='t002c012_f',
            name='Mi\'kmaq (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_f: DENOMINATOR },)

        t002c013_t = OBSColumn(
            id='t002c013_t',
            name='Ojibway (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_t: DENOMINATOR },)

        t002c013_m = OBSColumn(
            id='t002c013_m',
            name='Ojibway (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_m: DENOMINATOR },)

        t002c013_f = OBSColumn(
            id='t002c013_f',
            name='Ojibway (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_f: DENOMINATOR },)

        t002c014_t = OBSColumn(
            id='t002c014_t',
            name='Oji-Cree (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_t: DENOMINATOR },)

        t002c014_m = OBSColumn(
            id='t002c014_m',
            name='Oji-Cree (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_m: DENOMINATOR },)

        t002c014_f = OBSColumn(
            id='t002c014_f',
            name='Oji-Cree (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_f: DENOMINATOR },)

        t002c015_t = OBSColumn(
            id='t002c015_t',
            name='Stoney (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_t: DENOMINATOR },)

        t002c015_m = OBSColumn(
            id='t002c015_m',
            name='Stoney (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_m: DENOMINATOR },)

        t002c015_f = OBSColumn(
            id='t002c015_f',
            name='Stoney (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c006_f: DENOMINATOR },)

        t002c016_t = OBSColumn(
            id='t002c016_t',
            name='Selected non-Aboriginal languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c005_t: DENOMINATOR },)

        t002c016_m = OBSColumn(
            id='t002c016_m',
            name='Selected non-Aboriginal languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c005_m: DENOMINATOR },)

        t002c016_f = OBSColumn(
            id='t002c016_f',
            name='Selected non-Aboriginal languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c005_f: DENOMINATOR },)

        t002c017_t = OBSColumn(
            id='t002c017_t',
            name='African languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c017_m = OBSColumn(
            id='t002c017_m',
            name='African languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c017_f = OBSColumn(
            id='t002c017_f',
            name='African languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c018_t = OBSColumn(
            id='t002c018_t',
            name='Afrikaans (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c018_m = OBSColumn(
            id='t002c018_m',
            name='Afrikaans (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c018_f = OBSColumn(
            id='t002c018_f',
            name='Afrikaans (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c019_t = OBSColumn(
            id='t002c019_t',
            name='Akan (Twi) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c019_m = OBSColumn(
            id='t002c019_m',
            name='Akan (Twi) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c019_f = OBSColumn(
            id='t002c019_f',
            name='Akan (Twi) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c020_t = OBSColumn(
            id='t002c020_t',
            name='Albanian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c020_m = OBSColumn(
            id='t002c020_m',
            name='Albanian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c020_f = OBSColumn(
            id='t002c020_f',
            name='Albanian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c021_t = OBSColumn(
            id='t002c021_t',
            name='Amharic (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c021_m = OBSColumn(
            id='t002c021_m',
            name='Amharic (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c021_f = OBSColumn(
            id='t002c021_f',
            name='Amharic (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c022_t = OBSColumn(
            id='t002c022_t',
            name='Arabic (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c022_m = OBSColumn(
            id='t002c022_m',
            name='Arabic (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c022_f = OBSColumn(
            id='t002c022_f',
            name='Arabic (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c023_t = OBSColumn(
            id='t002c023_t',
            name='Armenian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c023_m = OBSColumn(
            id='t002c023_m',
            name='Armenian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c023_f = OBSColumn(
            id='t002c023_f',
            name='Armenian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c024_t = OBSColumn(
            id='t002c024_t',
            name='Bantu languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c024_m = OBSColumn(
            id='t002c024_m',
            name='Bantu languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c024_f = OBSColumn(
            id='t002c024_f',
            name='Bantu languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c025_t = OBSColumn(
            id='t002c025_t',
            name='Bengali (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c025_m = OBSColumn(
            id='t002c025_m',
            name='Bengali (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c025_f = OBSColumn(
            id='t002c025_f',
            name='Bengali (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c026_t = OBSColumn(
            id='t002c026_t',
            name='Berber languages (Kabyle) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c026_m = OBSColumn(
            id='t002c026_m',
            name='Berber languages (Kabyle) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c026_f = OBSColumn(
            id='t002c026_f',
            name='Berber languages (Kabyle) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c027_t = OBSColumn(
            id='t002c027_t',
            name='Bisayan languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c027_m = OBSColumn(
            id='t002c027_m',
            name='Bisayan languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c027_f = OBSColumn(
            id='t002c027_f',
            name='Bisayan languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c028_t = OBSColumn(
            id='t002c028_t',
            name='Bosnian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c028_m = OBSColumn(
            id='t002c028_m',
            name='Bosnian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c028_f = OBSColumn(
            id='t002c028_f',
            name='Bosnian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c029_t = OBSColumn(
            id='t002c029_t',
            name='Bulgarian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c029_m = OBSColumn(
            id='t002c029_m',
            name='Bulgarian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c029_f = OBSColumn(
            id='t002c029_f',
            name='Bulgarian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c030_t = OBSColumn(
            id='t002c030_t',
            name='Burmese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c030_m = OBSColumn(
            id='t002c030_m',
            name='Burmese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c030_f = OBSColumn(
            id='t002c030_f',
            name='Burmese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c031_t = OBSColumn(
            id='t002c031_t',
            name='Cantonese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c031_m = OBSColumn(
            id='t002c031_m',
            name='Cantonese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c031_f = OBSColumn(
            id='t002c031_f',
            name='Cantonese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c032_t = OBSColumn(
            id='t002c032_t',
            name='Chinese, n.o.s. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c032_m = OBSColumn(
            id='t002c032_m',
            name='Chinese, n.o.s. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c032_f = OBSColumn(
            id='t002c032_f',
            name='Chinese, n.o.s. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c033_t = OBSColumn(
            id='t002c033_t',
            name='Creoles (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c033_m = OBSColumn(
            id='t002c033_m',
            name='Creoles (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c033_f = OBSColumn(
            id='t002c033_f',
            name='Creoles (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c034_t = OBSColumn(
            id='t002c034_t',
            name='Croatian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c034_m = OBSColumn(
            id='t002c034_m',
            name='Croatian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c034_f = OBSColumn(
            id='t002c034_f',
            name='Croatian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c035_t = OBSColumn(
            id='t002c035_t',
            name='Czech (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c035_m = OBSColumn(
            id='t002c035_m',
            name='Czech (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c035_f = OBSColumn(
            id='t002c035_f',
            name='Czech (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c036_t = OBSColumn(
            id='t002c036_t',
            name='Danish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c036_m = OBSColumn(
            id='t002c036_m',
            name='Danish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c036_f = OBSColumn(
            id='t002c036_f',
            name='Danish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c037_t = OBSColumn(
            id='t002c037_t',
            name='Dutch (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c037_m = OBSColumn(
            id='t002c037_m',
            name='Dutch (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c037_f = OBSColumn(
            id='t002c037_f',
            name='Dutch (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c038_t = OBSColumn(
            id='t002c038_t',
            name='Estonian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c038_m = OBSColumn(
            id='t002c038_m',
            name='Estonian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c038_f = OBSColumn(
            id='t002c038_f',
            name='Estonian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c039_t = OBSColumn(
            id='t002c039_t',
            name='Finnish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c039_m = OBSColumn(
            id='t002c039_m',
            name='Finnish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c039_f = OBSColumn(
            id='t002c039_f',
            name='Finnish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c040_t = OBSColumn(
            id='t002c040_t',
            name='Flemish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c040_m = OBSColumn(
            id='t002c040_m',
            name='Flemish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c040_f = OBSColumn(
            id='t002c040_f',
            name='Flemish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c041_t = OBSColumn(
            id='t002c041_t',
            name='Fukien (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c041_m = OBSColumn(
            id='t002c041_m',
            name='Fukien (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c041_f = OBSColumn(
            id='t002c041_f',
            name='Fukien (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c042_t = OBSColumn(
            id='t002c042_t',
            name='German (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c042_m = OBSColumn(
            id='t002c042_m',
            name='German (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c042_f = OBSColumn(
            id='t002c042_f',
            name='German (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c043_t = OBSColumn(
            id='t002c043_t',
            name='Greek (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c043_m = OBSColumn(
            id='t002c043_m',
            name='Greek (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c043_f = OBSColumn(
            id='t002c043_f',
            name='Greek (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c044_t = OBSColumn(
            id='t002c044_t',
            name='Gujarati (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c044_m = OBSColumn(
            id='t002c044_m',
            name='Gujarati (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c044_f = OBSColumn(
            id='t002c044_f',
            name='Gujarati (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c045_t = OBSColumn(
            id='t002c045_t',
            name='Hakka (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c045_m = OBSColumn(
            id='t002c045_m',
            name='Hakka (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c045_f = OBSColumn(
            id='t002c045_f',
            name='Hakka (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c046_t = OBSColumn(
            id='t002c046_t',
            name='Hebrew (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c046_m = OBSColumn(
            id='t002c046_m',
            name='Hebrew (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c046_f = OBSColumn(
            id='t002c046_f',
            name='Hebrew (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c047_t = OBSColumn(
            id='t002c047_t',
            name='Hindi (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c047_m = OBSColumn(
            id='t002c047_m',
            name='Hindi (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c047_f = OBSColumn(
            id='t002c047_f',
            name='Hindi (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c048_t = OBSColumn(
            id='t002c048_t',
            name='Hungarian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c048_m = OBSColumn(
            id='t002c048_m',
            name='Hungarian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c048_f = OBSColumn(
            id='t002c048_f',
            name='Hungarian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c049_t = OBSColumn(
            id='t002c049_t',
            name='Ilocano (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c049_m = OBSColumn(
            id='t002c049_m',
            name='Ilocano (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c049_f = OBSColumn(
            id='t002c049_f',
            name='Ilocano (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c050_t = OBSColumn(
            id='t002c050_t',
            name='Indo-Iranian languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c050_m = OBSColumn(
            id='t002c050_m',
            name='Indo-Iranian languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c050_f = OBSColumn(
            id='t002c050_f',
            name='Indo-Iranian languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c051_t = OBSColumn(
            id='t002c051_t',
            name='Italian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c051_m = OBSColumn(
            id='t002c051_m',
            name='Italian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c051_f = OBSColumn(
            id='t002c051_f',
            name='Italian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c052_t = OBSColumn(
            id='t002c052_t',
            name='Japanese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c052_m = OBSColumn(
            id='t002c052_m',
            name='Japanese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c052_f = OBSColumn(
            id='t002c052_f',
            name='Japanese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c053_t = OBSColumn(
            id='t002c053_t',
            name='Khmer (Cambodian) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c053_m = OBSColumn(
            id='t002c053_m',
            name='Khmer (Cambodian) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c053_f = OBSColumn(
            id='t002c053_f',
            name='Khmer (Cambodian) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c054_t = OBSColumn(
            id='t002c054_t',
            name='Korean (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c054_m = OBSColumn(
            id='t002c054_m',
            name='Korean (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c054_f = OBSColumn(
            id='t002c054_f',
            name='Korean (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c055_t = OBSColumn(
            id='t002c055_t',
            name='Kurdish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c055_m = OBSColumn(
            id='t002c055_m',
            name='Kurdish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c055_f = OBSColumn(
            id='t002c055_f',
            name='Kurdish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c056_t = OBSColumn(
            id='t002c056_t',
            name='Lao (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c056_m = OBSColumn(
            id='t002c056_m',
            name='Lao (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c056_f = OBSColumn(
            id='t002c056_f',
            name='Lao (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c057_t = OBSColumn(
            id='t002c057_t',
            name='Latvian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c057_m = OBSColumn(
            id='t002c057_m',
            name='Latvian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c057_f = OBSColumn(
            id='t002c057_f',
            name='Latvian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c058_t = OBSColumn(
            id='t002c058_t',
            name='Lingala (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c058_m = OBSColumn(
            id='t002c058_m',
            name='Lingala (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c058_f = OBSColumn(
            id='t002c058_f',
            name='Lingala (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c059_t = OBSColumn(
            id='t002c059_t',
            name='Lithuanian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c059_m = OBSColumn(
            id='t002c059_m',
            name='Lithuanian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c059_f = OBSColumn(
            id='t002c059_f',
            name='Lithuanian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c060_t = OBSColumn(
            id='t002c060_t',
            name='Macedonian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c060_m = OBSColumn(
            id='t002c060_m',
            name='Macedonian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c060_f = OBSColumn(
            id='t002c060_f',
            name='Macedonian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c061_t = OBSColumn(
            id='t002c061_t',
            name='Malay (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c061_m = OBSColumn(
            id='t002c061_m',
            name='Malay (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c061_f = OBSColumn(
            id='t002c061_f',
            name='Malay (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c062_t = OBSColumn(
            id='t002c062_t',
            name='Malayalam (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c062_m = OBSColumn(
            id='t002c062_m',
            name='Malayalam (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c062_f = OBSColumn(
            id='t002c062_f',
            name='Malayalam (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c063_t = OBSColumn(
            id='t002c063_t',
            name='Maltese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c063_m = OBSColumn(
            id='t002c063_m',
            name='Maltese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c063_f = OBSColumn(
            id='t002c063_f',
            name='Maltese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c064_t = OBSColumn(
            id='t002c064_t',
            name='Mandarin (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c064_m = OBSColumn(
            id='t002c064_m',
            name='Mandarin (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c064_f = OBSColumn(
            id='t002c064_f',
            name='Mandarin (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c065_t = OBSColumn(
            id='t002c065_t',
            name='Marathi (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c065_m = OBSColumn(
            id='t002c065_m',
            name='Marathi (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c065_f = OBSColumn(
            id='t002c065_f',
            name='Marathi (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c066_t = OBSColumn(
            id='t002c066_t',
            name='Nepali (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c066_m = OBSColumn(
            id='t002c066_m',
            name='Nepali (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c066_f = OBSColumn(
            id='t002c066_f',
            name='Nepali (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c067_t = OBSColumn(
            id='t002c067_t',
            name='Niger-Congo languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c067_m = OBSColumn(
            id='t002c067_m',
            name='Niger-Congo languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c067_f = OBSColumn(
            id='t002c067_f',
            name='Niger-Congo languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c068_t = OBSColumn(
            id='t002c068_t',
            name='Norwegian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c068_m = OBSColumn(
            id='t002c068_m',
            name='Norwegian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c068_f = OBSColumn(
            id='t002c068_f',
            name='Norwegian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c069_t = OBSColumn(
            id='t002c069_t',
            name='Oromo (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c069_m = OBSColumn(
            id='t002c069_m',
            name='Oromo (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c069_f = OBSColumn(
            id='t002c069_f',
            name='Oromo (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c070_t = OBSColumn(
            id='t002c070_t',
            name='Panjabi (Punjabi) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c070_m = OBSColumn(
            id='t002c070_m',
            name='Panjabi (Punjabi) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c070_f = OBSColumn(
            id='t002c070_f',
            name='Panjabi (Punjabi) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c071_t = OBSColumn(
            id='t002c071_t',
            name='Pashto (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c071_m = OBSColumn(
            id='t002c071_m',
            name='Pashto (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c071_f = OBSColumn(
            id='t002c071_f',
            name='Pashto (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c072_t = OBSColumn(
            id='t002c072_t',
            name='Persian (Farsi) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c072_m = OBSColumn(
            id='t002c072_m',
            name='Persian (Farsi) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c072_f = OBSColumn(
            id='t002c072_f',
            name='Persian (Farsi) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c073_t = OBSColumn(
            id='t002c073_t',
            name='Polish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c073_m = OBSColumn(
            id='t002c073_m',
            name='Polish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c073_f = OBSColumn(
            id='t002c073_f',
            name='Polish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c074_t = OBSColumn(
            id='t002c074_t',
            name='Portuguese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c074_m = OBSColumn(
            id='t002c074_m',
            name='Portuguese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c074_f = OBSColumn(
            id='t002c074_f',
            name='Portuguese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c075_t = OBSColumn(
            id='t002c075_t',
            name='Romanian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c075_m = OBSColumn(
            id='t002c075_m',
            name='Romanian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c075_f = OBSColumn(
            id='t002c075_f',
            name='Romanian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c076_t = OBSColumn(
            id='t002c076_t',
            name='Rundi (Kirundi) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c076_m = OBSColumn(
            id='t002c076_m',
            name='Rundi (Kirundi) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c076_f = OBSColumn(
            id='t002c076_f',
            name='Rundi (Kirundi) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c077_t = OBSColumn(
            id='t002c077_t',
            name='Russian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c077_m = OBSColumn(
            id='t002c077_m',
            name='Russian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c077_f = OBSColumn(
            id='t002c077_f',
            name='Russian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c078_t = OBSColumn(
            id='t002c078_t',
            name='Rwanda (Kinyarwanda) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c078_m = OBSColumn(
            id='t002c078_m',
            name='Rwanda (Kinyarwanda) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c078_f = OBSColumn(
            id='t002c078_f',
            name='Rwanda (Kinyarwanda) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c079_t = OBSColumn(
            id='t002c079_t',
            name='Semitic languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c079_m = OBSColumn(
            id='t002c079_m',
            name='Semitic languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c079_f = OBSColumn(
            id='t002c079_f',
            name='Semitic languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c080_t = OBSColumn(
            id='t002c080_t',
            name='Serbian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c080_m = OBSColumn(
            id='t002c080_m',
            name='Serbian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c080_f = OBSColumn(
            id='t002c080_f',
            name='Serbian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c081_t = OBSColumn(
            id='t002c081_t',
            name='Serbo-Croatian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c081_m = OBSColumn(
            id='t002c081_m',
            name='Serbo-Croatian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c081_f = OBSColumn(
            id='t002c081_f',
            name='Serbo-Croatian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c082_t = OBSColumn(
            id='t002c082_t',
            name='Shanghainese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c082_m = OBSColumn(
            id='t002c082_m',
            name='Shanghainese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c082_f = OBSColumn(
            id='t002c082_f',
            name='Shanghainese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c083_t = OBSColumn(
            id='t002c083_t',
            name='Sign languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c083_m = OBSColumn(
            id='t002c083_m',
            name='Sign languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c083_f = OBSColumn(
            id='t002c083_f',
            name='Sign languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c084_t = OBSColumn(
            id='t002c084_t',
            name='Sindhi (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c084_m = OBSColumn(
            id='t002c084_m',
            name='Sindhi (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c084_f = OBSColumn(
            id='t002c084_f',
            name='Sindhi (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c085_t = OBSColumn(
            id='t002c085_t',
            name='Sinhala (Sinhalese) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c085_m = OBSColumn(
            id='t002c085_m',
            name='Sinhala (Sinhalese) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c085_f = OBSColumn(
            id='t002c085_f',
            name='Sinhala (Sinhalese) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c086_t = OBSColumn(
            id='t002c086_t',
            name='Sino-Tibetan languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c086_m = OBSColumn(
            id='t002c086_m',
            name='Sino-Tibetan languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c086_f = OBSColumn(
            id='t002c086_f',
            name='Sino-Tibetan languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c087_t = OBSColumn(
            id='t002c087_t',
            name='Slavic languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c087_m = OBSColumn(
            id='t002c087_m',
            name='Slavic languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c087_f = OBSColumn(
            id='t002c087_f',
            name='Slavic languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c088_t = OBSColumn(
            id='t002c088_t',
            name='Slovak (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c088_m = OBSColumn(
            id='t002c088_m',
            name='Slovak (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c088_f = OBSColumn(
            id='t002c088_f',
            name='Slovak (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c089_t = OBSColumn(
            id='t002c089_t',
            name='Slovenian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c089_m = OBSColumn(
            id='t002c089_m',
            name='Slovenian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c089_f = OBSColumn(
            id='t002c089_f',
            name='Slovenian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c090_t = OBSColumn(
            id='t002c090_t',
            name='Somali (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c090_m = OBSColumn(
            id='t002c090_m',
            name='Somali (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c090_f = OBSColumn(
            id='t002c090_f',
            name='Somali (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c091_t = OBSColumn(
            id='t002c091_t',
            name='Spanish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c091_m = OBSColumn(
            id='t002c091_m',
            name='Spanish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c091_f = OBSColumn(
            id='t002c091_f',
            name='Spanish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c092_t = OBSColumn(
            id='t002c092_t',
            name='Swahili (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c092_m = OBSColumn(
            id='t002c092_m',
            name='Swahili (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c092_f = OBSColumn(
            id='t002c092_f',
            name='Swahili (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c093_t = OBSColumn(
            id='t002c093_t',
            name='Swedish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c093_m = OBSColumn(
            id='t002c093_m',
            name='Swedish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c093_f = OBSColumn(
            id='t002c093_f',
            name='Swedish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c094_t = OBSColumn(
            id='t002c094_t',
            name='Tagalog (Pilipino, Filipino) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c094_m = OBSColumn(
            id='t002c094_m',
            name='Tagalog (Pilipino, Filipino) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c094_f = OBSColumn(
            id='t002c094_f',
            name='Tagalog (Pilipino, Filipino) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c095_t = OBSColumn(
            id='t002c095_t',
            name='Taiwanese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c095_m = OBSColumn(
            id='t002c095_m',
            name='Taiwanese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c095_f = OBSColumn(
            id='t002c095_f',
            name='Taiwanese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c096_t = OBSColumn(
            id='t002c096_t',
            name='Tamil (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c096_m = OBSColumn(
            id='t002c096_m',
            name='Tamil (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c096_f = OBSColumn(
            id='t002c096_f',
            name='Tamil (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c097_t = OBSColumn(
            id='t002c097_t',
            name='Telugu (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c097_m = OBSColumn(
            id='t002c097_m',
            name='Telugu (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c097_f = OBSColumn(
            id='t002c097_f',
            name='Telugu (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c098_t = OBSColumn(
            id='t002c098_t',
            name='Thai (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c098_m = OBSColumn(
            id='t002c098_m',
            name='Thai (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c098_f = OBSColumn(
            id='t002c098_f',
            name='Thai (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c099_t = OBSColumn(
            id='t002c099_t',
            name='Tibetan languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c099_m = OBSColumn(
            id='t002c099_m',
            name='Tibetan languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c099_f = OBSColumn(
            id='t002c099_f',
            name='Tibetan languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c100_t = OBSColumn(
            id='t002c100_t',
            name='Tigrigna (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c100_m = OBSColumn(
            id='t002c100_m',
            name='Tigrigna (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c100_f = OBSColumn(
            id='t002c100_f',
            name='Tigrigna (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c101_t = OBSColumn(
            id='t002c101_t',
            name='Turkish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c101_m = OBSColumn(
            id='t002c101_m',
            name='Turkish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c101_f = OBSColumn(
            id='t002c101_f',
            name='Turkish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c102_t = OBSColumn(
            id='t002c102_t',
            name='Ukrainian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c102_m = OBSColumn(
            id='t002c102_m',
            name='Ukrainian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c102_f = OBSColumn(
            id='t002c102_f',
            name='Ukrainian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c103_t = OBSColumn(
            id='t002c103_t',
            name='Urdu (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c103_m = OBSColumn(
            id='t002c103_m',
            name='Urdu (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c103_f = OBSColumn(
            id='t002c103_f',
            name='Urdu (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c104_t = OBSColumn(
            id='t002c104_t',
            name='Vietnamese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c104_m = OBSColumn(
            id='t002c104_m',
            name='Vietnamese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c104_f = OBSColumn(
            id='t002c104_f',
            name='Vietnamese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c105_t = OBSColumn(
            id='t002c105_t',
            name='Yiddish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_t: DENOMINATOR },)

        t002c105_m = OBSColumn(
            id='t002c105_m',
            name='Yiddish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_m: DENOMINATOR },)

        t002c105_f = OBSColumn(
            id='t002c105_f',
            name='Yiddish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c016_f: DENOMINATOR },)

        t002c106_t = OBSColumn(
            id='t002c106_t',
            name='Other languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c005_t: DENOMINATOR },)

        t002c106_m = OBSColumn(
            id='t002c106_m',
            name='Other languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c005_m: DENOMINATOR },)

        t002c106_f = OBSColumn(
            id='t002c106_f',
            name='Other languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c005_f: DENOMINATOR },)

        t002c107_t = OBSColumn(
            id='t002c107_t',
            name='Language spoken most often at home - Multiple responses (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_t: DENOMINATOR },)

        t002c107_m = OBSColumn(
            id='t002c107_m',
            name='Language spoken most often at home - Multiple responses (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_m: DENOMINATOR },)

        t002c107_f = OBSColumn(
            id='t002c107_f',
            name='Language spoken most often at home - Multiple responses (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_f: DENOMINATOR },)

        t002c108_t = OBSColumn(
            id='t002c108_t',
            name='English and French (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c107_t: DENOMINATOR },)

        t002c108_m = OBSColumn(
            id='t002c108_m',
            name='English and French (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c107_m: DENOMINATOR },)

        t002c108_f = OBSColumn(
            id='t002c108_f',
            name='English and French (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c107_f: DENOMINATOR },)

        t002c109_t = OBSColumn(
            id='t002c109_t',
            name='English and non-official language (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c107_t: DENOMINATOR },)

        t002c109_m = OBSColumn(
            id='t002c109_m',
            name='English and non-official language (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c107_m: DENOMINATOR },)

        t002c109_f = OBSColumn(
            id='t002c109_f',
            name='English and non-official language (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c107_f: DENOMINATOR },)

        t002c110_t = OBSColumn(
            id='t002c110_t',
            name='French and non-official language (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c107_t: DENOMINATOR },)

        t002c110_m = OBSColumn(
            id='t002c110_m',
            name='French and non-official language (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c107_m: DENOMINATOR },)

        t002c110_f = OBSColumn(
            id='t002c110_f',
            name='French and non-official language (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c107_f: DENOMINATOR },)

        t002c111_t = OBSColumn(
            id='t002c111_t',
            name='English, French and non-official language (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c107_t: DENOMINATOR },)

        t002c111_m = OBSColumn(
            id='t002c111_m',
            name='English, French and non-official language (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c107_m: DENOMINATOR },)

        t002c111_f = OBSColumn(
            id='t002c111_f',
            name='English, French and non-official language (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c107_f: DENOMINATOR },)

        t003c002_t = OBSColumn(
            id='t003c002_t',
            name='Detailed mother tongue - Single responses (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_t: DENOMINATOR },)

        t003c002_m = OBSColumn(
            id='t003c002_m',
            name='Detailed mother tongue - Single responses (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_m: DENOMINATOR },)

        t003c002_f = OBSColumn(
            id='t003c002_f',
            name='Detailed mother tongue - Single responses (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_f: DENOMINATOR },)

        t003c003_t = OBSColumn(
            id='t003c003_t',
            name='English (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c002_t: DENOMINATOR },)

        t003c003_m = OBSColumn(
            id='t003c003_m',
            name='English (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c002_m: DENOMINATOR },)

        t003c003_f = OBSColumn(
            id='t003c003_f',
            name='English (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c002_f: DENOMINATOR },)

        t003c004_t = OBSColumn(
            id='t003c004_t',
            name='French (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c002_t: DENOMINATOR },)

        t003c004_m = OBSColumn(
            id='t003c004_m',
            name='French (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c002_m: DENOMINATOR },)

        t003c004_f = OBSColumn(
            id='t003c004_f',
            name='French (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c002_f: DENOMINATOR },)

        t003c005_t = OBSColumn(
            id='t003c005_t',
            name='Non-official languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c002_t: DENOMINATOR },)

        t003c005_m = OBSColumn(
            id='t003c005_m',
            name='Non-official languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c002_m: DENOMINATOR },)

        t003c005_f = OBSColumn(
            id='t003c005_f',
            name='Non-official languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c002_f: DENOMINATOR },)

        t003c006_t = OBSColumn(
            id='t003c006_t',
            name='Selected Aboriginal languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c005_t: DENOMINATOR },)

        t003c006_m = OBSColumn(
            id='t003c006_m',
            name='Selected Aboriginal languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c005_m: DENOMINATOR },)

        t003c006_f = OBSColumn(
            id='t003c006_f',
            name='Selected Aboriginal languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c005_f: DENOMINATOR },)

        t003c007_t = OBSColumn(
            id='t003c007_t',
            name='Atikamekw (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_t: DENOMINATOR },)

        t003c007_m = OBSColumn(
            id='t003c007_m',
            name='Atikamekw (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_m: DENOMINATOR },)

        t003c007_f = OBSColumn(
            id='t003c007_f',
            name='Atikamekw (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_f: DENOMINATOR },)

        t003c008_t = OBSColumn(
            id='t003c008_t',
            name='Cree, n.o.s. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_t: DENOMINATOR },)

        t003c008_m = OBSColumn(
            id='t003c008_m',
            name='Cree, n.o.s. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_m: DENOMINATOR },)

        t003c008_f = OBSColumn(
            id='t003c008_f',
            name='Cree, n.o.s. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_f: DENOMINATOR },)

        t003c009_t = OBSColumn(
            id='t003c009_t',
            name='Dene (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_t: DENOMINATOR },)

        t003c009_m = OBSColumn(
            id='t003c009_m',
            name='Dene (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_m: DENOMINATOR },)

        t003c009_f = OBSColumn(
            id='t003c009_f',
            name='Dene (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_f: DENOMINATOR },)

        t003c010_t = OBSColumn(
            id='t003c010_t',
            name='Innu/Montagnais (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_t: DENOMINATOR },)

        t003c010_m = OBSColumn(
            id='t003c010_m',
            name='Innu/Montagnais (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_m: DENOMINATOR },)

        t003c010_f = OBSColumn(
            id='t003c010_f',
            name='Innu/Montagnais (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_f: DENOMINATOR },)

        t003c011_t = OBSColumn(
            id='t003c011_t',
            name='Inuktitut (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_t: DENOMINATOR },)

        t003c011_m = OBSColumn(
            id='t003c011_m',
            name='Inuktitut (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_m: DENOMINATOR },)

        t003c011_f = OBSColumn(
            id='t003c011_f',
            name='Inuktitut (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_f: DENOMINATOR },)

        t003c012_t = OBSColumn(
            id='t003c012_t',
            name='Mi\'kmaq (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_t: DENOMINATOR },)

        t003c012_m = OBSColumn(
            id='t003c012_m',
            name='Mi\'kmaq (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_m: DENOMINATOR },)

        t003c012_f = OBSColumn(
            id='t003c012_f',
            name='Mi\'kmaq (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_f: DENOMINATOR },)

        t003c013_t = OBSColumn(
            id='t003c013_t',
            name='Ojibway (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_t: DENOMINATOR },)

        t003c013_m = OBSColumn(
            id='t003c013_m',
            name='Ojibway (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_m: DENOMINATOR },)

        t003c013_f = OBSColumn(
            id='t003c013_f',
            name='Ojibway (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_f: DENOMINATOR },)

        t003c014_t = OBSColumn(
            id='t003c014_t',
            name='Oji-Cree (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_t: DENOMINATOR },)

        t003c014_m = OBSColumn(
            id='t003c014_m',
            name='Oji-Cree (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_m: DENOMINATOR },)

        t003c014_f = OBSColumn(
            id='t003c014_f',
            name='Oji-Cree (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_f: DENOMINATOR },)

        t003c015_t = OBSColumn(
            id='t003c015_t',
            name='Stoney (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_t: DENOMINATOR },)

        t003c015_m = OBSColumn(
            id='t003c015_m',
            name='Stoney (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_m: DENOMINATOR },)

        t003c015_f = OBSColumn(
            id='t003c015_f',
            name='Stoney (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c006_f: DENOMINATOR },)

        t003c016_t = OBSColumn(
            id='t003c016_t',
            name='Selected non-Aboriginal languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c005_t: DENOMINATOR },)

        t003c016_m = OBSColumn(
            id='t003c016_m',
            name='Selected non-Aboriginal languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c005_m: DENOMINATOR },)

        t003c016_f = OBSColumn(
            id='t003c016_f',
            name='Selected non-Aboriginal languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c005_f: DENOMINATOR },)

        t003c017_t = OBSColumn(
            id='t003c017_t',
            name='African languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c017_m = OBSColumn(
            id='t003c017_m',
            name='African languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c017_f = OBSColumn(
            id='t003c017_f',
            name='African languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c018_t = OBSColumn(
            id='t003c018_t',
            name='Afrikaans (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c018_m = OBSColumn(
            id='t003c018_m',
            name='Afrikaans (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c018_f = OBSColumn(
            id='t003c018_f',
            name='Afrikaans (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c019_t = OBSColumn(
            id='t003c019_t',
            name='Akan (Twi) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c019_m = OBSColumn(
            id='t003c019_m',
            name='Akan (Twi) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c019_f = OBSColumn(
            id='t003c019_f',
            name='Akan (Twi) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c020_t = OBSColumn(
            id='t003c020_t',
            name='Albanian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c020_m = OBSColumn(
            id='t003c020_m',
            name='Albanian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c020_f = OBSColumn(
            id='t003c020_f',
            name='Albanian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c021_t = OBSColumn(
            id='t003c021_t',
            name='Amharic (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c021_m = OBSColumn(
            id='t003c021_m',
            name='Amharic (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c021_f = OBSColumn(
            id='t003c021_f',
            name='Amharic (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c022_t = OBSColumn(
            id='t003c022_t',
            name='Arabic (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c022_m = OBSColumn(
            id='t003c022_m',
            name='Arabic (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c022_f = OBSColumn(
            id='t003c022_f',
            name='Arabic (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c023_t = OBSColumn(
            id='t003c023_t',
            name='Armenian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c023_m = OBSColumn(
            id='t003c023_m',
            name='Armenian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c023_f = OBSColumn(
            id='t003c023_f',
            name='Armenian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c024_t = OBSColumn(
            id='t003c024_t',
            name='Bantu languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c024_m = OBSColumn(
            id='t003c024_m',
            name='Bantu languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c024_f = OBSColumn(
            id='t003c024_f',
            name='Bantu languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c025_t = OBSColumn(
            id='t003c025_t',
            name='Bengali (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c025_m = OBSColumn(
            id='t003c025_m',
            name='Bengali (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c025_f = OBSColumn(
            id='t003c025_f',
            name='Bengali (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c026_t = OBSColumn(
            id='t003c026_t',
            name='Berber languages (Kabyle) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c026_m = OBSColumn(
            id='t003c026_m',
            name='Berber languages (Kabyle) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c026_f = OBSColumn(
            id='t003c026_f',
            name='Berber languages (Kabyle) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c027_t = OBSColumn(
            id='t003c027_t',
            name='Bisayan languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c027_m = OBSColumn(
            id='t003c027_m',
            name='Bisayan languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c027_f = OBSColumn(
            id='t003c027_f',
            name='Bisayan languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c028_t = OBSColumn(
            id='t003c028_t',
            name='Bosnian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c028_m = OBSColumn(
            id='t003c028_m',
            name='Bosnian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c028_f = OBSColumn(
            id='t003c028_f',
            name='Bosnian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c029_t = OBSColumn(
            id='t003c029_t',
            name='Bulgarian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c029_m = OBSColumn(
            id='t003c029_m',
            name='Bulgarian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c029_f = OBSColumn(
            id='t003c029_f',
            name='Bulgarian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c030_t = OBSColumn(
            id='t003c030_t',
            name='Burmese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c030_m = OBSColumn(
            id='t003c030_m',
            name='Burmese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c030_f = OBSColumn(
            id='t003c030_f',
            name='Burmese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c031_t = OBSColumn(
            id='t003c031_t',
            name='Cantonese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c031_m = OBSColumn(
            id='t003c031_m',
            name='Cantonese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c031_f = OBSColumn(
            id='t003c031_f',
            name='Cantonese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c032_t = OBSColumn(
            id='t003c032_t',
            name='Chinese, n.o.s. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c032_m = OBSColumn(
            id='t003c032_m',
            name='Chinese, n.o.s. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c032_f = OBSColumn(
            id='t003c032_f',
            name='Chinese, n.o.s. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c033_t = OBSColumn(
            id='t003c033_t',
            name='Creoles (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c033_m = OBSColumn(
            id='t003c033_m',
            name='Creoles (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c033_f = OBSColumn(
            id='t003c033_f',
            name='Creoles (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c034_t = OBSColumn(
            id='t003c034_t',
            name='Croatian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c034_m = OBSColumn(
            id='t003c034_m',
            name='Croatian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c034_f = OBSColumn(
            id='t003c034_f',
            name='Croatian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c035_t = OBSColumn(
            id='t003c035_t',
            name='Czech (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c035_m = OBSColumn(
            id='t003c035_m',
            name='Czech (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c035_f = OBSColumn(
            id='t003c035_f',
            name='Czech (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c036_t = OBSColumn(
            id='t003c036_t',
            name='Danish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c036_m = OBSColumn(
            id='t003c036_m',
            name='Danish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c036_f = OBSColumn(
            id='t003c036_f',
            name='Danish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c037_t = OBSColumn(
            id='t003c037_t',
            name='Dutch (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c037_m = OBSColumn(
            id='t003c037_m',
            name='Dutch (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c037_f = OBSColumn(
            id='t003c037_f',
            name='Dutch (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c038_t = OBSColumn(
            id='t003c038_t',
            name='Estonian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c038_m = OBSColumn(
            id='t003c038_m',
            name='Estonian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c038_f = OBSColumn(
            id='t003c038_f',
            name='Estonian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c039_t = OBSColumn(
            id='t003c039_t',
            name='Finnish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c039_m = OBSColumn(
            id='t003c039_m',
            name='Finnish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c039_f = OBSColumn(
            id='t003c039_f',
            name='Finnish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c040_t = OBSColumn(
            id='t003c040_t',
            name='Flemish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c040_m = OBSColumn(
            id='t003c040_m',
            name='Flemish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c040_f = OBSColumn(
            id='t003c040_f',
            name='Flemish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c041_t = OBSColumn(
            id='t003c041_t',
            name='Fukien (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c041_m = OBSColumn(
            id='t003c041_m',
            name='Fukien (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c041_f = OBSColumn(
            id='t003c041_f',
            name='Fukien (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c042_t = OBSColumn(
            id='t003c042_t',
            name='German (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c042_m = OBSColumn(
            id='t003c042_m',
            name='German (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c042_f = OBSColumn(
            id='t003c042_f',
            name='German (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c043_t = OBSColumn(
            id='t003c043_t',
            name='Greek (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c043_m = OBSColumn(
            id='t003c043_m',
            name='Greek (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c043_f = OBSColumn(
            id='t003c043_f',
            name='Greek (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c044_t = OBSColumn(
            id='t003c044_t',
            name='Gujarati (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c044_m = OBSColumn(
            id='t003c044_m',
            name='Gujarati (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c044_f = OBSColumn(
            id='t003c044_f',
            name='Gujarati (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c045_t = OBSColumn(
            id='t003c045_t',
            name='Hakka (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c045_m = OBSColumn(
            id='t003c045_m',
            name='Hakka (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c045_f = OBSColumn(
            id='t003c045_f',
            name='Hakka (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c046_t = OBSColumn(
            id='t003c046_t',
            name='Hebrew (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c046_m = OBSColumn(
            id='t003c046_m',
            name='Hebrew (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c046_f = OBSColumn(
            id='t003c046_f',
            name='Hebrew (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c047_t = OBSColumn(
            id='t003c047_t',
            name='Hindi (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c047_m = OBSColumn(
            id='t003c047_m',
            name='Hindi (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c047_f = OBSColumn(
            id='t003c047_f',
            name='Hindi (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c048_t = OBSColumn(
            id='t003c048_t',
            name='Hungarian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c048_m = OBSColumn(
            id='t003c048_m',
            name='Hungarian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c048_f = OBSColumn(
            id='t003c048_f',
            name='Hungarian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c049_t = OBSColumn(
            id='t003c049_t',
            name='Ilocano (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c049_m = OBSColumn(
            id='t003c049_m',
            name='Ilocano (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c049_f = OBSColumn(
            id='t003c049_f',
            name='Ilocano (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c050_t = OBSColumn(
            id='t003c050_t',
            name='Indo-Iranian languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c050_m = OBSColumn(
            id='t003c050_m',
            name='Indo-Iranian languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c050_f = OBSColumn(
            id='t003c050_f',
            name='Indo-Iranian languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c051_t = OBSColumn(
            id='t003c051_t',
            name='Italian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c051_m = OBSColumn(
            id='t003c051_m',
            name='Italian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c051_f = OBSColumn(
            id='t003c051_f',
            name='Italian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c052_t = OBSColumn(
            id='t003c052_t',
            name='Japanese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c052_m = OBSColumn(
            id='t003c052_m',
            name='Japanese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c052_f = OBSColumn(
            id='t003c052_f',
            name='Japanese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c053_t = OBSColumn(
            id='t003c053_t',
            name='Khmer (Cambodian) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c053_m = OBSColumn(
            id='t003c053_m',
            name='Khmer (Cambodian) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c053_f = OBSColumn(
            id='t003c053_f',
            name='Khmer (Cambodian) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c054_t = OBSColumn(
            id='t003c054_t',
            name='Korean (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c054_m = OBSColumn(
            id='t003c054_m',
            name='Korean (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c054_f = OBSColumn(
            id='t003c054_f',
            name='Korean (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c055_t = OBSColumn(
            id='t003c055_t',
            name='Kurdish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c055_m = OBSColumn(
            id='t003c055_m',
            name='Kurdish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c055_f = OBSColumn(
            id='t003c055_f',
            name='Kurdish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c056_t = OBSColumn(
            id='t003c056_t',
            name='Lao (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c056_m = OBSColumn(
            id='t003c056_m',
            name='Lao (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c056_f = OBSColumn(
            id='t003c056_f',
            name='Lao (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c057_t = OBSColumn(
            id='t003c057_t',
            name='Latvian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c057_m = OBSColumn(
            id='t003c057_m',
            name='Latvian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c057_f = OBSColumn(
            id='t003c057_f',
            name='Latvian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c058_t = OBSColumn(
            id='t003c058_t',
            name='Lingala (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c058_m = OBSColumn(
            id='t003c058_m',
            name='Lingala (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c058_f = OBSColumn(
            id='t003c058_f',
            name='Lingala (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c059_t = OBSColumn(
            id='t003c059_t',
            name='Lithuanian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c059_m = OBSColumn(
            id='t003c059_m',
            name='Lithuanian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c059_f = OBSColumn(
            id='t003c059_f',
            name='Lithuanian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c060_t = OBSColumn(
            id='t003c060_t',
            name='Macedonian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c060_m = OBSColumn(
            id='t003c060_m',
            name='Macedonian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c060_f = OBSColumn(
            id='t003c060_f',
            name='Macedonian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c061_t = OBSColumn(
            id='t003c061_t',
            name='Malay (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c061_m = OBSColumn(
            id='t003c061_m',
            name='Malay (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c061_f = OBSColumn(
            id='t003c061_f',
            name='Malay (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c062_t = OBSColumn(
            id='t003c062_t',
            name='Malayalam (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c062_m = OBSColumn(
            id='t003c062_m',
            name='Malayalam (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c062_f = OBSColumn(
            id='t003c062_f',
            name='Malayalam (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c063_t = OBSColumn(
            id='t003c063_t',
            name='Maltese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c063_m = OBSColumn(
            id='t003c063_m',
            name='Maltese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c063_f = OBSColumn(
            id='t003c063_f',
            name='Maltese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c064_t = OBSColumn(
            id='t003c064_t',
            name='Mandarin (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c064_m = OBSColumn(
            id='t003c064_m',
            name='Mandarin (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c064_f = OBSColumn(
            id='t003c064_f',
            name='Mandarin (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c065_t = OBSColumn(
            id='t003c065_t',
            name='Marathi (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c065_m = OBSColumn(
            id='t003c065_m',
            name='Marathi (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c065_f = OBSColumn(
            id='t003c065_f',
            name='Marathi (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c066_t = OBSColumn(
            id='t003c066_t',
            name='Nepali (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c066_m = OBSColumn(
            id='t003c066_m',
            name='Nepali (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c066_f = OBSColumn(
            id='t003c066_f',
            name='Nepali (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c067_t = OBSColumn(
            id='t003c067_t',
            name='Niger-Congo languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c067_m = OBSColumn(
            id='t003c067_m',
            name='Niger-Congo languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c067_f = OBSColumn(
            id='t003c067_f',
            name='Niger-Congo languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c068_t = OBSColumn(
            id='t003c068_t',
            name='Norwegian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c068_m = OBSColumn(
            id='t003c068_m',
            name='Norwegian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c068_f = OBSColumn(
            id='t003c068_f',
            name='Norwegian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c069_t = OBSColumn(
            id='t003c069_t',
            name='Oromo (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c069_m = OBSColumn(
            id='t003c069_m',
            name='Oromo (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c069_f = OBSColumn(
            id='t003c069_f',
            name='Oromo (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c070_t = OBSColumn(
            id='t003c070_t',
            name='Panjabi (Punjabi) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c070_m = OBSColumn(
            id='t003c070_m',
            name='Panjabi (Punjabi) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c070_f = OBSColumn(
            id='t003c070_f',
            name='Panjabi (Punjabi) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c071_t = OBSColumn(
            id='t003c071_t',
            name='Pashto (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c071_m = OBSColumn(
            id='t003c071_m',
            name='Pashto (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c071_f = OBSColumn(
            id='t003c071_f',
            name='Pashto (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c072_t = OBSColumn(
            id='t003c072_t',
            name='Persian (Farsi) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c072_m = OBSColumn(
            id='t003c072_m',
            name='Persian (Farsi) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c072_f = OBSColumn(
            id='t003c072_f',
            name='Persian (Farsi) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c073_t = OBSColumn(
            id='t003c073_t',
            name='Polish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c073_m = OBSColumn(
            id='t003c073_m',
            name='Polish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c073_f = OBSColumn(
            id='t003c073_f',
            name='Polish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c074_t = OBSColumn(
            id='t003c074_t',
            name='Portuguese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c074_m = OBSColumn(
            id='t003c074_m',
            name='Portuguese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c074_f = OBSColumn(
            id='t003c074_f',
            name='Portuguese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c075_t = OBSColumn(
            id='t003c075_t',
            name='Romanian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c075_m = OBSColumn(
            id='t003c075_m',
            name='Romanian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c075_f = OBSColumn(
            id='t003c075_f',
            name='Romanian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c076_t = OBSColumn(
            id='t003c076_t',
            name='Rundi (Kirundi) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c076_m = OBSColumn(
            id='t003c076_m',
            name='Rundi (Kirundi) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c076_f = OBSColumn(
            id='t003c076_f',
            name='Rundi (Kirundi) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c077_t = OBSColumn(
            id='t003c077_t',
            name='Russian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c077_m = OBSColumn(
            id='t003c077_m',
            name='Russian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c077_f = OBSColumn(
            id='t003c077_f',
            name='Russian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c078_t = OBSColumn(
            id='t003c078_t',
            name='Rwanda (Kinyarwanda) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c078_m = OBSColumn(
            id='t003c078_m',
            name='Rwanda (Kinyarwanda) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c078_f = OBSColumn(
            id='t003c078_f',
            name='Rwanda (Kinyarwanda) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c079_t = OBSColumn(
            id='t003c079_t',
            name='Semitic languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c079_m = OBSColumn(
            id='t003c079_m',
            name='Semitic languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c079_f = OBSColumn(
            id='t003c079_f',
            name='Semitic languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c080_t = OBSColumn(
            id='t003c080_t',
            name='Serbian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c080_m = OBSColumn(
            id='t003c080_m',
            name='Serbian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c080_f = OBSColumn(
            id='t003c080_f',
            name='Serbian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c081_t = OBSColumn(
            id='t003c081_t',
            name='Serbo-Croatian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c081_m = OBSColumn(
            id='t003c081_m',
            name='Serbo-Croatian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c081_f = OBSColumn(
            id='t003c081_f',
            name='Serbo-Croatian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c082_t = OBSColumn(
            id='t003c082_t',
            name='Shanghainese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c082_m = OBSColumn(
            id='t003c082_m',
            name='Shanghainese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c082_f = OBSColumn(
            id='t003c082_f',
            name='Shanghainese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c083_t = OBSColumn(
            id='t003c083_t',
            name='Sign languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c083_m = OBSColumn(
            id='t003c083_m',
            name='Sign languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c083_f = OBSColumn(
            id='t003c083_f',
            name='Sign languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c084_t = OBSColumn(
            id='t003c084_t',
            name='Sindhi (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c084_m = OBSColumn(
            id='t003c084_m',
            name='Sindhi (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c084_f = OBSColumn(
            id='t003c084_f',
            name='Sindhi (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c085_t = OBSColumn(
            id='t003c085_t',
            name='Sinhala (Sinhalese) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c085_m = OBSColumn(
            id='t003c085_m',
            name='Sinhala (Sinhalese) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c085_f = OBSColumn(
            id='t003c085_f',
            name='Sinhala (Sinhalese) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c086_t = OBSColumn(
            id='t003c086_t',
            name='Sino-Tibetan languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c086_m = OBSColumn(
            id='t003c086_m',
            name='Sino-Tibetan languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c086_f = OBSColumn(
            id='t003c086_f',
            name='Sino-Tibetan languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c087_t = OBSColumn(
            id='t003c087_t',
            name='Slavic languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c087_m = OBSColumn(
            id='t003c087_m',
            name='Slavic languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c087_f = OBSColumn(
            id='t003c087_f',
            name='Slavic languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c088_t = OBSColumn(
            id='t003c088_t',
            name='Slovak (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c088_m = OBSColumn(
            id='t003c088_m',
            name='Slovak (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c088_f = OBSColumn(
            id='t003c088_f',
            name='Slovak (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c089_t = OBSColumn(
            id='t003c089_t',
            name='Slovenian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c089_m = OBSColumn(
            id='t003c089_m',
            name='Slovenian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c089_f = OBSColumn(
            id='t003c089_f',
            name='Slovenian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c090_t = OBSColumn(
            id='t003c090_t',
            name='Somali (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c090_m = OBSColumn(
            id='t003c090_m',
            name='Somali (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c090_f = OBSColumn(
            id='t003c090_f',
            name='Somali (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c091_t = OBSColumn(
            id='t003c091_t',
            name='Spanish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c091_m = OBSColumn(
            id='t003c091_m',
            name='Spanish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c091_f = OBSColumn(
            id='t003c091_f',
            name='Spanish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c092_t = OBSColumn(
            id='t003c092_t',
            name='Swahili (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c092_m = OBSColumn(
            id='t003c092_m',
            name='Swahili (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c092_f = OBSColumn(
            id='t003c092_f',
            name='Swahili (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c093_t = OBSColumn(
            id='t003c093_t',
            name='Swedish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c093_m = OBSColumn(
            id='t003c093_m',
            name='Swedish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c093_f = OBSColumn(
            id='t003c093_f',
            name='Swedish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c094_t = OBSColumn(
            id='t003c094_t',
            name='Tagalog (Pilipino, Filipino) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c094_m = OBSColumn(
            id='t003c094_m',
            name='Tagalog (Pilipino, Filipino) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c094_f = OBSColumn(
            id='t003c094_f',
            name='Tagalog (Pilipino, Filipino) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c095_t = OBSColumn(
            id='t003c095_t',
            name='Taiwanese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c095_m = OBSColumn(
            id='t003c095_m',
            name='Taiwanese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c095_f = OBSColumn(
            id='t003c095_f',
            name='Taiwanese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c096_t = OBSColumn(
            id='t003c096_t',
            name='Tamil (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c096_m = OBSColumn(
            id='t003c096_m',
            name='Tamil (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c096_f = OBSColumn(
            id='t003c096_f',
            name='Tamil (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c097_t = OBSColumn(
            id='t003c097_t',
            name='Telugu (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c097_m = OBSColumn(
            id='t003c097_m',
            name='Telugu (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c097_f = OBSColumn(
            id='t003c097_f',
            name='Telugu (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c098_t = OBSColumn(
            id='t003c098_t',
            name='Thai (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c098_m = OBSColumn(
            id='t003c098_m',
            name='Thai (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c098_f = OBSColumn(
            id='t003c098_f',
            name='Thai (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c099_t = OBSColumn(
            id='t003c099_t',
            name='Tibetan languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c099_m = OBSColumn(
            id='t003c099_m',
            name='Tibetan languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c099_f = OBSColumn(
            id='t003c099_f',
            name='Tibetan languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c100_t = OBSColumn(
            id='t003c100_t',
            name='Tigrigna (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c100_m = OBSColumn(
            id='t003c100_m',
            name='Tigrigna (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c100_f = OBSColumn(
            id='t003c100_f',
            name='Tigrigna (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c101_t = OBSColumn(
            id='t003c101_t',
            name='Turkish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c101_m = OBSColumn(
            id='t003c101_m',
            name='Turkish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c101_f = OBSColumn(
            id='t003c101_f',
            name='Turkish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c102_t = OBSColumn(
            id='t003c102_t',
            name='Ukrainian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c102_m = OBSColumn(
            id='t003c102_m',
            name='Ukrainian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c102_f = OBSColumn(
            id='t003c102_f',
            name='Ukrainian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c103_t = OBSColumn(
            id='t003c103_t',
            name='Urdu (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c103_m = OBSColumn(
            id='t003c103_m',
            name='Urdu (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c103_f = OBSColumn(
            id='t003c103_f',
            name='Urdu (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c104_t = OBSColumn(
            id='t003c104_t',
            name='Vietnamese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c104_m = OBSColumn(
            id='t003c104_m',
            name='Vietnamese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c104_f = OBSColumn(
            id='t003c104_f',
            name='Vietnamese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c105_t = OBSColumn(
            id='t003c105_t',
            name='Yiddish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_t: DENOMINATOR },)

        t003c105_m = OBSColumn(
            id='t003c105_m',
            name='Yiddish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_m: DENOMINATOR },)

        t003c105_f = OBSColumn(
            id='t003c105_f',
            name='Yiddish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c016_f: DENOMINATOR },)

        t003c106_t = OBSColumn(
            id='t003c106_t',
            name='Other languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c005_t: DENOMINATOR },)

        t003c106_m = OBSColumn(
            id='t003c106_m',
            name='Other languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c005_m: DENOMINATOR },)

        t003c106_f = OBSColumn(
            id='t003c106_f',
            name='Other languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c005_f: DENOMINATOR },)

        t003c107_t = OBSColumn(
            id='t003c107_t',
            name='Detailed mother tongue - Multiple responses (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_t: DENOMINATOR },)

        t003c107_m = OBSColumn(
            id='t003c107_m',
            name='Detailed mother tongue - Multiple responses (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_m: DENOMINATOR },)

        t003c107_f = OBSColumn(
            id='t003c107_f',
            name='Detailed mother tongue - Multiple responses (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_f: DENOMINATOR },)

        t003c108_t = OBSColumn(
            id='t003c108_t',
            name='English and French (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c107_t: DENOMINATOR },)

        t003c108_m = OBSColumn(
            id='t003c108_m',
            name='English and French (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c107_m: DENOMINATOR },)

        t003c108_f = OBSColumn(
            id='t003c108_f',
            name='English and French (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c107_f: DENOMINATOR },)

        t003c109_t = OBSColumn(
            id='t003c109_t',
            name='English and non-official language (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c107_t: DENOMINATOR },)

        t003c109_m = OBSColumn(
            id='t003c109_m',
            name='English and non-official language (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c107_m: DENOMINATOR },)

        t003c109_f = OBSColumn(
            id='t003c109_f',
            name='English and non-official language (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c107_f: DENOMINATOR },)

        t003c110_t = OBSColumn(
            id='t003c110_t',
            name='French and non-official language (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c107_t: DENOMINATOR },)

        t003c110_m = OBSColumn(
            id='t003c110_m',
            name='French and non-official language (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c107_m: DENOMINATOR },)

        t003c110_f = OBSColumn(
            id='t003c110_f',
            name='French and non-official language (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c107_f: DENOMINATOR },)

        t003c111_t = OBSColumn(
            id='t003c111_t',
            name='English, French and non-official language (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c107_t: DENOMINATOR },)

        t003c111_m = OBSColumn(
            id='t003c111_m',
            name='English, French and non-official language (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c107_m: DENOMINATOR },)

        t003c111_f = OBSColumn(
            id='t003c111_f',
            name='English, French and non-official language (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t003c107_f: DENOMINATOR },)

        t004c002_t = OBSColumn(
            id='t004c002_t',
            name='Other language spoken regularly at home - None (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_t: DENOMINATOR },)

        t004c002_m = OBSColumn(
            id='t004c002_m',
            name='Other language spoken regularly at home - None (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_m: DENOMINATOR },)

        t004c002_f = OBSColumn(
            id='t004c002_f',
            name='Other language spoken regularly at home - None (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_f: DENOMINATOR },)

        t004c003_t = OBSColumn(
            id='t004c003_t',
            name='Other language spoken regularly at home - Single responses (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_t: DENOMINATOR },)

        t004c003_m = OBSColumn(
            id='t004c003_m',
            name='Other language spoken regularly at home - Single responses (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_m: DENOMINATOR },)

        t004c003_f = OBSColumn(
            id='t004c003_f',
            name='Other language spoken regularly at home - Single responses (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_f: DENOMINATOR },)

        t004c004_t = OBSColumn(
            id='t004c004_t',
            name='English (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c003_t: DENOMINATOR },)

        t004c004_m = OBSColumn(
            id='t004c004_m',
            name='English (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c003_m: DENOMINATOR },)

        t004c004_f = OBSColumn(
            id='t004c004_f',
            name='English (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c003_f: DENOMINATOR },)

        t004c005_t = OBSColumn(
            id='t004c005_t',
            name='French (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c003_t: DENOMINATOR },)

        t004c005_m = OBSColumn(
            id='t004c005_m',
            name='French (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c003_m: DENOMINATOR },)

        t004c005_f = OBSColumn(
            id='t004c005_f',
            name='French (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c003_f: DENOMINATOR },)

        t004c006_t = OBSColumn(
            id='t004c006_t',
            name='Non-official languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c003_t: DENOMINATOR },)

        t004c006_m = OBSColumn(
            id='t004c006_m',
            name='Non-official languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c003_m: DENOMINATOR },)

        t004c006_f = OBSColumn(
            id='t004c006_f',
            name='Non-official languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c003_f: DENOMINATOR },)

        t004c007_t = OBSColumn(
            id='t004c007_t',
            name='Selected Aboriginal languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c006_t: DENOMINATOR },)

        t004c007_m = OBSColumn(
            id='t004c007_m',
            name='Selected Aboriginal languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c006_m: DENOMINATOR },)

        t004c007_f = OBSColumn(
            id='t004c007_f',
            name='Selected Aboriginal languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c006_f: DENOMINATOR },)

        t004c008_t = OBSColumn(
            id='t004c008_t',
            name='Atikamekw (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_t: DENOMINATOR },)

        t004c008_m = OBSColumn(
            id='t004c008_m',
            name='Atikamekw (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_m: DENOMINATOR },)

        t004c008_f = OBSColumn(
            id='t004c008_f',
            name='Atikamekw (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_f: DENOMINATOR },)

        t004c009_t = OBSColumn(
            id='t004c009_t',
            name='Cree, n.o.s. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_t: DENOMINATOR },)

        t004c009_m = OBSColumn(
            id='t004c009_m',
            name='Cree, n.o.s. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_m: DENOMINATOR },)

        t004c009_f = OBSColumn(
            id='t004c009_f',
            name='Cree, n.o.s. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_f: DENOMINATOR },)

        t004c010_t = OBSColumn(
            id='t004c010_t',
            name='Dene (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_t: DENOMINATOR },)

        t004c010_m = OBSColumn(
            id='t004c010_m',
            name='Dene (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_m: DENOMINATOR },)

        t004c010_f = OBSColumn(
            id='t004c010_f',
            name='Dene (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_f: DENOMINATOR },)

        t004c011_t = OBSColumn(
            id='t004c011_t',
            name='Innu/Montagnais (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_t: DENOMINATOR },)

        t004c011_m = OBSColumn(
            id='t004c011_m',
            name='Innu/Montagnais (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_m: DENOMINATOR },)

        t004c011_f = OBSColumn(
            id='t004c011_f',
            name='Innu/Montagnais (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_f: DENOMINATOR },)

        t004c012_t = OBSColumn(
            id='t004c012_t',
            name='Inuktitut (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_t: DENOMINATOR },)

        t004c012_m = OBSColumn(
            id='t004c012_m',
            name='Inuktitut (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_m: DENOMINATOR },)

        t004c012_f = OBSColumn(
            id='t004c012_f',
            name='Inuktitut (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_f: DENOMINATOR },)

        t004c013_t = OBSColumn(
            id='t004c013_t',
            name='Mi\'kmaq (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_t: DENOMINATOR },)

        t004c013_m = OBSColumn(
            id='t004c013_m',
            name='Mi\'kmaq (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_m: DENOMINATOR },)

        t004c013_f = OBSColumn(
            id='t004c013_f',
            name='Mi\'kmaq (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_f: DENOMINATOR },)

        t004c014_t = OBSColumn(
            id='t004c014_t',
            name='Ojibway (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_t: DENOMINATOR },)

        t004c014_m = OBSColumn(
            id='t004c014_m',
            name='Ojibway (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_m: DENOMINATOR },)

        t004c014_f = OBSColumn(
            id='t004c014_f',
            name='Ojibway (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_f: DENOMINATOR },)

        t004c015_t = OBSColumn(
            id='t004c015_t',
            name='Oji-Cree (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_t: DENOMINATOR },)

        t004c015_m = OBSColumn(
            id='t004c015_m',
            name='Oji-Cree (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_m: DENOMINATOR },)

        t004c015_f = OBSColumn(
            id='t004c015_f',
            name='Oji-Cree (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_f: DENOMINATOR },)

        t004c016_t = OBSColumn(
            id='t004c016_t',
            name='Stoney (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_t: DENOMINATOR },)

        t004c016_m = OBSColumn(
            id='t004c016_m',
            name='Stoney (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_m: DENOMINATOR },)

        t004c016_f = OBSColumn(
            id='t004c016_f',
            name='Stoney (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c007_f: DENOMINATOR },)

        t004c017_t = OBSColumn(
            id='t004c017_t',
            name='Selected non-Aboriginal languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c006_t: DENOMINATOR },)

        t004c017_m = OBSColumn(
            id='t004c017_m',
            name='Selected non-Aboriginal languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c006_m: DENOMINATOR },)

        t004c017_f = OBSColumn(
            id='t004c017_f',
            name='Selected non-Aboriginal languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c006_f: DENOMINATOR },)

        t004c018_t = OBSColumn(
            id='t004c018_t',
            name='African languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c018_m = OBSColumn(
            id='t004c018_m',
            name='African languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c018_f = OBSColumn(
            id='t004c018_f',
            name='African languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c019_t = OBSColumn(
            id='t004c019_t',
            name='Afrikaans (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c019_m = OBSColumn(
            id='t004c019_m',
            name='Afrikaans (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c019_f = OBSColumn(
            id='t004c019_f',
            name='Afrikaans (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c020_t = OBSColumn(
            id='t004c020_t',
            name='Akan (Twi) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c020_m = OBSColumn(
            id='t004c020_m',
            name='Akan (Twi) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c020_f = OBSColumn(
            id='t004c020_f',
            name='Akan (Twi) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c021_t = OBSColumn(
            id='t004c021_t',
            name='Albanian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c021_m = OBSColumn(
            id='t004c021_m',
            name='Albanian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c021_f = OBSColumn(
            id='t004c021_f',
            name='Albanian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c022_t = OBSColumn(
            id='t004c022_t',
            name='Amharic (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c022_m = OBSColumn(
            id='t004c022_m',
            name='Amharic (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c022_f = OBSColumn(
            id='t004c022_f',
            name='Amharic (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c023_t = OBSColumn(
            id='t004c023_t',
            name='Arabic (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c023_m = OBSColumn(
            id='t004c023_m',
            name='Arabic (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c023_f = OBSColumn(
            id='t004c023_f',
            name='Arabic (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c024_t = OBSColumn(
            id='t004c024_t',
            name='Armenian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c024_m = OBSColumn(
            id='t004c024_m',
            name='Armenian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c024_f = OBSColumn(
            id='t004c024_f',
            name='Armenian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c025_t = OBSColumn(
            id='t004c025_t',
            name='Bantu languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c025_m = OBSColumn(
            id='t004c025_m',
            name='Bantu languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c025_f = OBSColumn(
            id='t004c025_f',
            name='Bantu languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c026_t = OBSColumn(
            id='t004c026_t',
            name='Bengali (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c026_m = OBSColumn(
            id='t004c026_m',
            name='Bengali (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c026_f = OBSColumn(
            id='t004c026_f',
            name='Bengali (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c027_t = OBSColumn(
            id='t004c027_t',
            name='Berber languages (Kabyle) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c027_m = OBSColumn(
            id='t004c027_m',
            name='Berber languages (Kabyle) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c027_f = OBSColumn(
            id='t004c027_f',
            name='Berber languages (Kabyle) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c028_t = OBSColumn(
            id='t004c028_t',
            name='Bisayan languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c028_m = OBSColumn(
            id='t004c028_m',
            name='Bisayan languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c028_f = OBSColumn(
            id='t004c028_f',
            name='Bisayan languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c029_t = OBSColumn(
            id='t004c029_t',
            name='Bosnian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c029_m = OBSColumn(
            id='t004c029_m',
            name='Bosnian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c029_f = OBSColumn(
            id='t004c029_f',
            name='Bosnian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c030_t = OBSColumn(
            id='t004c030_t',
            name='Bulgarian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c030_m = OBSColumn(
            id='t004c030_m',
            name='Bulgarian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c030_f = OBSColumn(
            id='t004c030_f',
            name='Bulgarian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c031_t = OBSColumn(
            id='t004c031_t',
            name='Burmese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c031_m = OBSColumn(
            id='t004c031_m',
            name='Burmese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c031_f = OBSColumn(
            id='t004c031_f',
            name='Burmese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c032_t = OBSColumn(
            id='t004c032_t',
            name='Cantonese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c032_m = OBSColumn(
            id='t004c032_m',
            name='Cantonese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c032_f = OBSColumn(
            id='t004c032_f',
            name='Cantonese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c033_t = OBSColumn(
            id='t004c033_t',
            name='Chinese, n.o.s. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c033_m = OBSColumn(
            id='t004c033_m',
            name='Chinese, n.o.s. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c033_f = OBSColumn(
            id='t004c033_f',
            name='Chinese, n.o.s. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c034_t = OBSColumn(
            id='t004c034_t',
            name='Creoles (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c034_m = OBSColumn(
            id='t004c034_m',
            name='Creoles (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c034_f = OBSColumn(
            id='t004c034_f',
            name='Creoles (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c035_t = OBSColumn(
            id='t004c035_t',
            name='Croatian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c035_m = OBSColumn(
            id='t004c035_m',
            name='Croatian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c035_f = OBSColumn(
            id='t004c035_f',
            name='Croatian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c036_t = OBSColumn(
            id='t004c036_t',
            name='Czech (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c036_m = OBSColumn(
            id='t004c036_m',
            name='Czech (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c036_f = OBSColumn(
            id='t004c036_f',
            name='Czech (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c037_t = OBSColumn(
            id='t004c037_t',
            name='Danish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c037_m = OBSColumn(
            id='t004c037_m',
            name='Danish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c037_f = OBSColumn(
            id='t004c037_f',
            name='Danish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c038_t = OBSColumn(
            id='t004c038_t',
            name='Dutch (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c038_m = OBSColumn(
            id='t004c038_m',
            name='Dutch (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c038_f = OBSColumn(
            id='t004c038_f',
            name='Dutch (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c039_t = OBSColumn(
            id='t004c039_t',
            name='Estonian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c039_m = OBSColumn(
            id='t004c039_m',
            name='Estonian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c039_f = OBSColumn(
            id='t004c039_f',
            name='Estonian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c040_t = OBSColumn(
            id='t004c040_t',
            name='Finnish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c040_m = OBSColumn(
            id='t004c040_m',
            name='Finnish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c040_f = OBSColumn(
            id='t004c040_f',
            name='Finnish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c041_t = OBSColumn(
            id='t004c041_t',
            name='Flemish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c041_m = OBSColumn(
            id='t004c041_m',
            name='Flemish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c041_f = OBSColumn(
            id='t004c041_f',
            name='Flemish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c042_t = OBSColumn(
            id='t004c042_t',
            name='Fukien (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c042_m = OBSColumn(
            id='t004c042_m',
            name='Fukien (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c042_f = OBSColumn(
            id='t004c042_f',
            name='Fukien (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c043_t = OBSColumn(
            id='t004c043_t',
            name='German (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c043_m = OBSColumn(
            id='t004c043_m',
            name='German (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c043_f = OBSColumn(
            id='t004c043_f',
            name='German (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c044_t = OBSColumn(
            id='t004c044_t',
            name='Greek (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c044_m = OBSColumn(
            id='t004c044_m',
            name='Greek (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c044_f = OBSColumn(
            id='t004c044_f',
            name='Greek (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c045_t = OBSColumn(
            id='t004c045_t',
            name='Gujarati (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c045_m = OBSColumn(
            id='t004c045_m',
            name='Gujarati (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c045_f = OBSColumn(
            id='t004c045_f',
            name='Gujarati (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c046_t = OBSColumn(
            id='t004c046_t',
            name='Hakka (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c046_m = OBSColumn(
            id='t004c046_m',
            name='Hakka (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c046_f = OBSColumn(
            id='t004c046_f',
            name='Hakka (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c047_t = OBSColumn(
            id='t004c047_t',
            name='Hebrew (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c047_m = OBSColumn(
            id='t004c047_m',
            name='Hebrew (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c047_f = OBSColumn(
            id='t004c047_f',
            name='Hebrew (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c048_t = OBSColumn(
            id='t004c048_t',
            name='Hindi (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c048_m = OBSColumn(
            id='t004c048_m',
            name='Hindi (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c048_f = OBSColumn(
            id='t004c048_f',
            name='Hindi (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c049_t = OBSColumn(
            id='t004c049_t',
            name='Hungarian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c049_m = OBSColumn(
            id='t004c049_m',
            name='Hungarian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c049_f = OBSColumn(
            id='t004c049_f',
            name='Hungarian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c050_t = OBSColumn(
            id='t004c050_t',
            name='Ilocano (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c050_m = OBSColumn(
            id='t004c050_m',
            name='Ilocano (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c050_f = OBSColumn(
            id='t004c050_f',
            name='Ilocano (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c051_t = OBSColumn(
            id='t004c051_t',
            name='Indo-Iranian languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c051_m = OBSColumn(
            id='t004c051_m',
            name='Indo-Iranian languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c051_f = OBSColumn(
            id='t004c051_f',
            name='Indo-Iranian languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c052_t = OBSColumn(
            id='t004c052_t',
            name='Italian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c052_m = OBSColumn(
            id='t004c052_m',
            name='Italian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c052_f = OBSColumn(
            id='t004c052_f',
            name='Italian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c053_t = OBSColumn(
            id='t004c053_t',
            name='Japanese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c053_m = OBSColumn(
            id='t004c053_m',
            name='Japanese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c053_f = OBSColumn(
            id='t004c053_f',
            name='Japanese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c054_t = OBSColumn(
            id='t004c054_t',
            name='Khmer (Cambodian) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c054_m = OBSColumn(
            id='t004c054_m',
            name='Khmer (Cambodian) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c054_f = OBSColumn(
            id='t004c054_f',
            name='Khmer (Cambodian) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c055_t = OBSColumn(
            id='t004c055_t',
            name='Korean (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c055_m = OBSColumn(
            id='t004c055_m',
            name='Korean (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c055_f = OBSColumn(
            id='t004c055_f',
            name='Korean (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c056_t = OBSColumn(
            id='t004c056_t',
            name='Kurdish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c056_m = OBSColumn(
            id='t004c056_m',
            name='Kurdish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c056_f = OBSColumn(
            id='t004c056_f',
            name='Kurdish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c057_t = OBSColumn(
            id='t004c057_t',
            name='Lao (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c057_m = OBSColumn(
            id='t004c057_m',
            name='Lao (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c057_f = OBSColumn(
            id='t004c057_f',
            name='Lao (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c058_t = OBSColumn(
            id='t004c058_t',
            name='Latvian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c058_m = OBSColumn(
            id='t004c058_m',
            name='Latvian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c058_f = OBSColumn(
            id='t004c058_f',
            name='Latvian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c059_t = OBSColumn(
            id='t004c059_t',
            name='Lingala (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c059_m = OBSColumn(
            id='t004c059_m',
            name='Lingala (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c059_f = OBSColumn(
            id='t004c059_f',
            name='Lingala (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c060_t = OBSColumn(
            id='t004c060_t',
            name='Lithuanian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c060_m = OBSColumn(
            id='t004c060_m',
            name='Lithuanian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c060_f = OBSColumn(
            id='t004c060_f',
            name='Lithuanian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c061_t = OBSColumn(
            id='t004c061_t',
            name='Macedonian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c061_m = OBSColumn(
            id='t004c061_m',
            name='Macedonian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c061_f = OBSColumn(
            id='t004c061_f',
            name='Macedonian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c062_t = OBSColumn(
            id='t004c062_t',
            name='Malay (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c062_m = OBSColumn(
            id='t004c062_m',
            name='Malay (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c062_f = OBSColumn(
            id='t004c062_f',
            name='Malay (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c063_t = OBSColumn(
            id='t004c063_t',
            name='Malayalam (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c063_m = OBSColumn(
            id='t004c063_m',
            name='Malayalam (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c063_f = OBSColumn(
            id='t004c063_f',
            name='Malayalam (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c064_t = OBSColumn(
            id='t004c064_t',
            name='Maltese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c064_m = OBSColumn(
            id='t004c064_m',
            name='Maltese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c064_f = OBSColumn(
            id='t004c064_f',
            name='Maltese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c065_t = OBSColumn(
            id='t004c065_t',
            name='Mandarin (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c065_m = OBSColumn(
            id='t004c065_m',
            name='Mandarin (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c065_f = OBSColumn(
            id='t004c065_f',
            name='Mandarin (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c066_t = OBSColumn(
            id='t004c066_t',
            name='Marathi (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c066_m = OBSColumn(
            id='t004c066_m',
            name='Marathi (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c066_f = OBSColumn(
            id='t004c066_f',
            name='Marathi (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c067_t = OBSColumn(
            id='t004c067_t',
            name='Nepali (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c067_m = OBSColumn(
            id='t004c067_m',
            name='Nepali (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c067_f = OBSColumn(
            id='t004c067_f',
            name='Nepali (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c068_t = OBSColumn(
            id='t004c068_t',
            name='Niger-Congo languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c068_m = OBSColumn(
            id='t004c068_m',
            name='Niger-Congo languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c068_f = OBSColumn(
            id='t004c068_f',
            name='Niger-Congo languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c069_t = OBSColumn(
            id='t004c069_t',
            name='Norwegian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c069_m = OBSColumn(
            id='t004c069_m',
            name='Norwegian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c069_f = OBSColumn(
            id='t004c069_f',
            name='Norwegian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c070_t = OBSColumn(
            id='t004c070_t',
            name='Oromo (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c070_m = OBSColumn(
            id='t004c070_m',
            name='Oromo (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c070_f = OBSColumn(
            id='t004c070_f',
            name='Oromo (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c071_t = OBSColumn(
            id='t004c071_t',
            name='Panjabi (Punjabi) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c071_m = OBSColumn(
            id='t004c071_m',
            name='Panjabi (Punjabi) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c071_f = OBSColumn(
            id='t004c071_f',
            name='Panjabi (Punjabi) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c072_t = OBSColumn(
            id='t004c072_t',
            name='Pashto (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c072_m = OBSColumn(
            id='t004c072_m',
            name='Pashto (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c072_f = OBSColumn(
            id='t004c072_f',
            name='Pashto (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c073_t = OBSColumn(
            id='t004c073_t',
            name='Persian (Farsi) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c073_m = OBSColumn(
            id='t004c073_m',
            name='Persian (Farsi) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c073_f = OBSColumn(
            id='t004c073_f',
            name='Persian (Farsi) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c074_t = OBSColumn(
            id='t004c074_t',
            name='Polish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c074_m = OBSColumn(
            id='t004c074_m',
            name='Polish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c074_f = OBSColumn(
            id='t004c074_f',
            name='Polish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c075_t = OBSColumn(
            id='t004c075_t',
            name='Portuguese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c075_m = OBSColumn(
            id='t004c075_m',
            name='Portuguese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c075_f = OBSColumn(
            id='t004c075_f',
            name='Portuguese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c076_t = OBSColumn(
            id='t004c076_t',
            name='Romanian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c076_m = OBSColumn(
            id='t004c076_m',
            name='Romanian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c076_f = OBSColumn(
            id='t004c076_f',
            name='Romanian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c077_t = OBSColumn(
            id='t004c077_t',
            name='Rundi (Kirundi) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c077_m = OBSColumn(
            id='t004c077_m',
            name='Rundi (Kirundi) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c077_f = OBSColumn(
            id='t004c077_f',
            name='Rundi (Kirundi) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c078_t = OBSColumn(
            id='t004c078_t',
            name='Russian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c078_m = OBSColumn(
            id='t004c078_m',
            name='Russian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c078_f = OBSColumn(
            id='t004c078_f',
            name='Russian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c079_t = OBSColumn(
            id='t004c079_t',
            name='Rwanda (Kinyarwanda) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c079_m = OBSColumn(
            id='t004c079_m',
            name='Rwanda (Kinyarwanda) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c079_f = OBSColumn(
            id='t004c079_f',
            name='Rwanda (Kinyarwanda) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c080_t = OBSColumn(
            id='t004c080_t',
            name='Semitic languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c080_m = OBSColumn(
            id='t004c080_m',
            name='Semitic languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c080_f = OBSColumn(
            id='t004c080_f',
            name='Semitic languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c081_t = OBSColumn(
            id='t004c081_t',
            name='Serbian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c081_m = OBSColumn(
            id='t004c081_m',
            name='Serbian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c081_f = OBSColumn(
            id='t004c081_f',
            name='Serbian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c082_t = OBSColumn(
            id='t004c082_t',
            name='Serbo-Croatian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c082_m = OBSColumn(
            id='t004c082_m',
            name='Serbo-Croatian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c082_f = OBSColumn(
            id='t004c082_f',
            name='Serbo-Croatian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c083_t = OBSColumn(
            id='t004c083_t',
            name='Shanghainese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c083_m = OBSColumn(
            id='t004c083_m',
            name='Shanghainese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c083_f = OBSColumn(
            id='t004c083_f',
            name='Shanghainese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c084_t = OBSColumn(
            id='t004c084_t',
            name='Sign languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c084_m = OBSColumn(
            id='t004c084_m',
            name='Sign languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c084_f = OBSColumn(
            id='t004c084_f',
            name='Sign languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c085_t = OBSColumn(
            id='t004c085_t',
            name='Sindhi (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c085_m = OBSColumn(
            id='t004c085_m',
            name='Sindhi (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c085_f = OBSColumn(
            id='t004c085_f',
            name='Sindhi (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c086_t = OBSColumn(
            id='t004c086_t',
            name='Sinhala (Sinhalese) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c086_m = OBSColumn(
            id='t004c086_m',
            name='Sinhala (Sinhalese) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c086_f = OBSColumn(
            id='t004c086_f',
            name='Sinhala (Sinhalese) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c087_t = OBSColumn(
            id='t004c087_t',
            name='Sino-Tibetan languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c087_m = OBSColumn(
            id='t004c087_m',
            name='Sino-Tibetan languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c087_f = OBSColumn(
            id='t004c087_f',
            name='Sino-Tibetan languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c088_t = OBSColumn(
            id='t004c088_t',
            name='Slavic languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c088_m = OBSColumn(
            id='t004c088_m',
            name='Slavic languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c088_f = OBSColumn(
            id='t004c088_f',
            name='Slavic languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c089_t = OBSColumn(
            id='t004c089_t',
            name='Slovak (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c089_m = OBSColumn(
            id='t004c089_m',
            name='Slovak (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c089_f = OBSColumn(
            id='t004c089_f',
            name='Slovak (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c090_t = OBSColumn(
            id='t004c090_t',
            name='Slovenian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c090_m = OBSColumn(
            id='t004c090_m',
            name='Slovenian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c090_f = OBSColumn(
            id='t004c090_f',
            name='Slovenian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c091_t = OBSColumn(
            id='t004c091_t',
            name='Somali (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c091_m = OBSColumn(
            id='t004c091_m',
            name='Somali (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c091_f = OBSColumn(
            id='t004c091_f',
            name='Somali (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c092_t = OBSColumn(
            id='t004c092_t',
            name='Spanish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c092_m = OBSColumn(
            id='t004c092_m',
            name='Spanish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c092_f = OBSColumn(
            id='t004c092_f',
            name='Spanish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c093_t = OBSColumn(
            id='t004c093_t',
            name='Swahili (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c093_m = OBSColumn(
            id='t004c093_m',
            name='Swahili (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c093_f = OBSColumn(
            id='t004c093_f',
            name='Swahili (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c094_t = OBSColumn(
            id='t004c094_t',
            name='Swedish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c094_m = OBSColumn(
            id='t004c094_m',
            name='Swedish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c094_f = OBSColumn(
            id='t004c094_f',
            name='Swedish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c095_t = OBSColumn(
            id='t004c095_t',
            name='Tagalog (Pilipino, Filipino) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c095_m = OBSColumn(
            id='t004c095_m',
            name='Tagalog (Pilipino, Filipino) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c095_f = OBSColumn(
            id='t004c095_f',
            name='Tagalog (Pilipino, Filipino) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c096_t = OBSColumn(
            id='t004c096_t',
            name='Taiwanese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c096_m = OBSColumn(
            id='t004c096_m',
            name='Taiwanese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c096_f = OBSColumn(
            id='t004c096_f',
            name='Taiwanese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c097_t = OBSColumn(
            id='t004c097_t',
            name='Tamil (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c097_m = OBSColumn(
            id='t004c097_m',
            name='Tamil (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c097_f = OBSColumn(
            id='t004c097_f',
            name='Tamil (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c098_t = OBSColumn(
            id='t004c098_t',
            name='Telugu (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c098_m = OBSColumn(
            id='t004c098_m',
            name='Telugu (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c098_f = OBSColumn(
            id='t004c098_f',
            name='Telugu (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c099_t = OBSColumn(
            id='t004c099_t',
            name='Thai (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c099_m = OBSColumn(
            id='t004c099_m',
            name='Thai (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c099_f = OBSColumn(
            id='t004c099_f',
            name='Thai (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c100_t = OBSColumn(
            id='t004c100_t',
            name='Tibetan languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c100_m = OBSColumn(
            id='t004c100_m',
            name='Tibetan languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c100_f = OBSColumn(
            id='t004c100_f',
            name='Tibetan languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c101_t = OBSColumn(
            id='t004c101_t',
            name='Tigrigna (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c101_m = OBSColumn(
            id='t004c101_m',
            name='Tigrigna (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c101_f = OBSColumn(
            id='t004c101_f',
            name='Tigrigna (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c102_t = OBSColumn(
            id='t004c102_t',
            name='Turkish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c102_m = OBSColumn(
            id='t004c102_m',
            name='Turkish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c102_f = OBSColumn(
            id='t004c102_f',
            name='Turkish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c103_t = OBSColumn(
            id='t004c103_t',
            name='Ukrainian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c103_m = OBSColumn(
            id='t004c103_m',
            name='Ukrainian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c103_f = OBSColumn(
            id='t004c103_f',
            name='Ukrainian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c104_t = OBSColumn(
            id='t004c104_t',
            name='Urdu (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c104_m = OBSColumn(
            id='t004c104_m',
            name='Urdu (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c104_f = OBSColumn(
            id='t004c104_f',
            name='Urdu (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c105_t = OBSColumn(
            id='t004c105_t',
            name='Vietnamese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c105_m = OBSColumn(
            id='t004c105_m',
            name='Vietnamese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c105_f = OBSColumn(
            id='t004c105_f',
            name='Vietnamese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c106_t = OBSColumn(
            id='t004c106_t',
            name='Yiddish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_t: DENOMINATOR },)

        t004c106_m = OBSColumn(
            id='t004c106_m',
            name='Yiddish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_m: DENOMINATOR },)

        t004c106_f = OBSColumn(
            id='t004c106_f',
            name='Yiddish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c017_f: DENOMINATOR },)

        t004c107_t = OBSColumn(
            id='t004c107_t',
            name='Other languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c006_t: DENOMINATOR },)

        t004c107_m = OBSColumn(
            id='t004c107_m',
            name='Other languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c006_m: DENOMINATOR },)

        t004c107_f = OBSColumn(
            id='t004c107_f',
            name='Other languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c006_f: DENOMINATOR },)

        t004c108_t = OBSColumn(
            id='t004c108_t',
            name='Other language spoken regularly at home - Multiple responses (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_t: DENOMINATOR },)

        t004c108_m = OBSColumn(
            id='t004c108_m',
            name='Other language spoken regularly at home - Multiple responses (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_m: DENOMINATOR },)

        t004c108_f = OBSColumn(
            id='t004c108_f',
            name='Other language spoken regularly at home - Multiple responses (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_f: DENOMINATOR },)

        t004c109_t = OBSColumn(
            id='t004c109_t',
            name='English and French (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c108_t: DENOMINATOR },)

        t004c109_m = OBSColumn(
            id='t004c109_m',
            name='English and French (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c108_m: DENOMINATOR },)

        t004c109_f = OBSColumn(
            id='t004c109_f',
            name='English and French (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c108_f: DENOMINATOR },)

        t004c110_t = OBSColumn(
            id='t004c110_t',
            name='English and non-official language (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c108_t: DENOMINATOR },)

        t004c110_m = OBSColumn(
            id='t004c110_m',
            name='English and non-official language (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c108_m: DENOMINATOR },)

        t004c110_f = OBSColumn(
            id='t004c110_f',
            name='English and non-official language (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c108_f: DENOMINATOR },)

        t004c111_t = OBSColumn(
            id='t004c111_t',
            name='French and non-official language (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c108_t: DENOMINATOR },)

        t004c111_m = OBSColumn(
            id='t004c111_m',
            name='French and non-official language (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c108_m: DENOMINATOR },)

        t004c111_f = OBSColumn(
            id='t004c111_f',
            name='French and non-official language (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c108_f: DENOMINATOR },)

        t004c112_t = OBSColumn(
            id='t004c112_t',
            name='English, French and non-official language (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c108_t: DENOMINATOR },)

        t004c112_m = OBSColumn(
            id='t004c112_m',
            name='English, French and non-official language (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c108_m: DENOMINATOR },)

        t004c112_f = OBSColumn(
            id='t004c112_f',
            name='English, French and non-official language (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t004c108_f: DENOMINATOR },)

        # FIXME
        # How is this different from ca.statcan.cols_nhs.t009c001_t?  Do we
        # need a new unit for "family"?
        #
        # This should also have a description of family according to the
        # Census.
        t005c001_t = OBSColumn(
            id='t005c001_t',
            name='Total number of census families in private households',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={},)

        t005c002_t = OBSColumn(
            id='t005c002_t',
            name='Size of census family: 2 persons',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c003_t = OBSColumn(
            id='t005c003_t',
            name='Size of census family: 3 persons',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c004_t = OBSColumn(
            id='t005c004_t',
            name='Size of census family: 4 persons',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c005_t = OBSColumn(
            id='t005c005_t',
            name='Size of census family: 5 or more persons',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c007_t = OBSColumn(
            id='t005c007_t',
            name='Couple families',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c008_t = OBSColumn(
            id='t005c008_t',
            name='Couple families - Married',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c007_t: DENOMINATOR },)

        t005c009_t = OBSColumn(
            id='t005c009_t',
            name='Couple families - Married - without children at home',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c008_t: DENOMINATOR },)

        t005c010_t = OBSColumn(
            id='t005c010_t',
            name='Couple families - Married - with children at home',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c008_t: DENOMINATOR },)

        t005c011_t = OBSColumn(
            id='t005c011_t',
            name='Couple families - Married - with 1 child at home',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c010_t: DENOMINATOR },)

        t005c012_t = OBSColumn(
            id='t005c012_t',
            name='Couple families - Married - with 2 children at home',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c010_t: DENOMINATOR },)

        t005c013_t = OBSColumn(
            id='t005c013_t',
            name='Couple families - Married - with 3+ children at home',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c010_t: DENOMINATOR },)

        t005c014_t = OBSColumn(
            id='t005c014_t',
            name='Couple families - Common-law',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c007_t: DENOMINATOR },)

        t005c015_t = OBSColumn(
            id='t005c015_t',
            name='Couple families - Common-law - without children at home',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c014_t: DENOMINATOR },)

        t005c016_t = OBSColumn(
            id='t005c016_t',
            name='Couple families - Common-law - with children at home',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c014_t: DENOMINATOR },)

        t005c017_t = OBSColumn(
            id='t005c017_t',
            name='Couple families - Common-law - with 1 child at home',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c016_t: DENOMINATOR },)

        t005c018_t = OBSColumn(
            id='t005c018_t',
            name='Couple families - Common-law - with 2 children at home',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c016_t: DENOMINATOR },)

        t005c019_t = OBSColumn(
            id='t005c019_t',
            name='Couple families - Common-law - with 3+ children at home',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c016_t: DENOMINATOR },)

        t005c020_t = OBSColumn(
            id='t005c020_t',
            name='Lone-parent families',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c021_t = OBSColumn(
            id='t005c021_t',
            name='Lone-parent families - Female parent',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c020_t: DENOMINATOR },)

        t005c022_t = OBSColumn(
            id='t005c022_t',
            name='Lone-parent families - Female parent - 1 child',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c021_t: DENOMINATOR },)

        t005c023_t = OBSColumn(
            id='t005c023_t',
            name='Lone-parent families - Female parent - 2 children',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c021_t: DENOMINATOR },)

        t005c024_t = OBSColumn(
            id='t005c024_t',
            name='Lone-parent families - Female parent - 3+ children',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c021_t: DENOMINATOR },)

        t005c025_t = OBSColumn(
            id='t005c025_t',
            name='Lone-parent families - Male parent',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c020_t: DENOMINATOR },)

        t005c026_t = OBSColumn(
            id='t005c026_t',
            name='Lone-parent families - Male parent - 1 child',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c025_t: DENOMINATOR },)

        t005c027_t = OBSColumn(
            id='t005c027_t',
            name='Lone-parent families - Male parent - 2 children',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c025_t: DENOMINATOR },)

        t005c028_t = OBSColumn(
            id='t005c028_t',
            name='Lone-parent families - Male parent - 3+ children',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['families']],
            targets={ t005c025_t: DENOMINATOR },)

        t005c029_t = OBSColumn(
            id='t005c029_t',
            name='Total children in census families in private households',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={},)

        t005c030_t = OBSColumn(
            id='t005c030_t',
            name='Total children in census families in private households (<6 years)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t005c029_t: DENOMINATOR },)

        t005c031_t = OBSColumn(
            id='t005c031_t',
            name='Total children in census families in private households (6-14 years)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t005c029_t: DENOMINATOR },)

        t005c032_t = OBSColumn(
            id='t005c032_t',
            name='Total children in census families in private households (15-17 years)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t005c029_t: DENOMINATOR },)

        t005c033_t = OBSColumn(
            id='t005c033_t',
            name='Total children in census families in private households (18-24 years)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t005c029_t: DENOMINATOR },)

        t005c034_t = OBSColumn(
            id='t005c034_t',
            name='Total children in census families in private households (25+ years)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t005c029_t: DENOMINATOR },)

        t005c035_t = OBSColumn(
            id='t005c035_t',
            name='Average number of children at home per census family',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_people, subsections['families']],
            targets={},)

        t006c002_t = OBSColumn(
            id='t006c002_t',
            name='First official language spoken - English (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_t: DENOMINATOR },)

        t006c002_m = OBSColumn(
            id='t006c002_m',
            name='First official language spoken - English (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_m: DENOMINATOR },)

        t006c002_f = OBSColumn(
            id='t006c002_f',
            name='First official language spoken - English (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_f: DENOMINATOR },)

        t006c003_t = OBSColumn(
            id='t006c003_t',
            name='First official language spoken - French (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_t: DENOMINATOR },)

        t006c003_m = OBSColumn(
            id='t006c003_m',
            name='First official language spoken - French (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_m: DENOMINATOR },)

        t006c003_f = OBSColumn(
            id='t006c003_f',
            name='First official language spoken - French (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_f: DENOMINATOR },)

        t006c004_t = OBSColumn(
            id='t006c004_t',
            name='First official language spoken - English and French (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_t: DENOMINATOR },)

        t006c004_m = OBSColumn(
            id='t006c004_m',
            name='First official language spoken - English and French (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_m: DENOMINATOR },)

        t006c004_f = OBSColumn(
            id='t006c004_f',
            name='First official language spoken - English and French (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_f: DENOMINATOR },)

        t006c005_t = OBSColumn(
            id='t006c005_t',
            name='First official language spoken - Neither English nor French (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_t: DENOMINATOR },)

        t006c005_m = OBSColumn(
            id='t006c005_m',
            name='First official language spoken - Neither English nor French (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_m: DENOMINATOR },)

        t006c005_f = OBSColumn(
            id='t006c005_f',
            name='First official language spoken - Neither English nor French (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_f: DENOMINATOR },)

        t006c006_t = OBSColumn(
            id='t006c006_t',
            name='Official language minority (number) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={},)

        t006c006_m = OBSColumn(
            id='t006c006_m',
            name='Official language minority (number) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={},)

        t006c006_f = OBSColumn(
            id='t006c006_f',
            name='Official language minority (number) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={},)

        t006c007_t = OBSColumn(
            id='t006c007_t',
            name='Official language minority (percentage) (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['language']],
            targets={},)

        t006c007_m = OBSColumn(
            id='t006c007_m',
            name='Official language minority (percentage) (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['language']],
            targets={},)

        t006c007_f = OBSColumn(
            id='t006c007_f',
            name='Official language minority (percentage) (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['language']],
            targets={},)

        # FIXME
        # what's the difference between this column and
        # 'ca.statcan.cols_nhs.t001c001_t'?

        # a single column can exist in multiple tables.  if the data is
        # identical, then these should be combined.
        t007c001_t = OBSColumn(
            id='t007c001_t',
            name='Total number of persons in private households (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={},)

        t007c001_m = OBSColumn(
            id='t007c001_m',
            name='Total number of persons in private households (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={},)

        t007c001_f = OBSColumn(
            id='t007c001_f',
            name='Total number of persons in private households (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={},)

        t007c002_t = OBSColumn(
            id='t007c002_t',
            name='Number of persons NOT IN census families (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c001_t: DENOMINATOR },)

        t007c002_m = OBSColumn(
            id='t007c002_m',
            name='Number of persons NOT IN census families (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c001_m: DENOMINATOR },)

        t007c002_f = OBSColumn(
            id='t007c002_f',
            name='Number of persons NOT IN census families (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c001_f: DENOMINATOR },)

        t007c003_t = OBSColumn(
            id='t007c003_t',
            name='Number of persons NOT IN census families - Living with relatives (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c002_t: DENOMINATOR },)

        t007c003_m = OBSColumn(
            id='t007c003_m',
            name='Number of persons NOT IN census families - Living with relatives (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c002_m: DENOMINATOR },)

        t007c003_f = OBSColumn(
            id='t007c003_f',
            name='Number of persons NOT IN census families - Living with relatives (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c002_f: DENOMINATOR },)

        t007c004_t = OBSColumn(
            id='t007c004_t',
            name='Number of persons NOT IN census families - Living with non-relatives only (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c002_t: DENOMINATOR },)

        t007c004_m = OBSColumn(
            id='t007c004_m',
            name='Number of persons NOT IN census families - Living with non-relatives only (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c002_m: DENOMINATOR },)

        t007c004_f = OBSColumn(
            id='t007c004_f',
            name='Number of persons NOT IN census families - Living with non-relatives only (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c002_f: DENOMINATOR },)

        t007c005_t = OBSColumn(
            id='t007c005_t',
            name='Number of persons NOT IN census families - Living alone (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c002_t: DENOMINATOR },)

        t007c005_m = OBSColumn(
            id='t007c005_m',
            name='Number of persons NOT IN census families - Living alone (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c002_m: DENOMINATOR },)

        t007c005_f = OBSColumn(
            id='t007c005_f',
            name='Number of persons NOT IN census families - Living alone (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c002_f: DENOMINATOR },)

        t007c006_t = OBSColumn(
            id='t007c006_t',
            name='Number of persons IN census families (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c001_t: DENOMINATOR },)

        t007c006_m = OBSColumn(
            id='t007c006_m',
            name='Number of persons IN census families (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c001_m: DENOMINATOR },)

        t007c006_f = OBSColumn(
            id='t007c006_f',
            name='Number of persons IN census families (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c001_f: DENOMINATOR },)

        t007c007_t = OBSColumn(
            id='t007c007_t',
            name='Average number of persons per census family',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_people, subsections['housing']],
            targets={},)

        t007c008_t = OBSColumn(
            id='t007c008_t',
            name='Number of persons aged 65+ in private households (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={},)

        t007c008_m = OBSColumn(
            id='t007c008_m',
            name='Number of persons aged 65+ in private households (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={},)

        t007c008_f = OBSColumn(
            id='t007c008_f',
            name='Number of persons aged 65+ in private households (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={},)

        t007c009_t = OBSColumn(
            id='t007c009_t',
            name='Number of persons aged 65+ in private households - NOT IN census families (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c008_t: DENOMINATOR },)

        t007c009_m = OBSColumn(
            id='t007c009_m',
            name='Number of persons aged 65+ in private households - NOT IN census families (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c008_m: DENOMINATOR },)

        t007c009_f = OBSColumn(
            id='t007c009_f',
            name='Number of persons aged 65+ in private households - NOT IN census families (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c008_f: DENOMINATOR },)

        t007c010_t = OBSColumn(
            id='t007c010_t',
            name='Number of persons aged 65+ in private households - NOT IN census families - Living with relatives (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c009_t: DENOMINATOR },)

        t007c010_m = OBSColumn(
            id='t007c010_m',
            name='Number of persons aged 65+ in private households - NOT IN census families - Living with relatives (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c009_m: DENOMINATOR },)

        t007c010_f = OBSColumn(
            id='t007c010_f',
            name='Number of persons aged 65+ in private households - NOT IN census families - Living with relatives (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c009_f: DENOMINATOR },)

        t007c011_t = OBSColumn(
            id='t007c011_t',
            name='Number of persons aged 65+ in private households - NOT IN census families - Living with non-relatives only (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c009_t: DENOMINATOR },)

        t007c011_m = OBSColumn(
            id='t007c011_m',
            name='Number of persons aged 65+ in private households - NOT IN census families - Living with non-relatives only (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c009_m: DENOMINATOR },)

        t007c011_f = OBSColumn(
            id='t007c011_f',
            name='Number of persons aged 65+ in private households - NOT IN census families - Living with non-relatives only (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c009_f: DENOMINATOR },)

        t007c012_t = OBSColumn(
            id='t007c012_t',
            name='Number of persons aged 65+ in private households - NOT IN census families - Living alone (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c009_t: DENOMINATOR },)

        t007c012_m = OBSColumn(
            id='t007c012_m',
            name='Number of persons aged 65+ in private households - NOT IN census families - Living alone (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c009_m: DENOMINATOR },)

        t007c012_f = OBSColumn(
            id='t007c012_f',
            name='Number of persons aged 65+ in private households - NOT IN census families - Living alone (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c009_f: DENOMINATOR },)

        t007c013_t = OBSColumn(
            id='t007c013_t',
            name='Number of persons aged 65+ in private households - IN census families (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c008_t: DENOMINATOR },)

        t007c013_m = OBSColumn(
            id='t007c013_m',
            name='Number of persons aged 65+ in private households - IN census families (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c008_m: DENOMINATOR },)

        t007c013_f = OBSColumn(
            id='t007c013_f',
            name='Number of persons aged 65+ in private households - IN census families (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t007c008_f: DENOMINATOR },)

        t007c014_t = OBSColumn(
            id='t007c014_t',
            name='Number of private households',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={},)

        # FIXME
        # A description of what a census-family household should be included
        # here.  Links can be made using restructured text formatting.
        # `link here <https://www12.statcan.gc.ca/census-recensement/2011/ref/dict/fam004-eng.cfm>`_
        t007c015_t = OBSColumn(
            id='t007c015_t',
            name='Number of private households - Census-family',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c014_t: DENOMINATOR },)

        t007c016_t = OBSColumn(
            id='t007c016_t',
            name='Number of private households - Census-family - One family only',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c015_t: DENOMINATOR },)

        t007c017_t = OBSColumn(
            id='t007c017_t',
            name='Number of private households - Census-family - One family only - Couple',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c016_t: DENOMINATOR },)

        t007c018_t = OBSColumn(
            id='t007c018_t',
            name='Number of private households - Census-family - One family only - Couple - w/o children',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c017_t: DENOMINATOR },)

        t007c019_t = OBSColumn(
            id='t007c019_t',
            name='Number of private households - Census-family - One family only - Couple - w/ children',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c017_t: DENOMINATOR },)

        t007c020_t = OBSColumn(
            id='t007c020_t',
            name='Number of private households - Census-family - One family only - Lone-parent',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c016_t: DENOMINATOR },)

        t007c021_t = OBSColumn(
            id='t007c021_t',
            name='Number of private households - Census-family - Other family',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c015_t: DENOMINATOR },)

        t007c022_t = OBSColumn(
            id='t007c022_t',
            name='Number of private households - Census-family - Other family - One family with persons not in a census family',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c021_t: DENOMINATOR },)

        t007c023_t = OBSColumn(
            id='t007c023_t',
            name='Number of private households - Census-family - Other family - One family with persons not in a census family - Couple',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c022_t: DENOMINATOR },)

        t007c024_t = OBSColumn(
            id='t007c024_t',
            name='Number of private households - Census-family - Other family - One family with persons not in a census family - Couple - w/o children',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c023_t: DENOMINATOR },)

        t007c025_t = OBSColumn(
            id='t007c025_t',
            name='Number of private households - Census-family - Other family - One family with persons not in a census family - Couple - w/ children',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c023_t: DENOMINATOR },)

        t007c026_t = OBSColumn(
            id='t007c026_t',
            name='Number of private households - Census-family - Other family - One family with persons not in a census family - Lone-parent',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c022_t: DENOMINATOR },)

        t007c027_t = OBSColumn(
            id='t007c027_t',
            name='Number of private households - Census-family - Other family - 2+ family households',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c021_t: DENOMINATOR },)

        t007c028_t = OBSColumn(
            id='t007c028_t',
            name='Number of private households - Non-census-family',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c014_t: DENOMINATOR },)

        t007c029_t = OBSColumn(
            id='t007c029_t',
            name='Number of private households - Non-census-family - 1 person',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c028_t: DENOMINATOR },)

        t007c030_t = OBSColumn(
            id='t007c030_t',
            name='Number of private households - Non-census-family - 2+ person',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c028_t: DENOMINATOR },)

        t007c032_t = OBSColumn(
            id='t007c032_t',
            name='Number of private households living in - Single-detached house',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c014_t: DENOMINATOR },)

        t007c033_t = OBSColumn(
            id='t007c033_t',
            name='Number of private households living in - Apartment, 5+ storeys',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c014_t: DENOMINATOR },)

        t007c034_t = OBSColumn(
            id='t007c034_t',
            name='Number of private households living in - Movable dwelling',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c014_t: DENOMINATOR },)

        t007c035_t = OBSColumn(
            id='t007c035_t',
            name='Number of private households living in - Other',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c014_t: DENOMINATOR },)

        t007c036_t = OBSColumn(
            id='t007c036_t',
            name='Number of private households living in - Other - Semi-detached house',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c035_t: DENOMINATOR },)

        t007c037_t = OBSColumn(
            id='t007c037_t',
            name='Number of private households living in - Other - Row house',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c035_t: DENOMINATOR },)

        t007c038_t = OBSColumn(
            id='t007c038_t',
            name='Number of private households living in - Other - Apartment, duplex',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c035_t: DENOMINATOR },)

        t007c039_t = OBSColumn(
            id='t007c039_t',
            name='Number of private households living in - Other - Apartment, <5 storeys',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c035_t: DENOMINATOR },)

        t007c040_t = OBSColumn(
            id='t007c040_t',
            name='Number of private households living in - Other - Other single-attached house',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c035_t: DENOMINATOR },)

        t007c042_t = OBSColumn(
            id='t007c042_t',
            name='Number of private households - w/ 1 person',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c014_t: DENOMINATOR },)

        t007c043_t = OBSColumn(
            id='t007c043_t',
            name='Number of private households - w/ 2 persons',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c014_t: DENOMINATOR },)

        t007c044_t = OBSColumn(
            id='t007c044_t',
            name='Number of private households - w/ 3 persons',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c014_t: DENOMINATOR },)

        t007c045_t = OBSColumn(
            id='t007c045_t',
            name='Number of private households - w/ 4 persons',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c014_t: DENOMINATOR },)

        t007c046_t = OBSColumn(
            id='t007c046_t',
            name='Number of private households - w/ 5 persons',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c014_t: DENOMINATOR },)

        t007c047_t = OBSColumn(
            id='t007c047_t',
            name='Number of private households - w/ 6+ persons',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_households, subsections['housing']],
            targets={ t007c014_t: DENOMINATOR },)

        t007c048_t = OBSColumn(
            id='t007c048_t',
            name='Number of persons in private households',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={},)

        t007c049_t = OBSColumn(
            id='t007c049_t',
            name='Average number of persons in private households',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_people, subsections['housing']],
            targets={},)

        t008c002_t = OBSColumn(
            id='t008c002_t',
            name='Knowledge of official languages - English only (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_t: DENOMINATOR },)

        t008c002_m = OBSColumn(
            id='t008c002_m',
            name='Knowledge of official languages - English only (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_m: DENOMINATOR },)

        t008c002_f = OBSColumn(
            id='t008c002_f',
            name='Knowledge of official languages - English only (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_f: DENOMINATOR },)

        t008c003_t = OBSColumn(
            id='t008c003_t',
            name='Knowledge of official languages - French only (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_t: DENOMINATOR },)

        t008c003_m = OBSColumn(
            id='t008c003_m',
            name='Knowledge of official languages - French only (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_m: DENOMINATOR },)

        t008c003_f = OBSColumn(
            id='t008c003_f',
            name='Knowledge of official languages - French only (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_f: DENOMINATOR },)

        t008c004_t = OBSColumn(
            id='t008c004_t',
            name='Knowledge of official languages - English and French (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_t: DENOMINATOR },)

        t008c004_m = OBSColumn(
            id='t008c004_m',
            name='Knowledge of official languages - English and French (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_m: DENOMINATOR },)

        t008c004_f = OBSColumn(
            id='t008c004_f',
            name='Knowledge of official languages - English and French (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_f: DENOMINATOR },)

        t008c005_t = OBSColumn(
            id='t008c005_t',
            name='Knowledge of official languages - Neither English nor French (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_t: DENOMINATOR },)

        t008c005_m = OBSColumn(
            id='t008c005_m',
            name='Knowledge of official languages - Neither English nor French (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_m: DENOMINATOR },)

        t008c005_f = OBSColumn(
            id='t008c005_f',
            name='Knowledge of official languages - Neither English nor French (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t002c001_f: DENOMINATOR },)

        # FIXME
        # Generally, we should never have "by" in a the name of a measurement,
        # and any columns targetting an OBSColumn that had a "by" in its name
        # need more qualification to not be ambiguous.
        #
        # For example, the columns below this should be prepended with
        # "Population" (like "Population married or living...") to avoid
        # name ambiguity with the number of Households with that same
        # marital status.
        #
        # These are duplicative of `ca.statcan.cols_nhs.t005c001`, which has
        # been removed.  Any column and table depending on that should use this
        # instead.
        t009c001_t = OBSColumn(
            id='t009c001_t',
            name='Total population 15 years and over',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={},)

        t009c001_m = OBSColumn(
            id='t009c001_m',
            name='Male population 15 years and over',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={},)

        t009c001_f = OBSColumn(
            id='t009c001_f',
            name='Female Population 15 years and over',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={},)

        t009c002_t = OBSColumn(
            id='t009c002_t',
            name='Married or living with a common-law partner (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c001_t: DENOMINATOR },)

        t009c002_m = OBSColumn(
            id='t009c002_m',
            name='Married or living with a common-law partner (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c001_m: DENOMINATOR },)

        t009c002_f = OBSColumn(
            id='t009c002_f',
            name='Married or living with a common-law partner (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c001_f: DENOMINATOR },)

        t009c003_t = OBSColumn(
            id='t009c003_t',
            name='Married (and not separated) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c002_t: DENOMINATOR },)

        t009c003_m = OBSColumn(
            id='t009c003_m',
            name='Married (and not separated) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c002_m: DENOMINATOR },)

        t009c003_f = OBSColumn(
            id='t009c003_f',
            name='Married (and not separated) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c002_f: DENOMINATOR },)

        t009c004_t = OBSColumn(
            id='t009c004_t',
            name='Living common law (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c002_t: DENOMINATOR },)

        t009c004_m = OBSColumn(
            id='t009c004_m',
            name='Living common law (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c002_m: DENOMINATOR },)

        t009c004_f = OBSColumn(
            id='t009c004_f',
            name='Living common law (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c002_f: DENOMINATOR },)

        t009c005_t = OBSColumn(
            id='t009c005_t',
            name='Not married and not living with a common-law partner (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c001_t: DENOMINATOR },)

        t009c005_m = OBSColumn(
            id='t009c005_m',
            name='Not married and not living with a common-law partner (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c001_m: DENOMINATOR },)

        t009c005_f = OBSColumn(
            id='t009c005_f',
            name='Not married and not living with a common-law partner (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c001_f: DENOMINATOR },)

        t009c006_t = OBSColumn(
            id='t009c006_t',
            name='Single (never legally married) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c005_t: DENOMINATOR },)

        t009c006_m = OBSColumn(
            id='t009c006_m',
            name='Single (never legally married) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c005_m: DENOMINATOR },)

        t009c006_f = OBSColumn(
            id='t009c006_f',
            name='Single (never legally married) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c005_f: DENOMINATOR },)

        t009c007_t = OBSColumn(
            id='t009c007_t',
            name='Separated (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c005_t: DENOMINATOR },)

        t009c007_m = OBSColumn(
            id='t009c007_m',
            name='Separated (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c005_m: DENOMINATOR },)

        t009c007_f = OBSColumn(
            id='t009c007_f',
            name='Separated (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c005_f: DENOMINATOR },)

        t009c008_t = OBSColumn(
            id='t009c008_t',
            name='Divorced (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c005_t: DENOMINATOR },)

        t009c008_m = OBSColumn(
            id='t009c008_m',
            name='Divorced (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c005_m: DENOMINATOR },)

        t009c008_f = OBSColumn(
            id='t009c008_f',
            name='Divorced (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c005_f: DENOMINATOR },)

        t009c009_t = OBSColumn(
            id='t009c009_t',
            name='Widowed (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c005_t: DENOMINATOR },)

        t009c009_m = OBSColumn(
            id='t009c009_m',
            name='Widowed (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c005_m: DENOMINATOR },)

        t009c009_f = OBSColumn(
            id='t009c009_f',
            name='Widowed (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t009c005_f: DENOMINATOR },)

        t010c002_t = OBSColumn(
            id='t010c002_t',
            name='Population in 2006',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['segments']],
            targets={},)

        t010c003_t = OBSColumn(
            id='t010c003_t',
            name='2006 to 2011 population change (%)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['segments']],
            targets={},)

        # FIXME
        # A description of what a "private dwelling" is should be here.
        t010c004_t = OBSColumn(
            id='t010c004_t',
            name='Total private dwellings',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_housing, subsections['segments']],
            targets={},)

        # FIXME
        # A description of what "usual residents" are should be here
        t010c005_t = OBSColumn(
            id='t010c005_t',
            name='Private dwellings occupied by usual residents',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_housing, subsections['segments']],
            targets={},)

        t010c006_t = OBSColumn(
            id='t010c006_t',
            name='Population density per square kilometre',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, subsections['segments']],
            targets={},)

        # FIXME
        # There should be a "unit_sq_km" added for this
        t010c007_t = OBSColumn(
            id='t010c007_t',
            name='Land area (square km)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, subsections['segments']],
            targets={},)

        return OrderedDict([
            ('t001c001_t', t001c001_t),
            ('t001c001_m', t001c001_m),
            ('t001c001_f', t001c001_f),
            ('t001c002_t', t001c002_t),
            ('t001c002_m', t001c002_m),
            ('t001c002_f', t001c002_f),
            ('t001c003_t', t001c003_t),
            ('t001c003_m', t001c003_m),
            ('t001c003_f', t001c003_f),
            ('t001c004_t', t001c004_t),
            ('t001c004_m', t001c004_m),
            ('t001c004_f', t001c004_f),
            ('t001c005_t', t001c005_t),
            ('t001c005_m', t001c005_m),
            ('t001c005_f', t001c005_f),
            ('t001c006_t', t001c006_t),
            ('t001c006_m', t001c006_m),
            ('t001c006_f', t001c006_f),
            ('t001c007_t', t001c007_t),
            ('t001c007_m', t001c007_m),
            ('t001c007_f', t001c007_f),
            ('t001c008_t', t001c008_t),
            ('t001c008_m', t001c008_m),
            ('t001c008_f', t001c008_f),
            ('t001c009_t', t001c009_t),
            ('t001c009_m', t001c009_m),
            ('t001c009_f', t001c009_f),
            ('t001c010_t', t001c010_t),
            ('t001c010_m', t001c010_m),
            ('t001c010_f', t001c010_f),
            ('t001c011_t', t001c011_t),
            ('t001c011_m', t001c011_m),
            ('t001c011_f', t001c011_f),
            ('t001c012_t', t001c012_t),
            ('t001c012_m', t001c012_m),
            ('t001c012_f', t001c012_f),
            ('t001c013_t', t001c013_t),
            ('t001c013_m', t001c013_m),
            ('t001c013_f', t001c013_f),
            ('t001c014_t', t001c014_t),
            ('t001c014_m', t001c014_m),
            ('t001c014_f', t001c014_f),
            ('t001c015_t', t001c015_t),
            ('t001c015_m', t001c015_m),
            ('t001c015_f', t001c015_f),
            ('t001c016_t', t001c016_t),
            ('t001c016_m', t001c016_m),
            ('t001c016_f', t001c016_f),
            ('t001c017_t', t001c017_t),
            ('t001c017_m', t001c017_m),
            ('t001c017_f', t001c017_f),
            ('t001c018_t', t001c018_t),
            ('t001c018_m', t001c018_m),
            ('t001c018_f', t001c018_f),
            ('t001c019_t', t001c019_t),
            ('t001c019_m', t001c019_m),
            ('t001c019_f', t001c019_f),
            ('t001c020_t', t001c020_t),
            ('t001c020_m', t001c020_m),
            ('t001c020_f', t001c020_f),
            ('t001c021_t', t001c021_t),
            ('t001c021_m', t001c021_m),
            ('t001c021_f', t001c021_f),
            ('t001c022_t', t001c022_t),
            ('t001c022_m', t001c022_m),
            ('t001c022_f', t001c022_f),
            ('t001c023_t', t001c023_t),
            ('t001c023_m', t001c023_m),
            ('t001c023_f', t001c023_f),
            ('t001c024_t', t001c024_t),
            ('t001c024_m', t001c024_m),
            ('t001c024_f', t001c024_f),
            ('t001c025_t', t001c025_t),
            ('t001c025_m', t001c025_m),
            ('t001c025_f', t001c025_f),
            ('t001c026_t', t001c026_t),
            ('t001c026_m', t001c026_m),
            ('t001c026_f', t001c026_f),
            ('t002c001_t', t002c001_t),
            ('t002c001_m', t002c001_m),
            ('t002c001_f', t002c001_f),
            ('t002c002_t', t002c002_t),
            ('t002c002_m', t002c002_m),
            ('t002c002_f', t002c002_f),
            ('t002c003_t', t002c003_t),
            ('t002c003_m', t002c003_m),
            ('t002c003_f', t002c003_f),
            ('t002c004_t', t002c004_t),
            ('t002c004_m', t002c004_m),
            ('t002c004_f', t002c004_f),
            ('t002c005_t', t002c005_t),
            ('t002c005_m', t002c005_m),
            ('t002c005_f', t002c005_f),
            ('t002c006_t', t002c006_t),
            ('t002c006_m', t002c006_m),
            ('t002c006_f', t002c006_f),
            ('t002c007_t', t002c007_t),
            ('t002c007_m', t002c007_m),
            ('t002c007_f', t002c007_f),
            ('t002c008_t', t002c008_t),
            ('t002c008_m', t002c008_m),
            ('t002c008_f', t002c008_f),
            ('t002c009_t', t002c009_t),
            ('t002c009_m', t002c009_m),
            ('t002c009_f', t002c009_f),
            ('t002c010_t', t002c010_t),
            ('t002c010_m', t002c010_m),
            ('t002c010_f', t002c010_f),
            ('t002c011_t', t002c011_t),
            ('t002c011_m', t002c011_m),
            ('t002c011_f', t002c011_f),
            ('t002c012_t', t002c012_t),
            ('t002c012_m', t002c012_m),
            ('t002c012_f', t002c012_f),
            ('t002c013_t', t002c013_t),
            ('t002c013_m', t002c013_m),
            ('t002c013_f', t002c013_f),
            ('t002c014_t', t002c014_t),
            ('t002c014_m', t002c014_m),
            ('t002c014_f', t002c014_f),
            ('t002c015_t', t002c015_t),
            ('t002c015_m', t002c015_m),
            ('t002c015_f', t002c015_f),
            ('t002c016_t', t002c016_t),
            ('t002c016_m', t002c016_m),
            ('t002c016_f', t002c016_f),
            ('t002c017_t', t002c017_t),
            ('t002c017_m', t002c017_m),
            ('t002c017_f', t002c017_f),
            ('t002c018_t', t002c018_t),
            ('t002c018_m', t002c018_m),
            ('t002c018_f', t002c018_f),
            ('t002c019_t', t002c019_t),
            ('t002c019_m', t002c019_m),
            ('t002c019_f', t002c019_f),
            ('t002c020_t', t002c020_t),
            ('t002c020_m', t002c020_m),
            ('t002c020_f', t002c020_f),
            ('t002c021_t', t002c021_t),
            ('t002c021_m', t002c021_m),
            ('t002c021_f', t002c021_f),
            ('t002c022_t', t002c022_t),
            ('t002c022_m', t002c022_m),
            ('t002c022_f', t002c022_f),
            ('t002c023_t', t002c023_t),
            ('t002c023_m', t002c023_m),
            ('t002c023_f', t002c023_f),
            ('t002c024_t', t002c024_t),
            ('t002c024_m', t002c024_m),
            ('t002c024_f', t002c024_f),
            ('t002c025_t', t002c025_t),
            ('t002c025_m', t002c025_m),
            ('t002c025_f', t002c025_f),
            ('t002c026_t', t002c026_t),
            ('t002c026_m', t002c026_m),
            ('t002c026_f', t002c026_f),
            ('t002c027_t', t002c027_t),
            ('t002c027_m', t002c027_m),
            ('t002c027_f', t002c027_f),
            ('t002c028_t', t002c028_t),
            ('t002c028_m', t002c028_m),
            ('t002c028_f', t002c028_f),
            ('t002c029_t', t002c029_t),
            ('t002c029_m', t002c029_m),
            ('t002c029_f', t002c029_f),
            ('t002c030_t', t002c030_t),
            ('t002c030_m', t002c030_m),
            ('t002c030_f', t002c030_f),
            ('t002c031_t', t002c031_t),
            ('t002c031_m', t002c031_m),
            ('t002c031_f', t002c031_f),
            ('t002c032_t', t002c032_t),
            ('t002c032_m', t002c032_m),
            ('t002c032_f', t002c032_f),
            ('t002c033_t', t002c033_t),
            ('t002c033_m', t002c033_m),
            ('t002c033_f', t002c033_f),
            ('t002c034_t', t002c034_t),
            ('t002c034_m', t002c034_m),
            ('t002c034_f', t002c034_f),
            ('t002c035_t', t002c035_t),
            ('t002c035_m', t002c035_m),
            ('t002c035_f', t002c035_f),
            ('t002c036_t', t002c036_t),
            ('t002c036_m', t002c036_m),
            ('t002c036_f', t002c036_f),
            ('t002c037_t', t002c037_t),
            ('t002c037_m', t002c037_m),
            ('t002c037_f', t002c037_f),
            ('t002c038_t', t002c038_t),
            ('t002c038_m', t002c038_m),
            ('t002c038_f', t002c038_f),
            ('t002c039_t', t002c039_t),
            ('t002c039_m', t002c039_m),
            ('t002c039_f', t002c039_f),
            ('t002c040_t', t002c040_t),
            ('t002c040_m', t002c040_m),
            ('t002c040_f', t002c040_f),
            ('t002c041_t', t002c041_t),
            ('t002c041_m', t002c041_m),
            ('t002c041_f', t002c041_f),
            ('t002c042_t', t002c042_t),
            ('t002c042_m', t002c042_m),
            ('t002c042_f', t002c042_f),
            ('t002c043_t', t002c043_t),
            ('t002c043_m', t002c043_m),
            ('t002c043_f', t002c043_f),
            ('t002c044_t', t002c044_t),
            ('t002c044_m', t002c044_m),
            ('t002c044_f', t002c044_f),
            ('t002c045_t', t002c045_t),
            ('t002c045_m', t002c045_m),
            ('t002c045_f', t002c045_f),
            ('t002c046_t', t002c046_t),
            ('t002c046_m', t002c046_m),
            ('t002c046_f', t002c046_f),
            ('t002c047_t', t002c047_t),
            ('t002c047_m', t002c047_m),
            ('t002c047_f', t002c047_f),
            ('t002c048_t', t002c048_t),
            ('t002c048_m', t002c048_m),
            ('t002c048_f', t002c048_f),
            ('t002c049_t', t002c049_t),
            ('t002c049_m', t002c049_m),
            ('t002c049_f', t002c049_f),
            ('t002c050_t', t002c050_t),
            ('t002c050_m', t002c050_m),
            ('t002c050_f', t002c050_f),
            ('t002c051_t', t002c051_t),
            ('t002c051_m', t002c051_m),
            ('t002c051_f', t002c051_f),
            ('t002c052_t', t002c052_t),
            ('t002c052_m', t002c052_m),
            ('t002c052_f', t002c052_f),
            ('t002c053_t', t002c053_t),
            ('t002c053_m', t002c053_m),
            ('t002c053_f', t002c053_f),
            ('t002c054_t', t002c054_t),
            ('t002c054_m', t002c054_m),
            ('t002c054_f', t002c054_f),
            ('t002c055_t', t002c055_t),
            ('t002c055_m', t002c055_m),
            ('t002c055_f', t002c055_f),
            ('t002c056_t', t002c056_t),
            ('t002c056_m', t002c056_m),
            ('t002c056_f', t002c056_f),
            ('t002c057_t', t002c057_t),
            ('t002c057_m', t002c057_m),
            ('t002c057_f', t002c057_f),
            ('t002c058_t', t002c058_t),
            ('t002c058_m', t002c058_m),
            ('t002c058_f', t002c058_f),
            ('t002c059_t', t002c059_t),
            ('t002c059_m', t002c059_m),
            ('t002c059_f', t002c059_f),
            ('t002c060_t', t002c060_t),
            ('t002c060_m', t002c060_m),
            ('t002c060_f', t002c060_f),
            ('t002c061_t', t002c061_t),
            ('t002c061_m', t002c061_m),
            ('t002c061_f', t002c061_f),
            ('t002c062_t', t002c062_t),
            ('t002c062_m', t002c062_m),
            ('t002c062_f', t002c062_f),
            ('t002c063_t', t002c063_t),
            ('t002c063_m', t002c063_m),
            ('t002c063_f', t002c063_f),
            ('t002c064_t', t002c064_t),
            ('t002c064_m', t002c064_m),
            ('t002c064_f', t002c064_f),
            ('t002c065_t', t002c065_t),
            ('t002c065_m', t002c065_m),
            ('t002c065_f', t002c065_f),
            ('t002c066_t', t002c066_t),
            ('t002c066_m', t002c066_m),
            ('t002c066_f', t002c066_f),
            ('t002c067_t', t002c067_t),
            ('t002c067_m', t002c067_m),
            ('t002c067_f', t002c067_f),
            ('t002c068_t', t002c068_t),
            ('t002c068_m', t002c068_m),
            ('t002c068_f', t002c068_f),
            ('t002c069_t', t002c069_t),
            ('t002c069_m', t002c069_m),
            ('t002c069_f', t002c069_f),
            ('t002c070_t', t002c070_t),
            ('t002c070_m', t002c070_m),
            ('t002c070_f', t002c070_f),
            ('t002c071_t', t002c071_t),
            ('t002c071_m', t002c071_m),
            ('t002c071_f', t002c071_f),
            ('t002c072_t', t002c072_t),
            ('t002c072_m', t002c072_m),
            ('t002c072_f', t002c072_f),
            ('t002c073_t', t002c073_t),
            ('t002c073_m', t002c073_m),
            ('t002c073_f', t002c073_f),
            ('t002c074_t', t002c074_t),
            ('t002c074_m', t002c074_m),
            ('t002c074_f', t002c074_f),
            ('t002c075_t', t002c075_t),
            ('t002c075_m', t002c075_m),
            ('t002c075_f', t002c075_f),
            ('t002c076_t', t002c076_t),
            ('t002c076_m', t002c076_m),
            ('t002c076_f', t002c076_f),
            ('t002c077_t', t002c077_t),
            ('t002c077_m', t002c077_m),
            ('t002c077_f', t002c077_f),
            ('t002c078_t', t002c078_t),
            ('t002c078_m', t002c078_m),
            ('t002c078_f', t002c078_f),
            ('t002c079_t', t002c079_t),
            ('t002c079_m', t002c079_m),
            ('t002c079_f', t002c079_f),
            ('t002c080_t', t002c080_t),
            ('t002c080_m', t002c080_m),
            ('t002c080_f', t002c080_f),
            ('t002c081_t', t002c081_t),
            ('t002c081_m', t002c081_m),
            ('t002c081_f', t002c081_f),
            ('t002c082_t', t002c082_t),
            ('t002c082_m', t002c082_m),
            ('t002c082_f', t002c082_f),
            ('t002c083_t', t002c083_t),
            ('t002c083_m', t002c083_m),
            ('t002c083_f', t002c083_f),
            ('t002c084_t', t002c084_t),
            ('t002c084_m', t002c084_m),
            ('t002c084_f', t002c084_f),
            ('t002c085_t', t002c085_t),
            ('t002c085_m', t002c085_m),
            ('t002c085_f', t002c085_f),
            ('t002c086_t', t002c086_t),
            ('t002c086_m', t002c086_m),
            ('t002c086_f', t002c086_f),
            ('t002c087_t', t002c087_t),
            ('t002c087_m', t002c087_m),
            ('t002c087_f', t002c087_f),
            ('t002c088_t', t002c088_t),
            ('t002c088_m', t002c088_m),
            ('t002c088_f', t002c088_f),
            ('t002c089_t', t002c089_t),
            ('t002c089_m', t002c089_m),
            ('t002c089_f', t002c089_f),
            ('t002c090_t', t002c090_t),
            ('t002c090_m', t002c090_m),
            ('t002c090_f', t002c090_f),
            ('t002c091_t', t002c091_t),
            ('t002c091_m', t002c091_m),
            ('t002c091_f', t002c091_f),
            ('t002c092_t', t002c092_t),
            ('t002c092_m', t002c092_m),
            ('t002c092_f', t002c092_f),
            ('t002c093_t', t002c093_t),
            ('t002c093_m', t002c093_m),
            ('t002c093_f', t002c093_f),
            ('t002c094_t', t002c094_t),
            ('t002c094_m', t002c094_m),
            ('t002c094_f', t002c094_f),
            ('t002c095_t', t002c095_t),
            ('t002c095_m', t002c095_m),
            ('t002c095_f', t002c095_f),
            ('t002c096_t', t002c096_t),
            ('t002c096_m', t002c096_m),
            ('t002c096_f', t002c096_f),
            ('t002c097_t', t002c097_t),
            ('t002c097_m', t002c097_m),
            ('t002c097_f', t002c097_f),
            ('t002c098_t', t002c098_t),
            ('t002c098_m', t002c098_m),
            ('t002c098_f', t002c098_f),
            ('t002c099_t', t002c099_t),
            ('t002c099_m', t002c099_m),
            ('t002c099_f', t002c099_f),
            ('t002c100_t', t002c100_t),
            ('t002c100_m', t002c100_m),
            ('t002c100_f', t002c100_f),
            ('t002c101_t', t002c101_t),
            ('t002c101_m', t002c101_m),
            ('t002c101_f', t002c101_f),
            ('t002c102_t', t002c102_t),
            ('t002c102_m', t002c102_m),
            ('t002c102_f', t002c102_f),
            ('t002c103_t', t002c103_t),
            ('t002c103_m', t002c103_m),
            ('t002c103_f', t002c103_f),
            ('t002c104_t', t002c104_t),
            ('t002c104_m', t002c104_m),
            ('t002c104_f', t002c104_f),
            ('t002c105_t', t002c105_t),
            ('t002c105_m', t002c105_m),
            ('t002c105_f', t002c105_f),
            ('t002c106_t', t002c106_t),
            ('t002c106_m', t002c106_m),
            ('t002c106_f', t002c106_f),
            ('t002c107_t', t002c107_t),
            ('t002c107_m', t002c107_m),
            ('t002c107_f', t002c107_f),
            ('t002c108_t', t002c108_t),
            ('t002c108_m', t002c108_m),
            ('t002c108_f', t002c108_f),
            ('t002c109_t', t002c109_t),
            ('t002c109_m', t002c109_m),
            ('t002c109_f', t002c109_f),
            ('t002c110_t', t002c110_t),
            ('t002c110_m', t002c110_m),
            ('t002c110_f', t002c110_f),
            ('t002c111_t', t002c111_t),
            ('t002c111_m', t002c111_m),
            ('t002c111_f', t002c111_f),
            ('t003c002_t', t003c002_t),
            ('t003c002_m', t003c002_m),
            ('t003c002_f', t003c002_f),
            ('t003c003_t', t003c003_t),
            ('t003c003_m', t003c003_m),
            ('t003c003_f', t003c003_f),
            ('t003c004_t', t003c004_t),
            ('t003c004_m', t003c004_m),
            ('t003c004_f', t003c004_f),
            ('t003c005_t', t003c005_t),
            ('t003c005_m', t003c005_m),
            ('t003c005_f', t003c005_f),
            ('t003c006_t', t003c006_t),
            ('t003c006_m', t003c006_m),
            ('t003c006_f', t003c006_f),
            ('t003c007_t', t003c007_t),
            ('t003c007_m', t003c007_m),
            ('t003c007_f', t003c007_f),
            ('t003c008_t', t003c008_t),
            ('t003c008_m', t003c008_m),
            ('t003c008_f', t003c008_f),
            ('t003c009_t', t003c009_t),
            ('t003c009_m', t003c009_m),
            ('t003c009_f', t003c009_f),
            ('t003c010_t', t003c010_t),
            ('t003c010_m', t003c010_m),
            ('t003c010_f', t003c010_f),
            ('t003c011_t', t003c011_t),
            ('t003c011_m', t003c011_m),
            ('t003c011_f', t003c011_f),
            ('t003c012_t', t003c012_t),
            ('t003c012_m', t003c012_m),
            ('t003c012_f', t003c012_f),
            ('t003c013_t', t003c013_t),
            ('t003c013_m', t003c013_m),
            ('t003c013_f', t003c013_f),
            ('t003c014_t', t003c014_t),
            ('t003c014_m', t003c014_m),
            ('t003c014_f', t003c014_f),
            ('t003c015_t', t003c015_t),
            ('t003c015_m', t003c015_m),
            ('t003c015_f', t003c015_f),
            ('t003c016_t', t003c016_t),
            ('t003c016_m', t003c016_m),
            ('t003c016_f', t003c016_f),
            ('t003c017_t', t003c017_t),
            ('t003c017_m', t003c017_m),
            ('t003c017_f', t003c017_f),
            ('t003c018_t', t003c018_t),
            ('t003c018_m', t003c018_m),
            ('t003c018_f', t003c018_f),
            ('t003c019_t', t003c019_t),
            ('t003c019_m', t003c019_m),
            ('t003c019_f', t003c019_f),
            ('t003c020_t', t003c020_t),
            ('t003c020_m', t003c020_m),
            ('t003c020_f', t003c020_f),
            ('t003c021_t', t003c021_t),
            ('t003c021_m', t003c021_m),
            ('t003c021_f', t003c021_f),
            ('t003c022_t', t003c022_t),
            ('t003c022_m', t003c022_m),
            ('t003c022_f', t003c022_f),
            ('t003c023_t', t003c023_t),
            ('t003c023_m', t003c023_m),
            ('t003c023_f', t003c023_f),
            ('t003c024_t', t003c024_t),
            ('t003c024_m', t003c024_m),
            ('t003c024_f', t003c024_f),
            ('t003c025_t', t003c025_t),
            ('t003c025_m', t003c025_m),
            ('t003c025_f', t003c025_f),
            ('t003c026_t', t003c026_t),
            ('t003c026_m', t003c026_m),
            ('t003c026_f', t003c026_f),
            ('t003c027_t', t003c027_t),
            ('t003c027_m', t003c027_m),
            ('t003c027_f', t003c027_f),
            ('t003c028_t', t003c028_t),
            ('t003c028_m', t003c028_m),
            ('t003c028_f', t003c028_f),
            ('t003c029_t', t003c029_t),
            ('t003c029_m', t003c029_m),
            ('t003c029_f', t003c029_f),
            ('t003c030_t', t003c030_t),
            ('t003c030_m', t003c030_m),
            ('t003c030_f', t003c030_f),
            ('t003c031_t', t003c031_t),
            ('t003c031_m', t003c031_m),
            ('t003c031_f', t003c031_f),
            ('t003c032_t', t003c032_t),
            ('t003c032_m', t003c032_m),
            ('t003c032_f', t003c032_f),
            ('t003c033_t', t003c033_t),
            ('t003c033_m', t003c033_m),
            ('t003c033_f', t003c033_f),
            ('t003c034_t', t003c034_t),
            ('t003c034_m', t003c034_m),
            ('t003c034_f', t003c034_f),
            ('t003c035_t', t003c035_t),
            ('t003c035_m', t003c035_m),
            ('t003c035_f', t003c035_f),
            ('t003c036_t', t003c036_t),
            ('t003c036_m', t003c036_m),
            ('t003c036_f', t003c036_f),
            ('t003c037_t', t003c037_t),
            ('t003c037_m', t003c037_m),
            ('t003c037_f', t003c037_f),
            ('t003c038_t', t003c038_t),
            ('t003c038_m', t003c038_m),
            ('t003c038_f', t003c038_f),
            ('t003c039_t', t003c039_t),
            ('t003c039_m', t003c039_m),
            ('t003c039_f', t003c039_f),
            ('t003c040_t', t003c040_t),
            ('t003c040_m', t003c040_m),
            ('t003c040_f', t003c040_f),
            ('t003c041_t', t003c041_t),
            ('t003c041_m', t003c041_m),
            ('t003c041_f', t003c041_f),
            ('t003c042_t', t003c042_t),
            ('t003c042_m', t003c042_m),
            ('t003c042_f', t003c042_f),
            ('t003c043_t', t003c043_t),
            ('t003c043_m', t003c043_m),
            ('t003c043_f', t003c043_f),
            ('t003c044_t', t003c044_t),
            ('t003c044_m', t003c044_m),
            ('t003c044_f', t003c044_f),
            ('t003c045_t', t003c045_t),
            ('t003c045_m', t003c045_m),
            ('t003c045_f', t003c045_f),
            ('t003c046_t', t003c046_t),
            ('t003c046_m', t003c046_m),
            ('t003c046_f', t003c046_f),
            ('t003c047_t', t003c047_t),
            ('t003c047_m', t003c047_m),
            ('t003c047_f', t003c047_f),
            ('t003c048_t', t003c048_t),
            ('t003c048_m', t003c048_m),
            ('t003c048_f', t003c048_f),
            ('t003c049_t', t003c049_t),
            ('t003c049_m', t003c049_m),
            ('t003c049_f', t003c049_f),
            ('t003c050_t', t003c050_t),
            ('t003c050_m', t003c050_m),
            ('t003c050_f', t003c050_f),
            ('t003c051_t', t003c051_t),
            ('t003c051_m', t003c051_m),
            ('t003c051_f', t003c051_f),
            ('t003c052_t', t003c052_t),
            ('t003c052_m', t003c052_m),
            ('t003c052_f', t003c052_f),
            ('t003c053_t', t003c053_t),
            ('t003c053_m', t003c053_m),
            ('t003c053_f', t003c053_f),
            ('t003c054_t', t003c054_t),
            ('t003c054_m', t003c054_m),
            ('t003c054_f', t003c054_f),
            ('t003c055_t', t003c055_t),
            ('t003c055_m', t003c055_m),
            ('t003c055_f', t003c055_f),
            ('t003c056_t', t003c056_t),
            ('t003c056_m', t003c056_m),
            ('t003c056_f', t003c056_f),
            ('t003c057_t', t003c057_t),
            ('t003c057_m', t003c057_m),
            ('t003c057_f', t003c057_f),
            ('t003c058_t', t003c058_t),
            ('t003c058_m', t003c058_m),
            ('t003c058_f', t003c058_f),
            ('t003c059_t', t003c059_t),
            ('t003c059_m', t003c059_m),
            ('t003c059_f', t003c059_f),
            ('t003c060_t', t003c060_t),
            ('t003c060_m', t003c060_m),
            ('t003c060_f', t003c060_f),
            ('t003c061_t', t003c061_t),
            ('t003c061_m', t003c061_m),
            ('t003c061_f', t003c061_f),
            ('t003c062_t', t003c062_t),
            ('t003c062_m', t003c062_m),
            ('t003c062_f', t003c062_f),
            ('t003c063_t', t003c063_t),
            ('t003c063_m', t003c063_m),
            ('t003c063_f', t003c063_f),
            ('t003c064_t', t003c064_t),
            ('t003c064_m', t003c064_m),
            ('t003c064_f', t003c064_f),
            ('t003c065_t', t003c065_t),
            ('t003c065_m', t003c065_m),
            ('t003c065_f', t003c065_f),
            ('t003c066_t', t003c066_t),
            ('t003c066_m', t003c066_m),
            ('t003c066_f', t003c066_f),
            ('t003c067_t', t003c067_t),
            ('t003c067_m', t003c067_m),
            ('t003c067_f', t003c067_f),
            ('t003c068_t', t003c068_t),
            ('t003c068_m', t003c068_m),
            ('t003c068_f', t003c068_f),
            ('t003c069_t', t003c069_t),
            ('t003c069_m', t003c069_m),
            ('t003c069_f', t003c069_f),
            ('t003c070_t', t003c070_t),
            ('t003c070_m', t003c070_m),
            ('t003c070_f', t003c070_f),
            ('t003c071_t', t003c071_t),
            ('t003c071_m', t003c071_m),
            ('t003c071_f', t003c071_f),
            ('t003c072_t', t003c072_t),
            ('t003c072_m', t003c072_m),
            ('t003c072_f', t003c072_f),
            ('t003c073_t', t003c073_t),
            ('t003c073_m', t003c073_m),
            ('t003c073_f', t003c073_f),
            ('t003c074_t', t003c074_t),
            ('t003c074_m', t003c074_m),
            ('t003c074_f', t003c074_f),
            ('t003c075_t', t003c075_t),
            ('t003c075_m', t003c075_m),
            ('t003c075_f', t003c075_f),
            ('t003c076_t', t003c076_t),
            ('t003c076_m', t003c076_m),
            ('t003c076_f', t003c076_f),
            ('t003c077_t', t003c077_t),
            ('t003c077_m', t003c077_m),
            ('t003c077_f', t003c077_f),
            ('t003c078_t', t003c078_t),
            ('t003c078_m', t003c078_m),
            ('t003c078_f', t003c078_f),
            ('t003c079_t', t003c079_t),
            ('t003c079_m', t003c079_m),
            ('t003c079_f', t003c079_f),
            ('t003c080_t', t003c080_t),
            ('t003c080_m', t003c080_m),
            ('t003c080_f', t003c080_f),
            ('t003c081_t', t003c081_t),
            ('t003c081_m', t003c081_m),
            ('t003c081_f', t003c081_f),
            ('t003c082_t', t003c082_t),
            ('t003c082_m', t003c082_m),
            ('t003c082_f', t003c082_f),
            ('t003c083_t', t003c083_t),
            ('t003c083_m', t003c083_m),
            ('t003c083_f', t003c083_f),
            ('t003c084_t', t003c084_t),
            ('t003c084_m', t003c084_m),
            ('t003c084_f', t003c084_f),
            ('t003c085_t', t003c085_t),
            ('t003c085_m', t003c085_m),
            ('t003c085_f', t003c085_f),
            ('t003c086_t', t003c086_t),
            ('t003c086_m', t003c086_m),
            ('t003c086_f', t003c086_f),
            ('t003c087_t', t003c087_t),
            ('t003c087_m', t003c087_m),
            ('t003c087_f', t003c087_f),
            ('t003c088_t', t003c088_t),
            ('t003c088_m', t003c088_m),
            ('t003c088_f', t003c088_f),
            ('t003c089_t', t003c089_t),
            ('t003c089_m', t003c089_m),
            ('t003c089_f', t003c089_f),
            ('t003c090_t', t003c090_t),
            ('t003c090_m', t003c090_m),
            ('t003c090_f', t003c090_f),
            ('t003c091_t', t003c091_t),
            ('t003c091_m', t003c091_m),
            ('t003c091_f', t003c091_f),
            ('t003c092_t', t003c092_t),
            ('t003c092_m', t003c092_m),
            ('t003c092_f', t003c092_f),
            ('t003c093_t', t003c093_t),
            ('t003c093_m', t003c093_m),
            ('t003c093_f', t003c093_f),
            ('t003c094_t', t003c094_t),
            ('t003c094_m', t003c094_m),
            ('t003c094_f', t003c094_f),
            ('t003c095_t', t003c095_t),
            ('t003c095_m', t003c095_m),
            ('t003c095_f', t003c095_f),
            ('t003c096_t', t003c096_t),
            ('t003c096_m', t003c096_m),
            ('t003c096_f', t003c096_f),
            ('t003c097_t', t003c097_t),
            ('t003c097_m', t003c097_m),
            ('t003c097_f', t003c097_f),
            ('t003c098_t', t003c098_t),
            ('t003c098_m', t003c098_m),
            ('t003c098_f', t003c098_f),
            ('t003c099_t', t003c099_t),
            ('t003c099_m', t003c099_m),
            ('t003c099_f', t003c099_f),
            ('t003c100_t', t003c100_t),
            ('t003c100_m', t003c100_m),
            ('t003c100_f', t003c100_f),
            ('t003c101_t', t003c101_t),
            ('t003c101_m', t003c101_m),
            ('t003c101_f', t003c101_f),
            ('t003c102_t', t003c102_t),
            ('t003c102_m', t003c102_m),
            ('t003c102_f', t003c102_f),
            ('t003c103_t', t003c103_t),
            ('t003c103_m', t003c103_m),
            ('t003c103_f', t003c103_f),
            ('t003c104_t', t003c104_t),
            ('t003c104_m', t003c104_m),
            ('t003c104_f', t003c104_f),
            ('t003c105_t', t003c105_t),
            ('t003c105_m', t003c105_m),
            ('t003c105_f', t003c105_f),
            ('t003c106_t', t003c106_t),
            ('t003c106_m', t003c106_m),
            ('t003c106_f', t003c106_f),
            ('t003c107_t', t003c107_t),
            ('t003c107_m', t003c107_m),
            ('t003c107_f', t003c107_f),
            ('t003c108_t', t003c108_t),
            ('t003c108_m', t003c108_m),
            ('t003c108_f', t003c108_f),
            ('t003c109_t', t003c109_t),
            ('t003c109_m', t003c109_m),
            ('t003c109_f', t003c109_f),
            ('t003c110_t', t003c110_t),
            ('t003c110_m', t003c110_m),
            ('t003c110_f', t003c110_f),
            ('t003c111_t', t003c111_t),
            ('t003c111_m', t003c111_m),
            ('t003c111_f', t003c111_f),
            ('t004c002_t', t004c002_t),
            ('t004c002_m', t004c002_m),
            ('t004c002_f', t004c002_f),
            ('t004c003_t', t004c003_t),
            ('t004c003_m', t004c003_m),
            ('t004c003_f', t004c003_f),
            ('t004c004_t', t004c004_t),
            ('t004c004_m', t004c004_m),
            ('t004c004_f', t004c004_f),
            ('t004c005_t', t004c005_t),
            ('t004c005_m', t004c005_m),
            ('t004c005_f', t004c005_f),
            ('t004c006_t', t004c006_t),
            ('t004c006_m', t004c006_m),
            ('t004c006_f', t004c006_f),
            ('t004c007_t', t004c007_t),
            ('t004c007_m', t004c007_m),
            ('t004c007_f', t004c007_f),
            ('t004c008_t', t004c008_t),
            ('t004c008_m', t004c008_m),
            ('t004c008_f', t004c008_f),
            ('t004c009_t', t004c009_t),
            ('t004c009_m', t004c009_m),
            ('t004c009_f', t004c009_f),
            ('t004c010_t', t004c010_t),
            ('t004c010_m', t004c010_m),
            ('t004c010_f', t004c010_f),
            ('t004c011_t', t004c011_t),
            ('t004c011_m', t004c011_m),
            ('t004c011_f', t004c011_f),
            ('t004c012_t', t004c012_t),
            ('t004c012_m', t004c012_m),
            ('t004c012_f', t004c012_f),
            ('t004c013_t', t004c013_t),
            ('t004c013_m', t004c013_m),
            ('t004c013_f', t004c013_f),
            ('t004c014_t', t004c014_t),
            ('t004c014_m', t004c014_m),
            ('t004c014_f', t004c014_f),
            ('t004c015_t', t004c015_t),
            ('t004c015_m', t004c015_m),
            ('t004c015_f', t004c015_f),
            ('t004c016_t', t004c016_t),
            ('t004c016_m', t004c016_m),
            ('t004c016_f', t004c016_f),
            ('t004c017_t', t004c017_t),
            ('t004c017_m', t004c017_m),
            ('t004c017_f', t004c017_f),
            ('t004c018_t', t004c018_t),
            ('t004c018_m', t004c018_m),
            ('t004c018_f', t004c018_f),
            ('t004c019_t', t004c019_t),
            ('t004c019_m', t004c019_m),
            ('t004c019_f', t004c019_f),
            ('t004c020_t', t004c020_t),
            ('t004c020_m', t004c020_m),
            ('t004c020_f', t004c020_f),
            ('t004c021_t', t004c021_t),
            ('t004c021_m', t004c021_m),
            ('t004c021_f', t004c021_f),
            ('t004c022_t', t004c022_t),
            ('t004c022_m', t004c022_m),
            ('t004c022_f', t004c022_f),
            ('t004c023_t', t004c023_t),
            ('t004c023_m', t004c023_m),
            ('t004c023_f', t004c023_f),
            ('t004c024_t', t004c024_t),
            ('t004c024_m', t004c024_m),
            ('t004c024_f', t004c024_f),
            ('t004c025_t', t004c025_t),
            ('t004c025_m', t004c025_m),
            ('t004c025_f', t004c025_f),
            ('t004c026_t', t004c026_t),
            ('t004c026_m', t004c026_m),
            ('t004c026_f', t004c026_f),
            ('t004c027_t', t004c027_t),
            ('t004c027_m', t004c027_m),
            ('t004c027_f', t004c027_f),
            ('t004c028_t', t004c028_t),
            ('t004c028_m', t004c028_m),
            ('t004c028_f', t004c028_f),
            ('t004c029_t', t004c029_t),
            ('t004c029_m', t004c029_m),
            ('t004c029_f', t004c029_f),
            ('t004c030_t', t004c030_t),
            ('t004c030_m', t004c030_m),
            ('t004c030_f', t004c030_f),
            ('t004c031_t', t004c031_t),
            ('t004c031_m', t004c031_m),
            ('t004c031_f', t004c031_f),
            ('t004c032_t', t004c032_t),
            ('t004c032_m', t004c032_m),
            ('t004c032_f', t004c032_f),
            ('t004c033_t', t004c033_t),
            ('t004c033_m', t004c033_m),
            ('t004c033_f', t004c033_f),
            ('t004c034_t', t004c034_t),
            ('t004c034_m', t004c034_m),
            ('t004c034_f', t004c034_f),
            ('t004c035_t', t004c035_t),
            ('t004c035_m', t004c035_m),
            ('t004c035_f', t004c035_f),
            ('t004c036_t', t004c036_t),
            ('t004c036_m', t004c036_m),
            ('t004c036_f', t004c036_f),
            ('t004c037_t', t004c037_t),
            ('t004c037_m', t004c037_m),
            ('t004c037_f', t004c037_f),
            ('t004c038_t', t004c038_t),
            ('t004c038_m', t004c038_m),
            ('t004c038_f', t004c038_f),
            ('t004c039_t', t004c039_t),
            ('t004c039_m', t004c039_m),
            ('t004c039_f', t004c039_f),
            ('t004c040_t', t004c040_t),
            ('t004c040_m', t004c040_m),
            ('t004c040_f', t004c040_f),
            ('t004c041_t', t004c041_t),
            ('t004c041_m', t004c041_m),
            ('t004c041_f', t004c041_f),
            ('t004c042_t', t004c042_t),
            ('t004c042_m', t004c042_m),
            ('t004c042_f', t004c042_f),
            ('t004c043_t', t004c043_t),
            ('t004c043_m', t004c043_m),
            ('t004c043_f', t004c043_f),
            ('t004c044_t', t004c044_t),
            ('t004c044_m', t004c044_m),
            ('t004c044_f', t004c044_f),
            ('t004c045_t', t004c045_t),
            ('t004c045_m', t004c045_m),
            ('t004c045_f', t004c045_f),
            ('t004c046_t', t004c046_t),
            ('t004c046_m', t004c046_m),
            ('t004c046_f', t004c046_f),
            ('t004c047_t', t004c047_t),
            ('t004c047_m', t004c047_m),
            ('t004c047_f', t004c047_f),
            ('t004c048_t', t004c048_t),
            ('t004c048_m', t004c048_m),
            ('t004c048_f', t004c048_f),
            ('t004c049_t', t004c049_t),
            ('t004c049_m', t004c049_m),
            ('t004c049_f', t004c049_f),
            ('t004c050_t', t004c050_t),
            ('t004c050_m', t004c050_m),
            ('t004c050_f', t004c050_f),
            ('t004c051_t', t004c051_t),
            ('t004c051_m', t004c051_m),
            ('t004c051_f', t004c051_f),
            ('t004c052_t', t004c052_t),
            ('t004c052_m', t004c052_m),
            ('t004c052_f', t004c052_f),
            ('t004c053_t', t004c053_t),
            ('t004c053_m', t004c053_m),
            ('t004c053_f', t004c053_f),
            ('t004c054_t', t004c054_t),
            ('t004c054_m', t004c054_m),
            ('t004c054_f', t004c054_f),
            ('t004c055_t', t004c055_t),
            ('t004c055_m', t004c055_m),
            ('t004c055_f', t004c055_f),
            ('t004c056_t', t004c056_t),
            ('t004c056_m', t004c056_m),
            ('t004c056_f', t004c056_f),
            ('t004c057_t', t004c057_t),
            ('t004c057_m', t004c057_m),
            ('t004c057_f', t004c057_f),
            ('t004c058_t', t004c058_t),
            ('t004c058_m', t004c058_m),
            ('t004c058_f', t004c058_f),
            ('t004c059_t', t004c059_t),
            ('t004c059_m', t004c059_m),
            ('t004c059_f', t004c059_f),
            ('t004c060_t', t004c060_t),
            ('t004c060_m', t004c060_m),
            ('t004c060_f', t004c060_f),
            ('t004c061_t', t004c061_t),
            ('t004c061_m', t004c061_m),
            ('t004c061_f', t004c061_f),
            ('t004c062_t', t004c062_t),
            ('t004c062_m', t004c062_m),
            ('t004c062_f', t004c062_f),
            ('t004c063_t', t004c063_t),
            ('t004c063_m', t004c063_m),
            ('t004c063_f', t004c063_f),
            ('t004c064_t', t004c064_t),
            ('t004c064_m', t004c064_m),
            ('t004c064_f', t004c064_f),
            ('t004c065_t', t004c065_t),
            ('t004c065_m', t004c065_m),
            ('t004c065_f', t004c065_f),
            ('t004c066_t', t004c066_t),
            ('t004c066_m', t004c066_m),
            ('t004c066_f', t004c066_f),
            ('t004c067_t', t004c067_t),
            ('t004c067_m', t004c067_m),
            ('t004c067_f', t004c067_f),
            ('t004c068_t', t004c068_t),
            ('t004c068_m', t004c068_m),
            ('t004c068_f', t004c068_f),
            ('t004c069_t', t004c069_t),
            ('t004c069_m', t004c069_m),
            ('t004c069_f', t004c069_f),
            ('t004c070_t', t004c070_t),
            ('t004c070_m', t004c070_m),
            ('t004c070_f', t004c070_f),
            ('t004c071_t', t004c071_t),
            ('t004c071_m', t004c071_m),
            ('t004c071_f', t004c071_f),
            ('t004c072_t', t004c072_t),
            ('t004c072_m', t004c072_m),
            ('t004c072_f', t004c072_f),
            ('t004c073_t', t004c073_t),
            ('t004c073_m', t004c073_m),
            ('t004c073_f', t004c073_f),
            ('t004c074_t', t004c074_t),
            ('t004c074_m', t004c074_m),
            ('t004c074_f', t004c074_f),
            ('t004c075_t', t004c075_t),
            ('t004c075_m', t004c075_m),
            ('t004c075_f', t004c075_f),
            ('t004c076_t', t004c076_t),
            ('t004c076_m', t004c076_m),
            ('t004c076_f', t004c076_f),
            ('t004c077_t', t004c077_t),
            ('t004c077_m', t004c077_m),
            ('t004c077_f', t004c077_f),
            ('t004c078_t', t004c078_t),
            ('t004c078_m', t004c078_m),
            ('t004c078_f', t004c078_f),
            ('t004c079_t', t004c079_t),
            ('t004c079_m', t004c079_m),
            ('t004c079_f', t004c079_f),
            ('t004c080_t', t004c080_t),
            ('t004c080_m', t004c080_m),
            ('t004c080_f', t004c080_f),
            ('t004c081_t', t004c081_t),
            ('t004c081_m', t004c081_m),
            ('t004c081_f', t004c081_f),
            ('t004c082_t', t004c082_t),
            ('t004c082_m', t004c082_m),
            ('t004c082_f', t004c082_f),
            ('t004c083_t', t004c083_t),
            ('t004c083_m', t004c083_m),
            ('t004c083_f', t004c083_f),
            ('t004c084_t', t004c084_t),
            ('t004c084_m', t004c084_m),
            ('t004c084_f', t004c084_f),
            ('t004c085_t', t004c085_t),
            ('t004c085_m', t004c085_m),
            ('t004c085_f', t004c085_f),
            ('t004c086_t', t004c086_t),
            ('t004c086_m', t004c086_m),
            ('t004c086_f', t004c086_f),
            ('t004c087_t', t004c087_t),
            ('t004c087_m', t004c087_m),
            ('t004c087_f', t004c087_f),
            ('t004c088_t', t004c088_t),
            ('t004c088_m', t004c088_m),
            ('t004c088_f', t004c088_f),
            ('t004c089_t', t004c089_t),
            ('t004c089_m', t004c089_m),
            ('t004c089_f', t004c089_f),
            ('t004c090_t', t004c090_t),
            ('t004c090_m', t004c090_m),
            ('t004c090_f', t004c090_f),
            ('t004c091_t', t004c091_t),
            ('t004c091_m', t004c091_m),
            ('t004c091_f', t004c091_f),
            ('t004c092_t', t004c092_t),
            ('t004c092_m', t004c092_m),
            ('t004c092_f', t004c092_f),
            ('t004c093_t', t004c093_t),
            ('t004c093_m', t004c093_m),
            ('t004c093_f', t004c093_f),
            ('t004c094_t', t004c094_t),
            ('t004c094_m', t004c094_m),
            ('t004c094_f', t004c094_f),
            ('t004c095_t', t004c095_t),
            ('t004c095_m', t004c095_m),
            ('t004c095_f', t004c095_f),
            ('t004c096_t', t004c096_t),
            ('t004c096_m', t004c096_m),
            ('t004c096_f', t004c096_f),
            ('t004c097_t', t004c097_t),
            ('t004c097_m', t004c097_m),
            ('t004c097_f', t004c097_f),
            ('t004c098_t', t004c098_t),
            ('t004c098_m', t004c098_m),
            ('t004c098_f', t004c098_f),
            ('t004c099_t', t004c099_t),
            ('t004c099_m', t004c099_m),
            ('t004c099_f', t004c099_f),
            ('t004c100_t', t004c100_t),
            ('t004c100_m', t004c100_m),
            ('t004c100_f', t004c100_f),
            ('t004c101_t', t004c101_t),
            ('t004c101_m', t004c101_m),
            ('t004c101_f', t004c101_f),
            ('t004c102_t', t004c102_t),
            ('t004c102_m', t004c102_m),
            ('t004c102_f', t004c102_f),
            ('t004c103_t', t004c103_t),
            ('t004c103_m', t004c103_m),
            ('t004c103_f', t004c103_f),
            ('t004c104_t', t004c104_t),
            ('t004c104_m', t004c104_m),
            ('t004c104_f', t004c104_f),
            ('t004c105_t', t004c105_t),
            ('t004c105_m', t004c105_m),
            ('t004c105_f', t004c105_f),
            ('t004c106_t', t004c106_t),
            ('t004c106_m', t004c106_m),
            ('t004c106_f', t004c106_f),
            ('t004c107_t', t004c107_t),
            ('t004c107_m', t004c107_m),
            ('t004c107_f', t004c107_f),
            ('t004c108_t', t004c108_t),
            ('t004c108_m', t004c108_m),
            ('t004c108_f', t004c108_f),
            ('t004c109_t', t004c109_t),
            ('t004c109_m', t004c109_m),
            ('t004c109_f', t004c109_f),
            ('t004c110_t', t004c110_t),
            ('t004c110_m', t004c110_m),
            ('t004c110_f', t004c110_f),
            ('t004c111_t', t004c111_t),
            ('t004c111_m', t004c111_m),
            ('t004c111_f', t004c111_f),
            ('t004c112_t', t004c112_t),
            ('t004c112_m', t004c112_m),
            ('t004c112_f', t004c112_f),
            ('t005c001_t', t005c001_t),
            ('t005c002_t', t005c002_t),
            ('t005c003_t', t005c003_t),
            ('t005c004_t', t005c004_t),
            ('t005c005_t', t005c005_t),
            ('t005c007_t', t005c007_t),
            ('t005c008_t', t005c008_t),
            ('t005c009_t', t005c009_t),
            ('t005c010_t', t005c010_t),
            ('t005c011_t', t005c011_t),
            ('t005c012_t', t005c012_t),
            ('t005c013_t', t005c013_t),
            ('t005c014_t', t005c014_t),
            ('t005c015_t', t005c015_t),
            ('t005c016_t', t005c016_t),
            ('t005c017_t', t005c017_t),
            ('t005c018_t', t005c018_t),
            ('t005c019_t', t005c019_t),
            ('t005c020_t', t005c020_t),
            ('t005c021_t', t005c021_t),
            ('t005c022_t', t005c022_t),
            ('t005c023_t', t005c023_t),
            ('t005c024_t', t005c024_t),
            ('t005c025_t', t005c025_t),
            ('t005c026_t', t005c026_t),
            ('t005c027_t', t005c027_t),
            ('t005c028_t', t005c028_t),
            ('t005c029_t', t005c029_t),
            ('t005c030_t', t005c030_t),
            ('t005c031_t', t005c031_t),
            ('t005c032_t', t005c032_t),
            ('t005c033_t', t005c033_t),
            ('t005c034_t', t005c034_t),
            ('t005c035_t', t005c035_t),
            ('t006c002_t', t006c002_t),
            ('t006c002_m', t006c002_m),
            ('t006c002_f', t006c002_f),
            ('t006c003_t', t006c003_t),
            ('t006c003_m', t006c003_m),
            ('t006c003_f', t006c003_f),
            ('t006c004_t', t006c004_t),
            ('t006c004_m', t006c004_m),
            ('t006c004_f', t006c004_f),
            ('t006c005_t', t006c005_t),
            ('t006c005_m', t006c005_m),
            ('t006c005_f', t006c005_f),
            ('t006c006_t', t006c006_t),
            ('t006c006_m', t006c006_m),
            ('t006c006_f', t006c006_f),
            ('t006c007_t', t006c007_t),
            ('t006c007_m', t006c007_m),
            ('t006c007_f', t006c007_f),
            ('t007c001_t', t007c001_t),
            ('t007c001_m', t007c001_m),
            ('t007c001_f', t007c001_f),
            ('t007c002_t', t007c002_t),
            ('t007c002_m', t007c002_m),
            ('t007c002_f', t007c002_f),
            ('t007c003_t', t007c003_t),
            ('t007c003_m', t007c003_m),
            ('t007c003_f', t007c003_f),
            ('t007c004_t', t007c004_t),
            ('t007c004_m', t007c004_m),
            ('t007c004_f', t007c004_f),
            ('t007c005_t', t007c005_t),
            ('t007c005_m', t007c005_m),
            ('t007c005_f', t007c005_f),
            ('t007c006_t', t007c006_t),
            ('t007c006_m', t007c006_m),
            ('t007c006_f', t007c006_f),
            ('t007c007_t', t007c007_t),
            ('t007c008_t', t007c008_t),
            ('t007c008_m', t007c008_m),
            ('t007c008_f', t007c008_f),
            ('t007c009_t', t007c009_t),
            ('t007c009_m', t007c009_m),
            ('t007c009_f', t007c009_f),
            ('t007c010_t', t007c010_t),
            ('t007c010_m', t007c010_m),
            ('t007c010_f', t007c010_f),
            ('t007c011_t', t007c011_t),
            ('t007c011_m', t007c011_m),
            ('t007c011_f', t007c011_f),
            ('t007c012_t', t007c012_t),
            ('t007c012_m', t007c012_m),
            ('t007c012_f', t007c012_f),
            ('t007c013_t', t007c013_t),
            ('t007c013_m', t007c013_m),
            ('t007c013_f', t007c013_f),
            ('t007c014_t', t007c014_t),
            ('t007c015_t', t007c015_t),
            ('t007c016_t', t007c016_t),
            ('t007c017_t', t007c017_t),
            ('t007c018_t', t007c018_t),
            ('t007c019_t', t007c019_t),
            ('t007c020_t', t007c020_t),
            ('t007c021_t', t007c021_t),
            ('t007c022_t', t007c022_t),
            ('t007c023_t', t007c023_t),
            ('t007c024_t', t007c024_t),
            ('t007c025_t', t007c025_t),
            ('t007c026_t', t007c026_t),
            ('t007c027_t', t007c027_t),
            ('t007c028_t', t007c028_t),
            ('t007c029_t', t007c029_t),
            ('t007c030_t', t007c030_t),
            ('t007c032_t', t007c032_t),
            ('t007c033_t', t007c033_t),
            ('t007c034_t', t007c034_t),
            ('t007c035_t', t007c035_t),
            ('t007c036_t', t007c036_t),
            ('t007c037_t', t007c037_t),
            ('t007c038_t', t007c038_t),
            ('t007c039_t', t007c039_t),
            ('t007c040_t', t007c040_t),
            ('t007c042_t', t007c042_t),
            ('t007c043_t', t007c043_t),
            ('t007c044_t', t007c044_t),
            ('t007c045_t', t007c045_t),
            ('t007c046_t', t007c046_t),
            ('t007c047_t', t007c047_t),
            ('t007c048_t', t007c048_t),
            ('t007c049_t', t007c049_t),
            ('t008c002_t', t008c002_t),
            ('t008c002_m', t008c002_m),
            ('t008c002_f', t008c002_f),
            ('t008c003_t', t008c003_t),
            ('t008c003_m', t008c003_m),
            ('t008c003_f', t008c003_f),
            ('t008c004_t', t008c004_t),
            ('t008c004_m', t008c004_m),
            ('t008c004_f', t008c004_f),
            ('t008c005_t', t008c005_t),
            ('t008c005_m', t008c005_m),
            ('t008c005_f', t008c005_f),
            ('t009c001_t', t009c001_t),
            ('t009c001_m', t009c001_m),
            ('t009c001_f', t009c001_f),
            ('t009c002_t', t009c002_t),
            ('t009c002_m', t009c002_m),
            ('t009c002_f', t009c002_f),
            ('t009c003_t', t009c003_t),
            ('t009c003_m', t009c003_m),
            ('t009c003_f', t009c003_f),
            ('t009c004_t', t009c004_t),
            ('t009c004_m', t009c004_m),
            ('t009c004_f', t009c004_f),
            ('t009c005_t', t009c005_t),
            ('t009c005_m', t009c005_m),
            ('t009c005_f', t009c005_f),
            ('t009c006_t', t009c006_t),
            ('t009c006_m', t009c006_m),
            ('t009c006_f', t009c006_f),
            ('t009c007_t', t009c007_t),
            ('t009c007_m', t009c007_m),
            ('t009c007_f', t009c007_f),
            ('t009c008_t', t009c008_t),
            ('t009c008_m', t009c008_m),
            ('t009c008_f', t009c008_f),
            ('t009c009_t', t009c009_t),
            ('t009c009_m', t009c009_m),
            ('t009c009_f', t009c009_f),
            ('t010c002_t', t010c002_t),
            ('t010c003_t', t010c003_t),
            ('t010c004_t', t010c004_t),
            ('t010c005_t', t010c005_t),
            ('t010c006_t', t010c006_t),
            ('t010c007_t', t010c007_t),
        ])

