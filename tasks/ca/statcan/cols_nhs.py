from tasks.meta import OBSColumn, DENOMINATOR, UNIVERSE
from tasks.util import ColumnsTask
from tasks.tags import SectionTags, SubsectionTags, UnitTags

from collections import OrderedDict

class NHSColumns(ColumnsTask):

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

        unit = input_['units']
        unit_people = unit['people']
        unit_money = unit['money']
        unit_housing = unit['housing_units']
        unit_household = unit['households']
        unit_years = unit['years']
        unit_minutes = unit['minutes']
        unit_ratio = unit['ratio']
        unit_education = unit['education_level']

        ca = input_['sections']['ca']

        t001c001_t = OBSColumn(
            id='t001c001_t',
            name='Total population in private households (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={},)

        t001c001_m = OBSColumn(
            id='t001c001_m',
            name='Total population in private households (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={},)

        t001c001_f = OBSColumn(
            id='t001c001_f',
            name='Total population in private households (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={},)
        # FIXME
        # There appear to be many columns in here that are direct duplicates,
        # both by content and by ID, of columns in cols_census.py.  Those
        # duplicates should be eliminated from here, and the Survey TableTask
        # should pull from both NHSColumns and CensusColumns for those columns
        # removed from here.
        #
        # The relevant duplicate columns below have been removed, butthe
        # TableTask still needs to be fixed.

        t001c002_t = OBSColumn(
            id='t001c002_t',
            name='Total population - Aboriginal identity (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c002_m = OBSColumn(
            id='t001c002_m',
            name='Total population - Aboriginal identity (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c002_f = OBSColumn(
            id='t001c002_f',
            name='Total population - Aboriginal identity (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c003_t = OBSColumn(
            id='t001c003_t',
            name='First Nations (North American Indian) single identity (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c002_t: DENOMINATOR },)

        t001c003_m = OBSColumn(
            id='t001c003_m',
            name='First Nations (North American Indian) single identity (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c002_m: DENOMINATOR },)

        t001c003_f = OBSColumn(
            id='t001c003_f',
            name='First Nations (North American Indian) single identity (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c002_f: DENOMINATOR },)

        t001c004_t = OBSColumn(
            id='t001c004_t',
            name=u'Métis single identity (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c002_t: DENOMINATOR },)

        t001c004_m = OBSColumn(
            id='t001c004_m',
            name=u'Métis single identity (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c002_m: DENOMINATOR },)

        t001c004_f = OBSColumn(
            id='t001c004_f',
            name=u'Métis single identity (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c002_f: DENOMINATOR },)

        t001c005_t = OBSColumn(
            id='t001c005_t',
            name='Inuk (Inuit) single identity (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c002_t: DENOMINATOR },)

        t001c005_m = OBSColumn(
            id='t001c005_m',
            name='Inuk (Inuit) single identity (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c002_m: DENOMINATOR },)

        t001c005_f = OBSColumn(
            id='t001c005_f',
            name='Inuk (Inuit) single identity (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c002_f: DENOMINATOR },)

        t001c006_t = OBSColumn(
            id='t001c006_t',
            name='Multiple Aboriginal identities (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c002_t: DENOMINATOR },)

        t001c006_m = OBSColumn(
            id='t001c006_m',
            name='Multiple Aboriginal identities (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c002_m: DENOMINATOR },)

        t001c006_f = OBSColumn(
            id='t001c006_f',
            name='Multiple Aboriginal identities (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c002_f: DENOMINATOR },)

        t001c007_t = OBSColumn(
            id='t001c007_t',
            name='Aboriginal identities not included elsewhere (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c002_t: DENOMINATOR },)

        t001c007_m = OBSColumn(
            id='t001c007_m',
            name='Aboriginal identities not included elsewhere (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c002_m: DENOMINATOR },)

        t001c007_f = OBSColumn(
            id='t001c007_f',
            name='Aboriginal identities not included elsewhere (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c002_f: DENOMINATOR },)

        t001c008_t = OBSColumn(
            id='t001c008_t',
            name='Total population - Non-Aboriginal identity (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c008_m = OBSColumn(
            id='t001c008_m',
            name='Total population - Non-Aboriginal identity (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c008_f = OBSColumn(
            id='t001c008_f',
            name='Total population - Non-Aboriginal identity (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c010_t = OBSColumn(
            id='t001c010_t',
            name='Total population - Registered or Treaty Indian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c010_m = OBSColumn(
            id='t001c010_m',
            name='Total population - Registered or Treaty Indian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c010_f = OBSColumn(
            id='t001c010_f',
            name='Total population - Registered or Treaty Indian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c011_t = OBSColumn(
            id='t001c011_t',
            name='Total population - Not a Registered or Treaty Indian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c011_m = OBSColumn(
            id='t001c011_m',
            name='Total population - Not a Registered or Treaty Indian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c011_f = OBSColumn(
            id='t001c011_f',
            name='Total population - Not a Registered or Treaty Indian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c013_t = OBSColumn(
            id='t001c013_t',
            name='Total population - Aboriginal ancestry (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c013_m = OBSColumn(
            id='t001c013_m',
            name='Total population - Aboriginal ancestry (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c013_f = OBSColumn(
            id='t001c013_f',
            name='Total population - Aboriginal ancestry (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_f: DENOMINATOR },)

        t001c014_t = OBSColumn(
            id='t001c014_t',
            name='First Nations (North American Indian) Aboriginal ancestry (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c013_t: DENOMINATOR },)

        t001c014_m = OBSColumn(
            id='t001c014_m',
            name='First Nations (North American Indian) Aboriginal ancestry (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c013_m: DENOMINATOR },)

        t001c014_f = OBSColumn(
            id='t001c014_f',
            name='First Nations (North American Indian) Aboriginal ancestry (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c013_f: DENOMINATOR },)

        t001c015_t = OBSColumn(
            id='t001c015_t',
            name=u'Métis ancestry (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c013_t: DENOMINATOR },)

        t001c015_m = OBSColumn(
            id='t001c015_m',
            name=u'Métis ancestry (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c013_m: DENOMINATOR },)

        t001c015_f = OBSColumn(
            id='t001c015_f',
            name=u'Métis ancestry (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c013_f: DENOMINATOR },)

        t001c016_t = OBSColumn(
            id='t001c016_t',
            name='Inuit ancestry (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c013_t: DENOMINATOR },)

        t001c016_m = OBSColumn(
            id='t001c016_m',
            name='Inuit ancestry (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c013_m: DENOMINATOR },)

        t001c016_f = OBSColumn(
            id='t001c016_f',
            name='Inuit ancestry (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c013_f: DENOMINATOR },)

        t001c017_t = OBSColumn(
            id='t001c017_t',
            name='Total population - Non-Aboriginal ancestry only (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_t: DENOMINATOR },)

        t001c017_m = OBSColumn(
            id='t001c017_m',
            name='Total population - Non-Aboriginal ancestry only (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_m: DENOMINATOR },)

        t001c017_f = OBSColumn(
            id='t001c017_f',
            name='Total population - Non-Aboriginal ancestry only (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_f: DENOMINATOR },)

        t002c001_t = OBSColumn(
            id='t002c001_t',
            name='Total immigrant population in private households (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={},)

        t002c001_m = OBSColumn(
            id='t002c001_m',
            name='Total immigrant population in private households (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={},)

        t002c001_f = OBSColumn(
            id='t002c001_f',
            name='Total immigrant population in private households (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={},)

        t002c002_t = OBSColumn(
            id='t002c002_t',
            name='Total immigrant population in private households - Under 5 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t002c001_t: DENOMINATOR },)

        t002c002_m = OBSColumn(
            id='t002c002_m',
            name='Total immigrant population in private households - Under 5 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t002c001_m: DENOMINATOR },)

        t002c002_f = OBSColumn(
            id='t002c002_f',
            name='Total immigrant population in private households - Under 5 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t002c001_f: DENOMINATOR },)

        t002c003_t = OBSColumn(
            id='t002c003_t',
            name='Total immigrant population in private households - 5 to 14 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t002c001_t: DENOMINATOR },)

        t002c003_m = OBSColumn(
            id='t002c003_m',
            name='Total immigrant population in private households - 5 to 14 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t002c001_m: DENOMINATOR },)

        t002c003_f = OBSColumn(
            id='t002c003_f',
            name='Total immigrant population in private households - 5 to 14 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t002c001_f: DENOMINATOR },)

        t002c004_t = OBSColumn(
            id='t002c004_t',
            name='Total immigrant population in private households - 15 to 24 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t002c001_t: DENOMINATOR },)

        t002c004_m = OBSColumn(
            id='t002c004_m',
            name='Total immigrant population in private households - 15 to 24 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t002c001_m: DENOMINATOR },)

        t002c004_f = OBSColumn(
            id='t002c004_f',
            name='Total immigrant population in private households - 15 to 24 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t002c001_f: DENOMINATOR },)

        t002c005_t = OBSColumn(
            id='t002c005_t',
            name='Total immigrant population in private households - 25 to 44 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t002c001_t: DENOMINATOR },)

        t002c005_m = OBSColumn(
            id='t002c005_m',
            name='Total immigrant population in private households - 25 to 44 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t002c001_m: DENOMINATOR },)

        t002c005_f = OBSColumn(
            id='t002c005_f',
            name='Total immigrant population in private households - 25 to 44 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t002c001_f: DENOMINATOR },)

        t002c006_t = OBSColumn(
            id='t002c006_t',
            name='Total immigrant population in private households - 45 years and over (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t002c001_t: DENOMINATOR },)

        t002c006_m = OBSColumn(
            id='t002c006_m',
            name='Total immigrant population in private households - 45 years and over (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t002c001_m: DENOMINATOR },)

        t002c006_f = OBSColumn(
            id='t002c006_f',
            name='Total immigrant population in private households - 45 years and over (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t002c001_f: DENOMINATOR },)

        t003c002_t = OBSColumn(
            id='t003c002_t',
            name='Total population - Canadian citizens (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t001c001_t: DENOMINATOR },)

        t003c002_m = OBSColumn(
            id='t003c002_m',
            name='Total population - Canadian citizens (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t001c001_m: DENOMINATOR },)

        t003c002_f = OBSColumn(
            id='t003c002_f',
            name='Total population - Canadian citizens (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t001c001_f: DENOMINATOR },)

        t003c003_t = OBSColumn(
            id='t003c003_t',
            name='Canadian citizens aged under 18 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t003c002_t: DENOMINATOR },)

        t003c003_m = OBSColumn(
            id='t003c003_m',
            name='Canadian citizens aged under 18 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t003c002_m: DENOMINATOR },)

        t003c003_f = OBSColumn(
            id='t003c003_f',
            name='Canadian citizens aged under 18 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t003c002_f: DENOMINATOR },)

        t003c004_t = OBSColumn(
            id='t003c004_t',
            name='Canadian citizens aged 18 and over (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t003c002_t: DENOMINATOR },)

        t003c004_m = OBSColumn(
            id='t003c004_m',
            name='Canadian citizens aged 18 and over (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t003c002_m: DENOMINATOR },)

        t003c004_f = OBSColumn(
            id='t003c004_f',
            name='Canadian citizens aged 18 and over (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t003c002_f: DENOMINATOR },)

        t003c005_t = OBSColumn(
            id='t003c005_t',
            name='Total population - Not Canadian citizens (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t001c001_t: DENOMINATOR },)

        t003c005_m = OBSColumn(
            id='t003c005_m',
            name='Total population - Not Canadian citizens (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t001c001_m: DENOMINATOR },)

        t003c005_f = OBSColumn(
            id='t003c005_f',
            name='Total population - Not Canadian citizens (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t001c001_f: DENOMINATOR },)

        t004c001_t = OBSColumn(
            id='t004c001_t',
            name='People aged 15+ in the labour force (total)',
            description='Refers to whether an employed person is an employee or is self-employed. The self-employed include persons with or without a business, as well as unpaid family workers.',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={},)

        t004c001_m = OBSColumn(
            id='t004c001_m',
            name='People aged 15+ in the labour force (male)',
            description='Refers to whether an employed person is an employee or is self-employed. The self-employed include persons with or without a business, as well as unpaid family workers.',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={},)

        t004c001_f = OBSColumn(
            id='t004c001_f',
            name='People aged 15+ in the labour force (female)',
            description='Refers to whether an employed person is an employee or is self-employed. The self-employed include persons with or without a business, as well as unpaid family workers.',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={},)

        t004c002_t = OBSColumn(
            id='t004c002_t',
            name='People aged 15+ in the labour force - Worker Class n/a (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_t: DENOMINATOR },)

        t004c002_m = OBSColumn(
            id='t004c002_m',
            name='People aged 15+ in the labour force - Worker Class n/a (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_m: DENOMINATOR },)

        t004c002_f = OBSColumn(
            id='t004c002_f',
            name='People aged 15+ in the labour force - Worker Class n/a (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_f: DENOMINATOR },)

        t004c003_t = OBSColumn(
            id='t004c003_t',
            name='People aged 15+ in the labour force - Worker Class (total)',
            description='Experienced labour force refers to persons who were employed and the unemployed who had last worked for pay or in self-employment',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_t: DENOMINATOR },)

        t004c003_m = OBSColumn(
            id='t004c003_m',
            name='People aged 15+ in the labour force - Worker Class (male)',
            description='Experienced labour force refers to persons who were employed and the unemployed who had last worked for pay or in self-employment',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_m: DENOMINATOR },)

        t004c003_f = OBSColumn(
            id='t004c003_f',
            name='People aged 15+ in the labour force - Worker Class (female)',
            description='Experienced labour force refers to persons who were employed and the unemployed who had last worked for pay or in self-employment',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_f: DENOMINATOR },)

        t004c004_t = OBSColumn(
            id='t004c004_t',
            name='People aged 15+ in the labour force - Worker Class - Employee (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c003_t: DENOMINATOR },)

        t004c004_m = OBSColumn(
            id='t004c004_m',
            name='People aged 15+ in the labour force - Worker Class - Employee (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c003_m: DENOMINATOR },)

        t004c004_f = OBSColumn(
            id='t004c004_f',
            name='People aged 15+ in the labour force - Worker Class - Employee (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c003_f: DENOMINATOR },)

        t004c005_t = OBSColumn(
            id='t004c005_t',
            name='People aged 15+ in the labour force - Worker Class - Self-employed (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c003_t: DENOMINATOR },)

        t004c005_m = OBSColumn(
            id='t004c005_m',
            name='People aged 15+ in the labour force - Worker Class - Self-employed (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c003_m: DENOMINATOR },)

        t004c005_f = OBSColumn(
            id='t004c005_f',
            name='People aged 15+ in the labour force - Worker Class - Self-employed (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c003_f: DENOMINATOR },)

        t005c001_t = OBSColumn(
            id='t005c001_t',
            name='People aged 15+ (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={},)

        t005c001_m = OBSColumn(
            id='t005c001_m',
            name='People aged 15+ (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={},)

        t005c001_f = OBSColumn(
            id='t005c001_f',
            name='People aged 15+ (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={},)

        # FIXME - DUP
        # deleted t005c001, should use `ca.statcan.cols_census.t009c001` instead.
        t005c002_t = OBSColumn(
            id='t005c002_t',
            name='People aged 15+ with no certificate, diploma or degree (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c002_m = OBSColumn(
            id='t005c002_m',
            name='People aged 15+ with no certificate, diploma or degree (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_m: DENOMINATOR },)

        t005c002_f = OBSColumn(
            id='t005c002_f',
            name='People aged 15+ with no certificate, diploma or degree (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_f: DENOMINATOR },)

        t005c003_t = OBSColumn(
            id='t005c003_t',
            name='People aged 15+ with high school diploma or equivalent (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c003_m = OBSColumn(
            id='t005c003_m',
            name='People aged 15+ with high school diploma or equivalent (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_m: DENOMINATOR },)

        t005c003_f = OBSColumn(
            id='t005c003_f',
            name='People aged 15+ with high school diploma or equivalent (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_f: DENOMINATOR },)

        t005c004_t = OBSColumn(
            id='t005c004_t',
            name='People aged 15+ with postsecondary certificate, diploma or degree (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c004_m = OBSColumn(
            id='t005c004_m',
            name='People aged 15+ with postsecondary certificate, diploma or degree (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_m: DENOMINATOR },)

        t005c004_f = OBSColumn(
            id='t005c004_f',
            name='People aged 15+ with postsecondary certificate, diploma or degree (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_f: DENOMINATOR },)

        t005c005_t = OBSColumn(
            id='t005c005_t',
            name='People aged 15+ with apprenticeship or trades certificate or diploma (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c004_t: DENOMINATOR },)

        t005c005_m = OBSColumn(
            id='t005c005_m',
            name='People aged 15+ with apprenticeship or trades certificate or diploma (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c004_m: DENOMINATOR },)

        t005c005_f = OBSColumn(
            id='t005c005_f',
            name='People aged 15+ with apprenticeship or trades certificate or diploma (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c004_f: DENOMINATOR },)

        t005c006_t = OBSColumn(
            id='t005c006_t',
            name='People aged 15+ with college, CEGEP or other non-university certificate or diploma (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c004_t: DENOMINATOR },)

        t005c006_m = OBSColumn(
            id='t005c006_m',
            name='People aged 15+ with college, CEGEP or other non-university certificate or diploma (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c004_m: DENOMINATOR },)

        t005c006_f = OBSColumn(
            id='t005c006_f',
            name='People aged 15+ with college, CEGEP or other non-university certificate or diploma (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c004_f: DENOMINATOR },)

        t005c007_t = OBSColumn(
            id='t005c007_t',
            name='People aged 15+ with university certificate or diploma below bachelor level (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c004_t: DENOMINATOR },)

        t005c007_m = OBSColumn(
            id='t005c007_m',
            name='People aged 15+ with university certificate or diploma below bachelor level (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c004_m: DENOMINATOR },)

        t005c007_f = OBSColumn(
            id='t005c007_f',
            name='People aged 15+ with university certificate or diploma below bachelor level (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c004_f: DENOMINATOR },)

        t005c008_t = OBSColumn(
            id='t005c008_t',
            name='People aged 15+ with university certificate, diploma or degree at bachelor level or above (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c004_t: DENOMINATOR },)

        t005c008_m = OBSColumn(
            id='t005c008_m',
            name='People aged 15+ with university certificate, diploma or degree at bachelor level or above (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c004_m: DENOMINATOR },)

        t005c008_f = OBSColumn(
            id='t005c008_f',
            name='People aged 15+ with university certificate, diploma or degree at bachelor level or above (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c004_f: DENOMINATOR },)

        t005c009_t = OBSColumn(
            id='t005c009_t',
            name='People aged 15+ with bachelor\'s degree (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c008_t: DENOMINATOR },)

        t005c009_m = OBSColumn(
            id='t005c009_m',
            name='People aged 15+ with bachelor\'s degree (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c008_m: DENOMINATOR },)

        t005c009_f = OBSColumn(
            id='t005c009_f',
            name='People aged 15+ with bachelor\'s degree (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c008_f: DENOMINATOR },)

        t005c010_t = OBSColumn(
            id='t005c010_t',
            name='People aged 15+ with university certificate, diploma or degree above bachelor level (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c008_t: DENOMINATOR },)

        t005c010_m = OBSColumn(
            id='t005c010_m',
            name='People aged 15+ with university certificate, diploma or degree above bachelor level (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c008_m: DENOMINATOR },)

        t005c010_f = OBSColumn(
            id='t005c010_f',
            name='People aged 15+ with university certificate, diploma or degree above bachelor level (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c008_f: DENOMINATOR },)

        t005c011_t = OBSColumn(
            id='t005c011_t',
            name='People aged 25-64',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={},)

        t005c011_m = OBSColumn(
            id='t005c011_m',
            name='People aged 25-64 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={},)

        t005c011_f = OBSColumn(
            id='t005c011_f',
            name='People aged 25-64 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={},)

        t005c012_t = OBSColumn(
            id='t005c012_t',
            name='People aged 25-64 with no certificate, diploma or degree (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c011_t: DENOMINATOR },)

        t005c012_m = OBSColumn(
            id='t005c012_m',
            name='People aged 25-64 with no certificate, diploma or degree (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c011_m: DENOMINATOR },)

        t005c012_f = OBSColumn(
            id='t005c012_f',
            name='People aged 25-64 with no certificate, diploma or degree (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c011_f: DENOMINATOR },)

        t005c013_t = OBSColumn(
            id='t005c013_t',
            name='People aged 25-64 with a high school diploma or equivalent (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c011_t: DENOMINATOR },)

        t005c013_m = OBSColumn(
            id='t005c013_m',
            name='People aged 25-64 with a high school diploma or equivalent (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c011_m: DENOMINATOR },)

        t005c013_f = OBSColumn(
            id='t005c013_f',
            name='People aged 25-64 with a high school diploma or equivalent (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c011_f: DENOMINATOR },)

        t005c014_t = OBSColumn(
            id='t005c014_t',
            name='People aged 25-64 with a postsecondary certificate, diploma or degree (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c011_t: DENOMINATOR },)

        t005c014_m = OBSColumn(
            id='t005c014_m',
            name='People aged 25-64 with a postsecondary certificate, diploma or degree (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c011_m: DENOMINATOR },)

        t005c014_f = OBSColumn(
            id='t005c014_f',
            name='People aged 25-64 with a postsecondary certificate, diploma or degree (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c011_f: DENOMINATOR },)

        t005c015_t = OBSColumn(
            id='t005c015_t',
            name='People aged 25-64 with an apprenticeship or trades certificate or diploma (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c014_t: DENOMINATOR },)

        t005c015_m = OBSColumn(
            id='t005c015_m',
            name='People aged 25-64 with an apprenticeship or trades certificate or diploma (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c014_m: DENOMINATOR },)

        t005c015_f = OBSColumn(
            id='t005c015_f',
            name='People aged 25-64 with an apprenticeship or trades certificate or diploma (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c014_f: DENOMINATOR },)

        t005c016_t = OBSColumn(
            id='t005c016_t',
            name='People aged 25-64 with a college, CEGEP or other non-university certificate or diploma (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c014_t: DENOMINATOR },)

        t005c016_m = OBSColumn(
            id='t005c016_m',
            name='People aged 25-64 with a college, CEGEP or other non-university certificate or diploma (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c014_m: DENOMINATOR },)

        t005c016_f = OBSColumn(
            id='t005c016_f',
            name='People aged 25-64 with a college, CEGEP or other non-university certificate or diploma (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c014_f: DENOMINATOR },)

        t005c017_t = OBSColumn(
            id='t005c017_t',
            name='People aged 25-64 with a university certificate or diploma below bachelor level (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c014_t: DENOMINATOR },)

        t005c017_m = OBSColumn(
            id='t005c017_m',
            name='People aged 25-64 with a university certificate or diploma below bachelor level (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c014_m: DENOMINATOR },)

        t005c017_f = OBSColumn(
            id='t005c017_f',
            name='People aged 25-64 with a university certificate or diploma below bachelor level (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c014_f: DENOMINATOR },)

        t005c018_t = OBSColumn(
            id='t005c018_t',
            name='People aged 25-64 with a university certificate, diploma or degree at bachelor level or above (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c014_t: DENOMINATOR },)

        t005c018_m = OBSColumn(
            id='t005c018_m',
            name='People aged 25-64 with a university certificate, diploma or degree at bachelor level or above (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c014_m: DENOMINATOR },)

        t005c018_f = OBSColumn(
            id='t005c018_f',
            name='People aged 25-64 with a university certificate, diploma or degree at bachelor level or above (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c014_f: DENOMINATOR },)

        t005c019_t = OBSColumn(
            id='t005c019_t',
            name='People aged 25-64 with a bachelor\'s degree (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c018_t: DENOMINATOR },)

        t005c019_m = OBSColumn(
            id='t005c019_m',
            name='People aged 25-64 with a bachelor\'s degree (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c018_m: DENOMINATOR },)

        t005c019_f = OBSColumn(
            id='t005c019_f',
            name='People aged 25-64 with a bachelor\'s degree (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c018_f: DENOMINATOR },)

        t005c020_t = OBSColumn(
            id='t005c020_t',
            name='People aged 25-64 with a university certificate, diploma or degree above bachelor level (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c018_t: DENOMINATOR },)

        t005c020_m = OBSColumn(
            id='t005c020_m',
            name='People aged 25-64 with a university certificate, diploma or degree above bachelor level (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c018_m: DENOMINATOR },)

        t005c020_f = OBSColumn(
            id='t005c020_f',
            name='People aged 25-64 with a university certificate, diploma or degree above bachelor level (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c018_f: DENOMINATOR },)

        t005c022_t = OBSColumn(
            id='t005c022_t',
            name='People aged 15+ majoring in no postsecondary certificate, diploma or degree (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c022_m = OBSColumn(
            id='t005c022_m',
            name='People aged 15+ majoring in no postsecondary certificate, diploma or degree (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_m: DENOMINATOR },)

        t005c022_f = OBSColumn(
            id='t005c022_f',
            name='People aged 15+ majoring in no postsecondary certificate, diploma or degree (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_f: DENOMINATOR },)

        t005c023_t = OBSColumn(
            id='t005c023_t',
            name='People aged 15+ majoring in Education (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c023_m = OBSColumn(
            id='t005c023_m',
            name='People aged 15+ majoring in Education (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_m: DENOMINATOR },)

        t005c023_f = OBSColumn(
            id='t005c023_f',
            name='People aged 15+ majoring in Education (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_f: DENOMINATOR },)

        t005c024_t = OBSColumn(
            id='t005c024_t',
            name='People aged 15+ majoring in Visual and performing arts, and communications technologies (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c024_m = OBSColumn(
            id='t005c024_m',
            name='People aged 15+ majoring in Visual and performing arts, and communications technologies (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_m: DENOMINATOR },)

        t005c024_f = OBSColumn(
            id='t005c024_f',
            name='People aged 15+ majoring in Visual and performing arts, and communications technologies (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_f: DENOMINATOR },)

        t005c025_t = OBSColumn(
            id='t005c025_t',
            name='People aged 15+ majoring in Humanities (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c025_m = OBSColumn(
            id='t005c025_m',
            name='People aged 15+ majoring in Humanities (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_m: DENOMINATOR },)

        t005c025_f = OBSColumn(
            id='t005c025_f',
            name='People aged 15+ majoring in Humanities (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_f: DENOMINATOR },)

        t005c026_t = OBSColumn(
            id='t005c026_t',
            name='People aged 15+ majoring in Social and behavioural sciences and law (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c026_m = OBSColumn(
            id='t005c026_m',
            name='People aged 15+ majoring in Social and behavioural sciences and law (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_m: DENOMINATOR },)

        t005c026_f = OBSColumn(
            id='t005c026_f',
            name='People aged 15+ majoring in Social and behavioural sciences and law (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_f: DENOMINATOR },)

        t005c027_t = OBSColumn(
            id='t005c027_t',
            name='People aged 15+ majoring in Business, management and public administration (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c027_m = OBSColumn(
            id='t005c027_m',
            name='People aged 15+ majoring in Business, management and public administration (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_m: DENOMINATOR },)

        t005c027_f = OBSColumn(
            id='t005c027_f',
            name='People aged 15+ majoring in Business, management and public administration (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_f: DENOMINATOR },)

        t005c028_t = OBSColumn(
            id='t005c028_t',
            name='People aged 15+ majoring in Physical and life sciences and technologies (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c028_m = OBSColumn(
            id='t005c028_m',
            name='People aged 15+ majoring in Physical and life sciences and technologies (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_m: DENOMINATOR },)

        t005c028_f = OBSColumn(
            id='t005c028_f',
            name='People aged 15+ majoring in Physical and life sciences and technologies (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_f: DENOMINATOR },)

        t005c029_t = OBSColumn(
            id='t005c029_t',
            name='People aged 15+ majoring in Mathematics, computer and information sciences (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c029_m = OBSColumn(
            id='t005c029_m',
            name='People aged 15+ majoring in Mathematics, computer and information sciences (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_m: DENOMINATOR },)

        t005c029_f = OBSColumn(
            id='t005c029_f',
            name='People aged 15+ majoring in Mathematics, computer and information sciences (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_f: DENOMINATOR },)

        t005c030_t = OBSColumn(
            id='t005c030_t',
            name='People aged 15+ majoring in Architecture, engineering, and related technologies (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c030_m = OBSColumn(
            id='t005c030_m',
            name='People aged 15+ majoring in Architecture, engineering, and related technologies (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_m: DENOMINATOR },)

        t005c030_f = OBSColumn(
            id='t005c030_f',
            name='People aged 15+ majoring in Architecture, engineering, and related technologies (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_f: DENOMINATOR },)

        t005c031_t = OBSColumn(
            id='t005c031_t',
            name='People aged 15+ majoring in Agriculture, natural resources and conservation (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c031_m = OBSColumn(
            id='t005c031_m',
            name='People aged 15+ majoring in Agriculture, natural resources and conservation (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_m: DENOMINATOR },)

        t005c031_f = OBSColumn(
            id='t005c031_f',
            name='People aged 15+ majoring in Agriculture, natural resources and conservation (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_f: DENOMINATOR },)

        t005c032_t = OBSColumn(
            id='t005c032_t',
            name='People aged 15+ majoring in Health and related fields (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c032_m = OBSColumn(
            id='t005c032_m',
            name='People aged 15+ majoring in Health and related fields (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_m: DENOMINATOR },)

        t005c032_f = OBSColumn(
            id='t005c032_f',
            name='People aged 15+ majoring in Health and related fields (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_f: DENOMINATOR },)

        t005c033_t = OBSColumn(
            id='t005c033_t',
            name='People aged 15+ majoring in Personal, protective and transportation services (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c033_m = OBSColumn(
            id='t005c033_m',
            name='People aged 15+ majoring in Personal, protective and transportation services (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_m: DENOMINATOR },)

        t005c033_f = OBSColumn(
            id='t005c033_f',
            name='People aged 15+ majoring in Personal, protective and transportation services (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_f: DENOMINATOR },)

        t005c034_t = OBSColumn(
            id='t005c034_t',
            name='People aged 15+ majoring in Other fields of study (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_t: DENOMINATOR },)

        t005c034_m = OBSColumn(
            id='t005c034_m',
            name='People aged 15+ majoring in Other fields of study (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_m: DENOMINATOR },)

        t005c034_f = OBSColumn(
            id='t005c034_f',
            name='People aged 15+ majoring in Other fields of study (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c001_f: DENOMINATOR },)

        t005c038_t = OBSColumn(
            id='t005c038_t',
            name='People aged 15+ who studied in Canada (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c004_t: DENOMINATOR },)

        t005c038_m = OBSColumn(
            id='t005c038_m',
            name='People aged 15+ who studied in Canada (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c004_m: DENOMINATOR },)

        t005c038_f = OBSColumn(
            id='t005c038_f',
            name='People aged 15+ who studied in Canada (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c004_f: DENOMINATOR },)

        t005c039_t = OBSColumn(
            id='t005c039_t',
            name='People aged 15+ who studied in the same province/territory of residence (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c038_t: DENOMINATOR },)

        t005c039_m = OBSColumn(
            id='t005c039_m',
            name='People aged 15+ who studied in the same province/territory of residence (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c038_m: DENOMINATOR },)

        t005c039_f = OBSColumn(
            id='t005c039_f',
            name='People aged 15+ who studied in the same province/territory of residence (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c038_f: DENOMINATOR },)

        t005c040_t = OBSColumn(
            id='t005c040_t',
            name='People aged 15+ who studied in another province/territory (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c038_t: DENOMINATOR },)

        t005c040_m = OBSColumn(
            id='t005c040_m',
            name='People aged 15+ who studied in another province/territory (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c038_m: DENOMINATOR },)

        t005c040_f = OBSColumn(
            id='t005c040_f',
            name='People aged 15+ who studied in another province/territory (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c038_f: DENOMINATOR },)

        t005c041_t = OBSColumn(
            id='t005c041_t',
            name='People aged 15+ who studied outside Canada (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c004_t: DENOMINATOR },)

        t005c041_m = OBSColumn(
            id='t005c041_m',
            name='People aged 15+ who studied outside Canada (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c004_m: DENOMINATOR },)

        t005c041_f = OBSColumn(
            id='t005c041_f',
            name='People aged 15+ who studied outside Canada (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['education']],
            targets={ t005c004_f: DENOMINATOR },)

        t006c002_t = OBSColumn(
            id='t006c002_t',
            name='Total population - North American Aboriginal origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_t: DENOMINATOR },)

        t006c002_m = OBSColumn(
            id='t006c002_m',
            name='North American Aboriginal origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_m: DENOMINATOR },)

        t006c002_f = OBSColumn(
            id='t006c002_f',
            name='North American Aboriginal origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_f: DENOMINATOR },)

        t006c003_t = OBSColumn(
            id='t006c003_t',
            name='First Nations (North American Indian) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c002_t: DENOMINATOR },)

        t006c003_m = OBSColumn(
            id='t006c003_m',
            name='First Nations (North American Indian) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c002_m: DENOMINATOR },)

        t006c003_f = OBSColumn(
            id='t006c003_f',
            name='First Nations (North American Indian) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c002_f: DENOMINATOR },)

        t006c004_t = OBSColumn(
            id='t006c004_t',
            name='Inuit (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c002_t: DENOMINATOR },)

        t006c004_m = OBSColumn(
            id='t006c004_m',
            name='Inuit (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c002_m: DENOMINATOR },)

        t006c004_f = OBSColumn(
            id='t006c004_f',
            name='Inuit (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c002_f: DENOMINATOR },)

        t006c005_t = OBSColumn(
            id='t006c005_t',
            name='M tis (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c002_t: DENOMINATOR },)

        t006c005_m = OBSColumn(
            id='t006c005_m',
            name='M tis (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c002_m: DENOMINATOR },)

        t006c005_f = OBSColumn(
            id='t006c005_f',
            name='M tis (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c002_f: DENOMINATOR },)

        t006c006_t = OBSColumn(
            id='t006c006_t',
            name='Total population - Other North American origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_t: DENOMINATOR },)

        t006c006_m = OBSColumn(
            id='t006c006_m',
            name='Other North American origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_m: DENOMINATOR },)

        t006c006_f = OBSColumn(
            id='t006c006_f',
            name='Other North American origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_f: DENOMINATOR },)

        t006c007_t = OBSColumn(
            id='t006c007_t',
            name='Acadian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_t: DENOMINATOR },)

        t006c007_m = OBSColumn(
            id='t006c007_m',
            name='Acadian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_m: DENOMINATOR },)

        t006c007_f = OBSColumn(
            id='t006c007_f',
            name='Acadian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_f: DENOMINATOR },)

        t006c008_t = OBSColumn(
            id='t006c008_t',
            name='American (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_t: DENOMINATOR },)

        t006c008_m = OBSColumn(
            id='t006c008_m',
            name='American (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_m: DENOMINATOR },)

        t006c008_f = OBSColumn(
            id='t006c008_f',
            name='American (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_f: DENOMINATOR },)

        t006c009_t = OBSColumn(
            id='t006c009_t',
            name='Canadian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_t: DENOMINATOR },)

        t006c009_m = OBSColumn(
            id='t006c009_m',
            name='Canadian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_m: DENOMINATOR },)

        t006c009_f = OBSColumn(
            id='t006c009_f',
            name='Canadian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_f: DENOMINATOR },)

        t006c010_t = OBSColumn(
            id='t006c010_t',
            name='New Brunswicker (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_t: DENOMINATOR },)

        t006c010_m = OBSColumn(
            id='t006c010_m',
            name='New Brunswicker (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_m: DENOMINATOR },)

        t006c010_f = OBSColumn(
            id='t006c010_f',
            name='New Brunswicker (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_f: DENOMINATOR },)

        t006c011_t = OBSColumn(
            id='t006c011_t',
            name='Newfoundlander (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_t: DENOMINATOR },)

        t006c011_m = OBSColumn(
            id='t006c011_m',
            name='Newfoundlander (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_m: DENOMINATOR },)

        t006c011_f = OBSColumn(
            id='t006c011_f',
            name='Newfoundlander (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_f: DENOMINATOR },)

        t006c012_t = OBSColumn(
            id='t006c012_t',
            name='Nova Scotian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_t: DENOMINATOR },)

        t006c012_m = OBSColumn(
            id='t006c012_m',
            name='Nova Scotian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_m: DENOMINATOR },)

        t006c012_f = OBSColumn(
            id='t006c012_f',
            name='Nova Scotian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_f: DENOMINATOR },)

        t006c013_t = OBSColumn(
            id='t006c013_t',
            name='Ontarian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_t: DENOMINATOR },)

        t006c013_m = OBSColumn(
            id='t006c013_m',
            name='Ontarian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_m: DENOMINATOR },)

        t006c013_f = OBSColumn(
            id='t006c013_f',
            name='Ontarian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_f: DENOMINATOR },)

        t006c014_t = OBSColumn(
            id='t006c014_t',
            name=u'Québécois (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_t: DENOMINATOR },)

        t006c014_m = OBSColumn(
            id='t006c014_m',
            name=u'Québécois (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_m: DENOMINATOR },)

        t006c014_f = OBSColumn(
            id='t006c014_f',
            name=u'Québécois (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_f: DENOMINATOR },)

        t006c015_t = OBSColumn(
            id='t006c015_t',
            name='Other North American origins, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_t: DENOMINATOR },)

        t006c015_m = OBSColumn(
            id='t006c015_m',
            name='Other North American origins, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_m: DENOMINATOR },)

        t006c015_f = OBSColumn(
            id='t006c015_f',
            name='Other North American origins, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c006_f: DENOMINATOR },)

        t006c016_t = OBSColumn(
            id='t006c016_t',
            name='Total population - European origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_t: DENOMINATOR },)

        t006c016_m = OBSColumn(
            id='t006c016_m',
            name='European origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_m: DENOMINATOR },)

        t006c016_f = OBSColumn(
            id='t006c016_f',
            name='European origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_f: DENOMINATOR },)

        t006c017_t = OBSColumn(
            id='t006c017_t',
            name='British Isles origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_t: DENOMINATOR },)

        t006c017_m = OBSColumn(
            id='t006c017_m',
            name='British Isles origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_m: DENOMINATOR },)

        t006c017_f = OBSColumn(
            id='t006c017_f',
            name='British Isles origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_f: DENOMINATOR },)

        t006c018_t = OBSColumn(
            id='t006c018_t',
            name='Channel Islander (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_t: DENOMINATOR },)

        t006c018_m = OBSColumn(
            id='t006c018_m',
            name='Channel Islander (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_m: DENOMINATOR },)

        t006c018_f = OBSColumn(
            id='t006c018_f',
            name='Channel Islander (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_f: DENOMINATOR },)

        t006c019_t = OBSColumn(
            id='t006c019_t',
            name='Cornish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_t: DENOMINATOR },)

        t006c019_m = OBSColumn(
            id='t006c019_m',
            name='Cornish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_m: DENOMINATOR },)

        t006c019_f = OBSColumn(
            id='t006c019_f',
            name='Cornish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_f: DENOMINATOR },)

        t006c020_t = OBSColumn(
            id='t006c020_t',
            name='English (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_t: DENOMINATOR },)

        t006c020_m = OBSColumn(
            id='t006c020_m',
            name='English (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_m: DENOMINATOR },)

        t006c020_f = OBSColumn(
            id='t006c020_f',
            name='English (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_f: DENOMINATOR },)

        t006c021_t = OBSColumn(
            id='t006c021_t',
            name='Irish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_t: DENOMINATOR },)

        t006c021_m = OBSColumn(
            id='t006c021_m',
            name='Irish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_m: DENOMINATOR },)

        t006c021_f = OBSColumn(
            id='t006c021_f',
            name='Irish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_f: DENOMINATOR },)

        t006c022_t = OBSColumn(
            id='t006c022_t',
            name='Manx (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_t: DENOMINATOR },)

        t006c022_m = OBSColumn(
            id='t006c022_m',
            name='Manx (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_m: DENOMINATOR },)

        t006c022_f = OBSColumn(
            id='t006c022_f',
            name='Manx (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_f: DENOMINATOR },)

        t006c023_t = OBSColumn(
            id='t006c023_t',
            name='Scottish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_t: DENOMINATOR },)

        t006c023_m = OBSColumn(
            id='t006c023_m',
            name='Scottish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_m: DENOMINATOR },)

        t006c023_f = OBSColumn(
            id='t006c023_f',
            name='Scottish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_f: DENOMINATOR },)

        t006c024_t = OBSColumn(
            id='t006c024_t',
            name='Welsh (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_t: DENOMINATOR },)

        t006c024_m = OBSColumn(
            id='t006c024_m',
            name='Welsh (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_m: DENOMINATOR },)

        t006c024_f = OBSColumn(
            id='t006c024_f',
            name='Welsh (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_f: DENOMINATOR },)

        t006c025_t = OBSColumn(
            id='t006c025_t',
            name='British Isles origins, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_t: DENOMINATOR },)

        t006c025_m = OBSColumn(
            id='t006c025_m',
            name='British Isles origins, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_m: DENOMINATOR },)

        t006c025_f = OBSColumn(
            id='t006c025_f',
            name='British Isles origins, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c017_f: DENOMINATOR },)

        t006c026_t = OBSColumn(
            id='t006c026_t',
            name='French origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_t: DENOMINATOR },)

        t006c026_m = OBSColumn(
            id='t006c026_m',
            name='French origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_m: DENOMINATOR },)

        t006c026_f = OBSColumn(
            id='t006c026_f',
            name='French origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_f: DENOMINATOR },)

        t006c027_t = OBSColumn(
            id='t006c027_t',
            name='Alsatian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c026_t: DENOMINATOR },)

        t006c027_m = OBSColumn(
            id='t006c027_m',
            name='Alsatian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c026_m: DENOMINATOR },)

        t006c027_f = OBSColumn(
            id='t006c027_f',
            name='Alsatian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c026_f: DENOMINATOR },)

        t006c028_t = OBSColumn(
            id='t006c028_t',
            name='Breton (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c026_t: DENOMINATOR },)

        t006c028_m = OBSColumn(
            id='t006c028_m',
            name='Breton (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c026_m: DENOMINATOR },)

        t006c028_f = OBSColumn(
            id='t006c028_f',
            name='Breton (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c026_f: DENOMINATOR },)

        t006c029_t = OBSColumn(
            id='t006c029_t',
            name='French (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c026_t: DENOMINATOR },)

        t006c029_m = OBSColumn(
            id='t006c029_m',
            name='French (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c026_m: DENOMINATOR },)

        t006c029_f = OBSColumn(
            id='t006c029_f',
            name='French (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c026_f: DENOMINATOR },)

        t006c030_t = OBSColumn(
            id='t006c030_t',
            name='Western European origins (except French origins) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_t: DENOMINATOR },)

        t006c030_m = OBSColumn(
            id='t006c030_m',
            name='Western European origins (except French origins) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_m: DENOMINATOR },)

        t006c030_f = OBSColumn(
            id='t006c030_f',
            name='Western European origins (except French origins) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_f: DENOMINATOR },)

        t006c031_t = OBSColumn(
            id='t006c031_t',
            name='Austrian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_t: DENOMINATOR },)

        t006c031_m = OBSColumn(
            id='t006c031_m',
            name='Austrian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_m: DENOMINATOR },)

        t006c031_f = OBSColumn(
            id='t006c031_f',
            name='Austrian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_f: DENOMINATOR },)

        t006c032_t = OBSColumn(
            id='t006c032_t',
            name='Belgian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_t: DENOMINATOR },)

        t006c032_m = OBSColumn(
            id='t006c032_m',
            name='Belgian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_m: DENOMINATOR },)

        t006c032_f = OBSColumn(
            id='t006c032_f',
            name='Belgian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_f: DENOMINATOR },)

        t006c033_t = OBSColumn(
            id='t006c033_t',
            name='Dutch (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_t: DENOMINATOR },)

        t006c033_m = OBSColumn(
            id='t006c033_m',
            name='Dutch (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_m: DENOMINATOR },)

        t006c033_f = OBSColumn(
            id='t006c033_f',
            name='Dutch (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_f: DENOMINATOR },)

        t006c034_t = OBSColumn(
            id='t006c034_t',
            name='Flemish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_t: DENOMINATOR },)

        t006c034_m = OBSColumn(
            id='t006c034_m',
            name='Flemish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_m: DENOMINATOR },)

        t006c034_f = OBSColumn(
            id='t006c034_f',
            name='Flemish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_f: DENOMINATOR },)

        t006c035_t = OBSColumn(
            id='t006c035_t',
            name='Frisian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_t: DENOMINATOR },)

        t006c035_m = OBSColumn(
            id='t006c035_m',
            name='Frisian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_m: DENOMINATOR },)

        t006c035_f = OBSColumn(
            id='t006c035_f',
            name='Frisian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_f: DENOMINATOR },)

        t006c036_t = OBSColumn(
            id='t006c036_t',
            name='German (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_t: DENOMINATOR },)

        t006c036_m = OBSColumn(
            id='t006c036_m',
            name='German (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_m: DENOMINATOR },)

        t006c036_f = OBSColumn(
            id='t006c036_f',
            name='German (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_f: DENOMINATOR },)

        t006c037_t = OBSColumn(
            id='t006c037_t',
            name='Luxembourger (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_t: DENOMINATOR },)

        t006c037_m = OBSColumn(
            id='t006c037_m',
            name='Luxembourger (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_m: DENOMINATOR },)

        t006c037_f = OBSColumn(
            id='t006c037_f',
            name='Luxembourger (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_f: DENOMINATOR },)

        t006c038_t = OBSColumn(
            id='t006c038_t',
            name='Swiss (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_t: DENOMINATOR },)

        t006c038_m = OBSColumn(
            id='t006c038_m',
            name='Swiss (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_m: DENOMINATOR },)

        t006c038_f = OBSColumn(
            id='t006c038_f',
            name='Swiss (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_f: DENOMINATOR },)

        t006c039_t = OBSColumn(
            id='t006c039_t',
            name='Western European origins, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_t: DENOMINATOR },)

        t006c039_m = OBSColumn(
            id='t006c039_m',
            name='Western European origins, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_m: DENOMINATOR },)

        t006c039_f = OBSColumn(
            id='t006c039_f',
            name='Western European origins, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c030_f: DENOMINATOR },)

        t006c040_t = OBSColumn(
            id='t006c040_t',
            name='Northern European origins (except British Isles origins) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_t: DENOMINATOR },)

        t006c040_m = OBSColumn(
            id='t006c040_m',
            name='Northern European origins (except British Isles origins) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_m: DENOMINATOR },)

        t006c040_f = OBSColumn(
            id='t006c040_f',
            name='Northern European origins (except British Isles origins) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_f: DENOMINATOR },)

        t006c041_t = OBSColumn(
            id='t006c041_t',
            name='Danish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c040_t: DENOMINATOR },)

        t006c041_m = OBSColumn(
            id='t006c041_m',
            name='Danish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c040_m: DENOMINATOR },)

        t006c041_f = OBSColumn(
            id='t006c041_f',
            name='Danish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c040_f: DENOMINATOR },)

        t006c042_t = OBSColumn(
            id='t006c042_t',
            name='Finnish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c040_t: DENOMINATOR },)

        t006c042_m = OBSColumn(
            id='t006c042_m',
            name='Finnish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c040_m: DENOMINATOR },)

        t006c042_f = OBSColumn(
            id='t006c042_f',
            name='Finnish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c040_f: DENOMINATOR },)

        t006c043_t = OBSColumn(
            id='t006c043_t',
            name='Icelandic (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c040_t: DENOMINATOR },)

        t006c043_m = OBSColumn(
            id='t006c043_m',
            name='Icelandic (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c040_m: DENOMINATOR },)

        t006c043_f = OBSColumn(
            id='t006c043_f',
            name='Icelandic (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c040_f: DENOMINATOR },)

        t006c044_t = OBSColumn(
            id='t006c044_t',
            name='Norwegian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c040_t: DENOMINATOR },)

        t006c044_m = OBSColumn(
            id='t006c044_m',
            name='Norwegian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c040_m: DENOMINATOR },)

        t006c044_f = OBSColumn(
            id='t006c044_f',
            name='Norwegian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c040_f: DENOMINATOR },)

        t006c045_t = OBSColumn(
            id='t006c045_t',
            name='Swedish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c040_t: DENOMINATOR },)

        t006c045_m = OBSColumn(
            id='t006c045_m',
            name='Swedish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c040_m: DENOMINATOR },)

        t006c045_f = OBSColumn(
            id='t006c045_f',
            name='Swedish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c040_f: DENOMINATOR },)

        t006c046_t = OBSColumn(
            id='t006c046_t',
            name='Northern European origins, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c040_t: DENOMINATOR },)

        t006c046_m = OBSColumn(
            id='t006c046_m',
            name='Northern European origins, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c040_m: DENOMINATOR },)

        t006c046_f = OBSColumn(
            id='t006c046_f',
            name='Northern European origins, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c040_f: DENOMINATOR },)

        t006c047_t = OBSColumn(
            id='t006c047_t',
            name='Eastern European origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_t: DENOMINATOR },)

        t006c047_m = OBSColumn(
            id='t006c047_m',
            name='Eastern European origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_m: DENOMINATOR },)

        t006c047_f = OBSColumn(
            id='t006c047_f',
            name='Eastern European origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_f: DENOMINATOR },)

        t006c048_t = OBSColumn(
            id='t006c048_t',
            name='Bulgarian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_t: DENOMINATOR },)

        t006c048_m = OBSColumn(
            id='t006c048_m',
            name='Bulgarian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_m: DENOMINATOR },)

        t006c048_f = OBSColumn(
            id='t006c048_f',
            name='Bulgarian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_f: DENOMINATOR },)

        t006c049_t = OBSColumn(
            id='t006c049_t',
            name='Byelorussian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_t: DENOMINATOR },)

        t006c049_m = OBSColumn(
            id='t006c049_m',
            name='Byelorussian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_m: DENOMINATOR },)

        t006c049_f = OBSColumn(
            id='t006c049_f',
            name='Byelorussian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_f: DENOMINATOR },)

        t006c050_t = OBSColumn(
            id='t006c050_t',
            name='Czech (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_t: DENOMINATOR },)

        t006c050_m = OBSColumn(
            id='t006c050_m',
            name='Czech (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_m: DENOMINATOR },)

        t006c050_f = OBSColumn(
            id='t006c050_f',
            name='Czech (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_f: DENOMINATOR },)

        t006c051_t = OBSColumn(
            id='t006c051_t',
            name='Czechoslovakian, n.o.s. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_t: DENOMINATOR },)

        t006c051_m = OBSColumn(
            id='t006c051_m',
            name='Czechoslovakian, n.o.s. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_m: DENOMINATOR },)

        t006c051_f = OBSColumn(
            id='t006c051_f',
            name='Czechoslovakian, n.o.s. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_f: DENOMINATOR },)

        t006c052_t = OBSColumn(
            id='t006c052_t',
            name='Estonian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_t: DENOMINATOR },)

        t006c052_m = OBSColumn(
            id='t006c052_m',
            name='Estonian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_m: DENOMINATOR },)

        t006c052_f = OBSColumn(
            id='t006c052_f',
            name='Estonian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_f: DENOMINATOR },)

        t006c053_t = OBSColumn(
            id='t006c053_t',
            name='Hungarian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_t: DENOMINATOR },)

        t006c053_m = OBSColumn(
            id='t006c053_m',
            name='Hungarian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_m: DENOMINATOR },)

        t006c053_f = OBSColumn(
            id='t006c053_f',
            name='Hungarian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_f: DENOMINATOR },)

        t006c054_t = OBSColumn(
            id='t006c054_t',
            name='Latvian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_t: DENOMINATOR },)

        t006c054_m = OBSColumn(
            id='t006c054_m',
            name='Latvian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_m: DENOMINATOR },)

        t006c054_f = OBSColumn(
            id='t006c054_f',
            name='Latvian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_f: DENOMINATOR },)

        t006c055_t = OBSColumn(
            id='t006c055_t',
            name='Lithuanian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_t: DENOMINATOR },)

        t006c055_m = OBSColumn(
            id='t006c055_m',
            name='Lithuanian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_m: DENOMINATOR },)

        t006c055_f = OBSColumn(
            id='t006c055_f',
            name='Lithuanian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_f: DENOMINATOR },)

        t006c056_t = OBSColumn(
            id='t006c056_t',
            name='Moldovan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_t: DENOMINATOR },)

        t006c056_m = OBSColumn(
            id='t006c056_m',
            name='Moldovan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_m: DENOMINATOR },)

        t006c056_f = OBSColumn(
            id='t006c056_f',
            name='Moldovan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_f: DENOMINATOR },)

        t006c057_t = OBSColumn(
            id='t006c057_t',
            name='Polish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_t: DENOMINATOR },)

        t006c057_m = OBSColumn(
            id='t006c057_m',
            name='Polish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_m: DENOMINATOR },)

        t006c057_f = OBSColumn(
            id='t006c057_f',
            name='Polish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_f: DENOMINATOR },)

        t006c058_t = OBSColumn(
            id='t006c058_t',
            name='Romanian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_t: DENOMINATOR },)

        t006c058_m = OBSColumn(
            id='t006c058_m',
            name='Romanian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_m: DENOMINATOR },)

        t006c058_f = OBSColumn(
            id='t006c058_f',
            name='Romanian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_f: DENOMINATOR },)

        t006c059_t = OBSColumn(
            id='t006c059_t',
            name='Russian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_t: DENOMINATOR },)

        t006c059_m = OBSColumn(
            id='t006c059_m',
            name='Russian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_m: DENOMINATOR },)

        t006c059_f = OBSColumn(
            id='t006c059_f',
            name='Russian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_f: DENOMINATOR },)

        t006c060_t = OBSColumn(
            id='t006c060_t',
            name='Slovak (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_t: DENOMINATOR },)

        t006c060_m = OBSColumn(
            id='t006c060_m',
            name='Slovak (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_m: DENOMINATOR },)

        t006c060_f = OBSColumn(
            id='t006c060_f',
            name='Slovak (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_f: DENOMINATOR },)

        t006c061_t = OBSColumn(
            id='t006c061_t',
            name='Ukrainian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_t: DENOMINATOR },)

        t006c061_m = OBSColumn(
            id='t006c061_m',
            name='Ukrainian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_m: DENOMINATOR },)

        t006c061_f = OBSColumn(
            id='t006c061_f',
            name='Ukrainian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_f: DENOMINATOR },)

        t006c062_t = OBSColumn(
            id='t006c062_t',
            name='Eastern European origins, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_t: DENOMINATOR },)

        t006c062_m = OBSColumn(
            id='t006c062_m',
            name='Eastern European origins, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_m: DENOMINATOR },)

        t006c062_f = OBSColumn(
            id='t006c062_f',
            name='Eastern European origins, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c047_f: DENOMINATOR },)

        t006c063_t = OBSColumn(
            id='t006c063_t',
            name='Southern European origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_t: DENOMINATOR },)

        t006c063_m = OBSColumn(
            id='t006c063_m',
            name='Southern European origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_m: DENOMINATOR },)

        t006c063_f = OBSColumn(
            id='t006c063_f',
            name='Southern European origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_f: DENOMINATOR },)

        t006c064_t = OBSColumn(
            id='t006c064_t',
            name='Albanian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_t: DENOMINATOR },)

        t006c064_m = OBSColumn(
            id='t006c064_m',
            name='Albanian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_m: DENOMINATOR },)

        t006c064_f = OBSColumn(
            id='t006c064_f',
            name='Albanian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_f: DENOMINATOR },)

        t006c065_t = OBSColumn(
            id='t006c065_t',
            name='Bosnian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_t: DENOMINATOR },)

        t006c065_m = OBSColumn(
            id='t006c065_m',
            name='Bosnian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_m: DENOMINATOR },)

        t006c065_f = OBSColumn(
            id='t006c065_f',
            name='Bosnian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_f: DENOMINATOR },)

        t006c066_t = OBSColumn(
            id='t006c066_t',
            name='Croatian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_t: DENOMINATOR },)

        t006c066_m = OBSColumn(
            id='t006c066_m',
            name='Croatian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_m: DENOMINATOR },)

        t006c066_f = OBSColumn(
            id='t006c066_f',
            name='Croatian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_f: DENOMINATOR },)

        t006c067_t = OBSColumn(
            id='t006c067_t',
            name='Cypriot (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_t: DENOMINATOR },)

        t006c067_m = OBSColumn(
            id='t006c067_m',
            name='Cypriot (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_m: DENOMINATOR },)

        t006c067_f = OBSColumn(
            id='t006c067_f',
            name='Cypriot (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_f: DENOMINATOR },)

        t006c068_t = OBSColumn(
            id='t006c068_t',
            name='Greek (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_t: DENOMINATOR },)

        t006c068_m = OBSColumn(
            id='t006c068_m',
            name='Greek (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_m: DENOMINATOR },)

        t006c068_f = OBSColumn(
            id='t006c068_f',
            name='Greek (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_f: DENOMINATOR },)

        t006c069_t = OBSColumn(
            id='t006c069_t',
            name='Italian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_t: DENOMINATOR },)

        t006c069_m = OBSColumn(
            id='t006c069_m',
            name='Italian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_m: DENOMINATOR },)

        t006c069_f = OBSColumn(
            id='t006c069_f',
            name='Italian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_f: DENOMINATOR },)

        t006c070_t = OBSColumn(
            id='t006c070_t',
            name='Kosovar (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_t: DENOMINATOR },)

        t006c070_m = OBSColumn(
            id='t006c070_m',
            name='Kosovar (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_m: DENOMINATOR },)

        t006c070_f = OBSColumn(
            id='t006c070_f',
            name='Kosovar (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_f: DENOMINATOR },)

        t006c071_t = OBSColumn(
            id='t006c071_t',
            name='Macedonian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_t: DENOMINATOR },)

        t006c071_m = OBSColumn(
            id='t006c071_m',
            name='Macedonian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_m: DENOMINATOR },)

        t006c071_f = OBSColumn(
            id='t006c071_f',
            name='Macedonian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_f: DENOMINATOR },)

        t006c072_t = OBSColumn(
            id='t006c072_t',
            name='Maltese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_t: DENOMINATOR },)

        t006c072_m = OBSColumn(
            id='t006c072_m',
            name='Maltese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_m: DENOMINATOR },)

        t006c072_f = OBSColumn(
            id='t006c072_f',
            name='Maltese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_f: DENOMINATOR },)

        t006c073_t = OBSColumn(
            id='t006c073_t',
            name='Montenegrin (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_t: DENOMINATOR },)

        t006c073_m = OBSColumn(
            id='t006c073_m',
            name='Montenegrin (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_m: DENOMINATOR },)

        t006c073_f = OBSColumn(
            id='t006c073_f',
            name='Montenegrin (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_f: DENOMINATOR },)

        t006c074_t = OBSColumn(
            id='t006c074_t',
            name='Portuguese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_t: DENOMINATOR },)

        t006c074_m = OBSColumn(
            id='t006c074_m',
            name='Portuguese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_m: DENOMINATOR },)

        t006c074_f = OBSColumn(
            id='t006c074_f',
            name='Portuguese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_f: DENOMINATOR },)

        t006c075_t = OBSColumn(
            id='t006c075_t',
            name='Serbian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_t: DENOMINATOR },)

        t006c075_m = OBSColumn(
            id='t006c075_m',
            name='Serbian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_m: DENOMINATOR },)

        t006c075_f = OBSColumn(
            id='t006c075_f',
            name='Serbian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_f: DENOMINATOR },)

        t006c076_t = OBSColumn(
            id='t006c076_t',
            name='Sicilian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_t: DENOMINATOR },)

        t006c076_m = OBSColumn(
            id='t006c076_m',
            name='Sicilian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_m: DENOMINATOR },)

        t006c076_f = OBSColumn(
            id='t006c076_f',
            name='Sicilian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_f: DENOMINATOR },)

        t006c077_t = OBSColumn(
            id='t006c077_t',
            name='Slovenian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_t: DENOMINATOR },)

        t006c077_m = OBSColumn(
            id='t006c077_m',
            name='Slovenian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_m: DENOMINATOR },)

        t006c077_f = OBSColumn(
            id='t006c077_f',
            name='Slovenian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_f: DENOMINATOR },)

        t006c078_t = OBSColumn(
            id='t006c078_t',
            name='Spanish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_t: DENOMINATOR },)

        t006c078_m = OBSColumn(
            id='t006c078_m',
            name='Spanish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_m: DENOMINATOR },)

        t006c078_f = OBSColumn(
            id='t006c078_f',
            name='Spanish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_f: DENOMINATOR },)

        t006c079_t = OBSColumn(
            id='t006c079_t',
            name='Yugoslavian, n.o.s. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_t: DENOMINATOR },)

        t006c079_m = OBSColumn(
            id='t006c079_m',
            name='Yugoslavian, n.o.s. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_m: DENOMINATOR },)

        t006c079_f = OBSColumn(
            id='t006c079_f',
            name='Yugoslavian, n.o.s. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_f: DENOMINATOR },)

        t006c080_t = OBSColumn(
            id='t006c080_t',
            name='Southern European origins, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_t: DENOMINATOR },)

        t006c080_m = OBSColumn(
            id='t006c080_m',
            name='Southern European origins, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_m: DENOMINATOR },)

        t006c080_f = OBSColumn(
            id='t006c080_f',
            name='Southern European origins, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c063_f: DENOMINATOR },)

        t006c081_t = OBSColumn(
            id='t006c081_t',
            name='Other European origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_t: DENOMINATOR },)

        t006c081_m = OBSColumn(
            id='t006c081_m',
            name='Other European origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_m: DENOMINATOR },)

        t006c081_f = OBSColumn(
            id='t006c081_f',
            name='Other European origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c016_f: DENOMINATOR },)

        t006c082_t = OBSColumn(
            id='t006c082_t',
            name='Basque (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c081_t: DENOMINATOR },)

        t006c082_m = OBSColumn(
            id='t006c082_m',
            name='Basque (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c081_m: DENOMINATOR },)

        t006c082_f = OBSColumn(
            id='t006c082_f',
            name='Basque (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c081_f: DENOMINATOR },)

        t006c083_t = OBSColumn(
            id='t006c083_t',
            name='Jewish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c081_t: DENOMINATOR },)

        t006c083_m = OBSColumn(
            id='t006c083_m',
            name='Jewish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c081_m: DENOMINATOR },)

        t006c083_f = OBSColumn(
            id='t006c083_f',
            name='Jewish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c081_f: DENOMINATOR },)

        t006c084_t = OBSColumn(
            id='t006c084_t',
            name='Roma (Gypsy) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c081_t: DENOMINATOR },)

        t006c084_m = OBSColumn(
            id='t006c084_m',
            name='Roma (Gypsy) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c081_m: DENOMINATOR },)

        t006c084_f = OBSColumn(
            id='t006c084_f',
            name='Roma (Gypsy) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c081_f: DENOMINATOR },)

        t006c085_t = OBSColumn(
            id='t006c085_t',
            name='Slavic, n.o.s. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c081_t: DENOMINATOR },)

        t006c085_m = OBSColumn(
            id='t006c085_m',
            name='Slavic, n.o.s. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c081_m: DENOMINATOR },)

        t006c085_f = OBSColumn(
            id='t006c085_f',
            name='Slavic, n.o.s. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c081_f: DENOMINATOR },)

        t006c086_t = OBSColumn(
            id='t006c086_t',
            name='Other European origins, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c081_t: DENOMINATOR },)

        t006c086_m = OBSColumn(
            id='t006c086_m',
            name='Other European origins, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c081_m: DENOMINATOR },)

        t006c086_f = OBSColumn(
            id='t006c086_f',
            name='Other European origins, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c081_f: DENOMINATOR },)

        t006c087_t = OBSColumn(
            id='t006c087_t',
            name='Total population - Caribbean origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_t: DENOMINATOR },)

        t006c087_m = OBSColumn(
            id='t006c087_m',
            name='Caribbean origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_m: DENOMINATOR },)

        t006c087_f = OBSColumn(
            id='t006c087_f',
            name='Caribbean origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_f: DENOMINATOR },)

        t006c088_t = OBSColumn(
            id='t006c088_t',
            name='Antiguan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_t: DENOMINATOR },)

        t006c088_m = OBSColumn(
            id='t006c088_m',
            name='Antiguan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_m: DENOMINATOR },)

        t006c088_f = OBSColumn(
            id='t006c088_f',
            name='Antiguan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_f: DENOMINATOR },)

        t006c089_t = OBSColumn(
            id='t006c089_t',
            name='Bahamian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_t: DENOMINATOR },)

        t006c089_m = OBSColumn(
            id='t006c089_m',
            name='Bahamian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_m: DENOMINATOR },)

        t006c089_f = OBSColumn(
            id='t006c089_f',
            name='Bahamian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_f: DENOMINATOR },)

        t006c090_t = OBSColumn(
            id='t006c090_t',
            name='Barbadian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_t: DENOMINATOR },)

        t006c090_m = OBSColumn(
            id='t006c090_m',
            name='Barbadian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_m: DENOMINATOR },)

        t006c090_f = OBSColumn(
            id='t006c090_f',
            name='Barbadian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_f: DENOMINATOR },)

        t006c091_t = OBSColumn(
            id='t006c091_t',
            name='Bermudan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_t: DENOMINATOR },)

        t006c091_m = OBSColumn(
            id='t006c091_m',
            name='Bermudan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_m: DENOMINATOR },)

        t006c091_f = OBSColumn(
            id='t006c091_f',
            name='Bermudan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_f: DENOMINATOR },)

        t006c092_t = OBSColumn(
            id='t006c092_t',
            name='Carib (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_t: DENOMINATOR },)

        t006c092_m = OBSColumn(
            id='t006c092_m',
            name='Carib (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_m: DENOMINATOR },)

        t006c092_f = OBSColumn(
            id='t006c092_f',
            name='Carib (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_f: DENOMINATOR },)

        t006c093_t = OBSColumn(
            id='t006c093_t',
            name='Cuban (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_t: DENOMINATOR },)

        t006c093_m = OBSColumn(
            id='t006c093_m',
            name='Cuban (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_m: DENOMINATOR },)

        t006c093_f = OBSColumn(
            id='t006c093_f',
            name='Cuban (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_f: DENOMINATOR },)

        t006c094_t = OBSColumn(
            id='t006c094_t',
            name='Dominican (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_t: DENOMINATOR },)

        t006c094_m = OBSColumn(
            id='t006c094_m',
            name='Dominican (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_m: DENOMINATOR },)

        t006c094_f = OBSColumn(
            id='t006c094_f',
            name='Dominican (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_f: DENOMINATOR },)

        t006c095_t = OBSColumn(
            id='t006c095_t',
            name='Grenadian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_t: DENOMINATOR },)

        t006c095_m = OBSColumn(
            id='t006c095_m',
            name='Grenadian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_m: DENOMINATOR },)

        t006c095_f = OBSColumn(
            id='t006c095_f',
            name='Grenadian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_f: DENOMINATOR },)

        t006c096_t = OBSColumn(
            id='t006c096_t',
            name='Haitian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_t: DENOMINATOR },)

        t006c096_m = OBSColumn(
            id='t006c096_m',
            name='Haitian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_m: DENOMINATOR },)

        t006c096_f = OBSColumn(
            id='t006c096_f',
            name='Haitian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_f: DENOMINATOR },)

        t006c097_t = OBSColumn(
            id='t006c097_t',
            name='Jamaican (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_t: DENOMINATOR },)

        t006c097_m = OBSColumn(
            id='t006c097_m',
            name='Jamaican (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_m: DENOMINATOR },)

        t006c097_f = OBSColumn(
            id='t006c097_f',
            name='Jamaican (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_f: DENOMINATOR },)

        t006c098_t = OBSColumn(
            id='t006c098_t',
            name='Kittitian/Nevisian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_t: DENOMINATOR },)

        t006c098_m = OBSColumn(
            id='t006c098_m',
            name='Kittitian/Nevisian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_m: DENOMINATOR },)

        t006c098_f = OBSColumn(
            id='t006c098_f',
            name='Kittitian/Nevisian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_f: DENOMINATOR },)

        t006c099_t = OBSColumn(
            id='t006c099_t',
            name='Martinican (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_t: DENOMINATOR },)

        t006c099_m = OBSColumn(
            id='t006c099_m',
            name='Martinican (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_m: DENOMINATOR },)

        t006c099_f = OBSColumn(
            id='t006c099_f',
            name='Martinican (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_f: DENOMINATOR },)

        t006c100_t = OBSColumn(
            id='t006c100_t',
            name='Montserratan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_t: DENOMINATOR },)

        t006c100_m = OBSColumn(
            id='t006c100_m',
            name='Montserratan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_m: DENOMINATOR },)

        t006c100_f = OBSColumn(
            id='t006c100_f',
            name='Montserratan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_f: DENOMINATOR },)

        t006c101_t = OBSColumn(
            id='t006c101_t',
            name='Puerto Rican (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_t: DENOMINATOR },)

        t006c101_m = OBSColumn(
            id='t006c101_m',
            name='Puerto Rican (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_m: DENOMINATOR },)

        t006c101_f = OBSColumn(
            id='t006c101_f',
            name='Puerto Rican (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_f: DENOMINATOR },)

        t006c102_t = OBSColumn(
            id='t006c102_t',
            name='St. Lucian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_t: DENOMINATOR },)

        t006c102_m = OBSColumn(
            id='t006c102_m',
            name='St. Lucian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_m: DENOMINATOR },)

        t006c102_f = OBSColumn(
            id='t006c102_f',
            name='St. Lucian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_f: DENOMINATOR },)

        t006c103_t = OBSColumn(
            id='t006c103_t',
            name='Trinidadian/Tobagonian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_t: DENOMINATOR },)

        t006c103_m = OBSColumn(
            id='t006c103_m',
            name='Trinidadian/Tobagonian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_m: DENOMINATOR },)

        t006c103_f = OBSColumn(
            id='t006c103_f',
            name='Trinidadian/Tobagonian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_f: DENOMINATOR },)

        t006c104_t = OBSColumn(
            id='t006c104_t',
            name='Vincentian/Grenadinian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_t: DENOMINATOR },)

        t006c104_m = OBSColumn(
            id='t006c104_m',
            name='Vincentian/Grenadinian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_m: DENOMINATOR },)

        t006c104_f = OBSColumn(
            id='t006c104_f',
            name='Vincentian/Grenadinian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_f: DENOMINATOR },)

        t006c105_t = OBSColumn(
            id='t006c105_t',
            name='West Indian, n.o.s. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_t: DENOMINATOR },)

        t006c105_m = OBSColumn(
            id='t006c105_m',
            name='West Indian, n.o.s. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_m: DENOMINATOR },)

        t006c105_f = OBSColumn(
            id='t006c105_f',
            name='West Indian, n.o.s. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_f: DENOMINATOR },)

        t006c106_t = OBSColumn(
            id='t006c106_t',
            name='Caribbean origins, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_t: DENOMINATOR },)

        t006c106_m = OBSColumn(
            id='t006c106_m',
            name='Caribbean origins, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_m: DENOMINATOR },)

        t006c106_f = OBSColumn(
            id='t006c106_f',
            name='Caribbean origins, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c087_f: DENOMINATOR },)

        t006c107_t = OBSColumn(
            id='t006c107_t',
            name='Total population - Latin, Central and South American origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_t: DENOMINATOR },)

        t006c107_m = OBSColumn(
            id='t006c107_m',
            name='Latin, Central and South American origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_m: DENOMINATOR },)

        t006c107_f = OBSColumn(
            id='t006c107_f',
            name='Latin, Central and South American origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_f: DENOMINATOR },)

        t006c108_t = OBSColumn(
            id='t006c108_t',
            name='Aboriginal from Central/South America (except Maya) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c108_m = OBSColumn(
            id='t006c108_m',
            name='Aboriginal from Central/South America (except Maya) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c108_f = OBSColumn(
            id='t006c108_f',
            name='Aboriginal from Central/South America (except Maya) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c109_t = OBSColumn(
            id='t006c109_t',
            name='Argentinian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c109_m = OBSColumn(
            id='t006c109_m',
            name='Argentinian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c109_f = OBSColumn(
            id='t006c109_f',
            name='Argentinian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c110_t = OBSColumn(
            id='t006c110_t',
            name='Belizean (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c110_m = OBSColumn(
            id='t006c110_m',
            name='Belizean (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c110_f = OBSColumn(
            id='t006c110_f',
            name='Belizean (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c111_t = OBSColumn(
            id='t006c111_t',
            name='Bolivian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c111_m = OBSColumn(
            id='t006c111_m',
            name='Bolivian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c111_f = OBSColumn(
            id='t006c111_f',
            name='Bolivian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c112_t = OBSColumn(
            id='t006c112_t',
            name='Brazilian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c112_m = OBSColumn(
            id='t006c112_m',
            name='Brazilian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c112_f = OBSColumn(
            id='t006c112_f',
            name='Brazilian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c113_t = OBSColumn(
            id='t006c113_t',
            name='Chilean (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c113_m = OBSColumn(
            id='t006c113_m',
            name='Chilean (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c113_f = OBSColumn(
            id='t006c113_f',
            name='Chilean (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c114_t = OBSColumn(
            id='t006c114_t',
            name='Colombian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c114_m = OBSColumn(
            id='t006c114_m',
            name='Colombian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c114_f = OBSColumn(
            id='t006c114_f',
            name='Colombian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c115_t = OBSColumn(
            id='t006c115_t',
            name='Costa Rican (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c115_m = OBSColumn(
            id='t006c115_m',
            name='Costa Rican (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c115_f = OBSColumn(
            id='t006c115_f',
            name='Costa Rican (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c116_t = OBSColumn(
            id='t006c116_t',
            name='Ecuadorian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c116_m = OBSColumn(
            id='t006c116_m',
            name='Ecuadorian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c116_f = OBSColumn(
            id='t006c116_f',
            name='Ecuadorian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c117_t = OBSColumn(
            id='t006c117_t',
            name='Guatemalan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c117_m = OBSColumn(
            id='t006c117_m',
            name='Guatemalan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c117_f = OBSColumn(
            id='t006c117_f',
            name='Guatemalan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c118_t = OBSColumn(
            id='t006c118_t',
            name='Guyanese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c118_m = OBSColumn(
            id='t006c118_m',
            name='Guyanese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c118_f = OBSColumn(
            id='t006c118_f',
            name='Guyanese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c119_t = OBSColumn(
            id='t006c119_t',
            name='Hispanic (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c119_m = OBSColumn(
            id='t006c119_m',
            name='Hispanic (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c119_f = OBSColumn(
            id='t006c119_f',
            name='Hispanic (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c120_t = OBSColumn(
            id='t006c120_t',
            name='Honduran (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c120_m = OBSColumn(
            id='t006c120_m',
            name='Honduran (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c120_f = OBSColumn(
            id='t006c120_f',
            name='Honduran (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c121_t = OBSColumn(
            id='t006c121_t',
            name='Maya (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c121_m = OBSColumn(
            id='t006c121_m',
            name='Maya (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c121_f = OBSColumn(
            id='t006c121_f',
            name='Maya (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c122_t = OBSColumn(
            id='t006c122_t',
            name='Mexican (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c122_m = OBSColumn(
            id='t006c122_m',
            name='Mexican (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c122_f = OBSColumn(
            id='t006c122_f',
            name='Mexican (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c123_t = OBSColumn(
            id='t006c123_t',
            name='Nicaraguan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c123_m = OBSColumn(
            id='t006c123_m',
            name='Nicaraguan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c123_f = OBSColumn(
            id='t006c123_f',
            name='Nicaraguan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c124_t = OBSColumn(
            id='t006c124_t',
            name='Panamanian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c124_m = OBSColumn(
            id='t006c124_m',
            name='Panamanian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c124_f = OBSColumn(
            id='t006c124_f',
            name='Panamanian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c125_t = OBSColumn(
            id='t006c125_t',
            name='Paraguayan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c125_m = OBSColumn(
            id='t006c125_m',
            name='Paraguayan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c125_f = OBSColumn(
            id='t006c125_f',
            name='Paraguayan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c126_t = OBSColumn(
            id='t006c126_t',
            name='Peruvian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c126_m = OBSColumn(
            id='t006c126_m',
            name='Peruvian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c126_f = OBSColumn(
            id='t006c126_f',
            name='Peruvian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c127_t = OBSColumn(
            id='t006c127_t',
            name='Salvadorean (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c127_m = OBSColumn(
            id='t006c127_m',
            name='Salvadorean (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c127_f = OBSColumn(
            id='t006c127_f',
            name='Salvadorean (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c128_t = OBSColumn(
            id='t006c128_t',
            name='Uruguayan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c128_m = OBSColumn(
            id='t006c128_m',
            name='Uruguayan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c128_f = OBSColumn(
            id='t006c128_f',
            name='Uruguayan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c129_t = OBSColumn(
            id='t006c129_t',
            name='Venezuelan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c129_m = OBSColumn(
            id='t006c129_m',
            name='Venezuelan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c129_f = OBSColumn(
            id='t006c129_f',
            name='Venezuelan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c130_t = OBSColumn(
            id='t006c130_t',
            name='Latin, Central and South American origins, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_t: DENOMINATOR },)

        t006c130_m = OBSColumn(
            id='t006c130_m',
            name='Latin, Central and South American origins, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_m: DENOMINATOR },)

        t006c130_f = OBSColumn(
            id='t006c130_f',
            name='Latin, Central and South American origins, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c107_f: DENOMINATOR },)

        t006c131_t = OBSColumn(
            id='t006c131_t',
            name='Total population - African origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_t: DENOMINATOR },)

        t006c131_m = OBSColumn(
            id='t006c131_m',
            name='African origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_m: DENOMINATOR },)

        t006c131_f = OBSColumn(
            id='t006c131_f',
            name='African origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_f: DENOMINATOR },)

        t006c132_t = OBSColumn(
            id='t006c132_t',
            name='Central and West African origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c131_t: DENOMINATOR },)

        t006c132_m = OBSColumn(
            id='t006c132_m',
            name='Central and West African origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c131_m: DENOMINATOR },)

        t006c132_f = OBSColumn(
            id='t006c132_f',
            name='Central and West African origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c131_f: DENOMINATOR },)

        t006c133_t = OBSColumn(
            id='t006c133_t',
            name='Akan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c133_m = OBSColumn(
            id='t006c133_m',
            name='Akan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c133_f = OBSColumn(
            id='t006c133_f',
            name='Akan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c134_t = OBSColumn(
            id='t006c134_t',
            name='Angolan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c134_m = OBSColumn(
            id='t006c134_m',
            name='Angolan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c134_f = OBSColumn(
            id='t006c134_f',
            name='Angolan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c135_t = OBSColumn(
            id='t006c135_t',
            name='Ashanti (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c135_m = OBSColumn(
            id='t006c135_m',
            name='Ashanti (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c135_f = OBSColumn(
            id='t006c135_f',
            name='Ashanti (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c136_t = OBSColumn(
            id='t006c136_t',
            name='Beninese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c136_m = OBSColumn(
            id='t006c136_m',
            name='Beninese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c136_f = OBSColumn(
            id='t006c136_f',
            name='Beninese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c137_t = OBSColumn(
            id='t006c137_t',
            name='Burkinabe (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c137_m = OBSColumn(
            id='t006c137_m',
            name='Burkinabe (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c137_f = OBSColumn(
            id='t006c137_f',
            name='Burkinabe (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c138_t = OBSColumn(
            id='t006c138_t',
            name='Cameroonian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c138_m = OBSColumn(
            id='t006c138_m',
            name='Cameroonian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c138_f = OBSColumn(
            id='t006c138_f',
            name='Cameroonian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c139_t = OBSColumn(
            id='t006c139_t',
            name='Chadian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c139_m = OBSColumn(
            id='t006c139_m',
            name='Chadian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c139_f = OBSColumn(
            id='t006c139_f',
            name='Chadian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c140_t = OBSColumn(
            id='t006c140_t',
            name='Congolese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c140_m = OBSColumn(
            id='t006c140_m',
            name='Congolese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c140_f = OBSColumn(
            id='t006c140_f',
            name='Congolese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c141_t = OBSColumn(
            id='t006c141_t',
            name='Gabonese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c141_m = OBSColumn(
            id='t006c141_m',
            name='Gabonese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c141_f = OBSColumn(
            id='t006c141_f',
            name='Gabonese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c142_t = OBSColumn(
            id='t006c142_t',
            name='Gambian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c142_m = OBSColumn(
            id='t006c142_m',
            name='Gambian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c142_f = OBSColumn(
            id='t006c142_f',
            name='Gambian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c143_t = OBSColumn(
            id='t006c143_t',
            name='Ghanaian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c143_m = OBSColumn(
            id='t006c143_m',
            name='Ghanaian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c143_f = OBSColumn(
            id='t006c143_f',
            name='Ghanaian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c144_t = OBSColumn(
            id='t006c144_t',
            name='Guinean (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c144_m = OBSColumn(
            id='t006c144_m',
            name='Guinean (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c144_f = OBSColumn(
            id='t006c144_f',
            name='Guinean (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c145_t = OBSColumn(
            id='t006c145_t',
            name='Ibo (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c145_m = OBSColumn(
            id='t006c145_m',
            name='Ibo (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c145_f = OBSColumn(
            id='t006c145_f',
            name='Ibo (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c146_t = OBSColumn(
            id='t006c146_t',
            name='Ivorian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c146_m = OBSColumn(
            id='t006c146_m',
            name='Ivorian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c146_f = OBSColumn(
            id='t006c146_f',
            name='Ivorian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c147_t = OBSColumn(
            id='t006c147_t',
            name='Liberian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c147_m = OBSColumn(
            id='t006c147_m',
            name='Liberian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c147_f = OBSColumn(
            id='t006c147_f',
            name='Liberian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c148_t = OBSColumn(
            id='t006c148_t',
            name='Malian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c148_m = OBSColumn(
            id='t006c148_m',
            name='Malian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c148_f = OBSColumn(
            id='t006c148_f',
            name='Malian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c149_t = OBSColumn(
            id='t006c149_t',
            name='Nigerian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c149_m = OBSColumn(
            id='t006c149_m',
            name='Nigerian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c149_f = OBSColumn(
            id='t006c149_f',
            name='Nigerian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c150_t = OBSColumn(
            id='t006c150_t',
            name='Peulh (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c150_m = OBSColumn(
            id='t006c150_m',
            name='Peulh (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c150_f = OBSColumn(
            id='t006c150_f',
            name='Peulh (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c151_t = OBSColumn(
            id='t006c151_t',
            name='Senegalese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c151_m = OBSColumn(
            id='t006c151_m',
            name='Senegalese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c151_f = OBSColumn(
            id='t006c151_f',
            name='Senegalese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c152_t = OBSColumn(
            id='t006c152_t',
            name='Sierra Leonean (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c152_m = OBSColumn(
            id='t006c152_m',
            name='Sierra Leonean (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c152_f = OBSColumn(
            id='t006c152_f',
            name='Sierra Leonean (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c153_t = OBSColumn(
            id='t006c153_t',
            name='Togolese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c153_m = OBSColumn(
            id='t006c153_m',
            name='Togolese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c153_f = OBSColumn(
            id='t006c153_f',
            name='Togolese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c154_t = OBSColumn(
            id='t006c154_t',
            name='Yoruba (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c154_m = OBSColumn(
            id='t006c154_m',
            name='Yoruba (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c154_f = OBSColumn(
            id='t006c154_f',
            name='Yoruba (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c155_t = OBSColumn(
            id='t006c155_t',
            name='Central and West African origins, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_t: DENOMINATOR },)

        t006c155_m = OBSColumn(
            id='t006c155_m',
            name='Central and West African origins, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_m: DENOMINATOR },)

        t006c155_f = OBSColumn(
            id='t006c155_f',
            name='Central and West African origins, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c132_f: DENOMINATOR },)

        t006c156_t = OBSColumn(
            id='t006c156_t',
            name='North African origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c131_t: DENOMINATOR },)

        t006c156_m = OBSColumn(
            id='t006c156_m',
            name='North African origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c131_m: DENOMINATOR },)

        t006c156_f = OBSColumn(
            id='t006c156_f',
            name='North African origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c131_f: DENOMINATOR },)

        t006c157_t = OBSColumn(
            id='t006c157_t',
            name='Algerian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_t: DENOMINATOR },)

        t006c157_m = OBSColumn(
            id='t006c157_m',
            name='Algerian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_m: DENOMINATOR },)

        t006c157_f = OBSColumn(
            id='t006c157_f',
            name='Algerian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_f: DENOMINATOR },)

        t006c158_t = OBSColumn(
            id='t006c158_t',
            name='Berber (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_t: DENOMINATOR },)

        t006c158_m = OBSColumn(
            id='t006c158_m',
            name='Berber (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_m: DENOMINATOR },)

        t006c158_f = OBSColumn(
            id='t006c158_f',
            name='Berber (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_f: DENOMINATOR },)

        t006c159_t = OBSColumn(
            id='t006c159_t',
            name='Coptic (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_t: DENOMINATOR },)

        t006c159_m = OBSColumn(
            id='t006c159_m',
            name='Coptic (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_m: DENOMINATOR },)

        t006c159_f = OBSColumn(
            id='t006c159_f',
            name='Coptic (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_f: DENOMINATOR },)

        t006c160_t = OBSColumn(
            id='t006c160_t',
            name='Dinka (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_t: DENOMINATOR },)

        t006c160_m = OBSColumn(
            id='t006c160_m',
            name='Dinka (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_m: DENOMINATOR },)

        t006c160_f = OBSColumn(
            id='t006c160_f',
            name='Dinka (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_f: DENOMINATOR },)

        t006c161_t = OBSColumn(
            id='t006c161_t',
            name='Egyptian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_t: DENOMINATOR },)

        t006c161_m = OBSColumn(
            id='t006c161_m',
            name='Egyptian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_m: DENOMINATOR },)

        t006c161_f = OBSColumn(
            id='t006c161_f',
            name='Egyptian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_f: DENOMINATOR },)

        t006c162_t = OBSColumn(
            id='t006c162_t',
            name='Libyan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_t: DENOMINATOR },)

        t006c162_m = OBSColumn(
            id='t006c162_m',
            name='Libyan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_m: DENOMINATOR },)

        t006c162_f = OBSColumn(
            id='t006c162_f',
            name='Libyan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_f: DENOMINATOR },)

        t006c163_t = OBSColumn(
            id='t006c163_t',
            name='Maure (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_t: DENOMINATOR },)

        t006c163_m = OBSColumn(
            id='t006c163_m',
            name='Maure (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_m: DENOMINATOR },)

        t006c163_f = OBSColumn(
            id='t006c163_f',
            name='Maure (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_f: DENOMINATOR },)

        t006c164_t = OBSColumn(
            id='t006c164_t',
            name='Moroccan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_t: DENOMINATOR },)

        t006c164_m = OBSColumn(
            id='t006c164_m',
            name='Moroccan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_m: DENOMINATOR },)

        t006c164_f = OBSColumn(
            id='t006c164_f',
            name='Moroccan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_f: DENOMINATOR },)

        t006c165_t = OBSColumn(
            id='t006c165_t',
            name='Sudanese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_t: DENOMINATOR },)

        t006c165_m = OBSColumn(
            id='t006c165_m',
            name='Sudanese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_m: DENOMINATOR },)

        t006c165_f = OBSColumn(
            id='t006c165_f',
            name='Sudanese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_f: DENOMINATOR },)

        t006c166_t = OBSColumn(
            id='t006c166_t',
            name='Tunisian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_t: DENOMINATOR },)

        t006c166_m = OBSColumn(
            id='t006c166_m',
            name='Tunisian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_m: DENOMINATOR },)

        t006c166_f = OBSColumn(
            id='t006c166_f',
            name='Tunisian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_f: DENOMINATOR },)

        t006c167_t = OBSColumn(
            id='t006c167_t',
            name='North African origins, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_t: DENOMINATOR },)

        t006c167_m = OBSColumn(
            id='t006c167_m',
            name='North African origins, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_m: DENOMINATOR },)

        t006c167_f = OBSColumn(
            id='t006c167_f',
            name='North African origins, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c156_f: DENOMINATOR },)

        t006c168_t = OBSColumn(
            id='t006c168_t',
            name='Southern and East African origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c131_t: DENOMINATOR },)

        t006c168_m = OBSColumn(
            id='t006c168_m',
            name='Southern and East African origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c131_m: DENOMINATOR },)

        t006c168_f = OBSColumn(
            id='t006c168_f',
            name='Southern and East African origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c131_f: DENOMINATOR },)

        t006c169_t = OBSColumn(
            id='t006c169_t',
            name='Afrikaner (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c169_m = OBSColumn(
            id='t006c169_m',
            name='Afrikaner (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c169_f = OBSColumn(
            id='t006c169_f',
            name='Afrikaner (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c170_t = OBSColumn(
            id='t006c170_t',
            name='Amhara (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c170_m = OBSColumn(
            id='t006c170_m',
            name='Amhara (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c170_f = OBSColumn(
            id='t006c170_f',
            name='Amhara (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c171_t = OBSColumn(
            id='t006c171_t',
            name='Bantu, n.o.s. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c171_m = OBSColumn(
            id='t006c171_m',
            name='Bantu, n.o.s. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c171_f = OBSColumn(
            id='t006c171_f',
            name='Bantu, n.o.s. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c172_t = OBSColumn(
            id='t006c172_t',
            name='Burundian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c172_m = OBSColumn(
            id='t006c172_m',
            name='Burundian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c172_f = OBSColumn(
            id='t006c172_f',
            name='Burundian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c173_t = OBSColumn(
            id='t006c173_t',
            name='Eritrean (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c173_m = OBSColumn(
            id='t006c173_m',
            name='Eritrean (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c173_f = OBSColumn(
            id='t006c173_f',
            name='Eritrean (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c174_t = OBSColumn(
            id='t006c174_t',
            name='Ethiopian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c174_m = OBSColumn(
            id='t006c174_m',
            name='Ethiopian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c174_f = OBSColumn(
            id='t006c174_f',
            name='Ethiopian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c175_t = OBSColumn(
            id='t006c175_t',
            name='Harari (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c175_m = OBSColumn(
            id='t006c175_m',
            name='Harari (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c175_f = OBSColumn(
            id='t006c175_f',
            name='Harari (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c176_t = OBSColumn(
            id='t006c176_t',
            name='Kenyan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c176_m = OBSColumn(
            id='t006c176_m',
            name='Kenyan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c176_f = OBSColumn(
            id='t006c176_f',
            name='Kenyan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c177_t = OBSColumn(
            id='t006c177_t',
            name='Malagasy (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c177_m = OBSColumn(
            id='t006c177_m',
            name='Malagasy (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c177_f = OBSColumn(
            id='t006c177_f',
            name='Malagasy (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c178_t = OBSColumn(
            id='t006c178_t',
            name='Mauritian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c178_m = OBSColumn(
            id='t006c178_m',
            name='Mauritian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c178_f = OBSColumn(
            id='t006c178_f',
            name='Mauritian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c179_t = OBSColumn(
            id='t006c179_t',
            name='Oromo (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c179_m = OBSColumn(
            id='t006c179_m',
            name='Oromo (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c179_f = OBSColumn(
            id='t006c179_f',
            name='Oromo (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c180_t = OBSColumn(
            id='t006c180_t',
            name='Rwandan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c180_m = OBSColumn(
            id='t006c180_m',
            name='Rwandan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c180_f = OBSColumn(
            id='t006c180_f',
            name='Rwandan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c181_t = OBSColumn(
            id='t006c181_t',
            name='Seychellois (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c181_m = OBSColumn(
            id='t006c181_m',
            name='Seychellois (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c181_f = OBSColumn(
            id='t006c181_f',
            name='Seychellois (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c182_t = OBSColumn(
            id='t006c182_t',
            name='Somali (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c182_m = OBSColumn(
            id='t006c182_m',
            name='Somali (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c182_f = OBSColumn(
            id='t006c182_f',
            name='Somali (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c183_t = OBSColumn(
            id='t006c183_t',
            name='South African (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c183_m = OBSColumn(
            id='t006c183_m',
            name='South African (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c183_f = OBSColumn(
            id='t006c183_f',
            name='South African (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c184_t = OBSColumn(
            id='t006c184_t',
            name='Tanzanian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c184_m = OBSColumn(
            id='t006c184_m',
            name='Tanzanian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c184_f = OBSColumn(
            id='t006c184_f',
            name='Tanzanian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c185_t = OBSColumn(
            id='t006c185_t',
            name='Tigrian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c185_m = OBSColumn(
            id='t006c185_m',
            name='Tigrian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c185_f = OBSColumn(
            id='t006c185_f',
            name='Tigrian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c186_t = OBSColumn(
            id='t006c186_t',
            name='Ugandan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c186_m = OBSColumn(
            id='t006c186_m',
            name='Ugandan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c186_f = OBSColumn(
            id='t006c186_f',
            name='Ugandan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c187_t = OBSColumn(
            id='t006c187_t',
            name='Zambian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c187_m = OBSColumn(
            id='t006c187_m',
            name='Zambian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c187_f = OBSColumn(
            id='t006c187_f',
            name='Zambian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c188_t = OBSColumn(
            id='t006c188_t',
            name='Zimbabwean (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c188_m = OBSColumn(
            id='t006c188_m',
            name='Zimbabwean (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c188_f = OBSColumn(
            id='t006c188_f',
            name='Zimbabwean (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c189_t = OBSColumn(
            id='t006c189_t',
            name='Zulu (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c189_m = OBSColumn(
            id='t006c189_m',
            name='Zulu (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c189_f = OBSColumn(
            id='t006c189_f',
            name='Zulu (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c190_t = OBSColumn(
            id='t006c190_t',
            name='Southern and East African origins, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_t: DENOMINATOR },)

        t006c190_m = OBSColumn(
            id='t006c190_m',
            name='Southern and East African origins, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_m: DENOMINATOR },)

        t006c190_f = OBSColumn(
            id='t006c190_f',
            name='Southern and East African origins, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c168_f: DENOMINATOR },)

        t006c191_t = OBSColumn(
            id='t006c191_t',
            name='Other African origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c131_t: DENOMINATOR },)

        t006c191_m = OBSColumn(
            id='t006c191_m',
            name='Other African origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c131_m: DENOMINATOR },)

        t006c191_f = OBSColumn(
            id='t006c191_f',
            name='Other African origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c131_f: DENOMINATOR },)

        t006c192_t = OBSColumn(
            id='t006c192_t',
            name='Black, n.o.s. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c191_t: DENOMINATOR },)

        t006c192_m = OBSColumn(
            id='t006c192_m',
            name='Black, n.o.s. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c191_m: DENOMINATOR },)

        t006c192_f = OBSColumn(
            id='t006c192_f',
            name='Black, n.o.s. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c191_f: DENOMINATOR },)

        t006c193_t = OBSColumn(
            id='t006c193_t',
            name='Other African origins, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c191_t: DENOMINATOR },)

        t006c193_m = OBSColumn(
            id='t006c193_m',
            name='Other African origins, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c191_m: DENOMINATOR },)

        t006c193_f = OBSColumn(
            id='t006c193_f',
            name='Other African origins, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c191_f: DENOMINATOR },)

        t006c194_t = OBSColumn(
            id='t006c194_t',
            name='Total population - Asian origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_t: DENOMINATOR },)

        t006c194_m = OBSColumn(
            id='t006c194_m',
            name='Asian origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_m: DENOMINATOR },)

        t006c194_f = OBSColumn(
            id='t006c194_f',
            name='Asian origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_f: DENOMINATOR },)

        t006c195_t = OBSColumn(
            id='t006c195_t',
            name='West Central Asian and Middle Eastern origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c194_t: DENOMINATOR },)

        t006c195_m = OBSColumn(
            id='t006c195_m',
            name='West Central Asian and Middle Eastern origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c194_m: DENOMINATOR },)

        t006c195_f = OBSColumn(
            id='t006c195_f',
            name='West Central Asian and Middle Eastern origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c194_f: DENOMINATOR },)

        t006c196_t = OBSColumn(
            id='t006c196_t',
            name='Afghan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c196_m = OBSColumn(
            id='t006c196_m',
            name='Afghan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c196_f = OBSColumn(
            id='t006c196_f',
            name='Afghan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c197_t = OBSColumn(
            id='t006c197_t',
            name='Arab, n.o.s. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c197_m = OBSColumn(
            id='t006c197_m',
            name='Arab, n.o.s. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c197_f = OBSColumn(
            id='t006c197_f',
            name='Arab, n.o.s. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c198_t = OBSColumn(
            id='t006c198_t',
            name='Armenian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c198_m = OBSColumn(
            id='t006c198_m',
            name='Armenian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c198_f = OBSColumn(
            id='t006c198_f',
            name='Armenian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c199_t = OBSColumn(
            id='t006c199_t',
            name='Assyrian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c199_m = OBSColumn(
            id='t006c199_m',
            name='Assyrian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c199_f = OBSColumn(
            id='t006c199_f',
            name='Assyrian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c200_t = OBSColumn(
            id='t006c200_t',
            name='Azerbaijani (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c200_m = OBSColumn(
            id='t006c200_m',
            name='Azerbaijani (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c200_f = OBSColumn(
            id='t006c200_f',
            name='Azerbaijani (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c201_t = OBSColumn(
            id='t006c201_t',
            name='Georgian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c201_m = OBSColumn(
            id='t006c201_m',
            name='Georgian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c201_f = OBSColumn(
            id='t006c201_f',
            name='Georgian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c202_t = OBSColumn(
            id='t006c202_t',
            name='Iranian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c202_m = OBSColumn(
            id='t006c202_m',
            name='Iranian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c202_f = OBSColumn(
            id='t006c202_f',
            name='Iranian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c203_t = OBSColumn(
            id='t006c203_t',
            name='Iraqi (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c203_m = OBSColumn(
            id='t006c203_m',
            name='Iraqi (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c203_f = OBSColumn(
            id='t006c203_f',
            name='Iraqi (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c204_t = OBSColumn(
            id='t006c204_t',
            name='Israeli (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c204_m = OBSColumn(
            id='t006c204_m',
            name='Israeli (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c204_f = OBSColumn(
            id='t006c204_f',
            name='Israeli (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c205_t = OBSColumn(
            id='t006c205_t',
            name='Jordanian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c205_m = OBSColumn(
            id='t006c205_m',
            name='Jordanian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c205_f = OBSColumn(
            id='t006c205_f',
            name='Jordanian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c206_t = OBSColumn(
            id='t006c206_t',
            name='Kazakh (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c206_m = OBSColumn(
            id='t006c206_m',
            name='Kazakh (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c206_f = OBSColumn(
            id='t006c206_f',
            name='Kazakh (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c207_t = OBSColumn(
            id='t006c207_t',
            name='Kurd (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c207_m = OBSColumn(
            id='t006c207_m',
            name='Kurd (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c207_f = OBSColumn(
            id='t006c207_f',
            name='Kurd (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c208_t = OBSColumn(
            id='t006c208_t',
            name='Kuwaiti (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c208_m = OBSColumn(
            id='t006c208_m',
            name='Kuwaiti (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c208_f = OBSColumn(
            id='t006c208_f',
            name='Kuwaiti (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c209_t = OBSColumn(
            id='t006c209_t',
            name='Lebanese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c209_m = OBSColumn(
            id='t006c209_m',
            name='Lebanese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c209_f = OBSColumn(
            id='t006c209_f',
            name='Lebanese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c210_t = OBSColumn(
            id='t006c210_t',
            name='Palestinian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c210_m = OBSColumn(
            id='t006c210_m',
            name='Palestinian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c210_f = OBSColumn(
            id='t006c210_f',
            name='Palestinian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c211_t = OBSColumn(
            id='t006c211_t',
            name='Pashtun (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c211_m = OBSColumn(
            id='t006c211_m',
            name='Pashtun (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c211_f = OBSColumn(
            id='t006c211_f',
            name='Pashtun (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c212_t = OBSColumn(
            id='t006c212_t',
            name='Saudi Arabian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c212_m = OBSColumn(
            id='t006c212_m',
            name='Saudi Arabian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c212_f = OBSColumn(
            id='t006c212_f',
            name='Saudi Arabian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c213_t = OBSColumn(
            id='t006c213_t',
            name='Syrian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c213_m = OBSColumn(
            id='t006c213_m',
            name='Syrian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c213_f = OBSColumn(
            id='t006c213_f',
            name='Syrian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c214_t = OBSColumn(
            id='t006c214_t',
            name='Tajik (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c214_m = OBSColumn(
            id='t006c214_m',
            name='Tajik (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c214_f = OBSColumn(
            id='t006c214_f',
            name='Tajik (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c215_t = OBSColumn(
            id='t006c215_t',
            name='Tatar (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c215_m = OBSColumn(
            id='t006c215_m',
            name='Tatar (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c215_f = OBSColumn(
            id='t006c215_f',
            name='Tatar (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c216_t = OBSColumn(
            id='t006c216_t',
            name='Turk (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c216_m = OBSColumn(
            id='t006c216_m',
            name='Turk (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c216_f = OBSColumn(
            id='t006c216_f',
            name='Turk (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c217_t = OBSColumn(
            id='t006c217_t',
            name='Uighur (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c217_m = OBSColumn(
            id='t006c217_m',
            name='Uighur (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c217_f = OBSColumn(
            id='t006c217_f',
            name='Uighur (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c218_t = OBSColumn(
            id='t006c218_t',
            name='Uzbek (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c218_m = OBSColumn(
            id='t006c218_m',
            name='Uzbek (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c218_f = OBSColumn(
            id='t006c218_f',
            name='Uzbek (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c219_t = OBSColumn(
            id='t006c219_t',
            name='Yemeni (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c219_m = OBSColumn(
            id='t006c219_m',
            name='Yemeni (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c219_f = OBSColumn(
            id='t006c219_f',
            name='Yemeni (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c220_t = OBSColumn(
            id='t006c220_t',
            name='West Central Asian and Middle Eastern origins, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_t: DENOMINATOR },)

        t006c220_m = OBSColumn(
            id='t006c220_m',
            name='West Central Asian and Middle Eastern origins, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_m: DENOMINATOR },)

        t006c220_f = OBSColumn(
            id='t006c220_f',
            name='West Central Asian and Middle Eastern origins, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c195_f: DENOMINATOR },)

        t006c221_t = OBSColumn(
            id='t006c221_t',
            name='South Asian origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c194_t: DENOMINATOR },)

        t006c221_m = OBSColumn(
            id='t006c221_m',
            name='South Asian origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c194_m: DENOMINATOR },)

        t006c221_f = OBSColumn(
            id='t006c221_f',
            name='South Asian origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c194_f: DENOMINATOR },)

        t006c222_t = OBSColumn(
            id='t006c222_t',
            name='Bangladeshi (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_t: DENOMINATOR },)

        t006c222_m = OBSColumn(
            id='t006c222_m',
            name='Bangladeshi (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_m: DENOMINATOR },)

        t006c222_f = OBSColumn(
            id='t006c222_f',
            name='Bangladeshi (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_f: DENOMINATOR },)

        t006c223_t = OBSColumn(
            id='t006c223_t',
            name='Bengali (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_t: DENOMINATOR },)

        t006c223_m = OBSColumn(
            id='t006c223_m',
            name='Bengali (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_m: DENOMINATOR },)

        t006c223_f = OBSColumn(
            id='t006c223_f',
            name='Bengali (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_f: DENOMINATOR },)

        t006c224_t = OBSColumn(
            id='t006c224_t',
            name='East Indian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_t: DENOMINATOR },)

        t006c224_m = OBSColumn(
            id='t006c224_m',
            name='East Indian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_m: DENOMINATOR },)

        t006c224_f = OBSColumn(
            id='t006c224_f',
            name='East Indian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_f: DENOMINATOR },)

        t006c225_t = OBSColumn(
            id='t006c225_t',
            name='Goan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_t: DENOMINATOR },)

        t006c225_m = OBSColumn(
            id='t006c225_m',
            name='Goan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_m: DENOMINATOR },)

        t006c225_f = OBSColumn(
            id='t006c225_f',
            name='Goan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_f: DENOMINATOR },)

        t006c226_t = OBSColumn(
            id='t006c226_t',
            name='Gujarati (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_t: DENOMINATOR },)

        t006c226_m = OBSColumn(
            id='t006c226_m',
            name='Gujarati (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_m: DENOMINATOR },)

        t006c226_f = OBSColumn(
            id='t006c226_f',
            name='Gujarati (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_f: DENOMINATOR },)

        t006c227_t = OBSColumn(
            id='t006c227_t',
            name='Kashmiri (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_t: DENOMINATOR },)

        t006c227_m = OBSColumn(
            id='t006c227_m',
            name='Kashmiri (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_m: DENOMINATOR },)

        t006c227_f = OBSColumn(
            id='t006c227_f',
            name='Kashmiri (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_f: DENOMINATOR },)

        t006c228_t = OBSColumn(
            id='t006c228_t',
            name='Nepali (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_t: DENOMINATOR },)

        t006c228_m = OBSColumn(
            id='t006c228_m',
            name='Nepali (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_m: DENOMINATOR },)

        t006c228_f = OBSColumn(
            id='t006c228_f',
            name='Nepali (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_f: DENOMINATOR },)

        t006c229_t = OBSColumn(
            id='t006c229_t',
            name='Pakistani (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_t: DENOMINATOR },)

        t006c229_m = OBSColumn(
            id='t006c229_m',
            name='Pakistani (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_m: DENOMINATOR },)

        t006c229_f = OBSColumn(
            id='t006c229_f',
            name='Pakistani (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_f: DENOMINATOR },)

        t006c230_t = OBSColumn(
            id='t006c230_t',
            name='Punjabi (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_t: DENOMINATOR },)

        t006c230_m = OBSColumn(
            id='t006c230_m',
            name='Punjabi (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_m: DENOMINATOR },)

        t006c230_f = OBSColumn(
            id='t006c230_f',
            name='Punjabi (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_f: DENOMINATOR },)

        t006c231_t = OBSColumn(
            id='t006c231_t',
            name='Sinhalese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_t: DENOMINATOR },)

        t006c231_m = OBSColumn(
            id='t006c231_m',
            name='Sinhalese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_m: DENOMINATOR },)

        t006c231_f = OBSColumn(
            id='t006c231_f',
            name='Sinhalese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_f: DENOMINATOR },)

        t006c232_t = OBSColumn(
            id='t006c232_t',
            name='Sri Lankan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_t: DENOMINATOR },)

        t006c232_m = OBSColumn(
            id='t006c232_m',
            name='Sri Lankan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_m: DENOMINATOR },)

        t006c232_f = OBSColumn(
            id='t006c232_f',
            name='Sri Lankan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_f: DENOMINATOR },)

        t006c233_t = OBSColumn(
            id='t006c233_t',
            name='Tamil (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_t: DENOMINATOR },)

        t006c233_m = OBSColumn(
            id='t006c233_m',
            name='Tamil (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_m: DENOMINATOR },)

        t006c233_f = OBSColumn(
            id='t006c233_f',
            name='Tamil (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_f: DENOMINATOR },)

        t006c234_t = OBSColumn(
            id='t006c234_t',
            name='South Asian origins, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_t: DENOMINATOR },)

        t006c234_m = OBSColumn(
            id='t006c234_m',
            name='South Asian origins, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_m: DENOMINATOR },)

        t006c234_f = OBSColumn(
            id='t006c234_f',
            name='South Asian origins, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c221_f: DENOMINATOR },)

        t006c235_t = OBSColumn(
            id='t006c235_t',
            name='East and Southeast Asian origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c194_t: DENOMINATOR },)

        t006c235_m = OBSColumn(
            id='t006c235_m',
            name='East and Southeast Asian origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c194_m: DENOMINATOR },)

        t006c235_f = OBSColumn(
            id='t006c235_f',
            name='East and Southeast Asian origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c194_f: DENOMINATOR },)

        t006c236_t = OBSColumn(
            id='t006c236_t',
            name='Burmese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_t: DENOMINATOR },)

        t006c236_m = OBSColumn(
            id='t006c236_m',
            name='Burmese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_m: DENOMINATOR },)

        t006c236_f = OBSColumn(
            id='t006c236_f',
            name='Burmese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_f: DENOMINATOR },)

        t006c237_t = OBSColumn(
            id='t006c237_t',
            name='Cambodian (Khmer) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_t: DENOMINATOR },)

        t006c237_m = OBSColumn(
            id='t006c237_m',
            name='Cambodian (Khmer) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_m: DENOMINATOR },)

        t006c237_f = OBSColumn(
            id='t006c237_f',
            name='Cambodian (Khmer) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_f: DENOMINATOR },)

        t006c238_t = OBSColumn(
            id='t006c238_t',
            name='Chinese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_t: DENOMINATOR },)

        t006c238_m = OBSColumn(
            id='t006c238_m',
            name='Chinese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_m: DENOMINATOR },)

        t006c238_f = OBSColumn(
            id='t006c238_f',
            name='Chinese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_f: DENOMINATOR },)

        t006c239_t = OBSColumn(
            id='t006c239_t',
            name='Filipino (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_t: DENOMINATOR },)

        t006c239_m = OBSColumn(
            id='t006c239_m',
            name='Filipino (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_m: DENOMINATOR },)

        t006c239_f = OBSColumn(
            id='t006c239_f',
            name='Filipino (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_f: DENOMINATOR },)

        t006c240_t = OBSColumn(
            id='t006c240_t',
            name='Hmong (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_t: DENOMINATOR },)

        t006c240_m = OBSColumn(
            id='t006c240_m',
            name='Hmong (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_m: DENOMINATOR },)

        t006c240_f = OBSColumn(
            id='t006c240_f',
            name='Hmong (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_f: DENOMINATOR },)

        t006c241_t = OBSColumn(
            id='t006c241_t',
            name='Indonesian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_t: DENOMINATOR },)

        t006c241_m = OBSColumn(
            id='t006c241_m',
            name='Indonesian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_m: DENOMINATOR },)

        t006c241_f = OBSColumn(
            id='t006c241_f',
            name='Indonesian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_f: DENOMINATOR },)

        t006c242_t = OBSColumn(
            id='t006c242_t',
            name='Japanese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_t: DENOMINATOR },)

        t006c242_m = OBSColumn(
            id='t006c242_m',
            name='Japanese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_m: DENOMINATOR },)

        t006c242_f = OBSColumn(
            id='t006c242_f',
            name='Japanese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_f: DENOMINATOR },)

        t006c243_t = OBSColumn(
            id='t006c243_t',
            name='Korean (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_t: DENOMINATOR },)

        t006c243_m = OBSColumn(
            id='t006c243_m',
            name='Korean (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_m: DENOMINATOR },)

        t006c243_f = OBSColumn(
            id='t006c243_f',
            name='Korean (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_f: DENOMINATOR },)

        t006c244_t = OBSColumn(
            id='t006c244_t',
            name='Laotian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_t: DENOMINATOR },)

        t006c244_m = OBSColumn(
            id='t006c244_m',
            name='Laotian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_m: DENOMINATOR },)

        t006c244_f = OBSColumn(
            id='t006c244_f',
            name='Laotian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_f: DENOMINATOR },)

        t006c245_t = OBSColumn(
            id='t006c245_t',
            name='Malaysian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_t: DENOMINATOR },)

        t006c245_m = OBSColumn(
            id='t006c245_m',
            name='Malaysian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_m: DENOMINATOR },)

        t006c245_f = OBSColumn(
            id='t006c245_f',
            name='Malaysian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_f: DENOMINATOR },)

        t006c246_t = OBSColumn(
            id='t006c246_t',
            name='Mongolian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_t: DENOMINATOR },)

        t006c246_m = OBSColumn(
            id='t006c246_m',
            name='Mongolian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_m: DENOMINATOR },)

        t006c246_f = OBSColumn(
            id='t006c246_f',
            name='Mongolian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_f: DENOMINATOR },)

        t006c247_t = OBSColumn(
            id='t006c247_t',
            name='Singaporean (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_t: DENOMINATOR },)

        t006c247_m = OBSColumn(
            id='t006c247_m',
            name='Singaporean (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_m: DENOMINATOR },)

        t006c247_f = OBSColumn(
            id='t006c247_f',
            name='Singaporean (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_f: DENOMINATOR },)

        t006c248_t = OBSColumn(
            id='t006c248_t',
            name='Taiwanese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_t: DENOMINATOR },)

        t006c248_m = OBSColumn(
            id='t006c248_m',
            name='Taiwanese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_m: DENOMINATOR },)

        t006c248_f = OBSColumn(
            id='t006c248_f',
            name='Taiwanese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_f: DENOMINATOR },)

        t006c249_t = OBSColumn(
            id='t006c249_t',
            name='Thai (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_t: DENOMINATOR },)

        t006c249_m = OBSColumn(
            id='t006c249_m',
            name='Thai (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_m: DENOMINATOR },)

        t006c249_f = OBSColumn(
            id='t006c249_f',
            name='Thai (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_f: DENOMINATOR },)

        t006c250_t = OBSColumn(
            id='t006c250_t',
            name='Tibetan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_t: DENOMINATOR },)

        t006c250_m = OBSColumn(
            id='t006c250_m',
            name='Tibetan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_m: DENOMINATOR },)

        t006c250_f = OBSColumn(
            id='t006c250_f',
            name='Tibetan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_f: DENOMINATOR },)

        t006c251_t = OBSColumn(
            id='t006c251_t',
            name='Vietnamese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_t: DENOMINATOR },)

        t006c251_m = OBSColumn(
            id='t006c251_m',
            name='Vietnamese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_m: DENOMINATOR },)

        t006c251_f = OBSColumn(
            id='t006c251_f',
            name='Vietnamese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_f: DENOMINATOR },)

        t006c252_t = OBSColumn(
            id='t006c252_t',
            name='East and Southeast Asian origins, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_t: DENOMINATOR },)

        t006c252_m = OBSColumn(
            id='t006c252_m',
            name='East and Southeast Asian origins, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_m: DENOMINATOR },)

        t006c252_f = OBSColumn(
            id='t006c252_f',
            name='East and Southeast Asian origins, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c235_f: DENOMINATOR },)

        t006c253_t = OBSColumn(
            id='t006c253_t',
            name='Other Asian origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c194_t: DENOMINATOR },)

        t006c253_m = OBSColumn(
            id='t006c253_m',
            name='Other Asian origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c194_m: DENOMINATOR },)

        t006c253_f = OBSColumn(
            id='t006c253_f',
            name='Other Asian origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c194_f: DENOMINATOR },)

        t006c254_t = OBSColumn(
            id='t006c254_t',
            name='Other Asian origins, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c253_t: DENOMINATOR },)

        t006c254_m = OBSColumn(
            id='t006c254_m',
            name='Other Asian origins, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c253_m: DENOMINATOR },)

        t006c254_f = OBSColumn(
            id='t006c254_f',
            name='Other Asian origins, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c253_f: DENOMINATOR },)

        t006c255_t = OBSColumn(
            id='t006c255_t',
            name='Total population - Oceania origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_t: DENOMINATOR },)

        t006c255_m = OBSColumn(
            id='t006c255_m',
            name='Oceania origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_m: DENOMINATOR },)

        t006c255_f = OBSColumn(
            id='t006c255_f',
            name='Oceania origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_f: DENOMINATOR },)

        t006c256_t = OBSColumn(
            id='t006c256_t',
            name='Australian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c255_t: DENOMINATOR },)

        t006c256_m = OBSColumn(
            id='t006c256_m',
            name='Australian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c255_m: DENOMINATOR },)

        t006c256_f = OBSColumn(
            id='t006c256_f',
            name='Australian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c255_f: DENOMINATOR },)

        t006c257_t = OBSColumn(
            id='t006c257_t',
            name='New Zealander (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c255_t: DENOMINATOR },)

        t006c257_m = OBSColumn(
            id='t006c257_m',
            name='New Zealander (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c255_m: DENOMINATOR },)

        t006c257_f = OBSColumn(
            id='t006c257_f',
            name='New Zealander (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c255_f: DENOMINATOR },)

        t006c258_t = OBSColumn(
            id='t006c258_t',
            name='Pacific Islands origins (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c255_t: DENOMINATOR },)

        t006c258_m = OBSColumn(
            id='t006c258_m',
            name='Pacific Islands origins (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c255_m: DENOMINATOR },)

        t006c258_f = OBSColumn(
            id='t006c258_f',
            name='Pacific Islands origins (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c255_f: DENOMINATOR },)

        t006c259_t = OBSColumn(
            id='t006c259_t',
            name='Fijian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c258_t: DENOMINATOR },)

        t006c259_m = OBSColumn(
            id='t006c259_m',
            name='Fijian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c258_m: DENOMINATOR },)

        t006c259_f = OBSColumn(
            id='t006c259_f',
            name='Fijian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c258_f: DENOMINATOR },)

        t006c260_t = OBSColumn(
            id='t006c260_t',
            name='Hawaiian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c258_t: DENOMINATOR },)

        t006c260_m = OBSColumn(
            id='t006c260_m',
            name='Hawaiian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c258_m: DENOMINATOR },)

        t006c260_f = OBSColumn(
            id='t006c260_f',
            name='Hawaiian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c258_f: DENOMINATOR },)

        t006c261_t = OBSColumn(
            id='t006c261_t',
            name='Maori (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c258_t: DENOMINATOR },)

        t006c261_m = OBSColumn(
            id='t006c261_m',
            name='Maori (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c258_m: DENOMINATOR },)

        t006c261_f = OBSColumn(
            id='t006c261_f',
            name='Maori (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c258_f: DENOMINATOR },)

        t006c262_t = OBSColumn(
            id='t006c262_t',
            name='Polynesian, n.o.s. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c258_t: DENOMINATOR },)

        t006c262_m = OBSColumn(
            id='t006c262_m',
            name='Polynesian, n.o.s. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c258_m: DENOMINATOR },)

        t006c262_f = OBSColumn(
            id='t006c262_f',
            name='Polynesian, n.o.s. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c258_f: DENOMINATOR },)

        t006c263_t = OBSColumn(
            id='t006c263_t',
            name='Samoan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c258_t: DENOMINATOR },)

        t006c263_m = OBSColumn(
            id='t006c263_m',
            name='Samoan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c258_m: DENOMINATOR },)

        t006c263_f = OBSColumn(
            id='t006c263_f',
            name='Samoan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c258_f: DENOMINATOR },)

        t006c264_t = OBSColumn(
            id='t006c264_t',
            name='Pacific Islands origins, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c258_t: DENOMINATOR },)

        t006c264_m = OBSColumn(
            id='t006c264_m',
            name='Pacific Islands origins, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c258_m: DENOMINATOR },)

        t006c264_f = OBSColumn(
            id='t006c264_f',
            name='Pacific Islands origins, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t006c258_f: DENOMINATOR },)

        t007c002_t = OBSColumn(
            id='t007c002_t',
            name='People aged 15+ in the labour force who did not work (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_t: DENOMINATOR },)

        t007c002_m = OBSColumn(
            id='t007c002_m',
            name='People aged 15+ in the labour force who did not work (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_m: DENOMINATOR },)

        t007c002_f = OBSColumn(
            id='t007c002_f',
            name='People aged 15+ in the labour force who did not work (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_f: DENOMINATOR },)

        t007c003_t = OBSColumn(
            id='t007c003_t',
            name='People aged 15+ in the labour force who worked (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_t: DENOMINATOR },)

        t007c003_m = OBSColumn(
            id='t007c003_m',
            name='People aged 15+ in the labour force who worked (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_m: DENOMINATOR },)

        t007c003_f = OBSColumn(
            id='t007c003_f',
            name='People aged 15+ in the labour force who worked (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_f: DENOMINATOR },)

        t007c004_t = OBSColumn(
            id='t007c004_t',
            name='People aged 15+ in the labour force who worked full-time (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t007c003_t: DENOMINATOR },)

        t007c004_m = OBSColumn(
            id='t007c004_m',
            name='People aged 15+ in the labour force who worked full-time (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t007c003_m: DENOMINATOR },)

        t007c004_f = OBSColumn(
            id='t007c004_f',
            name='People aged 15+ in the labour force who worked full-time (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t007c003_f: DENOMINATOR },)

        t007c005_t = OBSColumn(
            id='t007c005_t',
            name='People aged 15+ in the labour force who worked part-time (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t007c003_t: DENOMINATOR },)

        t007c005_m = OBSColumn(
            id='t007c005_m',
            name='People aged 15+ in the labour force who worked part-time (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t007c003_m: DENOMINATOR },)

        t007c005_f = OBSColumn(
            id='t007c005_f',
            name='People aged 15+ in the labour force who worked part-time (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t007c003_f: DENOMINATOR },)

        # FIXME
        # The "first generation", "second generation", etc. measures need
        # a clearer name, and possibly a description as well.  Is it first
        # generation immigrants?  If so, should they have the "immigration"
        # tag?
        t008c002_t = OBSColumn(
            id='t008c002_t',
            name='Total population - First generation (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t001c001_t: DENOMINATOR },)

        t008c002_m = OBSColumn(
            id='t008c002_m',
            name='Total population - First generation (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t001c001_m: DENOMINATOR },)

        t008c002_f = OBSColumn(
            id='t008c002_f',
            name='Total population - First generation (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t001c001_f: DENOMINATOR },)

        t008c003_t = OBSColumn(
            id='t008c003_t',
            name='Total population - Second generation (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t001c001_t: DENOMINATOR },)

        t008c003_m = OBSColumn(
            id='t008c003_m',
            name='Total population - Second generation (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t001c001_m: DENOMINATOR },)

        t008c003_f = OBSColumn(
            id='t008c003_f',
            name='Total population - Second generation (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t001c001_f: DENOMINATOR },)

        t008c004_t = OBSColumn(
            id='t008c004_t',
            name='Total population - Third generation or more (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t001c001_t: DENOMINATOR },)

        t008c004_m = OBSColumn(
            id='t008c004_m',
            name='Total population - Third generation or more (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t001c001_m: DENOMINATOR },)

        t008c004_f = OBSColumn(
            id='t008c004_f',
            name='Total population - Third generation or more (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['families']],
            targets={ t001c001_f: DENOMINATOR },)

        # FIXME
        # This should have a description of households according to the NHS.
        t009c001_t = OBSColumn(
            id='t009c001_t',
            name='Total number of private households',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_household, subsections['housing']],
            targets={},)

        t009c002_t = OBSColumn(
            id='t009c002_t',
            name='Total number of private households - Owner',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_household, subsections['housing']],
            targets={ t009c001_t: DENOMINATOR },)

        t009c003_t = OBSColumn(
            id='t009c003_t',
            name='Total number of private households - Renter',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_household, subsections['housing']],
            targets={ t009c001_t: DENOMINATOR },)

        # FIXME
        # This should have a definition of Band Housing in the description.
        t009c004_t = OBSColumn(
            id='t009c004_t',
            name='Total number of private households - Band housing',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_household, subsections['housing']],
            targets={ t009c001_t: DENOMINATOR },)

        t009c006_t = OBSColumn(
            id='t009c006_t',
            name='Total number of private households - Part of a condominium development',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_household, subsections['housing']],
            targets={ t009c001_t: DENOMINATOR },)

        t009c007_t = OBSColumn(
            id='t009c007_t',
            name='Total number of private households - Not part of a condominium development',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_household, subsections['housing']],
            targets={ t009c001_t: DENOMINATOR },)

        t009c009_t = OBSColumn(
            id='t009c009_t',
            name='Total number of private households - 1 household maintainer',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_household, subsections['housing']],
            targets={ t009c001_t: DENOMINATOR },)

        t009c010_t = OBSColumn(
            id='t009c010_t',
            name='Total number of private households - 2 household maintainers',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_household, subsections['housing']],
            targets={ t009c001_t: DENOMINATOR },)

        t009c011_t = OBSColumn(
            id='t009c011_t',
            name='Total number of private households - 3 or more household maintainers',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_household, subsections['housing']],
            targets={ t009c001_t: DENOMINATOR },)

        # FIXME
        # The names for these should be a little clearer -- is it that the
        # head of household is in the age band?
        t009c013_t = OBSColumn(
            id='t009c013_t',
            name='Total number of private households - Under 25 years',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_household, subsections['housing']],
            targets={ t009c001_t: DENOMINATOR },)

        t009c014_t = OBSColumn(
            id='t009c014_t',
            name='Total number of private households - 25 to 34 years',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_household, subsections['housing']],
            targets={ t009c001_t: DENOMINATOR },)

        t009c015_t = OBSColumn(
            id='t009c015_t',
            name='Total number of private households - 35 to 44 years',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_household, subsections['housing']],
            targets={ t009c001_t: DENOMINATOR },)

        t009c016_t = OBSColumn(
            id='t009c016_t',
            name='Total number of private households - 45 to 54 years',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_household, subsections['housing']],
            targets={ t009c001_t: DENOMINATOR },)

        t009c017_t = OBSColumn(
            id='t009c017_t',
            name='Total number of private households - 55 to 64 years',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_household, subsections['housing']],
            targets={ t009c001_t: DENOMINATOR },)

        t009c018_t = OBSColumn(
            id='t009c018_t',
            name='Total number of private households - 65 to 74 years',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_household, subsections['housing']],
            targets={ t009c001_t: DENOMINATOR },)

        t009c019_t = OBSColumn(
            id='t009c019_t',
            name='Total number of private households - 75 years and over',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_household, subsections['housing']],
            targets={ t009c001_t: DENOMINATOR },)

        t009c021_t = OBSColumn(
            id='t009c021_t',
            name='Total number of private households - One person or fewer per room',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_household, subsections['housing']],
            targets={ t009c001_t: DENOMINATOR },)

        t009c022_t = OBSColumn(
            id='t009c022_t',
            name='Total number of private households - More than one person per room',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_household, subsections['housing']],
            targets={ t009c001_t: DENOMINATOR },)

        # FIXME
        # What does suitable/not suitable mean?  Needs a clearer name and
        # possibly a description.
        t009c024_t = OBSColumn(
            id='t009c024_t',
            name='Total number of private households - Suitable',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_household, subsections['housing']],
            targets={ t009c001_t: DENOMINATOR },)

        t009c025_t = OBSColumn(
            id='t009c025_t',
            name='Total number of private households - Not suitable',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_household, subsections['housing']],
            targets={ t009c001_t: DENOMINATOR },)

        t010c002_t = OBSColumn(
            id='t010c002_t',
            name='Total population - Non-immigrants (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t001c001_t: DENOMINATOR },)

        t010c002_m = OBSColumn(
            id='t010c002_m',
            name='Total population - Non-immigrants (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t001c001_m: DENOMINATOR },)

        t010c002_f = OBSColumn(
            id='t010c002_f',
            name='Total population - Non-immigrants (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t001c001_f: DENOMINATOR },)

        t010c003_t = OBSColumn(
            id='t010c003_t',
            name='Total population - Immigrants (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t001c001_t: DENOMINATOR },)

        t010c003_m = OBSColumn(
            id='t010c003_m',
            name='Total population - Immigrants (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t001c001_m: DENOMINATOR },)

        t010c003_f = OBSColumn(
            id='t010c003_f',
            name='Total population - Immigrants (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t001c001_f: DENOMINATOR },)

        # FIXME
        # Names for immigration year need qualification, like
        # "Total population - immigrated before 1971 (total)", etc.
        t010c004_t = OBSColumn(
            id='t010c004_t',
            name='Before 1971 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c003_t: DENOMINATOR },)

        t010c004_m = OBSColumn(
            id='t010c004_m',
            name='Before 1971 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c003_m: DENOMINATOR },)

        t010c004_f = OBSColumn(
            id='t010c004_f',
            name='Before 1971 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c003_f: DENOMINATOR },)

        t010c005_t = OBSColumn(
            id='t010c005_t',
            name='1971 to 1980 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c003_t: DENOMINATOR },)

        t010c005_m = OBSColumn(
            id='t010c005_m',
            name='1971 to 1980 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c003_m: DENOMINATOR },)

        t010c005_f = OBSColumn(
            id='t010c005_f',
            name='1971 to 1980 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c003_f: DENOMINATOR },)

        t010c006_t = OBSColumn(
            id='t010c006_t',
            name='1981 to 1990 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c003_t: DENOMINATOR },)

        t010c006_m = OBSColumn(
            id='t010c006_m',
            name='1981 to 1990 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c003_m: DENOMINATOR },)

        t010c006_f = OBSColumn(
            id='t010c006_f',
            name='1981 to 1990 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c003_f: DENOMINATOR },)

        t010c007_t = OBSColumn(
            id='t010c007_t',
            name='1991 to 2000 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c003_t: DENOMINATOR },)

        t010c007_m = OBSColumn(
            id='t010c007_m',
            name='1991 to 2000 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c003_m: DENOMINATOR },)

        t010c007_f = OBSColumn(
            id='t010c007_f',
            name='1991 to 2000 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c003_f: DENOMINATOR },)

        t010c008_t = OBSColumn(
            id='t010c008_t',
            name='2001 to 2011 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c003_t: DENOMINATOR },)

        t010c008_m = OBSColumn(
            id='t010c008_m',
            name='2001 to 2011 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c003_m: DENOMINATOR },)

        t010c008_f = OBSColumn(
            id='t010c008_f',
            name='2001 to 2011 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c003_f: DENOMINATOR },)

        t010c009_t = OBSColumn(
            id='t010c009_t',
            name='2001 to 2005 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c008_t: DENOMINATOR },)

        t010c009_m = OBSColumn(
            id='t010c009_m',
            name='2001 to 2005 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c008_m: DENOMINATOR },)

        t010c009_f = OBSColumn(
            id='t010c009_f',
            name='2001 to 2005 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c008_f: DENOMINATOR },)

        t010c010_t = OBSColumn(
            id='t010c010_t',
            name='2006 to 2011 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c008_t: DENOMINATOR },)

        t010c010_m = OBSColumn(
            id='t010c010_m',
            name='2006 to 2011 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c008_m: DENOMINATOR },)

        t010c010_f = OBSColumn(
            id='t010c010_f',
            name='2006 to 2011 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t010c008_f: DENOMINATOR },)

        t010c011_t = OBSColumn(
            id='t010c011_t',
            name='Total population - Non-permanent residents (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t001c001_t: DENOMINATOR },)

        t010c011_m = OBSColumn(
            id='t010c011_m',
            name='Total population - Non-permanent residents (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t001c001_m: DENOMINATOR },)

        t010c011_f = OBSColumn(
            id='t010c011_f',
            name='Total population - Non-permanent residents (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t001c001_f: DENOMINATOR },)

        t011c002_t = OBSColumn(
            id='t011c002_t',
            name='Total population - Non-immigrants (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t001c001_t: DENOMINATOR },)

        t011c002_m = OBSColumn(
            id='t011c002_m',
            name='Total population - Non-immigrants (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t001c001_m: DENOMINATOR },)

        t011c002_f = OBSColumn(
            id='t011c002_f',
            name='Total population - Non-immigrants (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t001c001_f: DENOMINATOR },)

        t011c003_t = OBSColumn(
            id='t011c003_t',
            name='Born in province of residence (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c002_t: DENOMINATOR },)

        t011c003_m = OBSColumn(
            id='t011c003_m',
            name='Born in province of residence (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c002_m: DENOMINATOR },)

        t011c003_f = OBSColumn(
            id='t011c003_f',
            name='Born in province of residence (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c002_f: DENOMINATOR },)

        t011c004_t = OBSColumn(
            id='t011c004_t',
            name='Born outside province of residence (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c002_t: DENOMINATOR },)

        t011c004_m = OBSColumn(
            id='t011c004_m',
            name='Born outside province of residence (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c002_m: DENOMINATOR },)

        t011c004_f = OBSColumn(
            id='t011c004_f',
            name='Born outside province of residence (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c002_f: DENOMINATOR },)

        t011c005_t = OBSColumn(
            id='t011c005_t',
            name='Total population - Immigrants (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t001c001_t: DENOMINATOR },)

        t011c005_m = OBSColumn(
            id='t011c005_m',
            name='Total population - Immigrants (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t001c001_m: DENOMINATOR },)

        t011c005_f = OBSColumn(
            id='t011c005_f',
            name='Total population - Immigrants (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t001c001_f: DENOMINATOR },)

        # FIXME
        # The below need to be qualified that the area/country specified is
        # where they immigrated from.
        t011c006_t = OBSColumn(
            id='t011c006_t',
            name='Americas (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c005_t: DENOMINATOR },)

        t011c006_m = OBSColumn(
            id='t011c006_m',
            name='Americas (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c005_m: DENOMINATOR },)

        t011c006_f = OBSColumn(
            id='t011c006_f',
            name='Americas (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c005_f: DENOMINATOR },)

        t011c007_t = OBSColumn(
            id='t011c007_t',
            name='United States (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_t: DENOMINATOR },)

        t011c007_m = OBSColumn(
            id='t011c007_m',
            name='United States (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_m: DENOMINATOR },)

        t011c007_f = OBSColumn(
            id='t011c007_f',
            name='United States (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_f: DENOMINATOR },)

        t011c008_t = OBSColumn(
            id='t011c008_t',
            name='Jamaica (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_t: DENOMINATOR },)

        t011c008_m = OBSColumn(
            id='t011c008_m',
            name='Jamaica (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_m: DENOMINATOR },)

        t011c008_f = OBSColumn(
            id='t011c008_f',
            name='Jamaica (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_f: DENOMINATOR },)

        t011c009_t = OBSColumn(
            id='t011c009_t',
            name='Guyana (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_t: DENOMINATOR },)

        t011c009_m = OBSColumn(
            id='t011c009_m',
            name='Guyana (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_m: DENOMINATOR },)

        t011c009_f = OBSColumn(
            id='t011c009_f',
            name='Guyana (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_f: DENOMINATOR },)

        t011c010_t = OBSColumn(
            id='t011c010_t',
            name='Haiti (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_t: DENOMINATOR },)

        t011c010_m = OBSColumn(
            id='t011c010_m',
            name='Haiti (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_m: DENOMINATOR },)

        t011c010_f = OBSColumn(
            id='t011c010_f',
            name='Haiti (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_f: DENOMINATOR },)

        t011c011_t = OBSColumn(
            id='t011c011_t',
            name='Mexico (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_t: DENOMINATOR },)

        t011c011_m = OBSColumn(
            id='t011c011_m',
            name='Mexico (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_m: DENOMINATOR },)

        t011c011_f = OBSColumn(
            id='t011c011_f',
            name='Mexico (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_f: DENOMINATOR },)

        t011c012_t = OBSColumn(
            id='t011c012_t',
            name='Trinidad and Tobago (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_t: DENOMINATOR },)

        t011c012_m = OBSColumn(
            id='t011c012_m',
            name='Trinidad and Tobago (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_m: DENOMINATOR },)

        t011c012_f = OBSColumn(
            id='t011c012_f',
            name='Trinidad and Tobago (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_f: DENOMINATOR },)

        t011c013_t = OBSColumn(
            id='t011c013_t',
            name='Colombia (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_t: DENOMINATOR },)

        t011c013_m = OBSColumn(
            id='t011c013_m',
            name='Colombia (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_m: DENOMINATOR },)

        t011c013_f = OBSColumn(
            id='t011c013_f',
            name='Colombia (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_f: DENOMINATOR },)

        t011c014_t = OBSColumn(
            id='t011c014_t',
            name='El Salvador (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_t: DENOMINATOR },)

        t011c014_m = OBSColumn(
            id='t011c014_m',
            name='El Salvador (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_m: DENOMINATOR },)

        t011c014_f = OBSColumn(
            id='t011c014_f',
            name='El Salvador (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_f: DENOMINATOR },)

        t011c015_t = OBSColumn(
            id='t011c015_t',
            name='Peru (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_t: DENOMINATOR },)

        t011c015_m = OBSColumn(
            id='t011c015_m',
            name='Peru (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_m: DENOMINATOR },)

        t011c015_f = OBSColumn(
            id='t011c015_f',
            name='Peru (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_f: DENOMINATOR },)

        t011c016_t = OBSColumn(
            id='t011c016_t',
            name='Chile (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_t: DENOMINATOR },)

        t011c016_m = OBSColumn(
            id='t011c016_m',
            name='Chile (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_m: DENOMINATOR },)

        t011c016_f = OBSColumn(
            id='t011c016_f',
            name='Chile (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_f: DENOMINATOR },)

        t011c017_t = OBSColumn(
            id='t011c017_t',
            name='Other places of birth in Americas (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_t: DENOMINATOR },)

        t011c017_m = OBSColumn(
            id='t011c017_m',
            name='Other places of birth in Americas (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_m: DENOMINATOR },)

        t011c017_f = OBSColumn(
            id='t011c017_f',
            name='Other places of birth in Americas (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c006_f: DENOMINATOR },)

        t011c018_t = OBSColumn(
            id='t011c018_t',
            name='Europe (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c005_t: DENOMINATOR },)

        t011c018_m = OBSColumn(
            id='t011c018_m',
            name='Europe (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c005_m: DENOMINATOR },)

        t011c018_f = OBSColumn(
            id='t011c018_f',
            name='Europe (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c005_f: DENOMINATOR },)

        t011c019_t = OBSColumn(
            id='t011c019_t',
            name='United Kingdom (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_t: DENOMINATOR },)

        t011c019_m = OBSColumn(
            id='t011c019_m',
            name='United Kingdom (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_m: DENOMINATOR },)

        t011c019_f = OBSColumn(
            id='t011c019_f',
            name='United Kingdom (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_f: DENOMINATOR },)

        t011c020_t = OBSColumn(
            id='t011c020_t',
            name='Italy (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_t: DENOMINATOR },)

        t011c020_m = OBSColumn(
            id='t011c020_m',
            name='Italy (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_m: DENOMINATOR },)

        t011c020_f = OBSColumn(
            id='t011c020_f',
            name='Italy (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_f: DENOMINATOR },)

        t011c021_t = OBSColumn(
            id='t011c021_t',
            name='Germany (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_t: DENOMINATOR },)

        t011c021_m = OBSColumn(
            id='t011c021_m',
            name='Germany (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_m: DENOMINATOR },)

        t011c021_f = OBSColumn(
            id='t011c021_f',
            name='Germany (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_f: DENOMINATOR },)

        t011c022_t = OBSColumn(
            id='t011c022_t',
            name='Poland (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_t: DENOMINATOR },)

        t011c022_m = OBSColumn(
            id='t011c022_m',
            name='Poland (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_m: DENOMINATOR },)

        t011c022_f = OBSColumn(
            id='t011c022_f',
            name='Poland (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_f: DENOMINATOR },)

        t011c023_t = OBSColumn(
            id='t011c023_t',
            name='Portugal (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_t: DENOMINATOR },)

        t011c023_m = OBSColumn(
            id='t011c023_m',
            name='Portugal (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_m: DENOMINATOR },)

        t011c023_f = OBSColumn(
            id='t011c023_f',
            name='Portugal (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_f: DENOMINATOR },)

        t011c024_t = OBSColumn(
            id='t011c024_t',
            name='Netherlands (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_t: DENOMINATOR },)

        t011c024_m = OBSColumn(
            id='t011c024_m',
            name='Netherlands (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_m: DENOMINATOR },)

        t011c024_f = OBSColumn(
            id='t011c024_f',
            name='Netherlands (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_f: DENOMINATOR },)

        t011c025_t = OBSColumn(
            id='t011c025_t',
            name='France (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_t: DENOMINATOR },)

        t011c025_m = OBSColumn(
            id='t011c025_m',
            name='France (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_m: DENOMINATOR },)

        t011c025_f = OBSColumn(
            id='t011c025_f',
            name='France (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_f: DENOMINATOR },)

        t011c026_t = OBSColumn(
            id='t011c026_t',
            name='Romania (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_t: DENOMINATOR },)

        t011c026_m = OBSColumn(
            id='t011c026_m',
            name='Romania (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_m: DENOMINATOR },)

        t011c026_f = OBSColumn(
            id='t011c026_f',
            name='Romania (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_f: DENOMINATOR },)

        t011c027_t = OBSColumn(
            id='t011c027_t',
            name='Russian Federation (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_t: DENOMINATOR },)

        t011c027_m = OBSColumn(
            id='t011c027_m',
            name='Russian Federation (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_m: DENOMINATOR },)

        t011c027_f = OBSColumn(
            id='t011c027_f',
            name='Russian Federation (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_f: DENOMINATOR },)

        t011c028_t = OBSColumn(
            id='t011c028_t',
            name='Greece (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_t: DENOMINATOR },)

        t011c028_m = OBSColumn(
            id='t011c028_m',
            name='Greece (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_m: DENOMINATOR },)

        t011c028_f = OBSColumn(
            id='t011c028_f',
            name='Greece (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_f: DENOMINATOR },)

        t011c029_t = OBSColumn(
            id='t011c029_t',
            name='Ukraine (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_t: DENOMINATOR },)

        t011c029_m = OBSColumn(
            id='t011c029_m',
            name='Ukraine (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_m: DENOMINATOR },)

        t011c029_f = OBSColumn(
            id='t011c029_f',
            name='Ukraine (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_f: DENOMINATOR },)

        t011c030_t = OBSColumn(
            id='t011c030_t',
            name='Croatia (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_t: DENOMINATOR },)

        t011c030_m = OBSColumn(
            id='t011c030_m',
            name='Croatia (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_m: DENOMINATOR },)

        t011c030_f = OBSColumn(
            id='t011c030_f',
            name='Croatia (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_f: DENOMINATOR },)

        t011c031_t = OBSColumn(
            id='t011c031_t',
            name='Hungary (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_t: DENOMINATOR },)

        t011c031_m = OBSColumn(
            id='t011c031_m',
            name='Hungary (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_m: DENOMINATOR },)

        t011c031_f = OBSColumn(
            id='t011c031_f',
            name='Hungary (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_f: DENOMINATOR },)

        t011c032_t = OBSColumn(
            id='t011c032_t',
            name='Bosnia and Herzegovina (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_t: DENOMINATOR },)

        t011c032_m = OBSColumn(
            id='t011c032_m',
            name='Bosnia and Herzegovina (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_m: DENOMINATOR },)

        t011c032_f = OBSColumn(
            id='t011c032_f',
            name='Bosnia and Herzegovina (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_f: DENOMINATOR },)

        t011c033_t = OBSColumn(
            id='t011c033_t',
            name='Serbia (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_t: DENOMINATOR },)

        t011c033_m = OBSColumn(
            id='t011c033_m',
            name='Serbia (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_m: DENOMINATOR },)

        t011c033_f = OBSColumn(
            id='t011c033_f',
            name='Serbia (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_f: DENOMINATOR },)

        t011c034_t = OBSColumn(
            id='t011c034_t',
            name='Ireland, Republic of (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_t: DENOMINATOR },)

        t011c034_m = OBSColumn(
            id='t011c034_m',
            name='Ireland, Republic of (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_m: DENOMINATOR },)

        t011c034_f = OBSColumn(
            id='t011c034_f',
            name='Ireland, Republic of (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_f: DENOMINATOR },)

        t011c035_t = OBSColumn(
            id='t011c035_t',
            name='Other places of birth in Europe (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_t: DENOMINATOR },)

        t011c035_m = OBSColumn(
            id='t011c035_m',
            name='Other places of birth in Europe (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_m: DENOMINATOR },)

        t011c035_f = OBSColumn(
            id='t011c035_f',
            name='Other places of birth in Europe (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c018_f: DENOMINATOR },)

        t011c036_t = OBSColumn(
            id='t011c036_t',
            name='Africa (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c005_t: DENOMINATOR },)

        t011c036_m = OBSColumn(
            id='t011c036_m',
            name='Africa (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c005_m: DENOMINATOR },)

        t011c036_f = OBSColumn(
            id='t011c036_f',
            name='Africa (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c005_f: DENOMINATOR },)

        t011c037_t = OBSColumn(
            id='t011c037_t',
            name='Morocco (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_t: DENOMINATOR },)

        t011c037_m = OBSColumn(
            id='t011c037_m',
            name='Morocco (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_m: DENOMINATOR },)

        t011c037_f = OBSColumn(
            id='t011c037_f',
            name='Morocco (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_f: DENOMINATOR },)

        t011c038_t = OBSColumn(
            id='t011c038_t',
            name='Algeria (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_t: DENOMINATOR },)

        t011c038_m = OBSColumn(
            id='t011c038_m',
            name='Algeria (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_m: DENOMINATOR },)

        t011c038_f = OBSColumn(
            id='t011c038_f',
            name='Algeria (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_f: DENOMINATOR },)

        t011c039_t = OBSColumn(
            id='t011c039_t',
            name='Egypt (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_t: DENOMINATOR },)

        t011c039_m = OBSColumn(
            id='t011c039_m',
            name='Egypt (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_m: DENOMINATOR },)

        t011c039_f = OBSColumn(
            id='t011c039_f',
            name='Egypt (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_f: DENOMINATOR },)

        t011c040_t = OBSColumn(
            id='t011c040_t',
            name='South Africa, Republic of (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_t: DENOMINATOR },)

        t011c040_m = OBSColumn(
            id='t011c040_m',
            name='South Africa, Republic of (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_m: DENOMINATOR },)

        t011c040_f = OBSColumn(
            id='t011c040_f',
            name='South Africa, Republic of (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_f: DENOMINATOR },)

        t011c041_t = OBSColumn(
            id='t011c041_t',
            name='Nigeria (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_t: DENOMINATOR },)

        t011c041_m = OBSColumn(
            id='t011c041_m',
            name='Nigeria (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_m: DENOMINATOR },)

        t011c041_f = OBSColumn(
            id='t011c041_f',
            name='Nigeria (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_f: DENOMINATOR },)

        t011c042_t = OBSColumn(
            id='t011c042_t',
            name='Ethiopia (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_t: DENOMINATOR },)

        t011c042_m = OBSColumn(
            id='t011c042_m',
            name='Ethiopia (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_m: DENOMINATOR },)

        t011c042_f = OBSColumn(
            id='t011c042_f',
            name='Ethiopia (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_f: DENOMINATOR },)

        t011c043_t = OBSColumn(
            id='t011c043_t',
            name='Kenya (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_t: DENOMINATOR },)

        t011c043_m = OBSColumn(
            id='t011c043_m',
            name='Kenya (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_m: DENOMINATOR },)

        t011c043_f = OBSColumn(
            id='t011c043_f',
            name='Kenya (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_f: DENOMINATOR },)

        t011c044_t = OBSColumn(
            id='t011c044_t',
            name='Other places of birth in Africa (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_t: DENOMINATOR },)

        t011c044_m = OBSColumn(
            id='t011c044_m',
            name='Other places of birth in Africa (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_m: DENOMINATOR },)

        t011c044_f = OBSColumn(
            id='t011c044_f',
            name='Other places of birth in Africa (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c036_f: DENOMINATOR },)

        t011c045_t = OBSColumn(
            id='t011c045_t',
            name='Asia (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c005_t: DENOMINATOR },)

        t011c045_m = OBSColumn(
            id='t011c045_m',
            name='Asia (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c005_m: DENOMINATOR },)

        t011c045_f = OBSColumn(
            id='t011c045_f',
            name='Asia (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c005_f: DENOMINATOR },)

        t011c046_t = OBSColumn(
            id='t011c046_t',
            name='India (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_t: DENOMINATOR },)

        t011c046_m = OBSColumn(
            id='t011c046_m',
            name='India (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_m: DENOMINATOR },)

        t011c046_f = OBSColumn(
            id='t011c046_f',
            name='India (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_f: DENOMINATOR },)

        t011c047_t = OBSColumn(
            id='t011c047_t',
            name='China (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_t: DENOMINATOR },)

        t011c047_m = OBSColumn(
            id='t011c047_m',
            name='China (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_m: DENOMINATOR },)

        t011c047_f = OBSColumn(
            id='t011c047_f',
            name='China (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_f: DENOMINATOR },)

        t011c048_t = OBSColumn(
            id='t011c048_t',
            name='Philippines (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_t: DENOMINATOR },)

        t011c048_m = OBSColumn(
            id='t011c048_m',
            name='Philippines (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_m: DENOMINATOR },)

        t011c048_f = OBSColumn(
            id='t011c048_f',
            name='Philippines (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_f: DENOMINATOR },)

        t011c049_t = OBSColumn(
            id='t011c049_t',
            name='Hong Kong Special Administrative Region (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_t: DENOMINATOR },)

        t011c049_m = OBSColumn(
            id='t011c049_m',
            name='Hong Kong Special Administrative Region (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_m: DENOMINATOR },)

        t011c049_f = OBSColumn(
            id='t011c049_f',
            name='Hong Kong Special Administrative Region (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_f: DENOMINATOR },)

        t011c050_t = OBSColumn(
            id='t011c050_t',
            name='Viet Nam (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_t: DENOMINATOR },)

        t011c050_m = OBSColumn(
            id='t011c050_m',
            name='Viet Nam (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_m: DENOMINATOR },)

        t011c050_f = OBSColumn(
            id='t011c050_f',
            name='Viet Nam (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_f: DENOMINATOR },)

        t011c051_t = OBSColumn(
            id='t011c051_t',
            name='Pakistan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_t: DENOMINATOR },)

        t011c051_m = OBSColumn(
            id='t011c051_m',
            name='Pakistan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_m: DENOMINATOR },)

        t011c051_f = OBSColumn(
            id='t011c051_f',
            name='Pakistan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_f: DENOMINATOR },)

        t011c052_t = OBSColumn(
            id='t011c052_t',
            name='Sri Lanka (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_t: DENOMINATOR },)

        t011c052_m = OBSColumn(
            id='t011c052_m',
            name='Sri Lanka (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_m: DENOMINATOR },)

        t011c052_f = OBSColumn(
            id='t011c052_f',
            name='Sri Lanka (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_f: DENOMINATOR },)

        t011c053_t = OBSColumn(
            id='t011c053_t',
            name='Iran (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_t: DENOMINATOR },)

        t011c053_m = OBSColumn(
            id='t011c053_m',
            name='Iran (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_m: DENOMINATOR },)

        t011c053_f = OBSColumn(
            id='t011c053_f',
            name='Iran (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_f: DENOMINATOR },)

        t011c054_t = OBSColumn(
            id='t011c054_t',
            name='Korea, South (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_t: DENOMINATOR },)

        t011c054_m = OBSColumn(
            id='t011c054_m',
            name='Korea, South (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_m: DENOMINATOR },)

        t011c054_f = OBSColumn(
            id='t011c054_f',
            name='Korea, South (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_f: DENOMINATOR },)

        t011c055_t = OBSColumn(
            id='t011c055_t',
            name='Lebanon (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_t: DENOMINATOR },)

        t011c055_m = OBSColumn(
            id='t011c055_m',
            name='Lebanon (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_m: DENOMINATOR },)

        t011c055_f = OBSColumn(
            id='t011c055_f',
            name='Lebanon (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_f: DENOMINATOR },)

        t011c056_t = OBSColumn(
            id='t011c056_t',
            name='Taiwan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_t: DENOMINATOR },)

        t011c056_m = OBSColumn(
            id='t011c056_m',
            name='Taiwan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_m: DENOMINATOR },)

        t011c056_f = OBSColumn(
            id='t011c056_f',
            name='Taiwan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_f: DENOMINATOR },)

        t011c057_t = OBSColumn(
            id='t011c057_t',
            name='Iraq (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_t: DENOMINATOR },)

        t011c057_m = OBSColumn(
            id='t011c057_m',
            name='Iraq (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_m: DENOMINATOR },)

        t011c057_f = OBSColumn(
            id='t011c057_f',
            name='Iraq (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_f: DENOMINATOR },)

        t011c058_t = OBSColumn(
            id='t011c058_t',
            name='Bangladesh (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_t: DENOMINATOR },)

        t011c058_m = OBSColumn(
            id='t011c058_m',
            name='Bangladesh (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_m: DENOMINATOR },)

        t011c058_f = OBSColumn(
            id='t011c058_f',
            name='Bangladesh (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_f: DENOMINATOR },)

        t011c059_t = OBSColumn(
            id='t011c059_t',
            name='Afghanistan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_t: DENOMINATOR },)

        t011c059_m = OBSColumn(
            id='t011c059_m',
            name='Afghanistan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_m: DENOMINATOR },)

        t011c059_f = OBSColumn(
            id='t011c059_f',
            name='Afghanistan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_f: DENOMINATOR },)

        t011c060_t = OBSColumn(
            id='t011c060_t',
            name='Japan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_t: DENOMINATOR },)

        t011c060_m = OBSColumn(
            id='t011c060_m',
            name='Japan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_m: DENOMINATOR },)

        t011c060_f = OBSColumn(
            id='t011c060_f',
            name='Japan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_f: DENOMINATOR },)

        t011c061_t = OBSColumn(
            id='t011c061_t',
            name='Turkey (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_t: DENOMINATOR },)

        t011c061_m = OBSColumn(
            id='t011c061_m',
            name='Turkey (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_m: DENOMINATOR },)

        t011c061_f = OBSColumn(
            id='t011c061_f',
            name='Turkey (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_f: DENOMINATOR },)

        t011c062_t = OBSColumn(
            id='t011c062_t',
            name='Other places of birth in Asia (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_t: DENOMINATOR },)

        t011c062_m = OBSColumn(
            id='t011c062_m',
            name='Other places of birth in Asia (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_m: DENOMINATOR },)

        t011c062_f = OBSColumn(
            id='t011c062_f',
            name='Other places of birth in Asia (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c045_f: DENOMINATOR },)

        t011c063_t = OBSColumn(
            id='t011c063_t',
            name='Oceania and other (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c005_t: DENOMINATOR },)

        t011c063_m = OBSColumn(
            id='t011c063_m',
            name='Oceania and other (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c005_m: DENOMINATOR },)

        t011c063_f = OBSColumn(
            id='t011c063_f',
            name='Oceania and other (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c005_f: DENOMINATOR },)

        t011c064_t = OBSColumn(
            id='t011c064_t',
            name='Fiji (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c063_t: DENOMINATOR },)

        t011c064_m = OBSColumn(
            id='t011c064_m',
            name='Fiji (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c063_m: DENOMINATOR },)

        t011c064_f = OBSColumn(
            id='t011c064_f',
            name='Fiji (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c063_f: DENOMINATOR },)

        t011c065_t = OBSColumn(
            id='t011c065_t',
            name='Other places of birth (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c063_t: DENOMINATOR },)

        t011c065_m = OBSColumn(
            id='t011c065_m',
            name='Other places of birth (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c063_m: DENOMINATOR },)

        t011c065_f = OBSColumn(
            id='t011c065_f',
            name='Other places of birth (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t011c063_f: DENOMINATOR },)

        t011c066_t = OBSColumn(
            id='t011c066_t',
            name='Total population - Non-permanent residents (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t001c001_t: DENOMINATOR },)

        t011c066_m = OBSColumn(
            id='t011c066_m',
            name='Total population - Non-permanent residents (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t001c001_m: DENOMINATOR },)

        t011c066_f = OBSColumn(
            id='t011c066_f',
            name='Total population - Non-permanent residents (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t001c001_f: DENOMINATOR },)

        # FIXME
        # According to the name, this looks like it should be median/average
        # income.
        #
        # But having looked at the data, it looks like this is actually the
        # number of households, likely slightly less since the number of
        # households reporting income (what I think this is)
        # seems to be slightly less than `ca.statcan.cols_nhs.t009c001_t`.
        #
        # If it's the case that this very similar to `ca.statcan.cols_nhs.t009c001_t`,
        # either the name of this should be modified to make it clearer that
        # it's the number of households reporting income in 2010, or this
        # should simply be eliminated and the denominator for all the income
        # bands should be set to `ca.statcan.cols_nhs.t009c001_t`.
        t012c001_t = OBSColumn(
            id='t012c001_t',
            name='Household income in 2010 of private households',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={},)

        t012c002_t = OBSColumn(
            id='t012c002_t',
            name='Household total income - Under $5,000',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c003_t = OBSColumn(
            id='t012c003_t',
            name='Household total income - $5,000 to $9,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c004_t = OBSColumn(
            id='t012c004_t',
            name='Household total income - $10,000 to $14,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c005_t = OBSColumn(
            id='t012c005_t',
            name='Household total income - $15,000 to $19,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c006_t = OBSColumn(
            id='t012c006_t',
            name='Household total income - $20,000 to $29,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c007_t = OBSColumn(
            id='t012c007_t',
            name='Household total income - $30,000 to $39,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c008_t = OBSColumn(
            id='t012c008_t',
            name='Household total income - $40,000 to $49,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c009_t = OBSColumn(
            id='t012c009_t',
            name='Household total income - $50,000 to $59,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c010_t = OBSColumn(
            id='t012c010_t',
            name='Household total income - $60,000 to $79,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c011_t = OBSColumn(
            id='t012c011_t',
            name='Household total income - $80,000 to $99,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c012_t = OBSColumn(
            id='t012c012_t',
            name='Household total income - $100,000 to $124,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c013_t = OBSColumn(
            id='t012c013_t',
            name='Household total income - $125,000 to $149,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c014_t = OBSColumn(
            id='t012c014_t',
            name='Household total income - $150,000 and over',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c016_t = OBSColumn(
            id='t012c016_t',
            name='Household after-tax income - Under $5,000',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c017_t = OBSColumn(
            id='t012c017_t',
            name='Household after-tax income - $5,000 to $9,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c018_t = OBSColumn(
            id='t012c018_t',
            name='Household after-tax income - $10,000 to $14,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c019_t = OBSColumn(
            id='t012c019_t',
            name='Household after-tax income - $15,000 to $19,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c020_t = OBSColumn(
            id='t012c020_t',
            name='Household after-tax income - $20,000 to $29,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c021_t = OBSColumn(
            id='t012c021_t',
            name='Household after-tax income - $30,000 to $39,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c022_t = OBSColumn(
            id='t012c022_t',
            name='Household after-tax income - $40,000 to $49,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c023_t = OBSColumn(
            id='t012c023_t',
            name='Household after-tax income - $50,000 to $59,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c024_t = OBSColumn(
            id='t012c024_t',
            name='Household after-tax income - $60,000 to $79,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c025_t = OBSColumn(
            id='t012c025_t',
            name='Household after-tax income - $80,000 to $99,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c026_t = OBSColumn(
            id='t012c026_t',
            name='Household after-tax income - $100,000 and over',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        t012c027_t = OBSColumn(
            id='t012c027_t',
            name='Household after-tax income - $100,000 to $124,999',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c026_t: DENOMINATOR },)

        t012c028_t = OBSColumn(
            id='t012c028_t',
            name='Household after-tax income - $125,000 and over',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c026_t: DENOMINATOR },)

        # FIXME
        # Everything with universe isn't coming through right now, but this
        # seems to be an issue unrelated to metadata.  Made an issue #96.
        #
        # Since we define a unit money, the ($) are not necessary.
        t012c030_t = OBSColumn(
            id='t012c030_t',
            name='Median household total income ($)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t012c001_t: UNIVERSE },)

        t012c031_t = OBSColumn(
            id='t012c031_t',
            name='Average household total income ($)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t012c001_t: UNIVERSE },)

        t012c032_t = OBSColumn(
            id='t012c032_t',
            name='Median after-tax household income ($)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t012c001_t: UNIVERSE },)

        t012c033_t = OBSColumn(
            id='t012c033_t',
            name='Average after-tax household income ($)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t012c001_t: UNIVERSE },)

        t012c034_t = OBSColumn(
            id='t012c034_t',
            name='One-person private households',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        # FIXME
        # t012c034_t was redundant with ca.statcan.cols_census.t007c029_t,
        # removed.
        #
        # Its subcolumns should be renamed to clearly specify in their
        # names that the income is for one-person households
        t012c035_t = OBSColumn(
            id='t012c035_t',
            name='Median household total income ($)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t012c034_t: UNIVERSE },)

        t012c036_t = OBSColumn(
            id='t012c036_t',
            name='Average household total income ($)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t012c034_t: UNIVERSE },)

        t012c037_t = OBSColumn(
            id='t012c037_t',
            name='Median after-tax household income ($)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t012c034_t: UNIVERSE },)

        t012c038_t = OBSColumn(
            id='t012c038_t',
            name='Average after-tax household income ($)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t012c034_t: UNIVERSE },)

        t012c039_t = OBSColumn(
            id='t012c039_t',
            name='Two-or-more-persons private households',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t012c001_t: DENOMINATOR },)

        # FIXME
        # t012c039_t was redundant with ca.statcan.cols_census.t007c030_t,
        # removed.
        #
        # Its subcolumns should be renamed to clearly specify in their
        # names that the income is for two-or-more-persons households
        t012c040_t = OBSColumn(
            id='t012c040_t',
            name='Median household total income ($)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t012c039_t: UNIVERSE },)

        t012c041_t = OBSColumn(
            id='t012c041_t',
            name='Average household total income ($)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t012c039_t: UNIVERSE },)

        t012c042_t = OBSColumn(
            id='t012c042_t',
            name='Median after-tax household income ($)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t012c039_t: UNIVERSE },)

        t012c043_t = OBSColumn(
            id='t012c043_t',
            name='Average after-tax household income ($)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t012c039_t: UNIVERSE },)

        # FIXME
        # Are the three below the populations for which income was reported?
        # If so, that should either be more clearly specified, or they should
        # be removed, and their subcolumns instead pointed to a more generic
        # total population aged 15 years and over (like
        # `ca.statcan.cols_census.t009c001_t`)
        t013c001_t = OBSColumn(
            id='t013c001_t',
            name='Total income in 2010 of population aged 15 years and over (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={},)

        t013c001_m = OBSColumn(
            id='t013c001_m',
            name='Total income in 2010 of population aged 15 years and over (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={},)

        t013c001_f = OBSColumn(
            id='t013c001_f',
            name='Total income in 2010 of population aged 15 years and over (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={},)

        # Clearer titles for these would be "Population without income", etc.
        t013c002_t = OBSColumn(
            id='t013c002_t',
            name='Income in 2010 of population aged 15+ - Without income (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c001_t: DENOMINATOR },)

        t013c002_m = OBSColumn(
            id='t013c002_m',
            name='Income in 2010 of population aged 15+ - Without income (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c001_m: DENOMINATOR },)

        t013c002_f = OBSColumn(
            id='t013c002_f',
            name='Income in 2010 of population aged 15+ - Without income (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c001_f: DENOMINATOR },)

        t013c003_t = OBSColumn(
            id='t013c003_t',
            name='Income in 2010 of population aged 15+ - With income (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c001_t: DENOMINATOR },)

        t013c003_m = OBSColumn(
            id='t013c003_m',
            name='Income in 2010 of population aged 15+ - With income (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c001_m: DENOMINATOR },)

        t013c003_f = OBSColumn(
            id='t013c003_f',
            name='Income in 2010 of population aged 15+ - With income (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c001_f: DENOMINATOR },)

        # FIXME
        # The below should be more clearly indicated as the population with an
        # income in that band, rather than a household.
        t013c004_t = OBSColumn(
            id='t013c004_t',
            name='Total income ages 15+ - Under $5,000 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_t: DENOMINATOR },)

        t013c004_m = OBSColumn(
            id='t013c004_m',
            name='Total income ages 15+ - Under $5,000 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_m: DENOMINATOR },)

        t013c004_f = OBSColumn(
            id='t013c004_f',
            name='Total income ages 15+ - Under $5,000 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_f: DENOMINATOR },)

        t013c005_t = OBSColumn(
            id='t013c005_t',
            name='Total income ages 15+ - $5,000 to $9,999 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_t: DENOMINATOR },)

        t013c005_m = OBSColumn(
            id='t013c005_m',
            name='Total income ages 15+ - $5,000 to $9,999 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_m: DENOMINATOR },)

        t013c005_f = OBSColumn(
            id='t013c005_f',
            name='Total income ages 15+ - $5,000 to $9,999 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_f: DENOMINATOR },)

        t013c006_t = OBSColumn(
            id='t013c006_t',
            name='Total income ages 15+ - $10,000 to $14,999 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_t: DENOMINATOR },)

        t013c006_m = OBSColumn(
            id='t013c006_m',
            name='Total income ages 15+ - $10,000 to $14,999 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_m: DENOMINATOR },)

        t013c006_f = OBSColumn(
            id='t013c006_f',
            name='Total income ages 15+ - $10,000 to $14,999 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_f: DENOMINATOR },)

        t013c007_t = OBSColumn(
            id='t013c007_t',
            name='Total income ages 15+ - $15,000 to $19,999 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_t: DENOMINATOR },)

        t013c007_m = OBSColumn(
            id='t013c007_m',
            name='Total income ages 15+ - $15,000 to $19,999 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_m: DENOMINATOR },)

        t013c007_f = OBSColumn(
            id='t013c007_f',
            name='Total income ages 15+ - $15,000 to $19,999 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_f: DENOMINATOR },)

        t013c008_t = OBSColumn(
            id='t013c008_t',
            name='Total income ages 15+ - $20,000 to $29,999 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_t: DENOMINATOR },)

        t013c008_m = OBSColumn(
            id='t013c008_m',
            name='Total income ages 15+ - $20,000 to $29,999 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_m: DENOMINATOR },)

        t013c008_f = OBSColumn(
            id='t013c008_f',
            name='Total income ages 15+ - $20,000 to $29,999 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_f: DENOMINATOR },)

        t013c009_t = OBSColumn(
            id='t013c009_t',
            name='Total income ages 15+ - $30,000 to $39,999 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_t: DENOMINATOR },)

        t013c009_m = OBSColumn(
            id='t013c009_m',
            name='Total income ages 15+ - $30,000 to $39,999 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_m: DENOMINATOR },)

        t013c009_f = OBSColumn(
            id='t013c009_f',
            name='Total income ages 15+ - $30,000 to $39,999 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_f: DENOMINATOR },)

        t013c010_t = OBSColumn(
            id='t013c010_t',
            name='Total income ages 15+ - $40,000 to $49,999 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_t: DENOMINATOR },)

        t013c010_m = OBSColumn(
            id='t013c010_m',
            name='Total income ages 15+ - $40,000 to $49,999 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_m: DENOMINATOR },)

        t013c010_f = OBSColumn(
            id='t013c010_f',
            name='Total income ages 15+ - $40,000 to $49,999 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_f: DENOMINATOR },)

        t013c011_t = OBSColumn(
            id='t013c011_t',
            name='Total income ages 15+ - $50,000 to $59,999 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_t: DENOMINATOR },)

        t013c011_m = OBSColumn(
            id='t013c011_m',
            name='Total income ages 15+ - $50,000 to $59,999 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_m: DENOMINATOR },)

        t013c011_f = OBSColumn(
            id='t013c011_f',
            name='Total income ages 15+ - $50,000 to $59,999 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_f: DENOMINATOR },)

        t013c012_t = OBSColumn(
            id='t013c012_t',
            name='Total income ages 15+ - $60,000 to $79,999 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_t: DENOMINATOR },)

        t013c012_m = OBSColumn(
            id='t013c012_m',
            name='Total income ages 15+ - $60,000 to $79,999 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_m: DENOMINATOR },)

        t013c012_f = OBSColumn(
            id='t013c012_f',
            name='Total income ages 15+ - $60,000 to $79,999 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_f: DENOMINATOR },)

        t013c013_t = OBSColumn(
            id='t013c013_t',
            name='Total income ages 15+ - $80,000 to $99,999 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_t: DENOMINATOR },)

        t013c013_m = OBSColumn(
            id='t013c013_m',
            name='Total income ages 15+ - $80,000 to $99,999 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_m: DENOMINATOR },)

        t013c013_f = OBSColumn(
            id='t013c013_f',
            name='Total income ages 15+ - $80,000 to $99,999 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_f: DENOMINATOR },)

        t013c014_t = OBSColumn(
            id='t013c014_t',
            name='Total income ages 15+ - $100,000 and over (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_t: DENOMINATOR },)

        t013c014_m = OBSColumn(
            id='t013c014_m',
            name='Total income ages 15+ - $100,000 and over (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_m: DENOMINATOR },)

        t013c014_f = OBSColumn(
            id='t013c014_f',
            name='Total income ages 15+ - $100,000 and over (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c003_f: DENOMINATOR },)

        t013c015_t = OBSColumn(
            id='t013c015_t',
            name='Total income ages 15+ - $100,000 to $124,999 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c014_t: DENOMINATOR },)

        t013c015_m = OBSColumn(
            id='t013c015_m',
            name='Total income ages 15+ - $100,000 to $124,999 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c014_m: DENOMINATOR },)

        t013c015_f = OBSColumn(
            id='t013c015_f',
            name='Total income ages 15+ - $100,000 to $124,999 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c014_f: DENOMINATOR },)

        t013c016_t = OBSColumn(
            id='t013c016_t',
            name='Total income ages 15+ - $125,000 and over (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c014_t: DENOMINATOR },)

        t013c016_m = OBSColumn(
            id='t013c016_m',
            name='Total income ages 15+ - $125,000 and over (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c014_m: DENOMINATOR },)

        t013c016_f = OBSColumn(
            id='t013c016_f',
            name='Total income ages 15+ - $125,000 and over (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c014_f: DENOMINATOR },)

        # FIXME
        # Needs clearer name that this is the median individual income, as
        # opposed to median household income.
        #
        # Same goes for the similar columns below.
        t013c017_t = OBSColumn(
            id='t013c017_t',
            name='Income in 2010 of population aged 15+ - Median income ($) (total)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c001_t: UNIVERSE },)

        t013c017_m = OBSColumn(
            id='t013c017_m',
            name='Income in 2010 of population aged 15+ - Median income ($) (male)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c001_m: UNIVERSE },)

        t013c017_f = OBSColumn(
            id='t013c017_f',
            name='Income in 2010 of population aged 15+ - Median income ($) (female)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c001_f: UNIVERSE },)

        t013c018_t = OBSColumn(
            id='t013c018_t',
            name='Income in 2010 of population aged 15+ - Average income ($) (total)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c001_t: UNIVERSE },)

        t013c018_m = OBSColumn(
            id='t013c018_m',
            name='Income in 2010 of population aged 15+ - Average income ($) (male)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c001_m: UNIVERSE },)

        t013c018_f = OBSColumn(
            id='t013c018_f',
            name='Income in 2010 of population aged 15+ - Average income ($) (female)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c001_f: UNIVERSE },)

        # FIXME
        # The below need clearer names like "Population without after-tax income", ...
        t013c020_t = OBSColumn(
            id='t013c020_t',
            name='Without after-tax income (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c001_t: DENOMINATOR },)

        t013c020_m = OBSColumn(
            id='t013c020_m',
            name='Without after-tax income (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c001_m: DENOMINATOR },)

        t013c020_f = OBSColumn(
            id='t013c020_f',
            name='Without after-tax income (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c001_f: DENOMINATOR },)

        t013c021_t = OBSColumn(
            id='t013c021_t',
            name='With after-tax income (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c001_t: DENOMINATOR },)

        t013c021_m = OBSColumn(
            id='t013c021_m',
            name='With after-tax income (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c001_m: DENOMINATOR },)

        t013c021_f = OBSColumn(
            id='t013c021_f',
            name='With after-tax income (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c001_f: DENOMINATOR },)

        # FIXME
        # Same edits needed as above for Total income
        t013c022_t = OBSColumn(
            id='t013c022_t',
            name='After-tax income ages 15+ - Under $5,000 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_t: DENOMINATOR },)

        t013c022_m = OBSColumn(
            id='t013c022_m',
            name='After-tax income ages 15+ - Under $5,000 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_m: DENOMINATOR },)

        t013c022_f = OBSColumn(
            id='t013c022_f',
            name='After-tax income ages 15+ - Under $5,000 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_f: DENOMINATOR },)

        t013c023_t = OBSColumn(
            id='t013c023_t',
            name='After-tax income ages 15+ - $5,000 to $9,999 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_t: DENOMINATOR },)

        t013c023_m = OBSColumn(
            id='t013c023_m',
            name='After-tax income ages 15+ - $5,000 to $9,999 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_m: DENOMINATOR },)

        t013c023_f = OBSColumn(
            id='t013c023_f',
            name='After-tax income ages 15+ - $5,000 to $9,999 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_f: DENOMINATOR },)

        t013c024_t = OBSColumn(
            id='t013c024_t',
            name='After-tax income ages 15+ - $10,000 to $14,999 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_t: DENOMINATOR },)

        t013c024_m = OBSColumn(
            id='t013c024_m',
            name='After-tax income ages 15+ - $10,000 to $14,999 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_m: DENOMINATOR },)

        t013c024_f = OBSColumn(
            id='t013c024_f',
            name='After-tax income ages 15+ - $10,000 to $14,999 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_f: DENOMINATOR },)

        t013c025_t = OBSColumn(
            id='t013c025_t',
            name='After-tax income ages 15+ - $15,000 to $19,999 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_t: DENOMINATOR },)

        t013c025_m = OBSColumn(
            id='t013c025_m',
            name='After-tax income ages 15+ - $15,000 to $19,999 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_m: DENOMINATOR },)

        t013c025_f = OBSColumn(
            id='t013c025_f',
            name='After-tax income ages 15+ - $15,000 to $19,999 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_f: DENOMINATOR },)

        t013c026_t = OBSColumn(
            id='t013c026_t',
            name='After-tax income ages 15+ - $20,000 to $29,999 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_t: DENOMINATOR },)

        t013c026_m = OBSColumn(
            id='t013c026_m',
            name='After-tax income ages 15+ - $20,000 to $29,999 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_m: DENOMINATOR },)

        t013c026_f = OBSColumn(
            id='t013c026_f',
            name='After-tax income ages 15+ - $20,000 to $29,999 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_f: DENOMINATOR },)

        t013c027_t = OBSColumn(
            id='t013c027_t',
            name='After-tax income ages 15+ - $30,000 to $39,999 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_t: DENOMINATOR },)

        t013c027_m = OBSColumn(
            id='t013c027_m',
            name='After-tax income ages 15+ - $30,000 to $39,999 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_m: DENOMINATOR },)

        t013c027_f = OBSColumn(
            id='t013c027_f',
            name='After-tax income ages 15+ - $30,000 to $39,999 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_f: DENOMINATOR },)

        t013c028_t = OBSColumn(
            id='t013c028_t',
            name='After-tax income ages 15+ - $40,000 to $49,999 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_t: DENOMINATOR },)

        t013c028_m = OBSColumn(
            id='t013c028_m',
            name='After-tax income ages 15+ - $40,000 to $49,999 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_m: DENOMINATOR },)

        t013c028_f = OBSColumn(
            id='t013c028_f',
            name='After-tax income ages 15+ - $40,000 to $49,999 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_f: DENOMINATOR },)

        t013c029_t = OBSColumn(
            id='t013c029_t',
            name='After-tax income ages 15+ - $50,000 to $59,999 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_t: DENOMINATOR },)

        t013c029_m = OBSColumn(
            id='t013c029_m',
            name='After-tax income ages 15+ - $50,000 to $59,999 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_m: DENOMINATOR },)

        t013c029_f = OBSColumn(
            id='t013c029_f',
            name='After-tax income ages 15+ - $50,000 to $59,999 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_f: DENOMINATOR },)

        t013c030_t = OBSColumn(
            id='t013c030_t',
            name='After-tax income ages 15+ - $60,000 to $79,999 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_t: DENOMINATOR },)

        t013c030_m = OBSColumn(
            id='t013c030_m',
            name='After-tax income ages 15+ - $60,000 to $79,999 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_m: DENOMINATOR },)

        t013c030_f = OBSColumn(
            id='t013c030_f',
            name='After-tax income ages 15+ - $60,000 to $79,999 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_f: DENOMINATOR },)

        t013c031_t = OBSColumn(
            id='t013c031_t',
            name='After-tax income ages 15+ - $80,000 to $99,999 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_t: DENOMINATOR },)

        t013c031_m = OBSColumn(
            id='t013c031_m',
            name='After-tax income ages 15+ - $80,000 to $99,999 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_m: DENOMINATOR },)

        t013c031_f = OBSColumn(
            id='t013c031_f',
            name='After-tax income ages 15+ - $80,000 to $99,999 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_f: DENOMINATOR },)

        t013c032_t = OBSColumn(
            id='t013c032_t',
            name='After-tax income ages 15+ - $100,000 and over (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_t: DENOMINATOR },)

        t013c032_m = OBSColumn(
            id='t013c032_m',
            name='After-tax income ages 15+ - $100,000 and over (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_m: DENOMINATOR },)

        t013c032_f = OBSColumn(
            id='t013c032_f',
            name='After-tax income ages 15+ - $100,000 and over (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c021_f: DENOMINATOR },)

        t013c033_t = OBSColumn(
            id='t013c033_t',
            name='Median after-tax income ($) (total)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c001_t: UNIVERSE },)

        t013c033_m = OBSColumn(
            id='t013c033_m',
            name='Median after-tax income ($) (male)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c001_m: UNIVERSE },)

        t013c033_f = OBSColumn(
            id='t013c033_f',
            name='Median after-tax income ($) (female)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c001_f: UNIVERSE },)

        t013c034_t = OBSColumn(
            id='t013c034_t',
            name='Average after-tax income ($) (total)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c001_t: UNIVERSE },)

        t013c034_m = OBSColumn(
            id='t013c034_m',
            name='Average after-tax income ($) (male)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c001_m: UNIVERSE },)

        t013c034_f = OBSColumn(
            id='t013c034_f',
            name='Average after-tax income ($) (female)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c001_f: UNIVERSE },)

        #FIXME
        # The three below again appear to be repetitive with a count of
        # population over the age 15 upon whom income data can be reported,
        # and should be removed.
        t013c035_t = OBSColumn(
            id='t013c035_t',
            name='Composition of total income in 2010 of population 15 years and over (%) (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={},)

        t013c035_m = OBSColumn(
            id='t013c035_m',
            name='Composition of total income in 2010 of population 15 years and over (%) (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={},)

        t013c035_f = OBSColumn(
            id='t013c035_f',
            name='Composition of total income in 2010 of population 15 years and over (%) (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={},)

        t013c036_t = OBSColumn(
            id='t013c036_t',
            name='Market income (%) (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c035_t: UNIVERSE },)

        t013c036_m = OBSColumn(
            id='t013c036_m',
            name='Market income (%) (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c035_m: UNIVERSE },)

        t013c036_f = OBSColumn(
            id='t013c036_f',
            name='Market income (%) (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c035_f: UNIVERSE },)

        t013c037_t = OBSColumn(
            id='t013c037_t',
            name='Employment income (%) (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c036_t: UNIVERSE },)

        t013c037_m = OBSColumn(
            id='t013c037_m',
            name='Employment income (%) (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c036_m: UNIVERSE },)

        t013c037_f = OBSColumn(
            id='t013c037_f',
            name='Employment income (%) (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c036_f: UNIVERSE },)

        t013c038_t = OBSColumn(
            id='t013c038_t',
            name='Wages and salaries (%) (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c037_t: UNIVERSE },)

        t013c038_m = OBSColumn(
            id='t013c038_m',
            name='Wages and salaries (%) (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c037_m: UNIVERSE },)

        t013c038_f = OBSColumn(
            id='t013c038_f',
            name='Wages and salaries (%) (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c037_f: UNIVERSE },)

        t013c039_t = OBSColumn(
            id='t013c039_t',
            name='Self-employment income (%) (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c037_t: UNIVERSE },)

        t013c039_m = OBSColumn(
            id='t013c039_m',
            name='Self-employment income (%) (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c037_m: UNIVERSE },)

        t013c039_f = OBSColumn(
            id='t013c039_f',
            name='Self-employment income (%) (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c037_f: UNIVERSE },)

        t013c040_t = OBSColumn(
            id='t013c040_t',
            name='Investment income (%) (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c036_t: UNIVERSE },)

        t013c040_m = OBSColumn(
            id='t013c040_m',
            name='Investment income (%) (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c036_m: UNIVERSE },)

        t013c040_f = OBSColumn(
            id='t013c040_f',
            name='Investment income (%) (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c036_f: UNIVERSE },)

        t013c041_t = OBSColumn(
            id='t013c041_t',
            name='Retirement pensions, superannuation and annuities (%) (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c036_t: UNIVERSE },)

        t013c041_m = OBSColumn(
            id='t013c041_m',
            name='Retirement pensions, superannuation and annuities (%) (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c036_m: UNIVERSE },)

        t013c041_f = OBSColumn(
            id='t013c041_f',
            name='Retirement pensions, superannuation and annuities (%) (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c036_f: UNIVERSE },)

        t013c042_t = OBSColumn(
            id='t013c042_t',
            name='Other money income (%) (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c036_t: UNIVERSE },)

        t013c042_m = OBSColumn(
            id='t013c042_m',
            name='Other money income (%) (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c036_m: UNIVERSE },)

        t013c042_f = OBSColumn(
            id='t013c042_f',
            name='Other money income (%) (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c036_f: UNIVERSE },)

        t013c043_t = OBSColumn(
            id='t013c043_t',
            name='Government transfer payments (%) (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c035_t: UNIVERSE },)

        t013c043_m = OBSColumn(
            id='t013c043_m',
            name='Government transfer payments (%) (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c035_m: UNIVERSE },)

        t013c043_f = OBSColumn(
            id='t013c043_f',
            name='Government transfer payments (%) (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c035_f: UNIVERSE },)

        t013c044_t = OBSColumn(
            id='t013c044_t',
            name='Canada/Quebec Pension Plan benefits (%) (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c043_t: UNIVERSE },)

        t013c044_m = OBSColumn(
            id='t013c044_m',
            name='Canada/Quebec Pension Plan benefits (%) (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c043_m: UNIVERSE },)

        t013c044_f = OBSColumn(
            id='t013c044_f',
            name='Canada/Quebec Pension Plan benefits (%) (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c043_f: UNIVERSE },)

        t013c045_t = OBSColumn(
            id='t013c045_t',
            name='Old Age Security pensions and Guaranteed Income Supplement (%) (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c043_t: UNIVERSE },)

        t013c045_m = OBSColumn(
            id='t013c045_m',
            name='Old Age Security pensions and Guaranteed Income Supplement (%) (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c043_m: UNIVERSE },)

        t013c045_f = OBSColumn(
            id='t013c045_f',
            name='Old Age Security pensions and Guaranteed Income Supplement (%) (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c043_f: UNIVERSE },)

        t013c046_t = OBSColumn(
            id='t013c046_t',
            name='Employment Insurance benefits (%) (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c043_t: UNIVERSE },)

        t013c046_m = OBSColumn(
            id='t013c046_m',
            name='Employment Insurance benefits (%) (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c043_m: UNIVERSE },)

        t013c046_f = OBSColumn(
            id='t013c046_f',
            name='Employment Insurance benefits (%) (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c043_f: UNIVERSE },)

        t013c047_t = OBSColumn(
            id='t013c047_t',
            name='Child benefits (%) (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c043_t: UNIVERSE },)

        t013c047_m = OBSColumn(
            id='t013c047_m',
            name='Child benefits (%) (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c043_m: UNIVERSE },)

        t013c047_f = OBSColumn(
            id='t013c047_f',
            name='Child benefits (%) (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c043_f: UNIVERSE },)

        t013c048_t = OBSColumn(
            id='t013c048_t',
            name='Other income from government sources (%) (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c043_t: UNIVERSE },)

        t013c048_m = OBSColumn(
            id='t013c048_m',
            name='Other income from government sources (%) (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c043_m: UNIVERSE },)

        t013c048_f = OBSColumn(
            id='t013c048_f',
            name='Other income from government sources (%) (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c043_f: UNIVERSE },)

        t013c049_t = OBSColumn(
            id='t013c049_t',
            name='Income taxes paid as a % of total income (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c035_t: UNIVERSE },)

        t013c049_m = OBSColumn(
            id='t013c049_m',
            name='Income taxes paid as a % of total income (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c035_m: UNIVERSE },)

        t013c049_f = OBSColumn(
            id='t013c049_f',
            name='Income taxes paid as a % of total income (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c035_f: UNIVERSE },)

        t013c050_t = OBSColumn(
            id='t013c050_t',
            name='After-tax income as a % of total income (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c035_t: UNIVERSE },)

        t013c050_m = OBSColumn(
            id='t013c050_m',
            name='After-tax income as a % of total income (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c035_m: UNIVERSE },)

        t013c050_f = OBSColumn(
            id='t013c050_f',
            name='After-tax income as a % of total income (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c035_f: UNIVERSE },)

        t013c051_t = OBSColumn(
            id='t013c051_t',
            name='Net capital gains or losses as a % of total income (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={},)

        t013c051_m = OBSColumn(
            id='t013c051_m',
            name='Net capital gains or losses as a % of total income (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={},)

        t013c051_f = OBSColumn(
            id='t013c051_f',
            name='Net capital gains or losses as a % of total income (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={},)

        t013c052_t = OBSColumn(
            id='t013c052_t',
            name='Population aged 15 years and over who worked full year, full time and with employment income in 2010 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={},)

        t013c052_m = OBSColumn(
            id='t013c052_m',
            name='Population aged 15 years and over who worked full year, full time and with employment income in 2010 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={},)

        t013c052_f = OBSColumn(
            id='t013c052_f',
            name='Population aged 15 years and over who worked full year, full time and with employment income in 2010 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={},)

        # FIXME
        # As above, the ($) is redundant because of the unit.
        t013c053_t = OBSColumn(
            id='t013c053_t',
            name='Median employment income in 2010 ($) (total)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c052_t: UNIVERSE },)

        t013c053_m = OBSColumn(
            id='t013c053_m',
            name='Median employment income in 2010 ($) (male)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c052_m: UNIVERSE },)

        t013c053_f = OBSColumn(
            id='t013c053_f',
            name='Median employment income in 2010 ($) (female)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c052_f: UNIVERSE },)

        t013c054_t = OBSColumn(
            id='t013c054_t',
            name='Average employment income in 2010 ($) (total)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c052_t: UNIVERSE },)

        t013c054_m = OBSColumn(
            id='t013c054_m',
            name='Average employment income in 2010 ($) (male)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c052_m: UNIVERSE },)

        t013c054_f = OBSColumn(
            id='t013c054_f',
            name='Average employment income in 2010 ($) (female)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c052_f: UNIVERSE },)

        # FIXME
        # Is this a count of "Economic families"?  What does this mean --
        # families who reported economic data?
        #
        # Should the unit be families instead of people?
        t013c055_t = OBSColumn(
            id='t013c055_t',
            name='Family income in 2010 of economic families',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={},)

        # FIXME
        # The below need to be qualified as for the population in economic
        # families
        t013c056_t = OBSColumn(
            id='t013c056_t',
            name='Median family income ($)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c055_t: UNIVERSE },)

        t013c057_t = OBSColumn(
            id='t013c057_t',
            name='Average family income ($)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c055_t: UNIVERSE },)

        t013c058_t = OBSColumn(
            id='t013c058_t',
            name='Median after-tax family income ($)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c055_t: UNIVERSE },)

        t013c059_t = OBSColumn(
            id='t013c059_t',
            name='Average after-tax family income ($)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c055_t: UNIVERSE },)

        t013c060_t = OBSColumn(
            id='t013c060_t',
            name='Average family size',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c055_t: UNIVERSE },)

        t013c061_t = OBSColumn(
            id='t013c061_t',
            name='Couple-only economic families',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c055_t: DENOMINATOR },)

        # FIXME
        # The below need to be qualified as for the population in couple-only
        # economic families
        t013c062_t = OBSColumn(
            id='t013c062_t',
            name='Median family income ($)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c061_t: UNIVERSE },)

        t013c063_t = OBSColumn(
            id='t013c063_t',
            name='Average family income ($)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c061_t: UNIVERSE },)

        t013c064_t = OBSColumn(
            id='t013c064_t',
            name='Median after-tax family income ($)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c061_t: UNIVERSE },)

        t013c065_t = OBSColumn(
            id='t013c065_t',
            name='Average after-tax family income ($)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c061_t: UNIVERSE },)

        t013c066_t = OBSColumn(
            id='t013c066_t',
            name='Average family size',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c061_t: UNIVERSE },)

        t013c067_t = OBSColumn(
            id='t013c067_t',
            name='Couple-with-children economic families',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c055_t: DENOMINATOR },)

        # FIXME
        # The below need to be qualified as for the population in
        # couple-with-children economic families
        t013c068_t = OBSColumn(
            id='t013c068_t',
            name='Median family income ($)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c067_t: UNIVERSE },)

        t013c069_t = OBSColumn(
            id='t013c069_t',
            name='Average family income ($)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c067_t: UNIVERSE },)

        t013c070_t = OBSColumn(
            id='t013c070_t',
            name='Median after-tax family income ($)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c067_t: UNIVERSE },)

        t013c071_t = OBSColumn(
            id='t013c071_t',
            name='Average after-tax family income ($)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c067_t: UNIVERSE },)

        t013c072_t = OBSColumn(
            id='t013c072_t',
            name='Average family size',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c067_t: UNIVERSE },)

        t013c073_t = OBSColumn(
            id='t013c073_t',
            name='Lone-parent economic families',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c055_t: DENOMINATOR },)

        # FIXME
        # The below need to be qualified as for the population in
        # lone-parent economic families
        t013c074_t = OBSColumn(
            id='t013c074_t',
            name='Median family income ($)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c073_t: UNIVERSE },)

        t013c075_t = OBSColumn(
            id='t013c075_t',
            name='Average family income ($)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c073_t: UNIVERSE },)

        t013c076_t = OBSColumn(
            id='t013c076_t',
            name='Median after-tax family income ($)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c073_t: UNIVERSE },)

        t013c077_t = OBSColumn(
            id='t013c077_t',
            name='Average after-tax family income ($)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c073_t: UNIVERSE },)

        t013c078_t = OBSColumn(
            id='t013c078_t',
            name='Average family size',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c073_t: UNIVERSE },)

        t013c079_t = OBSColumn(
            id='t013c079_t',
            name='Income in 2010 of population aged 15 years and over not in economic families (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={},)

        t013c079_m = OBSColumn(
            id='t013c079_m',
            name='Income in 2010 of population aged 15 years and over not in economic families (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={},)

        t013c079_f = OBSColumn(
            id='t013c079_f',
            name='Income in 2010 of population aged 15 years and over not in economic families (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={},)

        # FIXME
        # The below need to be qualified as for the population not in economic
        # families
        t013c080_t = OBSColumn(
            id='t013c080_t',
            name='Median total income ($) (total)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c079_t: UNIVERSE },)

        t013c080_m = OBSColumn(
            id='t013c080_m',
            name='Median total income ($) (male)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c079_m: UNIVERSE },)

        t013c080_f = OBSColumn(
            id='t013c080_f',
            name='Median total income ($) (female)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c079_f: UNIVERSE },)

        t013c081_t = OBSColumn(
            id='t013c081_t',
            name='Average total income ($) (total)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c079_t: UNIVERSE },)

        t013c081_m = OBSColumn(
            id='t013c081_m',
            name='Average total income ($) (male)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c079_m: UNIVERSE },)

        t013c081_f = OBSColumn(
            id='t013c081_f',
            name='Average total income ($) (female)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c079_f: UNIVERSE },)

        t013c082_t = OBSColumn(
            id='t013c082_t',
            name='Median after-tax income ($) (total)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c079_t: UNIVERSE },)

        t013c082_m = OBSColumn(
            id='t013c082_m',
            name='Median after-tax income ($) (male)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c079_m: UNIVERSE },)

        t013c082_f = OBSColumn(
            id='t013c082_f',
            name='Median after-tax income ($) (female)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c079_f: UNIVERSE },)

        t013c083_t = OBSColumn(
            id='t013c083_t',
            name='Average after-tax income ($) (total)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c079_t: UNIVERSE },)

        t013c083_m = OBSColumn(
            id='t013c083_m',
            name='Average after-tax income ($) (male)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c079_m: UNIVERSE },)

        t013c083_f = OBSColumn(
            id='t013c083_f',
            name='Average after-tax income ($) (female)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['income']],
            targets={ t013c079_f: UNIVERSE },)

        # FIXME
        # These need a description of what the "Canadian distribution" is.
        t013c085_t = OBSColumn(
            id='t013c085_t',
            name='Total population - In bottom half of the Canadian distribution (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t001c001_t: DENOMINATOR },)

        t013c085_m = OBSColumn(
            id='t013c085_m',
            name='Total population - In bottom half of the Canadian distribution (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t001c001_m: DENOMINATOR },)

        t013c085_f = OBSColumn(
            id='t013c085_f',
            name='Total population - In bottom half of the Canadian distribution (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t001c001_f: DENOMINATOR },)

        # FIXME
        # The below need more clear names -- probably "In bottom decile of the Canadian distribution", etc.
        t013c086_t = OBSColumn(
            id='t013c086_t',
            name='In bottom decile (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c085_t: DENOMINATOR },)

        t013c086_m = OBSColumn(
            id='t013c086_m',
            name='In bottom decile (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c085_m: DENOMINATOR },)

        t013c086_f = OBSColumn(
            id='t013c086_f',
            name='In bottom decile (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c085_f: DENOMINATOR },)

        t013c087_t = OBSColumn(
            id='t013c087_t',
            name='In second decile (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c085_t: DENOMINATOR },)

        t013c087_m = OBSColumn(
            id='t013c087_m',
            name='In second decile (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c085_m: DENOMINATOR },)

        t013c087_f = OBSColumn(
            id='t013c087_f',
            name='In second decile (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c085_f: DENOMINATOR },)

        t013c088_t = OBSColumn(
            id='t013c088_t',
            name='In third decile (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c085_t: DENOMINATOR },)

        t013c088_m = OBSColumn(
            id='t013c088_m',
            name='In third decile (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c085_m: DENOMINATOR },)

        t013c088_f = OBSColumn(
            id='t013c088_f',
            name='In third decile (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c085_f: DENOMINATOR },)

        t013c089_t = OBSColumn(
            id='t013c089_t',
            name='In fourth decile (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c085_t: DENOMINATOR },)

        t013c089_m = OBSColumn(
            id='t013c089_m',
            name='In fourth decile (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c085_m: DENOMINATOR },)

        t013c089_f = OBSColumn(
            id='t013c089_f',
            name='In fourth decile (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c085_f: DENOMINATOR },)

        t013c090_t = OBSColumn(
            id='t013c090_t',
            name='In fifth decile (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c085_t: DENOMINATOR },)

        t013c090_m = OBSColumn(
            id='t013c090_m',
            name='In fifth decile (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c085_m: DENOMINATOR },)

        t013c090_f = OBSColumn(
            id='t013c090_f',
            name='In fifth decile (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c085_f: DENOMINATOR },)

        t013c091_t = OBSColumn(
            id='t013c091_t',
            name='Total population - In top half of the Canadian distribution (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t001c001_t: DENOMINATOR },)

        # FIXME
        # Same comments as for the bottom half and bottom decile columns
        t013c091_m = OBSColumn(
            id='t013c091_m',
            name='Total population - In top half of the Canadian distribution (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t001c001_m: DENOMINATOR },)

        t013c091_f = OBSColumn(
            id='t013c091_f',
            name='Total population - In top half of the Canadian distribution (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t001c001_f: DENOMINATOR },)

        t013c092_t = OBSColumn(
            id='t013c092_t',
            name='In sixth decile (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c091_t: DENOMINATOR },)

        t013c092_m = OBSColumn(
            id='t013c092_m',
            name='In sixth decile (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c091_m: DENOMINATOR },)

        t013c092_f = OBSColumn(
            id='t013c092_f',
            name='In sixth decile (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c091_f: DENOMINATOR },)

        t013c093_t = OBSColumn(
            id='t013c093_t',
            name='In seventh decile (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c091_t: DENOMINATOR },)

        t013c093_m = OBSColumn(
            id='t013c093_m',
            name='In seventh decile (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c091_m: DENOMINATOR },)

        t013c093_f = OBSColumn(
            id='t013c093_f',
            name='In seventh decile (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c091_f: DENOMINATOR },)

        t013c094_t = OBSColumn(
            id='t013c094_t',
            name='In eighth decile (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c091_t: DENOMINATOR },)

        t013c094_m = OBSColumn(
            id='t013c094_m',
            name='In eighth decile (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c091_m: DENOMINATOR },)

        t013c094_f = OBSColumn(
            id='t013c094_f',
            name='In eighth decile (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c091_f: DENOMINATOR },)

        t013c095_t = OBSColumn(
            id='t013c095_t',
            name='In ninth decile (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c091_t: DENOMINATOR },)

        t013c095_m = OBSColumn(
            id='t013c095_m',
            name='In ninth decile (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c091_m: DENOMINATOR },)

        t013c095_f = OBSColumn(
            id='t013c095_f',
            name='In ninth decile (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c091_f: DENOMINATOR },)

        t013c096_t = OBSColumn(
            id='t013c096_t',
            name='In top decile (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c091_t: DENOMINATOR },)

        t013c096_m = OBSColumn(
            id='t013c096_m',
            name='In top decile (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c091_m: DENOMINATOR },)

        t013c096_f = OBSColumn(
            id='t013c096_f',
            name='In top decile (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c091_f: DENOMINATOR },)

        # FIXME
        # What is being indicated in the columns below is unclear --
        # what does "for income status" mean, and why are the subcolumns ages?
        t013c097_t = OBSColumn(
            id='t013c097_t',
            name='Population in private households for income status (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={},)

        t013c097_m = OBSColumn(
            id='t013c097_m',
            name='Population in private households for income status (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={},)

        t013c097_f = OBSColumn(
            id='t013c097_f',
            name='Population in private households for income status (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={},)

        t013c098_t = OBSColumn(
            id='t013c098_t',
            name='Less than 18 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c097_t: DENOMINATOR },)

        t013c098_m = OBSColumn(
            id='t013c098_m',
            name='Less than 18 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c097_m: DENOMINATOR },)

        t013c098_f = OBSColumn(
            id='t013c098_f',
            name='Less than 18 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c097_f: DENOMINATOR },)

        t013c099_t = OBSColumn(
            id='t013c099_t',
            name='Less than 6 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c098_t: DENOMINATOR },)

        t013c099_m = OBSColumn(
            id='t013c099_m',
            name='Less than 6 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c098_m: DENOMINATOR },)

        t013c099_f = OBSColumn(
            id='t013c099_f',
            name='Less than 6 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c098_f: DENOMINATOR },)

        t013c100_t = OBSColumn(
            id='t013c100_t',
            name='18 to 64 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c097_t: DENOMINATOR },)

        t013c100_m = OBSColumn(
            id='t013c100_m',
            name='18 to 64 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c097_m: DENOMINATOR },)

        t013c100_f = OBSColumn(
            id='t013c100_f',
            name='18 to 64 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c097_f: DENOMINATOR },)

        t013c101_t = OBSColumn(
            id='t013c101_t',
            name='65 years and over (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c097_t: DENOMINATOR },)

        t013c101_m = OBSColumn(
            id='t013c101_m',
            name='65 years and over (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c097_m: DENOMINATOR },)

        t013c101_f = OBSColumn(
            id='t013c101_f',
            name='65 years and over (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c097_f: DENOMINATOR },)

        # FIXME
        # These need a description or link to description of LIM-AT.
        t013c102_t = OBSColumn(
            id='t013c102_t',
            name='In low income in 2010 based on after-tax low-income measure (LIM-AT) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={},)

        t013c102_m = OBSColumn(
            id='t013c102_m',
            name='In low income in 2010 based on after-tax low-income measure (LIM-AT) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={},)

        t013c102_f = OBSColumn(
            id='t013c102_f',
            name='In low income in 2010 based on after-tax low-income measure (LIM-AT) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={},)

        # FIXME the below need to be qualified as to what the age means --
        # is it the age of the head of household for a household in LIM-AT?
        # It must be something else, since less than 6 years is one of the
        # measures.
        t013c103_t = OBSColumn(
            id='t013c103_t',
            name='Less than 18 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c102_t: DENOMINATOR },)

        t013c103_m = OBSColumn(
            id='t013c103_m',
            name='Less than 18 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c102_m: DENOMINATOR },)

        t013c103_f = OBSColumn(
            id='t013c103_f',
            name='Less than 18 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c102_f: DENOMINATOR },)

        t013c104_t = OBSColumn(
            id='t013c104_t',
            name='Less than 6 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c103_t: DENOMINATOR },)

        t013c104_m = OBSColumn(
            id='t013c104_m',
            name='Less than 6 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c103_m: DENOMINATOR },)

        t013c104_f = OBSColumn(
            id='t013c104_f',
            name='Less than 6 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c103_f: DENOMINATOR },)

        t013c105_t = OBSColumn(
            id='t013c105_t',
            name='18 to 64 years (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c102_t: DENOMINATOR },)

        t013c105_m = OBSColumn(
            id='t013c105_m',
            name='18 to 64 years (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c102_m: DENOMINATOR },)

        t013c105_f = OBSColumn(
            id='t013c105_f',
            name='18 to 64 years (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c102_f: DENOMINATOR },)

        t013c106_t = OBSColumn(
            id='t013c106_t',
            name='65 years and over (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c102_t: DENOMINATOR },)

        t013c106_m = OBSColumn(
            id='t013c106_m',
            name='65 years and over (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c102_m: DENOMINATOR },)

        t013c106_f = OBSColumn(
            id='t013c106_f',
            name='65 years and over (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['income']],
            targets={ t013c102_f: DENOMINATOR },)

        t013c107_t = OBSColumn(
            id='t013c107_t',
            name='Prevalence of low income in 2010 based on after-tax low-income measure (%) (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={},)

        t013c107_m = OBSColumn(
            id='t013c107_m',
            name='Prevalence of low income in 2010 based on after-tax low-income measure (%) (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={},)

        t013c107_f = OBSColumn(
            id='t013c107_f',
            name='Prevalence of low income in 2010 based on after-tax low-income measure (%) (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={},)

        # FIXME
        # Names below need to clearly indicate that they refer to the
        # percentage of the population in an age band in poverty.
        t013c108_t = OBSColumn(
            id='t013c108_t',
            name='Less than 18 years (%) (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c107_t: UNIVERSE },)

        t013c108_m = OBSColumn(
            id='t013c108_m',
            name='Less than 18 years (%) (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c107_m: UNIVERSE },)

        t013c108_f = OBSColumn(
            id='t013c108_f',
            name='Less than 18 years (%) (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c107_f: UNIVERSE },)

        t013c109_t = OBSColumn(
            id='t013c109_t',
            name='Less than 6 years (%) (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c108_t: UNIVERSE },)

        t013c109_m = OBSColumn(
            id='t013c109_m',
            name='Less than 6 years (%) (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c108_m: UNIVERSE },)

        t013c109_f = OBSColumn(
            id='t013c109_f',
            name='Less than 6 years (%) (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c108_f: UNIVERSE },)

        t013c110_t = OBSColumn(
            id='t013c110_t',
            name='18 to 64 years (%) (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c107_t: UNIVERSE },)

        t013c110_m = OBSColumn(
            id='t013c110_m',
            name='18 to 64 years (%) (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c107_m: UNIVERSE },)

        t013c110_f = OBSColumn(
            id='t013c110_f',
            name='18 to 64 years (%) (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c107_f: UNIVERSE },)

        t013c111_t = OBSColumn(
            id='t013c111_t',
            name='65 years and over (%) (total)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c107_t: UNIVERSE },)

        t013c111_m = OBSColumn(
            id='t013c111_m',
            name='65 years and over (%) (male)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c107_m: UNIVERSE },)

        t013c111_f = OBSColumn(
            id='t013c111_f',
            name='65 years and over (%) (female)',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['income']],
            targets={ t013c107_f: UNIVERSE },)

        t014c002_t = OBSColumn(
            id='t014c002_t',
            name='People aged 15+ in the labour force - Industry - not applicable (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_t: DENOMINATOR },)

        t014c002_m = OBSColumn(
            id='t014c002_m',
            name='People aged 15+ in the labour force - Industry - not applicable (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_m: DENOMINATOR },)

        t014c002_f = OBSColumn(
            id='t014c002_f',
            name='People aged 15+ in the labour force - Industry - not applicable (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_f: DENOMINATOR },)

        t014c003_t = OBSColumn(
            id='t014c003_t',
            name='People aged 15+ in the labour force - All industries (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_t: DENOMINATOR },)

        t014c003_m = OBSColumn(
            id='t014c003_m',
            name='People aged 15+ in the labour force - All industries (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_m: DENOMINATOR },)

        t014c003_f = OBSColumn(
            id='t014c003_f',
            name='People aged 15+ in the labour force - All industries (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_f: DENOMINATOR },)

        #FIXME
        # Below should be more clearly named "Labour force employeed in
        # Agriculture, forestry, fishing and hunting (total)", etc.
        #
        # The NAICS code could be mentioned in the description.
        t014c004_t = OBSColumn(
            id='t014c004_t',
            name='11 Agriculture, forestry, fishing and hunting (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_t: DENOMINATOR },)

        t014c004_m = OBSColumn(
            id='t014c004_m',
            name='11 Agriculture, forestry, fishing and hunting (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_m: DENOMINATOR },)

        t014c004_f = OBSColumn(
            id='t014c004_f',
            name='11 Agriculture, forestry, fishing and hunting (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_f: DENOMINATOR },)

        t014c005_t = OBSColumn(
            id='t014c005_t',
            name='21 Mining, quarrying, and oil and gas extraction (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_t: DENOMINATOR },)

        t014c005_m = OBSColumn(
            id='t014c005_m',
            name='21 Mining, quarrying, and oil and gas extraction (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_m: DENOMINATOR },)

        t014c005_f = OBSColumn(
            id='t014c005_f',
            name='21 Mining, quarrying, and oil and gas extraction (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_f: DENOMINATOR },)

        t014c006_t = OBSColumn(
            id='t014c006_t',
            name='22 Utilities (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_t: DENOMINATOR },)

        t014c006_m = OBSColumn(
            id='t014c006_m',
            name='22 Utilities (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_m: DENOMINATOR },)

        t014c006_f = OBSColumn(
            id='t014c006_f',
            name='22 Utilities (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_f: DENOMINATOR },)

        t014c007_t = OBSColumn(
            id='t014c007_t',
            name='23 Construction (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_t: DENOMINATOR },)

        t014c007_m = OBSColumn(
            id='t014c007_m',
            name='23 Construction (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_m: DENOMINATOR },)

        t014c007_f = OBSColumn(
            id='t014c007_f',
            name='23 Construction (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_f: DENOMINATOR },)

        t014c008_t = OBSColumn(
            id='t014c008_t',
            name='31-33 Manufacturing (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_t: DENOMINATOR },)

        t014c008_m = OBSColumn(
            id='t014c008_m',
            name='31-33 Manufacturing (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_m: DENOMINATOR },)

        t014c008_f = OBSColumn(
            id='t014c008_f',
            name='31-33 Manufacturing (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_f: DENOMINATOR },)

        t014c009_t = OBSColumn(
            id='t014c009_t',
            name='41 Wholesale trade (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_t: DENOMINATOR },)

        t014c009_m = OBSColumn(
            id='t014c009_m',
            name='41 Wholesale trade (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_m: DENOMINATOR },)

        t014c009_f = OBSColumn(
            id='t014c009_f',
            name='41 Wholesale trade (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_f: DENOMINATOR },)

        t014c010_t = OBSColumn(
            id='t014c010_t',
            name='44-45 Retail trade (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_t: DENOMINATOR },)

        t014c010_m = OBSColumn(
            id='t014c010_m',
            name='44-45 Retail trade (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_m: DENOMINATOR },)

        t014c010_f = OBSColumn(
            id='t014c010_f',
            name='44-45 Retail trade (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_f: DENOMINATOR },)

        t014c011_t = OBSColumn(
            id='t014c011_t',
            name='48-49 Transportation and warehousing (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_t: DENOMINATOR },)

        t014c011_m = OBSColumn(
            id='t014c011_m',
            name='48-49 Transportation and warehousing (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_m: DENOMINATOR },)

        t014c011_f = OBSColumn(
            id='t014c011_f',
            name='48-49 Transportation and warehousing (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_f: DENOMINATOR },)

        t014c012_t = OBSColumn(
            id='t014c012_t',
            name='51 Information and cultural industries (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_t: DENOMINATOR },)

        t014c012_m = OBSColumn(
            id='t014c012_m',
            name='51 Information and cultural industries (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_m: DENOMINATOR },)

        t014c012_f = OBSColumn(
            id='t014c012_f',
            name='51 Information and cultural industries (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_f: DENOMINATOR },)

        t014c013_t = OBSColumn(
            id='t014c013_t',
            name='52 Finance and insurance (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_t: DENOMINATOR },)

        t014c013_m = OBSColumn(
            id='t014c013_m',
            name='52 Finance and insurance (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_m: DENOMINATOR },)

        t014c013_f = OBSColumn(
            id='t014c013_f',
            name='52 Finance and insurance (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_f: DENOMINATOR },)

        t014c014_t = OBSColumn(
            id='t014c014_t',
            name='53 Real estate and rental and leasing (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_t: DENOMINATOR },)

        t014c014_m = OBSColumn(
            id='t014c014_m',
            name='53 Real estate and rental and leasing (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_m: DENOMINATOR },)

        t014c014_f = OBSColumn(
            id='t014c014_f',
            name='53 Real estate and rental and leasing (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_f: DENOMINATOR },)

        t014c015_t = OBSColumn(
            id='t014c015_t',
            name='54 Professional, scientific and technical services (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_t: DENOMINATOR },)

        t014c015_m = OBSColumn(
            id='t014c015_m',
            name='54 Professional, scientific and technical services (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_m: DENOMINATOR },)

        t014c015_f = OBSColumn(
            id='t014c015_f',
            name='54 Professional, scientific and technical services (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_f: DENOMINATOR },)

        t014c016_t = OBSColumn(
            id='t014c016_t',
            name='55 Management of companies and enterprises (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_t: DENOMINATOR },)

        t014c016_m = OBSColumn(
            id='t014c016_m',
            name='55 Management of companies and enterprises (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_m: DENOMINATOR },)

        t014c016_f = OBSColumn(
            id='t014c016_f',
            name='55 Management of companies and enterprises (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_f: DENOMINATOR },)

        t014c017_t = OBSColumn(
            id='t014c017_t',
            name='56 Administrative and support, waste management and remediation services (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_t: DENOMINATOR },)

        t014c017_m = OBSColumn(
            id='t014c017_m',
            name='56 Administrative and support, waste management and remediation services (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_m: DENOMINATOR },)

        t014c017_f = OBSColumn(
            id='t014c017_f',
            name='56 Administrative and support, waste management and remediation services (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_f: DENOMINATOR },)

        t014c018_t = OBSColumn(
            id='t014c018_t',
            name='61 Educational services (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_t: DENOMINATOR },)

        t014c018_m = OBSColumn(
            id='t014c018_m',
            name='61 Educational services (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_m: DENOMINATOR },)

        t014c018_f = OBSColumn(
            id='t014c018_f',
            name='61 Educational services (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_f: DENOMINATOR },)

        t014c019_t = OBSColumn(
            id='t014c019_t',
            name='62 Health care and social assistance (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_t: DENOMINATOR },)

        t014c019_m = OBSColumn(
            id='t014c019_m',
            name='62 Health care and social assistance (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_m: DENOMINATOR },)

        t014c019_f = OBSColumn(
            id='t014c019_f',
            name='62 Health care and social assistance (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_f: DENOMINATOR },)

        t014c020_t = OBSColumn(
            id='t014c020_t',
            name='71 Arts, entertainment and recreation (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_t: DENOMINATOR },)

        t014c020_m = OBSColumn(
            id='t014c020_m',
            name='71 Arts, entertainment and recreation (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_m: DENOMINATOR },)

        t014c020_f = OBSColumn(
            id='t014c020_f',
            name='71 Arts, entertainment and recreation (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_f: DENOMINATOR },)

        t014c021_t = OBSColumn(
            id='t014c021_t',
            name='72 Accommodation and food services (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_t: DENOMINATOR },)

        t014c021_m = OBSColumn(
            id='t014c021_m',
            name='72 Accommodation and food services (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_m: DENOMINATOR },)

        t014c021_f = OBSColumn(
            id='t014c021_f',
            name='72 Accommodation and food services (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_f: DENOMINATOR },)

        t014c022_t = OBSColumn(
            id='t014c022_t',
            name='81 Other services (except public administration) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_t: DENOMINATOR },)

        t014c022_m = OBSColumn(
            id='t014c022_m',
            name='81 Other services (except public administration) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_m: DENOMINATOR },)

        t014c022_f = OBSColumn(
            id='t014c022_f',
            name='81 Other services (except public administration) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_f: DENOMINATOR },)

        t014c023_t = OBSColumn(
            id='t014c023_t',
            name='91 Public administration (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_t: DENOMINATOR },)

        t014c023_m = OBSColumn(
            id='t014c023_m',
            name='91 Public administration (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_m: DENOMINATOR },)

        t014c023_f = OBSColumn(
            id='t014c023_f',
            name='91 Public administration (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t014c003_f: DENOMINATOR },)

        t015c002_t = OBSColumn(
            id='t015c002_t',
            name='People aged 15+ - In the labour force (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t005c001_t: DENOMINATOR },)

        t015c002_m = OBSColumn(
            id='t015c002_m',
            name='People aged 15+ - In the labour force (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t005c001_m: DENOMINATOR },)

        t015c002_f = OBSColumn(
            id='t015c002_f',
            name='People aged 15+ - In the labour force (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t005c001_f: DENOMINATOR },)

        t015c003_t = OBSColumn(
            id='t015c003_t',
            name='Employed (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t015c002_t: DENOMINATOR },)

        t015c003_m = OBSColumn(
            id='t015c003_m',
            name='Employed (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t015c002_m: DENOMINATOR },)

        t015c003_f = OBSColumn(
            id='t015c003_f',
            name='Employed (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t015c002_f: DENOMINATOR },)

        t015c004_t = OBSColumn(
            id='t015c004_t',
            name='Unemployed (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t015c002_t: DENOMINATOR },)

        t015c004_m = OBSColumn(
            id='t015c004_m',
            name='Unemployed (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t015c002_m: DENOMINATOR },)

        t015c004_f = OBSColumn(
            id='t015c004_f',
            name='Unemployed (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t015c002_f: DENOMINATOR },)

        t015c005_t = OBSColumn(
            id='t015c005_t',
            name='People aged 15+ - Not in the labour force (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t005c001_t: DENOMINATOR },)

        t015c005_m = OBSColumn(
            id='t015c005_m',
            name='People aged 15+ - Not in the labour force (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t005c001_m: DENOMINATOR },)

        t015c005_f = OBSColumn(
            id='t015c005_f',
            name='People aged 15+ - Not in the labour force (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t005c001_f: DENOMINATOR },)

        # FIXME
        # Needs clearer name that this is the participation rate in the labour
        # fource (if that's what it is).
        t015c006_t = OBSColumn(
            id='t015c006_t',
            name='Participation rate (total)',
            type='Numeric',
            weight=3,
            aggregate='percent',
            tags=[ca, unit_ratio, subsections['employment']],
            targets={},)

        t015c006_m = OBSColumn(
            id='t015c006_m',
            name='Participation rate (male)',
            type='Numeric',
            weight=3,
            aggregate='percent',
            tags=[ca, unit_ratio, subsections['employment']],
            targets={},)

        t015c006_f = OBSColumn(
            id='t015c006_f',
            name='Participation rate (female)',
            type='Numeric',
            weight=3,
            aggregate='percent',
            tags=[ca, unit_ratio, subsections['employment']],
            targets={},)

        t015c007_t = OBSColumn(
            id='t015c007_t',
            name='Employment rate (total)',
            type='Numeric',
            weight=3,
            aggregate='percent',
            tags=[ca, unit_ratio, subsections['employment']],
            targets={},)

        t015c007_m = OBSColumn(
            id='t015c007_m',
            name='Employment rate (male)',
            type='Numeric',
            weight=3,
            aggregate='percent',
            tags=[ca, unit_ratio, subsections['employment']],
            targets={},)

        t015c007_f = OBSColumn(
            id='t015c007_f',
            name='Employment rate (female)',
            type='Numeric',
            weight=3,
            aggregate='percent',
            tags=[ca, unit_ratio, subsections['employment']],
            targets={},)

        t015c008_t = OBSColumn(
            id='t015c008_t',
            name='Unemployment rate (total)',
            type='Numeric',
            weight=3,
            aggregate='percent',
            tags=[ca, unit_ratio, subsections['employment']],
            targets={},)

        t015c008_m = OBSColumn(
            id='t015c008_m',
            name='Unemployment rate (male)',
            type='Numeric',
            weight=3,
            aggregate='percent',
            tags=[ca, unit_ratio, subsections['employment']],
            targets={},)

        t015c008_f = OBSColumn(
            id='t015c008_f',
            name='Unemployment rate (female)',
            type='Numeric',
            weight=3,
            aggregate='percent',
            tags=[ca, unit_ratio, subsections['employment']],
            targets={},)

        # FIXME
        # The below should be eliminated, and instead the more standard
        # "Population aged 15 years and over" used instead.
        t016c001_t = OBSColumn(
            id='t016c001_t',
            name='Total population aged 15 years and over by language used most often at work (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={},)

        t016c001_m = OBSColumn(
            id='t016c001_m',
            name='Total population aged 15 years and over by language used most often at work (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={},)

        t016c001_f = OBSColumn(
            id='t016c001_f',
            name='Total population aged 15 years and over by language used most often at work (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={},)

        # FIXME
        # The below should have clearer names, specifying that this is the
        # language they "use most often at work".
        t016c002_t = OBSColumn(
            id='t016c002_t',
            name='Single responses (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c001_t: DENOMINATOR },)

        t016c002_m = OBSColumn(
            id='t016c002_m',
            name='Single responses (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c001_m: DENOMINATOR },)

        t016c002_f = OBSColumn(
            id='t016c002_f',
            name='Single responses (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c001_f: DENOMINATOR },)

        t016c003_t = OBSColumn(
            id='t016c003_t',
            name='English (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c002_t: DENOMINATOR },)

        t016c003_m = OBSColumn(
            id='t016c003_m',
            name='English (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c002_m: DENOMINATOR },)

        t016c003_f = OBSColumn(
            id='t016c003_f',
            name='English (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c002_f: DENOMINATOR },)

        t016c004_t = OBSColumn(
            id='t016c004_t',
            name='French (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c002_t: DENOMINATOR },)

        t016c004_m = OBSColumn(
            id='t016c004_m',
            name='French (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c002_m: DENOMINATOR },)

        t016c004_f = OBSColumn(
            id='t016c004_f',
            name='French (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c002_f: DENOMINATOR },)

        t016c005_t = OBSColumn(
            id='t016c005_t',
            name='Non-official languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c002_t: DENOMINATOR },)

        t016c005_m = OBSColumn(
            id='t016c005_m',
            name='Non-official languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c002_m: DENOMINATOR },)

        t016c005_f = OBSColumn(
            id='t016c005_f',
            name='Non-official languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c002_f: DENOMINATOR },)

        t016c006_t = OBSColumn(
            id='t016c006_t',
            name='Chinese, n.o.s. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_t: DENOMINATOR },)

        t016c006_m = OBSColumn(
            id='t016c006_m',
            name='Chinese, n.o.s. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_m: DENOMINATOR },)

        t016c006_f = OBSColumn(
            id='t016c006_f',
            name='Chinese, n.o.s. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_f: DENOMINATOR },)

        t016c007_t = OBSColumn(
            id='t016c007_t',
            name='Cantonese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_t: DENOMINATOR },)

        t016c007_m = OBSColumn(
            id='t016c007_m',
            name='Cantonese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_m: DENOMINATOR },)

        t016c007_f = OBSColumn(
            id='t016c007_f',
            name='Cantonese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_f: DENOMINATOR },)

        t016c008_t = OBSColumn(
            id='t016c008_t',
            name='Panjabi (Punjabi) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_t: DENOMINATOR },)

        t016c008_m = OBSColumn(
            id='t016c008_m',
            name='Panjabi (Punjabi) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_m: DENOMINATOR },)

        t016c008_f = OBSColumn(
            id='t016c008_f',
            name='Panjabi (Punjabi) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_f: DENOMINATOR },)

        t016c009_t = OBSColumn(
            id='t016c009_t',
            name='Mandarin (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_t: DENOMINATOR },)

        t016c009_m = OBSColumn(
            id='t016c009_m',
            name='Mandarin (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_m: DENOMINATOR },)

        t016c009_f = OBSColumn(
            id='t016c009_f',
            name='Mandarin (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_f: DENOMINATOR },)

        t016c010_t = OBSColumn(
            id='t016c010_t',
            name='Spanish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_t: DENOMINATOR },)

        t016c010_m = OBSColumn(
            id='t016c010_m',
            name='Spanish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_m: DENOMINATOR },)

        t016c010_f = OBSColumn(
            id='t016c010_f',
            name='Spanish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_f: DENOMINATOR },)

        t016c011_t = OBSColumn(
            id='t016c011_t',
            name='Korean (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_t: DENOMINATOR },)

        t016c011_m = OBSColumn(
            id='t016c011_m',
            name='Korean (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_m: DENOMINATOR },)

        t016c011_f = OBSColumn(
            id='t016c011_f',
            name='Korean (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_f: DENOMINATOR },)

        t016c012_t = OBSColumn(
            id='t016c012_t',
            name='German (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_t: DENOMINATOR },)

        t016c012_m = OBSColumn(
            id='t016c012_m',
            name='German (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_m: DENOMINATOR },)

        t016c012_f = OBSColumn(
            id='t016c012_f',
            name='German (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_f: DENOMINATOR },)

        t016c013_t = OBSColumn(
            id='t016c013_t',
            name='Cree languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_t: DENOMINATOR },)

        t016c013_m = OBSColumn(
            id='t016c013_m',
            name='Cree languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_m: DENOMINATOR },)

        t016c013_f = OBSColumn(
            id='t016c013_f',
            name='Cree languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_f: DENOMINATOR },)

        t016c014_t = OBSColumn(
            id='t016c014_t',
            name='Portuguese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_t: DENOMINATOR },)

        t016c014_m = OBSColumn(
            id='t016c014_m',
            name='Portuguese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_m: DENOMINATOR },)

        t016c014_f = OBSColumn(
            id='t016c014_f',
            name='Portuguese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_f: DENOMINATOR },)

        t016c015_t = OBSColumn(
            id='t016c015_t',
            name='Inuktitut (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_t: DENOMINATOR },)

        t016c015_m = OBSColumn(
            id='t016c015_m',
            name='Inuktitut (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_m: DENOMINATOR },)

        t016c015_f = OBSColumn(
            id='t016c015_f',
            name='Inuktitut (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_f: DENOMINATOR },)

        t016c016_t = OBSColumn(
            id='t016c016_t',
            name='Other languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_t: DENOMINATOR },)

        t016c016_m = OBSColumn(
            id='t016c016_m',
            name='Other languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_m: DENOMINATOR },)

        t016c016_f = OBSColumn(
            id='t016c016_f',
            name='Other languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c005_f: DENOMINATOR },)

        t016c017_t = OBSColumn(
            id='t016c017_t',
            name='Multiple responses (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c001_t: DENOMINATOR },)

        t016c017_m = OBSColumn(
            id='t016c017_m',
            name='Multiple responses (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c001_m: DENOMINATOR },)

        t016c017_f = OBSColumn(
            id='t016c017_f',
            name='Multiple responses (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c001_f: DENOMINATOR },)

        t016c018_t = OBSColumn(
            id='t016c018_t',
            name='English and French (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c017_t: DENOMINATOR },)

        t016c018_m = OBSColumn(
            id='t016c018_m',
            name='English and French (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c017_m: DENOMINATOR },)

        t016c018_f = OBSColumn(
            id='t016c018_f',
            name='English and French (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c017_f: DENOMINATOR },)

        t016c019_t = OBSColumn(
            id='t016c019_t',
            name='English and non-official language (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c017_t: DENOMINATOR },)

        t016c019_m = OBSColumn(
            id='t016c019_m',
            name='English and non-official language (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c017_m: DENOMINATOR },)

        t016c019_f = OBSColumn(
            id='t016c019_f',
            name='English and non-official language (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c017_f: DENOMINATOR },)

        t016c020_t = OBSColumn(
            id='t016c020_t',
            name='French and non-official language (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c017_t: DENOMINATOR },)

        t016c020_m = OBSColumn(
            id='t016c020_m',
            name='French and non-official language (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c017_m: DENOMINATOR },)

        t016c020_f = OBSColumn(
            id='t016c020_f',
            name='French and non-official language (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c017_f: DENOMINATOR },)

        t016c021_t = OBSColumn(
            id='t016c021_t',
            name='English, French and non-official language (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c017_t: DENOMINATOR },)

        t016c021_m = OBSColumn(
            id='t016c021_m',
            name='English, French and non-official language (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c017_m: DENOMINATOR },)

        t016c021_f = OBSColumn(
            id='t016c021_f',
            name='English, French and non-official language (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c017_f: DENOMINATOR },)

        # FIXME
        # How are the below different than t016c001_t and the following?
        # Names are the same, and subcolumns look identical.
        t016c022_t = OBSColumn(
            id='t016c022_t',
            name='Total population aged 15 years and over by language used most often at work (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={},)

        t016c022_m = OBSColumn(
            id='t016c022_m',
            name='Total population aged 15 years and over by language used most often at work (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={},)

        t016c022_f = OBSColumn(
            id='t016c022_f',
            name='Total population aged 15 years and over by language used most often at work (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={},)

        t016c023_t = OBSColumn(
            id='t016c023_t',
            name='English (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_t: DENOMINATOR },)

        t016c023_m = OBSColumn(
            id='t016c023_m',
            name='English (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_m: DENOMINATOR },)

        t016c023_f = OBSColumn(
            id='t016c023_f',
            name='English (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_f: DENOMINATOR },)

        t016c024_t = OBSColumn(
            id='t016c024_t',
            name='French (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_t: DENOMINATOR },)

        t016c024_m = OBSColumn(
            id='t016c024_m',
            name='French (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_m: DENOMINATOR },)

        t016c024_f = OBSColumn(
            id='t016c024_f',
            name='French (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_f: DENOMINATOR },)

        t016c025_t = OBSColumn(
            id='t016c025_t',
            name='Non-official language (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_t: DENOMINATOR },)

        t016c025_m = OBSColumn(
            id='t016c025_m',
            name='Non-official language (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_m: DENOMINATOR },)

        t016c025_f = OBSColumn(
            id='t016c025_f',
            name='Non-official language (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_f: DENOMINATOR },)

        t016c026_t = OBSColumn(
            id='t016c026_t',
            name='Aboriginal (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c025_t: DENOMINATOR },)

        t016c026_m = OBSColumn(
            id='t016c026_m',
            name='Aboriginal (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c025_m: DENOMINATOR },)

        t016c026_f = OBSColumn(
            id='t016c026_f',
            name='Aboriginal (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c025_f: DENOMINATOR },)

        t016c027_t = OBSColumn(
            id='t016c027_t',
            name='Non-Aboriginal (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c025_t: DENOMINATOR },)

        t016c027_m = OBSColumn(
            id='t016c027_m',
            name='Non-Aboriginal (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c025_m: DENOMINATOR },)

        t016c027_f = OBSColumn(
            id='t016c027_f',
            name='Non-Aboriginal (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c025_f: DENOMINATOR },)

        t016c028_t = OBSColumn(
            id='t016c028_t',
            name='English and French (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_t: DENOMINATOR },)

        t016c028_m = OBSColumn(
            id='t016c028_m',
            name='English and French (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_m: DENOMINATOR },)

        t016c028_f = OBSColumn(
            id='t016c028_f',
            name='English and French (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_f: DENOMINATOR },)

        t016c029_t = OBSColumn(
            id='t016c029_t',
            name='English and non-official language (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_t: DENOMINATOR },)

        t016c029_m = OBSColumn(
            id='t016c029_m',
            name='English and non-official language (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_m: DENOMINATOR },)

        t016c029_f = OBSColumn(
            id='t016c029_f',
            name='English and non-official language (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_f: DENOMINATOR },)

        t016c030_t = OBSColumn(
            id='t016c030_t',
            name='French and non-official language (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_t: DENOMINATOR },)

        t016c030_m = OBSColumn(
            id='t016c030_m',
            name='French and non-official language (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_m: DENOMINATOR },)

        t016c030_f = OBSColumn(
            id='t016c030_f',
            name='French and non-official language (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_f: DENOMINATOR },)

        t016c031_t = OBSColumn(
            id='t016c031_t',
            name='English, French and non-official language (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_t: DENOMINATOR },)

        t016c031_m = OBSColumn(
            id='t016c031_m',
            name='English, French and non-official language (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_m: DENOMINATOR },)

        t016c031_f = OBSColumn(
            id='t016c031_f',
            name='English, French and non-official language (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t016c022_f: DENOMINATOR },)

        t017c001_t = OBSColumn(
            id='t017c001_t',
            name='People aged 15+ who commute to work (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={},)

        t017c001_m = OBSColumn(
            id='t017c001_m',
            name='People aged 15+ who commute to work (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={},)

        t017c001_f = OBSColumn(
            id='t017c001_f',
            name='People aged 15+ who commute to work (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={},)

        t017c002_t = OBSColumn(
            id='t017c002_t',
            name='Median commuting duration for people aged 15+ who commute to work (total)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_minutes, subsections['transportation']],
            targets={},)

        t017c002_m = OBSColumn(
            id='t017c002_m',
            name='Median commuting duration for people aged 15+ who commute to work (male)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_minutes, subsections['transportation']],
            targets={},)

        t017c002_f = OBSColumn(
            id='t017c002_f',
            name='Median commuting duration for people aged 15+ who commute to work (female)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_minutes, subsections['transportation']],
            targets={},)

        # FIXME
        # This should be replaced with a clearer name, and possibly eliminated
        # if the population used for move status overlaps with another, simpler
        # to understand column.
        t018c001_t = OBSColumn(
            id='t018c001_t',
            name='Total - Mobility status 1 year ago (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={},)

        t018c001_m = OBSColumn(
            id='t018c001_m',
            name='Total - Mobility status 1 year ago (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={},)

        t018c001_f = OBSColumn(
            id='t018c001_f',
            name='Total - Mobility status 1 year ago (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={},)

        # FIXME
        # The below need slightly clearer names, and possibly descriptions
        # about what "Movers" and "Non-movers" are -- are they people who
        # simply had some kind of change of dwelling place, or is it more
        # specific that they moved from one region to another?
        #
        # They also need to specify this is status as of 1 year ago.
        #
        # The same applies for migrants/non-migrants.
        t018c002_t = OBSColumn(
            id='t018c002_t',
            name='Non-movers (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c001_t: DENOMINATOR },)

        t018c002_m = OBSColumn(
            id='t018c002_m',
            name='Non-movers (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c001_m: DENOMINATOR },)

        t018c002_f = OBSColumn(
            id='t018c002_f',
            name='Non-movers (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c001_f: DENOMINATOR },)

        t018c003_t = OBSColumn(
            id='t018c003_t',
            name='Movers (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c001_t: DENOMINATOR },)

        t018c003_m = OBSColumn(
            id='t018c003_m',
            name='Movers (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c001_m: DENOMINATOR },)

        t018c003_f = OBSColumn(
            id='t018c003_f',
            name='Movers (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c001_f: DENOMINATOR },)

        t018c004_t = OBSColumn(
            id='t018c004_t',
            name='Non-migrants (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c003_t: DENOMINATOR },)

        t018c004_m = OBSColumn(
            id='t018c004_m',
            name='Non-migrants (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c003_m: DENOMINATOR },)

        t018c004_f = OBSColumn(
            id='t018c004_f',
            name='Non-migrants (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c003_f: DENOMINATOR },)

        t018c005_t = OBSColumn(
            id='t018c005_t',
            name='Migrants (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c003_t: DENOMINATOR },)

        t018c005_m = OBSColumn(
            id='t018c005_m',
            name='Migrants (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c003_m: DENOMINATOR },)

        t018c005_f = OBSColumn(
            id='t018c005_f',
            name='Migrants (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c003_f: DENOMINATOR },)

        t018c006_t = OBSColumn(
            id='t018c006_t',
            name='Internal migrants (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c005_t: DENOMINATOR },)

        t018c006_m = OBSColumn(
            id='t018c006_m',
            name='Internal migrants (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c005_m: DENOMINATOR },)

        t018c006_f = OBSColumn(
            id='t018c006_f',
            name='Internal migrants (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c005_f: DENOMINATOR },)

        t018c007_t = OBSColumn(
            id='t018c007_t',
            name='Intraprovincial migrants (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c006_t: DENOMINATOR },)

        t018c007_m = OBSColumn(
            id='t018c007_m',
            name='Intraprovincial migrants (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c006_m: DENOMINATOR },)

        t018c007_f = OBSColumn(
            id='t018c007_f',
            name='Intraprovincial migrants (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c006_f: DENOMINATOR },)

        t018c008_t = OBSColumn(
            id='t018c008_t',
            name='Interprovincial migrants (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c006_t: DENOMINATOR },)

        t018c008_m = OBSColumn(
            id='t018c008_m',
            name='Interprovincial migrants (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c006_m: DENOMINATOR },)

        t018c008_f = OBSColumn(
            id='t018c008_f',
            name='Interprovincial migrants (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c006_f: DENOMINATOR },)

        t018c009_t = OBSColumn(
            id='t018c009_t',
            name='External migrants (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c005_t: DENOMINATOR },)

        t018c009_m = OBSColumn(
            id='t018c009_m',
            name='External migrants (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c005_m: DENOMINATOR },)

        t018c009_f = OBSColumn(
            id='t018c009_f',
            name='External migrants (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c005_f: DENOMINATOR },)

        # FIXME
        # Same comments as above for move status as of 1 year ago.
        t018c010_t = OBSColumn(
            id='t018c010_t',
            name='Total - Mobility status 5 years ago (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={},)

        t018c010_m = OBSColumn(
            id='t018c010_m',
            name='Total - Mobility status 5 years ago (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={},)

        t018c010_f = OBSColumn(
            id='t018c010_f',
            name='Total - Mobility status 5 years ago (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={},)

        # FIXME
        # The below columns need names that specify the status is as of where
        # they were 5 years ago.
        t018c011_t = OBSColumn(
            id='t018c011_t',
            name='Non-movers (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c010_t: DENOMINATOR },)

        t018c011_m = OBSColumn(
            id='t018c011_m',
            name='Non-movers (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c010_m: DENOMINATOR },)

        t018c011_f = OBSColumn(
            id='t018c011_f',
            name='Non-movers (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c010_f: DENOMINATOR },)

        t018c012_t = OBSColumn(
            id='t018c012_t',
            name='Movers (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c010_t: DENOMINATOR },)

        t018c012_m = OBSColumn(
            id='t018c012_m',
            name='Movers (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c010_m: DENOMINATOR },)

        t018c012_f = OBSColumn(
            id='t018c012_f',
            name='Movers (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c010_f: DENOMINATOR },)

        t018c013_t = OBSColumn(
            id='t018c013_t',
            name='Non-migrants (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c012_t: DENOMINATOR },)

        t018c013_m = OBSColumn(
            id='t018c013_m',
            name='Non-migrants (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c012_m: DENOMINATOR },)

        t018c013_f = OBSColumn(
            id='t018c013_f',
            name='Non-migrants (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c012_f: DENOMINATOR },)

        t018c014_t = OBSColumn(
            id='t018c014_t',
            name='Migrants (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c012_t: DENOMINATOR },)

        t018c014_m = OBSColumn(
            id='t018c014_m',
            name='Migrants (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c012_m: DENOMINATOR },)

        t018c014_f = OBSColumn(
            id='t018c014_f',
            name='Migrants (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c012_f: DENOMINATOR },)

        t018c015_t = OBSColumn(
            id='t018c015_t',
            name='Internal migrants (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c014_t: DENOMINATOR },)

        t018c015_m = OBSColumn(
            id='t018c015_m',
            name='Internal migrants (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c014_m: DENOMINATOR },)

        t018c015_f = OBSColumn(
            id='t018c015_f',
            name='Internal migrants (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c014_f: DENOMINATOR },)

        t018c016_t = OBSColumn(
            id='t018c016_t',
            name='Intraprovincial migrants (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c015_t: DENOMINATOR },)

        t018c016_m = OBSColumn(
            id='t018c016_m',
            name='Intraprovincial migrants (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c015_m: DENOMINATOR },)

        t018c016_f = OBSColumn(
            id='t018c016_f',
            name='Intraprovincial migrants (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c015_f: DENOMINATOR },)

        t018c017_t = OBSColumn(
            id='t018c017_t',
            name='Interprovincial migrants (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c015_t: DENOMINATOR },)

        t018c017_m = OBSColumn(
            id='t018c017_m',
            name='Interprovincial migrants (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c015_m: DENOMINATOR },)

        t018c017_f = OBSColumn(
            id='t018c017_f',
            name='Interprovincial migrants (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c015_f: DENOMINATOR },)

        t018c018_t = OBSColumn(
            id='t018c018_t',
            name='External migrants (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c014_t: DENOMINATOR },)

        t018c018_m = OBSColumn(
            id='t018c018_m',
            name='External migrants (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c014_m: DENOMINATOR },)

        t018c018_f = OBSColumn(
            id='t018c018_f',
            name='External migrants (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['migration']],
            targets={ t018c014_f: DENOMINATOR },)

        t019c002_t = OBSColumn(
            id='t019c002_t',
            name='People aged 15+ who commute to work by car, truck or van - as a driver (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_t: DENOMINATOR },)

        t019c002_m = OBSColumn(
            id='t019c002_m',
            name='People aged 15+ who commute to work by car, truck or van - as a driver (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_m: DENOMINATOR },)

        t019c002_f = OBSColumn(
            id='t019c002_f',
            name='People aged 15+ who commute to work by car, truck or van - as a driver (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_f: DENOMINATOR },)

        t019c003_t = OBSColumn(
            id='t019c003_t',
            name='People aged 15+ who commute to work by car, truck or van - as a passenger (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_t: DENOMINATOR },)

        t019c003_m = OBSColumn(
            id='t019c003_m',
            name='People aged 15+ who commute to work by car, truck or van - as a passenger (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_m: DENOMINATOR },)

        t019c003_f = OBSColumn(
            id='t019c003_f',
            name='People aged 15+ who commute to work by car, truck or van - as a passenger (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_f: DENOMINATOR },)

        t019c004_t = OBSColumn(
            id='t019c004_t',
            name='People aged 15+ who commute to work by public transit (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_t: DENOMINATOR },)

        t019c004_m = OBSColumn(
            id='t019c004_m',
            name='People aged 15+ who commute to work by public transit (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_m: DENOMINATOR },)

        t019c004_f = OBSColumn(
            id='t019c004_f',
            name='People aged 15+ who commute to work by public transit (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_f: DENOMINATOR },)

        t019c005_t = OBSColumn(
            id='t019c005_t',
            name='People aged 15+ who commute to work by foot (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_t: DENOMINATOR },)

        t019c005_m = OBSColumn(
            id='t019c005_m',
            name='People aged 15+ who commute to work by foot (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_m: DENOMINATOR },)

        t019c005_f = OBSColumn(
            id='t019c005_f',
            name='People aged 15+ who commute to work by foot (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_f: DENOMINATOR },)

        t019c006_t = OBSColumn(
            id='t019c006_t',
            name='People aged 15+ who commute to work by bicycle (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_t: DENOMINATOR },)

        t019c006_m = OBSColumn(
            id='t019c006_m',
            name='People aged 15+ who commute to work by bicycle (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_m: DENOMINATOR },)

        t019c006_f = OBSColumn(
            id='t019c006_f',
            name='People aged 15+ who commute to work by bicycle (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_f: DENOMINATOR },)

        t019c007_t = OBSColumn(
            id='t019c007_t',
            name='People aged 15+ who commute to work by another method (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_t: DENOMINATOR },)

        t019c007_m = OBSColumn(
            id='t019c007_m',
            name='People aged 15+ who commute to work by another method (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_m: DENOMINATOR },)

        t019c007_f = OBSColumn(
            id='t019c007_f',
            name='People aged 15+ who commute to work by another method (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_f: DENOMINATOR },)

        # FIXME
        # The below three are redundant with another total population column
        # and should be eliminated.
        t020c001_t = OBSColumn(
            id='t020c001_t',
            name='Total population in private households by non-official languages spoken (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={},)

        t020c001_m = OBSColumn(
            id='t020c001_m',
            name='Total population in private households by non-official languages spoken (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={},)

        t020c001_f = OBSColumn(
            id='t020c001_f',
            name='Total population in private households by non-official languages spoken (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={},)

        # FIXME
        # The below should specify in their name that this is the population
        # in households speaking a non-official language.
        t020c002_t = OBSColumn(
            id='t020c002_t',
            name='Aboriginal languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c001_t: DENOMINATOR },)

        t020c002_m = OBSColumn(
            id='t020c002_m',
            name='Aboriginal languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c001_m: DENOMINATOR },)

        t020c002_f = OBSColumn(
            id='t020c002_f',
            name='Aboriginal languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c001_f: DENOMINATOR },)

        t020c003_t = OBSColumn(
            id='t020c003_t',
            name='Algonquin (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_t: DENOMINATOR },)

        t020c003_m = OBSColumn(
            id='t020c003_m',
            name='Algonquin (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_m: DENOMINATOR },)

        t020c003_f = OBSColumn(
            id='t020c003_f',
            name='Algonquin (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_f: DENOMINATOR },)

        t020c004_t = OBSColumn(
            id='t020c004_t',
            name='Atikamekw (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_t: DENOMINATOR },)

        t020c004_m = OBSColumn(
            id='t020c004_m',
            name='Atikamekw (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_m: DENOMINATOR },)

        t020c004_f = OBSColumn(
            id='t020c004_f',
            name='Atikamekw (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_f: DENOMINATOR },)

        t020c005_t = OBSColumn(
            id='t020c005_t',
            name='Blackfoot (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_t: DENOMINATOR },)

        t020c005_m = OBSColumn(
            id='t020c005_m',
            name='Blackfoot (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_m: DENOMINATOR },)

        t020c005_f = OBSColumn(
            id='t020c005_f',
            name='Blackfoot (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_f: DENOMINATOR },)

        t020c006_t = OBSColumn(
            id='t020c006_t',
            name='Cree languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_t: DENOMINATOR },)

        t020c006_m = OBSColumn(
            id='t020c006_m',
            name='Cree languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_m: DENOMINATOR },)

        t020c006_f = OBSColumn(
            id='t020c006_f',
            name='Cree languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_f: DENOMINATOR },)

        t020c007_t = OBSColumn(
            id='t020c007_t',
            name='Mi\'kmaq (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_t: DENOMINATOR },)

        t020c007_m = OBSColumn(
            id='t020c007_m',
            name='Mi\'kmaq (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_m: DENOMINATOR },)

        t020c007_f = OBSColumn(
            id='t020c007_f',
            name='Mi\'kmaq (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_f: DENOMINATOR },)

        t020c008_t = OBSColumn(
            id='t020c008_t',
            name='Innu/Montagnais (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_t: DENOMINATOR },)

        t020c008_m = OBSColumn(
            id='t020c008_m',
            name='Innu/Montagnais (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_m: DENOMINATOR },)

        t020c008_f = OBSColumn(
            id='t020c008_f',
            name='Innu/Montagnais (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_f: DENOMINATOR },)

        t020c009_t = OBSColumn(
            id='t020c009_t',
            name='Ojibway (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_t: DENOMINATOR },)

        t020c009_m = OBSColumn(
            id='t020c009_m',
            name='Ojibway (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_m: DENOMINATOR },)

        t020c009_f = OBSColumn(
            id='t020c009_f',
            name='Ojibway (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_f: DENOMINATOR },)

        t020c010_t = OBSColumn(
            id='t020c010_t',
            name='Oji-Cree (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_t: DENOMINATOR },)

        t020c010_m = OBSColumn(
            id='t020c010_m',
            name='Oji-Cree (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_m: DENOMINATOR },)

        t020c010_f = OBSColumn(
            id='t020c010_f',
            name='Oji-Cree (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_f: DENOMINATOR },)

        t020c011_t = OBSColumn(
            id='t020c011_t',
            name='Carrier (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_t: DENOMINATOR },)

        t020c011_m = OBSColumn(
            id='t020c011_m',
            name='Carrier (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_m: DENOMINATOR },)

        t020c011_f = OBSColumn(
            id='t020c011_f',
            name='Carrier (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_f: DENOMINATOR },)

        t020c012_t = OBSColumn(
            id='t020c012_t',
            name='Dene (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_t: DENOMINATOR },)

        t020c012_m = OBSColumn(
            id='t020c012_m',
            name='Dene (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_m: DENOMINATOR },)

        t020c012_f = OBSColumn(
            id='t020c012_f',
            name='Dene (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_f: DENOMINATOR },)

        t020c013_t = OBSColumn(
            id='t020c013_t',
            name='Tlicho (Dogrib) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_t: DENOMINATOR },)

        t020c013_m = OBSColumn(
            id='t020c013_m',
            name='Tlicho (Dogrib) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_m: DENOMINATOR },)

        t020c013_f = OBSColumn(
            id='t020c013_f',
            name='Tlicho (Dogrib) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_f: DENOMINATOR },)

        t020c014_t = OBSColumn(
            id='t020c014_t',
            name='Slavey, n.o.s. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_t: DENOMINATOR },)

        t020c014_m = OBSColumn(
            id='t020c014_m',
            name='Slavey, n.o.s. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_m: DENOMINATOR },)

        t020c014_f = OBSColumn(
            id='t020c014_f',
            name='Slavey, n.o.s. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_f: DENOMINATOR },)

        t020c015_t = OBSColumn(
            id='t020c015_t',
            name='Stoney (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_t: DENOMINATOR },)

        t020c015_m = OBSColumn(
            id='t020c015_m',
            name='Stoney (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_m: DENOMINATOR },)

        t020c015_f = OBSColumn(
            id='t020c015_f',
            name='Stoney (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_f: DENOMINATOR },)

        t020c016_t = OBSColumn(
            id='t020c016_t',
            name='Inuktitut (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_t: DENOMINATOR },)

        t020c016_m = OBSColumn(
            id='t020c016_m',
            name='Inuktitut (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_m: DENOMINATOR },)

        t020c016_f = OBSColumn(
            id='t020c016_f',
            name='Inuktitut (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_f: DENOMINATOR },)

        t020c017_t = OBSColumn(
            id='t020c017_t',
            name='Other Aboriginal languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_t: DENOMINATOR },)

        t020c017_m = OBSColumn(
            id='t020c017_m',
            name='Other Aboriginal languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_m: DENOMINATOR },)

        t020c017_f = OBSColumn(
            id='t020c017_f',
            name='Other Aboriginal languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c002_f: DENOMINATOR },)

        t020c018_t = OBSColumn(
            id='t020c018_t',
            name='Non-Aboriginal languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c001_t: DENOMINATOR },)

        t020c018_m = OBSColumn(
            id='t020c018_m',
            name='Non-Aboriginal languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c001_m: DENOMINATOR },)

        t020c018_f = OBSColumn(
            id='t020c018_f',
            name='Non-Aboriginal languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c001_f: DENOMINATOR },)

        t020c019_t = OBSColumn(
            id='t020c019_t',
            name='Italian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c019_m = OBSColumn(
            id='t020c019_m',
            name='Italian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c019_f = OBSColumn(
            id='t020c019_f',
            name='Italian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c020_t = OBSColumn(
            id='t020c020_t',
            name='Portuguese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c020_m = OBSColumn(
            id='t020c020_m',
            name='Portuguese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c020_f = OBSColumn(
            id='t020c020_f',
            name='Portuguese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c021_t = OBSColumn(
            id='t020c021_t',
            name='Romanian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c021_m = OBSColumn(
            id='t020c021_m',
            name='Romanian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c021_f = OBSColumn(
            id='t020c021_f',
            name='Romanian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c022_t = OBSColumn(
            id='t020c022_t',
            name='Spanish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c022_m = OBSColumn(
            id='t020c022_m',
            name='Spanish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c022_f = OBSColumn(
            id='t020c022_f',
            name='Spanish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c023_t = OBSColumn(
            id='t020c023_t',
            name='Dutch (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c023_m = OBSColumn(
            id='t020c023_m',
            name='Dutch (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c023_f = OBSColumn(
            id='t020c023_f',
            name='Dutch (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c024_t = OBSColumn(
            id='t020c024_t',
            name='Flemish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c024_m = OBSColumn(
            id='t020c024_m',
            name='Flemish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c024_f = OBSColumn(
            id='t020c024_f',
            name='Flemish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c025_t = OBSColumn(
            id='t020c025_t',
            name='German (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c025_m = OBSColumn(
            id='t020c025_m',
            name='German (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c025_f = OBSColumn(
            id='t020c025_f',
            name='German (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c026_t = OBSColumn(
            id='t020c026_t',
            name='Yiddish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c026_m = OBSColumn(
            id='t020c026_m',
            name='Yiddish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c026_f = OBSColumn(
            id='t020c026_f',
            name='Yiddish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c027_t = OBSColumn(
            id='t020c027_t',
            name='Danish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c027_m = OBSColumn(
            id='t020c027_m',
            name='Danish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c027_f = OBSColumn(
            id='t020c027_f',
            name='Danish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c028_t = OBSColumn(
            id='t020c028_t',
            name='Norwegian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c028_m = OBSColumn(
            id='t020c028_m',
            name='Norwegian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c028_f = OBSColumn(
            id='t020c028_f',
            name='Norwegian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c029_t = OBSColumn(
            id='t020c029_t',
            name='Swedish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c029_m = OBSColumn(
            id='t020c029_m',
            name='Swedish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c029_f = OBSColumn(
            id='t020c029_f',
            name='Swedish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c030_t = OBSColumn(
            id='t020c030_t',
            name='Afrikaans (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c030_m = OBSColumn(
            id='t020c030_m',
            name='Afrikaans (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c030_f = OBSColumn(
            id='t020c030_f',
            name='Afrikaans (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c031_t = OBSColumn(
            id='t020c031_t',
            name='Gaelic languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c031_m = OBSColumn(
            id='t020c031_m',
            name='Gaelic languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c031_f = OBSColumn(
            id='t020c031_f',
            name='Gaelic languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c032_t = OBSColumn(
            id='t020c032_t',
            name='Bosnian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c032_m = OBSColumn(
            id='t020c032_m',
            name='Bosnian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c032_f = OBSColumn(
            id='t020c032_f',
            name='Bosnian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c033_t = OBSColumn(
            id='t020c033_t',
            name='Bulgarian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c033_m = OBSColumn(
            id='t020c033_m',
            name='Bulgarian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c033_f = OBSColumn(
            id='t020c033_f',
            name='Bulgarian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c034_t = OBSColumn(
            id='t020c034_t',
            name='Croatian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c034_m = OBSColumn(
            id='t020c034_m',
            name='Croatian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c034_f = OBSColumn(
            id='t020c034_f',
            name='Croatian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c035_t = OBSColumn(
            id='t020c035_t',
            name='Czech (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c035_m = OBSColumn(
            id='t020c035_m',
            name='Czech (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c035_f = OBSColumn(
            id='t020c035_f',
            name='Czech (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c036_t = OBSColumn(
            id='t020c036_t',
            name='Macedonian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c036_m = OBSColumn(
            id='t020c036_m',
            name='Macedonian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c036_f = OBSColumn(
            id='t020c036_f',
            name='Macedonian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c037_t = OBSColumn(
            id='t020c037_t',
            name='Polish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c037_m = OBSColumn(
            id='t020c037_m',
            name='Polish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c037_f = OBSColumn(
            id='t020c037_f',
            name='Polish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c038_t = OBSColumn(
            id='t020c038_t',
            name='Russian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c038_m = OBSColumn(
            id='t020c038_m',
            name='Russian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c038_f = OBSColumn(
            id='t020c038_f',
            name='Russian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c039_t = OBSColumn(
            id='t020c039_t',
            name='Serbian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c039_m = OBSColumn(
            id='t020c039_m',
            name='Serbian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c039_f = OBSColumn(
            id='t020c039_f',
            name='Serbian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c040_t = OBSColumn(
            id='t020c040_t',
            name='Serbo-Croatian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c040_m = OBSColumn(
            id='t020c040_m',
            name='Serbo-Croatian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c040_f = OBSColumn(
            id='t020c040_f',
            name='Serbo-Croatian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c041_t = OBSColumn(
            id='t020c041_t',
            name='Slovak (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c041_m = OBSColumn(
            id='t020c041_m',
            name='Slovak (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c041_f = OBSColumn(
            id='t020c041_f',
            name='Slovak (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c042_t = OBSColumn(
            id='t020c042_t',
            name='Slovenian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c042_m = OBSColumn(
            id='t020c042_m',
            name='Slovenian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c042_f = OBSColumn(
            id='t020c042_f',
            name='Slovenian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c043_t = OBSColumn(
            id='t020c043_t',
            name='Ukrainian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c043_m = OBSColumn(
            id='t020c043_m',
            name='Ukrainian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c043_f = OBSColumn(
            id='t020c043_f',
            name='Ukrainian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c044_t = OBSColumn(
            id='t020c044_t',
            name='Latvian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c044_m = OBSColumn(
            id='t020c044_m',
            name='Latvian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c044_f = OBSColumn(
            id='t020c044_f',
            name='Latvian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c045_t = OBSColumn(
            id='t020c045_t',
            name='Lithuanian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c045_m = OBSColumn(
            id='t020c045_m',
            name='Lithuanian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c045_f = OBSColumn(
            id='t020c045_f',
            name='Lithuanian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c046_t = OBSColumn(
            id='t020c046_t',
            name='Greek (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c046_m = OBSColumn(
            id='t020c046_m',
            name='Greek (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c046_f = OBSColumn(
            id='t020c046_f',
            name='Greek (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c047_t = OBSColumn(
            id='t020c047_t',
            name='Armenian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c047_m = OBSColumn(
            id='t020c047_m',
            name='Armenian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c047_f = OBSColumn(
            id='t020c047_f',
            name='Armenian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c048_t = OBSColumn(
            id='t020c048_t',
            name='Albanian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c048_m = OBSColumn(
            id='t020c048_m',
            name='Albanian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c048_f = OBSColumn(
            id='t020c048_f',
            name='Albanian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c049_t = OBSColumn(
            id='t020c049_t',
            name='Estonian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c049_m = OBSColumn(
            id='t020c049_m',
            name='Estonian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c049_f = OBSColumn(
            id='t020c049_f',
            name='Estonian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c050_t = OBSColumn(
            id='t020c050_t',
            name='Finnish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c050_m = OBSColumn(
            id='t020c050_m',
            name='Finnish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c050_f = OBSColumn(
            id='t020c050_f',
            name='Finnish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c051_t = OBSColumn(
            id='t020c051_t',
            name='Hungarian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c051_m = OBSColumn(
            id='t020c051_m',
            name='Hungarian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c051_f = OBSColumn(
            id='t020c051_f',
            name='Hungarian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c052_t = OBSColumn(
            id='t020c052_t',
            name='Turkish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c052_m = OBSColumn(
            id='t020c052_m',
            name='Turkish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c052_f = OBSColumn(
            id='t020c052_f',
            name='Turkish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c053_t = OBSColumn(
            id='t020c053_t',
            name='Berber languages (Kabyle) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c053_m = OBSColumn(
            id='t020c053_m',
            name='Berber languages (Kabyle) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c053_f = OBSColumn(
            id='t020c053_f',
            name='Berber languages (Kabyle) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c054_t = OBSColumn(
            id='t020c054_t',
            name='Oromo (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c054_m = OBSColumn(
            id='t020c054_m',
            name='Oromo (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c054_f = OBSColumn(
            id='t020c054_f',
            name='Oromo (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c055_t = OBSColumn(
            id='t020c055_t',
            name='Somali (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c055_m = OBSColumn(
            id='t020c055_m',
            name='Somali (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c055_f = OBSColumn(
            id='t020c055_f',
            name='Somali (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c056_t = OBSColumn(
            id='t020c056_t',
            name='Amharic (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c056_m = OBSColumn(
            id='t020c056_m',
            name='Amharic (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c056_f = OBSColumn(
            id='t020c056_f',
            name='Amharic (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c057_t = OBSColumn(
            id='t020c057_t',
            name='Arabic (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c057_m = OBSColumn(
            id='t020c057_m',
            name='Arabic (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c057_f = OBSColumn(
            id='t020c057_f',
            name='Arabic (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c058_t = OBSColumn(
            id='t020c058_t',
            name='Hebrew (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c058_m = OBSColumn(
            id='t020c058_m',
            name='Hebrew (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c058_f = OBSColumn(
            id='t020c058_f',
            name='Hebrew (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c059_t = OBSColumn(
            id='t020c059_t',
            name='Maltese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c059_m = OBSColumn(
            id='t020c059_m',
            name='Maltese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c059_f = OBSColumn(
            id='t020c059_f',
            name='Maltese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c060_t = OBSColumn(
            id='t020c060_t',
            name='Tigrigna (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c060_m = OBSColumn(
            id='t020c060_m',
            name='Tigrigna (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c060_f = OBSColumn(
            id='t020c060_f',
            name='Tigrigna (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c061_t = OBSColumn(
            id='t020c061_t',
            name='Semitic languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c061_m = OBSColumn(
            id='t020c061_m',
            name='Semitic languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c061_f = OBSColumn(
            id='t020c061_f',
            name='Semitic languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c062_t = OBSColumn(
            id='t020c062_t',
            name='Bengali (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c062_m = OBSColumn(
            id='t020c062_m',
            name='Bengali (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c062_f = OBSColumn(
            id='t020c062_f',
            name='Bengali (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c063_t = OBSColumn(
            id='t020c063_t',
            name='Gujarati (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c063_m = OBSColumn(
            id='t020c063_m',
            name='Gujarati (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c063_f = OBSColumn(
            id='t020c063_f',
            name='Gujarati (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c064_t = OBSColumn(
            id='t020c064_t',
            name='Hindi (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c064_m = OBSColumn(
            id='t020c064_m',
            name='Hindi (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c064_f = OBSColumn(
            id='t020c064_f',
            name='Hindi (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c065_t = OBSColumn(
            id='t020c065_t',
            name='Konkani (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c065_m = OBSColumn(
            id='t020c065_m',
            name='Konkani (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c065_f = OBSColumn(
            id='t020c065_f',
            name='Konkani (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c066_t = OBSColumn(
            id='t020c066_t',
            name='Marathi (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c066_m = OBSColumn(
            id='t020c066_m',
            name='Marathi (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c066_f = OBSColumn(
            id='t020c066_f',
            name='Marathi (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c067_t = OBSColumn(
            id='t020c067_t',
            name='Panjabi (Punjabi) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c067_m = OBSColumn(
            id='t020c067_m',
            name='Panjabi (Punjabi) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c067_f = OBSColumn(
            id='t020c067_f',
            name='Panjabi (Punjabi) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c068_t = OBSColumn(
            id='t020c068_t',
            name='Sindhi (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c068_m = OBSColumn(
            id='t020c068_m',
            name='Sindhi (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c068_f = OBSColumn(
            id='t020c068_f',
            name='Sindhi (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c069_t = OBSColumn(
            id='t020c069_t',
            name='Sinhala (Sinhalese) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c069_m = OBSColumn(
            id='t020c069_m',
            name='Sinhala (Sinhalese) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c069_f = OBSColumn(
            id='t020c069_f',
            name='Sinhala (Sinhalese) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c070_t = OBSColumn(
            id='t020c070_t',
            name='Urdu (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c070_m = OBSColumn(
            id='t020c070_m',
            name='Urdu (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c070_f = OBSColumn(
            id='t020c070_f',
            name='Urdu (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c071_t = OBSColumn(
            id='t020c071_t',
            name='Nepali (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c071_m = OBSColumn(
            id='t020c071_m',
            name='Nepali (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c071_f = OBSColumn(
            id='t020c071_f',
            name='Nepali (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c072_t = OBSColumn(
            id='t020c072_t',
            name='Kurdish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c072_m = OBSColumn(
            id='t020c072_m',
            name='Kurdish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c072_f = OBSColumn(
            id='t020c072_f',
            name='Kurdish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c073_t = OBSColumn(
            id='t020c073_t',
            name='Pashto (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c073_m = OBSColumn(
            id='t020c073_m',
            name='Pashto (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c073_f = OBSColumn(
            id='t020c073_f',
            name='Pashto (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c074_t = OBSColumn(
            id='t020c074_t',
            name='Persian (Farsi) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c074_m = OBSColumn(
            id='t020c074_m',
            name='Persian (Farsi) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c074_f = OBSColumn(
            id='t020c074_f',
            name='Persian (Farsi) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c075_t = OBSColumn(
            id='t020c075_t',
            name='Indo-Iranian languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c075_m = OBSColumn(
            id='t020c075_m',
            name='Indo-Iranian languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c075_f = OBSColumn(
            id='t020c075_f',
            name='Indo-Iranian languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c076_t = OBSColumn(
            id='t020c076_t',
            name='Kannada (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c076_m = OBSColumn(
            id='t020c076_m',
            name='Kannada (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c076_f = OBSColumn(
            id='t020c076_f',
            name='Kannada (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c077_t = OBSColumn(
            id='t020c077_t',
            name='Malayalam (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c077_m = OBSColumn(
            id='t020c077_m',
            name='Malayalam (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c077_f = OBSColumn(
            id='t020c077_f',
            name='Malayalam (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c078_t = OBSColumn(
            id='t020c078_t',
            name='Tamil (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c078_m = OBSColumn(
            id='t020c078_m',
            name='Tamil (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c078_f = OBSColumn(
            id='t020c078_f',
            name='Tamil (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c079_t = OBSColumn(
            id='t020c079_t',
            name='Telugu (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c079_m = OBSColumn(
            id='t020c079_m',
            name='Telugu (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c079_f = OBSColumn(
            id='t020c079_f',
            name='Telugu (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c080_t = OBSColumn(
            id='t020c080_t',
            name='Japanese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c080_m = OBSColumn(
            id='t020c080_m',
            name='Japanese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c080_f = OBSColumn(
            id='t020c080_f',
            name='Japanese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c081_t = OBSColumn(
            id='t020c081_t',
            name='Korean (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c081_m = OBSColumn(
            id='t020c081_m',
            name='Korean (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c081_f = OBSColumn(
            id='t020c081_f',
            name='Korean (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c082_t = OBSColumn(
            id='t020c082_t',
            name='Cantonese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c082_m = OBSColumn(
            id='t020c082_m',
            name='Cantonese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c082_f = OBSColumn(
            id='t020c082_f',
            name='Cantonese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c083_t = OBSColumn(
            id='t020c083_t',
            name='Fukien (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c083_m = OBSColumn(
            id='t020c083_m',
            name='Fukien (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c083_f = OBSColumn(
            id='t020c083_f',
            name='Fukien (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c084_t = OBSColumn(
            id='t020c084_t',
            name='Hakka (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c084_m = OBSColumn(
            id='t020c084_m',
            name='Hakka (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c084_f = OBSColumn(
            id='t020c084_f',
            name='Hakka (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c085_t = OBSColumn(
            id='t020c085_t',
            name='Mandarin (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c085_m = OBSColumn(
            id='t020c085_m',
            name='Mandarin (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c085_f = OBSColumn(
            id='t020c085_f',
            name='Mandarin (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c086_t = OBSColumn(
            id='t020c086_t',
            name='Taiwanese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c086_m = OBSColumn(
            id='t020c086_m',
            name='Taiwanese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c086_f = OBSColumn(
            id='t020c086_f',
            name='Taiwanese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c087_t = OBSColumn(
            id='t020c087_t',
            name='Chinese, n.o.s. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c087_m = OBSColumn(
            id='t020c087_m',
            name='Chinese, n.o.s. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c087_f = OBSColumn(
            id='t020c087_f',
            name='Chinese, n.o.s. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c088_t = OBSColumn(
            id='t020c088_t',
            name='Lao (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c088_m = OBSColumn(
            id='t020c088_m',
            name='Lao (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c088_f = OBSColumn(
            id='t020c088_f',
            name='Lao (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c089_t = OBSColumn(
            id='t020c089_t',
            name='Thai (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c089_m = OBSColumn(
            id='t020c089_m',
            name='Thai (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c089_f = OBSColumn(
            id='t020c089_f',
            name='Thai (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c090_t = OBSColumn(
            id='t020c090_t',
            name='Khmer (Cambodian) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c090_m = OBSColumn(
            id='t020c090_m',
            name='Khmer (Cambodian) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c090_f = OBSColumn(
            id='t020c090_f',
            name='Khmer (Cambodian) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c091_t = OBSColumn(
            id='t020c091_t',
            name='Vietnamese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c091_m = OBSColumn(
            id='t020c091_m',
            name='Vietnamese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c091_f = OBSColumn(
            id='t020c091_f',
            name='Vietnamese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c092_t = OBSColumn(
            id='t020c092_t',
            name='Bisayan languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c092_m = OBSColumn(
            id='t020c092_m',
            name='Bisayan languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c092_f = OBSColumn(
            id='t020c092_f',
            name='Bisayan languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c093_t = OBSColumn(
            id='t020c093_t',
            name='Ilocano (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c093_m = OBSColumn(
            id='t020c093_m',
            name='Ilocano (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c093_f = OBSColumn(
            id='t020c093_f',
            name='Ilocano (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c094_t = OBSColumn(
            id='t020c094_t',
            name='Malay (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c094_m = OBSColumn(
            id='t020c094_m',
            name='Malay (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c094_f = OBSColumn(
            id='t020c094_f',
            name='Malay (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c095_t = OBSColumn(
            id='t020c095_t',
            name='Tagalog (Pilipino,Filipino) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c095_m = OBSColumn(
            id='t020c095_m',
            name='Tagalog (Pilipino,Filipino) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c095_f = OBSColumn(
            id='t020c095_f',
            name='Tagalog (Pilipino,Filipino) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c096_t = OBSColumn(
            id='t020c096_t',
            name='Akan (Twi) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c096_m = OBSColumn(
            id='t020c096_m',
            name='Akan (Twi) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c096_f = OBSColumn(
            id='t020c096_f',
            name='Akan (Twi) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c097_t = OBSColumn(
            id='t020c097_t',
            name='Lingala (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c097_m = OBSColumn(
            id='t020c097_m',
            name='Lingala (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c097_f = OBSColumn(
            id='t020c097_f',
            name='Lingala (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c098_t = OBSColumn(
            id='t020c098_t',
            name='Rundi (Kirundi) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c098_m = OBSColumn(
            id='t020c098_m',
            name='Rundi (Kirundi) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c098_f = OBSColumn(
            id='t020c098_f',
            name='Rundi (Kirundi) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c099_t = OBSColumn(
            id='t020c099_t',
            name='Rwanda (Kinyarwanda) (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c099_m = OBSColumn(
            id='t020c099_m',
            name='Rwanda (Kinyarwanda) (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c099_f = OBSColumn(
            id='t020c099_f',
            name='Rwanda (Kinyarwanda) (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c100_t = OBSColumn(
            id='t020c100_t',
            name='Swahili (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c100_m = OBSColumn(
            id='t020c100_m',
            name='Swahili (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c100_f = OBSColumn(
            id='t020c100_f',
            name='Swahili (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c101_t = OBSColumn(
            id='t020c101_t',
            name='Bantu languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c101_m = OBSColumn(
            id='t020c101_m',
            name='Bantu languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c101_f = OBSColumn(
            id='t020c101_f',
            name='Bantu languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c102_t = OBSColumn(
            id='t020c102_t',
            name='Niger-Congo languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c102_m = OBSColumn(
            id='t020c102_m',
            name='Niger-Congo languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c102_f = OBSColumn(
            id='t020c102_f',
            name='Niger-Congo languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c103_t = OBSColumn(
            id='t020c103_t',
            name='African languages, n.i.e. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c103_m = OBSColumn(
            id='t020c103_m',
            name='African languages, n.i.e. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c103_f = OBSColumn(
            id='t020c103_f',
            name='African languages, n.i.e. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c104_t = OBSColumn(
            id='t020c104_t',
            name='Creoles (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c104_m = OBSColumn(
            id='t020c104_m',
            name='Creoles (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c104_f = OBSColumn(
            id='t020c104_f',
            name='Creoles (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t020c105_t = OBSColumn(
            id='t020c105_t',
            name='Other non-Aboriginal languages (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_t: DENOMINATOR },)

        t020c105_m = OBSColumn(
            id='t020c105_m',
            name='Other non-Aboriginal languages (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_m: DENOMINATOR },)

        t020c105_f = OBSColumn(
            id='t020c105_f',
            name='Other non-Aboriginal languages (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['language']],
            targets={ t020c018_f: DENOMINATOR },)

        t021c002_t = OBSColumn(
            id='t021c002_t',
            name='People aged 15+ in the labour force - Occupation - not applicable (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_t: DENOMINATOR },)

        t021c002_m = OBSColumn(
            id='t021c002_m',
            name='People aged 15+ in the labour force - Occupation - not applicable (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_m: DENOMINATOR },)

        t021c002_f = OBSColumn(
            id='t021c002_f',
            name='People aged 15+ in the labour force - Occupation - not applicable (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_f: DENOMINATOR },)

        t021c003_t = OBSColumn(
            id='t021c003_t',
            name='People aged 15+ in the labour force - All occupations (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_t: DENOMINATOR },)

        t021c003_m = OBSColumn(
            id='t021c003_m',
            name='People aged 15+ in the labour force - All occupations (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_m: DENOMINATOR },)

        t021c003_f = OBSColumn(
            id='t021c003_f',
            name='People aged 15+ in the labour force - All occupations (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_f: DENOMINATOR },)

        # FIXME
        # The below should have a name that specifies this is the labour force
        # in that occupation, for example "Labour force in management
        # occupations (Total)".
        t021c004_t = OBSColumn(
            id='t021c004_t',
            name='0 Management occupations (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_t: DENOMINATOR },)

        t021c004_m = OBSColumn(
            id='t021c004_m',
            name='0 Management occupations (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_m: DENOMINATOR },)

        t021c004_f = OBSColumn(
            id='t021c004_f',
            name='0 Management occupations (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_f: DENOMINATOR },)

        t021c005_t = OBSColumn(
            id='t021c005_t',
            name='1 Business, finance and administration occupations (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_t: DENOMINATOR },)

        t021c005_m = OBSColumn(
            id='t021c005_m',
            name='1 Business, finance and administration occupations (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_m: DENOMINATOR },)

        t021c005_f = OBSColumn(
            id='t021c005_f',
            name='1 Business, finance and administration occupations (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_f: DENOMINATOR },)

        t021c006_t = OBSColumn(
            id='t021c006_t',
            name='2 Natural and applied sciences and related occupations (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_t: DENOMINATOR },)

        t021c006_m = OBSColumn(
            id='t021c006_m',
            name='2 Natural and applied sciences and related occupations (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_m: DENOMINATOR },)

        t021c006_f = OBSColumn(
            id='t021c006_f',
            name='2 Natural and applied sciences and related occupations (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_f: DENOMINATOR },)

        t021c007_t = OBSColumn(
            id='t021c007_t',
            name='3 Health occupations (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_t: DENOMINATOR },)

        t021c007_m = OBSColumn(
            id='t021c007_m',
            name='3 Health occupations (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_m: DENOMINATOR },)

        t021c007_f = OBSColumn(
            id='t021c007_f',
            name='3 Health occupations (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_f: DENOMINATOR },)

        t021c008_t = OBSColumn(
            id='t021c008_t',
            name='4 Occupations in education, law and social, community and government services (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_t: DENOMINATOR },)

        t021c008_m = OBSColumn(
            id='t021c008_m',
            name='4 Occupations in education, law and social, community and government services (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_m: DENOMINATOR },)

        t021c008_f = OBSColumn(
            id='t021c008_f',
            name='4 Occupations in education, law and social, community and government services (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_f: DENOMINATOR },)

        t021c009_t = OBSColumn(
            id='t021c009_t',
            name='5 Occupations in art, culture, recreation and sport (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_t: DENOMINATOR },)

        t021c009_m = OBSColumn(
            id='t021c009_m',
            name='5 Occupations in art, culture, recreation and sport (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_m: DENOMINATOR },)

        t021c009_f = OBSColumn(
            id='t021c009_f',
            name='5 Occupations in art, culture, recreation and sport (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_f: DENOMINATOR },)

        t021c010_t = OBSColumn(
            id='t021c010_t',
            name='6 Sales and service occupations (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_t: DENOMINATOR },)

        t021c010_m = OBSColumn(
            id='t021c010_m',
            name='6 Sales and service occupations (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_m: DENOMINATOR },)

        t021c010_f = OBSColumn(
            id='t021c010_f',
            name='6 Sales and service occupations (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_f: DENOMINATOR },)

        t021c011_t = OBSColumn(
            id='t021c011_t',
            name='7 Trades, transport and equipment operators and related occupations (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_t: DENOMINATOR },)

        t021c011_m = OBSColumn(
            id='t021c011_m',
            name='7 Trades, transport and equipment operators and related occupations (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_m: DENOMINATOR },)

        t021c011_f = OBSColumn(
            id='t021c011_f',
            name='7 Trades, transport and equipment operators and related occupations (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_f: DENOMINATOR },)

        t021c012_t = OBSColumn(
            id='t021c012_t',
            name='8 Natural resources, agriculture and related production occupations (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_t: DENOMINATOR },)

        t021c012_m = OBSColumn(
            id='t021c012_m',
            name='8 Natural resources, agriculture and related production occupations (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_m: DENOMINATOR },)

        t021c012_f = OBSColumn(
            id='t021c012_f',
            name='8 Natural resources, agriculture and related production occupations (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_f: DENOMINATOR },)

        t021c013_t = OBSColumn(
            id='t021c013_t',
            name='9 Occupations in manufacturing and utilities (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_t: DENOMINATOR },)

        t021c013_m = OBSColumn(
            id='t021c013_m',
            name='9 Occupations in manufacturing and utilities (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_m: DENOMINATOR },)

        t021c013_f = OBSColumn(
            id='t021c013_f',
            name='9 Occupations in manufacturing and utilities (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t021c003_f: DENOMINATOR },)

        # FIXME
        # Are the below the number of people in occupied private dwellings,
        # or the actual number of private dwellings?  Unit or name should
        # be adjusted accordingly.
        t022c001_t = OBSColumn(
            id='t022c001_t',
            name='Total number of occupied private dwellings',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={},)

        # FIXME
        # Depending on the resolution of issue above, these should be either
        # renamed "Dwellings with only regular maintenance or minor repair
        # needed" or "Population in dwellings with...", etc.
        t022c002_t = OBSColumn(
            id='t022c002_t',
            name='Dwelling condition - Only regular maintenance or minor repairs needed',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t022c001_t: DENOMINATOR },)

        t022c003_t = OBSColumn(
            id='t022c003_t',
            name='Dwelling condition - Major repairs needed',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t022c001_t: DENOMINATOR },)

        # FIXME
        # Below also need clarification with either unit or name referring to
        # population, and should also specify the date is referring to
        # construction.
        t022c005_t = OBSColumn(
            id='t022c005_t',
            name='Dwelling year - 1960 or before',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t022c001_t: DENOMINATOR },)

        t022c006_t = OBSColumn(
            id='t022c006_t',
            name='Dwelling year - 1961 to 1980',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t022c001_t: DENOMINATOR },)

        t022c007_t = OBSColumn(
            id='t022c007_t',
            name='Dwelling year - 1981 to 1990',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t022c001_t: DENOMINATOR },)

        t022c008_t = OBSColumn(
            id='t022c008_t',
            name='Dwelling year - 1991 to 2000',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t022c001_t: DENOMINATOR },)

        t022c009_t = OBSColumn(
            id='t022c009_t',
            name='Dwelling year - 2001 to 2005',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t022c001_t: DENOMINATOR },)

        t022c010_t = OBSColumn(
            id='t022c010_t',
            name='Dwelling year - 2006 to 2011',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t022c001_t: DENOMINATOR },)

        # FIXME
        # Same unit/population issue as above. Dashes could be replaced with
        # "with".
        t022c012_t = OBSColumn(
            id='t022c012_t',
            name='Dwelling - 1 to 4 rooms',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t022c001_t: DENOMINATOR },)

        t022c013_t = OBSColumn(
            id='t022c013_t',
            name='Dwelling - 5 rooms',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t022c001_t: DENOMINATOR },)

        t022c014_t = OBSColumn(
            id='t022c014_t',
            name='Dwelling - 6 rooms',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t022c001_t: DENOMINATOR },)

        t022c015_t = OBSColumn(
            id='t022c015_t',
            name='Dwelling - 7 rooms',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t022c001_t: DENOMINATOR },)

        t022c016_t = OBSColumn(
            id='t022c016_t',
            name='Dwelling - 8 or more rooms',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t022c001_t: DENOMINATOR },)

        t022c017_t = OBSColumn(
            id='t022c017_t',
            name='Average number of rooms per dwelling',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_people, subsections['housing']],
            targets={},)

        t022c019_t = OBSColumn(
            id='t022c019_t',
            name='Dwelling - 0 to 1 bedroom',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t022c001_t: DENOMINATOR },)

        t022c020_t = OBSColumn(
            id='t022c020_t',
            name='Dwelling - 2 bedrooms',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t022c001_t: DENOMINATOR },)

        t022c021_t = OBSColumn(
            id='t022c021_t',
            name='Dwelling - 3 bedrooms',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t022c001_t: DENOMINATOR },)

        t022c022_t = OBSColumn(
            id='t022c022_t',
            name='Dwelling - 4 or more bedrooms',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t022c001_t: DENOMINATOR },)

        t023c002_t = OBSColumn(
            id='t023c002_t',
            name='Employed people aged 15+ who worked at home (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t015c003_t: DENOMINATOR },)

        t023c002_m = OBSColumn(
            id='t023c002_m',
            name='Employed people aged 15+ who worked at home (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t015c003_m: DENOMINATOR },)

        t023c002_f = OBSColumn(
            id='t023c002_f',
            name='Employed people aged 15+ who worked at home (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t015c003_f: DENOMINATOR },)

        t023c003_t = OBSColumn(
            id='t023c003_t',
            name='Employed people aged 15+ who worked outside Canada (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t015c003_t: DENOMINATOR },)

        t023c003_m = OBSColumn(
            id='t023c003_m',
            name='Employed people aged 15+ who worked outside Canada (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t015c003_m: DENOMINATOR },)

        t023c003_f = OBSColumn(
            id='t023c003_f',
            name='Employed people aged 15+ who worked outside Canada (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t015c003_f: DENOMINATOR },)

        t023c004_t = OBSColumn(
            id='t023c004_t',
            name='Employed people aged 15+ with no fixed workplace address (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t015c003_t: DENOMINATOR },)

        t023c004_m = OBSColumn(
            id='t023c004_m',
            name='Employed people aged 15+ with no fixed workplace address (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t015c003_m: DENOMINATOR },)

        t023c004_f = OBSColumn(
            id='t023c004_f',
            name='Employed people aged 15+ with no fixed workplace address (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t015c003_f: DENOMINATOR },)

        t023c005_t = OBSColumn(
            id='t023c005_t',
            name='Employed people aged 15+ who worked at a usual place (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t015c003_t: DENOMINATOR },)

        t023c005_m = OBSColumn(
            id='t023c005_m',
            name='Employed people aged 15+ who worked at a usual place (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t015c003_m: DENOMINATOR },)

        t023c005_f = OBSColumn(
            id='t023c005_f',
            name='Employed people aged 15+ who worked at a usual place (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t015c003_f: DENOMINATOR },)

        t024c001_t = OBSColumn(
            id='t024c001_t',
            name='Recent immigrants in private households (total)',
            description='Recent immigrants are immigrants who landed in Canada since the last census survey',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={},)

        t024c001_m = OBSColumn(
            id='t024c001_m',
            name='Recent immigrants in private households (male)',
            description='Recent immigrants are immigrants who landed in Canada since the last census survey',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={},)

        t024c001_f = OBSColumn(
            id='t024c001_f',
            name='Recent immigrants in private households (female)',
            description='Recent immigrants are immigrants who landed in Canada since the last census survey',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={},)

        t024c002_t = OBSColumn(
            id='t024c002_t',
            name='Recent immigrants born in Americas (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c001_t: DENOMINATOR },)

        t024c002_m = OBSColumn(
            id='t024c002_m',
            name='Recent immigrants born in Americas (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c001_m: DENOMINATOR },)

        t024c002_f = OBSColumn(
            id='t024c002_f',
            name='Recent immigrants born in Americas (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c001_f: DENOMINATOR },)

        t024c003_t = OBSColumn(
            id='t024c003_t',
            name='Recent immigrants born in United States (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_t: DENOMINATOR },)

        t024c003_m = OBSColumn(
            id='t024c003_m',
            name='Recent immigrants born in United States (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_m: DENOMINATOR },)

        t024c003_f = OBSColumn(
            id='t024c003_f',
            name='Recent immigrants born in United States (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_f: DENOMINATOR },)

        t024c004_t = OBSColumn(
            id='t024c004_t',
            name='Recent immigrants born in Mexico (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_t: DENOMINATOR },)

        t024c004_m = OBSColumn(
            id='t024c004_m',
            name='Recent immigrants born in Mexico (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_m: DENOMINATOR },)

        t024c004_f = OBSColumn(
            id='t024c004_f',
            name='Recent immigrants born in Mexico (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_f: DENOMINATOR },)

        t024c005_t = OBSColumn(
            id='t024c005_t',
            name='Recent immigrants born in Cuba (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_t: DENOMINATOR },)

        t024c005_m = OBSColumn(
            id='t024c005_m',
            name='Recent immigrants born in Cuba (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_m: DENOMINATOR },)

        t024c005_f = OBSColumn(
            id='t024c005_f',
            name='Recent immigrants born in Cuba (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_f: DENOMINATOR },)

        t024c006_t = OBSColumn(
            id='t024c006_t',
            name='Recent immigrants born in Haiti (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_t: DENOMINATOR },)

        t024c006_m = OBSColumn(
            id='t024c006_m',
            name='Recent immigrants born in Haiti (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_m: DENOMINATOR },)

        t024c006_f = OBSColumn(
            id='t024c006_f',
            name='Recent immigrants born in Haiti (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_f: DENOMINATOR },)

        t024c007_t = OBSColumn(
            id='t024c007_t',
            name='Recent immigrants born in Jamaica (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_t: DENOMINATOR },)

        t024c007_m = OBSColumn(
            id='t024c007_m',
            name='Recent immigrants born in Jamaica (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_m: DENOMINATOR },)

        t024c007_f = OBSColumn(
            id='t024c007_f',
            name='Recent immigrants born in Jamaica (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_f: DENOMINATOR },)

        t024c008_t = OBSColumn(
            id='t024c008_t',
            name='Recent immigrants born in Brazil (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_t: DENOMINATOR },)

        t024c008_m = OBSColumn(
            id='t024c008_m',
            name='Recent immigrants born in Brazil (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_m: DENOMINATOR },)

        t024c008_f = OBSColumn(
            id='t024c008_f',
            name='Recent immigrants born in Brazil (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_f: DENOMINATOR },)

        t024c009_t = OBSColumn(
            id='t024c009_t',
            name='Recent immigrants born in Colombia (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_t: DENOMINATOR },)

        t024c009_m = OBSColumn(
            id='t024c009_m',
            name='Recent immigrants born in Colombia (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_m: DENOMINATOR },)

        t024c009_f = OBSColumn(
            id='t024c009_f',
            name='Recent immigrants born in Colombia (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_f: DENOMINATOR },)

        t024c010_t = OBSColumn(
            id='t024c010_t',
            name='Recent immigrants born in Guyana (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_t: DENOMINATOR },)

        t024c010_m = OBSColumn(
            id='t024c010_m',
            name='Recent immigrants born in Guyana (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_m: DENOMINATOR },)

        t024c010_f = OBSColumn(
            id='t024c010_f',
            name='Recent immigrants born in Guyana (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_f: DENOMINATOR },)

        t024c011_t = OBSColumn(
            id='t024c011_t',
            name='Recent immigrants born in Peru (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_t: DENOMINATOR },)

        t024c011_m = OBSColumn(
            id='t024c011_m',
            name='Recent immigrants born in Peru (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_m: DENOMINATOR },)

        t024c011_f = OBSColumn(
            id='t024c011_f',
            name='Recent immigrants born in Peru (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_f: DENOMINATOR },)

        t024c012_t = OBSColumn(
            id='t024c012_t',
            name='Recent immigrants born in Venezuela (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_t: DENOMINATOR },)

        t024c012_m = OBSColumn(
            id='t024c012_m',
            name='Recent immigrants born in Venezuela (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_m: DENOMINATOR },)

        t024c012_f = OBSColumn(
            id='t024c012_f',
            name='Recent immigrants born in Venezuela (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_f: DENOMINATOR },)

        t024c013_t = OBSColumn(
            id='t024c013_t',
            name='Recent immigrants born in other places in Americas (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_t: DENOMINATOR },)

        t024c013_m = OBSColumn(
            id='t024c013_m',
            name='Recent immigrants born in other places in Americas (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_m: DENOMINATOR },)

        t024c013_f = OBSColumn(
            id='t024c013_f',
            name='Recent immigrants born in other places in Americas (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c002_f: DENOMINATOR },)

        t024c014_t = OBSColumn(
            id='t024c014_t',
            name='Recent immigrants born in Europe (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c001_t: DENOMINATOR },)

        t024c014_m = OBSColumn(
            id='t024c014_m',
            name='Recent immigrants born in Europe (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c001_m: DENOMINATOR },)

        t024c014_f = OBSColumn(
            id='t024c014_f',
            name='Recent immigrants born in Europe (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c001_f: DENOMINATOR },)

        t024c015_t = OBSColumn(
            id='t024c015_t',
            name='Recent immigrants born in France (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_t: DENOMINATOR },)

        t024c015_m = OBSColumn(
            id='t024c015_m',
            name='Recent immigrants born in France (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_m: DENOMINATOR },)

        t024c015_f = OBSColumn(
            id='t024c015_f',
            name='Recent immigrants born in France (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_f: DENOMINATOR },)

        t024c016_t = OBSColumn(
            id='t024c016_t',
            name='Recent immigrants born in Germany (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_t: DENOMINATOR },)

        t024c016_m = OBSColumn(
            id='t024c016_m',
            name='Recent immigrants born in Germany (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_m: DENOMINATOR },)

        t024c016_f = OBSColumn(
            id='t024c016_f',
            name='Recent immigrants born in Germany (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_f: DENOMINATOR },)

        t024c017_t = OBSColumn(
            id='t024c017_t',
            name='Recent immigrants born in Poland (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_t: DENOMINATOR },)

        t024c017_m = OBSColumn(
            id='t024c017_m',
            name='Recent immigrants born in Poland (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_m: DENOMINATOR },)

        t024c017_f = OBSColumn(
            id='t024c017_f',
            name='Recent immigrants born in Poland (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_f: DENOMINATOR },)

        t024c018_t = OBSColumn(
            id='t024c018_t',
            name='Recent immigrants born in Romania (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_t: DENOMINATOR },)

        t024c018_m = OBSColumn(
            id='t024c018_m',
            name='Recent immigrants born in Romania (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_m: DENOMINATOR },)

        t024c018_f = OBSColumn(
            id='t024c018_f',
            name='Recent immigrants born in Romania (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_f: DENOMINATOR },)

        t024c019_t = OBSColumn(
            id='t024c019_t',
            name='Recent immigrants born in Moldova (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_t: DENOMINATOR },)

        t024c019_m = OBSColumn(
            id='t024c019_m',
            name='Recent immigrants born in Moldova (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_m: DENOMINATOR },)

        t024c019_f = OBSColumn(
            id='t024c019_f',
            name='Recent immigrants born in Moldova (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_f: DENOMINATOR },)

        t024c020_t = OBSColumn(
            id='t024c020_t',
            name='Recent immigrants born in Russian Federation (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_t: DENOMINATOR },)

        t024c020_m = OBSColumn(
            id='t024c020_m',
            name='Recent immigrants born in Russian Federation (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_m: DENOMINATOR },)

        t024c020_f = OBSColumn(
            id='t024c020_f',
            name='Recent immigrants born in Russian Federation (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_f: DENOMINATOR },)

        t024c021_t = OBSColumn(
            id='t024c021_t',
            name='Recent immigrants born in Ukraine (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_t: DENOMINATOR },)

        t024c021_m = OBSColumn(
            id='t024c021_m',
            name='Recent immigrants born in Ukraine (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_m: DENOMINATOR },)

        t024c021_f = OBSColumn(
            id='t024c021_f',
            name='Recent immigrants born in Ukraine (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_f: DENOMINATOR },)

        t024c022_t = OBSColumn(
            id='t024c022_t',
            name='Recent immigrants born in United Kingdom (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_t: DENOMINATOR },)

        t024c022_m = OBSColumn(
            id='t024c022_m',
            name='Recent immigrants born in United Kingdom (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_m: DENOMINATOR },)

        t024c022_f = OBSColumn(
            id='t024c022_f',
            name='Recent immigrants born in United Kingdom (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_f: DENOMINATOR },)

        t024c023_t = OBSColumn(
            id='t024c023_t',
            name='Recent immigrants born in other places in Europe (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_t: DENOMINATOR },)

        t024c023_m = OBSColumn(
            id='t024c023_m',
            name='Recent immigrants born in other places in Europe (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_m: DENOMINATOR },)

        t024c023_f = OBSColumn(
            id='t024c023_f',
            name='Recent immigrants born in other places in Europe (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c014_f: DENOMINATOR },)

        t024c024_t = OBSColumn(
            id='t024c024_t',
            name='Recent immigrants born in Africa (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c001_t: DENOMINATOR },)

        t024c024_m = OBSColumn(
            id='t024c024_m',
            name='Recent immigrants born in Africa (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c001_m: DENOMINATOR },)

        t024c024_f = OBSColumn(
            id='t024c024_f',
            name='Recent immigrants born in Africa (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c001_f: DENOMINATOR },)

        t024c025_t = OBSColumn(
            id='t024c025_t',
            name='Recent immigrants born in Nigeria (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_t: DENOMINATOR },)

        t024c025_m = OBSColumn(
            id='t024c025_m',
            name='Recent immigrants born in Nigeria (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_m: DENOMINATOR },)

        t024c025_f = OBSColumn(
            id='t024c025_f',
            name='Recent immigrants born in Nigeria (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_f: DENOMINATOR },)

        t024c026_t = OBSColumn(
            id='t024c026_t',
            name='Recent immigrants born in Ethiopia (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_t: DENOMINATOR },)

        t024c026_m = OBSColumn(
            id='t024c026_m',
            name='Recent immigrants born in Ethiopia (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_m: DENOMINATOR },)

        t024c026_f = OBSColumn(
            id='t024c026_f',
            name='Recent immigrants born in Ethiopia (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_f: DENOMINATOR },)

        t024c027_t = OBSColumn(
            id='t024c027_t',
            name='Recent immigrants born in Mauritius (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_t: DENOMINATOR },)

        t024c027_m = OBSColumn(
            id='t024c027_m',
            name='Recent immigrants born in Mauritius (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_m: DENOMINATOR },)

        t024c027_f = OBSColumn(
            id='t024c027_f',
            name='Recent immigrants born in Mauritius (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_f: DENOMINATOR },)

        t024c028_t = OBSColumn(
            id='t024c028_t',
            name='Recent immigrants born in Somalia (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_t: DENOMINATOR },)

        t024c028_m = OBSColumn(
            id='t024c028_m',
            name='Recent immigrants born in Somalia (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_m: DENOMINATOR },)

        t024c028_f = OBSColumn(
            id='t024c028_f',
            name='Recent immigrants born in Somalia (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_f: DENOMINATOR },)

        t024c029_t = OBSColumn(
            id='t024c029_t',
            name='Recent immigrants born in Algeria (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_t: DENOMINATOR },)

        t024c029_m = OBSColumn(
            id='t024c029_m',
            name='Recent immigrants born in Algeria (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_m: DENOMINATOR },)

        t024c029_f = OBSColumn(
            id='t024c029_f',
            name='Recent immigrants born in Algeria (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_f: DENOMINATOR },)

        t024c030_t = OBSColumn(
            id='t024c030_t',
            name='Recent immigrants born in Egypt (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_t: DENOMINATOR },)

        t024c030_m = OBSColumn(
            id='t024c030_m',
            name='Recent immigrants born in Egypt (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_m: DENOMINATOR },)

        t024c030_f = OBSColumn(
            id='t024c030_f',
            name='Recent immigrants born in Egypt (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_f: DENOMINATOR },)

        t024c031_t = OBSColumn(
            id='t024c031_t',
            name='Recent immigrants born in Morocco (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_t: DENOMINATOR },)

        t024c031_m = OBSColumn(
            id='t024c031_m',
            name='Recent immigrants born in Morocco (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_m: DENOMINATOR },)

        t024c031_f = OBSColumn(
            id='t024c031_f',
            name='Recent immigrants born in Morocco (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_f: DENOMINATOR },)

        t024c032_t = OBSColumn(
            id='t024c032_t',
            name='Recent immigrants born in Tunisia (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_t: DENOMINATOR },)

        t024c032_m = OBSColumn(
            id='t024c032_m',
            name='Recent immigrants born in Tunisia (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_m: DENOMINATOR },)

        t024c032_f = OBSColumn(
            id='t024c032_f',
            name='Recent immigrants born in Tunisia (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_f: DENOMINATOR },)

        t024c033_t = OBSColumn(
            id='t024c033_t',
            name='Recent immigrants born in Cameroon (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_t: DENOMINATOR },)

        t024c033_m = OBSColumn(
            id='t024c033_m',
            name='Recent immigrants born in Cameroon (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_m: DENOMINATOR },)

        t024c033_f = OBSColumn(
            id='t024c033_f',
            name='Recent immigrants born in Cameroon (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_f: DENOMINATOR },)

        t024c034_t = OBSColumn(
            id='t024c034_t',
            name='Recent immigrants born in Congo, The Democratic Republic of the (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_t: DENOMINATOR },)

        t024c034_m = OBSColumn(
            id='t024c034_m',
            name='Recent immigrants born in Congo, The Democratic Republic of the (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_m: DENOMINATOR },)

        t024c034_f = OBSColumn(
            id='t024c034_f',
            name='Recent immigrants born in Congo, The Democratic Republic of the (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_f: DENOMINATOR },)

        t024c035_t = OBSColumn(
            id='t024c035_t',
            name='Recent immigrants born in South Africa, Republic of (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_t: DENOMINATOR },)

        t024c035_m = OBSColumn(
            id='t024c035_m',
            name='Recent immigrants born in South Africa, Republic of (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_m: DENOMINATOR },)

        t024c035_f = OBSColumn(
            id='t024c035_f',
            name='Recent immigrants born in South Africa, Republic of (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_f: DENOMINATOR },)

        t024c036_t = OBSColumn(
            id='t024c036_t',
            name='Recent immigrants born in other places in Africa (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_t: DENOMINATOR },)

        t024c036_m = OBSColumn(
            id='t024c036_m',
            name='Recent immigrants born in other places in Africa (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_m: DENOMINATOR },)

        t024c036_f = OBSColumn(
            id='t024c036_f',
            name='Recent immigrants born in other places in Africa (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c024_f: DENOMINATOR },)

        t024c037_t = OBSColumn(
            id='t024c037_t',
            name='Recent immigrants born in Asia (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c001_t: DENOMINATOR },)

        t024c037_m = OBSColumn(
            id='t024c037_m',
            name='Recent immigrants born in Asia (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c001_m: DENOMINATOR },)

        t024c037_f = OBSColumn(
            id='t024c037_f',
            name='Recent immigrants born in Asia (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c001_f: DENOMINATOR },)

        t024c038_t = OBSColumn(
            id='t024c038_t',
            name='Recent immigrants born in Philippines (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c038_m = OBSColumn(
            id='t024c038_m',
            name='Recent immigrants born in Philippines (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c038_f = OBSColumn(
            id='t024c038_f',
            name='Recent immigrants born in Philippines (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c039_t = OBSColumn(
            id='t024c039_t',
            name='Recent immigrants born in China (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c039_m = OBSColumn(
            id='t024c039_m',
            name='Recent immigrants born in China (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c039_f = OBSColumn(
            id='t024c039_f',
            name='Recent immigrants born in China (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c040_t = OBSColumn(
            id='t024c040_t',
            name='Recent immigrants born in India (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c040_m = OBSColumn(
            id='t024c040_m',
            name='Recent immigrants born in India (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c040_f = OBSColumn(
            id='t024c040_f',
            name='Recent immigrants born in India (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c041_t = OBSColumn(
            id='t024c041_t',
            name='Recent immigrants born in Pakistan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c041_m = OBSColumn(
            id='t024c041_m',
            name='Recent immigrants born in Pakistan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c041_f = OBSColumn(
            id='t024c041_f',
            name='Recent immigrants born in Pakistan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c042_t = OBSColumn(
            id='t024c042_t',
            name='Recent immigrants born in Iran (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c042_m = OBSColumn(
            id='t024c042_m',
            name='Recent immigrants born in Iran (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c042_f = OBSColumn(
            id='t024c042_f',
            name='Recent immigrants born in Iran (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c043_t = OBSColumn(
            id='t024c043_t',
            name='Recent immigrants born in South Korea (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c043_m = OBSColumn(
            id='t024c043_m',
            name='Recent immigrants born in South Korea (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c043_f = OBSColumn(
            id='t024c043_f',
            name='Recent immigrants born in South Korea (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c044_t = OBSColumn(
            id='t024c044_t',
            name='Recent immigrants born in Sri Lanka (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c044_m = OBSColumn(
            id='t024c044_m',
            name='Recent immigrants born in Sri Lanka (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c044_f = OBSColumn(
            id='t024c044_f',
            name='Recent immigrants born in Sri Lanka (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c045_t = OBSColumn(
            id='t024c045_t',
            name='Recent immigrants born in Iraq (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c045_m = OBSColumn(
            id='t024c045_m',
            name='Recent immigrants born in Iraq (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c045_f = OBSColumn(
            id='t024c045_f',
            name='Recent immigrants born in Iraq (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c046_t = OBSColumn(
            id='t024c046_t',
            name='Recent immigrants born in Bangladesh (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c046_m = OBSColumn(
            id='t024c046_m',
            name='Recent immigrants born in Bangladesh (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c046_f = OBSColumn(
            id='t024c046_f',
            name='Recent immigrants born in Bangladesh (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c047_t = OBSColumn(
            id='t024c047_t',
            name='Recent immigrants born in Lebanon (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c047_m = OBSColumn(
            id='t024c047_m',
            name='Recent immigrants born in Lebanon (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c047_f = OBSColumn(
            id='t024c047_f',
            name='Recent immigrants born in Lebanon (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c048_t = OBSColumn(
            id='t024c048_t',
            name='Recent immigrants born in Viet Nam (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c048_m = OBSColumn(
            id='t024c048_m',
            name='Recent immigrants born in Viet Nam (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c048_f = OBSColumn(
            id='t024c048_f',
            name='Recent immigrants born in Viet Nam (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c049_t = OBSColumn(
            id='t024c049_t',
            name='Recent immigrants born in Taiwan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c049_m = OBSColumn(
            id='t024c049_m',
            name='Recent immigrants born in Taiwan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c049_f = OBSColumn(
            id='t024c049_f',
            name='Recent immigrants born in Taiwan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c050_t = OBSColumn(
            id='t024c050_t',
            name='Recent immigrants born in Afghanistan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c050_m = OBSColumn(
            id='t024c050_m',
            name='Recent immigrants born in Afghanistan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c050_f = OBSColumn(
            id='t024c050_f',
            name='Recent immigrants born in Afghanistan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c051_t = OBSColumn(
            id='t024c051_t',
            name='Recent immigrants born in Japan (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c051_m = OBSColumn(
            id='t024c051_m',
            name='Recent immigrants born in Japan (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c051_f = OBSColumn(
            id='t024c051_f',
            name='Recent immigrants born in Japan (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c052_t = OBSColumn(
            id='t024c052_t',
            name='Recent immigrants born in Turkey (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c052_m = OBSColumn(
            id='t024c052_m',
            name='Recent immigrants born in Turkey (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c052_f = OBSColumn(
            id='t024c052_f',
            name='Recent immigrants born in Turkey (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c053_t = OBSColumn(
            id='t024c053_t',
            name='Recent immigrants born in Israel (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c053_m = OBSColumn(
            id='t024c053_m',
            name='Recent immigrants born in Israel (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c053_f = OBSColumn(
            id='t024c053_f',
            name='Recent immigrants born in Israel (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c054_t = OBSColumn(
            id='t024c054_t',
            name='Recent immigrants born in Nepal (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c054_m = OBSColumn(
            id='t024c054_m',
            name='Recent immigrants born in Nepal (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c054_f = OBSColumn(
            id='t024c054_f',
            name='Recent immigrants born in Nepal (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c055_t = OBSColumn(
            id='t024c055_t',
            name='Recent immigrants born in Hong Kong Special Administrative Region (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c055_m = OBSColumn(
            id='t024c055_m',
            name='Recent immigrants born in Hong Kong Special Administrative Region (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c055_f = OBSColumn(
            id='t024c055_f',
            name='Recent immigrants born in Hong Kong Special Administrative Region (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c056_t = OBSColumn(
            id='t024c056_t',
            name='Recent immigrants born in United Arab Emirates (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c056_m = OBSColumn(
            id='t024c056_m',
            name='Recent immigrants born in United Arab Emirates (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c056_f = OBSColumn(
            id='t024c056_f',
            name='Recent immigrants born in United Arab Emirates (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c057_t = OBSColumn(
            id='t024c057_t',
            name='Recent immigrants born in Saudi Arabia (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c057_m = OBSColumn(
            id='t024c057_m',
            name='Recent immigrants born in Saudi Arabia (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c057_f = OBSColumn(
            id='t024c057_f',
            name='Recent immigrants born in Saudi Arabia (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c058_t = OBSColumn(
            id='t024c058_t',
            name='Recent immigrants born in Syria (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c058_m = OBSColumn(
            id='t024c058_m',
            name='Recent immigrants born in Syria (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c058_f = OBSColumn(
            id='t024c058_f',
            name='Recent immigrants born in Syria (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c059_t = OBSColumn(
            id='t024c059_t',
            name='Recent immigrants born in other places in Asia (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_t: DENOMINATOR },)

        t024c059_m = OBSColumn(
            id='t024c059_m',
            name='Recent immigrants born in other places in Asia (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_m: DENOMINATOR },)

        t024c059_f = OBSColumn(
            id='t024c059_f',
            name='Recent immigrants born in other places in Asia (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c037_f: DENOMINATOR },)

        t024c060_t = OBSColumn(
            id='t024c060_t',
            name='Recent immigrants born in Oceania and other (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c001_t: DENOMINATOR },)

        t024c060_m = OBSColumn(
            id='t024c060_m',
            name='Recent immigrants born in Oceania and other (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c001_m: DENOMINATOR },)

        t024c060_f = OBSColumn(
            id='t024c060_f',
            name='Recent immigrants born in Oceania and other (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['nationality']],
            targets={ t024c001_f: DENOMINATOR },)

        t025c002_t = OBSColumn(
            id='t025c002_t',
            name='People who are Buddhist (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_t: DENOMINATOR },)

        t025c002_m = OBSColumn(
            id='t025c002_m',
            name='People who are Buddhist (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_m: DENOMINATOR },)

        t025c002_f = OBSColumn(
            id='t025c002_f',
            name='People who are Buddhist (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_f: DENOMINATOR },)

        t025c003_t = OBSColumn(
            id='t025c003_t',
            name='People who are Christian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_t: DENOMINATOR },)

        t025c003_m = OBSColumn(
            id='t025c003_m',
            name='People who are Christian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_m: DENOMINATOR },)

        t025c003_f = OBSColumn(
            id='t025c003_f',
            name='People who are Christian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_f: DENOMINATOR },)

        t025c004_t = OBSColumn(
            id='t025c004_t',
            name='People who are Anglican (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_t: DENOMINATOR },)

        t025c004_m = OBSColumn(
            id='t025c004_m',
            name='People who are Anglican (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_m: DENOMINATOR },)

        t025c004_f = OBSColumn(
            id='t025c004_f',
            name='People who are Anglican (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_f: DENOMINATOR },)

        t025c005_t = OBSColumn(
            id='t025c005_t',
            name='People who are Baptist (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_t: DENOMINATOR },)

        t025c005_m = OBSColumn(
            id='t025c005_m',
            name='People who are Baptist (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_m: DENOMINATOR },)

        t025c005_f = OBSColumn(
            id='t025c005_f',
            name='People who are Baptist (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_f: DENOMINATOR },)

        t025c006_t = OBSColumn(
            id='t025c006_t',
            name='People who are Catholic (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_t: DENOMINATOR },)

        t025c006_m = OBSColumn(
            id='t025c006_m',
            name='People who are Catholic (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_m: DENOMINATOR },)

        t025c006_f = OBSColumn(
            id='t025c006_f',
            name='People who are Catholic (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_f: DENOMINATOR },)

        t025c007_t = OBSColumn(
            id='t025c007_t',
            name='People who are Christian Orthodox (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_t: DENOMINATOR },)

        t025c007_m = OBSColumn(
            id='t025c007_m',
            name='People who are Christian Orthodox (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_m: DENOMINATOR },)

        t025c007_f = OBSColumn(
            id='t025c007_f',
            name='People who are Christian Orthodox (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_f: DENOMINATOR },)

        t025c008_t = OBSColumn(
            id='t025c008_t',
            name='People who are Lutheran (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_t: DENOMINATOR },)

        t025c008_m = OBSColumn(
            id='t025c008_m',
            name='People who are Lutheran (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_m: DENOMINATOR },)

        t025c008_f = OBSColumn(
            id='t025c008_f',
            name='People who are Lutheran (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_f: DENOMINATOR },)

        t025c009_t = OBSColumn(
            id='t025c009_t',
            name='People who are Pentecostal (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_t: DENOMINATOR },)

        t025c009_m = OBSColumn(
            id='t025c009_m',
            name='People who are Pentecostal (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_m: DENOMINATOR },)

        t025c009_f = OBSColumn(
            id='t025c009_f',
            name='People who are Pentecostal (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_f: DENOMINATOR },)

        t025c010_t = OBSColumn(
            id='t025c010_t',
            name='People who are Presbyterian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_t: DENOMINATOR },)

        t025c010_m = OBSColumn(
            id='t025c010_m',
            name='People who are Presbyterian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_m: DENOMINATOR },)

        t025c010_f = OBSColumn(
            id='t025c010_f',
            name='People who are Presbyterian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_f: DENOMINATOR },)

        t025c011_t = OBSColumn(
            id='t025c011_t',
            name='People who are United Church (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_t: DENOMINATOR },)

        t025c011_m = OBSColumn(
            id='t025c011_m',
            name='People who are United Church (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_m: DENOMINATOR },)

        t025c011_f = OBSColumn(
            id='t025c011_f',
            name='People who are United Church (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_f: DENOMINATOR },)

        t025c012_t = OBSColumn(
            id='t025c012_t',
            name='People who are other Christian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_t: DENOMINATOR },)

        t025c012_m = OBSColumn(
            id='t025c012_m',
            name='People who are other Christian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_m: DENOMINATOR },)

        t025c012_f = OBSColumn(
            id='t025c012_f',
            name='People who are other Christian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t025c003_f: DENOMINATOR },)

        t025c013_t = OBSColumn(
            id='t025c013_t',
            name='People who are Hindu (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_t: DENOMINATOR },)

        t025c013_m = OBSColumn(
            id='t025c013_m',
            name='People who are Hindu (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_m: DENOMINATOR },)

        t025c013_f = OBSColumn(
            id='t025c013_f',
            name='People who are Hindu (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_f: DENOMINATOR },)

        t025c014_t = OBSColumn(
            id='t025c014_t',
            name='People who are Jewish (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_t: DENOMINATOR },)

        t025c014_m = OBSColumn(
            id='t025c014_m',
            name='People who are Jewish (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_m: DENOMINATOR },)

        t025c014_f = OBSColumn(
            id='t025c014_f',
            name='People who are Jewish (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_f: DENOMINATOR },)

        t025c015_t = OBSColumn(
            id='t025c015_t',
            name='People who are Muslim (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_t: DENOMINATOR },)

        t025c015_m = OBSColumn(
            id='t025c015_m',
            name='People who are Muslim (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_m: DENOMINATOR },)

        t025c015_f = OBSColumn(
            id='t025c015_f',
            name='People who are Muslim (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_f: DENOMINATOR },)

        t025c016_t = OBSColumn(
            id='t025c016_t',
            name='People who are Sikh (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_t: DENOMINATOR },)

        t025c016_m = OBSColumn(
            id='t025c016_m',
            name='People who are Sikh (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_m: DENOMINATOR },)

        t025c016_f = OBSColumn(
            id='t025c016_f',
            name='People who are Sikh (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_f: DENOMINATOR },)

        t025c017_t = OBSColumn(
            id='t025c017_t',
            name='People who practice traditional (Aboriginal) Spirituality (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_t: DENOMINATOR },)

        t025c017_m = OBSColumn(
            id='t025c017_m',
            name='People who practice traditional (Aboriginal) Spirituality (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_m: DENOMINATOR },)

        t025c017_f = OBSColumn(
            id='t025c017_f',
            name='People who practice traditional (Aboriginal) Spirituality (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_f: DENOMINATOR },)

        t025c018_t = OBSColumn(
            id='t025c018_t',
            name='People who practice other religions (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_t: DENOMINATOR },)

        t025c018_m = OBSColumn(
            id='t025c018_m',
            name='People who practice other religions (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_m: DENOMINATOR },)

        t025c018_f = OBSColumn(
            id='t025c018_f',
            name='People who practice other religions (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_f: DENOMINATOR },)

        t025c019_t = OBSColumn(
            id='t025c019_t',
            name='People who have no religious affiliation (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_t: DENOMINATOR },)

        t025c019_m = OBSColumn(
            id='t025c019_m',
            name='People who have no religious affiliation (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_m: DENOMINATOR },)

        t025c019_f = OBSColumn(
            id='t025c019_f',
            name='People who have no religious affiliation (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['religion']],
            targets={ t001c001_f: DENOMINATOR },)

        t026c001_t = OBSColumn(
            id='t026c001_t',
            name='Total number of owner and tenant households with household total income greater than zero, in non-farm, non-reserve private dwellings by shelter-cost-to-income ratio',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={},)

        t026c002_t = OBSColumn(
            id='t026c002_t',
            name='Spending less than 30% of household total income on shelter costs',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t026c001_t: DENOMINATOR },)

        t026c003_t = OBSColumn(
            id='t026c003_t',
            name='Spending 30% or more of household total income on shelter costs',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t026c001_t: DENOMINATOR },)

        t026c004_t = OBSColumn(
            id='t026c004_t',
            name='Spending 30% to less than 100% of household total income on shelter costs',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={ t026c003_t: DENOMINATOR },)

        t026c005_t = OBSColumn(
            id='t026c005_t',
            name='Number of owner households in non-farm, non-reserve private dwellings',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={},)

        t026c006_t = OBSColumn(
            id='t026c006_t',
            name='% of owner households with a mortgage',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['housing']],
            targets={ t026c005_t: UNIVERSE },)

        t026c007_t = OBSColumn(
            id='t026c007_t',
            name='% of owner households spending 30% or more of household total income on shelter costs',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['housing']],
            targets={ t026c005_t: UNIVERSE },)

        t026c008_t = OBSColumn(
            id='t026c008_t',
            name='Median monthly shelter costs for owned dwellings ($)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['housing']],
            targets={ t026c005_t: UNIVERSE },)

        t026c009_t = OBSColumn(
            id='t026c009_t',
            name='Average monthly shelter costs for owned dwellings ($)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['housing']],
            targets={ t026c005_t: UNIVERSE },)

        t026c010_t = OBSColumn(
            id='t026c010_t',
            name='Median value of dwellings ($)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['housing']],
            targets={ t026c005_t: UNIVERSE },)

        t026c011_t = OBSColumn(
            id='t026c011_t',
            name='Average value of dwellings ($)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['housing']],
            targets={ t026c005_t: UNIVERSE },)

        t026c012_t = OBSColumn(
            id='t026c012_t',
            name='Number of tenant households in non-farm, non-reserve private dwellings',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['housing']],
            targets={},)

        t026c013_t = OBSColumn(
            id='t026c013_t',
            name='% of tenant households in subsidized housing',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['housing']],
            targets={ t026c012_t: UNIVERSE },)

        t026c014_t = OBSColumn(
            id='t026c014_t',
            name='% of tenant households spending 30% or more of household total income on shelter costs',
            type='Numeric',
            weight=3,
            tags=[ca, unit_ratio, subsections['housing']],
            targets={ t026c012_t: UNIVERSE },)

        t026c015_t = OBSColumn(
            id='t026c015_t',
            name='Median monthly shelter costs for rented dwellings ($)',
            type='Numeric',
            weight=3,
            aggregate='median',
            tags=[ca, unit_money, subsections['housing']],
            targets={ t026c012_t: UNIVERSE },)

        t026c016_t = OBSColumn(
            id='t026c016_t',
            name='Average monthly shelter costs for rented dwellings ($)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit_money, subsections['housing']],
            targets={ t026c012_t: UNIVERSE },)

        t027c002_t = OBSColumn(
            id='t027c002_t',
            name='People aged 15+ who commute to work between 5 and 6:59 a.m. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_t: DENOMINATOR },)

        t027c002_m = OBSColumn(
            id='t027c002_m',
            name='People aged 15+ who commute to work between 5 and 6:59 a.m. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_m: DENOMINATOR },)

        t027c002_f = OBSColumn(
            id='t027c002_f',
            name='People aged 15+ who commute to work between 5 and 6:59 a.m. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_f: DENOMINATOR },)

        t027c003_t = OBSColumn(
            id='t027c003_t',
            name='People aged 15+ who commute to work between 7 and 9:00 a.m. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_t: DENOMINATOR },)

        t027c003_m = OBSColumn(
            id='t027c003_m',
            name='People aged 15+ who commute to work between 7 and 9:00 a.m. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_m: DENOMINATOR },)

        t027c003_f = OBSColumn(
            id='t027c003_f',
            name='People aged 15+ who commute to work between 7 and 9:00 a.m. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_f: DENOMINATOR },)

        t027c004_t = OBSColumn(
            id='t027c004_t',
            name='People aged 15+ who commute to work anytime after 9:00 a.m. (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_t: DENOMINATOR },)

        t027c004_m = OBSColumn(
            id='t027c004_m',
            name='People aged 15+ who commute to work anytime after 9:00 a.m. (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_m: DENOMINATOR },)

        t027c004_f = OBSColumn(
            id='t027c004_f',
            name='People aged 15+ who commute to work anytime after 9:00 a.m. (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['transportation']],
            targets={ t017c001_f: DENOMINATOR },)

        t028c002_t = OBSColumn(
            id='t028c002_t',
            name='People with a visible minority (total)',
            description='The Employment Equity Act defines visible minorities as "persons, other than Aboriginal peoples, who are non-Caucasian in race or non-white in colour."',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_t: DENOMINATOR },)

        t028c002_m = OBSColumn(
            id='t028c002_m',
            name='People with a visible minority (male)',
            description='The Employment Equity Act defines visible minorities as "persons, other than Aboriginal peoples, who are non-Caucasian in race or non-white in colour."',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_m: DENOMINATOR },)

        t028c002_f = OBSColumn(
            id='t028c002_f',
            name='People with a visible minority (female)',
            description='The Employment Equity Act defines visible minorities as "persons, other than Aboriginal peoples, who are non-Caucasian in race or non-white in colour."',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_f: DENOMINATOR },)

        t028c003_t = OBSColumn(
            id='t028c003_t',
            name='People with a visible minority - South Asian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_t: DENOMINATOR },)

        t028c003_m = OBSColumn(
            id='t028c003_m',
            name='People with a visible minority - South Asian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_m: DENOMINATOR },)

        t028c003_f = OBSColumn(
            id='t028c003_f',
            name='People with a visible minority - South Asian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_f: DENOMINATOR },)

        t028c004_t = OBSColumn(
            id='t028c004_t',
            name='People with a visible minority - Chinese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_t: DENOMINATOR },)

        t028c004_m = OBSColumn(
            id='t028c004_m',
            name='People with a visible minority - Chinese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_m: DENOMINATOR },)

        t028c004_f = OBSColumn(
            id='t028c004_f',
            name='People with a visible minority - Chinese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_f: DENOMINATOR },)

        t028c005_t = OBSColumn(
            id='t028c005_t',
            name='People with a visible minority - Black (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_t: DENOMINATOR },)

        t028c005_m = OBSColumn(
            id='t028c005_m',
            name='People with a visible minority - Black (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_m: DENOMINATOR },)

        t028c005_f = OBSColumn(
            id='t028c005_f',
            name='People with a visible minority - Black (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_f: DENOMINATOR },)

        t028c006_t = OBSColumn(
            id='t028c006_t',
            name='People with a visible minority - Filipino (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_t: DENOMINATOR },)

        t028c006_m = OBSColumn(
            id='t028c006_m',
            name='People with a visible minority - Filipino (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_m: DENOMINATOR },)

        t028c006_f = OBSColumn(
            id='t028c006_f',
            name='People with a visible minority - Filipino (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_f: DENOMINATOR },)

        t028c007_t = OBSColumn(
            id='t028c007_t',
            name='People with a visible minority - Latin American (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_t: DENOMINATOR },)

        t028c007_m = OBSColumn(
            id='t028c007_m',
            name='People with a visible minority - Latin American (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_m: DENOMINATOR },)

        t028c007_f = OBSColumn(
            id='t028c007_f',
            name='People with a visible minority - Latin American (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_f: DENOMINATOR },)

        t028c008_t = OBSColumn(
            id='t028c008_t',
            name='People with a visible minority - Arab (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_t: DENOMINATOR },)

        t028c008_m = OBSColumn(
            id='t028c008_m',
            name='People with a visible minority - Arab (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_m: DENOMINATOR },)

        t028c008_f = OBSColumn(
            id='t028c008_f',
            name='People with a visible minority - Arab (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_f: DENOMINATOR },)

        t028c009_t = OBSColumn(
            id='t028c009_t',
            name='People with a visible minority - Southeast Asian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_t: DENOMINATOR },)

        t028c009_m = OBSColumn(
            id='t028c009_m',
            name='People with a visible minority - Southeast Asian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_m: DENOMINATOR },)

        t028c009_f = OBSColumn(
            id='t028c009_f',
            name='People with a visible minority - Southeast Asian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_f: DENOMINATOR },)

        t028c010_t = OBSColumn(
            id='t028c010_t',
            name='People with a visible minority - West Asian (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_t: DENOMINATOR },)

        t028c010_m = OBSColumn(
            id='t028c010_m',
            name='People with a visible minority - West Asian (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_m: DENOMINATOR },)

        t028c010_f = OBSColumn(
            id='t028c010_f',
            name='People with a visible minority - West Asian (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_f: DENOMINATOR },)

        t028c011_t = OBSColumn(
            id='t028c011_t',
            name='People with a visible minority - Korean (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_t: DENOMINATOR },)

        t028c011_m = OBSColumn(
            id='t028c011_m',
            name='People with a visible minority - Korean (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_m: DENOMINATOR },)

        t028c011_f = OBSColumn(
            id='t028c011_f',
            name='People with a visible minority - Korean (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_f: DENOMINATOR },)

        t028c012_t = OBSColumn(
            id='t028c012_t',
            name='People with a visible minority - Japanese (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_t: DENOMINATOR },)

        t028c012_m = OBSColumn(
            id='t028c012_m',
            name='People with a visible minority - Japanese (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_m: DENOMINATOR },)

        t028c012_f = OBSColumn(
            id='t028c012_f',
            name='People with a visible minority - Japanese (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_f: DENOMINATOR },)

        t028c013_t = OBSColumn(
            id='t028c013_t',
            name='People with a visible minority - n.i.e. (total)',
            description='The abbreviation "n.i.e." means "not included elsewhere." Includes respondents who reported a write-in response such as "Guyanese," "West Indian," "Tibetan," "Polynesian," "Pacific Islander," etc.',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_t: DENOMINATOR },)

        t028c013_m = OBSColumn(
            id='t028c013_m',
            name='People with a visible minority - n.i.e. (male)',
            description='The abbreviation "n.i.e." means "not included elsewhere." Includes respondents who reported a write-in response such as "Guyanese," "West Indian," "Tibetan," "Polynesian," "Pacific Islander," etc.',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_m: DENOMINATOR },)

        t028c013_f = OBSColumn(
            id='t028c013_f',
            name='People with a visible minority - n.i.e. (female)',
            description='The abbreviation "n.i.e." means "not included elsewhere." Includes respondents who reported a write-in response such as "Guyanese," "West Indian," "Tibetan," "Polynesian," "Pacific Islander," etc.',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_f: DENOMINATOR },)

        t028c014_t = OBSColumn(
            id='t028c014_t',
            name='People with multiple visible minorities (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_t: DENOMINATOR },)

        t028c014_m = OBSColumn(
            id='t028c014_m',
            name='People with multiple visible minorities (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_m: DENOMINATOR },)

        t028c014_f = OBSColumn(
            id='t028c014_f',
            name='People with multiple visible minorities (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t028c002_f: DENOMINATOR },)

        t028c015_t = OBSColumn(
            id='t028c015_t',
            name='People without a visible minority (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_t: DENOMINATOR },)

        t028c015_m = OBSColumn(
            id='t028c015_m',
            name='People without a visible minority (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_m: DENOMINATOR },)

        t028c015_f = OBSColumn(
            id='t028c015_f',
            name='People without a visible minority (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['race_ethnicity']],
            targets={ t001c001_f: DENOMINATOR },)

        t029c002_t = OBSColumn(
            id='t029c002_t',
            name='People aged 15+ in the labour force who did not work in 2010 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_t: DENOMINATOR },)

        t029c002_m = OBSColumn(
            id='t029c002_m',
            name='People aged 15+ in the labour force who did not work in 2010 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_m: DENOMINATOR },)

        t029c002_f = OBSColumn(
            id='t029c002_f',
            name='People aged 15+ in the labour force who did not work in 2010 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_f: DENOMINATOR },)

        t029c003_t = OBSColumn(
            id='t029c003_t',
            name='People aged 15+ in the labour force who worked in 2010 (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_t: DENOMINATOR },)

        t029c003_m = OBSColumn(
            id='t029c003_m',
            name='People aged 15+ in the labour force who worked in 2010 (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_m: DENOMINATOR },)

        t029c003_f = OBSColumn(
            id='t029c003_f',
            name='People aged 15+ in the labour force who worked in 2010 (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t004c001_f: DENOMINATOR },)

        t029c004_t = OBSColumn(
            id='t029c004_t',
            name='People aged 15+ in the labour force who worked 1 to 13 weeks (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t029c003_t: DENOMINATOR },)

        t029c004_m = OBSColumn(
            id='t029c004_m',
            name='People aged 15+ in the labour force who worked 1 to 13 weeks (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t029c003_m: DENOMINATOR },)

        t029c004_f = OBSColumn(
            id='t029c004_f',
            name='People aged 15+ in the labour force who worked 1 to 13 weeks (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t029c003_f: DENOMINATOR },)

        t029c005_t = OBSColumn(
            id='t029c005_t',
            name='People aged 15+ in the labour force who worked 14 to 26 weeks (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t029c003_t: DENOMINATOR },)

        t029c005_m = OBSColumn(
            id='t029c005_m',
            name='People aged 15+ in the labour force who worked 14 to 26 weeks (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t029c003_m: DENOMINATOR },)

        t029c005_f = OBSColumn(
            id='t029c005_f',
            name='People aged 15+ in the labour force who worked 14 to 26 weeks (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t029c003_f: DENOMINATOR },)

        t029c006_t = OBSColumn(
            id='t029c006_t',
            name='People aged 15+ in the labour force who worked 27 to 39 weeks (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t029c003_t: DENOMINATOR },)

        t029c006_m = OBSColumn(
            id='t029c006_m',
            name='People aged 15+ in the labour force who worked 27 to 39 weeks (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t029c003_m: DENOMINATOR },)

        t029c006_f = OBSColumn(
            id='t029c006_f',
            name='People aged 15+ in the labour force who worked 27 to 39 weeks (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t029c003_f: DENOMINATOR },)

        t029c007_t = OBSColumn(
            id='t029c007_t',
            name='People aged 15+ in the labour force who worked 40 to 48 weeks (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t029c003_t: DENOMINATOR },)

        t029c007_m = OBSColumn(
            id='t029c007_m',
            name='People aged 15+ in the labour force who worked 40 to 48 weeks (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t029c003_m: DENOMINATOR },)

        t029c007_f = OBSColumn(
            id='t029c007_f',
            name='People aged 15+ in the labour force who worked 40 to 48 weeks (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t029c003_f: DENOMINATOR },)

        t029c008_t = OBSColumn(
            id='t029c008_t',
            name='People aged 15+ in the labour force who worked 49 to 52 weeks (total)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t029c003_t: DENOMINATOR },)

        t029c008_m = OBSColumn(
            id='t029c008_m',
            name='People aged 15+ in the labour force who worked 49 to 52 weeks (male)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t029c003_m: DENOMINATOR },)

        t029c008_f = OBSColumn(
            id='t029c008_f',
            name='People aged 15+ in the labour force who worked 49 to 52 weeks (female)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[ca, unit_people, subsections['employment']],
            targets={ t029c003_f: DENOMINATOR },)

        t029c009_t = OBSColumn(
            id='t029c009_t',
            name='Average weeks worked in 2010 (total)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit['weeks'], subsections['employment']],
            targets={},)

        t029c009_m = OBSColumn(
            id='t029c009_m',
            name='Average weeks worked in 2010 (male)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit['weeks'], subsections['employment']],
            targets={},)

        t029c009_f = OBSColumn(
            id='t029c009_f',
            name='Average weeks worked in 2010 (female)',
            type='Numeric',
            weight=3,
            aggregate='average',
            tags=[ca, unit['weeks'], subsections['employment']],
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
            ('t001c010_t', t001c010_t),
            ('t001c010_m', t001c010_m),
            ('t001c010_f', t001c010_f),
            ('t001c011_t', t001c011_t),
            ('t001c011_m', t001c011_m),
            ('t001c011_f', t001c011_f),
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
            ('t004c001_t', t004c001_t),
            ('t004c001_m', t004c001_m),
            ('t004c001_f', t004c001_f),
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
            ('t005c001_t', t005c001_t),
            ('t005c001_m', t005c001_m),
            ('t005c001_f', t005c001_f),
            ('t005c002_t', t005c002_t),
            ('t005c002_m', t005c002_m),
            ('t005c002_f', t005c002_f),
            ('t005c003_t', t005c003_t),
            ('t005c003_m', t005c003_m),
            ('t005c003_f', t005c003_f),
            ('t005c004_t', t005c004_t),
            ('t005c004_m', t005c004_m),
            ('t005c004_f', t005c004_f),
            ('t005c005_t', t005c005_t),
            ('t005c005_m', t005c005_m),
            ('t005c005_f', t005c005_f),
            ('t005c006_t', t005c006_t),
            ('t005c006_m', t005c006_m),
            ('t005c006_f', t005c006_f),
            ('t005c007_t', t005c007_t),
            ('t005c007_m', t005c007_m),
            ('t005c007_f', t005c007_f),
            ('t005c008_t', t005c008_t),
            ('t005c008_m', t005c008_m),
            ('t005c008_f', t005c008_f),
            ('t005c009_t', t005c009_t),
            ('t005c009_m', t005c009_m),
            ('t005c009_f', t005c009_f),
            ('t005c010_t', t005c010_t),
            ('t005c010_m', t005c010_m),
            ('t005c010_f', t005c010_f),
            ('t005c011_t', t005c011_t),
            ('t005c011_m', t005c011_m),
            ('t005c011_f', t005c011_f),
            ('t005c012_t', t005c012_t),
            ('t005c012_m', t005c012_m),
            ('t005c012_f', t005c012_f),
            ('t005c013_t', t005c013_t),
            ('t005c013_m', t005c013_m),
            ('t005c013_f', t005c013_f),
            ('t005c014_t', t005c014_t),
            ('t005c014_m', t005c014_m),
            ('t005c014_f', t005c014_f),
            ('t005c015_t', t005c015_t),
            ('t005c015_m', t005c015_m),
            ('t005c015_f', t005c015_f),
            ('t005c016_t', t005c016_t),
            ('t005c016_m', t005c016_m),
            ('t005c016_f', t005c016_f),
            ('t005c017_t', t005c017_t),
            ('t005c017_m', t005c017_m),
            ('t005c017_f', t005c017_f),
            ('t005c018_t', t005c018_t),
            ('t005c018_m', t005c018_m),
            ('t005c018_f', t005c018_f),
            ('t005c019_t', t005c019_t),
            ('t005c019_m', t005c019_m),
            ('t005c019_f', t005c019_f),
            ('t005c020_t', t005c020_t),
            ('t005c020_m', t005c020_m),
            ('t005c020_f', t005c020_f),
            ('t005c022_t', t005c022_t),
            ('t005c022_m', t005c022_m),
            ('t005c022_f', t005c022_f),
            ('t005c023_t', t005c023_t),
            ('t005c023_m', t005c023_m),
            ('t005c023_f', t005c023_f),
            ('t005c024_t', t005c024_t),
            ('t005c024_m', t005c024_m),
            ('t005c024_f', t005c024_f),
            ('t005c025_t', t005c025_t),
            ('t005c025_m', t005c025_m),
            ('t005c025_f', t005c025_f),
            ('t005c026_t', t005c026_t),
            ('t005c026_m', t005c026_m),
            ('t005c026_f', t005c026_f),
            ('t005c027_t', t005c027_t),
            ('t005c027_m', t005c027_m),
            ('t005c027_f', t005c027_f),
            ('t005c028_t', t005c028_t),
            ('t005c028_m', t005c028_m),
            ('t005c028_f', t005c028_f),
            ('t005c029_t', t005c029_t),
            ('t005c029_m', t005c029_m),
            ('t005c029_f', t005c029_f),
            ('t005c030_t', t005c030_t),
            ('t005c030_m', t005c030_m),
            ('t005c030_f', t005c030_f),
            ('t005c031_t', t005c031_t),
            ('t005c031_m', t005c031_m),
            ('t005c031_f', t005c031_f),
            ('t005c032_t', t005c032_t),
            ('t005c032_m', t005c032_m),
            ('t005c032_f', t005c032_f),
            ('t005c033_t', t005c033_t),
            ('t005c033_m', t005c033_m),
            ('t005c033_f', t005c033_f),
            ('t005c034_t', t005c034_t),
            ('t005c034_m', t005c034_m),
            ('t005c034_f', t005c034_f),
            ('t005c038_t', t005c038_t),
            ('t005c038_m', t005c038_m),
            ('t005c038_f', t005c038_f),
            ('t005c039_t', t005c039_t),
            ('t005c039_m', t005c039_m),
            ('t005c039_f', t005c039_f),
            ('t005c040_t', t005c040_t),
            ('t005c040_m', t005c040_m),
            ('t005c040_f', t005c040_f),
            ('t005c041_t', t005c041_t),
            ('t005c041_m', t005c041_m),
            ('t005c041_f', t005c041_f),
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
            ('t006c008_t', t006c008_t),
            ('t006c008_m', t006c008_m),
            ('t006c008_f', t006c008_f),
            ('t006c009_t', t006c009_t),
            ('t006c009_m', t006c009_m),
            ('t006c009_f', t006c009_f),
            ('t006c010_t', t006c010_t),
            ('t006c010_m', t006c010_m),
            ('t006c010_f', t006c010_f),
            ('t006c011_t', t006c011_t),
            ('t006c011_m', t006c011_m),
            ('t006c011_f', t006c011_f),
            ('t006c012_t', t006c012_t),
            ('t006c012_m', t006c012_m),
            ('t006c012_f', t006c012_f),
            ('t006c013_t', t006c013_t),
            ('t006c013_m', t006c013_m),
            ('t006c013_f', t006c013_f),
            ('t006c014_t', t006c014_t),
            ('t006c014_m', t006c014_m),
            ('t006c014_f', t006c014_f),
            ('t006c015_t', t006c015_t),
            ('t006c015_m', t006c015_m),
            ('t006c015_f', t006c015_f),
            ('t006c016_t', t006c016_t),
            ('t006c016_m', t006c016_m),
            ('t006c016_f', t006c016_f),
            ('t006c017_t', t006c017_t),
            ('t006c017_m', t006c017_m),
            ('t006c017_f', t006c017_f),
            ('t006c018_t', t006c018_t),
            ('t006c018_m', t006c018_m),
            ('t006c018_f', t006c018_f),
            ('t006c019_t', t006c019_t),
            ('t006c019_m', t006c019_m),
            ('t006c019_f', t006c019_f),
            ('t006c020_t', t006c020_t),
            ('t006c020_m', t006c020_m),
            ('t006c020_f', t006c020_f),
            ('t006c021_t', t006c021_t),
            ('t006c021_m', t006c021_m),
            ('t006c021_f', t006c021_f),
            ('t006c022_t', t006c022_t),
            ('t006c022_m', t006c022_m),
            ('t006c022_f', t006c022_f),
            ('t006c023_t', t006c023_t),
            ('t006c023_m', t006c023_m),
            ('t006c023_f', t006c023_f),
            ('t006c024_t', t006c024_t),
            ('t006c024_m', t006c024_m),
            ('t006c024_f', t006c024_f),
            ('t006c025_t', t006c025_t),
            ('t006c025_m', t006c025_m),
            ('t006c025_f', t006c025_f),
            ('t006c026_t', t006c026_t),
            ('t006c026_m', t006c026_m),
            ('t006c026_f', t006c026_f),
            ('t006c027_t', t006c027_t),
            ('t006c027_m', t006c027_m),
            ('t006c027_f', t006c027_f),
            ('t006c028_t', t006c028_t),
            ('t006c028_m', t006c028_m),
            ('t006c028_f', t006c028_f),
            ('t006c029_t', t006c029_t),
            ('t006c029_m', t006c029_m),
            ('t006c029_f', t006c029_f),
            ('t006c030_t', t006c030_t),
            ('t006c030_m', t006c030_m),
            ('t006c030_f', t006c030_f),
            ('t006c031_t', t006c031_t),
            ('t006c031_m', t006c031_m),
            ('t006c031_f', t006c031_f),
            ('t006c032_t', t006c032_t),
            ('t006c032_m', t006c032_m),
            ('t006c032_f', t006c032_f),
            ('t006c033_t', t006c033_t),
            ('t006c033_m', t006c033_m),
            ('t006c033_f', t006c033_f),
            ('t006c034_t', t006c034_t),
            ('t006c034_m', t006c034_m),
            ('t006c034_f', t006c034_f),
            ('t006c035_t', t006c035_t),
            ('t006c035_m', t006c035_m),
            ('t006c035_f', t006c035_f),
            ('t006c036_t', t006c036_t),
            ('t006c036_m', t006c036_m),
            ('t006c036_f', t006c036_f),
            ('t006c037_t', t006c037_t),
            ('t006c037_m', t006c037_m),
            ('t006c037_f', t006c037_f),
            ('t006c038_t', t006c038_t),
            ('t006c038_m', t006c038_m),
            ('t006c038_f', t006c038_f),
            ('t006c039_t', t006c039_t),
            ('t006c039_m', t006c039_m),
            ('t006c039_f', t006c039_f),
            ('t006c040_t', t006c040_t),
            ('t006c040_m', t006c040_m),
            ('t006c040_f', t006c040_f),
            ('t006c041_t', t006c041_t),
            ('t006c041_m', t006c041_m),
            ('t006c041_f', t006c041_f),
            ('t006c042_t', t006c042_t),
            ('t006c042_m', t006c042_m),
            ('t006c042_f', t006c042_f),
            ('t006c043_t', t006c043_t),
            ('t006c043_m', t006c043_m),
            ('t006c043_f', t006c043_f),
            ('t006c044_t', t006c044_t),
            ('t006c044_m', t006c044_m),
            ('t006c044_f', t006c044_f),
            ('t006c045_t', t006c045_t),
            ('t006c045_m', t006c045_m),
            ('t006c045_f', t006c045_f),
            ('t006c046_t', t006c046_t),
            ('t006c046_m', t006c046_m),
            ('t006c046_f', t006c046_f),
            ('t006c047_t', t006c047_t),
            ('t006c047_m', t006c047_m),
            ('t006c047_f', t006c047_f),
            ('t006c048_t', t006c048_t),
            ('t006c048_m', t006c048_m),
            ('t006c048_f', t006c048_f),
            ('t006c049_t', t006c049_t),
            ('t006c049_m', t006c049_m),
            ('t006c049_f', t006c049_f),
            ('t006c050_t', t006c050_t),
            ('t006c050_m', t006c050_m),
            ('t006c050_f', t006c050_f),
            ('t006c051_t', t006c051_t),
            ('t006c051_m', t006c051_m),
            ('t006c051_f', t006c051_f),
            ('t006c052_t', t006c052_t),
            ('t006c052_m', t006c052_m),
            ('t006c052_f', t006c052_f),
            ('t006c053_t', t006c053_t),
            ('t006c053_m', t006c053_m),
            ('t006c053_f', t006c053_f),
            ('t006c054_t', t006c054_t),
            ('t006c054_m', t006c054_m),
            ('t006c054_f', t006c054_f),
            ('t006c055_t', t006c055_t),
            ('t006c055_m', t006c055_m),
            ('t006c055_f', t006c055_f),
            ('t006c056_t', t006c056_t),
            ('t006c056_m', t006c056_m),
            ('t006c056_f', t006c056_f),
            ('t006c057_t', t006c057_t),
            ('t006c057_m', t006c057_m),
            ('t006c057_f', t006c057_f),
            ('t006c058_t', t006c058_t),
            ('t006c058_m', t006c058_m),
            ('t006c058_f', t006c058_f),
            ('t006c059_t', t006c059_t),
            ('t006c059_m', t006c059_m),
            ('t006c059_f', t006c059_f),
            ('t006c060_t', t006c060_t),
            ('t006c060_m', t006c060_m),
            ('t006c060_f', t006c060_f),
            ('t006c061_t', t006c061_t),
            ('t006c061_m', t006c061_m),
            ('t006c061_f', t006c061_f),
            ('t006c062_t', t006c062_t),
            ('t006c062_m', t006c062_m),
            ('t006c062_f', t006c062_f),
            ('t006c063_t', t006c063_t),
            ('t006c063_m', t006c063_m),
            ('t006c063_f', t006c063_f),
            ('t006c064_t', t006c064_t),
            ('t006c064_m', t006c064_m),
            ('t006c064_f', t006c064_f),
            ('t006c065_t', t006c065_t),
            ('t006c065_m', t006c065_m),
            ('t006c065_f', t006c065_f),
            ('t006c066_t', t006c066_t),
            ('t006c066_m', t006c066_m),
            ('t006c066_f', t006c066_f),
            ('t006c067_t', t006c067_t),
            ('t006c067_m', t006c067_m),
            ('t006c067_f', t006c067_f),
            ('t006c068_t', t006c068_t),
            ('t006c068_m', t006c068_m),
            ('t006c068_f', t006c068_f),
            ('t006c069_t', t006c069_t),
            ('t006c069_m', t006c069_m),
            ('t006c069_f', t006c069_f),
            ('t006c070_t', t006c070_t),
            ('t006c070_m', t006c070_m),
            ('t006c070_f', t006c070_f),
            ('t006c071_t', t006c071_t),
            ('t006c071_m', t006c071_m),
            ('t006c071_f', t006c071_f),
            ('t006c072_t', t006c072_t),
            ('t006c072_m', t006c072_m),
            ('t006c072_f', t006c072_f),
            ('t006c073_t', t006c073_t),
            ('t006c073_m', t006c073_m),
            ('t006c073_f', t006c073_f),
            ('t006c074_t', t006c074_t),
            ('t006c074_m', t006c074_m),
            ('t006c074_f', t006c074_f),
            ('t006c075_t', t006c075_t),
            ('t006c075_m', t006c075_m),
            ('t006c075_f', t006c075_f),
            ('t006c076_t', t006c076_t),
            ('t006c076_m', t006c076_m),
            ('t006c076_f', t006c076_f),
            ('t006c077_t', t006c077_t),
            ('t006c077_m', t006c077_m),
            ('t006c077_f', t006c077_f),
            ('t006c078_t', t006c078_t),
            ('t006c078_m', t006c078_m),
            ('t006c078_f', t006c078_f),
            ('t006c079_t', t006c079_t),
            ('t006c079_m', t006c079_m),
            ('t006c079_f', t006c079_f),
            ('t006c080_t', t006c080_t),
            ('t006c080_m', t006c080_m),
            ('t006c080_f', t006c080_f),
            ('t006c081_t', t006c081_t),
            ('t006c081_m', t006c081_m),
            ('t006c081_f', t006c081_f),
            ('t006c082_t', t006c082_t),
            ('t006c082_m', t006c082_m),
            ('t006c082_f', t006c082_f),
            ('t006c083_t', t006c083_t),
            ('t006c083_m', t006c083_m),
            ('t006c083_f', t006c083_f),
            ('t006c084_t', t006c084_t),
            ('t006c084_m', t006c084_m),
            ('t006c084_f', t006c084_f),
            ('t006c085_t', t006c085_t),
            ('t006c085_m', t006c085_m),
            ('t006c085_f', t006c085_f),
            ('t006c086_t', t006c086_t),
            ('t006c086_m', t006c086_m),
            ('t006c086_f', t006c086_f),
            ('t006c087_t', t006c087_t),
            ('t006c087_m', t006c087_m),
            ('t006c087_f', t006c087_f),
            ('t006c088_t', t006c088_t),
            ('t006c088_m', t006c088_m),
            ('t006c088_f', t006c088_f),
            ('t006c089_t', t006c089_t),
            ('t006c089_m', t006c089_m),
            ('t006c089_f', t006c089_f),
            ('t006c090_t', t006c090_t),
            ('t006c090_m', t006c090_m),
            ('t006c090_f', t006c090_f),
            ('t006c091_t', t006c091_t),
            ('t006c091_m', t006c091_m),
            ('t006c091_f', t006c091_f),
            ('t006c092_t', t006c092_t),
            ('t006c092_m', t006c092_m),
            ('t006c092_f', t006c092_f),
            ('t006c093_t', t006c093_t),
            ('t006c093_m', t006c093_m),
            ('t006c093_f', t006c093_f),
            ('t006c094_t', t006c094_t),
            ('t006c094_m', t006c094_m),
            ('t006c094_f', t006c094_f),
            ('t006c095_t', t006c095_t),
            ('t006c095_m', t006c095_m),
            ('t006c095_f', t006c095_f),
            ('t006c096_t', t006c096_t),
            ('t006c096_m', t006c096_m),
            ('t006c096_f', t006c096_f),
            ('t006c097_t', t006c097_t),
            ('t006c097_m', t006c097_m),
            ('t006c097_f', t006c097_f),
            ('t006c098_t', t006c098_t),
            ('t006c098_m', t006c098_m),
            ('t006c098_f', t006c098_f),
            ('t006c099_t', t006c099_t),
            ('t006c099_m', t006c099_m),
            ('t006c099_f', t006c099_f),
            ('t006c100_t', t006c100_t),
            ('t006c100_m', t006c100_m),
            ('t006c100_f', t006c100_f),
            ('t006c101_t', t006c101_t),
            ('t006c101_m', t006c101_m),
            ('t006c101_f', t006c101_f),
            ('t006c102_t', t006c102_t),
            ('t006c102_m', t006c102_m),
            ('t006c102_f', t006c102_f),
            ('t006c103_t', t006c103_t),
            ('t006c103_m', t006c103_m),
            ('t006c103_f', t006c103_f),
            ('t006c104_t', t006c104_t),
            ('t006c104_m', t006c104_m),
            ('t006c104_f', t006c104_f),
            ('t006c105_t', t006c105_t),
            ('t006c105_m', t006c105_m),
            ('t006c105_f', t006c105_f),
            ('t006c106_t', t006c106_t),
            ('t006c106_m', t006c106_m),
            ('t006c106_f', t006c106_f),
            ('t006c107_t', t006c107_t),
            ('t006c107_m', t006c107_m),
            ('t006c107_f', t006c107_f),
            ('t006c108_t', t006c108_t),
            ('t006c108_m', t006c108_m),
            ('t006c108_f', t006c108_f),
            ('t006c109_t', t006c109_t),
            ('t006c109_m', t006c109_m),
            ('t006c109_f', t006c109_f),
            ('t006c110_t', t006c110_t),
            ('t006c110_m', t006c110_m),
            ('t006c110_f', t006c110_f),
            ('t006c111_t', t006c111_t),
            ('t006c111_m', t006c111_m),
            ('t006c111_f', t006c111_f),
            ('t006c112_t', t006c112_t),
            ('t006c112_m', t006c112_m),
            ('t006c112_f', t006c112_f),
            ('t006c113_t', t006c113_t),
            ('t006c113_m', t006c113_m),
            ('t006c113_f', t006c113_f),
            ('t006c114_t', t006c114_t),
            ('t006c114_m', t006c114_m),
            ('t006c114_f', t006c114_f),
            ('t006c115_t', t006c115_t),
            ('t006c115_m', t006c115_m),
            ('t006c115_f', t006c115_f),
            ('t006c116_t', t006c116_t),
            ('t006c116_m', t006c116_m),
            ('t006c116_f', t006c116_f),
            ('t006c117_t', t006c117_t),
            ('t006c117_m', t006c117_m),
            ('t006c117_f', t006c117_f),
            ('t006c118_t', t006c118_t),
            ('t006c118_m', t006c118_m),
            ('t006c118_f', t006c118_f),
            ('t006c119_t', t006c119_t),
            ('t006c119_m', t006c119_m),
            ('t006c119_f', t006c119_f),
            ('t006c120_t', t006c120_t),
            ('t006c120_m', t006c120_m),
            ('t006c120_f', t006c120_f),
            ('t006c121_t', t006c121_t),
            ('t006c121_m', t006c121_m),
            ('t006c121_f', t006c121_f),
            ('t006c122_t', t006c122_t),
            ('t006c122_m', t006c122_m),
            ('t006c122_f', t006c122_f),
            ('t006c123_t', t006c123_t),
            ('t006c123_m', t006c123_m),
            ('t006c123_f', t006c123_f),
            ('t006c124_t', t006c124_t),
            ('t006c124_m', t006c124_m),
            ('t006c124_f', t006c124_f),
            ('t006c125_t', t006c125_t),
            ('t006c125_m', t006c125_m),
            ('t006c125_f', t006c125_f),
            ('t006c126_t', t006c126_t),
            ('t006c126_m', t006c126_m),
            ('t006c126_f', t006c126_f),
            ('t006c127_t', t006c127_t),
            ('t006c127_m', t006c127_m),
            ('t006c127_f', t006c127_f),
            ('t006c128_t', t006c128_t),
            ('t006c128_m', t006c128_m),
            ('t006c128_f', t006c128_f),
            ('t006c129_t', t006c129_t),
            ('t006c129_m', t006c129_m),
            ('t006c129_f', t006c129_f),
            ('t006c130_t', t006c130_t),
            ('t006c130_m', t006c130_m),
            ('t006c130_f', t006c130_f),
            ('t006c131_t', t006c131_t),
            ('t006c131_m', t006c131_m),
            ('t006c131_f', t006c131_f),
            ('t006c132_t', t006c132_t),
            ('t006c132_m', t006c132_m),
            ('t006c132_f', t006c132_f),
            ('t006c133_t', t006c133_t),
            ('t006c133_m', t006c133_m),
            ('t006c133_f', t006c133_f),
            ('t006c134_t', t006c134_t),
            ('t006c134_m', t006c134_m),
            ('t006c134_f', t006c134_f),
            ('t006c135_t', t006c135_t),
            ('t006c135_m', t006c135_m),
            ('t006c135_f', t006c135_f),
            ('t006c136_t', t006c136_t),
            ('t006c136_m', t006c136_m),
            ('t006c136_f', t006c136_f),
            ('t006c137_t', t006c137_t),
            ('t006c137_m', t006c137_m),
            ('t006c137_f', t006c137_f),
            ('t006c138_t', t006c138_t),
            ('t006c138_m', t006c138_m),
            ('t006c138_f', t006c138_f),
            ('t006c139_t', t006c139_t),
            ('t006c139_m', t006c139_m),
            ('t006c139_f', t006c139_f),
            ('t006c140_t', t006c140_t),
            ('t006c140_m', t006c140_m),
            ('t006c140_f', t006c140_f),
            ('t006c141_t', t006c141_t),
            ('t006c141_m', t006c141_m),
            ('t006c141_f', t006c141_f),
            ('t006c142_t', t006c142_t),
            ('t006c142_m', t006c142_m),
            ('t006c142_f', t006c142_f),
            ('t006c143_t', t006c143_t),
            ('t006c143_m', t006c143_m),
            ('t006c143_f', t006c143_f),
            ('t006c144_t', t006c144_t),
            ('t006c144_m', t006c144_m),
            ('t006c144_f', t006c144_f),
            ('t006c145_t', t006c145_t),
            ('t006c145_m', t006c145_m),
            ('t006c145_f', t006c145_f),
            ('t006c146_t', t006c146_t),
            ('t006c146_m', t006c146_m),
            ('t006c146_f', t006c146_f),
            ('t006c147_t', t006c147_t),
            ('t006c147_m', t006c147_m),
            ('t006c147_f', t006c147_f),
            ('t006c148_t', t006c148_t),
            ('t006c148_m', t006c148_m),
            ('t006c148_f', t006c148_f),
            ('t006c149_t', t006c149_t),
            ('t006c149_m', t006c149_m),
            ('t006c149_f', t006c149_f),
            ('t006c150_t', t006c150_t),
            ('t006c150_m', t006c150_m),
            ('t006c150_f', t006c150_f),
            ('t006c151_t', t006c151_t),
            ('t006c151_m', t006c151_m),
            ('t006c151_f', t006c151_f),
            ('t006c152_t', t006c152_t),
            ('t006c152_m', t006c152_m),
            ('t006c152_f', t006c152_f),
            ('t006c153_t', t006c153_t),
            ('t006c153_m', t006c153_m),
            ('t006c153_f', t006c153_f),
            ('t006c154_t', t006c154_t),
            ('t006c154_m', t006c154_m),
            ('t006c154_f', t006c154_f),
            ('t006c155_t', t006c155_t),
            ('t006c155_m', t006c155_m),
            ('t006c155_f', t006c155_f),
            ('t006c156_t', t006c156_t),
            ('t006c156_m', t006c156_m),
            ('t006c156_f', t006c156_f),
            ('t006c157_t', t006c157_t),
            ('t006c157_m', t006c157_m),
            ('t006c157_f', t006c157_f),
            ('t006c158_t', t006c158_t),
            ('t006c158_m', t006c158_m),
            ('t006c158_f', t006c158_f),
            ('t006c159_t', t006c159_t),
            ('t006c159_m', t006c159_m),
            ('t006c159_f', t006c159_f),
            ('t006c160_t', t006c160_t),
            ('t006c160_m', t006c160_m),
            ('t006c160_f', t006c160_f),
            ('t006c161_t', t006c161_t),
            ('t006c161_m', t006c161_m),
            ('t006c161_f', t006c161_f),
            ('t006c162_t', t006c162_t),
            ('t006c162_m', t006c162_m),
            ('t006c162_f', t006c162_f),
            ('t006c163_t', t006c163_t),
            ('t006c163_m', t006c163_m),
            ('t006c163_f', t006c163_f),
            ('t006c164_t', t006c164_t),
            ('t006c164_m', t006c164_m),
            ('t006c164_f', t006c164_f),
            ('t006c165_t', t006c165_t),
            ('t006c165_m', t006c165_m),
            ('t006c165_f', t006c165_f),
            ('t006c166_t', t006c166_t),
            ('t006c166_m', t006c166_m),
            ('t006c166_f', t006c166_f),
            ('t006c167_t', t006c167_t),
            ('t006c167_m', t006c167_m),
            ('t006c167_f', t006c167_f),
            ('t006c168_t', t006c168_t),
            ('t006c168_m', t006c168_m),
            ('t006c168_f', t006c168_f),
            ('t006c169_t', t006c169_t),
            ('t006c169_m', t006c169_m),
            ('t006c169_f', t006c169_f),
            ('t006c170_t', t006c170_t),
            ('t006c170_m', t006c170_m),
            ('t006c170_f', t006c170_f),
            ('t006c171_t', t006c171_t),
            ('t006c171_m', t006c171_m),
            ('t006c171_f', t006c171_f),
            ('t006c172_t', t006c172_t),
            ('t006c172_m', t006c172_m),
            ('t006c172_f', t006c172_f),
            ('t006c173_t', t006c173_t),
            ('t006c173_m', t006c173_m),
            ('t006c173_f', t006c173_f),
            ('t006c174_t', t006c174_t),
            ('t006c174_m', t006c174_m),
            ('t006c174_f', t006c174_f),
            ('t006c175_t', t006c175_t),
            ('t006c175_m', t006c175_m),
            ('t006c175_f', t006c175_f),
            ('t006c176_t', t006c176_t),
            ('t006c176_m', t006c176_m),
            ('t006c176_f', t006c176_f),
            ('t006c177_t', t006c177_t),
            ('t006c177_m', t006c177_m),
            ('t006c177_f', t006c177_f),
            ('t006c178_t', t006c178_t),
            ('t006c178_m', t006c178_m),
            ('t006c178_f', t006c178_f),
            ('t006c179_t', t006c179_t),
            ('t006c179_m', t006c179_m),
            ('t006c179_f', t006c179_f),
            ('t006c180_t', t006c180_t),
            ('t006c180_m', t006c180_m),
            ('t006c180_f', t006c180_f),
            ('t006c181_t', t006c181_t),
            ('t006c181_m', t006c181_m),
            ('t006c181_f', t006c181_f),
            ('t006c182_t', t006c182_t),
            ('t006c182_m', t006c182_m),
            ('t006c182_f', t006c182_f),
            ('t006c183_t', t006c183_t),
            ('t006c183_m', t006c183_m),
            ('t006c183_f', t006c183_f),
            ('t006c184_t', t006c184_t),
            ('t006c184_m', t006c184_m),
            ('t006c184_f', t006c184_f),
            ('t006c185_t', t006c185_t),
            ('t006c185_m', t006c185_m),
            ('t006c185_f', t006c185_f),
            ('t006c186_t', t006c186_t),
            ('t006c186_m', t006c186_m),
            ('t006c186_f', t006c186_f),
            ('t006c187_t', t006c187_t),
            ('t006c187_m', t006c187_m),
            ('t006c187_f', t006c187_f),
            ('t006c188_t', t006c188_t),
            ('t006c188_m', t006c188_m),
            ('t006c188_f', t006c188_f),
            ('t006c189_t', t006c189_t),
            ('t006c189_m', t006c189_m),
            ('t006c189_f', t006c189_f),
            ('t006c190_t', t006c190_t),
            ('t006c190_m', t006c190_m),
            ('t006c190_f', t006c190_f),
            ('t006c191_t', t006c191_t),
            ('t006c191_m', t006c191_m),
            ('t006c191_f', t006c191_f),
            ('t006c192_t', t006c192_t),
            ('t006c192_m', t006c192_m),
            ('t006c192_f', t006c192_f),
            ('t006c193_t', t006c193_t),
            ('t006c193_m', t006c193_m),
            ('t006c193_f', t006c193_f),
            ('t006c194_t', t006c194_t),
            ('t006c194_m', t006c194_m),
            ('t006c194_f', t006c194_f),
            ('t006c195_t', t006c195_t),
            ('t006c195_m', t006c195_m),
            ('t006c195_f', t006c195_f),
            ('t006c196_t', t006c196_t),
            ('t006c196_m', t006c196_m),
            ('t006c196_f', t006c196_f),
            ('t006c197_t', t006c197_t),
            ('t006c197_m', t006c197_m),
            ('t006c197_f', t006c197_f),
            ('t006c198_t', t006c198_t),
            ('t006c198_m', t006c198_m),
            ('t006c198_f', t006c198_f),
            ('t006c199_t', t006c199_t),
            ('t006c199_m', t006c199_m),
            ('t006c199_f', t006c199_f),
            ('t006c200_t', t006c200_t),
            ('t006c200_m', t006c200_m),
            ('t006c200_f', t006c200_f),
            ('t006c201_t', t006c201_t),
            ('t006c201_m', t006c201_m),
            ('t006c201_f', t006c201_f),
            ('t006c202_t', t006c202_t),
            ('t006c202_m', t006c202_m),
            ('t006c202_f', t006c202_f),
            ('t006c203_t', t006c203_t),
            ('t006c203_m', t006c203_m),
            ('t006c203_f', t006c203_f),
            ('t006c204_t', t006c204_t),
            ('t006c204_m', t006c204_m),
            ('t006c204_f', t006c204_f),
            ('t006c205_t', t006c205_t),
            ('t006c205_m', t006c205_m),
            ('t006c205_f', t006c205_f),
            ('t006c206_t', t006c206_t),
            ('t006c206_m', t006c206_m),
            ('t006c206_f', t006c206_f),
            ('t006c207_t', t006c207_t),
            ('t006c207_m', t006c207_m),
            ('t006c207_f', t006c207_f),
            ('t006c208_t', t006c208_t),
            ('t006c208_m', t006c208_m),
            ('t006c208_f', t006c208_f),
            ('t006c209_t', t006c209_t),
            ('t006c209_m', t006c209_m),
            ('t006c209_f', t006c209_f),
            ('t006c210_t', t006c210_t),
            ('t006c210_m', t006c210_m),
            ('t006c210_f', t006c210_f),
            ('t006c211_t', t006c211_t),
            ('t006c211_m', t006c211_m),
            ('t006c211_f', t006c211_f),
            ('t006c212_t', t006c212_t),
            ('t006c212_m', t006c212_m),
            ('t006c212_f', t006c212_f),
            ('t006c213_t', t006c213_t),
            ('t006c213_m', t006c213_m),
            ('t006c213_f', t006c213_f),
            ('t006c214_t', t006c214_t),
            ('t006c214_m', t006c214_m),
            ('t006c214_f', t006c214_f),
            ('t006c215_t', t006c215_t),
            ('t006c215_m', t006c215_m),
            ('t006c215_f', t006c215_f),
            ('t006c216_t', t006c216_t),
            ('t006c216_m', t006c216_m),
            ('t006c216_f', t006c216_f),
            ('t006c217_t', t006c217_t),
            ('t006c217_m', t006c217_m),
            ('t006c217_f', t006c217_f),
            ('t006c218_t', t006c218_t),
            ('t006c218_m', t006c218_m),
            ('t006c218_f', t006c218_f),
            ('t006c219_t', t006c219_t),
            ('t006c219_m', t006c219_m),
            ('t006c219_f', t006c219_f),
            ('t006c220_t', t006c220_t),
            ('t006c220_m', t006c220_m),
            ('t006c220_f', t006c220_f),
            ('t006c221_t', t006c221_t),
            ('t006c221_m', t006c221_m),
            ('t006c221_f', t006c221_f),
            ('t006c222_t', t006c222_t),
            ('t006c222_m', t006c222_m),
            ('t006c222_f', t006c222_f),
            ('t006c223_t', t006c223_t),
            ('t006c223_m', t006c223_m),
            ('t006c223_f', t006c223_f),
            ('t006c224_t', t006c224_t),
            ('t006c224_m', t006c224_m),
            ('t006c224_f', t006c224_f),
            ('t006c225_t', t006c225_t),
            ('t006c225_m', t006c225_m),
            ('t006c225_f', t006c225_f),
            ('t006c226_t', t006c226_t),
            ('t006c226_m', t006c226_m),
            ('t006c226_f', t006c226_f),
            ('t006c227_t', t006c227_t),
            ('t006c227_m', t006c227_m),
            ('t006c227_f', t006c227_f),
            ('t006c228_t', t006c228_t),
            ('t006c228_m', t006c228_m),
            ('t006c228_f', t006c228_f),
            ('t006c229_t', t006c229_t),
            ('t006c229_m', t006c229_m),
            ('t006c229_f', t006c229_f),
            ('t006c230_t', t006c230_t),
            ('t006c230_m', t006c230_m),
            ('t006c230_f', t006c230_f),
            ('t006c231_t', t006c231_t),
            ('t006c231_m', t006c231_m),
            ('t006c231_f', t006c231_f),
            ('t006c232_t', t006c232_t),
            ('t006c232_m', t006c232_m),
            ('t006c232_f', t006c232_f),
            ('t006c233_t', t006c233_t),
            ('t006c233_m', t006c233_m),
            ('t006c233_f', t006c233_f),
            ('t006c234_t', t006c234_t),
            ('t006c234_m', t006c234_m),
            ('t006c234_f', t006c234_f),
            ('t006c235_t', t006c235_t),
            ('t006c235_m', t006c235_m),
            ('t006c235_f', t006c235_f),
            ('t006c236_t', t006c236_t),
            ('t006c236_m', t006c236_m),
            ('t006c236_f', t006c236_f),
            ('t006c237_t', t006c237_t),
            ('t006c237_m', t006c237_m),
            ('t006c237_f', t006c237_f),
            ('t006c238_t', t006c238_t),
            ('t006c238_m', t006c238_m),
            ('t006c238_f', t006c238_f),
            ('t006c239_t', t006c239_t),
            ('t006c239_m', t006c239_m),
            ('t006c239_f', t006c239_f),
            ('t006c240_t', t006c240_t),
            ('t006c240_m', t006c240_m),
            ('t006c240_f', t006c240_f),
            ('t006c241_t', t006c241_t),
            ('t006c241_m', t006c241_m),
            ('t006c241_f', t006c241_f),
            ('t006c242_t', t006c242_t),
            ('t006c242_m', t006c242_m),
            ('t006c242_f', t006c242_f),
            ('t006c243_t', t006c243_t),
            ('t006c243_m', t006c243_m),
            ('t006c243_f', t006c243_f),
            ('t006c244_t', t006c244_t),
            ('t006c244_m', t006c244_m),
            ('t006c244_f', t006c244_f),
            ('t006c245_t', t006c245_t),
            ('t006c245_m', t006c245_m),
            ('t006c245_f', t006c245_f),
            ('t006c246_t', t006c246_t),
            ('t006c246_m', t006c246_m),
            ('t006c246_f', t006c246_f),
            ('t006c247_t', t006c247_t),
            ('t006c247_m', t006c247_m),
            ('t006c247_f', t006c247_f),
            ('t006c248_t', t006c248_t),
            ('t006c248_m', t006c248_m),
            ('t006c248_f', t006c248_f),
            ('t006c249_t', t006c249_t),
            ('t006c249_m', t006c249_m),
            ('t006c249_f', t006c249_f),
            ('t006c250_t', t006c250_t),
            ('t006c250_m', t006c250_m),
            ('t006c250_f', t006c250_f),
            ('t006c251_t', t006c251_t),
            ('t006c251_m', t006c251_m),
            ('t006c251_f', t006c251_f),
            ('t006c252_t', t006c252_t),
            ('t006c252_m', t006c252_m),
            ('t006c252_f', t006c252_f),
            ('t006c253_t', t006c253_t),
            ('t006c253_m', t006c253_m),
            ('t006c253_f', t006c253_f),
            ('t006c254_t', t006c254_t),
            ('t006c254_m', t006c254_m),
            ('t006c254_f', t006c254_f),
            ('t006c255_t', t006c255_t),
            ('t006c255_m', t006c255_m),
            ('t006c255_f', t006c255_f),
            ('t006c256_t', t006c256_t),
            ('t006c256_m', t006c256_m),
            ('t006c256_f', t006c256_f),
            ('t006c257_t', t006c257_t),
            ('t006c257_m', t006c257_m),
            ('t006c257_f', t006c257_f),
            ('t006c258_t', t006c258_t),
            ('t006c258_m', t006c258_m),
            ('t006c258_f', t006c258_f),
            ('t006c259_t', t006c259_t),
            ('t006c259_m', t006c259_m),
            ('t006c259_f', t006c259_f),
            ('t006c260_t', t006c260_t),
            ('t006c260_m', t006c260_m),
            ('t006c260_f', t006c260_f),
            ('t006c261_t', t006c261_t),
            ('t006c261_m', t006c261_m),
            ('t006c261_f', t006c261_f),
            ('t006c262_t', t006c262_t),
            ('t006c262_m', t006c262_m),
            ('t006c262_f', t006c262_f),
            ('t006c263_t', t006c263_t),
            ('t006c263_m', t006c263_m),
            ('t006c263_f', t006c263_f),
            ('t006c264_t', t006c264_t),
            ('t006c264_m', t006c264_m),
            ('t006c264_f', t006c264_f),
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
            ('t008c002_t', t008c002_t),
            ('t008c002_m', t008c002_m),
            ('t008c002_f', t008c002_f),
            ('t008c003_t', t008c003_t),
            ('t008c003_m', t008c003_m),
            ('t008c003_f', t008c003_f),
            ('t008c004_t', t008c004_t),
            ('t008c004_m', t008c004_m),
            ('t008c004_f', t008c004_f),
            ('t009c001_t', t009c001_t),
            ('t009c002_t', t009c002_t),
            ('t009c003_t', t009c003_t),
            ('t009c004_t', t009c004_t),
            ('t009c006_t', t009c006_t),
            ('t009c007_t', t009c007_t),
            ('t009c009_t', t009c009_t),
            ('t009c010_t', t009c010_t),
            ('t009c011_t', t009c011_t),
            ('t009c013_t', t009c013_t),
            ('t009c014_t', t009c014_t),
            ('t009c015_t', t009c015_t),
            ('t009c016_t', t009c016_t),
            ('t009c017_t', t009c017_t),
            ('t009c018_t', t009c018_t),
            ('t009c019_t', t009c019_t),
            ('t009c021_t', t009c021_t),
            ('t009c022_t', t009c022_t),
            ('t009c024_t', t009c024_t),
            ('t009c025_t', t009c025_t),
            ('t010c002_t', t010c002_t),
            ('t010c002_m', t010c002_m),
            ('t010c002_f', t010c002_f),
            ('t010c003_t', t010c003_t),
            ('t010c003_m', t010c003_m),
            ('t010c003_f', t010c003_f),
            ('t010c004_t', t010c004_t),
            ('t010c004_m', t010c004_m),
            ('t010c004_f', t010c004_f),
            ('t010c005_t', t010c005_t),
            ('t010c005_m', t010c005_m),
            ('t010c005_f', t010c005_f),
            ('t010c006_t', t010c006_t),
            ('t010c006_m', t010c006_m),
            ('t010c006_f', t010c006_f),
            ('t010c007_t', t010c007_t),
            ('t010c007_m', t010c007_m),
            ('t010c007_f', t010c007_f),
            ('t010c008_t', t010c008_t),
            ('t010c008_m', t010c008_m),
            ('t010c008_f', t010c008_f),
            ('t010c009_t', t010c009_t),
            ('t010c009_m', t010c009_m),
            ('t010c009_f', t010c009_f),
            ('t010c010_t', t010c010_t),
            ('t010c010_m', t010c010_m),
            ('t010c010_f', t010c010_f),
            ('t010c011_t', t010c011_t),
            ('t010c011_m', t010c011_m),
            ('t010c011_f', t010c011_f),
            ('t011c002_t', t011c002_t),
            ('t011c002_m', t011c002_m),
            ('t011c002_f', t011c002_f),
            ('t011c003_t', t011c003_t),
            ('t011c003_m', t011c003_m),
            ('t011c003_f', t011c003_f),
            ('t011c004_t', t011c004_t),
            ('t011c004_m', t011c004_m),
            ('t011c004_f', t011c004_f),
            ('t011c005_t', t011c005_t),
            ('t011c005_m', t011c005_m),
            ('t011c005_f', t011c005_f),
            ('t011c006_t', t011c006_t),
            ('t011c006_m', t011c006_m),
            ('t011c006_f', t011c006_f),
            ('t011c007_t', t011c007_t),
            ('t011c007_m', t011c007_m),
            ('t011c007_f', t011c007_f),
            ('t011c008_t', t011c008_t),
            ('t011c008_m', t011c008_m),
            ('t011c008_f', t011c008_f),
            ('t011c009_t', t011c009_t),
            ('t011c009_m', t011c009_m),
            ('t011c009_f', t011c009_f),
            ('t011c010_t', t011c010_t),
            ('t011c010_m', t011c010_m),
            ('t011c010_f', t011c010_f),
            ('t011c011_t', t011c011_t),
            ('t011c011_m', t011c011_m),
            ('t011c011_f', t011c011_f),
            ('t011c012_t', t011c012_t),
            ('t011c012_m', t011c012_m),
            ('t011c012_f', t011c012_f),
            ('t011c013_t', t011c013_t),
            ('t011c013_m', t011c013_m),
            ('t011c013_f', t011c013_f),
            ('t011c014_t', t011c014_t),
            ('t011c014_m', t011c014_m),
            ('t011c014_f', t011c014_f),
            ('t011c015_t', t011c015_t),
            ('t011c015_m', t011c015_m),
            ('t011c015_f', t011c015_f),
            ('t011c016_t', t011c016_t),
            ('t011c016_m', t011c016_m),
            ('t011c016_f', t011c016_f),
            ('t011c017_t', t011c017_t),
            ('t011c017_m', t011c017_m),
            ('t011c017_f', t011c017_f),
            ('t011c018_t', t011c018_t),
            ('t011c018_m', t011c018_m),
            ('t011c018_f', t011c018_f),
            ('t011c019_t', t011c019_t),
            ('t011c019_m', t011c019_m),
            ('t011c019_f', t011c019_f),
            ('t011c020_t', t011c020_t),
            ('t011c020_m', t011c020_m),
            ('t011c020_f', t011c020_f),
            ('t011c021_t', t011c021_t),
            ('t011c021_m', t011c021_m),
            ('t011c021_f', t011c021_f),
            ('t011c022_t', t011c022_t),
            ('t011c022_m', t011c022_m),
            ('t011c022_f', t011c022_f),
            ('t011c023_t', t011c023_t),
            ('t011c023_m', t011c023_m),
            ('t011c023_f', t011c023_f),
            ('t011c024_t', t011c024_t),
            ('t011c024_m', t011c024_m),
            ('t011c024_f', t011c024_f),
            ('t011c025_t', t011c025_t),
            ('t011c025_m', t011c025_m),
            ('t011c025_f', t011c025_f),
            ('t011c026_t', t011c026_t),
            ('t011c026_m', t011c026_m),
            ('t011c026_f', t011c026_f),
            ('t011c027_t', t011c027_t),
            ('t011c027_m', t011c027_m),
            ('t011c027_f', t011c027_f),
            ('t011c028_t', t011c028_t),
            ('t011c028_m', t011c028_m),
            ('t011c028_f', t011c028_f),
            ('t011c029_t', t011c029_t),
            ('t011c029_m', t011c029_m),
            ('t011c029_f', t011c029_f),
            ('t011c030_t', t011c030_t),
            ('t011c030_m', t011c030_m),
            ('t011c030_f', t011c030_f),
            ('t011c031_t', t011c031_t),
            ('t011c031_m', t011c031_m),
            ('t011c031_f', t011c031_f),
            ('t011c032_t', t011c032_t),
            ('t011c032_m', t011c032_m),
            ('t011c032_f', t011c032_f),
            ('t011c033_t', t011c033_t),
            ('t011c033_m', t011c033_m),
            ('t011c033_f', t011c033_f),
            ('t011c034_t', t011c034_t),
            ('t011c034_m', t011c034_m),
            ('t011c034_f', t011c034_f),
            ('t011c035_t', t011c035_t),
            ('t011c035_m', t011c035_m),
            ('t011c035_f', t011c035_f),
            ('t011c036_t', t011c036_t),
            ('t011c036_m', t011c036_m),
            ('t011c036_f', t011c036_f),
            ('t011c037_t', t011c037_t),
            ('t011c037_m', t011c037_m),
            ('t011c037_f', t011c037_f),
            ('t011c038_t', t011c038_t),
            ('t011c038_m', t011c038_m),
            ('t011c038_f', t011c038_f),
            ('t011c039_t', t011c039_t),
            ('t011c039_m', t011c039_m),
            ('t011c039_f', t011c039_f),
            ('t011c040_t', t011c040_t),
            ('t011c040_m', t011c040_m),
            ('t011c040_f', t011c040_f),
            ('t011c041_t', t011c041_t),
            ('t011c041_m', t011c041_m),
            ('t011c041_f', t011c041_f),
            ('t011c042_t', t011c042_t),
            ('t011c042_m', t011c042_m),
            ('t011c042_f', t011c042_f),
            ('t011c043_t', t011c043_t),
            ('t011c043_m', t011c043_m),
            ('t011c043_f', t011c043_f),
            ('t011c044_t', t011c044_t),
            ('t011c044_m', t011c044_m),
            ('t011c044_f', t011c044_f),
            ('t011c045_t', t011c045_t),
            ('t011c045_m', t011c045_m),
            ('t011c045_f', t011c045_f),
            ('t011c046_t', t011c046_t),
            ('t011c046_m', t011c046_m),
            ('t011c046_f', t011c046_f),
            ('t011c047_t', t011c047_t),
            ('t011c047_m', t011c047_m),
            ('t011c047_f', t011c047_f),
            ('t011c048_t', t011c048_t),
            ('t011c048_m', t011c048_m),
            ('t011c048_f', t011c048_f),
            ('t011c049_t', t011c049_t),
            ('t011c049_m', t011c049_m),
            ('t011c049_f', t011c049_f),
            ('t011c050_t', t011c050_t),
            ('t011c050_m', t011c050_m),
            ('t011c050_f', t011c050_f),
            ('t011c051_t', t011c051_t),
            ('t011c051_m', t011c051_m),
            ('t011c051_f', t011c051_f),
            ('t011c052_t', t011c052_t),
            ('t011c052_m', t011c052_m),
            ('t011c052_f', t011c052_f),
            ('t011c053_t', t011c053_t),
            ('t011c053_m', t011c053_m),
            ('t011c053_f', t011c053_f),
            ('t011c054_t', t011c054_t),
            ('t011c054_m', t011c054_m),
            ('t011c054_f', t011c054_f),
            ('t011c055_t', t011c055_t),
            ('t011c055_m', t011c055_m),
            ('t011c055_f', t011c055_f),
            ('t011c056_t', t011c056_t),
            ('t011c056_m', t011c056_m),
            ('t011c056_f', t011c056_f),
            ('t011c057_t', t011c057_t),
            ('t011c057_m', t011c057_m),
            ('t011c057_f', t011c057_f),
            ('t011c058_t', t011c058_t),
            ('t011c058_m', t011c058_m),
            ('t011c058_f', t011c058_f),
            ('t011c059_t', t011c059_t),
            ('t011c059_m', t011c059_m),
            ('t011c059_f', t011c059_f),
            ('t011c060_t', t011c060_t),
            ('t011c060_m', t011c060_m),
            ('t011c060_f', t011c060_f),
            ('t011c061_t', t011c061_t),
            ('t011c061_m', t011c061_m),
            ('t011c061_f', t011c061_f),
            ('t011c062_t', t011c062_t),
            ('t011c062_m', t011c062_m),
            ('t011c062_f', t011c062_f),
            ('t011c063_t', t011c063_t),
            ('t011c063_m', t011c063_m),
            ('t011c063_f', t011c063_f),
            ('t011c064_t', t011c064_t),
            ('t011c064_m', t011c064_m),
            ('t011c064_f', t011c064_f),
            ('t011c065_t', t011c065_t),
            ('t011c065_m', t011c065_m),
            ('t011c065_f', t011c065_f),
            ('t011c066_t', t011c066_t),
            ('t011c066_m', t011c066_m),
            ('t011c066_f', t011c066_f),
            ('t012c001_t', t012c001_t),
            ('t012c002_t', t012c002_t),
            ('t012c003_t', t012c003_t),
            ('t012c004_t', t012c004_t),
            ('t012c005_t', t012c005_t),
            ('t012c006_t', t012c006_t),
            ('t012c007_t', t012c007_t),
            ('t012c008_t', t012c008_t),
            ('t012c009_t', t012c009_t),
            ('t012c010_t', t012c010_t),
            ('t012c011_t', t012c011_t),
            ('t012c012_t', t012c012_t),
            ('t012c013_t', t012c013_t),
            ('t012c014_t', t012c014_t),
            ('t012c016_t', t012c016_t),
            ('t012c017_t', t012c017_t),
            ('t012c018_t', t012c018_t),
            ('t012c019_t', t012c019_t),
            ('t012c020_t', t012c020_t),
            ('t012c021_t', t012c021_t),
            ('t012c022_t', t012c022_t),
            ('t012c023_t', t012c023_t),
            ('t012c024_t', t012c024_t),
            ('t012c025_t', t012c025_t),
            ('t012c026_t', t012c026_t),
            ('t012c027_t', t012c027_t),
            ('t012c028_t', t012c028_t),
            ('t012c030_t', t012c030_t),
            ('t012c031_t', t012c031_t),
            ('t012c032_t', t012c032_t),
            ('t012c033_t', t012c033_t),
            ('t012c034_t', t012c034_t),
            ('t012c035_t', t012c035_t),
            ('t012c036_t', t012c036_t),
            ('t012c037_t', t012c037_t),
            ('t012c038_t', t012c038_t),
            ('t012c039_t', t012c039_t),
            ('t012c040_t', t012c040_t),
            ('t012c041_t', t012c041_t),
            ('t012c042_t', t012c042_t),
            ('t012c043_t', t012c043_t),
            ('t013c001_t', t013c001_t),
            ('t013c001_m', t013c001_m),
            ('t013c001_f', t013c001_f),
            ('t013c002_t', t013c002_t),
            ('t013c002_m', t013c002_m),
            ('t013c002_f', t013c002_f),
            ('t013c003_t', t013c003_t),
            ('t013c003_m', t013c003_m),
            ('t013c003_f', t013c003_f),
            ('t013c004_t', t013c004_t),
            ('t013c004_m', t013c004_m),
            ('t013c004_f', t013c004_f),
            ('t013c005_t', t013c005_t),
            ('t013c005_m', t013c005_m),
            ('t013c005_f', t013c005_f),
            ('t013c006_t', t013c006_t),
            ('t013c006_m', t013c006_m),
            ('t013c006_f', t013c006_f),
            ('t013c007_t', t013c007_t),
            ('t013c007_m', t013c007_m),
            ('t013c007_f', t013c007_f),
            ('t013c008_t', t013c008_t),
            ('t013c008_m', t013c008_m),
            ('t013c008_f', t013c008_f),
            ('t013c009_t', t013c009_t),
            ('t013c009_m', t013c009_m),
            ('t013c009_f', t013c009_f),
            ('t013c010_t', t013c010_t),
            ('t013c010_m', t013c010_m),
            ('t013c010_f', t013c010_f),
            ('t013c011_t', t013c011_t),
            ('t013c011_m', t013c011_m),
            ('t013c011_f', t013c011_f),
            ('t013c012_t', t013c012_t),
            ('t013c012_m', t013c012_m),
            ('t013c012_f', t013c012_f),
            ('t013c013_t', t013c013_t),
            ('t013c013_m', t013c013_m),
            ('t013c013_f', t013c013_f),
            ('t013c014_t', t013c014_t),
            ('t013c014_m', t013c014_m),
            ('t013c014_f', t013c014_f),
            ('t013c015_t', t013c015_t),
            ('t013c015_m', t013c015_m),
            ('t013c015_f', t013c015_f),
            ('t013c016_t', t013c016_t),
            ('t013c016_m', t013c016_m),
            ('t013c016_f', t013c016_f),
            ('t013c017_t', t013c017_t),
            ('t013c017_m', t013c017_m),
            ('t013c017_f', t013c017_f),
            ('t013c018_t', t013c018_t),
            ('t013c018_m', t013c018_m),
            ('t013c018_f', t013c018_f),
            ('t013c020_t', t013c020_t),
            ('t013c020_m', t013c020_m),
            ('t013c020_f', t013c020_f),
            ('t013c021_t', t013c021_t),
            ('t013c021_m', t013c021_m),
            ('t013c021_f', t013c021_f),
            ('t013c022_t', t013c022_t),
            ('t013c022_m', t013c022_m),
            ('t013c022_f', t013c022_f),
            ('t013c023_t', t013c023_t),
            ('t013c023_m', t013c023_m),
            ('t013c023_f', t013c023_f),
            ('t013c024_t', t013c024_t),
            ('t013c024_m', t013c024_m),
            ('t013c024_f', t013c024_f),
            ('t013c025_t', t013c025_t),
            ('t013c025_m', t013c025_m),
            ('t013c025_f', t013c025_f),
            ('t013c026_t', t013c026_t),
            ('t013c026_m', t013c026_m),
            ('t013c026_f', t013c026_f),
            ('t013c027_t', t013c027_t),
            ('t013c027_m', t013c027_m),
            ('t013c027_f', t013c027_f),
            ('t013c028_t', t013c028_t),
            ('t013c028_m', t013c028_m),
            ('t013c028_f', t013c028_f),
            ('t013c029_t', t013c029_t),
            ('t013c029_m', t013c029_m),
            ('t013c029_f', t013c029_f),
            ('t013c030_t', t013c030_t),
            ('t013c030_m', t013c030_m),
            ('t013c030_f', t013c030_f),
            ('t013c031_t', t013c031_t),
            ('t013c031_m', t013c031_m),
            ('t013c031_f', t013c031_f),
            ('t013c032_t', t013c032_t),
            ('t013c032_m', t013c032_m),
            ('t013c032_f', t013c032_f),
            ('t013c033_t', t013c033_t),
            ('t013c033_m', t013c033_m),
            ('t013c033_f', t013c033_f),
            ('t013c034_t', t013c034_t),
            ('t013c034_m', t013c034_m),
            ('t013c034_f', t013c034_f),
            ('t013c035_t', t013c035_t),
            ('t013c035_m', t013c035_m),
            ('t013c035_f', t013c035_f),
            ('t013c036_t', t013c036_t),
            ('t013c036_m', t013c036_m),
            ('t013c036_f', t013c036_f),
            ('t013c037_t', t013c037_t),
            ('t013c037_m', t013c037_m),
            ('t013c037_f', t013c037_f),
            ('t013c038_t', t013c038_t),
            ('t013c038_m', t013c038_m),
            ('t013c038_f', t013c038_f),
            ('t013c039_t', t013c039_t),
            ('t013c039_m', t013c039_m),
            ('t013c039_f', t013c039_f),
            ('t013c040_t', t013c040_t),
            ('t013c040_m', t013c040_m),
            ('t013c040_f', t013c040_f),
            ('t013c041_t', t013c041_t),
            ('t013c041_m', t013c041_m),
            ('t013c041_f', t013c041_f),
            ('t013c042_t', t013c042_t),
            ('t013c042_m', t013c042_m),
            ('t013c042_f', t013c042_f),
            ('t013c043_t', t013c043_t),
            ('t013c043_m', t013c043_m),
            ('t013c043_f', t013c043_f),
            ('t013c044_t', t013c044_t),
            ('t013c044_m', t013c044_m),
            ('t013c044_f', t013c044_f),
            ('t013c045_t', t013c045_t),
            ('t013c045_m', t013c045_m),
            ('t013c045_f', t013c045_f),
            ('t013c046_t', t013c046_t),
            ('t013c046_m', t013c046_m),
            ('t013c046_f', t013c046_f),
            ('t013c047_t', t013c047_t),
            ('t013c047_m', t013c047_m),
            ('t013c047_f', t013c047_f),
            ('t013c048_t', t013c048_t),
            ('t013c048_m', t013c048_m),
            ('t013c048_f', t013c048_f),
            ('t013c049_t', t013c049_t),
            ('t013c049_m', t013c049_m),
            ('t013c049_f', t013c049_f),
            ('t013c050_t', t013c050_t),
            ('t013c050_m', t013c050_m),
            ('t013c050_f', t013c050_f),
            ('t013c051_t', t013c051_t),
            ('t013c051_m', t013c051_m),
            ('t013c051_f', t013c051_f),
            ('t013c052_t', t013c052_t),
            ('t013c052_m', t013c052_m),
            ('t013c052_f', t013c052_f),
            ('t013c053_t', t013c053_t),
            ('t013c053_m', t013c053_m),
            ('t013c053_f', t013c053_f),
            ('t013c054_t', t013c054_t),
            ('t013c054_m', t013c054_m),
            ('t013c054_f', t013c054_f),
            ('t013c055_t', t013c055_t),
            ('t013c056_t', t013c056_t),
            ('t013c057_t', t013c057_t),
            ('t013c058_t', t013c058_t),
            ('t013c059_t', t013c059_t),
            ('t013c060_t', t013c060_t),
            ('t013c061_t', t013c061_t),
            ('t013c062_t', t013c062_t),
            ('t013c063_t', t013c063_t),
            ('t013c064_t', t013c064_t),
            ('t013c065_t', t013c065_t),
            ('t013c066_t', t013c066_t),
            ('t013c067_t', t013c067_t),
            ('t013c068_t', t013c068_t),
            ('t013c069_t', t013c069_t),
            ('t013c070_t', t013c070_t),
            ('t013c071_t', t013c071_t),
            ('t013c072_t', t013c072_t),
            ('t013c073_t', t013c073_t),
            ('t013c074_t', t013c074_t),
            ('t013c075_t', t013c075_t),
            ('t013c076_t', t013c076_t),
            ('t013c077_t', t013c077_t),
            ('t013c078_t', t013c078_t),
            ('t013c079_t', t013c079_t),
            ('t013c079_m', t013c079_m),
            ('t013c079_f', t013c079_f),
            ('t013c080_t', t013c080_t),
            ('t013c080_m', t013c080_m),
            ('t013c080_f', t013c080_f),
            ('t013c081_t', t013c081_t),
            ('t013c081_m', t013c081_m),
            ('t013c081_f', t013c081_f),
            ('t013c082_t', t013c082_t),
            ('t013c082_m', t013c082_m),
            ('t013c082_f', t013c082_f),
            ('t013c083_t', t013c083_t),
            ('t013c083_m', t013c083_m),
            ('t013c083_f', t013c083_f),
            ('t013c085_t', t013c085_t),
            ('t013c085_m', t013c085_m),
            ('t013c085_f', t013c085_f),
            ('t013c086_t', t013c086_t),
            ('t013c086_m', t013c086_m),
            ('t013c086_f', t013c086_f),
            ('t013c087_t', t013c087_t),
            ('t013c087_m', t013c087_m),
            ('t013c087_f', t013c087_f),
            ('t013c088_t', t013c088_t),
            ('t013c088_m', t013c088_m),
            ('t013c088_f', t013c088_f),
            ('t013c089_t', t013c089_t),
            ('t013c089_m', t013c089_m),
            ('t013c089_f', t013c089_f),
            ('t013c090_t', t013c090_t),
            ('t013c090_m', t013c090_m),
            ('t013c090_f', t013c090_f),
            ('t013c091_t', t013c091_t),
            ('t013c091_m', t013c091_m),
            ('t013c091_f', t013c091_f),
            ('t013c092_t', t013c092_t),
            ('t013c092_m', t013c092_m),
            ('t013c092_f', t013c092_f),
            ('t013c093_t', t013c093_t),
            ('t013c093_m', t013c093_m),
            ('t013c093_f', t013c093_f),
            ('t013c094_t', t013c094_t),
            ('t013c094_m', t013c094_m),
            ('t013c094_f', t013c094_f),
            ('t013c095_t', t013c095_t),
            ('t013c095_m', t013c095_m),
            ('t013c095_f', t013c095_f),
            ('t013c096_t', t013c096_t),
            ('t013c096_m', t013c096_m),
            ('t013c096_f', t013c096_f),
            ('t013c097_t', t013c097_t),
            ('t013c097_m', t013c097_m),
            ('t013c097_f', t013c097_f),
            ('t013c098_t', t013c098_t),
            ('t013c098_m', t013c098_m),
            ('t013c098_f', t013c098_f),
            ('t013c099_t', t013c099_t),
            ('t013c099_m', t013c099_m),
            ('t013c099_f', t013c099_f),
            ('t013c100_t', t013c100_t),
            ('t013c100_m', t013c100_m),
            ('t013c100_f', t013c100_f),
            ('t013c101_t', t013c101_t),
            ('t013c101_m', t013c101_m),
            ('t013c101_f', t013c101_f),
            ('t013c102_t', t013c102_t),
            ('t013c102_m', t013c102_m),
            ('t013c102_f', t013c102_f),
            ('t013c103_t', t013c103_t),
            ('t013c103_m', t013c103_m),
            ('t013c103_f', t013c103_f),
            ('t013c104_t', t013c104_t),
            ('t013c104_m', t013c104_m),
            ('t013c104_f', t013c104_f),
            ('t013c105_t', t013c105_t),
            ('t013c105_m', t013c105_m),
            ('t013c105_f', t013c105_f),
            ('t013c106_t', t013c106_t),
            ('t013c106_m', t013c106_m),
            ('t013c106_f', t013c106_f),
            ('t013c107_t', t013c107_t),
            ('t013c107_m', t013c107_m),
            ('t013c107_f', t013c107_f),
            ('t013c108_t', t013c108_t),
            ('t013c108_m', t013c108_m),
            ('t013c108_f', t013c108_f),
            ('t013c109_t', t013c109_t),
            ('t013c109_m', t013c109_m),
            ('t013c109_f', t013c109_f),
            ('t013c110_t', t013c110_t),
            ('t013c110_m', t013c110_m),
            ('t013c110_f', t013c110_f),
            ('t013c111_t', t013c111_t),
            ('t013c111_m', t013c111_m),
            ('t013c111_f', t013c111_f),
            ('t014c002_t', t014c002_t),
            ('t014c002_m', t014c002_m),
            ('t014c002_f', t014c002_f),
            ('t014c003_t', t014c003_t),
            ('t014c003_m', t014c003_m),
            ('t014c003_f', t014c003_f),
            ('t014c004_t', t014c004_t),
            ('t014c004_m', t014c004_m),
            ('t014c004_f', t014c004_f),
            ('t014c005_t', t014c005_t),
            ('t014c005_m', t014c005_m),
            ('t014c005_f', t014c005_f),
            ('t014c006_t', t014c006_t),
            ('t014c006_m', t014c006_m),
            ('t014c006_f', t014c006_f),
            ('t014c007_t', t014c007_t),
            ('t014c007_m', t014c007_m),
            ('t014c007_f', t014c007_f),
            ('t014c008_t', t014c008_t),
            ('t014c008_m', t014c008_m),
            ('t014c008_f', t014c008_f),
            ('t014c009_t', t014c009_t),
            ('t014c009_m', t014c009_m),
            ('t014c009_f', t014c009_f),
            ('t014c010_t', t014c010_t),
            ('t014c010_m', t014c010_m),
            ('t014c010_f', t014c010_f),
            ('t014c011_t', t014c011_t),
            ('t014c011_m', t014c011_m),
            ('t014c011_f', t014c011_f),
            ('t014c012_t', t014c012_t),
            ('t014c012_m', t014c012_m),
            ('t014c012_f', t014c012_f),
            ('t014c013_t', t014c013_t),
            ('t014c013_m', t014c013_m),
            ('t014c013_f', t014c013_f),
            ('t014c014_t', t014c014_t),
            ('t014c014_m', t014c014_m),
            ('t014c014_f', t014c014_f),
            ('t014c015_t', t014c015_t),
            ('t014c015_m', t014c015_m),
            ('t014c015_f', t014c015_f),
            ('t014c016_t', t014c016_t),
            ('t014c016_m', t014c016_m),
            ('t014c016_f', t014c016_f),
            ('t014c017_t', t014c017_t),
            ('t014c017_m', t014c017_m),
            ('t014c017_f', t014c017_f),
            ('t014c018_t', t014c018_t),
            ('t014c018_m', t014c018_m),
            ('t014c018_f', t014c018_f),
            ('t014c019_t', t014c019_t),
            ('t014c019_m', t014c019_m),
            ('t014c019_f', t014c019_f),
            ('t014c020_t', t014c020_t),
            ('t014c020_m', t014c020_m),
            ('t014c020_f', t014c020_f),
            ('t014c021_t', t014c021_t),
            ('t014c021_m', t014c021_m),
            ('t014c021_f', t014c021_f),
            ('t014c022_t', t014c022_t),
            ('t014c022_m', t014c022_m),
            ('t014c022_f', t014c022_f),
            ('t014c023_t', t014c023_t),
            ('t014c023_m', t014c023_m),
            ('t014c023_f', t014c023_f),
            ('t015c002_t', t015c002_t),
            ('t015c002_m', t015c002_m),
            ('t015c002_f', t015c002_f),
            ('t015c003_t', t015c003_t),
            ('t015c003_m', t015c003_m),
            ('t015c003_f', t015c003_f),
            ('t015c004_t', t015c004_t),
            ('t015c004_m', t015c004_m),
            ('t015c004_f', t015c004_f),
            ('t015c005_t', t015c005_t),
            ('t015c005_m', t015c005_m),
            ('t015c005_f', t015c005_f),
            ('t015c006_t', t015c006_t),
            ('t015c006_m', t015c006_m),
            ('t015c006_f', t015c006_f),
            ('t015c007_t', t015c007_t),
            ('t015c007_m', t015c007_m),
            ('t015c007_f', t015c007_f),
            ('t015c008_t', t015c008_t),
            ('t015c008_m', t015c008_m),
            ('t015c008_f', t015c008_f),
            ('t016c001_t', t016c001_t),
            ('t016c001_m', t016c001_m),
            ('t016c001_f', t016c001_f),
            ('t016c002_t', t016c002_t),
            ('t016c002_m', t016c002_m),
            ('t016c002_f', t016c002_f),
            ('t016c003_t', t016c003_t),
            ('t016c003_m', t016c003_m),
            ('t016c003_f', t016c003_f),
            ('t016c004_t', t016c004_t),
            ('t016c004_m', t016c004_m),
            ('t016c004_f', t016c004_f),
            ('t016c005_t', t016c005_t),
            ('t016c005_m', t016c005_m),
            ('t016c005_f', t016c005_f),
            ('t016c006_t', t016c006_t),
            ('t016c006_m', t016c006_m),
            ('t016c006_f', t016c006_f),
            ('t016c007_t', t016c007_t),
            ('t016c007_m', t016c007_m),
            ('t016c007_f', t016c007_f),
            ('t016c008_t', t016c008_t),
            ('t016c008_m', t016c008_m),
            ('t016c008_f', t016c008_f),
            ('t016c009_t', t016c009_t),
            ('t016c009_m', t016c009_m),
            ('t016c009_f', t016c009_f),
            ('t016c010_t', t016c010_t),
            ('t016c010_m', t016c010_m),
            ('t016c010_f', t016c010_f),
            ('t016c011_t', t016c011_t),
            ('t016c011_m', t016c011_m),
            ('t016c011_f', t016c011_f),
            ('t016c012_t', t016c012_t),
            ('t016c012_m', t016c012_m),
            ('t016c012_f', t016c012_f),
            ('t016c013_t', t016c013_t),
            ('t016c013_m', t016c013_m),
            ('t016c013_f', t016c013_f),
            ('t016c014_t', t016c014_t),
            ('t016c014_m', t016c014_m),
            ('t016c014_f', t016c014_f),
            ('t016c015_t', t016c015_t),
            ('t016c015_m', t016c015_m),
            ('t016c015_f', t016c015_f),
            ('t016c016_t', t016c016_t),
            ('t016c016_m', t016c016_m),
            ('t016c016_f', t016c016_f),
            ('t016c017_t', t016c017_t),
            ('t016c017_m', t016c017_m),
            ('t016c017_f', t016c017_f),
            ('t016c018_t', t016c018_t),
            ('t016c018_m', t016c018_m),
            ('t016c018_f', t016c018_f),
            ('t016c019_t', t016c019_t),
            ('t016c019_m', t016c019_m),
            ('t016c019_f', t016c019_f),
            ('t016c020_t', t016c020_t),
            ('t016c020_m', t016c020_m),
            ('t016c020_f', t016c020_f),
            ('t016c021_t', t016c021_t),
            ('t016c021_m', t016c021_m),
            ('t016c021_f', t016c021_f),
            ('t016c022_t', t016c022_t),
            ('t016c022_m', t016c022_m),
            ('t016c022_f', t016c022_f),
            ('t016c023_t', t016c023_t),
            ('t016c023_m', t016c023_m),
            ('t016c023_f', t016c023_f),
            ('t016c024_t', t016c024_t),
            ('t016c024_m', t016c024_m),
            ('t016c024_f', t016c024_f),
            ('t016c025_t', t016c025_t),
            ('t016c025_m', t016c025_m),
            ('t016c025_f', t016c025_f),
            ('t016c026_t', t016c026_t),
            ('t016c026_m', t016c026_m),
            ('t016c026_f', t016c026_f),
            ('t016c027_t', t016c027_t),
            ('t016c027_m', t016c027_m),
            ('t016c027_f', t016c027_f),
            ('t016c028_t', t016c028_t),
            ('t016c028_m', t016c028_m),
            ('t016c028_f', t016c028_f),
            ('t016c029_t', t016c029_t),
            ('t016c029_m', t016c029_m),
            ('t016c029_f', t016c029_f),
            ('t016c030_t', t016c030_t),
            ('t016c030_m', t016c030_m),
            ('t016c030_f', t016c030_f),
            ('t016c031_t', t016c031_t),
            ('t016c031_m', t016c031_m),
            ('t016c031_f', t016c031_f),
            ('t017c001_t', t017c001_t),
            ('t017c001_m', t017c001_m),
            ('t017c001_f', t017c001_f),
            ('t017c002_t', t017c002_t),
            ('t017c002_m', t017c002_m),
            ('t017c002_f', t017c002_f),
            ('t018c001_t', t018c001_t),
            ('t018c001_m', t018c001_m),
            ('t018c001_f', t018c001_f),
            ('t018c002_t', t018c002_t),
            ('t018c002_m', t018c002_m),
            ('t018c002_f', t018c002_f),
            ('t018c003_t', t018c003_t),
            ('t018c003_m', t018c003_m),
            ('t018c003_f', t018c003_f),
            ('t018c004_t', t018c004_t),
            ('t018c004_m', t018c004_m),
            ('t018c004_f', t018c004_f),
            ('t018c005_t', t018c005_t),
            ('t018c005_m', t018c005_m),
            ('t018c005_f', t018c005_f),
            ('t018c006_t', t018c006_t),
            ('t018c006_m', t018c006_m),
            ('t018c006_f', t018c006_f),
            ('t018c007_t', t018c007_t),
            ('t018c007_m', t018c007_m),
            ('t018c007_f', t018c007_f),
            ('t018c008_t', t018c008_t),
            ('t018c008_m', t018c008_m),
            ('t018c008_f', t018c008_f),
            ('t018c009_t', t018c009_t),
            ('t018c009_m', t018c009_m),
            ('t018c009_f', t018c009_f),
            ('t018c010_t', t018c010_t),
            ('t018c010_m', t018c010_m),
            ('t018c010_f', t018c010_f),
            ('t018c011_t', t018c011_t),
            ('t018c011_m', t018c011_m),
            ('t018c011_f', t018c011_f),
            ('t018c012_t', t018c012_t),
            ('t018c012_m', t018c012_m),
            ('t018c012_f', t018c012_f),
            ('t018c013_t', t018c013_t),
            ('t018c013_m', t018c013_m),
            ('t018c013_f', t018c013_f),
            ('t018c014_t', t018c014_t),
            ('t018c014_m', t018c014_m),
            ('t018c014_f', t018c014_f),
            ('t018c015_t', t018c015_t),
            ('t018c015_m', t018c015_m),
            ('t018c015_f', t018c015_f),
            ('t018c016_t', t018c016_t),
            ('t018c016_m', t018c016_m),
            ('t018c016_f', t018c016_f),
            ('t018c017_t', t018c017_t),
            ('t018c017_m', t018c017_m),
            ('t018c017_f', t018c017_f),
            ('t018c018_t', t018c018_t),
            ('t018c018_m', t018c018_m),
            ('t018c018_f', t018c018_f),
            ('t019c002_t', t019c002_t),
            ('t019c002_m', t019c002_m),
            ('t019c002_f', t019c002_f),
            ('t019c003_t', t019c003_t),
            ('t019c003_m', t019c003_m),
            ('t019c003_f', t019c003_f),
            ('t019c004_t', t019c004_t),
            ('t019c004_m', t019c004_m),
            ('t019c004_f', t019c004_f),
            ('t019c005_t', t019c005_t),
            ('t019c005_m', t019c005_m),
            ('t019c005_f', t019c005_f),
            ('t019c006_t', t019c006_t),
            ('t019c006_m', t019c006_m),
            ('t019c006_f', t019c006_f),
            ('t019c007_t', t019c007_t),
            ('t019c007_m', t019c007_m),
            ('t019c007_f', t019c007_f),
            ('t020c001_t', t020c001_t),
            ('t020c001_m', t020c001_m),
            ('t020c001_f', t020c001_f),
            ('t020c002_t', t020c002_t),
            ('t020c002_m', t020c002_m),
            ('t020c002_f', t020c002_f),
            ('t020c003_t', t020c003_t),
            ('t020c003_m', t020c003_m),
            ('t020c003_f', t020c003_f),
            ('t020c004_t', t020c004_t),
            ('t020c004_m', t020c004_m),
            ('t020c004_f', t020c004_f),
            ('t020c005_t', t020c005_t),
            ('t020c005_m', t020c005_m),
            ('t020c005_f', t020c005_f),
            ('t020c006_t', t020c006_t),
            ('t020c006_m', t020c006_m),
            ('t020c006_f', t020c006_f),
            ('t020c007_t', t020c007_t),
            ('t020c007_m', t020c007_m),
            ('t020c007_f', t020c007_f),
            ('t020c008_t', t020c008_t),
            ('t020c008_m', t020c008_m),
            ('t020c008_f', t020c008_f),
            ('t020c009_t', t020c009_t),
            ('t020c009_m', t020c009_m),
            ('t020c009_f', t020c009_f),
            ('t020c010_t', t020c010_t),
            ('t020c010_m', t020c010_m),
            ('t020c010_f', t020c010_f),
            ('t020c011_t', t020c011_t),
            ('t020c011_m', t020c011_m),
            ('t020c011_f', t020c011_f),
            ('t020c012_t', t020c012_t),
            ('t020c012_m', t020c012_m),
            ('t020c012_f', t020c012_f),
            ('t020c013_t', t020c013_t),
            ('t020c013_m', t020c013_m),
            ('t020c013_f', t020c013_f),
            ('t020c014_t', t020c014_t),
            ('t020c014_m', t020c014_m),
            ('t020c014_f', t020c014_f),
            ('t020c015_t', t020c015_t),
            ('t020c015_m', t020c015_m),
            ('t020c015_f', t020c015_f),
            ('t020c016_t', t020c016_t),
            ('t020c016_m', t020c016_m),
            ('t020c016_f', t020c016_f),
            ('t020c017_t', t020c017_t),
            ('t020c017_m', t020c017_m),
            ('t020c017_f', t020c017_f),
            ('t020c018_t', t020c018_t),
            ('t020c018_m', t020c018_m),
            ('t020c018_f', t020c018_f),
            ('t020c019_t', t020c019_t),
            ('t020c019_m', t020c019_m),
            ('t020c019_f', t020c019_f),
            ('t020c020_t', t020c020_t),
            ('t020c020_m', t020c020_m),
            ('t020c020_f', t020c020_f),
            ('t020c021_t', t020c021_t),
            ('t020c021_m', t020c021_m),
            ('t020c021_f', t020c021_f),
            ('t020c022_t', t020c022_t),
            ('t020c022_m', t020c022_m),
            ('t020c022_f', t020c022_f),
            ('t020c023_t', t020c023_t),
            ('t020c023_m', t020c023_m),
            ('t020c023_f', t020c023_f),
            ('t020c024_t', t020c024_t),
            ('t020c024_m', t020c024_m),
            ('t020c024_f', t020c024_f),
            ('t020c025_t', t020c025_t),
            ('t020c025_m', t020c025_m),
            ('t020c025_f', t020c025_f),
            ('t020c026_t', t020c026_t),
            ('t020c026_m', t020c026_m),
            ('t020c026_f', t020c026_f),
            ('t020c027_t', t020c027_t),
            ('t020c027_m', t020c027_m),
            ('t020c027_f', t020c027_f),
            ('t020c028_t', t020c028_t),
            ('t020c028_m', t020c028_m),
            ('t020c028_f', t020c028_f),
            ('t020c029_t', t020c029_t),
            ('t020c029_m', t020c029_m),
            ('t020c029_f', t020c029_f),
            ('t020c030_t', t020c030_t),
            ('t020c030_m', t020c030_m),
            ('t020c030_f', t020c030_f),
            ('t020c031_t', t020c031_t),
            ('t020c031_m', t020c031_m),
            ('t020c031_f', t020c031_f),
            ('t020c032_t', t020c032_t),
            ('t020c032_m', t020c032_m),
            ('t020c032_f', t020c032_f),
            ('t020c033_t', t020c033_t),
            ('t020c033_m', t020c033_m),
            ('t020c033_f', t020c033_f),
            ('t020c034_t', t020c034_t),
            ('t020c034_m', t020c034_m),
            ('t020c034_f', t020c034_f),
            ('t020c035_t', t020c035_t),
            ('t020c035_m', t020c035_m),
            ('t020c035_f', t020c035_f),
            ('t020c036_t', t020c036_t),
            ('t020c036_m', t020c036_m),
            ('t020c036_f', t020c036_f),
            ('t020c037_t', t020c037_t),
            ('t020c037_m', t020c037_m),
            ('t020c037_f', t020c037_f),
            ('t020c038_t', t020c038_t),
            ('t020c038_m', t020c038_m),
            ('t020c038_f', t020c038_f),
            ('t020c039_t', t020c039_t),
            ('t020c039_m', t020c039_m),
            ('t020c039_f', t020c039_f),
            ('t020c040_t', t020c040_t),
            ('t020c040_m', t020c040_m),
            ('t020c040_f', t020c040_f),
            ('t020c041_t', t020c041_t),
            ('t020c041_m', t020c041_m),
            ('t020c041_f', t020c041_f),
            ('t020c042_t', t020c042_t),
            ('t020c042_m', t020c042_m),
            ('t020c042_f', t020c042_f),
            ('t020c043_t', t020c043_t),
            ('t020c043_m', t020c043_m),
            ('t020c043_f', t020c043_f),
            ('t020c044_t', t020c044_t),
            ('t020c044_m', t020c044_m),
            ('t020c044_f', t020c044_f),
            ('t020c045_t', t020c045_t),
            ('t020c045_m', t020c045_m),
            ('t020c045_f', t020c045_f),
            ('t020c046_t', t020c046_t),
            ('t020c046_m', t020c046_m),
            ('t020c046_f', t020c046_f),
            ('t020c047_t', t020c047_t),
            ('t020c047_m', t020c047_m),
            ('t020c047_f', t020c047_f),
            ('t020c048_t', t020c048_t),
            ('t020c048_m', t020c048_m),
            ('t020c048_f', t020c048_f),
            ('t020c049_t', t020c049_t),
            ('t020c049_m', t020c049_m),
            ('t020c049_f', t020c049_f),
            ('t020c050_t', t020c050_t),
            ('t020c050_m', t020c050_m),
            ('t020c050_f', t020c050_f),
            ('t020c051_t', t020c051_t),
            ('t020c051_m', t020c051_m),
            ('t020c051_f', t020c051_f),
            ('t020c052_t', t020c052_t),
            ('t020c052_m', t020c052_m),
            ('t020c052_f', t020c052_f),
            ('t020c053_t', t020c053_t),
            ('t020c053_m', t020c053_m),
            ('t020c053_f', t020c053_f),
            ('t020c054_t', t020c054_t),
            ('t020c054_m', t020c054_m),
            ('t020c054_f', t020c054_f),
            ('t020c055_t', t020c055_t),
            ('t020c055_m', t020c055_m),
            ('t020c055_f', t020c055_f),
            ('t020c056_t', t020c056_t),
            ('t020c056_m', t020c056_m),
            ('t020c056_f', t020c056_f),
            ('t020c057_t', t020c057_t),
            ('t020c057_m', t020c057_m),
            ('t020c057_f', t020c057_f),
            ('t020c058_t', t020c058_t),
            ('t020c058_m', t020c058_m),
            ('t020c058_f', t020c058_f),
            ('t020c059_t', t020c059_t),
            ('t020c059_m', t020c059_m),
            ('t020c059_f', t020c059_f),
            ('t020c060_t', t020c060_t),
            ('t020c060_m', t020c060_m),
            ('t020c060_f', t020c060_f),
            ('t020c061_t', t020c061_t),
            ('t020c061_m', t020c061_m),
            ('t020c061_f', t020c061_f),
            ('t020c062_t', t020c062_t),
            ('t020c062_m', t020c062_m),
            ('t020c062_f', t020c062_f),
            ('t020c063_t', t020c063_t),
            ('t020c063_m', t020c063_m),
            ('t020c063_f', t020c063_f),
            ('t020c064_t', t020c064_t),
            ('t020c064_m', t020c064_m),
            ('t020c064_f', t020c064_f),
            ('t020c065_t', t020c065_t),
            ('t020c065_m', t020c065_m),
            ('t020c065_f', t020c065_f),
            ('t020c066_t', t020c066_t),
            ('t020c066_m', t020c066_m),
            ('t020c066_f', t020c066_f),
            ('t020c067_t', t020c067_t),
            ('t020c067_m', t020c067_m),
            ('t020c067_f', t020c067_f),
            ('t020c068_t', t020c068_t),
            ('t020c068_m', t020c068_m),
            ('t020c068_f', t020c068_f),
            ('t020c069_t', t020c069_t),
            ('t020c069_m', t020c069_m),
            ('t020c069_f', t020c069_f),
            ('t020c070_t', t020c070_t),
            ('t020c070_m', t020c070_m),
            ('t020c070_f', t020c070_f),
            ('t020c071_t', t020c071_t),
            ('t020c071_m', t020c071_m),
            ('t020c071_f', t020c071_f),
            ('t020c072_t', t020c072_t),
            ('t020c072_m', t020c072_m),
            ('t020c072_f', t020c072_f),
            ('t020c073_t', t020c073_t),
            ('t020c073_m', t020c073_m),
            ('t020c073_f', t020c073_f),
            ('t020c074_t', t020c074_t),
            ('t020c074_m', t020c074_m),
            ('t020c074_f', t020c074_f),
            ('t020c075_t', t020c075_t),
            ('t020c075_m', t020c075_m),
            ('t020c075_f', t020c075_f),
            ('t020c076_t', t020c076_t),
            ('t020c076_m', t020c076_m),
            ('t020c076_f', t020c076_f),
            ('t020c077_t', t020c077_t),
            ('t020c077_m', t020c077_m),
            ('t020c077_f', t020c077_f),
            ('t020c078_t', t020c078_t),
            ('t020c078_m', t020c078_m),
            ('t020c078_f', t020c078_f),
            ('t020c079_t', t020c079_t),
            ('t020c079_m', t020c079_m),
            ('t020c079_f', t020c079_f),
            ('t020c080_t', t020c080_t),
            ('t020c080_m', t020c080_m),
            ('t020c080_f', t020c080_f),
            ('t020c081_t', t020c081_t),
            ('t020c081_m', t020c081_m),
            ('t020c081_f', t020c081_f),
            ('t020c082_t', t020c082_t),
            ('t020c082_m', t020c082_m),
            ('t020c082_f', t020c082_f),
            ('t020c083_t', t020c083_t),
            ('t020c083_m', t020c083_m),
            ('t020c083_f', t020c083_f),
            ('t020c084_t', t020c084_t),
            ('t020c084_m', t020c084_m),
            ('t020c084_f', t020c084_f),
            ('t020c085_t', t020c085_t),
            ('t020c085_m', t020c085_m),
            ('t020c085_f', t020c085_f),
            ('t020c086_t', t020c086_t),
            ('t020c086_m', t020c086_m),
            ('t020c086_f', t020c086_f),
            ('t020c087_t', t020c087_t),
            ('t020c087_m', t020c087_m),
            ('t020c087_f', t020c087_f),
            ('t020c088_t', t020c088_t),
            ('t020c088_m', t020c088_m),
            ('t020c088_f', t020c088_f),
            ('t020c089_t', t020c089_t),
            ('t020c089_m', t020c089_m),
            ('t020c089_f', t020c089_f),
            ('t020c090_t', t020c090_t),
            ('t020c090_m', t020c090_m),
            ('t020c090_f', t020c090_f),
            ('t020c091_t', t020c091_t),
            ('t020c091_m', t020c091_m),
            ('t020c091_f', t020c091_f),
            ('t020c092_t', t020c092_t),
            ('t020c092_m', t020c092_m),
            ('t020c092_f', t020c092_f),
            ('t020c093_t', t020c093_t),
            ('t020c093_m', t020c093_m),
            ('t020c093_f', t020c093_f),
            ('t020c094_t', t020c094_t),
            ('t020c094_m', t020c094_m),
            ('t020c094_f', t020c094_f),
            ('t020c095_t', t020c095_t),
            ('t020c095_m', t020c095_m),
            ('t020c095_f', t020c095_f),
            ('t020c096_t', t020c096_t),
            ('t020c096_m', t020c096_m),
            ('t020c096_f', t020c096_f),
            ('t020c097_t', t020c097_t),
            ('t020c097_m', t020c097_m),
            ('t020c097_f', t020c097_f),
            ('t020c098_t', t020c098_t),
            ('t020c098_m', t020c098_m),
            ('t020c098_f', t020c098_f),
            ('t020c099_t', t020c099_t),
            ('t020c099_m', t020c099_m),
            ('t020c099_f', t020c099_f),
            ('t020c100_t', t020c100_t),
            ('t020c100_m', t020c100_m),
            ('t020c100_f', t020c100_f),
            ('t020c101_t', t020c101_t),
            ('t020c101_m', t020c101_m),
            ('t020c101_f', t020c101_f),
            ('t020c102_t', t020c102_t),
            ('t020c102_m', t020c102_m),
            ('t020c102_f', t020c102_f),
            ('t020c103_t', t020c103_t),
            ('t020c103_m', t020c103_m),
            ('t020c103_f', t020c103_f),
            ('t020c104_t', t020c104_t),
            ('t020c104_m', t020c104_m),
            ('t020c104_f', t020c104_f),
            ('t020c105_t', t020c105_t),
            ('t020c105_m', t020c105_m),
            ('t020c105_f', t020c105_f),
            ('t021c002_t', t021c002_t),
            ('t021c002_m', t021c002_m),
            ('t021c002_f', t021c002_f),
            ('t021c003_t', t021c003_t),
            ('t021c003_m', t021c003_m),
            ('t021c003_f', t021c003_f),
            ('t021c004_t', t021c004_t),
            ('t021c004_m', t021c004_m),
            ('t021c004_f', t021c004_f),
            ('t021c005_t', t021c005_t),
            ('t021c005_m', t021c005_m),
            ('t021c005_f', t021c005_f),
            ('t021c006_t', t021c006_t),
            ('t021c006_m', t021c006_m),
            ('t021c006_f', t021c006_f),
            ('t021c007_t', t021c007_t),
            ('t021c007_m', t021c007_m),
            ('t021c007_f', t021c007_f),
            ('t021c008_t', t021c008_t),
            ('t021c008_m', t021c008_m),
            ('t021c008_f', t021c008_f),
            ('t021c009_t', t021c009_t),
            ('t021c009_m', t021c009_m),
            ('t021c009_f', t021c009_f),
            ('t021c010_t', t021c010_t),
            ('t021c010_m', t021c010_m),
            ('t021c010_f', t021c010_f),
            ('t021c011_t', t021c011_t),
            ('t021c011_m', t021c011_m),
            ('t021c011_f', t021c011_f),
            ('t021c012_t', t021c012_t),
            ('t021c012_m', t021c012_m),
            ('t021c012_f', t021c012_f),
            ('t021c013_t', t021c013_t),
            ('t021c013_m', t021c013_m),
            ('t021c013_f', t021c013_f),
            ('t022c001_t', t022c001_t),
            ('t022c002_t', t022c002_t),
            ('t022c003_t', t022c003_t),
            ('t022c005_t', t022c005_t),
            ('t022c006_t', t022c006_t),
            ('t022c007_t', t022c007_t),
            ('t022c008_t', t022c008_t),
            ('t022c009_t', t022c009_t),
            ('t022c010_t', t022c010_t),
            ('t022c012_t', t022c012_t),
            ('t022c013_t', t022c013_t),
            ('t022c014_t', t022c014_t),
            ('t022c015_t', t022c015_t),
            ('t022c016_t', t022c016_t),
            ('t022c017_t', t022c017_t),
            ('t022c019_t', t022c019_t),
            ('t022c020_t', t022c020_t),
            ('t022c021_t', t022c021_t),
            ('t022c022_t', t022c022_t),
            ('t023c002_t', t023c002_t),
            ('t023c002_m', t023c002_m),
            ('t023c002_f', t023c002_f),
            ('t023c003_t', t023c003_t),
            ('t023c003_m', t023c003_m),
            ('t023c003_f', t023c003_f),
            ('t023c004_t', t023c004_t),
            ('t023c004_m', t023c004_m),
            ('t023c004_f', t023c004_f),
            ('t023c005_t', t023c005_t),
            ('t023c005_m', t023c005_m),
            ('t023c005_f', t023c005_f),
            ('t024c001_t', t024c001_t),
            ('t024c001_m', t024c001_m),
            ('t024c001_f', t024c001_f),
            ('t024c002_t', t024c002_t),
            ('t024c002_m', t024c002_m),
            ('t024c002_f', t024c002_f),
            ('t024c003_t', t024c003_t),
            ('t024c003_m', t024c003_m),
            ('t024c003_f', t024c003_f),
            ('t024c004_t', t024c004_t),
            ('t024c004_m', t024c004_m),
            ('t024c004_f', t024c004_f),
            ('t024c005_t', t024c005_t),
            ('t024c005_m', t024c005_m),
            ('t024c005_f', t024c005_f),
            ('t024c006_t', t024c006_t),
            ('t024c006_m', t024c006_m),
            ('t024c006_f', t024c006_f),
            ('t024c007_t', t024c007_t),
            ('t024c007_m', t024c007_m),
            ('t024c007_f', t024c007_f),
            ('t024c008_t', t024c008_t),
            ('t024c008_m', t024c008_m),
            ('t024c008_f', t024c008_f),
            ('t024c009_t', t024c009_t),
            ('t024c009_m', t024c009_m),
            ('t024c009_f', t024c009_f),
            ('t024c010_t', t024c010_t),
            ('t024c010_m', t024c010_m),
            ('t024c010_f', t024c010_f),
            ('t024c011_t', t024c011_t),
            ('t024c011_m', t024c011_m),
            ('t024c011_f', t024c011_f),
            ('t024c012_t', t024c012_t),
            ('t024c012_m', t024c012_m),
            ('t024c012_f', t024c012_f),
            ('t024c013_t', t024c013_t),
            ('t024c013_m', t024c013_m),
            ('t024c013_f', t024c013_f),
            ('t024c014_t', t024c014_t),
            ('t024c014_m', t024c014_m),
            ('t024c014_f', t024c014_f),
            ('t024c015_t', t024c015_t),
            ('t024c015_m', t024c015_m),
            ('t024c015_f', t024c015_f),
            ('t024c016_t', t024c016_t),
            ('t024c016_m', t024c016_m),
            ('t024c016_f', t024c016_f),
            ('t024c017_t', t024c017_t),
            ('t024c017_m', t024c017_m),
            ('t024c017_f', t024c017_f),
            ('t024c018_t', t024c018_t),
            ('t024c018_m', t024c018_m),
            ('t024c018_f', t024c018_f),
            ('t024c019_t', t024c019_t),
            ('t024c019_m', t024c019_m),
            ('t024c019_f', t024c019_f),
            ('t024c020_t', t024c020_t),
            ('t024c020_m', t024c020_m),
            ('t024c020_f', t024c020_f),
            ('t024c021_t', t024c021_t),
            ('t024c021_m', t024c021_m),
            ('t024c021_f', t024c021_f),
            ('t024c022_t', t024c022_t),
            ('t024c022_m', t024c022_m),
            ('t024c022_f', t024c022_f),
            ('t024c023_t', t024c023_t),
            ('t024c023_m', t024c023_m),
            ('t024c023_f', t024c023_f),
            ('t024c024_t', t024c024_t),
            ('t024c024_m', t024c024_m),
            ('t024c024_f', t024c024_f),
            ('t024c025_t', t024c025_t),
            ('t024c025_m', t024c025_m),
            ('t024c025_f', t024c025_f),
            ('t024c026_t', t024c026_t),
            ('t024c026_m', t024c026_m),
            ('t024c026_f', t024c026_f),
            ('t024c027_t', t024c027_t),
            ('t024c027_m', t024c027_m),
            ('t024c027_f', t024c027_f),
            ('t024c028_t', t024c028_t),
            ('t024c028_m', t024c028_m),
            ('t024c028_f', t024c028_f),
            ('t024c029_t', t024c029_t),
            ('t024c029_m', t024c029_m),
            ('t024c029_f', t024c029_f),
            ('t024c030_t', t024c030_t),
            ('t024c030_m', t024c030_m),
            ('t024c030_f', t024c030_f),
            ('t024c031_t', t024c031_t),
            ('t024c031_m', t024c031_m),
            ('t024c031_f', t024c031_f),
            ('t024c032_t', t024c032_t),
            ('t024c032_m', t024c032_m),
            ('t024c032_f', t024c032_f),
            ('t024c033_t', t024c033_t),
            ('t024c033_m', t024c033_m),
            ('t024c033_f', t024c033_f),
            ('t024c034_t', t024c034_t),
            ('t024c034_m', t024c034_m),
            ('t024c034_f', t024c034_f),
            ('t024c035_t', t024c035_t),
            ('t024c035_m', t024c035_m),
            ('t024c035_f', t024c035_f),
            ('t024c036_t', t024c036_t),
            ('t024c036_m', t024c036_m),
            ('t024c036_f', t024c036_f),
            ('t024c037_t', t024c037_t),
            ('t024c037_m', t024c037_m),
            ('t024c037_f', t024c037_f),
            ('t024c038_t', t024c038_t),
            ('t024c038_m', t024c038_m),
            ('t024c038_f', t024c038_f),
            ('t024c039_t', t024c039_t),
            ('t024c039_m', t024c039_m),
            ('t024c039_f', t024c039_f),
            ('t024c040_t', t024c040_t),
            ('t024c040_m', t024c040_m),
            ('t024c040_f', t024c040_f),
            ('t024c041_t', t024c041_t),
            ('t024c041_m', t024c041_m),
            ('t024c041_f', t024c041_f),
            ('t024c042_t', t024c042_t),
            ('t024c042_m', t024c042_m),
            ('t024c042_f', t024c042_f),
            ('t024c043_t', t024c043_t),
            ('t024c043_m', t024c043_m),
            ('t024c043_f', t024c043_f),
            ('t024c044_t', t024c044_t),
            ('t024c044_m', t024c044_m),
            ('t024c044_f', t024c044_f),
            ('t024c045_t', t024c045_t),
            ('t024c045_m', t024c045_m),
            ('t024c045_f', t024c045_f),
            ('t024c046_t', t024c046_t),
            ('t024c046_m', t024c046_m),
            ('t024c046_f', t024c046_f),
            ('t024c047_t', t024c047_t),
            ('t024c047_m', t024c047_m),
            ('t024c047_f', t024c047_f),
            ('t024c048_t', t024c048_t),
            ('t024c048_m', t024c048_m),
            ('t024c048_f', t024c048_f),
            ('t024c049_t', t024c049_t),
            ('t024c049_m', t024c049_m),
            ('t024c049_f', t024c049_f),
            ('t024c050_t', t024c050_t),
            ('t024c050_m', t024c050_m),
            ('t024c050_f', t024c050_f),
            ('t024c051_t', t024c051_t),
            ('t024c051_m', t024c051_m),
            ('t024c051_f', t024c051_f),
            ('t024c052_t', t024c052_t),
            ('t024c052_m', t024c052_m),
            ('t024c052_f', t024c052_f),
            ('t024c053_t', t024c053_t),
            ('t024c053_m', t024c053_m),
            ('t024c053_f', t024c053_f),
            ('t024c054_t', t024c054_t),
            ('t024c054_m', t024c054_m),
            ('t024c054_f', t024c054_f),
            ('t024c055_t', t024c055_t),
            ('t024c055_m', t024c055_m),
            ('t024c055_f', t024c055_f),
            ('t024c056_t', t024c056_t),
            ('t024c056_m', t024c056_m),
            ('t024c056_f', t024c056_f),
            ('t024c057_t', t024c057_t),
            ('t024c057_m', t024c057_m),
            ('t024c057_f', t024c057_f),
            ('t024c058_t', t024c058_t),
            ('t024c058_m', t024c058_m),
            ('t024c058_f', t024c058_f),
            ('t024c059_t', t024c059_t),
            ('t024c059_m', t024c059_m),
            ('t024c059_f', t024c059_f),
            ('t024c060_t', t024c060_t),
            ('t024c060_m', t024c060_m),
            ('t024c060_f', t024c060_f),
            ('t025c002_t', t025c002_t),
            ('t025c002_m', t025c002_m),
            ('t025c002_f', t025c002_f),
            ('t025c003_t', t025c003_t),
            ('t025c003_m', t025c003_m),
            ('t025c003_f', t025c003_f),
            ('t025c004_t', t025c004_t),
            ('t025c004_m', t025c004_m),
            ('t025c004_f', t025c004_f),
            ('t025c005_t', t025c005_t),
            ('t025c005_m', t025c005_m),
            ('t025c005_f', t025c005_f),
            ('t025c006_t', t025c006_t),
            ('t025c006_m', t025c006_m),
            ('t025c006_f', t025c006_f),
            ('t025c007_t', t025c007_t),
            ('t025c007_m', t025c007_m),
            ('t025c007_f', t025c007_f),
            ('t025c008_t', t025c008_t),
            ('t025c008_m', t025c008_m),
            ('t025c008_f', t025c008_f),
            ('t025c009_t', t025c009_t),
            ('t025c009_m', t025c009_m),
            ('t025c009_f', t025c009_f),
            ('t025c010_t', t025c010_t),
            ('t025c010_m', t025c010_m),
            ('t025c010_f', t025c010_f),
            ('t025c011_t', t025c011_t),
            ('t025c011_m', t025c011_m),
            ('t025c011_f', t025c011_f),
            ('t025c012_t', t025c012_t),
            ('t025c012_m', t025c012_m),
            ('t025c012_f', t025c012_f),
            ('t025c013_t', t025c013_t),
            ('t025c013_m', t025c013_m),
            ('t025c013_f', t025c013_f),
            ('t025c014_t', t025c014_t),
            ('t025c014_m', t025c014_m),
            ('t025c014_f', t025c014_f),
            ('t025c015_t', t025c015_t),
            ('t025c015_m', t025c015_m),
            ('t025c015_f', t025c015_f),
            ('t025c016_t', t025c016_t),
            ('t025c016_m', t025c016_m),
            ('t025c016_f', t025c016_f),
            ('t025c017_t', t025c017_t),
            ('t025c017_m', t025c017_m),
            ('t025c017_f', t025c017_f),
            ('t025c018_t', t025c018_t),
            ('t025c018_m', t025c018_m),
            ('t025c018_f', t025c018_f),
            ('t025c019_t', t025c019_t),
            ('t025c019_m', t025c019_m),
            ('t025c019_f', t025c019_f),
            ('t026c001_t', t026c001_t),
            ('t026c002_t', t026c002_t),
            ('t026c003_t', t026c003_t),
            ('t026c004_t', t026c004_t),
            ('t026c005_t', t026c005_t),
            ('t026c006_t', t026c006_t),
            ('t026c007_t', t026c007_t),
            ('t026c008_t', t026c008_t),
            ('t026c009_t', t026c009_t),
            ('t026c010_t', t026c010_t),
            ('t026c011_t', t026c011_t),
            ('t026c012_t', t026c012_t),
            ('t026c013_t', t026c013_t),
            ('t026c014_t', t026c014_t),
            ('t026c015_t', t026c015_t),
            ('t026c016_t', t026c016_t),
            ('t027c002_t', t027c002_t),
            ('t027c002_m', t027c002_m),
            ('t027c002_f', t027c002_f),
            ('t027c003_t', t027c003_t),
            ('t027c003_m', t027c003_m),
            ('t027c003_f', t027c003_f),
            ('t027c004_t', t027c004_t),
            ('t027c004_m', t027c004_m),
            ('t027c004_f', t027c004_f),
            ('t028c002_t', t028c002_t),
            ('t028c002_m', t028c002_m),
            ('t028c002_f', t028c002_f),
            ('t028c003_t', t028c003_t),
            ('t028c003_m', t028c003_m),
            ('t028c003_f', t028c003_f),
            ('t028c004_t', t028c004_t),
            ('t028c004_m', t028c004_m),
            ('t028c004_f', t028c004_f),
            ('t028c005_t', t028c005_t),
            ('t028c005_m', t028c005_m),
            ('t028c005_f', t028c005_f),
            ('t028c006_t', t028c006_t),
            ('t028c006_m', t028c006_m),
            ('t028c006_f', t028c006_f),
            ('t028c007_t', t028c007_t),
            ('t028c007_m', t028c007_m),
            ('t028c007_f', t028c007_f),
            ('t028c008_t', t028c008_t),
            ('t028c008_m', t028c008_m),
            ('t028c008_f', t028c008_f),
            ('t028c009_t', t028c009_t),
            ('t028c009_m', t028c009_m),
            ('t028c009_f', t028c009_f),
            ('t028c010_t', t028c010_t),
            ('t028c010_m', t028c010_m),
            ('t028c010_f', t028c010_f),
            ('t028c011_t', t028c011_t),
            ('t028c011_m', t028c011_m),
            ('t028c011_f', t028c011_f),
            ('t028c012_t', t028c012_t),
            ('t028c012_m', t028c012_m),
            ('t028c012_f', t028c012_f),
            ('t028c013_t', t028c013_t),
            ('t028c013_m', t028c013_m),
            ('t028c013_f', t028c013_f),
            ('t028c014_t', t028c014_t),
            ('t028c014_m', t028c014_m),
            ('t028c014_f', t028c014_f),
            ('t028c015_t', t028c015_t),
            ('t028c015_m', t028c015_m),
            ('t028c015_f', t028c015_f),
            ('t029c002_t', t029c002_t),
            ('t029c002_m', t029c002_m),
            ('t029c002_f', t029c002_f),
            ('t029c003_t', t029c003_t),
            ('t029c003_m', t029c003_m),
            ('t029c003_f', t029c003_f),
            ('t029c004_t', t029c004_t),
            ('t029c004_m', t029c004_m),
            ('t029c004_f', t029c004_f),
            ('t029c005_t', t029c005_t),
            ('t029c005_m', t029c005_m),
            ('t029c005_f', t029c005_f),
            ('t029c006_t', t029c006_t),
            ('t029c006_m', t029c006_m),
            ('t029c006_f', t029c006_f),
            ('t029c007_t', t029c007_t),
            ('t029c007_m', t029c007_m),
            ('t029c007_f', t029c007_f),
            ('t029c008_t', t029c008_t),
            ('t029c008_m', t029c008_m),
            ('t029c008_f', t029c008_f),
            ('t029c009_t', t029c009_t),
            ('t029c009_m', t029c009_m),
            ('t029c009_f', t029c009_f),
        ])
