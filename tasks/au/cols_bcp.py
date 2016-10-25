from tasks.meta import OBSColumn, DENOMINATOR, UNIVERSE
from tasks.util import ColumnsTask
from tasks.tags import SectionTags, SubsectionTags, UnitTags

from collections import OrderedDict

class BCPColumns(ColumnsTask):

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

        au = input_['sections']['au']

        Tot_P_M = OBSColumn(
            id='Tot_P_M',
            name='Total Persons Males',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[au, unit_people, subsections['age_gender']],
            targets={},)

        Tot_P_F = OBSColumn(
            id='Tot_P_F',
            name='Total Persons Females',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[au, unit_people, subsections['age_gender']],
            targets={},)

        Tot_P_P = OBSColumn(
            id='Tot_P_P',
            name='Total Persons Persons',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[au, unit_people, subsections['age_gender']],
            targets={},)


        return OrderedDict([
            ('Tot_P_M', Tot_P_M),
            ('Tot_P_F', Tot_P_F),
            ('Tot_P_P', Tot_P_P),
        ])
