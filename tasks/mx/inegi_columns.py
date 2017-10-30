# -*- coding: utf-8 -*-

import os
from tasks.meta import OBSColumn, DENOMINATOR, OBSTag
from tasks.util import ColumnsTask, TagsTask
from tasks.tags import SectionTags, SubsectionTags, UnitTags
from lib.columns import ColumnsDeclarations
from luigi import Parameter
from collections import OrderedDict


class SourceTags(TagsTask):
    def version(self):
        return 1

    def tags(self):
        return [
            OBSTag(id='inegi-source',
                   name='Instituto Nacional de Estadística y Geografía (INEGI)',
                   type='source',
                   description='INEGI data provided by `Diego Valle-Jones <https://www.diegovalle.net/>`_'
                  )]


class LicenseTags(TagsTask):
    def version(self):
        return 2

    def tags(self):
        return [
            OBSTag(id='inegi-license',
                   name='Free use of information from INEGI',
                   type='license',
                   description='Terms of free use can be read in detail `here <http://www.beta.inegi.org.mx/contenidos/inegi/doc/terminos_info.pdf>`_.'
                  )]


class DemographicColumns(ColumnsTask):
    resolution = Parameter()
    table = Parameter()

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'unittags': UnitTags(),
            'source': SourceTags(),
            'license': LicenseTags(),
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
        license = input_['license']['inegi-license']
        source = input_['source']['inegi-source']

        mexico = input_['sections']['mx']

        pop = OBSColumn(
            id='POB1',
            name='Total population',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={},
        )
        pop_0_2 = OBSColumn(
            id='POB2',
            name='Population 0 to 2 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_0_4 = OBSColumn(
            id='POB3',
            name='Population 0 to 4 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_3_5 = OBSColumn(
            id='POB4',
            name='Population 3 to 5 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_6_11 = OBSColumn(
            id='POB5',
            name='Population 6 to 11 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_8_14 = OBSColumn(
            id='POB6',
            name='Population 8 to 14 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_12_14 = OBSColumn(
            id='POB7',
            name='Population 12 to 14 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_0_14 = OBSColumn(
            id='POB8',
            name='Population 0 to 14 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_15_17 = OBSColumn(
            id='POB9',
            name='Population 15 to 17 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_15_24 = OBSColumn(
            id='POB10',
            name='Population 15 to 24 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_15_29 = OBSColumn(
            id='POB11',
            name='Population 15 to 29 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_15_64 = OBSColumn(
            id='POB12',
            name='Population 15 to 64 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_18_24 = OBSColumn(
            id='POB13',
            name='Population 18 to 24 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_30_49 = OBSColumn(
            id='POB14',
            name='Population 30 to 49 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_50_59 = OBSColumn(
            id='POB15',
            name='Population 50 to 59 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_60_64 = OBSColumn(
            id='POB16',
            name='Population 60 to 64 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_3_more = OBSColumn(
            id='POB17',
            name='Population 3 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_5_more = OBSColumn(
            id='POB18',
            name='Population 5 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_12_more = OBSColumn(
            id='POB19',
            name='Population 12 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_15_more = OBSColumn(
            id='POB20',
            name='Population 15 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_18_more = OBSColumn(
            id='POB21',
            name='Population 18 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_25_more = OBSColumn(
            id='POB22',
            name='Population 25 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_60_more = OBSColumn(
            id='POB23',
            name='Population 60 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_65_more = OBSColumn(
            id='POB24',
            name='Population 65 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        pop_70_more = OBSColumn(
            id='POB25',
            name='Population 70 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        female = OBSColumn(
            id='POB31',
            name='Female population',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        female_0_2 = OBSColumn(
            id='POB32',
            name='Female population 0 to 2 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_0_2: DENOMINATOR},
        )
        female_0_4 = OBSColumn(
            id='POB33',
            name='Female population 0 to 4 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_0_4: DENOMINATOR},
        )
        female_3_5 = OBSColumn(
            id='POB34',
            name='Female population 3 to 5 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_3_5: DENOMINATOR},
        )
        female_6_11 = OBSColumn(
            id='POB35',
            name='Female population 6 to 11 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_6_11: DENOMINATOR},
        )
        female_8_14 = OBSColumn(
            id='POB36',
            name='Female population 8 to 14 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_8_14: DENOMINATOR},
        )
        female_12_14 = OBSColumn(
            id='POB37',
            name='Female population 12 to 14 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_12_14: DENOMINATOR},
        )
        female_0_14 = OBSColumn(
            id='POB38',
            name='Female population 0 to 14 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_0_14: DENOMINATOR},
        )
        female_15_17 = OBSColumn(
            id='POB39',
            name='Female population 15 to 17 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_15_17: DENOMINATOR},
        )
        female_15_24 = OBSColumn(
            id='POB40',
            name='Female population 15 to 24 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_15_24: DENOMINATOR},
        )
        female_15_29 = OBSColumn(
            id='POB41',
            name='Female population 15 to 29 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_15_29: DENOMINATOR},
        )
        female_15_49 = OBSColumn(
            id='POB42',
            name='Female population 15 to 49 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR},
        )
        female_15_64 = OBSColumn(
            id='POB43',
            name='Female population 15 to 64 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_15_64: DENOMINATOR},
        )
        female_18_24 = OBSColumn(
            id='POB44',
            name='Female population 18 to 24 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_18_24: DENOMINATOR},
        )
        female_30_49 = OBSColumn(
            id='POB45',
            name='Female population 30 to 49 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_30_49: DENOMINATOR},
        )
        female_50_59 = OBSColumn(
            id='POB46',
            name='Female population 50 to 59 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_50_59: DENOMINATOR},
        )
        female_60_64 = OBSColumn(
            id='POB47',
            name='Female population 60 to 64 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_60_64: DENOMINATOR},
        )
        female_3_more = OBSColumn(
            id='POB48',
            name='Female population 3 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_3_more: DENOMINATOR},
        )
        female_5_more = OBSColumn(
            id='POB49',
            name='Female population 5 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_5_more: DENOMINATOR},
        )
        female_12_more = OBSColumn(
            id='POB50',
            name='Female population 12 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_12_more: DENOMINATOR},
        )
        female_15_more = OBSColumn(
            id='POB51',
            name='Female population 15 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_15_more: DENOMINATOR},
        )
        female_18_more = OBSColumn(
            id='POB52',
            name='Female population 18 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_18_more: DENOMINATOR},
        )
        female_25_more = OBSColumn(
            id='POB53',
            name='Female population 25 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_25_more: DENOMINATOR},
        )
        female_60_more = OBSColumn(
            id='POB54',
            name='Female population 60 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_60_more: DENOMINATOR},
        )
        female_65_more = OBSColumn(
            id='POB55',
            name='Female population 65 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_65_more: DENOMINATOR},
        )
        female_70_more = OBSColumn(
            id='POB56',
            name='Female population 70 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={female: DENOMINATOR, pop_70_more: DENOMINATOR},
        )
        male = OBSColumn(
            id='POB57',
            name='Male population',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={pop: DENOMINATOR},
        )
        male_0_2 = OBSColumn(
            id='POB58',
            name='Male population 0 to 2 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_0_2: DENOMINATOR},
        )
        male_0_4 = OBSColumn(
            id='POB59',
            name='Male population 0 to 4 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_0_4: DENOMINATOR},
        )
        male_3_5 = OBSColumn(
            id='POB60',
            name='Male population 3 to 5 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_3_5: DENOMINATOR},
        )
        male_6_11 = OBSColumn(
            id='POB61',
            name='Male population 6 to 11 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_6_11: DENOMINATOR},
        )
        male_8_14 = OBSColumn(
            id='POB62',
            name='Male population 8 to 14 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_8_14: DENOMINATOR},
        )
        male_12_14 = OBSColumn(
            id='POB63',
            name='Male population 12 to 14 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_12_14: DENOMINATOR},
        )
        male_0_14 = OBSColumn(
            id='POB64',
            name='Male population 0 to 14 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_0_14: DENOMINATOR},
        )
        male_15_17 = OBSColumn(
            id='POB65',
            name='Male population 15 to 17 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_15_17: DENOMINATOR},
        )
        male_15_24 = OBSColumn(
            id='POB66',
            name='Male population 15 to 24 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_15_24: DENOMINATOR},
        )
        male_15_29 = OBSColumn(
            id='POB67',
            name='Male population 15 to 29 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_15_29: DENOMINATOR},
        )
        male_15_64 = OBSColumn(
            id='POB68',
            name='Male population 15 to 64 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_15_64: DENOMINATOR},
        )
        male_18_24 = OBSColumn(
            id='POB69',
            name='Male population 18 to 24 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_18_24: DENOMINATOR},
        )
        male_30_49 = OBSColumn(
            id='POB70',
            name='Male population 30 to 49 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_30_49: DENOMINATOR},
        )
        male_50_59 = OBSColumn(
            id='POB71',
            name='Male population 50 to 59 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_50_59: DENOMINATOR},
        )
        male_60_64 = OBSColumn(
            id='POB72',
            name='Male population 60 to 64 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_60_64: DENOMINATOR},
        )
        male_3_more = OBSColumn(
            id='POB73',
            name='Male population 3 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_3_more: DENOMINATOR},
        )
        male_5_more = OBSColumn(
            id='POB74',
            name='Male population 5 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_5_more: DENOMINATOR},
        )
        male_12_more = OBSColumn(
            id='POB75',
            name='Male population 12 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_12_more: DENOMINATOR},
        )
        male_15_more = OBSColumn(
            id='POB76',
            name='Male population 15 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_15_more: DENOMINATOR},
        )
        male_18_more = OBSColumn(
            id='POB77',
            name='Male population 18 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_18_more: DENOMINATOR},
        )
        male_25_more = OBSColumn(
            id='POB78',
            name='Male population 25 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_25_more: DENOMINATOR},
        )
        male_60_more = OBSColumn(
            id='POB79',
            name='Male population 60 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_60_more: DENOMINATOR},
        )
        male_65_more = OBSColumn(
            id='POB80',
            name='Male population 65 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_65_more: DENOMINATOR},
        )
        male_70_more = OBSColumn(
            id='POB81',
            name='Male population 70 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['age_gender']],
            targets={male: DENOMINATOR, pop_70_more: DENOMINATOR},
        )
        born_in_state = OBSColumn(
            id='MIG1',
            name='Population born in the state',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['migration']],
            targets={pop: DENOMINATOR},
        )
        female_born_in_state = OBSColumn(
            id='MIG2',
            name='Female population born in the state',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['migration']],
            targets={born_in_state: DENOMINATOR, female: DENOMINATOR},
        )
        male_born_in_state = OBSColumn(
            id='MIG3',
            name='Male population born in the state',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['migration'], ],
            targets={born_in_state: DENOMINATOR, male: DENOMINATOR, },
        )
        born_in_another_state = OBSColumn(
            id='MIG4',
            name='Population born in another state',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['migration'], ],
            targets={pop: DENOMINATOR, },
        )
        female_born_in_another_state = OBSColumn(
            id='MIG5',
            name='Female population born in another state',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['migration'], ],
            targets={born_in_another_state: DENOMINATOR, female: DENOMINATOR, },
        )
        male_born_in_another_state = OBSColumn(
            id='MIG6',
            name='Male population born in another state',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['migration'], ],
            targets={born_in_another_state: DENOMINATOR, male: DENOMINATOR, },
        )
        born_in_another_country = OBSColumn(
            id='MIG7',
            name='Population born in another country',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['migration'], ],
            targets={pop: DENOMINATOR, },
        )
        in_state_june_05 = OBSColumn(
            id='MIG8',
            name='Population resident in the state in June 2005',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['migration'], ],
            targets={pop_5_more: DENOMINATOR, },
        )
        female_in_state_june_05 = OBSColumn(
            id='MIG9',
            name='Female population resident in the state in June 2005',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['migration'], ],
            targets={female_5_more: DENOMINATOR, in_state_june_05: DENOMINATOR, },
        )
        male_in_state_june_05 = OBSColumn(
            id='MIG10',
            name='Male population resident in the state in June 2005',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['migration'], ],
            targets={male_5_more: DENOMINATOR, in_state_june_05: DENOMINATOR, },
        )
        another_state_june_05 = OBSColumn(
            id='MIG11',
            name='Population resident in another state in June 2005',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['migration'], ],
            targets={pop_5_more: DENOMINATOR, },
        )
        female_another_state_june_05 = OBSColumn(
            id='MIG12',
            name='Female population resident in another state in June 2005',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['migration'], ],
            targets={female_5_more: DENOMINATOR, another_state_june_05: DENOMINATOR, },
        )
        male_another_state_june_05 = OBSColumn(
            id='MIG13',
            name='Male population resident in another state in June 2005',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['migration'], ],
            targets={male_5_more: DENOMINATOR, another_state_june_05: DENOMINATOR, },
        )
        another_country_june_05 = OBSColumn(
            id='MIG14',
            name='Population resident in another country in June 2005',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['migration'], ],
            targets={pop_5_more: DENOMINATOR, },
        )
        united_states_june_05 = OBSColumn(
            id='MIG15',
            name='Population age 5 or more resident in the United States in June 2005',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['migration'], ],
            targets={pop_5_more: DENOMINATOR, },
        )
        speak_ind_language = OBSColumn(
            id='INDI1',
            name='Population age 3 or more who speak an indigenous language',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['language'], ],
            targets={pop_3_more: DENOMINATOR, },
        )
        female_ind_language = OBSColumn(
            id='INDI2',
            name='Female population age 3 or more who speak an indigenous language',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['language'], ],
            targets={female_3_more: DENOMINATOR, speak_ind_language: DENOMINATOR, },
        )
        male_ind_language = OBSColumn(
            id='INDI3',
            name='Male population age 3 or more who speak an indigenous language',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['language'], ],
            targets={male_3_more: DENOMINATOR, speak_ind_language: DENOMINATOR, },
        )
        ind_language_no_spanish = OBSColumn(
            id='INDI4',
            name='Population 3 or more years old who speak an indigenous language and do not speak Spanish',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['language'], ],
            targets={pop_3_more: DENOMINATOR, },
        )
        female_ind_language_no_spanish = OBSColumn(
            id='INDI5',
            name='Female population 3 or more years old who speak an indigenous '
                 'language and do not speak Spanish',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['language'], ],
            targets={female_3_more: DENOMINATOR, ind_language_no_spanish: DENOMINATOR},
        )
        male_ind_language_no_spanish = OBSColumn(
            id='INDI6',
            name='Male population 3 or more years old who speak an indigenous '
                 'language and do not speak Spanish',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['language'], ],
            targets={male_3_more: DENOMINATOR, ind_language_no_spanish: DENOMINATOR},
        )
        ind_language_and_spanish = OBSColumn(
            id='INDI7',
            name='Population 3 or more years old who speak an indigenous language and speak Spanish',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['language'], ],
            targets={pop_3_more: DENOMINATOR, },
        )
        female_ind_language_and_spanish = OBSColumn(
            id='INDI8',
            name='Female population 3 or more years old who speak an indigenous language and speak Spanish',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['language'], ],
            targets={female_3_more: DENOMINATOR, ind_language_and_spanish: DENOMINATOR, },
        )
        male_ind_language_and_spanish = OBSColumn(
            id='INDI9',
            name='Male population 3 or more years old who speak an indigenous language and speak Spanish',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['language'], ],
            targets={male_3_more: DENOMINATOR, ind_language_and_spanish: DENOMINATOR, },
        )
        speak_ind_language_age_5_more = OBSColumn(
            id='INDI10',
            name='Population 5 or more years old who speak an indigenous language',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['language'], ],
            targets={pop_5_more: DENOMINATOR, },
        )
        female_speak_ind_language_age_5_more = OBSColumn(
            id='INDI11',
            name='Female population 5 or more years old who speak an indigenous language',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['language'], ],
            targets={female_5_more: DENOMINATOR, speak_ind_language_age_5_more: DENOMINATOR, },
        )
        male_speak_ind_language_age_5_more = OBSColumn(
            id='INDI12',
            name='Male population 5 or more years old who speak an indigenous language',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['language'], ],
            targets={male_5_more: DENOMINATOR, speak_ind_language_age_5_more: DENOMINATOR, },
        )
        speak_ind_language_no_spanish_age_5_more = OBSColumn(
            id='INDI13',
            name='Population 5 or more years old who speak an indigenous language and do not speak Spanish',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['language'], ],
            targets={pop_5_more: DENOMINATOR, },
        )
        female_speak_ind_language_no_spanish_age_5_more = OBSColumn(
            id='INDI14',
            name='Female population 5 or more years old who speak an indigenous '
                 'language and do not speak Spanish',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['language'], ],
            targets={female_5_more: DENOMINATOR, speak_ind_language_no_spanish_age_5_more: DENOMINATOR, },
        )
        male_speak_ind_language_no_spanish_age_5_more = OBSColumn(
            id='INDI15',
            name='Male population 5 or more years old who speak an indigenous '
                 'language and do not speak Spanish',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['language'], ],
            targets={male_5_more: DENOMINATOR, speak_ind_language_no_spanish_age_5_more: DENOMINATOR, },
        )
        speak_ind_language_and_spanish_age_5_more = OBSColumn(
            id='INDI16',
            name='Population 5 or more years old who speak an indigenous language and speak Spanish',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['language'], ],
            targets={pop_5_more: DENOMINATOR, },
        )
        female_speak_ind_language_and_spanish_age_5_more = OBSColumn(
            id='INDI17',
            name='Female population 5 or more years old who speak an indigenous language and speak Spanish',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['language'], ],
            targets={female_5_more: DENOMINATOR,
                     speak_ind_language_and_spanish_age_5_more: DENOMINATOR},
        )
        male_speak_ind_language_and_spanish_age_5_more = OBSColumn(
            id='INDI18',
            name='Male population 5 or more years old who speak an indigenous language and speak Spanish',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['language'], ],
            targets={male_5_more: DENOMINATOR,
                     speak_ind_language_and_spanish_age_5_more: DENOMINATOR},
        )
        pop_ind_households = OBSColumn(
            id='INDI20',
            name='Population in indigenous census households',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['race_ethnicity'], ],
            targets={pop: DENOMINATOR, },
        )
        disability = OBSColumn(
            id='DISC1',
            name='Population with a disability',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, ],
            targets={pop: DENOMINATOR},
        )
        female_disability = OBSColumn(
            id='DISC2',
            name='Female population with a disability',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, ],
            targets={disability: DENOMINATOR, female: DENOMINATOR, },
        )
        male_disability = OBSColumn(
            id='DISC3',
            name='Male population with a disability.',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, ],
            targets={disability: DENOMINATOR, male: DENOMINATOR, },
        )
        disability_age_14_less = OBSColumn(
            id='DISC4',
            name='Population age 14 or less with a disability',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, ],
            targets={pop_0_14: DENOMINATOR, disability: DENOMINATOR, },
        )
        disability_age_15_59 = OBSColumn(
            id='DISC5',
            name='Population age 15 to 59 with a disability',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, ],
            targets={disability: DENOMINATOR, },
        )
        disability_age_60_more = OBSColumn(
            id='DISC6',
            name='Population age 60 or more years old with a disability.',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, ],
            targets={pop_60_more: DENOMINATOR, disability: DENOMINATOR, },
        )
        disability_movement = OBSColumn(
            id='DISC7',
            name='Population with a disability of movement',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, ],
            targets={disability: DENOMINATOR, },
        )
        disability_sight = OBSColumn(
            id='DISC8',
            name='Population with limited sight, even using lenses',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, ],
            targets={disability: DENOMINATOR, },
        )
        disability_speak = OBSColumn(
            id='DISC9',
            name='Population with limited ability to speak',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, ],
            targets={disability: DENOMINATOR, },
        )
        hearing_disability = OBSColumn(
            id='DISC10',
            name='Population with limited ability to hear',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, ],
            targets={disability: DENOMINATOR, },
        )
        disability_dress_bathe_eat = OBSColumn(
            id='DISC11',
            name='Population with limited ability to dress, bathe or eat',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, ],
            targets={disability: DENOMINATOR, },
        )
        learning_disability = OBSColumn(
            id='DISC12',
            name='Population with a learning disability',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, ],
            targets={disability: DENOMINATOR, },
        )
        mental_limitation = OBSColumn(
            id='DISC13',
            name='Population with a mental limitation',
            type='Numeric',
            weight=0,
            aggregate='sum',
            tags=[mexico, unit_people, ],
            targets={},
        )
        limited_activity_insurance = OBSColumn(
            id='DISC14',
            name='Population with limited activity in insured health services',
            type='Numeric',
            weight=0,
            aggregate='sum',
            tags=[mexico, unit_people, ],
            targets={},
        )
        in_school_3_5 = OBSColumn(
            id='EDU1',
            name='Population from 3 to 5 years old who go to school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={pop_3_5: DENOMINATOR, },
        )
        female_in_school_3_5 = OBSColumn(
            id='EDU2',
            name='Female population from 3 to 5 years old who go to school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={female_3_5: DENOMINATOR, in_school_3_5: DENOMINATOR, },
        )
        male_in_school_3_5 = OBSColumn(
            id='EDU3',
            name='Male population from 3 to 5 years old who go to school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={male_3_5: DENOMINATOR, in_school_3_5: DENOMINATOR, },
        )
        not_in_school_3_5 = OBSColumn(
            id='EDU4',
            name='Population from 3 to 5 years old who do not go to school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={pop_3_5: DENOMINATOR, },
        )
        female_not_in_school_3_5 = OBSColumn(
            id='EDU5',
            name='Female population from 3 to 5 years old who do not go to school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={female_3_5: DENOMINATOR, not_in_school_3_5: DENOMINATOR, },
        )
        male_not_in_school_3_5 = OBSColumn(
            id='EDU6',
            name='Male population from 3 to 5 years old who do not go to school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={male_3_5: DENOMINATOR, not_in_school_3_5: DENOMINATOR, },
        )
        in_school_6_11 = OBSColumn(
            id='EDU7',
            name='Population from 6 to 11 years old who go to school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={pop_6_11: DENOMINATOR, },
        )
        female_in_school_6_11 = OBSColumn(
            id='EDU8',
            name='Female population from 6 to 11 years old who go to school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={female_6_11: DENOMINATOR, in_school_6_11: DENOMINATOR, },
        )
        male_in_school_6_11 = OBSColumn(
            id='EDU9',
            name='Male population from 6 to 11 years old who go to school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={male_6_11: DENOMINATOR, in_school_6_11: DENOMINATOR, },
        )
        not_in_school_6_11 = OBSColumn(
            id='EDU10',
            name='Population from 6 to 11 years old who do not go to school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={pop_6_11: DENOMINATOR, },
        )
        female_not_in_school_6_11 = OBSColumn(
            id='EDU11',
            name='Female population from 6 to 11 years old who do not go to school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={female_6_11: DENOMINATOR, not_in_school_6_11: DENOMINATOR, },
        )
        male_not_in_school_6_11 = OBSColumn(
            id='EDU12',
            name='Male population from 6 to 11 years old who do not go to school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={male_6_11: DENOMINATOR, not_in_school_6_11: DENOMINATOR, },
        )
        in_school_12_14 = OBSColumn(
            id='EDU13',
            name='Population from 12 to 14 years old who go to school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={pop_12_14: DENOMINATOR, },
        )
        female_in_school_12_14 = OBSColumn(
            id='EDU14',
            name='Female population from 12 to 14 years old who go to school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={female_12_14: DENOMINATOR, in_school_12_14: DENOMINATOR, },
        )
        male_in_school_12_14 = OBSColumn(
            id='EDU15',
            name='Male population from 12 to 14 years old who go to school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={male_12_14: DENOMINATOR, in_school_12_14: DENOMINATOR, },
        )
        not_in_school_12_14 = OBSColumn(
            id='EDU16',
            name='Population from 12 to 14 years old who do not go to school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={pop_12_14: DENOMINATOR, },
        )
        female_not_in_school_12_14 = OBSColumn(
            id='EDU17',
            name='Female population from 12 to 14 years old who do not go to school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={female_12_14: DENOMINATOR, not_in_school_12_14: DENOMINATOR, },
        )
        male_not_in_school_12_14 = OBSColumn(
            id='EDU18',
            name='Male population from 12 to 14 years old who do not go to school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={male_12_14: DENOMINATOR, not_in_school_12_14: DENOMINATOR, },
        )
        literate_8_14 = OBSColumn(
            id='EDU19',
            name='Population from 8 to 14 years old who are literate',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={pop_8_14: DENOMINATOR, },
        )
        female_literate_8_14 = OBSColumn(
            id='EDU20',
            name='Female population from 8 to 14 years old who are literate',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={female_8_14: DENOMINATOR, literate_8_14: DENOMINATOR, },
        )
        male_literate_8_14 = OBSColumn(
            id='EDU21',
            name='Male population from 8 to 14 years old who are literate',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={male_8_14: DENOMINATOR, literate_8_14: DENOMINATOR, },
        )
        illiterate_8_14 = OBSColumn(
            id='EDU22',
            name='Population from 8 to 14 years old who are illiterate',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={pop_8_14: DENOMINATOR, },
        )
        female_illiterate_8_14 = OBSColumn(
            id='EDU23',
            name='Female population from 8 to 14 years old who are illiterate',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={female_8_14: DENOMINATOR, illiterate_8_14: DENOMINATOR, },
        )
        male_illiterate_8_14 = OBSColumn(
            id='EDU24',
            name='Male population from 8 to 14 years old who are illiterate',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={male_8_14: DENOMINATOR, illiterate_8_14: DENOMINATOR, },
        )
        literate_15_more = OBSColumn(
            id='EDU25',
            name='Population 15 or more years old who are literate',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={pop_15_more: DENOMINATOR, },
        )
        female_literate_15_more = OBSColumn(
            id='EDU26',
            name='Female population 15 or more years old who are literate',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={female_15_more: DENOMINATOR, literate_15_more: DENOMINATOR, },
        )
        male_literate_15_more = OBSColumn(
            id='EDU27',
            name='Male population 15 or more years old who are literate',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={female_15_more: DENOMINATOR, literate_15_more: DENOMINATOR, },
        )
        illiterate_15_more = OBSColumn(
            id='EDU28',
            name='Population 15 or more years old who are illiterate',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={pop_15_more: DENOMINATOR, },
        )
        female_illiterate_15_more = OBSColumn(
            id='EDU29',
            name='Female population 15 or more years old who are illiterate',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={female_15_more: DENOMINATOR, illiterate_15_more: DENOMINATOR, },
        )
        male_illiterate_15_more = OBSColumn(
            id='EDU30',
            name='Male population 15 or more years old who are illiterate',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={male_15_more: DENOMINATOR, illiterate_15_more: DENOMINATOR, },
        )
        no_school_15_more = OBSColumn(
            id='EDU31',
            name='Population 15 or more years old without schooling',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={pop_15_more: DENOMINATOR, },
        )
        female_no_school_15_more = OBSColumn(
            id='EDU32',
            name='Female population 15 or more years old without schooling',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={no_school_15_more: DENOMINATOR, female_15_more: DENOMINATOR, },
        )
        male_no_school_15_more = OBSColumn(
            id='EDU33',
            name='Male population 15 or more years old without schooling',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={no_school_15_more: DENOMINATOR, male_15_more: DENOMINATOR, },
        )
        no_basic_education_15_more = OBSColumn(
            id='EDU34',
            name='Population 15 or more years old who did not complete basic education',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={pop_15_more: DENOMINATOR, },
        )
        female_no_basic_education_15_more = OBSColumn(
            id='EDU35',
            name='Female population 15 or more years old who did not complete basic education',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={female_15_more: DENOMINATOR, no_basic_education_15_more: DENOMINATOR, },
        )
        male_no_basic_education_15_more = OBSColumn(
            id='EDU36',
            name='Male population 15 or more years old who did not complete basic education',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={female_15_more: DENOMINATOR, no_basic_education_15_more: DENOMINATOR, },
        )
        basic_education_15_more = OBSColumn(
            id='EDU37',
            name='Population 15 or more years old who completed basic education',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={pop_15_more: DENOMINATOR, },
        )
        female_basic_education_15_more = OBSColumn(
            id='EDU38',
            name='Female population 15 or more years old who completed basic education',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={female_15_more: DENOMINATOR, basic_education_15_more: DENOMINATOR, },
        )
        male_basic_education_15_more = OBSColumn(
            id='EDU39',
            name='Male population 15 or more years old who completed basic education',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={male_15_more: DENOMINATOR, basic_education_15_more: DENOMINATOR, },
        )
        some_high_school_15_more = OBSColumn(
            id='EDU40',
            name='Population 15 or more years old with some high school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={pop_15_more: DENOMINATOR, },
        )
        female_some_high_school_15_more = OBSColumn(
            id='EDU41',
            name='Female population 15 or more years old with some high school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={female_15_more: DENOMINATOR, some_high_school_15_more: DENOMINATOR, },
        )
        male_some_high_school_15_more = OBSColumn(
            id='EDU42',
            name='Male population 15 or more years old with some high school',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={male_15_more: DENOMINATOR, some_high_school_15_more: DENOMINATOR, },
        )
        high_school_degree = OBSColumn(
            id='EDU43',
            name='Population 18 or more years old with a high school degree',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={pop_18_more: DENOMINATOR, },
        )
        female_high_school_degree = OBSColumn(
            id='EDU44',
            name='Female population 18 or more years old with a high school degree',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={female_18_more: DENOMINATOR, high_school_degree: DENOMINATOR, },
        )
        male_high_school_degree = OBSColumn(
            id='EDU45',
            name='Male population 18 or more years old with a high school degree',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={male_18_more: DENOMINATOR, high_school_degree: DENOMINATOR, },
        )
        university_degree = OBSColumn(
            id='EDU46',
            name='Population 25 or more years old with a university degree',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={pop_25_more: DENOMINATOR, },
        )
        female_university_degree = OBSColumn(
            id='EDU47',
            name='Female population 25 or more years old with a university degree',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={female_25_more: DENOMINATOR, university_degree: DENOMINATOR, },
        )
        male_university_degree = OBSColumn(
            id='EDU48',
            name='Male population 25 or more years old with a university degree',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['education'], ],
            targets={male_25_more: DENOMINATOR, university_degree: DENOMINATOR, },
        )
        econ_active = OBSColumn(
            id='ECO1',
            name='Economically active population',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'], ],
            targets={pop: DENOMINATOR, },
        )
        female_econ_active = OBSColumn(
            id='ECO2',
            name='Economically active female population',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'], ],
            targets={female: DENOMINATOR, econ_active: DENOMINATOR, },
        )
        male_econ_active = OBSColumn(
            id='ECO3',
            name='Economically active male population',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'], ],
            targets={male: DENOMINATOR, econ_active: DENOMINATOR, },
        )
        employed = OBSColumn(
            id='ECO4',
            name='Employed',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'], ],
            targets={econ_active: DENOMINATOR, },
        )
        female_employed = OBSColumn(
            id='ECO5',
            name='Employed female',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'], ],
            targets={employed: DENOMINATOR, female_econ_active: DENOMINATOR, },
        )
        male_employed = OBSColumn(
            id='ECO6',
            name='Employed male',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'], ],
            targets={employed: DENOMINATOR, male_econ_active: DENOMINATOR, },
        )
        employed_without_school = OBSColumn(
            id='ECO7',
            name='Employed population without schooling',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],
                  subsections['education'], ],
            targets={employed: DENOMINATOR, },
        )
        female_employed_without_school = OBSColumn(
            id='ECO8',
            name='Employed female population without schooling',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],
                  subsections['education'], ],
            targets={employed_without_school: DENOMINATOR,
                     female_employed: DENOMINATOR},
        )
        male_employed_without_school = OBSColumn(
            id='ECO9',
            name='Employed male population without schooling',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],
                  subsections['education'], ],
            targets={employed_without_school: DENOMINATOR,
                     male_employed: DENOMINATOR},
        )
        employed_primary_education = OBSColumn(
            id='ECO10',
            name='Employed population with primary education',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],
                  subsections['education'], ],
            targets={employed: DENOMINATOR, },
        )
        female_employed_primary_education = OBSColumn(
            id='ECO11',
            name='Employed female population with primary education',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],
                  subsections['education'], ],
            targets={employed_primary_education: DENOMINATOR,
                     female_employed: DENOMINATOR, },
        )
        male_employed_primary_education = OBSColumn(
            id='ECO12',
            name='Employed male population with primary education',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],
                  subsections['education'], ],
            targets={employed_primary_education: DENOMINATOR,
                     male_employed: DENOMINATOR, },
        )
        employed_incomplete_secondary_education = OBSColumn(
            id='ECO13',
            name='Employed population with incomplete secondary education',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],
                  subsections['education'], ],
            targets={employed: DENOMINATOR, },
        )
        female_employed_incomplete_secondary_education = OBSColumn(
            id='ECO14',
            name='Employed female population with incomplete secondary education',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],
                  subsections['education'], ],
            targets={employed_incomplete_secondary_education: DENOMINATOR,
                     female_employed: DENOMINATOR, },
        )
        male_employed_incomplete_secondary_education = OBSColumn(
            id='ECO15',
            name='Employed male population with incomplete secondary education',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],
                  subsections['education'], ],
            targets={employed_incomplete_secondary_education: DENOMINATOR,
                     male_employed: DENOMINATOR, },
        )
        employed_basic_education = OBSColumn(
            id='ECO16',
            name='Employed population who completed basic education',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],
                  subsections['education'], ],
            targets={employed: DENOMINATOR, },
        )
        female_employed_basic_education = OBSColumn(
            id='ECO17',
            name='Employed female population who completed basic education',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],
                  subsections['education'], ],
            targets={employed_basic_education: DENOMINATOR,
                     female_employed: DENOMINATOR, },
        )
        male_employed_basic_education = OBSColumn(
            id='ECO18',
            name='Employed male population who completed basic education',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],
                  subsections['education'], ],
            targets={employed_basic_education: DENOMINATOR,
                     male_employed: DENOMINATOR, },
        )
        employed_high_school_degree = OBSColumn(
            id='ECO19',
            name='Employed population with a high school degree',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],
                  subsections['education'], ],
            targets={employed: DENOMINATOR, },
        )
        female_employed_high_school_degree = OBSColumn(
            id='ECO20',
            name='Employed female population with a high school degree',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],
                  subsections['education'], ],
            targets={employed_high_school_degree: DENOMINATOR,
                     female_employed: DENOMINATOR, },
        )
        male_employed_high_school_degree = OBSColumn(
            id='ECO21',
            name='Employed male population with a high school degree',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],
                  subsections['education'], ],
            targets={employed_high_school_degree: DENOMINATOR,
                     male_employed: DENOMINATOR, },
        )
        employed_university_degree = OBSColumn(
            id='ECO22',
            name='Employed population with a university degree or more',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],
                  subsections['education'], ],
            targets={employed: DENOMINATOR, },
        )
        female_employed_university_degree = OBSColumn(
            id='ECO23',
            name='Employed female population with a university degree or more',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],
                  subsections['education'], ],
            targets={female_employed: DENOMINATOR,
                     employed_university_degree: DENOMINATOR, },
        )
        male_employed_university_degree = OBSColumn(
            id='ECO24',
            name='Employed male population with a university degree or more',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],
                  subsections['education'], ],
            targets={male_employed: DENOMINATOR,
                     employed_university_degree: DENOMINATOR, },
        )
        unemployed = OBSColumn(
            id='ECO25',
            name='Unemployed',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],],
            targets={econ_active: DENOMINATOR, },
        )
        female_unemployed = OBSColumn(
            id='ECO26',
            name='Unemployed female',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],],
            targets={unemployed: DENOMINATOR, female_econ_active: DENOMINATOR, },
        )
        male_unemployed = OBSColumn(
            id='ECO27',
            name='Unemployed male',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],],
            targets={unemployed: DENOMINATOR, male_econ_active: DENOMINATOR, },
        )
        econ_inactive = OBSColumn(
            id='ECO28',
            name='Economically inactive',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],],
            targets={pop: DENOMINATOR, },
        )
        female_econ_inactive = OBSColumn(
            id='ECO29',
            name='Female economically inactive population',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],],
            targets={female: DENOMINATOR, econ_inactive: DENOMINATOR, },
        )
        male_econ_inactive = OBSColumn(
            id='ECO30',
            name='Male economically inactive population',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],],
            targets={male: DENOMINATOR, econ_inactive: DENOMINATOR, },
        )
        retiree = OBSColumn(
            id='ECO31',
            name='Retiree or receiving pension',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],],
            targets={econ_inactive: DENOMINATOR, },
        )
        female_retiree = OBSColumn(
            id='ECO32',
            name='Female retiree or receiving pension',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],],
            targets={retiree: DENOMINATOR, female_econ_inactive: DENOMINATOR, },
        )
        male_retiree = OBSColumn(
            id='ECO33',
            name='Male retiree or receiving pension',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],],
            targets={retiree: DENOMINATOR, male_econ_inactive: DENOMINATOR, },
        )
        student = OBSColumn(
            id='ECO34',
            name='Full time student',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],],
            targets={econ_inactive: DENOMINATOR, },
        )
        female_student = OBSColumn(
            id='ECO35',
            name='Female full time student',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],],
            targets={student: DENOMINATOR, female_econ_inactive: DENOMINATOR, },
        )
        male_student = OBSColumn(
            id='ECO36',
            name='Male full time student',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],],
            targets={student: DENOMINATOR, male_econ_inactive: DENOMINATOR, },
        )
        houseworker = OBSColumn(
            id='ECO37',
            name='Dedicated to housework',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],],
            targets={econ_inactive: DENOMINATOR, },
        )
        female_houseworker = OBSColumn(
            id='ECO38',
            name='Female dedicated to housework',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],],
            targets={houseworker: DENOMINATOR, female_econ_inactive: DENOMINATOR, },
        )
        male_houseworker = OBSColumn(
            id='ECO39',
            name='Male dedicated to housework',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],],
            targets={houseworker: DENOMINATOR, male_econ_inactive: DENOMINATOR, },
        )
        econ_inactive_phys_mental_limitation = OBSColumn(
            id='ECO40',
            name='Economically inactive with a physical or mental limitation impeding work',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],],
            targets={econ_inactive: DENOMINATOR, },
        )
        female_econ_inactive_phys_mental_limitation = OBSColumn(
            id='ECO41',
            name='Female economically inactive with a physical or mental limitation impeding work',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],],
            targets={econ_inactive_phys_mental_limitation: DENOMINATOR,
                     female_econ_inactive: DENOMINATOR, },
        )
        male_econ_inactive_phys_mental_limitation = OBSColumn(
            id='ECO42',
            name='Male economically inactive with a physical or mental limitation impeding work',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],],
            targets={econ_inactive_phys_mental_limitation: DENOMINATOR,
                     male_econ_inactive: DENOMINATOR, },
        )
        econ_inactive_some_other_reason = OBSColumn(
            id='ECO43',
            name='Economically inactive with some other reason that impedes work',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],],
            targets={econ_inactive: DENOMINATOR, },
        )
        female_econ_inactive_some_other_reason = OBSColumn(
            id='ECO44',
            name='Female economically inactive with some other reason that impedes work',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'],],
            targets={econ_inactive_some_other_reason: DENOMINATOR,
                     female_econ_inactive: DENOMINATOR},
        )
        male_econ_inactive_some_other_reason = OBSColumn(
            id='ECO45',
            name='Male economically inactive with some other reason that impedes work',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['employment'], ],
            targets={econ_inactive_some_other_reason: DENOMINATOR,
                     male_econ_inactive: DENOMINATOR},
        )
        health_services = OBSColumn(
            id='SALUD1',
            name='Population entitled to health services',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['health'], ],
            targets={pop: DENOMINATOR, },
        )
        no_health_services = OBSColumn(
            id='SALUD2',
            name='Population without access to health services',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['health'], ],
            targets={pop: DENOMINATOR, },
        )
        imss = OBSColumn(
            id='SALUD3',
            name='Population with IMSS',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['health'], ],
            targets={pop: DENOMINATOR, },
        )
        issste = OBSColumn(
            id='SALUD4',
            name='Population with ISSSTE or state ISSSTE',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['health'], ],
            targets={pop: DENOMINATOR, },
        )
        seguro_popular_or_medico = OBSColumn(
            id='SALUD5',
            name='Population with Seguro Popular or Seguro Medico para una Nueva Generacion',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['health'], ],
            targets={pop: DENOMINATOR, },
        )
        pemex_sedena_semar = OBSColumn(
            id='SALUD6',
            name='Population with Pemex, Sedena or Semar',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['health'], ],
            targets={pop: DENOMINATOR, },
        )
        single = OBSColumn(
            id='SCONY1',
            name='Single population 12 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['families'], ],
            targets={pop_12_more: DENOMINATOR, },
        )
        female_single = OBSColumn(
            id='SCONY2',
            name='Female single population 12 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['families'], ],
            targets={female_12_more: DENOMINATOR, single: DENOMINATOR, },
        )
        male_single = OBSColumn(
            id='SCONY3',
            name='Male single population 12 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['families'], ],
            targets={male_12_more: DENOMINATOR, single: DENOMINATOR, },
        )
        married = OBSColumn(
            id='SCONY4',
            name='Married or cohabited two years population 12 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['families'], ],
            targets={pop_12_more: DENOMINATOR, },
        )
        female_married = OBSColumn(
            id='SCONY5',
            name='Female married or cohabited two years population 12 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['families'], ],
            targets={female_12_more: DENOMINATOR, married: DENOMINATOR, },
        )
        male_married = OBSColumn(
            id='SCONY6',
            name='Male married or cohabited two years population 12 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['families'], ],
            targets={male_12_more: DENOMINATOR, married: DENOMINATOR, },
        )
        formerly_married = OBSColumn(
            id='SCONY7',
            name='Formerly married population 12 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['families'], ],
            targets={pop_12_more: DENOMINATOR, },
        )
        female_formerly_married = OBSColumn(
            id='SCONY8',
            name='Female formerly married population 12 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['families'], ],
            targets={female_12_more: DENOMINATOR, formerly_married: DENOMINATOR, },
        )
        male_formerly_married = OBSColumn(
            id='SCONY9',
            name='Male formerly married population 12 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['families'], ],
            targets={male_12_more: DENOMINATOR, formerly_married: DENOMINATOR, },
        )
        married_15_24 = OBSColumn(
            id='SCONY10',
            name='Married or cohabited two years population 15 to 24 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['families'], ],
            targets={pop_15_24: DENOMINATOR, },
        )
        female_married_15_24 = OBSColumn(
            id='SCONY11',
            name='Female married or cohabited two years population 15 to 24 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['families'], ],
            targets={female_15_24: DENOMINATOR, married_15_24: DENOMINATOR, },
        )
        male_married_15_24 = OBSColumn(
            id='SCONY12',
            name='Male married or cohabited two years population 15 to 24 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['families'], ],
            targets={male_15_24: DENOMINATOR, married_15_24: DENOMINATOR, },
        )
        catholic = OBSColumn(
            id='RELIG1',
            name='Catholic population',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['religion'], ],
            targets={pop: DENOMINATOR, },
        )
        protestant = OBSColumn(
            id='RELIG2',
            name='Protestant and evangelical population',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['religion'], ],
            targets={pop: DENOMINATOR, },
        )
        other_religion = OBSColumn(
            id='RELIG3',
            name='Population with other religion',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['religion'], ],
            targets={pop: DENOMINATOR, },
        )
        no_religion = OBSColumn(
            id='RELIG4',
            name='Population with no religion',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_people, subsections['religion'], ],
            targets={pop: DENOMINATOR, },
        )
        households = OBSColumn(
            id='HOGAR1',
            name='Households',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={},
        )
        ind_households = OBSColumn(
            id='INDI19',
            name='Indigenous census households',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['race_ethnicity'], ],
            targets={households: DENOMINATOR},
        )
        female_households = OBSColumn(
            id='HOGAR2',
            name='Female headed households',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={households: DENOMINATOR, },
        )
        male_households = OBSColumn(
            id='HOGAR3',
            name='Male headed households',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={households: DENOMINATOR, },
        )
        pop_in_households = OBSColumn(
            id='HOGAR4',
            name='Population in households',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={pop: DENOMINATOR, },
        )
        pop_in_female_households = OBSColumn(
            id='HOGAR5',
            name='Population in female headed households',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={pop_in_households: DENOMINATOR, },
        )
        pop_in_male_households = OBSColumn(
            id='HOGAR6',
            name='Population in male headed households',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={pop_in_households: DENOMINATOR, },
        )
        household_under_30 = OBSColumn(
            id='HOGAR7',
            name='Household with head under 30 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={households: DENOMINATOR, },
        )
        female_household_under_30 = OBSColumn(
            id='HOGAR8',
            name='Household with female head under 30 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={household_under_30: DENOMINATOR, female_households: DENOMINATOR, },
        )
        male_household_under_30 = OBSColumn(
            id='HOGAR9',
            name='Household with male head under 30 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={household_under_30: DENOMINATOR, male_households: DENOMINATOR, },
        )
        pop_in_households_under_30 = OBSColumn(
            id='HOGAR10',
            name='Population in households with head under 30 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={pop_in_households: DENOMINATOR, },
        )
        pop_in_female_households_under_30 = OBSColumn(
            id='HOGAR11',
            name='Population in households with female head under 30 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={pop_in_households_under_30: DENOMINATOR,
                     pop_in_female_households: DENOMINATOR,},
        )
        pop_in_male_households_under_30 = OBSColumn(
            id='HOGAR12',
            name='Population in households with male head under 30 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={pop_in_households_under_30: DENOMINATOR,
                     pop_in_male_households: DENOMINATOR,},
        )
        household_30_59 = OBSColumn(
            id='HOGAR13',
            name='Household with head between 30 and 59 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={households: DENOMINATOR, }
        )
        female_household_30_59 = OBSColumn(
            id='HOGAR14',
            name='Household with female head between 30 and 59 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={household_30_59: DENOMINATOR, female_households: DENOMINATOR, }
        )
        male_household_30_59 = OBSColumn(
            id='HOGAR15',
            name='Household with male head between 30 and 59 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={household_30_59: DENOMINATOR, male_households: DENOMINATOR, }
        )
        pop_in_households_30_59 = OBSColumn(
            id='HOGAR16',
            name='Population in households with head between 30 and 59 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={pop_in_households: DENOMINATOR, }
        )
        pop_in_female_households_30_59 = OBSColumn(
            id='HOGAR17',
            name='Population in households with female head between 30 and 59 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={pop_in_households_30_59: DENOMINATOR,
                     pop_in_female_households: DENOMINATOR, }
        )
        pop_in_male_households_30_59 = OBSColumn(
            id='HOGAR18',
            name='Population in households with male head between 30 and 59 years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={pop_in_households_30_59: DENOMINATOR,
                     pop_in_male_households: DENOMINATOR, }
        )
        households_60_more = OBSColumn(
            id='HOGAR19',
            name='Household with head 60 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={households: DENOMINATOR, }
        )
        female_households_60_more = OBSColumn(
            id='HOGAR20',
            name='Household with female head 60 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={female_households: DENOMINATOR,
                     households_60_more: DENOMINATOR, }
        )
        male_households_60_more = OBSColumn(
            id='HOGAR21',
            name='Household with male head 60 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={male_households: DENOMINATOR,
                     households_60_more: DENOMINATOR, }
        )
        pop_in_households_60_more = OBSColumn(
            id='HOGAR22',
            name='Population in households with head 60 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={pop_in_households: DENOMINATOR, }
        )
        pop_in_female_households_60_more = OBSColumn(
            id='HOGAR23',
            name='Population in households with female head 60 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={pop_in_female_households: DENOMINATOR,
                     pop_in_households_60_more: DENOMINATOR, }
        )
        pop_in_male_households_60_more = OBSColumn(
            id='HOGAR24',
            name='Population in households with male head 60 or more years old',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={pop_in_male_households: DENOMINATOR,
                     pop_in_households_60_more: DENOMINATOR, }
        )
        nuclear_households = OBSColumn(
            id='HOGAR25',
            name='Nuclear households (head with children under 18 years of age)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={households: DENOMINATOR, },
        )
        pop_in_nuclear_households = OBSColumn(
            id='HOGAR26',
            name='Population in nuclear households (head with children under 18 years of age)',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_household, subsections['families'], ],
            targets={pop_in_households: DENOMINATOR, },
        )
        dwellings = OBSColumn(
            id='VIV0',
            name='Dwellings',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={},
        )
        occupied_dwellings = OBSColumn(
            id='VIV1',
            name='Occupied dwellings',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={dwellings: DENOMINATOR, },
        )
        occupied_private_dwellings = OBSColumn(
            id='VIV2',
            name='Occupied private dwellings',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_dwellings: DENOMINATOR, },
        )
        pop_in_occupied_private_dwellings = OBSColumn(
            id='VIV3',
            name='Population in occupied private dwellings',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={pop: DENOMINATOR, },
        )
        earthen_floor = OBSColumn(
            id='VIV6',
            name='Private occupied dwellings with earthen floor',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        one_bedroom = OBSColumn(
            id='VIV7',
            name='Private occupied dwellings with one bedroom',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        two_bedrooms_more = OBSColumn(
            id='VIV8',
            name='Private occupied dwellings with two bedrooms or more',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        dwelling_2_5_more_people_per_bedroom = OBSColumn(
            id='VIV9',
            name='Private occupied dwellings with 2.5 people per bedroom or more',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        one_room_dwelling = OBSColumn(
            id='VIV10',
            name='One room private occupied dwellings',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        two_room_dwelling = OBSColumn(
            id='VIV11',
            name='Two room private occupied dwellings',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        three_or_more_room_dwelling = OBSColumn(
            id='VIV12',
            name='Three or more room private occupied dwellings',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        dwelling_3_more_people_per_bedroom = OBSColumn(
            id='VIV13',
            name='Private occupied dwellings with 3 people per bedroom or more',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        electric_light_dwelling = OBSColumn(
            id='VIV14',
            name='Private occupied dwellings with electric light',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        no_electric_light_dwelling = OBSColumn(
            id='VIV15',
            name='Private occupied dwellings without electric light',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        running_water = OBSColumn(
            id='VIV16',
            name='Private occupied dwellings with running water',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        no_running_water = OBSColumn(
            id='VIV17',
            name='Private occupied dwellings without running water',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        pop_with_running_water = OBSColumn(
            id='VIV18',
            name='Population in private occupied dwellings with running water',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={pop: DENOMINATOR, },
        )
        bathroom_toilet = OBSColumn(
            id='VIV19',
            name='Private occupied dwellings with a bathroom or toilet',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        no_bathroom_toilet = OBSColumn(
            id='VIV20',
            name='Private occupied dwellings without a bathroom or toilet',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        pop_with_bathroom_toilet = OBSColumn(
            id='VIV21',
            name='Population in private occupied dwellings with a bathroom or toilet',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={pop: DENOMINATOR, },
        )
        with_drainage = OBSColumn(
            id='VIV22',
            name='Private occupied dwellings with drainage',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        without_drainage = OBSColumn(
            id='VIV23',
            name='Private occupied dwellings without drainage',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        light_running_water_drainage = OBSColumn(
            id='VIV24',
            name='Private occupied dwellings with electric light, running water and drainage',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        no_light_running_water_drainage = OBSColumn(
            id='VIV25',
            name='Private occupied dwellings with neither electric light, running water nor drainage',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        with_fridge = OBSColumn(
            id='VIV26',
            name='Private occupied dwellings with a refrigator',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        with_washer = OBSColumn(
            id='VIV27',
            name='Private occupied dwellings with a washer',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        with_car_van = OBSColumn(
            id='VIV28',
            name='Private occupied dwellings with a car or van',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], subsections['transportation']],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        no_fridge_washer = OBSColumn(
            id='VIV29',
            name='Private occupied dwellings with neither refigerator nor washer',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        no_fridge_washer_car_van = OBSColumn(
            id='VIV30',
            name='Private occupied dwellings with neither refigerator, washer, nor car or van',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], subsections['transportation']],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        with_radio = OBSColumn(
            id='VIV31',
            name='Private occupied dwellings with a radio',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        with_television = OBSColumn(
            id='VIV32',
            name='Private occupied dwellings with a television',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        with_computer = OBSColumn(
            id='VIV33',
            name='Private occupied dwellings with a computer',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        with_landline = OBSColumn(
            id='VIV34',
            name='Private occupied dwellings with a landline',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        with_cellphone = OBSColumn(
            id='VIV35',
            name='Private occupied dwellings with a cellphone',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        with_internet = OBSColumn(
            id='VIV36',
            name='Private occupied dwellings with Internet access',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        with_no_television_radio = OBSColumn(
            id='VIV37',
            name='Private occupied dwellings with neither television nor radio',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        with_no_landline_cellphone = OBSColumn(
            id='VIV38',
            name='Private occupied dwellings with neither a landline nor a cellphone',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        with_no_computer_internet = OBSColumn(
            id='VIV39',
            name='Private occupied dwellings with neither a computer nor Internet access',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        with_no_tic = OBSColumn(
            id='VIV40',
            name='Private occupied dwellings without any information or communication technology',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        no_phys_property = OBSColumn(
            id='VIV41',
            name='Private occupied dwellings with no physical property',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[mexico, unit_housing, subsections['housing'], ],
            targets={occupied_private_dwellings: DENOMINATOR, },
        )
        prod_dep_ratio = OBSColumn(
            id='POB27_R',
            name='Ratio between productive and dependent populations',
            type='Numeric',
            weight=3,
            aggregate='ratio',
            tags=[mexico, unit_ratio, ],
            targets={},
        )
        prod_child_dep_ratio = OBSColumn(
            id='POB28_R',
            name='Ratio between productive and child dependent populations',
            type='Numeric',
            weight=3,
            aggregate='ratio',
            tags=[mexico, unit_ratio, ],
            targets={},
        )
        prod_old_age_ratio = OBSColumn(
            id='POB29_R',
            name='Ratio between productive and old-age dependent populations',
            type='Numeric',
            weight=3,
            aggregate='ratio',
            tags=[mexico, unit_ratio, ],
            targets={},
        )
        median_age = OBSColumn(
            id='POB30_R',
            name='Median age',
            type='Numeric',
            weight=8,
            aggregate='ratio',
            tags=[mexico, unit_years, subsections['age_gender']],
            targets={},
        )
        birthrate = OBSColumn(
            id='FEC1_R',
            name='Birthrate',
            type='Numeric',
            weight=3,
            aggregate='ratio',
            tags=[mexico, unit_ratio, subsections['families']],
            targets={},
        )
        birthrate_15_49 = OBSColumn(
            id='FEC2_R',
            name='Birthrate to women age 15 to 49',
            type='Numeric',
            weight=3,
            aggregate='ratio',
            tags=[mexico, unit_ratio, subsections['families']],
            targets={},
        )
        percent_women_15_19_with_child = OBSColumn(
            id='FEC3_R',
            name='Percentage of women age 15 to 19 with a child',
            type='Numeric',
            weight=3,
            aggregate='ratio',
            tags=[mexico, unit_ratio, subsections['families'], ],
            targets={},
        )
        male_female_ratio_in_us_june_05 = OBSColumn(
            id='MIG16_R',
            name='Male-female ratio in population 5 years and over who resided in the '
                 'United States in June 2005',
            type='Numeric',
            weight=3,
            aggregate='ratio',
            tags=[mexico, unit_ratio, subsections['age_gender'],
                  subsections['migration'], ],
            targets={},
        )
        average_education_level = OBSColumn(
            id='EDU49_R',
            name='Average education level',
            type='Numeric',
            weight=3,
            aggregate=None,
            tags=[mexico, unit_education, subsections['education'], ],
            targets={},
        )
        female_average_education_level = OBSColumn(
            id='EDU50_R',
            name='Average female education level',
            type='Numeric',
            weight=3,
            aggregate=None,
            tags=[mexico, unit_education, subsections['education'], ],
            targets={},
        )
        male_average_education = OBSColumn(
            id='EDU51_R',
            name='Average male education level',
            type='Numeric',
            weight=3,
            aggregate=None,
            tags=[mexico, unit_education, subsections['education'], ],
            targets={},
        )
        people_per_dwelling = OBSColumn(
            id='VIV4_R',
            name='Average number of people per private occupied dwellings',
            type='Numeric',
            weight=3,
            aggregate=None,
            tags=[mexico, unit_ratio, subsections['housing'], ],
            targets={},
        )
        people_per_room = OBSColumn(
            id='VIV5_R',
            name='Average number of people per room in private occupied dwellings',
            type='Numeric',
            weight=3,
            aggregate=None,
            tags=[mexico, unit_ratio, subsections['housing'], ],
            targets={},
        )

        allColumns = OrderedDict([
            ('pop', pop),
            ('pop_0_2', pop_0_2),
            ('pop_0_4', pop_0_4),
            ('pop_3_5', pop_3_5),
            ('pop_6_11', pop_6_11),
            ('pop_8_14', pop_8_14),
            ('pop_12_14', pop_12_14),
            ('pop_0_14', pop_0_14),
            ('pop_15_17', pop_15_17),
            ('pop_15_24', pop_15_24),
            ('pop_15_29', pop_15_29),
            ('pop_15_64', pop_15_64),
            ('pop_18_24', pop_18_24),
            ('pop_30_49', pop_30_49),
            ('pop_50_59', pop_50_59),
            ('pop_60_64', pop_60_64),
            ('pop_3_more', pop_3_more),
            ('pop_5_more', pop_5_more),
            ('pop_12_more', pop_12_more),
            ('pop_15_more', pop_15_more),
            ('pop_18_more', pop_18_more),
            ('pop_25_more', pop_25_more),
            ('pop_60_more', pop_60_more),
            ('pop_65_more', pop_65_more),
            ('pop_70_more', pop_70_more),
            ('female', female),
            ('female_0_2', female_0_2),
            ('female_0_4', female_0_4),
            ('female_3_5', female_3_5),
            ('female_6_11', female_6_11),
            ('female_8_14', female_8_14),
            ('female_12_14', female_12_14),
            ('female_0_14', female_0_14),
            ('female_15_17', female_15_17),
            ('female_15_24', female_15_24),
            ('female_15_29', female_15_29),
            ('female_15_49', female_15_49),
            ('female_15_64', female_15_64),
            ('female_18_24', female_18_24),
            ('female_30_49', female_30_49),
            ('female_50_59', female_50_59),
            ('female_60_64', female_60_64),
            ('female_3_more', female_3_more),
            ('female_5_more', female_5_more),
            ('female_12_more', female_12_more),
            ('female_15_more', female_15_more),
            ('female_18_more', female_18_more),
            ('female_25_more', female_25_more),
            ('female_60_more', female_60_more),
            ('female_65_more', female_65_more),
            ('female_70_more', female_70_more),
            ('male', male),
            ('male_0_2', male_0_2),
            ('male_0_4', male_0_4),
            ('male_3_5', male_3_5),
            ('male_6_11', male_6_11),
            ('male_8_14', male_8_14),
            ('male_12_14', male_12_14),
            ('male_0_14', male_0_14),
            ('male_15_17', male_15_17),
            ('male_15_24', male_15_24),
            ('male_15_29', male_15_29),
            ('male_15_64', male_15_64),
            ('male_18_24', male_18_24),
            ('male_30_49', male_30_49),
            ('male_50_59', male_50_59),
            ('male_60_64', male_60_64),
            ('male_3_more', male_3_more),
            ('male_5_more', male_5_more),
            ('male_12_more', male_12_more),
            ('male_15_more', male_15_more),
            ('male_18_more', male_18_more),
            ('male_25_more', male_25_more),
            ('male_60_more', male_60_more),
            ('male_65_more', male_65_more),
            ('male_70_more', male_70_more),
            ('born_in_state', born_in_state),
            ('female_born_in_state', female_born_in_state),
            ('male_born_in_state', male_born_in_state),
            ('born_in_another_state', born_in_another_state),
            ('female_born_in_another_state', female_born_in_another_state),
            ('male_born_in_another_state', male_born_in_another_state),
            ('born_in_another_country', born_in_another_country),
            ('in_state_june_05', in_state_june_05),
            ('female_in_state_june_05', female_in_state_june_05),
            ('male_in_state_june_05', male_in_state_june_05),
            ('another_state_june_05', another_state_june_05),
            ('female_another_state_june_05', female_another_state_june_05),
            ('male_another_state_june_05', male_another_state_june_05),
            ('another_country_june_05', another_country_june_05),
            ('united_states_june_05', united_states_june_05),
            ('speak_ind_language', speak_ind_language),
            ('female_ind_language', female_ind_language),
            ('male_ind_language', male_ind_language),
            ('ind_language_no_spanish', ind_language_no_spanish),
            ('female_ind_language_no_spanish', female_ind_language_no_spanish),
            ('male_ind_language_no_spanish', male_ind_language_no_spanish),
            ('ind_language_and_spanish', ind_language_and_spanish),
            ('female_ind_language_and_spanish', female_ind_language_and_spanish),
            ('male_ind_language_and_spanish', male_ind_language_and_spanish),
            ('speak_ind_language_age_5_more', speak_ind_language_age_5_more),
            ('female_speak_ind_language_age_5_more', female_speak_ind_language_age_5_more),
            ('male_speak_ind_language_age_5_more', male_speak_ind_language_age_5_more),
            ('speak_ind_language_no_spanish_age_5_more', speak_ind_language_no_spanish_age_5_more),
            ('female_speak_ind_language_no_spanish_age_5_more',
             female_speak_ind_language_no_spanish_age_5_more),
            ('male_speak_ind_language_no_spanish_age_5_more',
             male_speak_ind_language_no_spanish_age_5_more),
            ('speak_ind_language_and_spanish_age_5_more',
             speak_ind_language_and_spanish_age_5_more),
            ('female_speak_ind_language_and_spanish_age_5_more',
             female_speak_ind_language_and_spanish_age_5_more),
            ('male_speak_ind_language_and_spanish_age_5_more',
             male_speak_ind_language_and_spanish_age_5_more),
            ('pop_ind_households', pop_ind_households),
            ('disability', disability),
            ('female_disability', female_disability),
            ('male_disability', male_disability),
            ('disability_age_14_less', disability_age_14_less),
            ('disability_age_15_59', disability_age_15_59),
            ('disability_age_60_more', disability_age_60_more),
            ('disability_movement', disability_movement),
            ('disability_sight', disability_sight),
            ('disability_speak', disability_speak),
            ('hearing_disability', hearing_disability),
            ('disability_dress_bathe_eat', disability_dress_bathe_eat),
            ('learning_disability', learning_disability),
            ('mental_limitation', mental_limitation),
            ('limited_activity_insurance', limited_activity_insurance),
            ('in_school_3_5', in_school_3_5),
            ('female_in_school_3_5', female_in_school_3_5),
            ('male_in_school_3_5', male_in_school_3_5),
            ('not_in_school_3_5', not_in_school_3_5),
            ('female_not_in_school_3_5', female_not_in_school_3_5),
            ('male_not_in_school_3_5', male_not_in_school_3_5),
            ('in_school_6_11', in_school_6_11),
            ('female_in_school_6_11', female_in_school_6_11),
            ('male_in_school_6_11', male_in_school_6_11),
            ('not_in_school_6_11', not_in_school_6_11),
            ('female_not_in_school_6_11', female_not_in_school_6_11),
            ('male_not_in_school_6_11', male_not_in_school_6_11),
            ('in_school_12_14', in_school_12_14),
            ('female_in_school_12_14', female_in_school_12_14),
            ('male_in_school_12_14', male_in_school_12_14),
            ('not_in_school_12_14', not_in_school_12_14),
            ('female_not_in_school_12_14', female_not_in_school_12_14),
            ('male_not_in_school_12_14', male_not_in_school_12_14),
            ('literate_8_14', literate_8_14),
            ('female_literate_8_14', female_literate_8_14),
            ('male_literate_8_14', male_literate_8_14),
            ('illiterate_8_14', illiterate_8_14),
            ('female_illiterate_8_14', female_illiterate_8_14),
            ('male_illiterate_8_14', male_illiterate_8_14),
            ('literate_15_more', literate_15_more),
            ('female_literate_15_more', female_literate_15_more),
            ('male_literate_15_more', male_literate_15_more),
            ('illiterate_15_more', illiterate_15_more),
            ('female_illiterate_15_more', female_illiterate_15_more),
            ('male_illiterate_15_more', male_illiterate_15_more),
            ('no_school_15_more', no_school_15_more),
            ('female_no_school_15_more', female_no_school_15_more),
            ('male_no_school_15_more', male_no_school_15_more),
            ('no_basic_education_15_more', no_basic_education_15_more),
            ('female_no_basic_education_15_more', female_no_basic_education_15_more),
            ('male_no_basic_education_15_more', male_no_basic_education_15_more),
            ('basic_education_15_more', basic_education_15_more),
            ('female_basic_education_15_more', female_basic_education_15_more),
            ('male_basic_education_15_more', male_basic_education_15_more),
            ('some_high_school_15_more', some_high_school_15_more),
            ('female_some_high_school_15_more', female_some_high_school_15_more),
            ('male_some_high_school_15_more', male_some_high_school_15_more),
            ('high_school_degree', high_school_degree),
            ('female_high_school_degree', female_high_school_degree),
            ('male_high_school_degree', male_high_school_degree),
            ('university_degree', university_degree),
            ('female_university_degree', female_university_degree),
            ('male_university_degree', male_university_degree),
            ('econ_active', econ_active),
            ('female_econ_active', female_econ_active),
            ('male_econ_active', male_econ_active),
            ('employed', employed),
            ('female_employed', female_employed),
            ('male_employed', male_employed),
            ('employed_without_school', employed_without_school),
            ('female_employed_without_school', female_employed_without_school),
            ('male_employed_without_school', male_employed_without_school),
            ('employed_primary_education', employed_primary_education),
            ('female_employed_primary_education', female_employed_primary_education),
            ('male_employed_primary_education', male_employed_primary_education),
            ('employed_incomplete_secondary_education', employed_incomplete_secondary_education),
            ('female_employed_incomplete_secondary_education',
             female_employed_incomplete_secondary_education),
            ('male_employed_incomplete_secondary_education',
             male_employed_incomplete_secondary_education),
            ('employed_basic_education', employed_basic_education),
            ('female_employed_basic_education', female_employed_basic_education),
            ('male_employed_basic_education', male_employed_basic_education),
            ('employed_high_school_degree', employed_high_school_degree),
            ('female_employed_high_school_degree', female_employed_high_school_degree),
            ('male_employed_high_school_degree', male_employed_high_school_degree),
            ('employed_university_degree', employed_university_degree),
            ('female_employed_university_degree', female_employed_university_degree),
            ('male_employed_university_degree', male_employed_university_degree),
            ('unemployed', unemployed),
            ('female_unemployed', female_unemployed),
            ('male_unemployed', male_unemployed),
            ('econ_inactive', econ_inactive),
            ('female_econ_inactive', female_econ_inactive),
            ('male_econ_inactive', male_econ_inactive),
            ('retiree', retiree),
            ('female_retiree', female_retiree),
            ('male_retiree', male_retiree),
            ('student', student),
            ('female_student', female_student),
            ('male_student', male_student),
            ('houseworker', houseworker),
            ('female_houseworker', female_houseworker),
            ('male_houseworker', male_houseworker),
            ('econ_inactive_phys_mental_limitation', econ_inactive_phys_mental_limitation),
            ('female_econ_inactive_phys_mental_limitation',
             female_econ_inactive_phys_mental_limitation),
            ('male_econ_inactive_phys_mental_limitation',
             male_econ_inactive_phys_mental_limitation),
            ('econ_inactive_some_other_reason', econ_inactive_some_other_reason),
            ('female_econ_inactive_some_other_reason', female_econ_inactive_some_other_reason),
            ('male_econ_inactive_some_other_reason', male_econ_inactive_some_other_reason),
            ('health_services', health_services),
            ('no_health_services', no_health_services),
            ('imss', imss),
            ('issste', issste),
            ('seguro_popular_or_medico', seguro_popular_or_medico),
            ('pemex_sedena_semar', pemex_sedena_semar),
            ('single', single),
            ('female_single', female_single),
            ('male_single', male_single),
            ('married', married),
            ('female_married', female_married),
            ('male_married', male_married),
            ('formerly_married', formerly_married),
            ('female_formerly_married', female_formerly_married),
            ('male_formerly_married', male_formerly_married),
            ('married_15_24', married_15_24),
            ('female_married_15_24', female_married_15_24),
            ('male_married_15_24', male_married_15_24),
            ('catholic', catholic),
            ('protestant', protestant),
            ('other_religion', other_religion),
            ('no_religion', no_religion),
            ('households', households),
            ('ind_households', ind_households),
            ('female_households', female_households),
            ('male_households', male_households),
            ('pop_in_households', pop_in_households),
            ('pop_in_female_households', pop_in_female_households),
            ('pop_in_male_households', pop_in_male_households),
            ('household_under_30', household_under_30),
            ('female_household_under_30', female_household_under_30),
            ('male_household_under_30', male_household_under_30),
            ('pop_in_households_under_30', pop_in_households_under_30),
            ('pop_in_female_households_under_30', pop_in_female_households_under_30),
            ('pop_in_male_households_under_30', pop_in_male_households_under_30),
            ('household_30_59', household_30_59),
            ('female_household_30_59', female_household_30_59),
            ('male_household_30_59', male_household_30_59),
            ('pop_in_households_30_59', pop_in_households_30_59),
            ('pop_in_female_households_30_59', pop_in_female_households_30_59),
            ('pop_in_male_households_30_59', pop_in_male_households_30_59),
            ('households_60_more', households_60_more),
            ('female_households_60_more', female_households_60_more),
            ('male_households_60_more', male_households_60_more),
            ('pop_in_households_60_more', pop_in_households_60_more),
            ('pop_in_female_households_60_more', pop_in_female_households_60_more),
            ('pop_in_male_households_60_more', pop_in_male_households_60_more),
            ('nuclear_households', nuclear_households),
            ('pop_in_nuclear_households', pop_in_nuclear_households),
            ('dwellings', dwellings),
            ('occupied_dwellings', occupied_dwellings),
            ('occupied_private_dwellings', occupied_private_dwellings),
            ('pop_in_occupied_private_dwellings', pop_in_occupied_private_dwellings),
            ('earthen_floor', earthen_floor),
            ('one_bedroom', one_bedroom),
            ('two_bedrooms_more', two_bedrooms_more),
            ('dwelling_2_5_more_people_per_bedroom', dwelling_2_5_more_people_per_bedroom),
            ('one_room_dwelling', one_room_dwelling),
            ('two_room_dwelling', two_room_dwelling),
            ('three_or_more_room_dwelling', three_or_more_room_dwelling),
            ('dwelling_3_more_people_per_bedroom', dwelling_3_more_people_per_bedroom),
            ('electric_light_dwelling', electric_light_dwelling),
            ('no_electric_light_dwelling', no_electric_light_dwelling),
            ('running_water', running_water),
            ('no_running_water', no_running_water),
            ('pop_with_running_water', pop_with_running_water),
            ('bathroom_toilet', bathroom_toilet),
            ('no_bathroom_toilet', no_bathroom_toilet),
            ('pop_with_bathroom_toilet', pop_with_bathroom_toilet),
            ('with_drainage', with_drainage),
            ('without_drainage', without_drainage),
            ('light_running_water_drainage', light_running_water_drainage),
            ('no_light_running_water_drainage', no_light_running_water_drainage),
            ('with_fridge', with_fridge),
            ('with_washer', with_washer),
            ('with_car_van', with_car_van),
            ('no_fridge_washer', no_fridge_washer),
            ('no_fridge_washer_car_van', no_fridge_washer_car_van),
            ('with_radio', with_radio),
            ('with_television', with_television),
            ('with_computer', with_computer),
            ('with_landline', with_landline),
            ('with_cellphone', with_cellphone),
            ('with_internet', with_internet),
            ('with_no_television_radio', with_no_television_radio),
            ('with_no_landline_cellphone', with_no_landline_cellphone),
            ('with_no_computer_internet', with_no_computer_internet),
            ('with_no_tic', with_no_tic),
            ('no_phys_property', no_phys_property),
            ('prod_dep_ratio', prod_dep_ratio),
            ('prod_child_dep_ratio', prod_child_dep_ratio),
            ('prod_old_age_ratio', prod_old_age_ratio),
            ('median_age', median_age),
            ('birthrate', birthrate),
            ('birthrate_15_49', birthrate_15_49),
            ('percent_women_15_19_with_child', percent_women_15_19_with_child),
            ('male_female_ratio_in_us_june_05', male_female_ratio_in_us_june_05),
            ('average_education_level', average_education_level),
            ('female_average_education_level', female_average_education_level),
            ('male_average_education', male_average_education),
            ('people_per_dwelling', people_per_dwelling),
            ('people_per_room', people_per_room),
        ])

        columnsFilter = ColumnsDeclarations(os.path.join(os.path.dirname(__file__), 'inegi_columns.json'))
        parameters = '{{"resolution":"{resolution}","table":"{table}"}}'.format(
                        resolution=self.resolution, table=self.table)
        columns = columnsFilter.filter_columns(allColumns, parameters)

<<<<<<< HEAD
        for _, col in columns.iteritems():
=======
        for _, col in columns.items():
>>>>>>> 539e828f0f4f6d421716c8d37a168d78ed10dc65
            col.tags.append(source)
            col.tags.append(license)

        return columns
