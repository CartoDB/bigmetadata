#!/usr/bin/env python

'''
Bigmetadata tasks

tasks to download and create metadata
'''

from collections import OrderedDict
from sqlalchemy import Column, Numeric, Text
from luigi import Parameter, BooleanParameter, Task, WrapperTask, LocalTarget
from psycopg2 import ProgrammingError

from tasks.util import (LoadPostgresFromURL, classpath, shell, grouper,
                        CartoDBTarget, get_logger, underscore_slugify, TableTask,
                        ColumnTarget, ColumnsTask, TagsTask, MetaWrapper)
from tasks.us.census.tiger import load_sumlevels, SumLevel
from tasks.us.census.tiger import (SUMLEVELS, GeoidColumns, SUMLEVELS_BY_SLUG)
from tasks.us.census.segments import SegmentTags

from tasks.meta import (OBSColumn, OBSTag, OBSColumnTable, current_session)
from tasks.tags import SectionTags, SubsectionTags, UnitTags, LicenseTags

from time import time
LOGGER = get_logger(__name__)


class ACSTags(TagsTask):

    def version(self):
        return 3

    def tags(self):
        return [
            OBSTag(id='acs',
                   name='US American Community Survey',
                   type='source',
                   description='`The United States American Community Survey <https://www.census.gov/programs-surveys/acs/>`_'),
            OBSTag(id='segments',
                   name='US Population Segments',
                   type='subsection',
                   description='Segmentation of the United States population'),
        ]


class Columns(ColumnsTask):

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'censustags': ACSTags(),
            'segmenttags': SegmentTags(),
            'unittags': UnitTags(),
            'license': LicenseTags(),
        }

    def version(self):
        return 18

    def columns(self):
        input_ = self.input()
        subsections = input_['subsections']
        unit_people = input_['unittags']['people']
        unit_housing = input_['unittags']['housing_units']
        unit_households = input_['unittags']['households']
        unit_money = input_['unittags']['money']
        unit_years = input_['unittags']['years']
        unit_ratio = input_['unittags']['ratio']
        #segmenttags = input_['segmenttags']
        #tag_middle_aged_men = segmenttags['middle_aged_men']
        #tag_families_with_young_children = segmenttags['families_with_young_children']
        housing_units = OBSColumn(
            id='B25001001',
            type='Numeric',
            name='Housing Units',
            description='A count of housing units in each geography.  A housing '
            'unit is a house, an apartment, a mobile home or trailer, a group of '
            'rooms, or a single room occupied as separate living quarters, or if '
            'vacant, intended for occupancy as separate living quarters.',
            weight=8,
            aggregate='sum',
            tags=[subsections['housing'], unit_housing])
        occupied_housing_units = OBSColumn(
            id='B25003001',
            type='Numeric',
            name='Occupied housing units',
            description='A housing unit is classified as occupied if it is the usual place of residence of the person or group of people living in it at the time of enumeration.',
            weight=5,
            aggregate='sum',
            targets={housing_units: 'denominator'},
            tags=[subsections['housing'], unit_housing])
        housing_units_renter_occupied = OBSColumn(
            id='B25003003',
            type='Numeric',
            name='Renter occupied housing units',
            description='All occupied units which are not owner occupied, whether they are rented for cash rent or occupied without payment of cash rent, are classified as renter-occupied.',
            weight=5,
            aggregate='sum',
            targets={occupied_housing_units: 'denominator'},
            tags=[subsections['housing'], unit_housing])
        rent_burden_not_computed = OBSColumn(
            id='B25070011',
            type='Numeric',
            name='Housing units without rent burden computed',
            description= 'Units for which no rent is paid and units occupied by '
            'households that reported no income or a net loss comprise this category',
            weight=5,
            aggregate='sum',
            targets={housing_units_renter_occupied:'denominator'},
            tags=[subsections['housing'],subsections['income'],unit_housing])
        rent_over_50_percent = OBSColumn(
            id='B25070010',
            type='Numeric',
            name='Housing units spending over 50% income on rent',
            description='Gross rent over 50 percent of household income. '
            'Computed ratio of monthly gross rent to monthly household income '
            '(total household income divided by 12). '
            'The ratio is computed separately for each unit and is rounded to the nearest tenth. '
            'Units for which no rent is paid and units occupied by households that report no income or a net '
            'loss comprise the category, "Not computed". '
            'Gross rent as a percentage of household income provides information on the monthly housing cost expenses for renters. ',
            weight=5,
            aggregate='sum',
            targets={housing_units_renter_occupied:'denominator'},
            tags=[subsections['housing'],subsections['income'],unit_housing])
        rent_40_to_50_percent = OBSColumn(
            id='B25070009',
            type='Numeric',
            name='Housing units spending 40 to 49.9% income on rent',
            description='Gross rent from 40.0 to 49.9 percent of household income. '
            'Computed ratio of monthly gross rent to monthly household income '
            '(total household income divided by 12). '
            'The ratio is computed separately for each unit and is rounded to the nearest tenth. '
            'Units for which no rent is paid and units occupied by households that report no income or a net '
            'loss comprise the category, "Not computed". '
            'Gross rent as a percentage of household income provides information on the monthly housing cost expenses for renters. ',
            weight=5,
            aggregate='sum',
            targets={housing_units_renter_occupied:'denominator'},
            tags=[subsections['housing'],subsections['income'],unit_housing])
        rent_35_to_40_percent = OBSColumn(
            id='B25070008',
            type='Numeric',
            name='Housing units spending 35 to 39.9% income on rent',
            description='Gross rent from 35.0 to 39.9 percent of household income. '
            'Computed ratio of monthly gross rent to monthly household income '
            '(total household income divided by 12). '
            'The ratio is computed separately for each unit and is rounded to the nearest tenth. '
            'Units for which no rent is paid and units occupied by households that report no income or a net '
            'loss comprise the category, "Not computed". '
            'Gross rent as a percentage of household income provides information on the monthly housing cost expenses for renters. ',
            weight=5,
            aggregate='sum',
            targets={housing_units_renter_occupied:'denominator'},
            tags=[subsections['housing'],subsections['income'],unit_housing])
        rent_30_to_35_percent = OBSColumn(
            id='B25070007',
            type='Numeric',
            name='Housing units spending 30 to 34.9% income on rent',
            description='Gross rent from 30.0 to 34.9 percent of household income. '
            'Computed ratio of monthly gross rent to monthly household income '
            '(total household income divided by 12). '
            'The ratio is computed separately for each unit and is rounded to the nearest tenth. '
            'Units for which no rent is paid and units occupied by households that report no income or a net '
            'loss comprise the category, "Not computed". '
            'Gross rent as a percentage of household income provides information on the monthly housing cost expenses for renters. ',
            weight=5,
            aggregate='sum',
            targets={housing_units_renter_occupied:'denominator'},
            tags=[subsections['housing'],subsections['income'],unit_housing])
        rent_25_to_30_percent = OBSColumn(
            id='B25070006',
            type='Numeric',
            name='Housing units spending 25 to 29.9% income on rent',
            description='Gross rent from 25.0 to 29.9 percent of household income. '
            'Computed ratio of monthly gross rent to monthly household income '
            '(total household income divided by 12). '
            'The ratio is computed separately for each unit and is rounded to the nearest tenth. '
            'Units for which no rent is paid and units occupied by households that report no income or a net '
            'loss comprise the category, "Not computed". '
            'Gross rent as a percentage of household income provides information on the monthly housing cost expenses for renters. ',
            weight=5,
            aggregate='sum',
            targets={housing_units_renter_occupied:'denominator'},
            tags=[subsections['housing'],subsections['income'],unit_housing])
        rent_20_to_25_percent = OBSColumn(
            id='B25070005',
            type='Numeric',
            name='Housing units spending 20 to 24.9% income on rent',
            description='Gross rent from 20.0 to 24.9 percent of household income. '
            'Computed ratio of monthly gross rent to monthly household income '
            '(total household income divided by 12). '
            'The ratio is computed separately for each unit and is rounded to the nearest tenth. '
            'Units for which no rent is paid and units occupied by households that report no income or a net '
            'loss comprise the category, "Not computed". '
            'Gross rent as a percentage of household income provides information on the monthly housing cost expenses for renters. ',
            weight=5,
            aggregate='sum',
            targets={housing_units_renter_occupied:'denominator'},
            tags=[subsections['housing'],subsections['income'],unit_housing])
        rent_15_to_20_percent = OBSColumn(
            id='B25070004',
            type='Numeric',
            name='Housing units spending 15 to 19.9% income on rent',
            description='Gross rent from 15.0 to 19.9 percent of household income. '
            'Computed ratio of monthly gross rent to monthly household income '
            '(total household income divided by 12). '
            'The ratio is computed separately for each unit and is rounded to the nearest tenth. '
            'Units for which no rent is paid and units occupied by households that report no income or a net '
            'loss comprise the category, "Not computed". '
            'Gross rent as a percentage of household income provides information on the monthly housing cost expenses for renters. ',
            weight=5,
            aggregate='sum',
            targets={housing_units_renter_occupied:'denominator'},
            tags=[subsections['housing'],subsections['income'],unit_housing])
    	rent_10_to_15_percent = OBSColumn(
    		id='B25070003',
    		type='Numeric',
    		name='Housing units spending 10 to 14.9% income on rent',
    		description='Gross rent from 10.0 to 14.9 percent of household income. '
            'Computed ratio of monthly gross rent to monthly household income '
            '(total household income divided by 12). '
            'The ratio is computed separately for each unit and is rounded to the nearest tenth. '
            'Units for which no rent is paid and units occupied by households that report no income or a net '
            'loss comprise the category, "Not computed". '
            'Gross rent as a percentage of household income provides information on the monthly housing cost expenses for renters. ',
    		weight=5,
    		aggregate='sum',
    		targets={housing_units_renter_occupied:'denominator'},
    		tags=[subsections['housing'],subsections['income'],unit_housing])
    	rent_under_10_percent = OBSColumn(
            id='B25070002',
            type='Numeric',
            name='Housing units spending less than 10% on rent',
            description='Gross rent less than 10 percent of household income. '
            'Computed ratio of monthly gross rent to monthly household income '
            '(total household income divided by 12). '
            'The ratio is computed separately for each unit and is rounded to the nearest tenth. '
            'Units for which no rent is paid and units occupied by households that report no income or a net '
            'loss comprise the category, "Not computed". '
            'Gross rent as a percentage of household income provides information on the monthly housing cost expenses for renters. ',
            weight=5,
            aggregate='sum',
            targets={housing_units_renter_occupied:'denominator'},
            tags=[subsections['housing'],subsections['income'],unit_housing])
        households = OBSColumn(
            id='B11001001',
            type='Numeric',
            name='Households',
            description='A count of the number of households in each geography. '
            'A household consists of one or more people who live in the same '
            'dwelling and also share at meals or living accommodation, and may '
            'consist of a single family or some other grouping of people. ',
            weight=8,
            aggregate='sum',
            tags=[subsections['housing'], unit_households])
        total_pop = OBSColumn(
            id='B01003001',
            type='Numeric',
            name="Total Population",
            description='The total number of all people living in a given '
            'geographic area.  This is a very useful catch-all denominator '
            'when calculating rates.',
            aggregate='sum',
            weight=10,
            tags=[subsections['age_gender'], subsections['race_ethnicity'], unit_people]
        )
        male_pop = OBSColumn(
            id='B01001002',
            type='Numeric',
            name="Male Population",
            description="The number of people within each geography who are male.",
            aggregate='sum',
            weight=8,
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_pop = OBSColumn(
            id='B01001026',
            type='Numeric',
            name="Female Population",
            description='The number of people within each geography who are female.',
            aggregate='sum',
            weight=8,
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        median_age = OBSColumn(
            id='B01002001',
            type='Numeric',
            name="Median Age",
            description="The median age of all people in a given geographic area.",
            aggregate='median',
            weight=2,
            targets={total_pop: 'universe'},
            tags=[subsections['age_gender'], unit_years]
        )
        white_pop = OBSColumn(
            id='B03002003',
            type='Numeric',
            name="White Population",
            description="The number of people identifying as white, "
                        "non-Hispanic in each geography.",
            aggregate='sum',
            weight=7,
            targets={total_pop: 'denominator'},
            tags=[subsections['race_ethnicity'], unit_people]
        )
        black_pop = OBSColumn(
            id='B03002004',
            type='Numeric',
            name='Black or African American Population',
            description="The number of people identifying as black or African American, non-Hispanic in each geography.",
            aggregate='sum',
            weight=7,
            targets={total_pop: 'denominator'},
            tags=[subsections['race_ethnicity'], unit_people]
        )
        amerindian_pop = OBSColumn(
            id='B03002005',
            type='Numeric',
            name='American Indian and Alaska Native Population',
            description="The number of people identifying as American Indian or Alaska native in each geography.",
            aggregate='sum',
            weight=1,
            targets={total_pop: 'denominator'},
            tags=[subsections['race_ethnicity'], unit_people]
        )
        asian_pop = OBSColumn(
            id='B03002006',
            type='Numeric',
            name='Asian Population',
            description="The number of people identifying as Asian, non-Hispanic in each geography.",
            aggregate='sum',
            weight=7,
            targets={total_pop: 'denominator'},
            tags=[subsections['race_ethnicity'], unit_people]
        )
        other_race_pop = OBSColumn(
            id='B03002008',
            type='Numeric',
            name='Other Race population',
            description="The number of people identifying as another race in each geography",
            aggregate='sum',
            weight=1,
            targets={total_pop: 'denominator'},
            tags=[subsections['race_ethnicity'], unit_people]
        )
        two_or_more_races_pop = OBSColumn(
            id='B03002009',
            type='Numeric',
            name='Two or more races population',
            description="The number of people identifying as two or more races in each geography",
            aggregate='sum',
            weight=1,
            targets={total_pop: 'denominator'},
            tags=[subsections['race_ethnicity'], unit_people]
        )
        not_hispanic_pop = OBSColumn(
            id='B03002002',
            type='Numeric',
            name='Population not Hispanic',
            description="The number of people not identifying as Hispanic or Latino in each geography.",
            aggregate='sum',
            weight=1,
            targets={total_pop: 'denominator'},
            tags=[subsections['race_ethnicity'], unit_people]
        )
        hispanic_pop = OBSColumn(
            id='B03002012',
            type='Numeric',
            name='Hispanic Population',
            description="The number of people identifying as Hispanic or Latino in each geography.",
            aggregate='sum',
            weight=7,
            targets={total_pop: 'denominator'},
            tags=[subsections['race_ethnicity'], unit_people]
        )
        not_us_citizen_pop = OBSColumn(
            id='B05001006',
            type='Numeric',
            name='Not a U.S. Citizen Population',
            description=
            "The number of people within each geography who indicated that "
            "they are not U.S. citizens.",
            aggregate='sum',
            weight=3,
            targets={total_pop: 'denominator'},
            tags=[subsections['nationality'], unit_people]
        )
        workers_16_and_over = OBSColumn(
            id='B08006001',
            type='Numeric',
            name='Workers over the Age of 16',
            description="The number of people in each geography who work. "
            "Workers include those employed at private for-profit companies, the "
            "self-employed, government workers and non-profit employees.",
            aggregate='sum',
            weight=5,
            tags=[subsections['employment'], unit_people]
        )
        commuters_by_car_truck_van = OBSColumn(
            id='B08006002',
            type='Numeric',
            name='Commuters by Car, Truck, or Van',
            description='The number of workers age 16 years and over within '
            ' a geographic area who primarily traveled to work by car, truck or '
            ' van.  This is the principal mode of travel or type of conveyance, '
            ' by distance rather than time, that the worker usually used to get '
            ' from home to work.',
            weight=4,
            aggregate='sum',
            targets={workers_16_and_over: 'denominator'},
            tags=[subsections['transportation'], unit_people])
        commuters_drove_alone = OBSColumn(
            id='B08006003',
            type='Numeric',
            name='Commuters who drove alone',
            description='The number of workers age 16 years and over within '
            'a geographic area who primarily traveled by car driving alone. '
            'This is the principal mode of travel or type of conveyance, by '
            'distance rather than time, that the worker usually used to get from '
            'home to work.',
            weight=2,
            aggregate='sum',
            targets={commuters_by_car_truck_van: 'denominator'},
            tags=[subsections['transportation'], unit_people])
        commuters_by_carpool = OBSColumn(
            id='B08006004',
            type='Numeric',
            name='Commuters by Carpool',
            description='The number of workers age 16 years and over within '
            'a geographic area who primarily traveled to work by carpool.  This '
            'is the principal mode of travel or type of conveyance, by distance '
            'rather than time, that the worker usually used to get from home to '
            'work.',
            weight=2,
            aggregate='sum',
            targets={commuters_by_car_truck_van: 'denominator'},
            tags=[subsections['transportation'], unit_people])
        no_cars = OBSColumn(
            id='B08201002',
            type='Numeric',
            name='Car-free households',
            description='The number of households without passenger cars, vans, '
            'and pickup or panel trucks of one-ton capacity or less kept at '
            'home and available for the use of household members. Vehicles '
            'rented or leased for one month or more, company vehicles, and '
            'police and government vehicles are included if kept at home and '
            'used for non-business purposes. Dismantled or immobile vehicles '
            'ware excluded. Vehicles kept at home but used only for business '
            'purposes also are excluded.',
            weight=2,
            aggregate='sum',
            targets={households: 'denominator'},
            tags=[subsections['transportation'], unit_households])
        one_car = OBSColumn(
            id='B08201003',
            type='Numeric',
            name='One car households',
            description='The number of households with one passenger car, van, '
            ', pickup or panel trucks of one-ton capacity or less kept at '
            'home and available for the use of household members. Vehicles '
            'rented or leased for one month or more, company vehicles, and '
            'police and government vehicles are included if kept at home and '
            'used for non-business purposes. Dismantled or immobile vehicles '
            'ware excluded. Vehicles kept at home but used only for business '
            'purposes also are excluded.',
            weight=2,
            aggregate='sum',
            targets={households: 'denominator'},
            tags=[subsections['transportation'], unit_households])
        two_cars = OBSColumn(
            id='B08201004',
            type='Numeric',
            name='Two car households',
            description='The number of households with two passenger cars, vans, '
            'pickup or panel trucks of one-ton capacity or less kept at '
            'home and available for the use of household members. Vehicles '
            'rented or leased for one month or more, company vehicles, and '
            'police and government vehicles are included if kept at home and '
            'used for non-business purposes. Dismantled or immobile vehicles '
            'ware excluded. Vehicles kept at home but used only for business '
            'purposes also are excluded.',
            weight=2,
            aggregate='sum',
            targets={households: 'denominator'},
            tags=[subsections['transportation'], unit_households])
        three_cars = OBSColumn(
            id='B08201005',
            type='Numeric',
            name='Three car households',
            description='The number of households with one passenger cars, vans, '
            'pickup or panel trucks of one-ton capacity or less kept at '
            'home and available for the use of household members. Vehicles '
            'rented or leased for one month or more, company vehicles, and '
            'police and government vehicles are included if kept at home and '
            'used for non-business purposes. Dismantled or immobile vehicles '
            'ware excluded. Vehicles kept at home but used only for business '
            'purposes also are excluded.',
            weight=2,
            aggregate='sum',
            targets={households: 'denominator'},
            tags=[subsections['transportation'], unit_households])
        four_more_cars = OBSColumn(
            id='B08201006',
            type='Numeric',
            name='Four car households',
            description='The number of households with four or more '
            'passenger cars, vans, '
            'pickup or panel trucks of one-ton capacity or less kept at '
            'home and available for the use of household members. Vehicles '
            'rented or leased for one month or more, company vehicles, and '
            'police and government vehicles are included if kept at home and '
            'used for non-business purposes. Dismantled or immobile vehicles '
            'ware excluded. Vehicles kept at home but used only for business '
            'purposes also are excluded.',
            weight=2,
            aggregate='sum',
            targets={households: 'denominator'},
            tags=[subsections['transportation'], unit_households])
        commuters_by_public_transportation = OBSColumn(
            id='B08301010',
            type='Numeric',
            name='Commuters by Public Transportation',
            description='The number of workers age 16 years and over within '
            'a geographic area who primarily traveled to work by public '
            'transportation.  This is the principal mode of travel or type of '
            'conveyance, by distance rather than time, that the worker usually '
            'used to get from home to work.',
            weight=4,
            aggregate='sum',
            targets={workers_16_and_over: 'denominator'},
            tags=[subsections['transportation'], unit_people])
        commuters_by_bus = OBSColumn(
            id='B08006009',
            type='Numeric',
            name='Commuters by Bus',
            description='The number of workers age 16 years and over within '
            'a geographic area who primarily traveled to work by bus.  This is '
            'the principal mode of travel or type of conveyance, by distance '
            'rather than time, that the worker usually used to get from home to '
            'work.  This is a subset of workers who commuted by public '
            'transport.',
            weight=3,
            aggregate='sum',
            targets={commuters_by_public_transportation: 'denominator'},
            tags=[subsections['transportation'], unit_people])
        commuters_by_subway_or_elevated = OBSColumn(
            id='B08006011',
            type='Numeric',
            name='Commuters by Subway or Elevated',
            description='The number of workers age 16 years and over within '
            'a geographic area who primarily traveled to work by subway or '
            'elevated train.  This is the principal mode of travel or type of '
            'conveyance, by distance rather than time, that the worker usually '
            'used to get from home to work.  This is a subset of workers who '
            'commuted by public transport.',
            weight=3,
            aggregate='sum',
            targets={commuters_by_public_transportation: 'denominator'},
            tags=[subsections['transportation'], unit_people])
        walked_to_work = OBSColumn(
            id='B08006015',
            type='Numeric',
            name='Walked to Work',
            description='The number of workers age 16 years and over within '
            'a geographic area who primarily walked to work.  This would mean '
            'that of any way of getting to work, they travelled the most '
            'distance walking.',
            weight=4,
            aggregate='sum',
            targets={workers_16_and_over: 'denominator'},
            tags=[subsections['transportation'], unit_people])
        worked_at_home = OBSColumn(
            id='B08006017',
            type='Numeric',
            name='Worked at Home',
            description='The count within a geographical area of workers over '
            'the age of 16 who worked at home.',
            weight=4,
            aggregate='sum',
            targets={workers_16_and_over: 'denominator'},
            tags=[subsections['transportation'], subsections['employment'], unit_people])
        children = OBSColumn(
            id='B09001001',
            type='Numeric',
            name='Children under 18 Years of Age',
            description='The number of people within each geography who are '
            'under 18 years of age.',
            weight=4,
            aggregate='sum',
            tags=[subsections['age_gender'], subsections['families'], unit_people])
        children_in_single_female_hh = OBSColumn(
            id='B09005005',
            type='Numeric',
            name='Children under 18 years of age in single female-led household',
            weight=1,
            aggregate='sum',
            targets={children: 'denominator'}
        )
        married_households = OBSColumn(
            id='B11001003',
            type='Numeric',
            name='Married households',
            description='People in formal marriages, as well as people in common-law marriages, are included. Does not include same-sex marriages.',
            weight=1,
            targets={households: 'denominator'},
            aggregate='sum',
            tags=[subsections['families'], unit_households]
        )
        male_male_households = OBSColumn(
            id='B11009003',
            type='Numeric',
            name='Households with two male partners',
            description='An unmarried partner is a person age 15 years and over, who is not related to the householder, who shares living quarters, and who has an intimate relationship with the householder.',
            weight=1,
            targets={households: 'denominator'},
            aggregate='sum',
            tags=[subsections['families'], unit_households]
        )
        female_female_households = OBSColumn(
            id='B11009005',
            type='Numeric',
            name='Households with two female partners',
            description='An unmarried partner is a person age 15 years and over, who is not related to the householder, who shares living quarters, and who has an intimate relationship with the householder.',
            weight=1,
            aggregate='sum',
            targets={households: 'denominator'},
            tags=[subsections['families'], unit_households]
        )
        population_3_years_over = OBSColumn(
            id='B14001001',
            type='Numeric',
            name='Population 3 Years and Over',
            description='The total number of people in each geography age '
            '3 years and over.  This denominator is mostly used to calculate '
            'rates of school enrollment.',
            weight=4,
            aggregate='sum',
            tags=[subsections['education'], unit_people])
        in_school = OBSColumn(
            id='B14001002',
            type='Numeric',
            name='Students Enrolled in School',
            description='The total number of people in each geography currently '
            'enrolled at any level of school, from nursery or pre-school to '
            'advanced post-graduate education.  Only includes those over the age '
            'of 3.',
            weight=6,
            aggregate='sum',
            targets={population_3_years_over: 'denominator'},
            tags=[subsections['education'], unit_people])
        in_grades_1_to_4 = OBSColumn(
            id='B14001005',
            type='Numeric',
            name='Students Enrolled in Grades 1 to 4',
            description='The total number of people in each geography currently '
            'enrolled in grades 1 through 4 inclusive.  This corresponds roughly '
            'to elementary school.',
            weight=3,
            aggregate='sum',
            targets={in_school: 'denominator'},
            tags=[subsections['education'], unit_people])
        in_grades_5_to_8 = OBSColumn(
            id='B14001006',
            type='Numeric',
            name='Students Enrolled in Grades 5 to 8',
            description='The total number of people in each geography currently '
            'enrolled in grades 5 through 8 inclusive.  This corresponds roughly '
            'to middle school.',
            weight=3,
            aggregate='sum',
            targets={in_school: 'denominator'},
            tags=[subsections['education'], unit_people])
        in_grades_9_to_12 = OBSColumn(
            id='B14001007',
            type='Numeric',
            name='Students Enrolled in Grades 9 to 12',
            description='The total number of people in each geography currently '
            'enrolled in grades 9 through 12 inclusive.  This corresponds '
            'roughly to high school.',
            weight=3,
            aggregate='sum',
            targets={in_school: 'denominator'},
            tags=[subsections['education'], unit_people])
        in_undergrad_college = OBSColumn(
            id='B14001008',
            type='Numeric',
            name='Students Enrolled as Undergraduate in College',
            description='The number of people in a geographic area who are '
            'enrolled in college at the undergraduate level. Enrollment refers '
            'to being registered or listed as a student in an educational '
            'program leading to a college degree. This may be a public school or '
            'college, a private school or college.',
            weight=5,
            aggregate='sum',
            targets={in_school: 'denominator'},
            tags=[subsections['education'], unit_people])
        pop_25_years_over = OBSColumn(
            id='B15003001',
            type='Numeric',
            name='Population 25 Years and Over',
            description='The number of people in a geographic area who are over '
            'the age of 25.  This is used mostly as a denominator of educational '
            'attainment.',
            weight=2,
            aggregate='sum',
            tags=[subsections['education'], unit_people])
        less_than_high_school_graduate = OBSColumn(
            id='B07009002',
            type='Numeric',
            name='Less than high school graduate',
            description='The number of people in a geographic area over the age '
            'of 25 who have not completed high school or any other advanced '
            'degree.',
            weight=1,
            aggregate='sum',
            targets={pop_25_years_over: 'denominator'},
            tags=[subsections['education'], unit_people]
        )
        high_school_diploma = OBSColumn(
            id='B15003017',
            type='Numeric',
            name='Population Completed High School',
            description='The number of people in a geographic area over the age '
            'of 25 who completed high school, and did not complete a more '
            'advanced degree.',
            weight=4,
            aggregate='sum',
            targets={pop_25_years_over: 'denominator'},
            tags=[subsections['education'], unit_people])
        high_school_including_ged = OBSColumn(
            id='B07009003',
            type='Numeric',
            name='Population with high school degree, including GED',
            description="The number of people in a geographic area over the age "
            "of 25 who attained a high school degree or GED.",
            weight=1,
            aggregate='sum',
            targets={pop_25_years_over: 'denominator'},
            tags=[subsections['education'], unit_people]
        )
        less_one_year_college = OBSColumn(
            id='B15003019',
            type='Numeric',
            name="Population completed less than one year of college, no degree",
            description="The number of people in a geographic area over the age "
            "of 25 who attended college for less than one year and no further.",
            weight=4,
            aggregate='sum',
            targets={pop_25_years_over: 'denominator'},
            tags=[subsections['education'], unit_people]
        )
        one_year_more_college = OBSColumn(
            id='B15003020',
            type='Numeric',
            name="Population completed more than one year of college, no degree",
            description="The number of people in a geographic area over the age "
            "of 25 who attended college for more than one year but did not "
            "obtain a degree",
            weight=4,
            aggregate='sum',
            targets={pop_25_years_over: 'denominator'},
            tags=[subsections['education'], unit_people]
        )
        associates_degree = OBSColumn(
            id='B15003021',
            type='Numeric',
            name="Population Completed Associate's Degree",
            description="The number of people in a geographic area over the age "
            "of 25 who obtained a associate's degree, and did not complete "
            "a more advanced degree.",
            weight=4,
            aggregate='sum',
            targets={pop_25_years_over: 'denominator'},
            tags=[subsections['education'], unit_people])
        some_college_and_associates_degree = OBSColumn(
            id='B07009004',
            type='Numeric',
            name='Population who completed some college or obtained associate\'s degree',
            description="The number of people in a geographic area over the age "
            "of 25 who obtained an associate's degree, and did not complete a more "
            "advanced degree.",
            weight=1,
            aggregate='sum',
            targets={pop_25_years_over: 'denominator'},
            tags=[subsections['education'], unit_people])
        bachelors_degree = OBSColumn(
            id='B15003022',
            type='Numeric',
            name="Population Completed Bachelor's Degree",
            description="The number of people in a geographic area over the age "
            "of 25 who obtained a bachelor's degree, and did not complete a more "
            "advanced degree.",
            weight=4,
            aggregate='sum',
            targets={pop_25_years_over: 'denominator'},
            tags=[subsections['education'], unit_people])
        bachelors_degree_2 = OBSColumn(
            id='B07009005',
            type='Numeric',
            name='Population who completed a bachelor\'s degree. From mobility table.',
            description='',
            weight=0,
            aggregate='sum',
            targets={pop_25_years_over: 'denominator'},
        )
        masters_degree = OBSColumn(
            id='B15003023',
            type='Numeric',
            name="Population Completed Master's Degree",
            description="The number of people in a geographic area over the age "
            "of 25 who obtained a master's degree, but did not complete a more "
            "advanced degree.",
            weight=4,
            aggregate='sum',
            targets={pop_25_years_over: 'denominator'},
            tags=[subsections['education'], unit_people])
        graduate_professional_degree = OBSColumn(
            id='B07009006',
            type='Numeric',
            name='Population who completed a graduate or professional degree',
            description='',
            weight=0,
            aggregate='sum',
            targets={pop_25_years_over: 'denominator'},
            tags=[subsections['education'], unit_people])
        pop_5_years_over = OBSColumn(
            id='B16001001',
            type='Numeric',
            name='Population 5 Years and Over',
            description='The number of people in a geographic area who are over '
            'the age of 5.  This is primarily used as a denominator of measures '
            'of language spoken at home.',
            weight=2,
            aggregate='sum',
            tags=[subsections['language'], unit_people])
        speak_only_english_at_home = OBSColumn(
            id='B16001002',
            type='Numeric',
            name='Speaks only English at Home',
            description='The number of people in a geographic area over age '
            '5 who speak only English at home.',
            weight=3,
            aggregate='sum',
            targets={pop_5_years_over: 'denominator'},
            tags=[subsections['language'], unit_people])
        speak_spanish_at_home = OBSColumn(
            id='B16001003',
            type='Numeric',
            name='Speaks Spanish at Home',
            description='The number of people in a geographic area over age '
            '5 who speak Spanish at home, possibly in addition to other '
            'languages.',
            weight=4,
            aggregate='sum',
            targets={pop_5_years_over: 'denominator'},
            tags=[subsections['language'], unit_people])
        speak_spanish_at_home_low_english = OBSColumn(
            id='B16001005',
            type='Numeric',
            name='Speaks Spanish at Home, speaks English less than "very well"',
            description='',
            weight=0,
            aggregate='sum',
            targets={pop_5_years_over: 'denominator'},
            tags=[subsections['language'], unit_people])
        pop_determined_poverty_status = OBSColumn(
            id='B17001001',
            type='Numeric',
            name='Population for Whom Poverty Status Determined',
            description='The number of people in each geography who could be '
            'identified as either living in poverty or not.  This should be used '
            'as the denominator when calculating poverty rates, as it excludes '
            'people for whom it was not possible to determine poverty.',
            weight=2,
            aggregate='sum',
            tags=[subsections['income'], unit_people])
        poverty = OBSColumn(
            id='B17001002',
            type='Numeric',
            name='Income In The Past 12 Months Below Poverty Level',
            description="The number of people in a geographic area who are part "
            "of a family (which could be just them as an individual) determined "
            "to be \"in poverty\" following the Office of Management and "
            "Budget's Directive 14. "
            "(https://www.census.gov/hhes/povmeas/methodology/ombdir14.html)",
            weight=2,
            aggregate='sum',
            targets={pop_determined_poverty_status: 'denominator'},
            tags=[subsections['income'], unit_people])
        median_income = OBSColumn(
            id='B19013001',
            type='Numeric',
            name='Median Household Income in the past 12 Months',
            description="Within a geographic area, the median income received "
            "by every household on a regular basis before payments for personal "
            "income taxes, social security, union dues, medicare deductions, "
            "etc.  It includes income received from wages, salary, commissions, "
            "bonuses, and tips; self-employment income from own nonfarm or farm "
            "businesses, including proprietorships and partnerships; interest, "
            "dividends, net rental income, royalty income, or income from "
            "estates and trusts; Social Security or Railroad Retirement income; "
            "Supplemental Security Income (SSI); any cash public assistance or "
            "welfare payments from the state or local welfare office; "
            "retirement, survivor, or disability benefits; and any other sources "
            "of income received regularly such as Veterans' (VA) payments, "
            "unemployment and/or worker's compensation, child support, and "
            "alimony.",
            weight=8,
            aggregate='median',
            targets={households: 'universe'},
            tags=[subsections['income'], unit_money])
        gini_index = OBSColumn(
            id='B19083001',
            type='Numeric',
            name='Gini Index',
            description='The Gini index, or index of income concentration, is '
            'a statistical measure of income inequality ranging from 0 to 1. '
            'A measure of 1 indicates perfect inequality, i.e., one household '
            'having all the income and rest having none. A measure of '
            '0 indicates perfect equality, i.e., all households having an equal '
            'share of income.',
            weight=5,
            aggregate='',
            tags=[subsections['income'], unit_ratio])
        income_per_capita = OBSColumn(
            id='B19301001',
            type='Numeric',
            name='Per Capita Income in the past 12 Months',
            description='Per capita income is the mean income computed for every man, woman, and child in a particular group. It is derived by dividing the total income of a particular group by the total population.',
            weight=7,
            aggregate='average',
            targets={total_pop: 'universe'},
            tags=[subsections['income'], unit_money])
        vacant_housing_units = OBSColumn(
            id='B25002003',
            type='Numeric',
            name='Vacant Housing Units',
            description="The count of vacant housing units in a geographic "
            "area. A housing unit is vacant if no one is living in it at the "
            "time of enumeration, unless its occupants are only temporarily "
            "absent. Units temporarily occupied at the time of enumeration "
            "entirely by people who have a usual residence elsewhere are also "
            "classified as vacant.",
            weight=8,
            aggregate='sum',
            targets={housing_units: 'denominator'},
            tags=[subsections['housing'], unit_housing])
        vacant_housing_units_for_rent = OBSColumn(
            id='B25004002',
            type='Numeric',
            name='Vacant Housing Units for Rent',
            description="The count of vacant housing units in a geographic area "
            "that are for rent. A housing unit is vacant if no one is living in "
            "it at the time of enumeration, unless its occupants are only "
            "temporarily absent. Units temporarily occupied at the time of "
            "enumeration entirely by people who have a usual residence elsewhere "
            "are also classified as vacant.",
            weight=7,
            aggregate='sum',
            targets={vacant_housing_units: 'denominator'},
            tags=[subsections['housing'], unit_housing])
        vacant_housing_units_for_sale = OBSColumn(
            id='B25004004',
            type='Numeric',
            name='Vacant Housing Units for Sale',
            description="The count of vacant housing units in a geographic area "
            "that are for sale. A housing unit is vacant if no one is living in "
            "it at the time of enumeration, unless its occupants are only "
            "temporarily absent. Units temporarily occupied at the time of "
            "enumeration entirely by people who have a usual residence elsewhere "
            "are also classified as vacant.",
            weight=7,
            aggregate='sum',
            targets={vacant_housing_units: 'denominator'},
            tags=[subsections['housing'], unit_housing])
        median_rent = OBSColumn(
            id='B25058001',
            type='Numeric',
            name='Median Rent',
            description="The median contract rent within a geographic area. The "
            "contract rent is the monthly rent agreed to or contracted for, "
            "regardless of any furnishings, utilities, fees, meals, or services "
            "that may be included. For vacant units, it is the monthly rent "
            "asked for the rental unit at the time of interview.",
            weight=8,
            aggregate='median',
            targets={housing_units_renter_occupied: 'universe'},
            tags=[subsections['housing'], unit_money])
        percent_income_spent_on_rent = OBSColumn(
            id='B25071001',
            type='Numeric',
            name='Percent of Household Income Spent on Rent',
            description="Within a geographic area, the median percentage of "
            "household income which was spent on gross rent.  Gross rent is the "
            "amount of the contract rent plus the estimated average monthly cost "
            "of utilities (electricity, gas, water, sewer etc.) and fuels (oil, "
            "coal, wood, etc.) if these are paid by the renter.  Household "
            "income is the sum of the income of all people 15 years and older "
            "living in the household.",
            weight=4,
            aggregate='average',
            targets={households: 'universe'},
            tags=[subsections['housing'], subsections['income'], unit_ratio])
        owner_occupied_housing_units = OBSColumn(
            id='B25075001',
            type='Numeric',
            name='Owner-occupied Housing Units',
            description="",
            weight=5,
            aggregate='sum',
            targets={housing_units: 'denominator'},
            tags=[subsections['housing'], unit_housing])
        million_dollar_housing_units = OBSColumn(
            id='B25075025',
            type='Numeric',
            name='Owner-occupied Housing Units valued at $1,000,000 or more.',
            description="The count of owner occupied housing units in "
            "a geographic area that are valued at $1,000,000 or more.  Value is "
            "the respondent's estimate of how much the property (house and lot, "
            "mobile home and lot, or condominium unit) would sell for if it were "
            "for sale.",
            weight=5,
            aggregate='sum',
            targets={owner_occupied_housing_units: 'denominator'},
            tags=[subsections['housing'], unit_housing])
        mortgaged_housing_units = OBSColumn(
            id='B25081002',
            type='Numeric',
            name='Owner-occupied Housing Units with a Mortgage',
            description="The count of housing units within a geographic area "
            "that are mortagaged. \"Mortgage\" refers to all forms of debt where "
            "the property is pledged as security for repayment of the debt, "
            "including deeds of trust, trust deed, contracts to purchase, land "
            "contracts, junior mortgages, and home equity loans.",
            weight=4,
            aggregate='sum',
            targets={owner_occupied_housing_units: 'denominator'},
            tags=[subsections['housing'], unit_housing])
        dwellings_1_units_detached = OBSColumn(
            id='B25024002',
            type='Numeric',
            name='Single-family (one unit) detached dwellings',
            description='',
            weight=0,
            aggregate='sum',
            targets={housing_units: 'denominator'},
            tags=[subsections['housing'], unit_housing]
        )
        dwellings_1_units_attached = OBSColumn(
            id='B25024003',
            type='Numeric',
            name='Single-family (one unit) attached dwellings',
            description='',
            weight=0,
            aggregate='sum',
            targets={housing_units: 'denominator'},
            tags=[subsections['housing'], unit_housing]
        )
        dwellings_2_units = OBSColumn(
            id='B25024004',
            type='Numeric',
            name='Two-family (two unit) dwellings',
            description='',
            weight=0,
            aggregate='sum',
            targets={housing_units: 'denominator'},
            tags=[subsections['housing'], unit_housing]
        )
        dwellings_3_to_4_units = OBSColumn(
            id='B25024005',
            type='Numeric',
            name='Multifamily dwellings with three to 4 units',
            description='',
            weight=0,
            aggregate='sum',
            targets={housing_units: 'denominator'},
            tags=[subsections['housing'], unit_housing]
        )
        dwellings_5_to_9_units = OBSColumn(
            id='B25024006',
            type='Numeric',
            name='Apartment buildings with 5 to 9 units',
            description='',
            weight=0,
            aggregate='sum',
            targets={housing_units: 'denominator'},
            tags=[subsections['housing'], unit_housing]
        )
        dwellings_10_to_19_units = OBSColumn(
            id='B25024007',
            type='Numeric',
            name='Apartment buildings with 10 to 19 units',
            description='',
            weight=0,
            aggregate='sum',
            targets={housing_units: 'denominator'},
            tags=[subsections['housing'], unit_housing]
        )
        dwellings_20_to_49_units = OBSColumn(
            id='B25024008',
            type='Numeric',
            name='Apartment buildings with 20 to 49 units',
            description='',
            weight=0,
            aggregate='sum',
            targets={housing_units: 'denominator'},
            tags=[subsections['housing'], unit_housing]
        )
        dwellings_50_or_more_units = OBSColumn(
            id='B25024009',
            type='Numeric',
            name='Apartment buildings with 50 or more units',
            description='',
            weight=0,
            aggregate='sum',
            targets={housing_units: 'denominator'},
            tags=[subsections['housing'], unit_housing]
        )
        mobile_homes = OBSColumn(
            id='B25024010',
            type='Numeric',
            name='Mobile homes',
            description='A manufactured home is defined as a movable dwelling, 8 feet or more wide and 40 feet or more long, designed to be towed on its own chassis, with transportation gear integral to the unit when it leaves the factory, and without need of a permanent foundation. These homes are built in accordance with the U.S. Department of Housing and Urban Development (HUD) building code.',
            weight=1,
            aggregate='sum',
            targets={housing_units: 'denominator'},
            tags=[subsections['housing'], unit_housing]
        )
        housing_built_2005_or_later = OBSColumn(
            id='B25034002',
            type='Numeric',
            name='Housing units built in 2005 or later',
            description='A house, an apartment, a mobile home or trailer, a group of rooms, or a single room occupied as separate living quarters, or if vacant, intended for occupancy as separate living quarters built in 2005 or later.',
            aggregate='sum',
            weight=1,
            targets={housing_units: 'denominator'},
            tags=[subsections['housing'], unit_housing]
        )
        housing_built_2000_to_2004 = OBSColumn(
            id='B25034003',
            type='Numeric',
            name='Housing units built between 2000 and 2004',
            description='A house, an apartment, a mobile home or trailer, a group of rooms, or a single room occupied as separate living quarters, or if vacant, intended for occupancy as separate living quarters built from 2000 to 2004.',
            weight=1,
            aggregate='sum',
            targets={housing_units: 'denominator'},
            tags=[subsections['housing'], unit_housing]
        )
        housing_built_1939_or_earlier = OBSColumn(
            id='B25034010',
            type='Numeric',
            name='Housing units built before 1939',
            description='A house, an apartment, a mobile home or trailer, a group of rooms, or a single room occupied as separate living quarters, or if vacant, intended for occupancy as separate living quarters built in 1939 or earlier.',
            weight=1,
            aggregate='sum',
            targets={housing_units: 'denominator'},
            tags=[subsections['housing'], unit_housing]
        )

        #* families with young children (under 6 years of age):
        #  - B23008002: total families with children under 6 years
        families_with_young_children = OBSColumn(
            id='B23008002',
            type='Numeric',
            name='Families with young children (under 6 years of age)',
            description='',
            weight=0,
            aggregate='sum',
            tags=[subsections['families'], unit_households])

        #  - B23008003: living with two parents
        two_parent_families_with_young_children = OBSColumn(
            id='B23008003',
            type='Numeric',
            name='Two-parent families with young children (under 6 years of age)',
            description='',
            weight=0,
            aggregate='sum',
            targets={families_with_young_children: 'denominator'},
            tags=[subsections['families'], unit_households])
        #  - B23008004: living with two parents, both in labor force
        two_parents_in_labor_force_families_with_young_children = OBSColumn(
            id='B23008004',
            type='Numeric',
            name='Two-parent families, both parents in labor force with young '
            'children (under 6 years of age)',
            description='',
            weight=0,
            aggregate='sum',
            targets={families_with_young_children: 'denominator'},
            tags=[subsections['families'], unit_households])
        #  - B23008005: living with two parents, father only in labor force
        two_parents_father_in_labor_force_families_with_young_children = OBSColumn(
            id='B23008005',
            type='Numeric',
            name='Two-parent families, father only in labor force with young '
            'children (under 6 years of age)',
            description='',
            weight=0,
            aggregate='sum',
            targets={families_with_young_children: 'denominator'},
            tags=[subsections['families'], subsections['employment'], unit_households])

        #  - B23008006: living with two parents, mother only in labor force
        two_parents_mother_in_labor_force_families_with_young_children = OBSColumn(
            id='B23008006',
            type='Numeric',
            name='Two-parent families, mother only in labor force with young '
            'children (under 6 years of age)',
            description='',
            weight=0,
            aggregate='sum',
            targets={families_with_young_children: 'denominator'},
            tags=[subsections['families'], subsections['employment'], unit_households])

        #  - B23008007: living with two parents, neither parent in labor force
        two_parents_not_in_labor_force_families_with_young_children = OBSColumn(
            id='B23008007',
            type='Numeric',
            name='Two-parent families, neither parent in labor force with young '
            'children (under 6 years of age)',
            description='',
            weight=0,
            aggregate='sum',
            targets={families_with_young_children: 'denominator'},
            tags=[subsections['families'], subsections['employment'], unit_households])

        #  - B23008008: living with one parent
        one_parent_families_with_young_children = OBSColumn(
            id='B23008008',
            type='Numeric',
            name='One-parent families with young children (under 6 years of age)',
            description='',
            weight=0,
            aggregate='sum',
            targets={families_with_young_children: 'denominator'},
            tags=[subsections['families'], unit_households])

        #  - B23008009: living with father
        father_one_parent_families_with_young_children = OBSColumn(
            id='B23008009',
            type='Numeric',
            name='One-parent families, father, with young children (under '
            '6 years of age)',
            description='',
            weight=0,
            aggregate='sum',
            targets={families_with_young_children: 'denominator'},
            tags=[subsections['families'], unit_households])

        #  - B23008010: living with father who is in labor force
        father_in_labor_force_one_parent_families_with_young_children = OBSColumn(
            id='B23008010',
            type='Numeric',
            name='One-parent families, father in labor force, with young '
            'children (under 6 years of age)',
            description='',
            weight=0,
            aggregate='sum',
            targets={families_with_young_children: 'denominator'},
            tags=[subsections['families'], subsections['employment'], unit_households])

        # - B23025001: population age 16 and over
        pop_16_over = OBSColumn(
            id='B23025001',
            type='Numeric',
            name='Population age 16 and over',
            description='The number of people in each geography who are age '
            '16 or over.',
            weight=1,
            aggregate='sum',
            targets={},
            tags=[subsections['age_gender'], subsections['employment'], unit_people]
        )

        # - B23025002: pop in labor force
        pop_in_labor_force = OBSColumn(
            id='B23025002',
            type='Numeric',
            name='Population in Labor Force',
            description='The number of people in each geography who are either '
            'in the civilian labor force or are '
            'members of the U.S. Armed Forces (people on active duty with the '
            'United States Army, Air Force, Navy, Marine Corps, or Coast Guard).',
            weight=1,
            aggregate='sum',
            targets={pop_16_over: 'denominator'},
            tags=[subsections['employment'], unit_people]
        )

        # - B23025003: civilian labor force
        civilian_labor_force = OBSColumn(
            id='B23025003',
            type='Numeric',
            name='Population in Civilian Labor Force',
            description='The number of civilians 16 years and over in each '
            'geography who can be classified '
            'as either "employed" or "unemployed" below.',
            weight=1,
            aggregate='sum',
            targets={pop_in_labor_force: 'denominator'},
            tags=[subsections['employment'], unit_people]
        )

        # - B23025004: employed population
        employed_pop = OBSColumn(
            id='B23025004',
            type='Numeric',
            name='Employed Population',
            description='The number of civilians 16 years old and '
            'over in each geography who either (1) were "at work," that is, '
            'those who did any work '
            'at all during the reference week as paid employees, worked in their '
            'own business or profession, worked on their own farm, or worked 15 '
            'hours or more as unpaid workers on a family farm or in a family '
            'business; or (2) were "with a job but not at work," that is, those '
            'who did not work during the reference week but had jobs or '
            'businesses from which they were temporarily absent due to illness, '
            'bad weather, industrial dispute, vacation, or other personal '
            'reasons. Excluded from the employed are people whose only activity '
            'consisted of work around the house or unpaid volunteer work for '
            'religious, charitable, and similar organizations; also excluded are '
            'all institutionalized people and people on active duty in the '
            'United States Armed Forces.',
            weight=1,
            aggregate='sum',
            targets={civilian_labor_force: 'denominator'},
            tags=[subsections['employment'], unit_people]
        )

        # - B23025005: unemployed population
        unemployed_pop = OBSColumn(
            id='B23025005',
            type='Numeric',
            name='Unemployed Population',
            description='The number of civilians in each geography who are 16 '
            'years old and over are classified as '
            'unemployed if they (1) were neither "at work" nor "with a job but '
            'not at work" during the reference week, and (2) were actively '
            'looking for work during the last 4 weeks, and (3) were available to '
            'start a job. Also included as unemployed are civilians who did not '
            'work at all during the reference week, were waiting to be called '
            'back to a job from which they had been laid off, and were available '
            'for work except for temporary illness. Examples of job seeking '
            'activities are:' '''
              * Registering at a public or private employment office
              * Meeting with prospective employers
              * Investigating possibilities for starting a professional
                practice or opening a business
              * Placing or answering advertisements
              * Writing letters of application
              * Being on a union or professional register''',
            weight=1,
            aggregate='sum',
            targets={civilian_labor_force: 'denominator'},
            tags=[subsections['employment'], unit_people]
        )

        # - B23025006: in armed forces
        armed_forces = OBSColumn(
            id='B23025006',
            type='Numeric',
            name='Population in Armed Forces',
            description='The number of people in each geography who are members '
            'of the U.S. Armed Forces (people on active duty with the United '
            'States Army, Air Force, Navy, Marine Corps, or Coast Guard).',
            weight=1,
            aggregate='sum',
            targets={pop_in_labor_force: 'denominator'},
            tags=[subsections['employment'], unit_people]
        )

        # - B23025007: not in labor force
        not_in_labor_force = OBSColumn(
            id='B23025007',
            type='Numeric',
            name='Population Not in Labor Force',
            description='The number of people in each geography who are 16 '
            'years old and over who are not '
            'classified as members of the labor force. This category consists '
            'mainly of students, homemakers, retired workers, seasonal workers '
            'interviewed in an off season who were not looking for work, '
            'institutionalized people, and people doing only incidental unpaid '
            'family work (less than 15 hours during the reference week).',
            weight=1,
            aggregate='sum',
            targets={pop_16_over: 'denominator'},
            tags=[subsections['employment'], unit_people]
        )

        employed_agriculture_forestry_fishing_hunting_mining = OBSColumn(
            id='C24050002',
            type='Numeric',
            name='Workers employed in firms in agriculture, forestry, fishing, hunting, or mining',
            weight=1,
            aggregate='sum',
            targets={employed_pop: 'denominator'},
            description='The Agriculture, Forestry, Fishing and Hunting sector comprises establishments primarily engaged in growing crops, raising animals, harvesting timber, and harvesting fish and other animals from a farm, ranch, or their natural habitats.',
            tags=[subsections['employment'], unit_people]
        )
        employed_construction = OBSColumn(
            id='C24050003',
            type='Numeric',
            name='Workers employed in firms in construction',
            weight=1,
            aggregate='sum',
            targets={employed_pop: 'denominator'},
            description='The Construction sector comprises establishments primarily engaged in the construction of buildings or engineering projects (e.g., highways and utility systems). Construction work done may include new work, additions, alterations, or maintenance and repairs.',
            tags=[subsections['employment'], unit_people]
        )
        employed_manufacturing = OBSColumn(
            id='C24050004',
            type='Numeric',
            name='Workers employed in firms in manufacturing',
            weight=1,
            aggregate='sum',
            targets={employed_pop: 'denominator'},
            description='The Manufacturing sector comprises establishments engaged in the mechanical, physical, or chemical transformation of materials, substances, or components into new products.',
            tags=[subsections['employment'], unit_people]
        )
        employed_wholesale_trade = OBSColumn(
            id='C24050005',
            type='Numeric',
            name='Workers employed in firms in wholesale trade',
            weight=1,
            aggregate='sum',
            targets={employed_pop: 'denominator'},
            description='The Wholesale Trade sector comprises establishments engaged in wholesaling merchandise, generally without transformation, and rendering services incidental to the sale of merchandise. The wholesaling process is an intermediate step in the distribution of merchandise. Wholesalers are organized to sell or arrange the purchase or sale of (a) goods for resale (i.e., goods sold to other wholesalers or retailers), (b) capital or durable nonconsumer goods, and (c) raw and intermediate materials and supplies used in production.',
            tags=[subsections['employment'], unit_people]
        )
        employed_retail_trade = OBSColumn(
            id='C24050006',
            type='Numeric',
            name='Workers employed in firms in retail trade',
            weight=1,
            aggregate='sum',
            targets={employed_pop: 'denominator'},
            description='The Retail Trade sector comprises establishments engaged in retailing merchandise, generally without transformation, and rendering services incidental to the sale of merchandise. The retailing process is the final step in the distribution of merchandise; retailers are, therefore, organized to sell merchandise in small quantities to the general public.',
            tags=[subsections['employment'], unit_people]
        )
        employed_transportation_warehousing_utilities = OBSColumn(
            id='C24050007',
            type='Numeric',
            name='Workers employed in firms in transportation, warehousing, and utilities',
            weight=1,
            aggregate='sum',
            targets={employed_pop: 'denominator'},
            description='The Transportation and Warehousing sector includes industries providing transportation of passengers and cargo, warehousing and storage for goods, scenic and sightseeing transportation, and support activities related to modes of transportation. The modes of transportation are air, rail, water, road, and pipeline.',
            tags=[subsections['employment'], unit_people]
        )
        employed_information = OBSColumn(
            id='C24050008',
            type='Numeric',
            name='Workers employed in firms in information',
            weight=1,
            aggregate='sum',
            targets={employed_pop: 'denominator'},
            description='The Information sector comprises establishments engaged in the following processes: (a) producing and distributing information and cultural products, (b) providing the means to transmit or distribute these products as well as data or communications, and (c) processing data. Included are the publishing industries, the motion picture and sound recording industries; the broadcasting industries, the telecommunications industries; Web search portals, data processing industries, and the information services industries.',
            tags=[subsections['employment'], unit_people]
        )
        employed_finance_insurance_real_estate = OBSColumn(
            id='C24050009',
            type='Numeric',
            name='Workers employed in firms in finance, insurance, real estate and rental and leasing',
            weight=1,
            aggregate='sum',
            targets={employed_pop: 'denominator'},
            description='The Real Estate and Rental and Leasing sector comprises establishments primarily engaged in renting, leasing, or otherwise allowing the use of tangible or intangible assets, and establishments providing related services. The major portion of this sector comprises establishments that rent, lease, or otherwise allow the use of their own assets by others. The assets may be tangible, as is the case of real estate and equipment, or intangible, as is the case with patents and trademarks.',
            tags=[subsections['employment'], unit_people]
        )
        employed_science_management_admin_waste = OBSColumn(
            id='C24050010',
            type='Numeric',
            name='Workers employed in firms in professional scientific, management, administrative and waste management services',
            weight=1,
            aggregate='sum',
            targets={employed_pop: 'denominator'},
            description='The Administrative and Support and Waste Management and Remediation Services sector comprises establishments performing routine support activities for the day-to-day operations of other organizations. The establishments in this sector specialize in one or more of these support activities and provide these services to clients in a variety of industries and, in some cases, to households. Activities performed include office administration, hiring and placing of personnel, document preparation and similar clerical services, solicitation, collection, security and surveillance services, cleaning, and waste disposal services.',
            tags=[subsections['employment'], unit_people]
        )
        employed_education_health_social = OBSColumn(
            id='C24050011',
            type='Numeric',
            name='Workers employed in firms in educational services, health care, and social assistance',
            weight=1,
            aggregate='sum',
            targets={employed_pop: 'denominator'},
            description='Outpatient health services, other than hospital care, including: public health administration; research and education; categorical health programs; treatment and immunization clinics; nursing; environmental health activities such as air and water pollution control; ambulance service if provided separately from fire protection services, and other general public health activities such as mosquito abatement. School health services provided by health agencies (rather than school agencies) are included here. Sewage treatment operations are classified under Sewerage.',
            tags=[subsections['employment'], unit_people]
        )
        employed_arts_entertainment_recreation_accommodation_food = OBSColumn(
            id='C24050012',
            type='Numeric',
            name='Workers employed in firms in arts, entertainment, recreation, accommodation and food services',
            weight=1,
            aggregate='sum',
            targets={employed_pop: 'denominator'},
            description='The Arts, Entertainment, and Recreation sector includes a wide range of establishments that operate facilities or provide services to meet varied cultural, entertainment, and recreational interests of their patrons. This sector comprises (1) establishments that are involved in producing, promoting, or participating in live performances, events, or exhibits intended for public viewing; (2) establishments that preserve and exhibit objects and sites of historical, cultural, or educational interest; and (3) establishments that operate facilities or provide services that enable patrons to participate in recreational activities or pursue amusement, hobby, and leisure-time interests.',
            tags=[subsections['employment'], unit_people]
        )
        employed_other_services_not_public_admin = OBSColumn(
            id='C24050013',
            type='Numeric',
            name='Workers employed in firms in other services except public administration',
            weight=1,
            aggregate='sum',
            targets={employed_pop: 'denominator'},
            description='The Other Services (Except Public Administration) sector comprises establishments engaged in providing services not specifically provided for elsewhere in the classification system. Establishments in this sector are primarily engaged in activities such as equipment and machinery repairing, promoting or administering religious activities, grantmaking, advocacy, and providing drycleaning and laundry services, personal care services, death care services, pet care services, photofinishing services, temporary parking services, and dating services. Private households that engage in employing workers on or about the premises in activities primarily concerned with the operation of the household are included in this sector.',
            tags=[subsections['employment'], unit_people]
        )
        employed_public_administration = OBSColumn(
            id='C24050014',
            type='Numeric',
            name='Workers employed in firms in public administration',
            weight=1,
            aggregate='sum',
            targets={employed_pop: 'denominator'},
            description='The Public Administration sector consists of establishments of federal, state, and local government agencies that administer, oversee, and manage public programs and have executive, legislative, or judicial authority over other institutions within a given area. These agencies also set policy, create laws, adjudicate civil and criminal legal cases, provide for public safety and for national defense. In general, government establishments in the public administration sector oversee governmental programs and activities that are not performed by private establishments.',
            tags=[subsections['employment'], unit_people]
        )
        occupation_management_arts = OBSColumn(
            id='C24050015',
            type='Numeric',
            name='Workers employed in management business science and arts occupations',
            weight=0,
            aggregate='sum',
            targets={employed_pop: 'denominator'},
            description='',
            tags=[subsections['employment'], unit_people]
        )
        occupation_services = OBSColumn(
            id='C24050029',
            type='Numeric',
            name='Workers employed in service occupations',
            weight=0,
            aggregate='sum',
            targets={employed_pop: 'denominator'},
            description='',
            tags=[subsections['employment'], unit_people]
        )
        occupation_sales_office = OBSColumn(
            id='C24050043',
            type='Numeric',
            name='Workers employed in sales and office occupations',
            weight=0,
            aggregate='sum',
            targets={employed_pop: 'denominator'},
            description='',
            tags=[subsections['employment'], unit_people]
        )
        occupation_natural_resources_construction_maintenance = OBSColumn(
            id='C24050057',
            type='Numeric',
            name='Workers employed in natural resources, construction, and maintenance occupations',
            weight=0,
            aggregate='sum',
            targets={employed_pop: 'denominator'},
            description='',
            tags=[subsections['employment'], unit_people]
        )
        occupation_production_transportation_material = OBSColumn(
            id='C24050071',
            type='Numeric',
            name='Workers employed in production, transportation, and material moving',
            weight=0,
            aggregate='sum',
            targets={employed_pop: 'denominator'},
            description='',
            tags=[subsections['employment'], unit_people]
        )

        # TODO
        #  - B23008011: living with father who is not in labor force
        #  - B23008012: living with mother
        #  - B23008013: living with mother, who is in labor force
        #  - B23008014: living with mother, who is not in labor force
        #  - B11003004: married couple
        #  - B11003011: male householder, no wife
        #  - B11003017: female householder, no husband
        #  - B23003003: living with female 20 to 64 years of age
        #  - B23003004: living with female 20 to 64 years of age who is in labor force
        #  - B23003005: living with female 20 to 64 years of age who is in armed forces
        #  - B23003006: living with female 20 to 64 years of age who is in civilian labor force
        #  - B23003007: living with female 20 to 64 years of age who is employed in civilian labor force
        #  - B23003008: living with female 20 to 64 years of age who is unemployed in civilian labor force
        #  - B23003009: living with female 20 to 64 years of age who is not in labor force

        male_under_5 = OBSColumn(
            id='B01001003',
            type='Numeric',
            name='Male under 5 years',
            description='The male population over the age of five years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        male_5_to_9 = OBSColumn(
            id='B01001004',
            type='Numeric',
            name='Male age 5 to 9',
            description='The male population between the age of five years to nine years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        male_10_to_14 = OBSColumn(
            id='B01001005',
            type='Numeric',
            name='Male age 10 to 14',
            description='The male population between the age of ten years to fourteen years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        male_15_to_17 = OBSColumn(
            id='B01001006',
            type='Numeric',
            name='Male age 15 to 17',
            description='The male population between the age of fifteeen years to seventeen years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        male_18_to_19 = OBSColumn(
            id='B01001007',
            type='Numeric',
            name='Male age 18 and 19',
            description='The male population between the age of eighteen years to nineteen years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        male_20 = OBSColumn(
            id='B01001008',
            type='Numeric',
            name='Male age 20',
            description='The male population with an age of twenty years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        male_21 = OBSColumn(
            id='B01001009',
            type='Numeric',
            name='Male age 21',
            description='The male population with an age of twenty-one years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        male_22_to_24 = OBSColumn(
            id='B01001010',
            type='Numeric',
            name='Male age 22 to 24',
            description='The male population between the age of twenty-two years to twenty-four years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        male_25_to_29 = OBSColumn(
            id='B01001011',
            type='Numeric',
            name='Male age 25 to 29',
            description='The male population between the age of twenty-five years to twenty-nine years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        male_30_to_34 = OBSColumn(
            id='B01001012',
            type='Numeric',
            name='Male age 30 to 34',
            description='The male population between the age of thirty years to thirty-four years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        male_35_to_39 = OBSColumn(
            id='B01001013',
            type='Numeric',
            name='Male age 35 to 39',
            description='The male population between the age of thirty-five years to thirty-nine years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        male_40_to_44 = OBSColumn(
            id='B01001014',
            type='Numeric',
            name='Male age 40 to 44',
            description='The male population between the age of fourty years to fourty-four years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )

        male_65_to_66 = OBSColumn(
            id='B01001020',
            type='Numeric',
            name='Male age 65 to 66',
            description='The male population between the age of sixty-five years to sixty-six years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        male_67_to_69 = OBSColumn(
            id='B01001021',
            type='Numeric',
            name='Male age 67 to 69',
            description='The male population between the age of sixty-seven years to sixty-nine years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        male_70_to_74 = OBSColumn(
            id='B01001022',
            type='Numeric',
            name='Male age 70 to 74',
            description='The male population between the age of seventy years to seventy-four years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        male_75_to_79 = OBSColumn(
            id='B01001023',
            type='Numeric',
            name='Male age 75 to 79',
            description='The male population between the age of seventy-five years to seventy-nine years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        male_80_to_84 = OBSColumn(
            id='B01001024',
            type='Numeric',
            name='Male age 80 to 84',
            description='The male population between the age of eighty years to eighty-four years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        male_85_and_over = OBSColumn(
            id='B01001025',
            type='Numeric',
            name='Male age 85 and over',
            description='The male population of the age of eighty-five years and over within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )

        # female
        female_under_5 = OBSColumn(
            id='B01001027',
            type='Numeric',
            name='Female under 5 years',
            description='The female population over the age of five years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_5_to_9 = OBSColumn(
            id='B01001028',
            type='Numeric',
            name='Female age 5 to 9',
            description='The female population between the age of five years to nine years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_10_to_14 = OBSColumn(
            id='B01001029',
            type='Numeric',
            name='Female age 10 to 14',
            description='The female population between the age of ten years to fourteen years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_15_to_17 = OBSColumn(
            id='B01001030',
            type='Numeric',
            name='Female age 15 to 17',
            description='The female population between the age of fifteeen years to seventeen years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_18_to_19 = OBSColumn(
            id='B01001031',
            type='Numeric',
            name='Female age 18 and 19',
            description='The female population between the age of eighteen years to nineteen years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_20 = OBSColumn(
            id='B01001032',
            type='Numeric',
            name='Female age 20',
            description='The female population with an age of twenty years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_21 = OBSColumn(
            id='B01001033',
            type='Numeric',
            name='Female age 21',
            description='The female population with an age of twenty-one years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_22_to_24 = OBSColumn(
            id='B01001034',
            type='Numeric',
            name='Female age 22 to 24',
            description='The female population between the age of twenty-two years to twenty-four years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_25_to_29 = OBSColumn(
            id='B01001035',
            type='Numeric',
            name='Female age 25 to 29',
            description='The female population between the age of twenty-five years to twenty-nine years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_30_to_34 = OBSColumn(
            id='B01001036',
            type='Numeric',
            name='Female age 30 to 34',
            description='The female population between the age of thirty years to thirty-four years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_35_to_39 = OBSColumn(
            id='B01001037',
            type='Numeric',
            name='Female age 35 to 39',
            description='The female population between the age of thirty-five years to thirty-nine years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_40_to_44 = OBSColumn(
            id='B01001038',
            type='Numeric',
            name='Female age 40 to 44',
            description='The female population between the age of fourty years to fourty-four years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_45_to_49 = OBSColumn(
            id='B01001039',
            type='Numeric',
            name='Female age 45 to 49',
            description='The female population between the age of fourty-five years to fourty-nine years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_50_to_54 = OBSColumn(
            id='B01001040',
            type='Numeric',
            name='Female age 50 to 54',
            description='The female population between the age of fifty years to fifty-four years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_55_to_59 = OBSColumn(
            id='B01001041',
            type='Numeric',
            name='Female age 55 to 59',
            description='The female population between the age of fifty-five years to fifty-nine years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_60_to_61 = OBSColumn(
            id='B01001042',
            type='Numeric',
            name='Female age 60 and 61',
            description='The female population between the age of sixty years to sixty-one years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_62_to_64 = OBSColumn(
            id='B01001043',
            type='Numeric',
            name='Female age 62 to 64',
            description='The female population between the age of sixty-two years to sixty-four years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )

        female_65_to_66 = OBSColumn(
            id='B01001044',
            type='Numeric',
            name='Female age 65 to 66',
            description='The female population between the age of sixty-five years to sixty-six years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_67_to_69 = OBSColumn(
            id='B01001045',
            type='Numeric',
            name='Female age 67 to 69',
            description='The female population between the age of sixty-seven years to sixty-nine years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_70_to_74 = OBSColumn(
            id='B01001046',
            type='Numeric',
            name='Female age 70 to 74',
            description='The female population between the age of seventy years to seventy-four years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_75_to_79 = OBSColumn(
            id='B01001047',
            type='Numeric',
            name='Female age 75 to 79',
            description='The female population between the age of seventy-five years to seventy-nine years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_80_to_84 = OBSColumn(
            id='B01001048',
            type='Numeric',
            name='Female age 80 to 84',
            description='The female population between the age of eighty years to eighty-four years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )
        female_85_and_over = OBSColumn(
            id='B01001049',
            type='Numeric',
            name='Female age 85 and over',
            description='The female population of the age of eighty-five years and over within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people]
        )

        white_including_hispanic = OBSColumn(
            id='B02001002',
            type='Numeric',
            name='White including Hispanic',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['race_ethnicity'], unit_people]
        )
        black_including_hispanic = OBSColumn(
            id='B02001003',
            type='Numeric',
            name='Black including Hispanic',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['race_ethnicity'], unit_people]
        )
        amerindian_including_hispanic = OBSColumn(
            id= 'B02001004',
            type='Numeric',
            name='American Indian including Hispanic',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['race_ethnicity'], unit_people]
        )
        asian_including_hispanic = OBSColumn(
            id='B02001005',
            type='Numeric',
            name='Asian including Hispanic',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['race_ethnicity'], unit_people]
        )

        hispanic_any_race = OBSColumn(
            id='B03001003',
            type='Numeric',
            name='Hispanic of any race',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['race_ethnicity'], unit_people]
        )

        #* men in middle age (45-64)
        male_45_to_64 = OBSColumn(
            id='B15001027',
            type='Numeric',
            name='Men age 45 to 64 ("middle aged")',
            description='The male population between the age of fourty-five years to sixty-four years within the specified area.',
            weight=1,
            aggregate='sum',
            tags=[subsections['age_gender'], unit_people])

        #  - B01001015: 45 To 49 Years
        male_45_to_49 = OBSColumn(
            id='B01001015',
            type='Numeric',
            name='Men age 45 to 49',
            description='The male population between the age of fourty-five years to fourty-nine years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people])

        #  - B01001016: 50 To 54 Years
        male_50_to_54 = OBSColumn(
            id='B01001016',
            type='Numeric',
            name='Men age 50 to 54',
            description='The male population between the age of fifty years to fifty-four years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people])

        #  - B01001017: 55 To 59 Years
        male_55_to_59 = OBSColumn(
            id='B01001017',
            type='Numeric',
            name='Men age 55 to 59',
            description='The male population between the age of fifty-five years to fifty-nine years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people])

        #  - B01001018: 60 and 61 Years
        male_60_61 = OBSColumn(
            id='B01001018',
            type='Numeric',
            name='Men age 60 to 61',
            description='The male population between the age of sixty years to sixty-one years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people])

        #  - B01001019: 62 To 64 Years
        male_62_64 = OBSColumn(
            id='B01001019',
            type='Numeric',
            name='Men age 62 to 64',
            description='The male population between the age of sixty-two years to sixty-four years within the specified area.',
            weight=1,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[subsections['age_gender'], unit_people])

        #  - B01001B012: black, 45 to 54 Years
        black_male_45_54 = OBSColumn(
            id='B01001B012',
            type='Numeric',
            name='Black Men age 45 to 54',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[unit_people])

        #  - B01001B013: black, 55 to 64 Years
        black_male_55_64 = OBSColumn(
            id='B01001B013',
            type='Numeric',
            name='Black Men age 55 to 64',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[unit_people])

        #  - B01001I012: Hispanic, 45 to 54 Years
        hispanic_male_45_54 = OBSColumn(
            id='B01001I012',
            type='Numeric',
            name='Hispanic Men age 45 to 54',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[unit_people])

        #  - B01001I013: Hispanic, 55 to 64 Years
        hispanic_male_55_64 = OBSColumn(
            id='B01001I013',
            type='Numeric',
            name='Hispanic Men age 55 to 64',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[unit_people])

        #  - B01001H012: white non-hispanic, 45 to 54 Years
        white_male_45_54 = OBSColumn(
            id='B01001H012',
            type='Numeric',
            name='White Men age 45 to 54',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[unit_people])

        #  - B01001H013: white non-hispanic, 55 to 64 Years
        white_male_55_64 = OBSColumn(
            id='B01001H013',
            type='Numeric',
            name='White Men age 55 to 64',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[unit_people])

        #  - B01001D012: asian, 45 to 54 Years
        asian_male_45_54 = OBSColumn(
            id='B01001D012',
            type='Numeric',
            name='Asian Men age 45 to 54',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[unit_people])

        #  - B01001D013: asian, 55 to 64 Years
        asian_male_55_64 = OBSColumn(
            id='B01001D013',
            type='Numeric',
            name='Asian Men age 55 to 64',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[unit_people])

        #  - B05013012: foreign born, 45 to 49 Years
        #  - B05013013: foreign born, 50 to 54 Years
        #  - B05013014: foreign born, 55 to 59 Years
        #  - B05013015: foreign born, 60 to 64 Years
        #  - B15001028: less than 9th grade education
        male_45_64_less_than_9_grade = OBSColumn(
            id='B15001028',
            type='Numeric',
            name='Men age 45 to 64 who attained less than a 9th grade education',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[unit_people])

        #  - B15001029: completed between 9th to 12th grade, no diploma
        male_45_64_grade_9_12 = OBSColumn(
            id='B15001029',
            type='Numeric',
            name='Men age 45 to 64 who attained between 9th and 12th grade, no diploma',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[unit_people])
        #  - B15001030: high school graduate including GED
        male_45_64_high_school = OBSColumn(
            id='B15001030',
            type='Numeric',
            name='Men age 45 to 64 who completed high school or obtained GED',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[unit_people])

        #  - B15001031: some college, no degree
        male_45_64_some_college = OBSColumn(
            id='B15001031',
            type='Numeric',
            name='Men age 45 to 64 who completed some college, no degree',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[unit_people])

        #  - B15001032: associate's degree
        male_45_64_associates_degree = OBSColumn(
            id='B15001032',
            type='Numeric',
            name='Men age 45 to 64 who obtained an associate\'s degree',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[unit_people])

        #  - B15001033: bachelor's degree
        male_45_64_bachelors_degree = OBSColumn(
            id='B15001033',
            type='Numeric',
            name='Men age 45 to 64 who obtained a bachelor\'s degree',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[unit_people])

        #  - B15001034: graduate/professional degree
        male_45_64_graduate_degree = OBSColumn(
            id='B15001034',
            type='Numeric',
            name='Men age 45 to 64 who obtained a graduate or professional degree',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[unit_people])

        #  - B17001013: income below poverty, 45 to 54 years
        #  - B17001014: income below poverty, 55 to 64 years
        #  - B17001015: income above poverty, 45 to 54 years
        #  - B17001016: income above poverty, 55 to 64 years
        #  - B23001046: in labor force, 45 to 54 years
        #male_45_64_in_labor_force = OBSColumn(
        #    id='B23001046',
        #    type='Numeric',
        #    name='Men age 45 to 64 who are in the labor force',
        #    description='',
        #    weight=0,
        #    aggregate='sum',
        #    #targets={total_pop: 'denominator'},
        #    tags=[tag_middle_aged_men])

        ##  - B23001047: in armed forces, 45 to 54 years
        ##  - B23001048: in civilian labor force, 45 to 54 years
        ##  - B23001049: employed in civilian labor force, 45 to 54 years
        ##  - B23001050: unemployed in civilian labor force, 45 to 54 years
        #male_45_64_unemployed = OBSColumn(
        #    id='B23001050',
        #    type='Numeric',
        #    name='Men age 45 to 64 who are in the labor force',
        #    description='',
        #    weight=0,
        #    aggregate='sum',
        #    targets={male_45_64_in_labor_force: 'denominator'},
        #    tags=[tag_middle_aged_men])

        ##  - B23001051 not in labor force, 45 to 54 years
        #male_45_64_not_in_labor_force = OBSColumn(
        #    id='B23001051',
        #    type='Numeric',
        #    name='Men age 45 to 64 who are not in the labor force',
        #    description='',
        #    weight=0,
        #    aggregate='sum',
        #    targets={male_45_64_in_labor_force: 'denominator'},
        #    tags=[tag_middle_aged_men])

        # Pitney bowes
        pop_15_and_over = OBSColumn(
            id="B12005001",
            type='Numeric',
            name='Population 15 Years and Over',
            description='The number of people in a geographic area who are over '
            'the age of 15.  This is used mostly as a denominator of marital '
            'status.',
            weight=2,
            aggregate='sum',
            tags=[subsections['families'], subsections['age_gender'], unit_people]
        )
        pop_never_married = OBSColumn(
            id="B12005002",
            targets={pop_15_and_over: 'denominator'},
            type='Numeric',
            name='Never Married',
            description='The number of people in a geographic area who have never been married.',
            weight=2,
            aggregate='sum',
            tags=[subsections['families'], unit_people]
        )
        pop_now_married = OBSColumn(
            id="B12005005",
            targets={pop_15_and_over: 'denominator'},
            type='Numeric',
            name='Currently married',
            description='The number of people in a geographic area who are currently married',
            weight=2,
            aggregate='sum',
            tags=[subsections['families'], unit_people]
        )
        pop_separated = OBSColumn(
            id='B12005008',
            targets={pop_15_and_over: 'denominator'},
            type='Numeric',
            name='Married but separated',
            description='The number of people in a geographic area who are married but separated',
            weight=2,
            aggregate='sum',
            tags=[subsections['families'], unit_people]
        )
        pop_widowed = OBSColumn(
            id='B12005012',
            targets={pop_15_and_over: 'denominator'},
            type='Numeric',
            name='Widowed',
            description='The number of people in a geographic area who are widowed',
            weight=2,
            aggregate='sum',
            tags=[subsections['families'], unit_people]
        )
        pop_divorced = OBSColumn(
            id='B12005015',
            targets={pop_15_and_over: 'denominator'},
            type='Numeric',
            name='Divorced',
            description='The number of people in a geographic area who are divorced',
            weight=2,
            aggregate='sum',
            tags=[subsections['families'], unit_people]
        )

        commuters_16_over = OBSColumn(
            id='B08134001',
            type='Numeric',
            name='Workers age 16 and over who do not work from home',
            description='The number of workers in a geographic area over the age of 16 who do not '
                        'work from home',
            weight=2,
            aggregate='sum',
            tags=[subsections['employment'], subsections['transportation'], unit_people]
        )
        commute_less_10_mins = OBSColumn(
            id='B08134002',
            targets={commuters_16_over: 'denominator'},
            type='Numeric',
            name='Number of workers with less than 10 minute commute',
            description='The number of workers in a geographic area over the age of 16 who do not '
                        'work from home and commute in less than 10 minutes.',
            weight=2,
            aggregate='sum',
            tags=[subsections['employment'], subsections['transportation'], unit_people]
        )
        commute_5_9_mins = OBSColumn(
            id='B08303003',
            type='Numeric',
            name='Number of workers with a commute between 5 and 9 minutes',
            description='The number of workers in a geographic area over the age of 16 who do not '
                        'work from home and commute in between 5 and 9 minutes.',
            weight=1,
            aggregate='sum',
            targets={commuters_16_over: 'denominator'},
            tags=[subsections['employment'], subsections['transportation'], unit_people]
        )
        commute_10_14_mins = OBSColumn(
            id='B08303004',
            targets={commuters_16_over: 'denominator'},
            type='Numeric',
            name='Number of workers with a commute between 10 and 14 minutes',
            description='The number of workers in a geographic area over the age of 16 who do not '
                        'work from home and commute in between 10 and 14 minutes. ',
            weight=2,
            aggregate='sum',
            tags=[subsections['employment'], subsections['transportation'], unit_people]
        )
        commute_15_19_mins = OBSColumn(
            id='B08303005',
            targets={commuters_16_over: 'denominator'},
            type='Numeric',
            name='Number of workers with a commute between 15 and 19 minutes',
            description='The number of workers in a geographic area over the age of 16 who do not '
                        'work from home and commute in between 15 and 19 minutes. ',
            weight=2,
            aggregate='sum',
            tags=[subsections['employment'], subsections['transportation'], unit_people]
        )
        commute_20_24_mins = OBSColumn(
            id='B08303006',
            targets={commuters_16_over: 'denominator'},
            type='Numeric',
            name='Number of workers with a commute between 20 and 24 minutes',
            description='The number of workers in a geographic area over the age of 16 who do not '
                        'work from home and commute in between 20 and 24 minutes.',
            weight=2,
            aggregate='sum',
            tags=[subsections['employment'], subsections['transportation'], unit_people]
        )
        commute_25_29_mins = OBSColumn(
            id='B08303007',
            targets={commuters_16_over: 'denominator'},
            type='Numeric',
            name='Number of workers with a commute between 25 and 29 minutes',
            description='The number of workers in a geographic area over the age of 16 who do not '
                        'work from home and commute in between 25 and 29 minutes. ',
            weight=2,
            aggregate='sum',
            tags=[subsections['employment'], subsections['transportation'], unit_people]
        )
        commute_30_34_mins = OBSColumn(
            id='B08303008',
            targets={commuters_16_over: 'denominator'},
            type='Numeric',
            name='Number of workers with a commute between 30 and 34 minutes',
            description='The number of workers in a geographic area over the age of 16 who do not '
                        'work from home and commute in between 30 and 34 minutes. ',
            weight=2,
            aggregate='sum',
            tags=[subsections['employment'], subsections['transportation'], unit_people]
        )
        commute_35_39_mins = OBSColumn(
            id='B08303009',
            type='Numeric',
            name='Number of workers with a commute between 35 and 39 minutes',
            description='The number of workers in a geographic area over the age of 16 who do not '
                        'work from home and commute in between 35 and 39 minutes. ',
            weight=1,
            aggregate='sum',
            targets={commuters_16_over: 'denominator'},
            tags=[subsections['employment'], subsections['transportation'], unit_people]
        )
        commute_40_44_mins = OBSColumn(
            id='B08303010',
            type='Numeric',
            name='Number of workers with a commute between 40 and 44 minutes',
            description='The number of workers in a geographic area over the age of 16 who do not '
                        'work from home and commute in between 40 and 44 minutes. ',
            weight=1,
            aggregate='sum',
            targets={commuters_16_over: 'denominator'},
            tags=[subsections['employment'], subsections['transportation'], unit_people]
        )
        commute_35_44_mins = OBSColumn(
            id='B08134008',
            targets={commuters_16_over: 'denominator'},
            type='Numeric',
            name='Number of workers with a commute between 35 and 44 minutes',
            description='The number of workers in a geographic area over the age of 16 who do not '
                        'work from home and commute in between 35 and 44 minutes. ',
            weight=2,
            aggregate='sum',
            tags=[subsections['employment'], subsections['transportation'], unit_people]
        )
        commute_45_59_mins = OBSColumn(
            id='B08303011',
            targets={commuters_16_over: 'denominator'},
            type='Numeric',
            name='Number of workers with a commute between 45 and 59 minutes',
            description='The number of workers in a geographic area over the age of 16 who do not '
                        'work from home and commute in between 45 and 59 minutes. ',
            weight=2,
            aggregate='sum',
            tags=[subsections['employment'], subsections['transportation'], unit_people]
        )
        commute_60_more_mins = OBSColumn(
            id='B08134010',
            targets={commuters_16_over: 'denominator'},
            type='Numeric',
            name='Number of workers with a commute of over 60 minutes',
            description='The number of workers in a geographic area over the age of 16 who do not '
                        'work from home and commute in over 60 minutes.',
            weight=2,
            aggregate='sum',
            tags=[subsections['employment'], subsections['transportation'], unit_people]
        )
        commute_60_89_mins = OBSColumn(
            id='B08303012',
            type='Numeric',
            name='Number of workers with a commute between 60 and 89 minutes',
            description='The number of workers in a geographic area over the age of 16 who do not '
                        'work from home and commute in between 60 and 89 minutes .',
            weight=1,
            aggregate='sum',
            targets={commuters_16_over: 'denominator'},
            tags=[subsections['employment'], subsections['transportation'], unit_people]
        )
        commute_90_more_mins = OBSColumn(
            id='B08303013',
            type='Numeric',
            name='Number of workers with a commute of more than 90 minutes',
            description='The number of workers in a geographic area over the age of 16 who do not '
                        'work from home and commute more than 90 minutes.',
            weight=1,
            aggregate='sum',
            targets={commuters_16_over: 'denominator'},
            tags=[subsections['employment'], subsections['transportation'], unit_people]
        )
        aggregate_travel_time_to_work = OBSColumn(
            id='B08135001',
            type='Numeric',
            name='Aggregate travel time to work',
            description='The total number of minutes every worker in a geographic area over the age '
                        'of 16 who did not work from home spent spent '
                        'commuting to work in one day',
            weight=0,
            aggregate='sum',
            targets={commuters_16_over: 'divisor'},
            tags=[subsections['employment'], subsections['transportation']]
        )

        income_less_10000 = OBSColumn(
            id='B19001002',
            type='Numeric',
            aggregate='sum',
            name='Households with income less than $10,000',
            description='The number of households in a geographic area whose '
                        'annual income was less than $10,000.',
            weight=2,
            tags=[subsections['income'], unit_households],
            targets={households: 'denominator'}
        )
        income_10000_14999 = OBSColumn(
            id='B19001003',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $10,000 to $14,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $10,000 and $14,999.',
            weight=2,
            tags=[subsections['income'], unit_households],
            targets={households: 'denominator'}
        )
        income_15000_19999 = OBSColumn(
            id='B19001004',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $15,000 to $19,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $15,000 and $19,999.',
            weight=2,
            tags=[subsections['income'], unit_households],
            targets={households: 'denominator'}
        )
        income_20000_24999 = OBSColumn(
            id='B19001005',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $20,000 to $24,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $20,000 and $24,999.',
            weight=2,
            tags=[subsections['income'], unit_households],
            targets={households: 'denominator'}
        )
        income_25000_29999 = OBSColumn(
            id='B19001006',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $25,000 to $29,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $20,000 and $24,999.',
            weight=2,
            tags=[subsections['income'], unit_households],
            targets={households: 'denominator'}
        )
        income_30000_34999 = OBSColumn(
            id='B19001007',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $30,000 to $34,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $30,000 and $34,999.',
            weight=2,
            tags=[subsections['income'], unit_households],
            targets={households: 'denominator'}
        )
        income_35000_39999 = OBSColumn(
            id='B19001008',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $35,000 to $39,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $35,000 and $39,999.',
            weight=2,
            tags=[subsections['income'], unit_households],
            targets={households: 'denominator'}
        )
        income_40000_44999 = OBSColumn(
            id='B19001009',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $40,000 to $44,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $40,000 and $44,999.',
            weight=2,
            tags=[subsections['income'], unit_households],
            targets={households: 'denominator'}
        )
        income_45000_49999 = OBSColumn(
            id='B19001010',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $45,000 to $49,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $45,000 and $49,999.',
            weight=2,
            tags=[subsections['income'], unit_households],
            targets={households: 'denominator'}
        )
        income_50000_59999 = OBSColumn(
            id='B19001011',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $50,000 to $59,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $50,000 and $59,999.',
            weight=2,
            tags=[subsections['income'], unit_households],
            targets={households: 'denominator'}
        )
        income_60000_74999 = OBSColumn(
            id='B19001012',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $60,000 to $74,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $60,000 and $74,999.',
            weight=2,
            tags=[subsections['income'], unit_households],
            targets={households: 'denominator'}
        )
        income_75000_99999 = OBSColumn(
            id='B19001013',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $75,000 to $99,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $75,000 and $99,999.',
            weight=2,
            tags=[subsections['income'], unit_households],
            targets={households: 'denominator'}
        )
        income_100000_124999 = OBSColumn(
            id='B19001014',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $100,000 to $124,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $100,000 and $124,999.',
            weight=2,
            tags=[subsections['income'], unit_households],
            targets={households: 'denominator'}
        )
        income_125000_149999 = OBSColumn(
            id='B19001015',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $125,000 to $149,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $125,000 and $149,999.',
            weight=2,
            tags=[subsections['income'], unit_households],
            targets={households: 'denominator'}
        )
        income_150000_199999 = OBSColumn(
            id='B19001016',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $150,000 to $199,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $150,000 and $1999,999.',
            weight=2,
            tags=[subsections['income'], unit_households],
            targets={households: 'denominator'}
        )
        income_200000_or_more = OBSColumn(
            id='B19001017',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $200,000 Or More',
            description='The number of households in a geographic area whose '
                        'annual income was more than $200,000.',
            weight=2,
            tags=[subsections['income'], unit_households],
            targets={households: 'denominator'}
        )
        #TODO
        #average_travel_time_to_work = OBSColumn(
        #    id='B08135001',
        #    aggregate='sum'
        #)

        households_public_asst_or_food_stamps = OBSColumn(
            id='B19058002',
            type='Numeric',
            aggregate='sum',
            weight=0,
            name='Households on cash public assistance or receiving food stamps (SNAP)',
            targets={households: 'denominator'},
            tags=[subsections['income'], unit_households],
        )

        households_retirement_income = OBSColumn(
            id='B19059002',
            type='Numeric',
            aggregate='sum',
            weight=0,
            name='Households receiving retirement income',
            targets={households: 'denominator'},
            tags=[subsections['income'], unit_households],
        )

        renter_occupied_housing_units_paying_cash_median_gross_rent = OBSColumn(
            id='B25064001',
            type='Numeric',
            aggregate='sum',
            name='Renter-Occupied Housing Units Paying Cash Rent Median Gross Rent',
            description='',
            weight=0,
            tags=[subsections['housing'], unit_housing],
        )

        owner_occupied_housing_units_lower_value_quartile = OBSColumn(
            id='B25076001',
            type='Numeric',
            name='Owner-Occupied Housing Units Lower Value Quartile',
            description='',
            weight=0,
            tags=[subsections['housing'], unit_housing],
        )

        owner_occupied_housing_units_median_value = OBSColumn(
            id='B25077001',
            type='Numeric',
            aggregate='median',
            name='Owner-Occupied Housing Units Median Value',
            description='The middle value (median) in a geographic area owner occupied housing units.',
            weight=1,
            targets={owner_occupied_housing_units: 'universe'},
            tags=[subsections['housing'], unit_housing],
        )

        owner_occupied_housing_units_upper_value_quartile = OBSColumn(
            id='B25078001',
            type='Numeric',
            name='Owner-Occupied Housing Units Upper Value Quartile',
            description='',
            weight=0,
            tags=[subsections['housing'], unit_housing],
        )

        population_1_year_and_over = OBSColumn(
            id='B07204001',
            type='Numeric',
            name='Population 1 year and over',
            description='All people, male and female, child and adult, living in a given geographic area that are 1 year and older.',
            weight=1,
            aggregate='sum',
            tags=[subsections['age_gender'], unit_people],
        )
        different_house_year_ago_same_city = OBSColumn(
            id='B07204004',
            type='Numeric',
            name='Lived in a different house one year ago in the same city',
            description='All people in a geographic area who lived in the same city but moved to a different unit within the year prior to the survey.',
            weight=1,
            targets={population_1_year_and_over: 'denominator'},
            aggregate='sum',
            tags=[subsections['housing'], unit_people],
        )
        different_house_year_ago_different_city = OBSColumn(
            id='B07204007',
            type='Numeric',
            name='Lived in a different house one year ago in a different city',
            description='All people in a geographic area who lived in a different city within the year prior to the survey.',
            weight=1,
            aggregate='sum',
            targets={population_1_year_and_over: 'denominator'},
            tags=[subsections['housing'], unit_people],
        )

        group_quarters = OBSColumn(
            id='B26001001',
            type='Numeric',
            name='Population living in group quarters',
            description='',
            aggregate='sum',
            weight=0,
            tags=[subsections['housing'], unit_people],
        )
        no_car = OBSColumn(
            id='B08014002',
            type='Numeric',
            name='Workers age 16 and over with no vehicle',
            description='All people in a geographic area over the age of 16 who do not own a car.',
            weight=1,
            aggregate='sum',
            targets={workers_16_and_over: 'denominator'},
            tags=[subsections['transportation'], unit_people],
        )

        columns = OrderedDict([
            ("rent_burden_not_computed", rent_burden_not_computed),
            ("rent_over_50_percent", rent_over_50_percent),
            ("rent_40_to_50_percent", rent_40_to_50_percent),
            ("rent_35_to_40_percent", rent_35_to_40_percent),
            ("rent_30_to_35_percent", rent_30_to_35_percent),
            ("rent_25_to_30_percent", rent_25_to_30_percent),
            ("rent_20_to_25_percent", rent_20_to_25_percent),
            ("rent_15_to_20_percent", rent_15_to_20_percent),
            ("rent_10_to_15_percent", rent_10_to_15_percent),
            ("rent_under_10_percent", rent_under_10_percent),
            ("total_pop", total_pop),
            ("male_pop", male_pop),
            ("female_pop", female_pop),
            ("median_age", median_age),
            ("white_pop", white_pop),
            ("black_pop", black_pop),
            ("asian_pop", asian_pop),
            ("hispanic_pop", hispanic_pop),
            ("amerindian_pop", amerindian_pop),
            ("other_race_pop", other_race_pop),
            ("two_or_more_races_pop", two_or_more_races_pop),
            ("not_hispanic_pop", not_hispanic_pop),
            ("not_us_citizen_pop", not_us_citizen_pop),
            ("workers_16_and_over", workers_16_and_over),
            ("commuters_by_car_truck_van", commuters_by_car_truck_van),
            ('commuters_drove_alone', commuters_drove_alone),
            ('no_cars', no_cars),
            ('one_car', one_car),
            ('two_cars', two_cars),
            ('three_cars', three_cars),
            ('four_more_cars', four_more_cars),
            ('commuters_by_carpool', commuters_by_carpool),
            ("commuters_by_public_transportation", commuters_by_public_transportation),
            ("commuters_by_bus", commuters_by_bus),
            ("commuters_by_subway_or_elevated", commuters_by_subway_or_elevated),
            ("walked_to_work", walked_to_work),
            ("worked_at_home", worked_at_home),
            ("children", children),
            ("households", households),
            ("population_3_years_over", population_3_years_over),
            ("in_school", in_school),
            ("in_grades_1_to_4", in_grades_1_to_4),
            ("in_grades_5_to_8", in_grades_5_to_8),
            ("in_grades_9_to_12", in_grades_9_to_12),
            ("in_undergrad_college", in_undergrad_college),
            ("pop_25_years_over", pop_25_years_over),
            ("high_school_diploma", high_school_diploma),
            ("less_one_year_college", less_one_year_college),
            ("one_year_more_college", one_year_more_college),
            ("associates_degree", associates_degree),
            ("bachelors_degree", bachelors_degree),
            ("masters_degree", masters_degree),
            ("pop_5_years_over", pop_5_years_over),
            ("speak_only_english_at_home", speak_only_english_at_home),
            ("speak_spanish_at_home", speak_spanish_at_home),
            ("pop_determined_poverty_status", pop_determined_poverty_status),
            ("poverty", poverty),
            ("median_income", median_income),
            ("gini_index", gini_index),
            ("income_per_capita", income_per_capita),
            ("housing_units", housing_units),
            ("vacant_housing_units", vacant_housing_units),
            ("vacant_housing_units_for_rent", vacant_housing_units_for_rent),
            ("vacant_housing_units_for_sale", vacant_housing_units_for_sale),
            ("median_rent", median_rent),
            ("percent_income_spent_on_rent", percent_income_spent_on_rent),
            ("owner_occupied_housing_units", owner_occupied_housing_units),
            ("million_dollar_housing_units", million_dollar_housing_units),
            ("mortgaged_housing_units", mortgaged_housing_units),
            ("families_with_young_children", families_with_young_children),
            ("two_parent_families_with_young_children", two_parent_families_with_young_children),
            ("two_parents_in_labor_force_families_with_young_children",
             two_parents_in_labor_force_families_with_young_children),
            ("two_parents_father_in_labor_force_families_with_young_children",
             two_parents_father_in_labor_force_families_with_young_children),
            ("two_parents_mother_in_labor_force_families_with_young_children",
             two_parents_mother_in_labor_force_families_with_young_children),
            ("two_parents_not_in_labor_force_families_with_young_children",
             two_parents_not_in_labor_force_families_with_young_children),
            ("one_parent_families_with_young_children",
             one_parent_families_with_young_children),
            ("father_one_parent_families_with_young_children",
             father_one_parent_families_with_young_children),
            ('pop_16_over', pop_16_over),
            ('pop_in_labor_force', pop_in_labor_force),
            ('civilian_labor_force', civilian_labor_force),
            ('employed_pop', employed_pop),
            ('unemployed_pop', unemployed_pop),
            ('armed_forces', armed_forces),
            ('not_in_labor_force', not_in_labor_force),
            ('black_male_45_54', black_male_45_54),
            ('black_male_55_64', black_male_55_64),
            ('hispanic_male_45_54', hispanic_male_45_54),
            ('hispanic_male_55_64', hispanic_male_55_64),
            ('white_male_45_54', white_male_45_54),
            ('white_male_55_64', white_male_55_64),
            ('asian_male_45_54', asian_male_45_54),
            ('asian_male_55_64', asian_male_55_64),
            ('male_45_64_less_than_9_grade', male_45_64_less_than_9_grade),
            ('male_45_64_grade_9_12', male_45_64_grade_9_12),
            ('male_45_64_high_school', male_45_64_high_school),
            ('male_45_64_some_college', male_45_64_some_college),
            ('male_45_64_associates_degree', male_45_64_associates_degree),
            ('male_45_64_bachelors_degree', male_45_64_bachelors_degree),
            ('male_45_64_graduate_degree', male_45_64_graduate_degree),
            ("two_parent_families_with_young_children", two_parent_families_with_young_children),
            ("two_parents_in_labor_force_families_with_young_children",
             two_parents_in_labor_force_families_with_young_children),
            ("two_parents_father_in_labor_force_families_with_young_children",
             two_parents_father_in_labor_force_families_with_young_children),
            ("two_parents_mother_in_labor_force_families_with_young_children",
             two_parents_mother_in_labor_force_families_with_young_children),
            ("two_parents_not_in_labor_force_families_with_young_children",
             two_parents_not_in_labor_force_families_with_young_children),
            ("one_parent_families_with_young_children",
             one_parent_families_with_young_children),
            ("father_one_parent_families_with_young_children",
             father_one_parent_families_with_young_children),
            ("father_in_labor_force_one_parent_families_with_young_children",
             father_in_labor_force_one_parent_families_with_young_children),
            ("pop_15_and_over", pop_15_and_over),
            ("pop_never_married", pop_never_married),
            ("pop_now_married", pop_now_married),
            ("pop_separated", pop_separated),
            ("pop_widowed", pop_widowed),
            ("pop_divorced", pop_divorced),
            ("commuters_16_over", commuters_16_over),
            ("commute_less_10_mins", commute_less_10_mins),
            ("commute_10_14_mins", commute_10_14_mins),
            ("commute_15_19_mins", commute_15_19_mins),
            ("commute_20_24_mins", commute_20_24_mins),
            ("commute_25_29_mins", commute_25_29_mins),
            ("commute_30_34_mins", commute_30_34_mins),
            ("commute_35_44_mins", commute_35_44_mins),
            ("commute_45_59_mins", commute_45_59_mins),
            ("commute_60_more_mins", commute_60_more_mins),
            ("aggregate_travel_time_to_work", aggregate_travel_time_to_work),
            ("income_less_10000", income_less_10000),
            ("income_10000_14999", income_10000_14999),
            ("income_15000_19999", income_15000_19999),
            ("income_20000_24999", income_20000_24999),
            ("income_25000_29999", income_25000_29999),
            ("income_30000_34999", income_30000_34999),
            ("income_35000_39999", income_35000_39999),
            ("income_40000_44999", income_40000_44999),
            ("income_45000_49999", income_45000_49999),
            ("income_50000_59999", income_50000_59999),
            ("income_60000_74999", income_60000_74999),
            ("income_75000_99999", income_75000_99999),
            ("income_100000_124999", income_100000_124999),
            ("income_125000_149999", income_125000_149999),
            ("income_150000_199999", income_150000_199999),
            ("income_200000_or_more", income_200000_or_more),
            ("renter_occupied_housing_units_paying_cash_median_gross_rent",
             renter_occupied_housing_units_paying_cash_median_gross_rent),
            ("owner_occupied_housing_units_lower_value_quartile",
             owner_occupied_housing_units_lower_value_quartile),
            ("owner_occupied_housing_units_median_value",
             owner_occupied_housing_units_median_value),
            ("owner_occupied_housing_units_upper_value_quartile",
             owner_occupied_housing_units_upper_value_quartile),
            ('less_than_high_school_graduate', less_than_high_school_graduate),
            ('high_school_including_ged', high_school_including_ged),
            ('some_college_and_associates_degree', some_college_and_associates_degree),
            ('bachelors_degree_2', bachelors_degree_2),
            ('graduate_professional_degree', graduate_professional_degree),
            ('children_in_single_female_hh', children_in_single_female_hh),
            ('married_households', married_households),
            ('male_male_households', male_male_households),
            ('female_female_households', female_female_households),
            ('occupied_housing_units', occupied_housing_units),
            ('children_in_single_female_hh', children_in_single_female_hh),
            ('married_households', married_households),
            ('male_male_households', male_male_households),
            ('female_female_households', female_female_households),
            ('some_college_and_associates_degree', some_college_and_associates_degree),
            ('speak_spanish_at_home_low_english', speak_spanish_at_home_low_english),
            ('housing_units_renter_occupied', housing_units_renter_occupied),
            ('dwellings_1_units_detached', dwellings_1_units_detached),
            ('dwellings_1_units_attached', dwellings_1_units_attached),
            ('dwellings_2_units', dwellings_2_units),
            ('dwellings_3_to_4_units', dwellings_3_to_4_units),
            ('dwellings_5_to_9_units', dwellings_5_to_9_units),
            ('dwellings_10_to_19_units', dwellings_10_to_19_units),
            ('dwellings_20_to_49_units', dwellings_20_to_49_units),
            ('dwellings_50_or_more_units', dwellings_50_or_more_units),
            ('mobile_homes', mobile_homes),
            ('housing_built_2005_or_later', housing_built_2005_or_later),
            ('housing_built_2000_to_2004', housing_built_2000_to_2004),
            ('housing_built_1939_or_earlier', housing_built_1939_or_earlier),
            ('two_parent_families_with_young_children', two_parent_families_with_young_children),
            ('employed_agriculture_forestry_fishing_hunting_mining',
             employed_agriculture_forestry_fishing_hunting_mining),
            ('employed_construction', employed_construction),
            ('employed_manufacturing', employed_manufacturing),
            ('employed_wholesale_trade', employed_wholesale_trade),
            ('employed_retail_trade', employed_retail_trade),
            ('employed_transportation_warehousing_utilities',
             employed_transportation_warehousing_utilities),
            ('employed_information', employed_information),
            ('employed_finance_insurance_real_estate', employed_finance_insurance_real_estate),
            ('employed_science_management_admin_waste', employed_science_management_admin_waste),
            ('employed_education_health_social', employed_education_health_social),
            ('employed_arts_entertainment_recreation_accommodation_food',
             employed_arts_entertainment_recreation_accommodation_food),
            ('employed_other_services_not_public_admin', employed_other_services_not_public_admin),
            ('employed_public_administration', employed_public_administration),
            ('occupation_management_arts', occupation_management_arts),
            ('occupation_services', occupation_services),
            ('occupation_sales_office', occupation_sales_office),
            ('occupation_natural_resources_construction_maintenance',
             occupation_natural_resources_construction_maintenance),
            ('occupation_production_transportation_material',
             occupation_production_transportation_material),
            ('male_under_5', male_under_5),
            ('male_5_to_9', male_5_to_9),
            ('male_10_to_14', male_10_to_14),
            ('male_15_to_17', male_15_to_17),
            ('male_18_to_19', male_18_to_19),
            ('male_20', male_20),
            ('male_21', male_21),
            ('male_22_to_24', male_22_to_24),
            ('male_25_to_29', male_25_to_29),
            ('male_30_to_34', male_30_to_34),
            ('male_35_to_39', male_35_to_39),
            ('male_40_to_44', male_40_to_44),
            ('male_45_to_64', male_45_to_64),
            ('male_45_to_49', male_45_to_49),
            ('male_50_to_54', male_50_to_54),
            ('male_55_to_59', male_55_to_59),
            ('male_60_61', male_60_61),
            ('male_62_64', male_62_64),
            ('male_65_to_66', male_65_to_66),
            ('male_67_to_69', male_67_to_69),
            ('male_70_to_74', male_70_to_74),
            ('male_75_to_79', male_75_to_79),
            ('male_80_to_84', male_80_to_84),
            ('male_85_and_over', male_85_and_over),
            ('female_under_5', female_under_5),
            ('female_5_to_9', female_5_to_9),
            ('female_10_to_14', female_10_to_14),
            ('female_15_to_17', female_15_to_17),
            ('female_18_to_19', female_18_to_19),
            ('female_20', female_20),
            ('female_21', female_21),
            ('female_22_to_24', female_22_to_24),
            ('female_25_to_29', female_25_to_29),
            ('female_30_to_34', female_30_to_34),
            ('female_35_to_39', female_35_to_39),
            ('female_40_to_44', female_40_to_44),
            ('female_45_to_49', female_45_to_49),
            ('female_50_to_54', female_50_to_54),
            ('female_55_to_59', female_55_to_59),
            ('female_60_to_61', female_60_to_61),
            ('female_62_to_64', female_62_to_64),
            ('female_65_to_66', female_65_to_66),
            ('female_67_to_69', female_67_to_69),
            ('female_70_to_74', female_70_to_74),
            ('female_75_to_79', female_75_to_79),
            ('female_80_to_84', female_80_to_84),
            ('female_85_and_over', female_85_and_over),
            ('white_including_hispanic', white_including_hispanic),
            ('black_including_hispanic', black_including_hispanic),
            ('amerindian_including_hispanic', amerindian_including_hispanic),
            ('asian_including_hispanic', asian_including_hispanic),
            ('hispanic_any_race', hispanic_any_race),
            ('commute_5_9_mins', commute_5_9_mins),
            ('commute_35_39_mins', commute_35_39_mins),
            ('commute_40_44_mins', commute_40_44_mins),
            ('commute_60_89_mins', commute_60_89_mins),
            ('commute_90_more_mins', commute_90_more_mins),
            ('households_public_asst_or_food_stamps', households_public_asst_or_food_stamps),
            ('households_retirement_income', households_retirement_income),
            ('renter_occupied_housing_units_paying_cash_median_gross_rent',
             renter_occupied_housing_units_paying_cash_median_gross_rent),
            ('owner_occupied_housing_units_lower_value_quartile',
             owner_occupied_housing_units_lower_value_quartile),
            ('owner_occupied_housing_units_median_value',
             owner_occupied_housing_units_median_value),
            ('owner_occupied_housing_units_upper_value_quartile',
             owner_occupied_housing_units_upper_value_quartile),
            ('population_1_year_and_over', population_1_year_and_over),
            ('different_house_year_ago_same_city', different_house_year_ago_same_city),
            ('different_house_year_ago_different_city', different_house_year_ago_different_city),
            ('group_quarters', group_quarters),
            ('no_car', no_car),
        ])
        united_states_section = input_['sections']['united_states']
        acs_source = input_['censustags']['acs']
        no_restrictions = input_['license']['no-restrictions']
        for _, col in columns.iteritems():
            col.tags.append(united_states_section)
            col.tags.append(acs_source)
            col.tags.append(no_restrictions)
        return columns


class DownloadACS(LoadPostgresFromURL):

    # http://censusreporter.tumblr.com/post/73727555158/easier-access-to-acs-data
    url_template = 'https://s3.amazonaws.com/census-backup/acs/{year}/' \
            'acs{year}_{sample}/acs{year}_{sample}_backup.sql.gz'

    year = Parameter()
    sample = Parameter()

    @property
    def schema(self):
        return 'acs{year}_{sample}'.format(year=self.year, sample=self.sample)

    def run(self):
        cursor = current_session()
        cursor.execute('DROP SCHEMA IF EXISTS {schema} CASCADE'.format(schema=self.schema))
        self.load_from_url(self.url_template.format(year=self.year, sample=self.sample))


class QuantileColumns(ColumnsTask):

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'censustags': ACSTags(),
            'segmenttags': SegmentTags(),
            'unittags': UnitTags(),
            'license': LicenseTags(),
            'columns': Columns(),
        }

    def version(self):
        return 7

    def columns(self):
        quantile_columns = OrderedDict()
        input_ = self.input()
        for colname, coltarget in input_['columns'].iteritems():
            col = coltarget.get(current_session())
            quantile_columns[colname+'_quantile'] = OBSColumn(
                id=col.id.split('.')[-1]+'_quantile',
                type='Numeric',
                name='Quantile:'+col.name,
                description=col.description,
                aggregate='quantile',
                targets={col: 'quantile_source'},
                tags=[input_['license']['no-restrictions'], input_['censustags']['acs'],
                      input_['sections']['united_states']],
                weight=1
            )
        return quantile_columns



class Quantiles(TableTask):
    '''
    Calculate the quantiles for each ACS column
    '''

    year = Parameter()
    sample = Parameter()
    geography = Parameter()

    def requires(self):
        return {
            'columns' : QuantileColumns(),
            'table'   : Extract(year=self.year,
                                sample=self.sample,
                                geography=self.geography),
            'tiger'   : GeoidColumns()
        }

    def version(self):
        return 10

    def columns(self):
        input_ = self.input()
        columns = OrderedDict({
            'geoid': input_['tiger'][self.geography + '_geoid']
        })
        columns.update(input_['columns'])
        return columns

    def timespan(self):
        sample = int(self.sample[0])
        return '{start} - {end}'.format(start=int(self.year) - sample + 1,
                                        end=int(self.year))

    def populate(self):
        connection = current_session()
        input_ = self.input()
        quant_col_names = input_['columns'].keys()
        old_col_names = [name.split("_quantile")[0]
                         for name in quant_col_names]

        insert = True
        for cols in grouper(zip(quant_col_names, old_col_names), 20):
            selects = [" percent_rank() OVER (ORDER BY {old_col} ASC) as {old_col} ".format(old_col=c[1])
                       for c in cols if c is not None]

            insert_statment = ", ".join([c[0] for c in cols if c is not None])
            old_cols_statment = ", ".join([c[1] for c in cols if c is not None])
            select_statment = ", ".join(selects)
            before = time()
            if insert:
                stmt = '''
                    INSERT INTO {table}
                    (geoid, {insert_statment})
                    SELECT geoid, {select_statment}
                    FROM {source_table}
                '''.format(
                    table        = self.output().table,
                    insert_statment = insert_statment,
                    select_statment = select_statment,
                    source_table = input_['table'].table
                )
                insert = False
            else:
                stmt = '''
                    WITH data as (
                        SELECT geoid, {select_statment}
                        FROM {source_table}
                    )
                    UPDATE {table} SET ({insert_statment}) = ({old_cols_statment})
                    FROM data
                    WHERE data.geoid = {table}.geoid
                '''.format(
                    table        = self.output().table,
                    insert_statment = insert_statment,
                    select_statment = select_statment,
                    old_cols_statment = old_cols_statment,
                    source_table = input_['table'].table
                )
            connection.execute(stmt)
            after = time()
            LOGGER.info('quantile calculation time taken : %s', int(after - before))


class Extract(TableTask):
    '''
    Generate an extract of important ACS columns for CartoDB
    '''

    year = Parameter()
    sample = Parameter()
    geography = Parameter()

    def version(self):
        return 10

    def requires(self):
        return {
            'acs': Columns(),
            'tiger': GeoidColumns(),
            'data': DownloadACS(year=self.year, sample=self.sample),
            'tigerdata': SumLevel(geography=self.geography, year='2014')
        }

    def timespan(self):
        sample = int(self.sample[0])
        return '{start} - {end}'.format(start=int(self.year) - sample + 1,
                                        end=int(self.year))

    def columns(self):
        input_ = self.input()
        cols = OrderedDict([
            ('geoid', input_['tiger'][self.geography + '_geoid']),
        ])
        for colkey, col in input_['acs'].iteritems():
            cols[colkey] = col
        return cols

    def populate(self):
        '''
        load relevant columns from underlying census tables
        '''
        session = current_session()
        sumlevel = SUMLEVELS_BY_SLUG[self.geography]['summary_level']
        colids = []
        colnames = []
        tableids = set()
        inputschema = 'acs{year}_{sample}'.format(year=self.year, sample=self.sample)
        for colname, coltarget in self.columns().iteritems():
            colid = coltarget.get(session).id
            tableid = colid.split('.')[-1][0:-3]
            if colid.endswith('geoid'):
                colids.append('SUBSTR(geoid, 8)')
            else:
                colid = coltarget._id.split('.')[-1]
                resp = session.execute('SELECT COUNT(*) FROM information_schema.columns '
                                       "WHERE table_schema = '{inputschema}'  "
                                       "  AND table_name ILIKE '{inputtable}' "
                                       "  AND column_name ILIKE '{colid}' ".format(
                                           inputschema=inputschema,
                                           inputtable=tableid,
                                           colid=colid))
                if int(resp.fetchone()[0]) == 0:
                    continue
                colids.append(colid)
                tableids.add(tableid)
            colnames.append(colname)

        tableclause = '{inputschema}.{inputtable} '.format(
            inputschema=inputschema, inputtable=tableids.pop())
        for tableid in tableids:
            resp = session.execute('SELECT COUNT(*) FROM information_schema.tables '
                                   "WHERE table_schema = '{inputschema}'  "
                                   "  AND table_name ILIKE '{inputtable}' ".format(
                                       inputschema=inputschema,
                                       inputtable=tableid))
            if int(resp.fetchone()[0]) > 0:
                tableclause += ' JOIN {inputschema}.{inputtable} ' \
                               ' USING (geoid) '.format(inputschema=inputschema,
                                                        inputtable=tableid)
        table_id = self.output().table
        session.execute('INSERT INTO {output} ({colnames}) '
                        '  SELECT {colids} '
                        '  FROM {tableclause} '
                        '  WHERE geoid LIKE :sumlevelprefix '
                        ''.format(
                            output=table_id,
                            colnames=', '.join(colnames),
                            colids=', '.join(colids),
                            tableclause=tableclause
                        ), {
                            'sumlevelprefix': sumlevel + '00US%'
                        })


class ExtractAll(WrapperTask):
    year = Parameter()
    sample = Parameter()

    def requires(self):
        geographies = set(['state', 'county', 'census_tract', 'block_group',
                           'puma', 'zcta5', 'school_district_elementary',
                           'congressional_district',
                           'school_district_secondary',
                           'school_district_unified', 'cbsa', 'place'])
        if self.year == '2010':
            geographies.remove('zcta5')
        for geo in geographies:
            yield Quantiles(geography=geo, year=self.year, sample=self.sample)

class ACSMetaWrapper(MetaWrapper):

    geography = Parameter()
    year = Parameter()
    sample = Parameter()

    params = {
        'geography':['state', 'county', 'census_tract', 'block_group',
                           'puma', 'zcta5', 'school_district_elementary',
                           'congressional_district',
                           'school_district_secondary',
                           'school_district_unified', 'cbsa', 'place'],
        'year': ['2015', '2010'],
        'sample': ['5yr','1yr']
    }

    def tables(self):
        # no ZCTA for 2010
        if self.year == '2010' and self.geography == 'zcta5':
            pass
        # 1yr sample doesn't have block group or census_tract
        elif self.sample == '1yr' and self.geography in (
            'census_tract', 'block_group', 'zcta5'):
            pass
        else:
            yield Quantiles(geography=self.geography, year=self.year, sample=self.sample)
