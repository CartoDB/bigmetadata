#!/usr/bin/env python

'''
Bigmetadata tasks

tasks to download and create metadata
'''

#import requests
#import datetime
#import json
import csv
import json
import os
from collections import OrderedDict
from sqlalchemy import Column, Numeric, Text
from luigi import Parameter, BooleanParameter, Task, WrapperTask, LocalTarget
from psycopg2 import ProgrammingError

from tasks.util import (LoadPostgresFromURL, classpath, pg_cursor, shell,
                        CartoDBTarget, get_logger, underscore_slugify, TableTask,
                        ColumnTarget, ColumnsTask, TagsTask)
from tasks.us.census.tiger import load_sumlevels, SumLevel
from tasks.us.census.tiger import (SUMLEVELS, load_sumlevels, GeoidColumns,
                                   SUMLEVELS_BY_SLUG, ShorelineClipTiger)
from tasks.us.census.segments import SegmentTags

from tasks.meta import (OBSColumn, OBSTag, OBSColumnTable, current_session)
from tasks.tags import CategoryTags

LOGGER = get_logger(__name__)


class ACSTags(TagsTask):

    def version(self):
        return 0

    def tags(self):
        return [
            OBSTag(id='demographics',
                   name='US American Community Survey Demographics',
                   type='catalog',
                   description='Standard Demographic Data from the US American Community Survey')
        ]


class Columns(ColumnsTask):

    def requires(self):
        return {
            'tags': CategoryTags(),
            'censustags': ACSTags(),
            'segmenttags': SegmentTags()
        }

    def version(self):
        return 1


    def columns(self):
        tags = self.input()['tags']
        censustags = self.input()['censustags']
        segmenttags = self.input()['segmenttags']
        tag_middle_aged_men = segmenttags['middle_aged_men']
        tag_families_with_young_children = segmenttags['families_with_young_children']
        total_pop = OBSColumn(
            id='B01001001',
            type='Numeric',
            name="Total Population",
            description='The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.',
            aggregate='sum',
            weight=10,
            tags=[censustags['demographics'], tags['denominator'], tags['population']]
        )
        male_pop = OBSColumn(
            id='B01001002',
            type='Numeric',
            name="Male Population",
            description="The number of people within each geography who are male.",
            aggregate='sum',
            weight=8,
            targets={total_pop: 'denominator'},
            tags=[censustags['demographics'], tags['population']]
        )
        female_pop = OBSColumn(
            id='B01001026',
            type='Numeric',
            name="Female Population",
            description='The number of people within each geography who are female.',
            aggregate='sum',
            weight=8,
            targets={total_pop: 'denominator'},
            tags=[censustags['demographics'], tags['population']]
        )
        median_age = OBSColumn(
            id='B01002001',
            type='Numeric',
            name="Median Age",
            description="The median age of all people in a given geographic area.",
            aggregate='median',
            weight=2,
            tags=[censustags['demographics'], tags['population']]
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
            tags=[censustags['demographics'], tags['population'], tags['race_age_gender']]
        )
        black_pop = OBSColumn(
            id='B03002004',
            type='Numeric',
            name='Black or African American Population',
            description="The number of people identifying as black or African American, non-Hispanic in each geography.",
            aggregate='sum',
            weight=7,
            targets={total_pop: 'denominator'},
            tags=[censustags['demographics'], tags['population'], tags['race_age_gender']]
        )
        amerindian_pop = OBSColumn(
            id='B03002005',
            type='Numeric',
            name='American Indian and Alaska Native Population',
            description="The number of people identifying as American Indian or Alaska native in each geography.",
            aggregate='sum',
            weight=0,
            targets={total_pop: 'denominator'},
            tags=[censustags['demographics'], tags['population'], tags['race_age_gender']]
        )
        asian_pop = OBSColumn(
            id='B03002006',
            type='Numeric',
            name='Asian Population',
            description="The number of people identifying as Asian, non-Hispanic in each geography.",
            aggregate='sum',
            weight=7,
            targets={total_pop: 'denominator'},
            tags=[censustags['demographics'], tags['population'], tags['race_age_gender']]
        )
        other_race_pop = OBSColumn(
            id='B03002008',
            type='Numeric',
            name='Other Race population',
            description="The number of people identifying as another race in each geography",
            aggregate='sum',
            weight=0,
            targets={total_pop: 'denominator'},
            tags=[censustags['demographics'], tags['population'], tags['race_age_gender']]
        )
        two_or_more_races_pop = OBSColumn(
            id='B03002009',
            type='Numeric',
            name='Two or more races population',
            description="The number of people identifying as two or more races in each geography",
            aggregate='sum',
            weight=0,
            targets={total_pop: 'denominator'},
            tags=[censustags['demographics'], tags['population'], tags['race_age_gender']]
        )
        not_hispanic_pop = OBSColumn(
            id='B03002002',
            type='Numeric',
            name='Population not Hispanic',
            description="The number of people not identifying as Hispanic or Latino in each geography.",
            aggregate='sum',
            weight=0,
            targets={total_pop: 'denominator'},
            tags=[censustags['demographics'], tags['population'], tags['race_age_gender']]
        )
        hispanic_pop = OBSColumn(
            id='B03002012',
            type='Numeric',
            name='Hispanic Population',
            description="The number of people identifying as Hispanic or Latino in each geography.",
            aggregate='sum',
            weight=7,
            targets={total_pop: 'denominator'},
            tags=[censustags['demographics'], tags['population'], tags['race_age_gender']]
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
            tags=[censustags['demographics'], tags['population']]
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
            tags=[censustags['demographics'], tags['denominator'],
                  tags['income_education_employment']]
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
            tags=[censustags['demographics'], tags['transportation']])
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
            tags=[censustags['demographics'], tags['transportation']])
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
            tags=[censustags['demographics'], tags['transportation']])
        commuters_by_public_transportation = OBSColumn(
            id='B08006008',
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
            tags=[censustags['demographics'], tags['transportation']])
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
            tags=[censustags['demographics'], tags['transportation']])
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
            tags=[censustags['demographics'], tags['transportation']])
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
            targets={commuters_by_public_transportation: 'denominator'},
            tags=[censustags['demographics'], tags['transportation']])
        worked_at_home = OBSColumn(
            id='B08006017',
            type='Numeric',
            name='Worked at Home',
            description='The count within a geographical area of workers over '
            'the age of 16 who worked at home.',
            weight=4,
            aggregate='sum',
            targets={workers_16_and_over: 'denominator'},
            tags=[censustags['demographics'], tags['transportation']])
        children = OBSColumn(
            id='B09001001',
            type='Numeric',
            name='children under 18 Years of Age',
            description='The number of people within each geography who are '
            'under 18 years of age.',
            weight=4,
            aggregate='sum',
            tags=[censustags['demographics'], tags['denominator'], tags['race_age_gender']])
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
            tags=[censustags['demographics'], tags['housing']])
        population_3_years_over = OBSColumn(
            id='B14001001',
            type='Numeric',
            name='Population 3 Years and Over',
            description='The total number of people in each geography age '
            '3 years and over.  This denominator is mostly used to calculate '
            'rates of school enrollment.',
            weight=4,
            aggregate='sum',
            tags=[censustags['demographics'], tags['income_education_employment']])
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
            tags=[censustags['demographics'], tags['income_education_employment']])
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
            tags=[censustags['demographics'], tags['income_education_employment']])
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
            tags=[censustags['demographics'], tags['income_education_employment']])
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
            tags=[censustags['demographics'], tags['income_education_employment']])
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
            tags=[censustags['demographics'], tags['income_education_employment']])
        pop_25_years_over = OBSColumn(
            id='B15003001',
            type='Numeric',
            name='Population 25 Years and Over',
            description='The number of people in a geographic area who are over '
            'the age of 25.  This is used mostly as a denominator of educational '
            'attainment.',
            weight=2,
            aggregate='sum',
            tags=[censustags['demographics'], tags['denominator'],
                  tags['income_education_employment']])
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
            tags=[censustags['demographics'], tags['income_education_employment']])
        less_one_year_college = OBSColumn(
            id='B15003019',
            type='Numeric',
            name="Population completed less than one year of college, no degree",
            description="The number of people in a geographic area over the age "
            "of 25 who attended college for less than one year and no further.",
            weight=4,
            aggregate='sum',
            targets={pop_25_years_over: 'denominator'},
            tags=[censustags['demographics'], tags['income_education_employment']])
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
            tags=[censustags['demographics'], tags['income_education_employment']])
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
            tags=[censustags['demographics'], tags['income_education_employment']])
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
            tags=[censustags['demographics'], tags['income_education_employment']])
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
            tags=[censustags['demographics'], tags['income_education_employment']])
        pop_5_years_over = OBSColumn(
            id='B16001001',
            type='Numeric',
            name='Population 5 Years and Over',
            description='The number of people in a geographic area who are over '
            'the age of 5.  This is primarily used as a denominator of measures '
            'of language spoken at home.',
            weight=2,
            aggregate='sum',
            tags=[censustags['demographics'], tags['denominator'], tags['language']])
        speak_only_english_at_home = OBSColumn(
            id='B16001002',
            type='Numeric',
            name='Speaks only English at Home',
            description='The number of people in a geographic area over age '
            '5 who speak only English at home.',
            weight=3,
            aggregate='sum',
            targets={pop_5_years_over: 'denominator'},
            tags=[censustags['demographics'], tags['language']])
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
            tags=[censustags['demographics'], tags['language']])
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
            tags=[censustags['demographics'], tags['denominator']])
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
            tags=[censustags['demographics'], tags['denominator'],
                  tags['income_education_employment']])
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
            tags=[censustags['demographics'],
                  tags['income_education_employment']])
        gini_index = OBSColumn(
            id='B19083001',
            type='Numeric',
            name='Gini Index',
            description='',
            weight=5,
            aggregate='',
            tags=[censustags['demographics'], tags['income_education_employment']])
        income_per_capita = OBSColumn(
            id='B19301001',
            type='Numeric',
            name='Per Capita Income in the past 12 Months',
            description='',
            weight=7,
            aggregate='average',
            tags=[censustags['demographics'], tags['income_education_employment']])
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
            tags=[censustags['demographics'], tags['housing'], tags['denominator']])
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
            tags=[censustags['demographics'], tags['housing']])
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
            tags=[censustags['demographics'], tags['housing']])
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
            tags=[censustags['demographics'], tags['housing']])
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
            tags=[censustags['demographics'], tags['housing']])
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
            tags=[censustags['demographics'], tags['housing'],
                  tags['income_education_employment']])
        owner_occupied_housing_units = OBSColumn(
            id='B25075001',
            type='Numeric',
            name='Owner-occupied Housing Units',
            description="",
            weight=5,
            aggregate='sum',
            targets={housing_units: 'denominator'},
            tags=[censustags['demographics'], tags['housing']])
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
            tags=[censustags['demographics'], tags['housing']])

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
            tags=[censustags['demographics'], tags['housing']])

        #* families with young children (under 6 years of age):
        #  - B23008002: total families with children under 6 years
        families_with_young_children = OBSColumn(
            id='B23008002',
            type='Numeric',
            name='Families with young children (under 6 years of age)',
            description='',
            weight=0,
            aggregate='sum',
            tags=[tag_families_with_young_children])

        #  - B23008003: living with two parents
        two_parent_families_with_young_children = OBSColumn(
            id='B23008003',
            type='Numeric',
            name='Two-parent families with young children (under 6 years of age)',
            description='',
            weight=0,
            aggregate='sum',
            targets={families_with_young_children: 'denominator'},
            tags=[tag_families_with_young_children])
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
            tags=[tag_families_with_young_children])
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
            tags=[tag_families_with_young_children])

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
            tags=[tag_families_with_young_children])

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
            tags=[tag_families_with_young_children])

        #  - B23008008: living with one parent
        one_parent_families_with_young_children = OBSColumn(
            id='B23008008',
            type='Numeric',
            name='One-parent families with young children (under 6 years of age)',
            description='',
            weight=0,
            aggregate='sum',
            targets={families_with_young_children: 'denominator'},
            tags=[tag_families_with_young_children])

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
            tags=[tag_families_with_young_children])

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
            tags=[tag_families_with_young_children])

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

        #* men in middle age (45-64)
        men_45_to_64 = OBSColumn(
            id='B15001027',
            type='Numeric',
            name='Men age 45 to 64 ("middle aged")',
            description=0,
            aggregate='sum',
            tags=[tag_middle_aged_men])

        #  - B01001015: 45 To 49 Years
        men_45_to_49 = OBSColumn(
            id='B01001015',
            type='Numeric',
            name='Men age 45 to 49',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[tag_middle_aged_men])

        #  - B01001016: 50 To 54 Years
        men_50_to_54 = OBSColumn(
            id='B01001016',
            type='Numeric',
            name='Men age 50 to 54',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[tag_middle_aged_men])

        #  - B01001017: 55 To 59 Years
        men_55_to_59 = OBSColumn(
            id='B01001017',
            type='Numeric',
            name='Men age 55 to 59',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[tag_middle_aged_men])

        #  - B01001018: 60 and 61 Years
        men_60_61 = OBSColumn(
            id='B01001018',
            type='Numeric',
            name='Men age 60 to 61',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[tag_middle_aged_men])

        #  - B01001019: 62 To 64 Years
        men_62_64 = OBSColumn(
            id='B01001019',
            type='Numeric',
            name='Men age 62 to 64',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[tag_middle_aged_men])

        #  - B01001B012: black, 45 to 54 Years
        black_men_45_54 = OBSColumn(
            id='B01001B012',
            type='Numeric',
            name='Black Men age 45 to 54',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[tag_middle_aged_men])

        #  - B01001B013: black, 55 to 64 Years
        black_men_55_64 = OBSColumn(
            id='B01001B013',
            type='Numeric',
            name='Black Men age 55 to 64',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[tag_middle_aged_men])

        #  - B01001I012: Hispanic, 45 to 54 Years
        hispanic_men_45_54 = OBSColumn(
            id='B01001I012',
            type='Numeric',
            name='Hispanic Men age 45 to 54',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[tag_middle_aged_men])

        #  - B01001I013: Hispanic, 55 to 64 Years
        hispanic_men_55_64 = OBSColumn(
            id='B01001I013',
            type='Numeric',
            name='Hispanic Men age 55 to 64',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[tag_middle_aged_men])

        #  - B01001H012: white non-hispanic, 45 to 54 Years
        white_men_45_54 = OBSColumn(
            id='B01001H012',
            type='Numeric',
            name='White Men age 45 to 54',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[tag_middle_aged_men])

        #  - B01001H013: white non-hispanic, 55 to 64 Years
        white_men_55_64 = OBSColumn(
            id='B01001H013',
            type='Numeric',
            name='White Men age 55 to 64',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[tag_middle_aged_men])

        #  - B01001D012: asian, 45 to 54 Years
        asian_men_45_54 = OBSColumn(
            id='B01001D012',
            type='Numeric',
            name='Asian Men age 45 to 54',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[tag_middle_aged_men])

        #  - B01001D013: asian, 55 to 64 Years
        asian_men_55_64 = OBSColumn(
            id='B01001D013',
            type='Numeric',
            name='Asian Men age 55 to 64',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[tag_middle_aged_men])

        #  - B05013012: foreign born, 45 to 49 Years
        #  - B05013013: foreign born, 50 to 54 Years
        #  - B05013014: foreign born, 55 to 59 Years
        #  - B05013015: foreign born, 60 to 64 Years
        #  - B15001028: less than 9th grade education
        men_45_64_less_than_9_grade = OBSColumn(
            id='B15001028',
            type='Numeric',
            name='Men age 45 to 64 who attained less than a 9th grade education',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[tag_middle_aged_men])

        #  - B15001029: completed between 9th to 12th grade, no diploma
        men_45_64_grade_9_12 = OBSColumn(
            id='B15001029',
            type='Numeric',
            name='Men age 45 to 64 who attained between 9th and 12th grade, no diploma',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[tag_middle_aged_men])

        #  - B15001030: high school graduate including GED
        men_45_64_high_school = OBSColumn(
            id='B15001030',
            type='Numeric',
            name='Men age 45 to 64 who completed high school or obtained GED',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[tag_middle_aged_men])

        #  - B15001031: some college, no degree
        men_45_64_some_college = OBSColumn(
            id='B15001031',
            type='Numeric',
            name='Men age 45 to 64 who completed some college, no degree',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[tag_middle_aged_men])

        #  - B15001032: associate's degree
        men_45_64_associates_degree = OBSColumn(
            id='B15001032',
            type='Numeric',
            name='Men age 45 to 64 who obtained an associate\'s degree',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[tag_middle_aged_men])

        #  - B15001033: bachelor's degree
        men_45_64_bachelors_degree = OBSColumn(
            id='B15001033',
            type='Numeric',
            name='Men age 45 to 64 who obtained a bachelor\'s degree',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[tag_middle_aged_men])

        #  - B15001034: graduate/professional degree
        men_45_64_graduate_degree = OBSColumn(
            id='B15001034',
            type='Numeric',
            name='Men age 45 to 64 who obtained a graduate or professional degree',
            description='',
            weight=0,
            aggregate='sum',
            targets={total_pop: 'denominator'},
            tags=[tag_middle_aged_men])

        #  - B17001013: income below poverty, 45 to 54 years
        #  - B17001014: income below poverty, 55 to 64 years
        #  - B17001015: income above poverty, 45 to 54 years
        #  - B17001016: income above poverty, 55 to 64 years
        #  - B23001046: in labor force, 45 to 54 years
        #men_45_64_in_labor_force = OBSColumn(
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
        #men_45_64_unemployed = OBSColumn(
        #    id='B23001050',
        #    type='Numeric',
        #    name='Men age 45 to 64 who are in the labor force',
        #    description='',
        #    weight=0,
        #    aggregate='sum',
        #    targets={men_45_64_in_labor_force: 'denominator'},
        #    tags=[tag_middle_aged_men])

        ##  - B23001051 not in labor force, 45 to 54 years
        #men_45_64_not_in_labor_force = OBSColumn(
        #    id='B23001051',
        #    type='Numeric',
        #    name='Men age 45 to 64 who are not in the labor force',
        #    description='',
        #    weight=0,
        #    aggregate='sum',
        #    targets={men_45_64_in_labor_force: 'denominator'},
        #    tags=[tag_middle_aged_men])

        # Pitney bowes
        #for
        pop_15_and_over = OBSColumn(
            id="B12005001",
            type='Numeric',
            name='Population 15 Years and Over',
            description='The number of people in a geographic area who are over '
            'the age of 15.  This is used mostly as a denominator of marital '
            'status.',
            weight=2,
            aggregate='sum',
            tags=[censustags['demographics'], tags['denominator']]
        )
        pop_never_married = OBSColumn(
            id="B12005002",
            targets={pop_15_and_over: 'denominator'},
            type='Numeric',
            name='Never Married',
            description='The number of people in a geographic area who have never been married.',
            weight=2,
            aggregate='sum',
            tags=[censustags['demographics']]
        )
        pop_now_married = OBSColumn(
            id="B12005005",
            targets={pop_15_and_over: 'denominator'},
            type='Numeric',
            name='Currently married',
            description='The number of people in a geographic area who are currently married',
            weight=2,
            aggregate='sum',
            tags=[censustags['demographics']]
        )
        pop_separated = OBSColumn(
            id='B12005008',
            targets={pop_15_and_over: 'denominator'},
            type='Numeric',
            name='Married but separated',
            description='The number of people in a geographic area who are married but separated',
            weight=2,
            aggregate='sum',
            tags=[censustags['demographics']]
        )
        pop_widowed = OBSColumn(
            id='B12005012',
            targets={pop_15_and_over: 'denominator'},
            type='Numeric',
            name='Widowed',
            description='The number of people in a geographic area who are widowed',
            weight=2,
            aggregate='sum',
            tags=[censustags['demographics']]
        )
        pop_divorced = OBSColumn(
            id='B12005015',
            targets={pop_15_and_over: 'denominator'},
            type='Numeric',
            name='Divorced',
            description='The number of people in a geographic area who are divorced',
            weight=2,
            aggregate='sum',
            tags=[censustags['demographics']]
        )

        commuters_16_over = OBSColumn(
            id='B08134001',
            type='Numeric',
            name='Workers age 16 and over who do not work from home',
            description='The number of workers over the age of 16 who do not '
                        'work from home in a geographic area',
            weight=2,
            aggregate='sum',
            tags=[censustags['demographics'], tags['income_education_employment']]
        )
        commute_less_10_mins = OBSColumn(
            id='B08134002',
            targets={commuters_16_over: 'denominator'},
            type='Numeric',
            name='Number of workers with less than 10 minute commute',
            description='The number of workers over the age of 16 who do not '
                        'work from home and commute in less than 10 minutes '
                        'in a geographic area',
            weight=2,
            aggregate='sum',
            tags=[censustags['demographics'], tags['income_education_employment']]
        )
        commute_10_14_mins = OBSColumn(
            id='B08134003',
            targets={commuters_16_over: 'denominator'},
            type='Numeric',
            name='Number of workers with a commute between 10 and 14 minutes',
            description='The number of workers over the age of 16 who do not '
                        'work from home and commute in between 10 and 14 minutes '
                        'in a geographic area',
            weight=2,
            aggregate='sum',
            tags=[censustags['demographics'], tags['income_education_employment']]

        )
        commute_15_19_mins = OBSColumn(
            id='B08134004',
            targets={commuters_16_over: 'denominator'},
            type='Numeric',
            name='Number of workers with a commute between 15 and 19 minutes',
            description='The number of workers over the age of 16 who do not '
                        'work from home and commute in between 15 and 19 minutes '
                        'in a geographic area',
            weight=2,
            aggregate='sum',
            tags=[censustags['demographics'], tags['income_education_employment']]

        )
        commute_20_24_mins = OBSColumn(
            id='B08134005',
            targets={commuters_16_over: 'denominator'},
            type='Numeric',
            name='Number of workers with a commute between 20 and 24 minutes',
            description='The number of workers over the age of 16 who do not '
                        'work from home and commute in between 20 and 24 minutes '
                        'in a geographic area',
            weight=2,
            aggregate='sum',
            tags=[censustags['demographics'], tags['income_education_employment']]
        )
        commute_25_29_mins = OBSColumn(
            id='B08134006',
            targets={commuters_16_over: 'denominator'},
            type='Numeric',
            name='Number of workers with a commute between 25 and 29 minutes',
            description='The number of workers over the age of 16 who do not '
                        'work from home and commute in between 25 and 29 minutes '
                        'in a geographic area',
            weight=2,
            aggregate='sum',
            tags=[censustags['demographics'], tags['income_education_employment']]
        )
        commute_30_34_mins = OBSColumn(
            id='B08134007',
            targets={commuters_16_over: 'denominator'},
            type='Numeric',
            name='Number of workers with a commute between 30 and 34 minutes',
            description='The number of workers over the age of 16 who do not '
                        'work from home and commute in between 30 and 34 minutes '
                        'in a geographic area',
            weight=2,
            aggregate='sum',
            tags=[censustags['demographics'], tags['income_education_employment']]
        )
        commute_35_44_mins = OBSColumn(
            id='B08134008',
            targets={commuters_16_over: 'denominator'},
            type='Numeric',
            name='Number of workers with a commute between 35 and 44 minutes',
            description='The number of workers over the age of 16 who do not '
                        'work from home and commute in between 35 and 44 minutes '
                        'in a geographic area',
            weight=2,
            aggregate='sum',
            tags=[censustags['demographics'], tags['income_education_employment']]
        )
        commute_45_59_mins = OBSColumn(
            id='B08134009',
            targets={commuters_16_over: 'denominator'},
            type='Numeric',
            name='Number of workers with a commute between 45 and 59 minutes',
            description='The number of workers over the age of 16 who do not '
                        'work from home and commute in between 45 and 59 minutes '
                        'in a geographic area',
            weight=2,
            aggregate='sum',
            tags=[censustags['demographics'], tags['income_education_employment']]
        )
        commute_60_more_mins = OBSColumn(
            id='B08134010',
            targets={commuters_16_over: 'denominator'},
            type='Numeric',
            name='Number of workers with a commute of over 60 minutes',
            description='The number of workers over the age of 16 who do not '
                        'work from home and commute in over 60 minutes '
                        'in a geographic area',
            weight=2,
            aggregate='sum',
            tags=[censustags['demographics'], tags['income_education_employment']]
        )

        aggregate_travel_time_to_work = OBSColumn(
            id='B08135001',
            type='Numeric',
            name='Aggregate travel time to work',
            description='The total number of minutes every worker over the age '
                        'of 16 who did not work from home spent spent '
                        'commuting to work in one day in a geographic area',
            weight=2,
            aggregate='sum',
            targets={commuters_16_over: 'divisor'},
            tags=[censustags['demographics'], tags['income_education_employment']]
        )

        income_less_10000 = OBSColumn(
            id='B19001002',
            type='Numeric',
            aggregate='sum',
            name='Households with income less than $10,000',
            description='The number of households in a geographic area whose '
                        'annual income was less than $10,000.',
            weight=2,
            tags=[censustags['demographics'], tags['income_education_employment']],
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
            tags=[censustags['demographics'], tags['income_education_employment']],
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
            tags=[censustags['demographics'], tags['income_education_employment']],
            targets={households: 'denominator'}
        )
        income_20000_24999 = OBSColumn(
            id='B19001005',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $20,000 To $24,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $20,000 and $24,999.',
            weight=2,
            tags=[censustags['demographics'], tags['income_education_employment']],
            targets={households: 'denominator'}
        )
        income_25000_29999 = OBSColumn(
            id='B19001006',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $25,000 To $29,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $20,000 and $24,999.',
            weight=2,
            tags=[censustags['demographics'], tags['income_education_employment']],
            targets={households: 'denominator'}
        )
        income_30000_34999 = OBSColumn(
            id='B19001007',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $30,000 To $34,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $30,000 and $34,999.',
            weight=2,
            tags=[censustags['demographics'], tags['income_education_employment']],
            targets={households: 'denominator'}
        )
        income_35000_39999 = OBSColumn(
            id='B19001008',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $35,000 To $39,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $35,000 and $39,999.',
            weight=2,
            tags=[censustags['demographics'], tags['income_education_employment']],
            targets={households: 'denominator'}
        )
        income_40000_44999 = OBSColumn(
            id='B19001009',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $40,000 To $44,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $40,000 and $44,999.',
            weight=2,
            tags=[censustags['demographics'], tags['income_education_employment']],
            targets={households: 'denominator'}
        )
        income_45000_49999 = OBSColumn(
            id='B19001010',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $45,000 To $49,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $45,000 and $49,999.',
            weight=2,
            tags=[censustags['demographics'], tags['income_education_employment']],
            targets={households: 'denominator'}
        )
        income_50000_59999 = OBSColumn(
            id='B19001011',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $50,000 To $59,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $50,000 and $59,999.',
            weight=2,
            tags=[censustags['demographics'], tags['income_education_employment']],
            targets={households: 'denominator'}
        )
        income_60000_74999 = OBSColumn(
            id='B19001012',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $60,000 To $74,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $60,000 and $74,999.',
            weight=2,
            tags=[censustags['demographics'], tags['income_education_employment']],
            targets={households: 'denominator'}
        )
        income_75000_99999 = OBSColumn(
            id='B19001013',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $75,000 To $99,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $75,000 and $99,999.',
            weight=2,
            tags=[censustags['demographics'], tags['income_education_employment']],
            targets={households: 'denominator'}
        )
        income_100000_124999 = OBSColumn(
            id='B19001014',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $100,000 To $124,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $100,000 and $124,999.',
            weight=2,
            tags=[censustags['demographics'], tags['income_education_employment']],
            targets={households: 'denominator'}
        )
        income_125000_149999 = OBSColumn(
            id='B19001015',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $125,000 To $149,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $125,000 and $149,999.',
            weight=2,
            tags=[censustags['demographics'], tags['income_education_employment']],
            targets={households: 'denominator'}
        )
        income_150000_199999 = OBSColumn(
            id='B19001016',
            type='Numeric',
            aggregate='sum',
            name='Households with income of $150,000 To $199,999',
            description='The number of households in a geographic area whose '
                        'annual income was between $150,000 and $1999,999.',
            weight=2,
            tags=[censustags['demographics'], tags['income_education_employment']],
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
            tags=[censustags['demographics'], tags['income_education_employment']],
            targets={households: 'denominator'}
        )
        #TODO
        #average_travel_time_to_work = OBSColumn(
        #    id='B08135001',
        #    aggregate='sum'
        #)

        return OrderedDict([
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
            ('men_45_to_64', men_45_to_64),
            ('men_45_to_49', men_45_to_49),
            ('men_50_to_54', men_50_to_54),
            ('men_55_to_59', men_55_to_59),
            ('men_60_61', men_60_61),
            ('men_62_64', men_62_64),
            ('black_men_45_54', black_men_45_54),
            ('black_men_55_64', black_men_55_64),
            ('hispanic_men_45_54', hispanic_men_45_54),
            ('hispanic_men_55_64', hispanic_men_55_64),
            ('white_men_45_54', white_men_45_54),
            ('white_men_55_64', white_men_55_64),
            ('asian_men_45_54', asian_men_45_54),
            ('asian_men_55_64', asian_men_55_64),
            ('men_45_64_less_than_9_grade', men_45_64_less_than_9_grade),
            ('men_45_64_grade_9_12', men_45_64_grade_9_12),
            ('men_45_64_high_school', men_45_64_high_school),
            ('men_45_64_some_college', men_45_64_some_college),
            ('men_45_64_associates_degree', men_45_64_associates_degree),
            ('men_45_64_bachelors_degree', men_45_64_bachelors_degree),
            ('men_45_64_graduate_degree', men_45_64_graduate_degree),
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
        ])


class DownloadACS(LoadPostgresFromURL):

    # http://censusreporter.tumblr.com/post/73727555158/easier-access-to-acs-data
    url_template = 'https://s3.amazonaws.com/census-backup/acs/{year}/' \
            'acs{year}_{sample}/acs{year}_{sample}_backup.sql.gz'

    year = Parameter()
    sample = Parameter()

    @property
    def schema(self):
        return 'acs{year}_{sample}'.format(year=self.year, sample=self.sample)

    def identifier(self):
        return self.schema

    def run(self):
        cursor = pg_cursor()
        try:
            cursor.execute('CREATE ROLE census')
            cursor.connection.commit()
        except ProgrammingError:
            cursor.connection.rollback()
        try:
            cursor.execute('DROP SCHEMA {schema} CASCADE'.format(schema=self.schema))
            cursor.connection.commit()
        except ProgrammingError:
            cursor.connection.rollback()
        url = self.url_template.format(year=self.year, sample=self.sample)
        self.load_from_url(url)


class QuantileColumns(ColumnsTask):

    def requires(self):
        return Columns()

    def version(self):
        return 3

    def columns(self):
        quantile_columns = OrderedDict()
        for colname, coltarget in self.input().iteritems():
            col = coltarget.get(current_session())
            quantile_columns[colname+'_quantile'] = OBSColumn(
                id=col.id.split('.')[-1]+'_quantile',
                type='Numeric',
                name='Quantile:'+col.name,
                description=col.description,
                aggregate='quantile',
                targets={col: 'quantile_source'}
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
        return 5

    def columns(self):
        columns = OrderedDict({
            'geoid': self.input()['tiger'][self.geography + '_geoid']
        })
        columns.update(self.input()['columns'])
        return columns

    def bounds(self):
        return 'BOX(0 0,0 0)'

    def timespan(self):
        sample = int(self.sample[0])
        return '{start} - {end}'.format(start=int(self.year) - sample + 1,
                                        end=int(self.year))

    def populate(self):
        connection = current_session()
        quant_col_names = self.input()['columns'].keys()
        old_col_names = [name.split("_quantile")[0]
                         for name in quant_col_names]
        selects = [" percent_rank() OVER (ORDER BY {old_col} ASC) ".format(old_col=name)
                   for name in old_col_names]

        insert_statment = ", ".join(quant_col_names)
        select_statment = ", ".join(selects)

        connection.execute('''
            INSERT INTO {table}
            (geoid, {insert_statment})
            SELECT geoid, {select_statment}
            FROM {source_table}
        '''.format(
            table        = self.output().table,
            insert_statment = insert_statment,
            select_statment = select_statment,
            source_table = self.input()['table'].table
        ))

class Extract(TableTask):
    '''
    Generate an extract of important ACS columns for CartoDB
    '''

    year = Parameter()
    sample = Parameter()
    geography = Parameter()

    def version(self):
        return 1

    def requires(self):
        return {
            'acs': Columns(),
            'tiger': GeoidColumns(),
            'data': DownloadACS(year=self.year, sample=self.sample),
            'tigerdata': SumLevel(geography=self.geography, year=self.year)
        }

    def timespan(self):
        sample = int(self.sample[0])
        return '{start} - {end}'.format(start=int(self.year) - sample + 1,
                                        end=int(self.year))

    def bounds(self):
        session = current_session()
        if self.input()['tigerdata'].exists():
            return self.input()['tigerdata'].get(session).bounds

    def columns(self):
        cols = OrderedDict([
            ('geoid', self.input()['tiger'][self.geography + '_geoid']),
        ])
        for colkey, col in self.input()['acs'].iteritems():
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
        inputschema = self.input()['data'].table
        for colname, coltarget in self.columns().iteritems():
            colid = coltarget.get(session).id
            colnames.append(colname)
            if colid.endswith('geoid'):
                colids.append('SUBSTR(geoid, 8)')
            else:
                colids.append(coltarget.name)
                tableids.add(colid.split('.')[-1][0:-3])
        tableclause = '"{inputschema}".{inputtable} '.format(
            inputschema=inputschema, inputtable=tableids.pop())
        for tableid in tableids:
            tableclause += ' JOIN "{inputschema}".{inputtable} ' \
                           ' USING (geoid) '.format(inputschema=inputschema,
                                                    inputtable=tableid)
        table_id = self.output().get(session).id
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
        for geo in ('state', 'county', 'census_tract', 'block_group', 'puma', 'zcta5',):
            yield Quantiles(geography=geo, year=self.year, sample=self.sample)
