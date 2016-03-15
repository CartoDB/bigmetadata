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
from tasks.util import (LoadPostgresFromURL, classpath, pg_cursor, shell,
                        CartoDBTarget, get_logger, underscore_slugify, TableTask,
                        session_scope, ColumnTarget, ColumnsTask)
from tasks.us.census.tiger import load_sumlevels, SumLevel
from psycopg2 import ProgrammingError
from tasks.us.census.tiger import (SUMLEVELS, load_sumlevels, GeoidColumns,
                                   SUMLEVELS_BY_SLUG, ShorelineClipTiger)

from tasks.meta import (BMDColumn, BMDTag, BMDColumnToColumn, BMDColumnTag,
                        BMDColumnTable)
from tasks.tags import Tags

LOGGER = get_logger(__name__)


class Columns(ColumnsTask):

    def requires(self):
        return {
            'tags': Tags(),
        }

    def columns(self):
        total_pop = BMDColumn(
            id='B01001001',
            type='Numeric',
            name="Total Population",
            description='The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.',
            aggregate='sum',
            weight=10,
            tags=[BMDColumnTag(tag_id=self.input()['tags']['denominator']._id),
                  BMDColumnTag(tag_id=self.input()['tags']['population']._id)]
        )
        male_pop = BMDColumn(
            id='B01001002',
            type='Numeric',
            name="Male Population",
            description="The number of people within each geography who are male.",
            aggregate='sum',
            weight=8,
            target_columns=[BMDColumnToColumn(target=total_pop, reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['population']._id)]
        )
        female_pop = BMDColumn(
            id='B01001026',
            type='Numeric',
            name="Total Population",
            description='The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.',
            aggregate='sum',
            weight=8,
            target_columns=[BMDColumnToColumn(target=total_pop, reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['population']._id)]
        )
        median_age = BMDColumn(
            id='B01002001',
            type='Numeric',
            name="Median Age",
            description="The median age of all people in a given geographic area.",
            aggregate='median',
            weight=2,
            tags=[BMDColumnTag(tag_id=self.input()['tags']['population']._id)]
        )
        white_pop = BMDColumn(
            id='B03002003',
            type='Numeric',
            name="White Population",
            description="The number of people identifying as white, non-Hispanic in each geography.",
            aggregate='sum',
            weight=7,
            target_columns=[BMDColumnToColumn(target=total_pop, reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['population']._id)]
        )
        black_pop = BMDColumn(
            id='B03002004',
            type='Numeric',
            name='Black or African American Population',
            description="The number of people identifying as black or African American, non-Hispanic in each geography.",
            aggregate='sum',
            weight=7,
            target_columns=[BMDColumnToColumn(target=total_pop, reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['population']._id)]
        )
        asian_pop = BMDColumn(
            id='B03002006',
            type='Numeric',
            name='Asian Population',
            description="The number of people identifying as Asian, non-Hispanic in each geography.",
            aggregate='sum',
            weight=7,
            target_columns=[BMDColumnToColumn(target=total_pop, reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['population']._id)]
        )
        hispanic_pop = BMDColumn(
            id='B03002012',
            type='Numeric',
            name='Asian Population',
            description="The number of people identifying as Hispanic or Latino in each geography.",
            aggregate='sum',
            weight=7,
            target_columns=[BMDColumnToColumn(target=total_pop, reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['population']._id)]
        )
        not_us_citizen_pop = BMDColumn(
            id='B05001006',
            type='Numeric',
            name='Not a U.S. Citizen Population',
            description="The number of people within each geography who indicated that they are not U.S. citizens.",
            aggregate='sum',
            weight=3,
            target_columns=[BMDColumnToColumn(target=total_pop, reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['population']._id)]
        )
        workers_16_and_over = BMDColumn(
            id='B08006001',
            type='Numeric',
            name='Workers over the Age of 16',
            description="The number of people in each geography who work.  Workers include those employed at private for-profit companies, the self-employed, government workers and non-profit employees.",
            aggregate='sum',
            weight=5,
            tags=[BMDColumnTag(tag_id=self.input()['tags']['denominator']._id),
                  BMDColumnTag(tag_id=self.input()['tags']['income_education_employment']._id)]
        )
        commuters_by_car_truck_van = BMDColumn(
            id='B08006002',
            type='Numeric',
            name='Commuters by Car, Truck, or Van',
            description='The number of workers age 16 years and over within a geographic area who primarily traveled to work by car, truck or van.  This is the principal mode of travel or type of conveyance, by distance rather than time, that the worker usually used to get from home to work.',
            weight=4,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=workers_16_and_over, reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['transportation']._id)])
        commuters_by_public_transportation = BMDColumn(
            id='B08006008',
            type='Numeric',
            name='Commuters by Public Transportation',
            description='The number of workers age 16 years and over within a geographic area who primarily traveled to work by public transportation.  This is the principal mode of travel or type of conveyance, by distance rather than time, that the worker usually used to get from home to work.',
            weight=4,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=workers_16_and_over, reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['transportation']._id)])
        commuters_by_bus = BMDColumn(
            id='B08006009',
            type='Numeric',
            name='Commuters by Bus',
            description='The number of workers age 16 years and over within a geographic area who primarily traveled to work by bus.  This is the principal mode of travel or type of conveyance, by distance rather than time, that the worker usually used to get from home to work.  This is a subset of workers who commuted by public transport.',
            weight=3,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=commuters_by_public_transportation, reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['transportation']._id)])
        commuters_by_subway_or_elevated = BMDColumn(
            id='B08006011',
            type='Numeric',
            name='Commuters by Subway or Elevated',
            description='The number of workers age 16 years and over within a geographic area who primarily traveled to work by subway or elevated train.  This is the principal mode of travel or type of conveyance, by distance rather than time, that the worker usually used to get from home to work.  This is a subset of workers who commuted by public transport.',
            weight=3,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=commuters_by_public_transportation, reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['transportation']._id)])
        walked_to_work = BMDColumn(
            id='B08006015',
            type='Numeric',
            name='Walked to Work',
            description='The number of workers age 16 years and over within a geographic area who primarily walked to work.  This would mean that of any way of getting to work, they travelled the most distance walking.',
            weight=4,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=workers_16_and_over, reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['transportation']._id)])
        worked_at_home = BMDColumn(
            id='B08006017',
            type='Numeric',
            name='Worked at Home',
            description='The count within a geographical area of workers over the age of 16 who worked at home.',
            weight=4,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=workers_16_and_over, reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['transportation']._id)])
        children = BMDColumn(
            id='B09001001',
            type='Numeric',
            name='Children under 18 Years of Age',
            description='The number of people within each geography who are under 18 years of age.',
            weight=4,
            aggregate='sum',
            tags=[BMDColumnTag(tag_id=self.input()['tags']['transportation']._id),
                  BMDColumnTag(tag_id=self.input()['tags']['race_age_gender']._id) ])
        households = BMDColumn(
            id='B11001001',
            type='Numeric',
            name='Households',
            description='A count of the number of households in each geography.  A household consists of one or more people who live in the same dwelling and also share at meals or living accommodation, and may consist of a single family or some other grouping of people. ',
            weight=8,
            aggregate='sum',
            tags=[BMDColumnTag(tag_id=self.input()['tags']['housing']._id)])
        population_3_years_over = BMDColumn(
            id='B14001001',
            type='Numeric',
            name='Population 3 Years and Over',
            description='The total number of people in each geography age 3 years and over.  This denominator is mostly used to calculate rates of school enrollment.',
            weight=4,
            aggregate='sum',
            tags=[BMDColumnTag(tag_id=self.input()['tags']['income_education_employment']._id)])
        in_school = BMDColumn(
            id='B14001002',
            type='Numeric',
            name='Students Enrolled in School',
            description='The total number of people in each geography currently enrolled at any level of school, from nursery or pre-school to advanced post-graduate education.  Only includes those over the age of 3.',
            weight=6,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=population_3_years_over,
                                              reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['income_education_employment']._id)])
        in_grades_1_to_4 = BMDColumn(
            id='B14001005',
            type='Numeric',
            name='Students Enrolled in Grades 1 to 4',
            description='The total number of people in each geography currently enrolled in grades 1 through 4 inclusive.  This corresponds roughly to elementary school.',
            weight=3,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=in_school,
                                              reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['income_education_employment']._id)])
        in_grades_5_to_8 = BMDColumn(
            id='B14001006',
            type='Numeric',
            name='Students Enrolled in Grades 5 to 8',
            description='The total number of people in each geography currently enrolled in grades 5 through 8 inclusive.  This corresponds roughly to middle school.',
            weight=3,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=in_school,
                                              reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['income_education_employment']._id)])
        in_grades_9_to_12 = BMDColumn(
            id='B14001007',
            type='Numeric',
            name='Students Enrolled in Grades 9 to 12',
            description='The total number of people in each geography currently enrolled in grades 9 through 12 inclusive.  This corresponds roughly to high school.',
            weight=3,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=in_school,
                                              reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['income_education_employment']._id)])
        in_undergrad_college = BMDColumn(
            id='B14001008',
            type='Numeric',
            name='Students Enrolled as Undergraduate in College',
            description='The number of people in a geographic area who are enrolled in college at the undergraduate level. Enrollment refers to being registered or listed as a student in an educational program leading to a college degree. This may be a public school or college, a private school or college.',
            weight=5,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=in_school,
                                              reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['income_education_employment']._id)])
        pop_25_years_over = BMDColumn(
            id='B15003001',
            type='Numeric',
            name='Population 25 Years and Over',
            description='The number of people in a geographic area who are over the age of 25.  This is used mostly as a denominator of educational attainment.',
            weight=2,
            aggregate='sum',
            tags=[BMDColumnTag(tag_id=self.input()['tags']['denominator']._id),
                  BMDColumnTag(tag_id=self.input()['tags']['income_education_employment']._id)])
        high_school_diploma = BMDColumn(
            id='B15003017',
            type='Numeric',
            name='Population Completed High School',
            description='The number of people in a geographic area over the age of 25 who completed high school, and did not complete a more advanced degree.',
            weight=4,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=pop_25_years_over,
                                              reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['income_education_employment']._id)])
        bachelors_degree = BMDColumn(
            id='B15003022',
            type='Numeric',
            name="Population Completed Bachelor's Degree",
            description="The number of people in a geographic area over the age of 25 who obtained a bachelor's degree, and did not complete a more advanced degree.",
            weight=4,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=pop_25_years_over,
                                              reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['income_education_employment']._id)])
        masters_degree = BMDColumn(
            id='B15003023',
            type='Numeric',
            name="Population Completed Master's Degree",
            description="The number of people in a geographic area over the age of 25 who obtained a master's degree, but did not complete a more advanced degree.",
            weight=4,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=pop_25_years_over,
                                              reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['income_education_employment']._id)])
        pop_5_years_over = BMDColumn(
            id='B16001001',
            type='Numeric',
            name='Population 5 Years and Over',
            description='The number of people in a geographic area who are over the age of 5.  This is primarily used as a denominator of measures of language spoken at home.',
            weight=2,
            aggregate='sum',
            tags=[BMDColumnTag(tag_id=self.input()['tags']['denominator']._id),
                  BMDColumnTag(tag_id=self.input()['tags']['language']._id)])
        speak_only_english_at_home = BMDColumn(
            id='B16001002',
            type='Numeric',
            name='Speaks only English at Home',
            description='The number of people in a geographic area over age 5 who speak only English at home.',
            weight=3,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=pop_5_years_over,
                                              reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['language']._id)])
        speak_spanish_at_home = BMDColumn(
            id='B16001003',
            type='Numeric',
            name='Speaks Spanish at Home',
            description='The number of people in a geographic area over age 5 who speak Spanish at home, possibly in addition to other languages.',
            weight=4,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=pop_5_years_over,
                                              reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['language']._id)])
        pop_determined_poverty_status = BMDColumn(
            id='B17001001',
            type='Numeric',
            name='Population for Whom Poverty Status Determined',
            description='The number of people in each geography who could be identified as either living in poverty or not.  This should be used as the denominator when calculating poverty rates, as it excludes people for whom it was not possible to determine poverty.',
            weight=2,
            aggregate='sum',
            tags=[BMDColumnTag(tag_id=self.input()['tags']['denominator']._id)])
        poverty = BMDColumn(
            id='B17001002',
            type='Numeric',
            name='Population for Whom Poverty Status Determined',
            description="The number of people in a geographic area who are part of a family (which could be just them as an individual) determined to be \"in poverty\" following the Office of Management and Budget's Directive 14. (https://www.census.gov/hhes/povmeas/methodology/ombdir14.html)",
            weight=2,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=pop_determined_poverty_status,
                                              reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['denominator']._id),
                  BMDColumnTag(tag_id=self.input()['tags']['income_education_employment']._id)])
        median_income = BMDColumn(
            id='B19013001',
            type='Numeric',
            name='Median Household Income in the past 12 Months',
            description="Within a geographic area, the median income received by every household on a regular basis before payments for personal income taxes, social security, union dues, medicare deductions, etc.  It includes income received from wages, salary, commissions, bonuses, and tips; self-employment income from own nonfarm or farm businesses, including proprietorships and partnerships; interest, dividends, net rental income, royalty income, or income from estates and trusts; Social Security or Railroad Retirement income; Supplemental Security Income (SSI); any cash public assistance or welfare payments from the state or local welfare office; retirement, survivor, or disability benefits; and any other sources of income received regularly such as Veterans' (VA) payments, unemployment and/or worker's compensation, child support, and alimony.",
            weight=8,
            aggregate='median',
            tags=[BMDColumnTag(tag_id=self.input()['tags']['income_education_employment']._id)])
        gini_index = BMDColumn(
            id='B19083001',
            type='Numeric',
            name='Gini Index',
            description='',
            weight=5,
            aggregate='',
            tags=[BMDColumnTag(tag_id=self.input()['tags']['income_education_employment']._id)])
        income_per_capita = BMDColumn(
            id='B19301001',
            type='Numeric',
            name='Per Capita Income in the past 12 Months',
            description='',
            weight=7,
            aggregate='average',
            tags=[BMDColumnTag(tag_id=self.input()['tags']['income_education_employment']._id)])
        housing_units = BMDColumn(
            id='B25001001',
            type='Numeric',
            name='Housing Units',
            description='A count of housing units in each geography.  A housing unit is a house, an apartment, a mobile home or trailer, a group of rooms, or a single room occupied as separate living quarters, or if vacant, intended for occupancy as separate living quarters.',
            weight=8,
            aggregate='sum',
            tags=[BMDColumnTag(tag_id=self.input()['tags']['housing']._id),
                  BMDColumnTag(tag_id=self.input()['tags']['denominator']._id)])
        vacant_housing_units = BMDColumn(
            id='B25002003',
            type='Numeric',
            name='Vacant Housing Units',
            description="The count of vacant housing units in a geographic area. A housing unit is vacant if no one is living in it at the time of enumeration, unless its occupants are only temporarily absent. Units temporarily occupied at the time of enumeration entirely by people who have a usual residence elsewhere are also classified as vacant.",
            weight=8,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=housing_units,
                                              reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['housing']._id)])
        vacant_housing_units_for_rent = BMDColumn(
            id='B25004002',
            type='Numeric',
            name='Vacant Housing Units for Rent',
            description="The count of vacant housing units in a geographic area that are for rent. A housing unit is vacant if no one is living in it at the time of enumeration, unless its occupants are only temporarily absent. Units temporarily occupied at the time of enumeration entirely by people who have a usual residence elsewhere are also classified as vacant.",
            weight=7,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=vacant_housing_units,
                                              reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['housing']._id)])
        vacant_housing_units_for_sale = BMDColumn(
            id='B25004004',
            type='Numeric',
            name='Vacant Housing Units for Sale',
            description="The count of vacant housing units in a geographic area that are for sale. A housing unit is vacant if no one is living in it at the time of enumeration, unless its occupants are only temporarily absent. Units temporarily occupied at the time of enumeration entirely by people who have a usual residence elsewhere are also classified as vacant.",
            weight=7,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=vacant_housing_units,
                                              reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['housing']._id)])
        median_rent = BMDColumn(
            id='B25058001',
            type='Numeric',
            name='Median Rent',
            description="The median contract rent within a geographic area. The contract rent is the monthly rent agreed to or contracted for, regardless of any furnishings, utilities, fees, meals, or services that may be included. For vacant units, it is the monthly rent asked for the rental unit at the time of interview.",
            weight=8,
            aggregate='median',
            tags=[BMDColumnTag(tag_id=self.input()['tags']['housing']._id)])
        percent_income_spent_on_rent = BMDColumn(
            id='B25071001',
            type='Numeric',
            name='Percent of Household Income Spent on Rent',
            description="Within a geographic area, the median percentage of household income which was spent on gross rent.  Gross rent is the amount of the contract rent plus the estimated average monthly cost of utilities (electricity, gas, water, sewer etc.) and fuels (oil, coal, wood, etc.) if these are paid by the renter.  Household income is the sum of the income of all people 15 years and older living in the household.",
            weight=4,
            aggregate='average',
            tags=[BMDColumnTag(tag_id=self.input()['tags']['housing']._id),
                  BMDColumnTag(tag_id=self.input()['tags']['income_education_employment']._id)])
        owner_occupied_housing_units = BMDColumn(
            id='B25075001',
            type='Numeric',
            name='Owner-occupied Housing Units',
            description="",
            weight=5,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=housing_units,
                                              reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['housing']._id)])
        million_dollar_housing_units = BMDColumn(
            id='B25075025',
            type='Numeric',
            name='Owner-occupied Housing Units valued at $1,000,000 or more.',
            description="The count of owner occupied housing units in a geographic area that are valued at $1,000,000 or more.  Value is the respondent's estimate of how much the property (house and lot, mobile home and lot, or condominium unit) would sell for if it were for sale.",
            weight=5,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=owner_occupied_housing_units,
                                              reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['housing']._id)])

        mortgaged_housing_units = BMDColumn(
            id='B25081002',
            type='Numeric',
            name='Owner-occupied Housing Units with a Mortgage',
            description="The count of housing units within a geographic area that are mortagaged. \"Mortgage\" refers to all forms of debt where the property is pledged as security for repayment of the debt, including deeds of trust, trust deed, contracts to purchase, land contracts, junior mortgages, and home equity loans.",
            weight=4,
            aggregate='sum',
            target_columns=[BMDColumnToColumn(target=owner_occupied_housing_units,
                                              reltype='denominator')],
            tags=[BMDColumnTag(tag_id=self.input()['tags']['housing']._id)])
        return OrderedDict({
            "total_pop":                        total_pop,
            "male_pop":                         male_pop,
            "female_pop":                       female_pop,
            "median_age":                       median_age,
            "white_pop":                        white_pop,
            "black_pop":                        black_pop,
            "asian_pop":                        asian_pop,
            "hispanic_pop":                     hispanic_pop,
            "not_us_citizen_pop":               not_us_citizen_pop,
            "workers_16_and_over":              workers_16_and_over,
            "commuters_by_car_truck_van":       commuters_by_car_truck_van,
            "commuters_by_public_transportation":commuters_by_public_transportation,
            "commuters_by_bus":                 commuters_by_bus,
            "commuters_by_subway_or_elevated":  commuters_by_subway_or_elevated,
            "walked_to_work":                   walked_to_work,
            "worked_at_home":                   worked_at_home,
            "children":                         children,
            "households":                       households,
            "population_3_years_over":          population_3_years_over,
            "in_school":                        in_school,
            "in_grades_1_to_4":                 in_grades_1_to_4,
            "in_grades_5_to_8":                 in_grades_5_to_8,
            "in_grades_9_to_12":                in_grades_9_to_12,
            "in_undergrad_college":             in_undergrad_college,
            "pop_25_years_over":                pop_25_years_over,
            "high_school_diploma":              high_school_diploma,
            "bachelors_degree":                 bachelors_degree,
            "masters_degree":                   masters_degree,
            "pop_5_years_over":                 pop_5_years_over,
            "speak_only_english_at_home":       speak_only_english_at_home,
            "speak_spanish_at_home":            speak_spanish_at_home,
            "pop_determined_poverty_status":    pop_determined_poverty_status,
            "poverty":                          poverty,
            "median_income":                    median_income,
            "gini_index":                       gini_index,
            "income_per_capita":                income_per_capita,
            "housing_units":                    housing_units,
            "vacant_housing_units":             vacant_housing_units,
            "vacant_housing_units_for_rent":    vacant_housing_units_for_rent,
            "vacant_housing_units_for_sale":    vacant_housing_units_for_sale,
            "median_rent":                      median_rent,
            "percent_income_spent_on_rent":     percent_income_spent_on_rent,
            "owner_occupied_housing_units":     owner_occupied_housing_units,
            "million_dollar_housing_units":     million_dollar_housing_units,
            "mortgaged_housing_units":          mortgaged_housing_units,
        })


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


class Extract(TableTask):
    '''
    Generate an extract of important ACS columns for CartoDB
    '''

    year = Parameter()
    sample = Parameter()
    geography = Parameter()

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
        if self.input()['tigerdata'].exists():
            with session_scope() as session:
                return self.input()['tigerdata'].get(session).bounds

    def columns(self):
        cols = OrderedDict([
            ('geoid', self.input()['tiger'][self.geography + '_geoid']),
        ])
        for colkey, col in self.input()['acs'].iteritems():
            cols[colkey] = col
        return cols

    def runsession(self, session):
        '''
        load relevant columns from underlying census tables
        '''
        sumlevel = SUMLEVELS_BY_SLUG[self.geography]['summary_level']
        colids = []
        colnames = []
        tableids = set()
        inputschema = self.input()['data'].table
        for colname, coltarget in self.columns().iteritems():
            colid = coltarget.get(session).id
            colnames.append(colname)
            if colid.endswith('geoid'):
                colids.append('geoid')
            else:
                colids.append(coltarget.name)
                tableids.add(colid.split('.')[-1][0:6])
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
            yield Extract(geography=geo, year=self.year, sample=self.sample)
