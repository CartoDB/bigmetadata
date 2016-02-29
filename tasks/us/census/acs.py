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
from sqlalchemy import Column, Numeric, Text
from luigi import Parameter, BooleanParameter, Task, WrapperTask, LocalTarget
from tasks.util import (LoadPostgresFromURL, classpath, pg_cursor, shell,
                        CartoDBTarget, get_logger, slug_column, SessionTask,
                        session_scope)
from tasks.us.census.tiger import load_sumlevels
from psycopg2 import ProgrammingError
from tasks.us.census.tiger import (SUMLEVELS, load_sumlevels, GeoidColumns,
                                   ShorelineClipTiger)

from tasks.meta import (BMDColumn, BMDTag, BMDColumnToColumn, BMDColumnTag,
                        BMDColumnTable)
from tasks.tags import Tags

LOGGER = get_logger(__name__)


class Columns(object):
    def total_pop(self):
        return dict(
            id=classpath(self) + '.B01001001',
            type='Numeric',
            name="Total Population",
            description='The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.',
            aggregate='sum',
            weight=10,
            #tags=[BMDColumnTag(tag=Tags.denominator),
                  #BMDColumnTag(tag=Tags.population)]
        )

    def male_pop(self):
        return dict(
            id='B01001002',
            type='Numeric',
            name="Male Population",
            description="The number of people within each geography who are male.",
            aggregate='sum',
            weight=8,
            #target_columns=[BMDColumnToColumn(target=self.total_pop(), reltype='denominator')],
            #tags=[BMDColumnTag(tag=Tags.population)]
        )

    def female_pop(self):
        return dict(
            id='B01001026',
            type='Numeric',
            name="Female Population",
            description="The number of people within each geography who are female.",
            aggregate='sum',
            weight=8,
            #target_columns=[BMDColumnToColumn(target=self.total_pop(), reltype='denominator')],
            #tags=[BMDColumnTag(tag=Tags.population)]
        )

    def median_age(self):
        return dict(
            id='B01002001',
            type='Numeric',
            name="Median Age",
            description="The median age of all people in a given geographic area.",
            aggregate='median',
            weight=2,
            #tags=[BMDColumnTag(tag=Tags.population)]
        )
    #white_pop = BMDColumn(
    #    id='B03002003',
    #    type='Numeric',
    #    name="White Population",
    #    description="The number of people identifying as white, non-Hispanic in each geography.",
    #    aggregate='sum',
    #    weight=7,
    #    target_columns=[BMDColumnToColumn(target=total_pop, reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.population)]
    #)
    #black_pop = BMDColumn(
    #    id='B03002004',
    #    type='Numeric',
    #    name='Black or African American Population',
    #    description="The number of people identifying as black or African American, non-Hispanic in each geography.",
    #    aggregate='sum',
    #    weight=7,
    #    target_columns=[BMDColumnToColumn(target=total_pop, reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.population)]
    #)
    #asian_pop = BMDColumn(
    #    id='B03002006',
    #    type='Numeric',
    #    name='Asian Population',
    #    description="The number of people identifying as Asian, non-Hispanic in each geography.",
    #    aggregate='sum',
    #    weight=7,
    #    target_columns=[BMDColumnToColumn(target=total_pop, reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.population)]
    #)
    #hispanic_pop = BMDColumn(
    #    id='B03002012',
    #    type='Numeric',
    #    name='Asian Population',
    #    description="The number of people identifying as Hispanic or Latino in each geography.",
    #    aggregate='sum',
    #    weight=7,
    #    target_columns=[BMDColumnToColumn(target=total_pop, reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.population)]
    #)
    #not_us_citizen_pop = BMDColumn(
    #    id='B05001006',
    #    type='Numeric',
    #    name='Not a U.S. Citizen Population',
    #    description="The number of people within each geography who indicated that they are not U.S. citizens.",
    #    aggregate='sum',
    #    weight=3,
    #    target_columns=[BMDColumnToColumn(target=total_pop, reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.population)]
    #)
    #workers_16_and_over = BMDColumn(
    #    id='B08006001',
    #    type='Numeric',
    #    name='Workers over the Age of 16',
    #    description="The number of people in each geography who work.  Workers include those employed at private for-profit companies, the self-employed, government workers and non-profit employees.",
    #    aggregate='sum',
    #    weight=5,
    #    tags=[BMDColumnTag(tag=Tags.denominator), BMDColumnTag(tag=Tags.income_education_employment)]
    #)
    #commuters_by_car_truck_van = BMDColumn(
    #    id='B08006002',
    #    type='Numeric',
    #    name='Commuters by Car, Truck, or Van',
    #    description='The number of workers age 16 years and over within a geographic area who primarily traveled to work by car, truck or van.  This is the principal mode of travel or type of conveyance, by distance rather than time, that the worker usually used to get from home to work.',
    #    weight=4,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=workers_16_and_over, reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.transportation)])
    #commuters_by_public_transportation = BMDColumn(
    #    id='B08006008',
    #    type='Numeric',
    #    name='Commuters by Public Transportation',
    #    description='The number of workers age 16 years and over within a geographic area who primarily traveled to work by public transportation.  This is the principal mode of travel or type of conveyance, by distance rather than time, that the worker usually used to get from home to work.',
    #    weight=4,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=workers_16_and_over, reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.transportation)])
    #commuters_by_public_transportation = BMDColumn(
    #    id='B08006009',
    #    type='Numeric',
    #    name='Commuters by Bus',
    #    description='The number of workers age 16 years and over within a geographic area who primarily traveled to work by bus.  This is the principal mode of travel or type of conveyance, by distance rather than time, that the worker usually used to get from home to work.  This is a subset of workers who commuted by public transport.',
    #    weight=3,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=commuters_by_public_transportation, reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.transportation)])
    #commuters_by_public_transportation = BMDColumn(
    #    id='B08006011',
    #    type='Numeric',
    #    name='Commuters by Subway or Elevated',
    #    description='The number of workers age 16 years and over within a geographic area who primarily traveled to work by subway or elevated train.  This is the principal mode of travel or type of conveyance, by distance rather than time, that the worker usually used to get from home to work.  This is a subset of workers who commuted by public transport.',
    #    weight=3,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=commuters_by_public_transportation, reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.transportation)])
    #commuters_by_public_transportation = BMDColumn(
    #    id='B08006015',
    #    type='Numeric',
    #    name='Walked to Work',
    #    description='The number of workers age 16 years and over within a geographic area who primarily walked to work.  This would mean that of any way of getting to work, they travelled the most distance walking.',
    #    weight=4,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=workers_16_and_over, reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.transportation)])
    #commuters_by_public_transportation = BMDColumn(
    #    id='B08006017',
    #    type='Numeric',
    #    name='Worked at Home',
    #    description='The count within a geographical area of workers over the age of 16 who worked at home.',
    #    weight=4,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=workers_16_and_over, reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.transportation)])
    #children = BMDColumn(
    #    id='B09001001',
    #    type='Numeric',
    #    name='Children under 18 Years of Age',
    #    description='The number of people within each geography who are under 18 years of age.',
    #    weight=4,
    #    aggregate='sum',
    #    tags=[BMDColumnTag(tag=Tags.denominator), BMDColumnTag(tag=Tags.race_age_gender)])
    #households = BMDColumn(
    #    id='B11001001',
    #    type='Numeric',
    #    name='Households',
    #    description='A count of the number of households in each geography.  A household consists of one or more people who live in the same dwelling and also share at meals or living accommodation, and may consist of a single family or some other grouping of people. ',
    #    weight=8,
    #    aggregate='sum',
    #    tags=[BMDColumnTag(tag=Tags.denominator), BMDColumnTag(tag=Tags.housing)])
    #population_3_years_over = BMDColumn(
    #    id='B14001001',
    #    type='Numeric',
    #    name='Population 3 Years and Over',
    #    description='The total number of people in each geography age 3 years and over.  This denominator is mostly used to calculate rates of school enrollment.',
    #    weight=4,
    #    aggregate='sum',
    #    tags=[BMDColumnTag(tag=Tags.denominator), BMDColumnTag(tag=Tags.income_education_employment)])
    #in_school = BMDColumn(
    #    id='B14001002',
    #    type='Numeric',
    #    name='Students Enrolled in School',
    #    description='The total number of people in each geography currently enrolled at any level of school, from nursery or pre-school to advanced post-graduate education.  Only includes those over the age of 3.',
    #    weight=6,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=population_3_years_over,
    #                                      reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.income_education_employment)])
    #in_grades_1_to_4 = BMDColumn(
    #    id='B14001005',
    #    type='Numeric',
    #    name='Students Enrolled in Grades 1 to 4',
    #    description='The total number of people in each geography currently enrolled in grades 1 through 4 inclusive.  This corresponds roughly to elementary school.',
    #    weight=3,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=in_school,
    #                                      reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.income_education_employment)])
    #in_grades_5_to_8 = BMDColumn(
    #    id='B14001006',
    #    type='Numeric',
    #    name='Students Enrolled in Grades 5 to 8',
    #    description='The total number of people in each geography currently enrolled in grades 5 through 8 inclusive.  This corresponds roughly to middle school.',
    #    weight=3,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=in_school,
    #                                      reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.income_education_employment)])
    #in_grades_9_to_12 = BMDColumn(
    #    id='B14001007',
    #    type='Numeric',
    #    name='Students Enrolled in Grades 9 to 12',
    #    description='The total number of people in each geography currently enrolled in grades 9 through 12 inclusive.  This corresponds roughly to high school.',
    #    weight=3,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=in_school,
    #                                      reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.income_education_employment)])
    #in_undergrad_college = BMDColumn(
    #    id='B14001008',
    #    type='Numeric',
    #    name='Students Enrolled as Undergraduate in College',
    #    description='The number of people in a geographic area who are enrolled in college at the undergraduate level. Enrollment refers to being registered or listed as a student in an educational program leading to a college degree. This may be a public school or college, a private school or college.',
    #    weight=5,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=in_school,
    #                                      reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.income_education_employment)])
    #pop_25_years_over = BMDColumn(
    #    id='B15003001',
    #    type='Numeric',
    #    name='Population 25 Years and Over',
    #    description='The number of people in a geographic area who are over the age of 25.  This is used mostly as a denominator of educational attainment.',
    #    weight=2,
    #    aggregate='sum',
    #    tags=[BMDColumnTag(tag=Tags.denominator),
    #          BMDColumnTag(tag=Tags.income_education_employment)])
    #high_school_diploma = BMDColumn(
    #    id='B15003017',
    #    type='Numeric',
    #    name='Population Completed High School',
    #    description='The number of people in a geographic area over the age of 25 who completed high school, and did not complete a more advanced degree.',
    #    weight=4,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=pop_25_years_over,
    #                                      reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.income_education_employment)])
    #bachelors_degree = BMDColumn(
    #    id='B15003022',
    #    type='Numeric',
    #    name="Population Completed Bachelor's Degree",
    #    description="The number of people in a geographic area over the age of 25 who obtained a bachelor's degree, and did not complete a more advanced degree.",
    #    weight=4,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=pop_25_years_over,
    #                                      reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.income_education_employment)])
    #masters_degree = BMDColumn(
    #    id='B15003023',
    #    type='Numeric',
    #    name="Population Completed Master's Degree",
    #    description="The number of people in a geographic area over the age of 25 who obtained a master's degree, but did not complete a more advanced degree.",
    #    weight=4,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=pop_25_years_over,
    #                                      reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.income_education_employment)])
    #pop_5_years_over = BMDColumn(
    #    id='B16001001',
    #    type='Numeric',
    #    name='Population 5 Years and Over',
    #    description='The number of people in a geographic area who are over the age of 5.  This is primarily used as a denominator of measures of language spoken at home.',
    #    weight=2,
    #    aggregate='sum',
    #    tags=[BMDColumnTag(tag=Tags.denominator),
    #          BMDColumnTag(tag=Tags.language)])
    #speak_only_english_at_home = BMDColumn(
    #    id='B16001002',
    #    type='Numeric',
    #    name='Speaks only English at Home',
    #    description='The number of people in a geographic area over age 5 who speak only English at home.',
    #    weight=3,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=pop_5_years_over,
    #                                      reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.language)])
    #speak_spanish_at_home = BMDColumn(
    #    id='B16001003',
    #    type='Numeric',
    #    name='Speaks Spanish at Home',
    #    description='The number of people in a geographic area over age 5 who speak Spanish at home, possibly in addition to other languages.',
    #    weight=4,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=pop_5_years_over,
    #                                      reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.language)])
    #pop_determined_poverty_status = BMDColumn(
    #    id='B17001001',
    #    type='Numeric',
    #    name='Population for Whom Poverty Status Determined',
    #    description='The number of people in each geography who could be identified as either living in poverty or not.  This should be used as the denominator when calculating poverty rates, as it excludes people for whom it was not possible to determine poverty.',
    #    weight=2,
    #    aggregate='sum',
    #    tags=[BMDColumnTag(tag=Tags.denominator)])
    #poverty = BMDColumn(
    #    id='B17001002',
    #    type='Numeric',
    #    name='Population for Whom Poverty Status Determined',
    #    description="The number of people in a geographic area who are part of a family (which could be just them as an individual) determined to be \"in poverty\" following the Office of Management and Budget's Directive 14. (https://www.census.gov/hhes/povmeas/methodology/ombdir14.html)",
    #    weight=2,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=pop_determined_poverty_status,
    #                                      reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.denominator),
    #          BMDColumnTag(tag=Tags.income_education_employment)])
    #median_income = BMDColumn(
    #    id='B19013001',
    #    type='Numeric',
    #    name='Median Household Income in the past 12 Months',
    #    description="Within a geographic area, the median income received by every household on a regular basis before payments for personal income taxes, social security, union dues, medicare deductions, etc.  It includes income received from wages, salary, commissions, bonuses, and tips; self-employment income from own nonfarm or farm businesses, including proprietorships and partnerships; interest, dividends, net rental income, royalty income, or income from estates and trusts; Social Security or Railroad Retirement income; Supplemental Security Income (SSI); any cash public assistance or welfare payments from the state or local welfare office; retirement, survivor, or disability benefits; and any other sources of income received regularly such as Veterans' (VA) payments, unemployment and/or worker's compensation, child support, and alimony.",
    #    weight=8,
    #    aggregate='median',
    #    tags=[BMDColumnTag(tag=Tags.income_education_employment)])
    #gini_index = BMDColumn(
    #    id='B19083001',
    #    type='Numeric',
    #    name='Gini Index',
    #    description='',
    #    weight=5,
    #    aggregate='',
    #    tags=[BMDColumnTag(tag=Tags.income_education_employment)])
    #income_per_capita = BMDColumn(
    #    id='B19301001',
    #    type='Numeric',
    #    name='Per Capita Income in the past 12 Months',
    #    description='',
    #    weight=7,
    #    aggregate='average',
    #    tags=[BMDColumnTag(tag=Tags.income_education_employment)])
    #housing_units = BMDColumn(
    #    id='B25001001',
    #    type='Numeric',
    #    name='Housing Units',
    #    description='A count of housing units in each geography.  A housing unit is a house, an apartment, a mobile home or trailer, a group of rooms, or a single room occupied as separate living quarters, or if vacant, intended for occupancy as separate living quarters.',
    #    weight=8,
    #    aggregate='sum',
    #    tags=[BMDColumnTag(tag=Tags.housing),
    #          BMDColumnTag(tag=Tags.denominator)])
    #vacant_housing_units = BMDColumn(
    #    id='B25002003',
    #    type='Numeric',
    #    name='Vacant Housing Units',
    #    description="The count of vacant housing units in a geographic area. A housing unit is vacant if no one is living in it at the time of enumeration, unless its occupants are only temporarily absent. Units temporarily occupied at the time of enumeration entirely by people who have a usual residence elsewhere are also classified as vacant.",
    #    weight=8,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=housing_units,
    #                                      reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.housing)])
    #median_rent = BMDColumn(
    #    id='B25058001',
    #    type='Numeric',
    #    name='Median Rent',
    #    description="The median contract rent within a geographic area. The contract rent is the monthly rent agreed to or contracted for, regardless of any furnishings, utilities, fees, meals, or services that may be included. For vacant units, it is the monthly rent asked for the rental unit at the time of interview.",
    #    weight=8,
    #    aggregate='median',
    #    tags=[BMDColumnTag(tag=Tags.housing)])
    #percent_income_spent_on_rent = BMDColumn(
    #    id='B25071001',
    #    type='Numeric',
    #    name='Percent of Household Income Spent on Rent',
    #    description="Within a geographic area, the median percentage of household income which was spent on gross rent.  Gross rent is the amount of the contract rent plus the estimated average monthly cost of utilities (electricity, gas, water, sewer etc.) and fuels (oil, coal, wood, etc.) if these are paid by the renter.  Household income is the sum of the income of all people 15 years and older living in the household.",
    #    weight=4,
    #    aggregate='average',
    #    tags=[BMDColumnTag(tag=Tags.housing),
    #          BMDColumnTag(tag=Tags.income_education_employment)])
    #owner_occupied_housing_units = BMDColumn(
    #    id='B25075001',
    #    type='Numeric',
    #    name='Owner-occupied Housing Units',
    #    description="",
    #    weight=5,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=housing_units,
    #                                      reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.housing)])
    #million_dollar_housing_units = BMDColumn(
    #    id='B25075025',
    #    type='Numeric',
    #    name='Owner-occupied Housing Units valued at $1,000,000 or more.',
    #    description="The count of owner occupied housing units in a geographic area that are valued at $1,000,000 or more.  Value is the respondent's estimate of how much the property (house and lot, mobile home and lot, or condominium unit) would sell for if it were for sale.",
    #    weight=5,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=owner_occupied_housing_units,
    #                                      reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.housing)])

    #mortgaged_housing_units = BMDColumn(
    #    id='B25081002',
    #    type='Numeric',
    #    name='Owner-occupied Housing Units with a Mortgage',
    #    description="The count of housing units within a geographic area that are mortagaged. \"Mortgage\" refers to all forms of debt where the property is pledged as security for repayment of the debt, including deeds of trust, trust deed, contracts to purchase, land contracts, junior mortgages, and home equity loans.",
    #    weight=4,
    #    aggregate='sum',
    #    target_columns=[BMDColumnToColumn(target=owner_occupied_housing_units,
    #                                      reltype='denominator')],
    #    tags=[BMDColumnTag(tag=Tags.housing)])


#ACS_COLUMNS = Columns()

#
#HIGH_WEIGHT_TABLES = set([
#    'B01001',
#    'B01002',
#    'B03002',
#    'B05001',
#    #'B05011',
#    #'B07101',
#    'B08006',
#    #'B08013',
#    #'B08101',
#    'B09001',
#    'B11001',
#    #'B11002',
#    #'B11012',
#    'B14001',
#    'B15003',
#    'B16001',
#    'B17001',
#    'B19013',
#    'B19083',
#    'B19301',
#    'B25001',
#    'B25002',
#    'B25003',
#    #'B25056',
#    'B25058',
#    'B25071',
#    'B25075',
#    'B25081',
#    #'B25114',
#])
#
#MEDIUM_WEIGHT_TABLES = set([
#    "B02001",
#    "B04001",
#    "B05002",
#    "B05012",
#    "B06011",
#    "B06012",
#    "B07001",
#    "B07204",
#    "B08011",
#    "B08012",
#    "B08103",
#    "B08134",
#    "B08136",
#    "B08301",
#    "B08303",
#    "B09002",
#    "B09005",
#    "B09008",
#    "B09010",
#    "B09018",
#    "B09019",
#    "B11005",
#    "B11006",
#    "B11007",
#    "B11011",
#    "B11014",
#    "B11016",
#    "B11017",
#    "B12001",
#    "B12002",
#    "B12007",
#    "B12501",
#    "B12503",
#    "B12504",
#    "B12505",
#    "B13002",
#    "B13016",
#    "B14002",
#    "B14003",
#    "B15001",
#    "B15002",
#    "B16002",
#    "B16006",
#    "B17015",
#    "B19001",
#    "B19013A",
#    "B19013B",
#    "B19013C",
#    "B19013D",
#    "B19013E",
#    "B19013F",
#    "B19013G",
#    "B19013H",
#    "B19013I",
#    "B19019",
#    "B19051",
#    "B19052",
#    "B19053",
#    "B19054",
#    "B19055",
#    "B19056",
#    "B19057",
#    "B19058",
#    "B19059",
#    "B19060",
#    "B19080",
#    "B19081",
#    "B19082",
#    "B19101",
#    "B19113",
#    "B19301A",
#    "B19301B",
#    "B19301C",
#    "B19301D",
#    "B19301E",
#    "B19301F",
#    "B19301G",
#    "B19301H",
#    "B19301I",
#    "B23001",
#    "B23006",
#    "B23020",
#    "B23025",
#    "B25003A",
#    "B25003B",
#    "B25003C",
#    "B25003D",
#    "B25003E",
#    "B25003F",
#    "B25003G",
#    "B25003H",
#    "B25003I",
#    "B25004",
#    "B25017",
#    "B25018",
#    "B25019",
#    "B25024",
#    "B25026",
#    "B25027",
#    "B25034",
#    "B25035",
#    "B25036",
#    "B25037",
#    "B25040",
#    "B25041",
#    "B25057",
#    "B25059",
#    "B25060",
#    "B25061",
#    "B25062",
#    "B25063",
#    "B25064",
#    "B25065",
#    "B25070",
#    "B25076",
#    "B25077",
#    "B25078",
#    "B25085",
#    "B25104",
#    "B25105",
#    "B27001",
#    "B27002",
#    "B27003",
#    "B27010",
#    "B27011",
#    "B27015",
#    "B27019",
#    "B27020",
#    "B27022",
#    "C02003",
#    "C15002A",
#    "C15010",
#    "C17002",
#    "C24010",
#    "C24020",
#    "C24030",
#    "C24040"
#])

# STEPS:
#
# 1. load ACS SQL into postgres
# 2. extract usable metadata from the imported tables, persist as json
#

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


class DumpACS(WrapperTask):
    #TODO
    '''
    Dump a table in postgres compressed format
    '''
    year = Parameter()
    sample = Parameter()

    def requires(self):
        pass



class AllACS(WrapperTask):

    force = BooleanParameter(default=False)

    def requires(self):
        for year in xrange(2010, 2014):
            for sample in ('1yr', '3yr', '5yr'):
                yield ProcessACS(year=year, sample=sample, force=self.force)


class Extract(SessionTask):
    '''
    Generate an extract of important ACS columns for CartoDB
    '''

    year = Parameter()
    sample = Parameter()
    geography = Parameter()
    force = BooleanParameter(default=False)
    clipped = BooleanParameter()

    def requires(self):
        if self.clipped:
            geography = load_sumlevels()[self.sumlevel]['table']
            return ShorelineClipTiger(year=self.year, geography=geography)

    def generate_columns(self):
        columns = [
            ('geoid', BMDColumn(**getattr(GeoidColumns(), self.geography)())),
            ('total_pop', BMDColumn(**Columns().total_pop())),
            ('male_pop', BMDColumn(**Columns().male_pop())),
            ('female_pop', BMDColumn(**Columns().female_pop())),
        ]
        #import pdb
        #pdb.set_trace()
        #for colname, col in Columns().__dict__.iteritems():
        #    if not colname.startswith('__'):
        #        columns.append((colname, col(), ))
        return columns

    def requires(self):
        return DownloadACS(year=self.year, sample=self.sample)

    def runsession(self, session):
        '''
        load relevant columns from underlying census tables
        '''
        sumlevel = '040' # TODO
        colids = []
        colnames = []
        tableids = set()
        inputschema = self.input().table
        for coltable in self.columns():
            session.add(coltable)
            session.add(coltable.column)
            colid = coltable.column.id.split('.')[-1]
            colids.append(colid)
            colnames.append(coltable.colname)
            tableids.add(colid[0:6])
        tableclause = '"{inputschema}".{inputtable} '.format(
            inputschema=inputschema, inputtable=tableids.pop())
        for tableid in tableids:
            tableclause += 'JOIN "{inputschema}".{inputtable} ' \
                           'USING (geoid)'.format(inputschema=inputschema,
                                                  inputtable=tableid)
        session.execute('INSERT INTO {output} ({colnames}) '
                        '  SELECT {colids} '
                        '  FROM {tableclause} '
                        '  WHERE geoid LIKE :sumlevelprefix '
                        ''.format(
                            output=self.output().table_id,
                            colnames=', '.join(colnames),
                            colids=', '.join(colids),
                            tableclause=tableclause
                        ), {
                            'sumlevelprefix': sumlevel + '00US%'
                        })


class ExtractAllACS(Task):
    force = BooleanParameter(default=False)
    year = Parameter()
    sample = Parameter()
    clipped = BooleanParameter()

    def requires(self):
        #for sumlevel in ('040', '050', '140', '150', '795', '860',):
        for sumlevel in ('state', 'county', 'census-tract', 'census-block', 'puma', 'zcta5',):
            yield ExtractACS(sumlevel=sumlevel, year=self.year,
                             clipped=self.clipped,
                             sample=self.sample, force=self.force)
