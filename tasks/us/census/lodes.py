'''
tasks to download LODES origin-destination, residence, and workplace
characteristics files
'''

import os
import subprocess

from collections import OrderedDict
from tasks.meta import OBSColumn, OBSColumnToColumn, OBSColumnTag
from tasks.util import (shell, DefaultPostgresTarget, pg_cursor, classpath,
                        ColumnsTask, TableTask)
from tasks.tags import Tags

from luigi import Task, Parameter, LocalTarget, BooleanParameter


STATES = set(["al", "ak", "az", "ar", "ca", "co", "ct", "de", "dc", "fl", "ga",
              "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md", "ma",
              "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny",
              "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd", "tn",
              "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy"])

MISSING_STATES = {
    2013: set(['ks', 'ma', 'pr'])
}


class DownloadLODESFile(Task):

    # od, wac, or rac
    filetype = Parameter()

    # [YEAR] = Year of job data. Can have the value of 2002-2013 for most states.
    year = Parameter()

    # [ST] =   lowercase, 2-letter postal code for a chosen state
    state = Parameter()

    # [SEG] = (RAC/WAC only) Segment of the workforce, can have th e values of
    # "S000", "SA01", "SA02", "SA03", "SE01", "SE02", "SE03", "SI01", "SI02",
    # or "SI03". These correspond to the same segments of the workforce as are
    # listed in the OD file structure above. 
    # [PART] = (OD only) Part of the state file, can have a value of either "main" or "aux".
    #          Complimentary parts of the state file, the main part includes jobs with both
    #          workplace and residence in the state and the aux part includes jobs with the
    #          workplace in the state and the residence outside of the state.
    part_or_segment = Parameter()

    # [TYPE] = Job Type, can have a value of "JT00" for All Jobs, "JT01" for Primary
    #          Jobs, "JT02" for All Private Jobs, "JT03" for Private Primary Jobs,
    #          "JT04" for All Federal Jobs, or "JT05" for Federal Primary Jobs.
    job_type = Parameter(default="JT00")

    def filename(self):
        #   [STATE]_[FILETYPE]_[PART/SEG]_[TYPE]_[YEAR].csv.gz   where
        return '{}_{}_{}_{}_{}.csv.gz'.format(self.state, self.filetype,
                                              self.part_or_segment,
                                              self.job_type, self.year)

    def url(self):
        return 'http://lehd.ces.census.gov/data/lodes/LODES7/{}/{}/{}'.format(
            self.state, self.filetype, self.filename())

    def run(self):
        self.output().makedirs()
        try:
            shell('wget {url} -O {target}'.format(url=self.url(), target=self.output().path))
        except subprocess.CalledProcessError:
            shell('rm -f {target}'.format(target=self.output().path))

    def output(self):
        return LocalTarget(path=os.path.join(classpath(self), self.filename()))
        #return DefaultPostgresTarget(self.filename())


class WorkplaceAreaCharacteristicsColumns(ColumnsTask):

    def requires(self):
        return {
            'tags': Tags()
        }

    def columns(self):
        tags = self.input()['tags']
        total_jobs = OBSColumn(
            id='total_jobs',
            type='Integer',
            name='Total Jobs',
            description='Total number of jobs',
            weight=8,
            aggregate='sum',
            tags=[tags['income_education_employment'], tags['denominator']]
        )
        return OrderedDict([
            #work_census_block TEXT, --w_geocode Char15 Workplace Census Block Code
            ('total_jobs', total_jobs),
            ('jobs_age_29_or_younger', OBSColumn(
                id='jobs_age_29_or_younger',
                type='Integer',
                name='Jobs for workers age 29 or younger',
                description='Number of jobs of workers age 29 or younger',
                weight=3,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment'], tags['race_age_gender']]
            )),
            ('jobs_age_30_to_54', OBSColumn(
                id='jobs_age_30_to_54',
                type='Integer',
                name='Jobs for workers age 30 to 54',
                description='Number of jobs for workers age 30 to 54',
                weight=3,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment'], tags['race_age_gender']]
            )),
            ('jobs_age_55_or_older', OBSColumn(
                id='jobs_age_55_or_older',
                type='Integer',
                name='Jobs for workers age 55 or older',
                description='Number of jobs for workers age 55 or older',
                weight=3,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment'], tags['race_age_gender']]
            )),
            ('jobs_earning_15000_or_less', OBSColumn(
                id='jobs_earning_15000_or_less',
                type='Integer',
                name='Jobs earning up to $15,000 per year',
                description='Number of jobs with earnings $1250/month or less ($15,000 per year)',
                weight=3,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_earning_15001_to_40000', OBSColumn(
                id='jobs_earning_15001_to_40000',
                type='Integer',
                name='Jobs earning $15,000 to $40,000 per year',
                description='Number of jobs with earnings $1251/month to $3333/month ($15,000 to $40,000 per year)',
                weight=5,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_earning_40001_or_more', OBSColumn(
                id='jobs_earning_40001_or_more',
                type='Integer',
                name='Jobs with earnings greater than $40,000 per year',
                description='Number of Jobs with earnings greater than $3333/month',
                weight=5,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_11_agriculture_forestry_fishing', OBSColumn(
                id='jobs_11_agriculture_forestry_fishing',
                type='Integer',
                name='Agriculture, Forestry, Fishing and Hunting jobs',
                description='Number of jobs in NAICS sector 11 (Agriculture, Forestry, Fishing and Hunting)',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_21_mining_quarrying_oil_gas', OBSColumn(
                id='jobs_21_mining_quarrying_oil_gas',
                type='Integer',
                name='Mining, Quarrying, and Oil and Gas Extraction jobs',
                description='Number of jobs in NAICS sector 21 (Mining, Quarrying, and Oil and Gas Extraction) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_22_utilities', OBSColumn(
                id='jobs_22_utilities',
                type='Integer',
                name='Utilities Jobs',
                description='Number of jobs in NAICS sector 22 (Utilities) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_23_construction', OBSColumn(
                id='jobs_23_construction',
                type='Integer',
                name='Construction Jobs',
                description='Number of jobs in NAICS sector 23 (Construction) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_31_33_manufacturing', OBSColumn(
                id='jobs_31_33_manufacturing',
                type='Integer',
                name='Manufacturing Jobs',
                description='Number of jobs in NAICS sector 31-33 (Manufacturing) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_42_wholesale_trade', OBSColumn(
                id='jobs_42_wholesale_trade',
                type='Integer',
                name='Wholesale Trade Jobs',
                description='Number of jobs in NAICS sector 42 (Wholesale Trade) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_44_45_retail_trade', OBSColumn(
                id='jobs_44_45_retail_trade',
                type='Integer',
                name='Retail Trade Jobs',
                description='Number of jobs in NAICS sector 44-45 (Retail Trade) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_48_49_transport_warehousing', OBSColumn(
                id='jobs_48_49_transport_warehousing',
                type='Integer',
                name='Transport and Warehousing Jobs',
                description='Number of jobs in NAICS sector 48-49 (Transportation and Warehousing) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_51_information', OBSColumn(
                id='jobs_51_information',
                type='Integer',
                name='Information Jobs',
                description='Number of jobs in NAICS sector 51 (Information) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_52_finance_and_insurance', OBSColumn(
                id='jobs_52_finance_and_insurance',
                type='Integer',
                name='Finance and Insurance Jobs',
                description='Number of jobs in NAICS sector 52 (Finance and Insurance)',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_53_real_estate_rental_leasing', OBSColumn(
                id='jobs_53_real_estate_rental_leasing',
                type='Integer',
                name='Real Estate and Rental and Leasing Jobs',
                description='Number of jobs in NAICS sector 53 (Real Estate and Rental and Leasing) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_54_professional_scientific_tech_services', OBSColumn(
                id='jobs_54_professional_scientific_tech_services',
                type='Integer',
                name='Professional, Scientific, and Technical Services Jobs',
                description='Number of jobs in NAICS sector 54 (Professional, Scientific, and Technical Services) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_55_management_of_companies_enterprises', OBSColumn(
                id='jobs_55_management_of_companies_enterprises',
                type='Integer',
                name='Management of Companies and Enterprises Jobs',
                description='Number of jobs in NAICS sector 55 (Management of Companies and Enterprises) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_56_admin_support_waste_management', OBSColumn(
                id='jobs_56_admin_support_waste_management',
                type='Integer',
                name='Administrative and Support and Waste Management and Remediation Services Jobs',
                description='Number of jobs in NAICS sector 56 (Administrative and Support and Waste Management and Remediation Services) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_61_educational_services', OBSColumn(
                id='jobs_61_educational_services',
                type='Integer',
                name='Educational Services Jobs',
                description='Number of jobs in NAICS sector 61 (Educational Services) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_62_healthcare_social_assistance', OBSColumn(
                id='jobs_62_healthcare_social_assistance',
                type='Integer',
                name='Health Care and Social Assistance Jobs',
                description='Number of jobs in NAICS sector 62 (Health Care and Social Assistance) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_71_arts_entertainment_recreation', OBSColumn(
                id='jobs_71_arts_entertainment_recreation',
                type='Integer',
                name='Arts, Entertainment, and Recreation jobs',
                description='Number of jobs in NAICS sector 71 (Arts, Entertainment, and Recreation) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_72_accommodation_and_food', OBSColumn(
                id='jobs_72_accommodation_and_food',
                type='Integer',
                name='Accommodation and Food Services jobs',
                description='Number of jobs in NAICS sector 72 (Accommodation and Food Services) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_81_other_services_except_public_admin', OBSColumn(
                id='jobs_81_other_services_except_public_admin',
                type='Integer',
                name='Other Services (except Public Administration) jobs',
                description='Jobs in NAICS sector 81 (Other Services [except Public Administration])',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_92_public_administration', OBSColumn(
                id='jobs_92_public_administration',
                type='Integer',
                name='Public Administration jobs',
                description='Number of jobs in NAICS sector 92 (Public Administration) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_white', OBSColumn(
                id='jobs_white',
                type='Integer',
                name='Jobs held by workers who are white',
                description='Number of jobs for workers with Race: White, Alone',
                weight=2,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment'], tags['race_age_gender']]
            )),
            ('jobs_black', OBSColumn(
                id='jobs_black',
                type='Integer',
                name='Jobs held by workers who are black',
                description='Number of jobs for workers with Race: Black or African American Alone',
                weight=2,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment'], tags['race_age_gender']]
            )),
            ('jobs_amerindian', OBSColumn(
                id='jobs_amerindian',
                type='Integer',
                name='Jobs held by workers who are American Indian or Alaska Native Alone',
                description='Number of jobs for workers with Race: American Indian or Alaska Native Alone',
                weight=0,
                aggregate='sum',
            )),
            ('jobs_asian', OBSColumn(
                id='jobs_asian',
                type='Integer',
                name='Jobs held by workers who are Asian',
                description='Number of jobs for workers with Race: Asian Alone',
                weight=2,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment'], tags['race_age_gender']]
            )),
            ('jobs_hawaiian', OBSColumn(
                id='jobs_hawaiian',
                type='Integer',
                name='Jobs held by workers who are Native Hawaiian or Other Pacific Islander Alone',
                description='Number of jobs for workers with Race: Native Hawaiian or Other Pacific Islander Alone',
                weight=0,
                aggregate='sum',
            )),
            ('jobs_two_or_more_races', OBSColumn(
                id='jobs_two_or_more_races',
                type='Integer',
                name='Jobs held by workers who reported Two or More Race Groups',
                description='Number of jobs for workers with Race: Two or More Race Groups',
                weight=0,
                aggregate='sum',
            )),
            ('jobs_not_hispanic', OBSColumn(
                id='jobs_not_hispanic',
                type='Integer',
                name='Jobs held by workers who are Not Hispanic or Latino',
                description='Number of jobs for workers with Ethnicity: Not Hispanic or Latino',
                weight=0,
                aggregate='sum',
            )),
            ('jobs_hispanic', OBSColumn(
                id='jobs_hispanic',
                type='Integer',
                name='Jobs held by workers who are Hispanic or Latino',
                description='Number of jobs for workers with Ethnicity: Hispanic or Latino',
                weight=1,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment'], tags['race_age_gender']]
            )),
            ('jobs_less_than_high_school', OBSColumn(
                id='jobs_less_than_high_school',
                type='Integer',
                name='Jobs held by workers who did not complete high school',
                description='Number of jobs for workers with Educational Attainment: Less than high school',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_high_school', OBSColumn(
                id='jobs_high_school',
                type='Integer',
                name='Jobs held by workers who completed high school',
                description='Number of jobs for workers with Educational Attainment: High school or equivalent, no college',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_some_college', OBSColumn(
                id='jobs_some_college',
                type='Integer',
                name='Jobs held by workers who completed some college or Associate degree',
                description='Number of jobs for workers with Educational Attainment: Some college or Associate degree',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_bachelors_or_advanced', OBSColumn(
                id='jobs_bachelors_or_advanced',
                type='Integer',
                name='Jobs held by workers who obtained a Bachelor\'s degree or advanced degree',
                description='Number of jobs for workers with Educational Attainment: Bachelor\'s degree or advanced degree',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_male', OBSColumn(
                id='jobs_male',
                type='Integer',
                name='Jobs held by men',
                description='Number of jobs for male workers',
                weight=2,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['race_age_gender'], tags['income_education_employment']]
            )),
            ('jobs_female', OBSColumn(
                id='jobs_female',
                type='Integer',
                name='Jobs held by women',
                description='Number of jobs for female workers',
                weight=2,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['race_age_gender'], tags['income_education_employment']]
            )),
            ('jobs_firm_age_0_1_years', OBSColumn(
                id='jobs_firm_age_0_1_years',
                type='Integer',
                name='Jobs at firms aged 0-1 Years',
                description='Number of jobs for workers at firms with Firm Age: 0-1 Years',
                weight=1,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_firm_age_2_3_years', OBSColumn(
                id='jobs_firm_age_2_3_years',
                type='Integer',
                name='Jobs at firms aged 2-3 Years',
                description='Number of jobs for workers at firms with Firm Age: 2-3 Years',
                weight=1,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_firm_age_4_5_years', OBSColumn(
                id='jobs_firm_age_4_5_years',
                type='Integer',
                name='Jobs at firms aged 4-5 Years',
                description='Number of jobs for workers at firms with Firm Age: 4-5 Years',
                weight=1,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_firm_age_6_10_years', OBSColumn(
                id='jobs_firm_age_6_10_years',
                type='Integer',
                name='Jobs at firms aged 6-10 years',
                description='Number of jobs for workers at firms with Firm Age: 6-10 Years',
                weight=1,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_firm_age_11_more_years', OBSColumn(
                id='jobs_firm_age_11_more_years',
                type='Integer',
                name='Jobs at firms aged 11+ Years',
                description='Number of jobs for workers at firms with Firm Age: 11+ Years',
                weight=1,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_firm_age_0_19_employees', OBSColumn(
                id='jobs_firm_age_0_19_employees',
                type='Integer',
                name='Jobs at firms with 0-19 Employees',
                description='Number of jobs for workers at firms with Firm Size: 0-19 Employees',
                weight=1,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_firm_age_20_49_employees', OBSColumn(
                id='jobs_firm_age_20_49_employees',
                type='Integer',
                name='Jobs at firms with 20-49 Employees',
                description='Number of jobs for workers at firms with Firm Size: 20-49 Employees',
                weight=1,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_firm_age_50_249_employees', OBSColumn(
                id='jobs_firm_age_50_249_employees',
                type='Integer',
                name='Jobs at firms with 0-249 Employees',
                description='Number of jobs for workers at firms with Firm Size: 50-249 Employees',
                weight=1,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_firm_age_250_499_employees', OBSColumn(
                id='jobs_firm_age_250_499_employees',
                type='Integer',
                name='Jobs at firms with 250-499 Employees',
                description='Number of jobs for workers at firms with Firm Size: 250-499 Employees',
                weight=1,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('jobs_firm_age_500_more_employees', OBSColumn(
                id='jobs_firm_age_500_or_more_employees',
                type='Integer',
                name='Jobs at firms with 500+ Employees',
                description='Number of jobs for workers at firms with Firm Size: 500+ Employees',
                weight=1,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['income_education_employment']]
            )),
            ('createdate', OBSColumn(
                id='createdate',
                type='Date',
                name='Date on which data was created, formatted as YYYYMMDD ',
                weight=0
            )),
        ])

class WorkplaceAreaCharacteristics(TableTask):

    force = BooleanParameter(default=False)
    year = Parameter(default=2013)

    def tablename(self):
        return '"{}".wac_{}'.format(classpath(self), self.year)


    def requires(self):
        for state in STATES - MISSING_STATES.get(self.year, set()):
            yield DownloadLODESFile(filetype='wac',
                                    part_or_segment='S000', # all jobs
                                    year=self.year,
                                    state=state)

    def run(self):
        # make the table
        cursor = pg_cursor()
        cursor.execute('CREATE SCHEMA IF NOT EXISTS "{schema}"'.format(
            schema=classpath(self)))
        cursor.execute('''
DROP TABLE IF EXISTS {tablename};
CREATE TABLE {tablename} (
    {columns}
)
                       '''.format(tablename=self.tablename(),
                                  columns=self.columns()))

        cursor.connection.commit()

        for infile in self.input():
            # gunzip each CSV into the table
            cmd = r"gunzip -c '{input}' | psql -c '\copy {tablename} FROM STDIN " \
                  r"WITH CSV HEADER'".format(input=infile.path, tablename=self.tablename())
            shell(cmd)

        self.output().touch()

    def output(self):
        output = DefaultPostgresTarget(table=self.tablename())
        if self.force:
            output.untouch()
        return output


class ResidenceAreaCharacteristics(Task):

    force = BooleanParameter(default=False)
    year = Parameter(default=2013)


    def requires(self):
        for state in STATES - MISSING_STATES.get(self.year, set()):
            for part in ('main', 'aux',):
                yield DownloadLODESFile(filetype='rac', year=self.year,
                                        state=state, part_or_segment=part)


class OriginDestination(Task):

    force = BooleanParameter(default=False)
    year = Parameter(default=2013)

    def tablename(self):
        return '"{}".od_{}'.format(classpath(self), self.year)

    def columns(self):
        return '''
work_census_block Text, -- Workplace Census Block Code
home_census_block Text, --   Residence Census Block Code
total_jobs INTEGER, -- Total number of jobs
jobs_age_29_or_younger INTEGER, -- Number of jobs of workers age 29 or younger 11
jobs_age_30_to_54 INTEGER, -- Number of jobs for workers age 30 to 54 11
jobs_age_55_or_older INTEGER, -- Number of jobs for workers age 55 or older 11
jobs_earning_15000_or_less INTEGER, --  Number of jobs with earnings $1250/month or less
jobs_earning_15001_to_40000 INTEGER, --  Number of jobs with earnings $1251/month to $3333/month
jobs_earning_40001_or_more INTEGER, --  Number of jobs with earnings greater than $3333/month
jobs_in_goods_production INTEGER, --   Number of jobs in Goods Producing industry sectors
jobs_in_trade_transport_and_util INTEGER, --   Number of jobs in Trade, Tr sportation, and Utilities industry sectors
jobs_in_all_other_service INTEGER, --   Number of jobs in All Other Services industry sectors
createdate DATE -- Date on which da ta was created, formatted as YYYYMMDD
'''

    def requires(self):
        for state in self.STATES - self.MISSING_STATES.get(self.year, set()):
            for part in ('main', 'aux',):
                yield DownloadLODESFile(filetype='od', year=self.year,
                                        state=state, part=part)

    def run(self):
        # make the table
        cursor = pg_cursor()
        cursor.execute('CREATE SCHEMA IF NOT EXISTS "{schema}"'.format(
            schema=classpath(self)))
        cursor.execute('''
DROP TABLE IF EXISTS {tablename};
CREATE TABLE {tablename} (
    {columns}
)
                       '''.format(tablename=self.tablename(),
                                  columns=self.columns()))

        cursor.connection.commit()

        for infile in self.input():
            # gunzip each CSV into the table
            cmd = r"gunzip -c '{input}' | psql -c '\copy {tablename} FROM STDIN " \
                  r"WITH CSV HEADER'".format(input=infile.path, tablename=self.tablename())
            shell(cmd)

        self.output().touch()

    def output(self):
        output = DefaultPostgresTarget(table=self.tablename())
        if self.force:
            output.untouch()
        return output
