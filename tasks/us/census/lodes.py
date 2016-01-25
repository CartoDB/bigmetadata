'''
tasks to download LODES origin-destination, residence, and workplace
characteristics files
'''

import os
import subprocess

from tasks.util import shell, DefaultPostgresTarget, pg_cursor, classpath
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

    # [TYPE] = Job Type, can have a value of "JT00" for All Jobs, "JT01" for Primary
    #          Jobs, "JT02" for All Private Jobs, "JT03" for Private Primary Jobs,
    #          "JT04" for All Federal Jobs, or "JT05" for Federal Primary Jobs.
    job_type = Parameter(default="JT00")

    # [PART] = Part of the state file, can have a value of either "main" or "aux".
    #          Complimentary parts of the state file, the main part includes jobs with both
    #          workplace and residence in the state and the aux part includes jobs with the
    #          workplace in the state and the residence outside of the state.
    part = Parameter()

    def filename(self):
        # Filename of the OD datasets are described by the following
        #          templates [ST]_od_[PART]_[TYPE]_[YEAR].csv.gz   where
        return '{}_{}_{}_{}_{}.csv.gz'.format(self.state, self.filetype, self.part,
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


class WorkplaceAreaCharacteristics(Task):

    force = BooleanParameter(default=False)
    year = Parameter(default=2013)

    def tablename(self):
        return '"{}".wac_{}'.format(classpath(self), self.year)

    def columns(self):
        return '''
work_census_block TEXT, --w_geocode       Char15  Workplace Census Block Code 
total_jobs INTEGER, -- C000    Num     Total number of jobs 
jobs_age_29_or_younger INTEGER, -- Number of jobs of workers age 29 or younger 11
jobs_age_30_to_54 INTEGER, -- Number of jobs for workers age 30 to 54 11
jobs_age_55_or_older INTEGER, -- Number of jobs for workers age 55 or older 11
jobs_earning_15000_or_less INTEGER, --  Number of jobs with earnings $1250/month or less
jobs_earning_15001_to_40000 INTEGER, --  Number of jobs with earnings $1251/month to $3333/month
jobs_earning_40001_or_more INTEGER, --  Number of jobs with earnings greater than $3333/month
jobs_11_agriculture_forestry_fishing INTEGER, -- CNS01   Num     Number of jobs in NAICS sector 11 (Agriculture, Forestry, Fishing and Hunting) 
jobs_21_mining_quarrying_oil-gas INTEGER, -- CNS02   Num     Number of jobs in NAICS sector 21 (Mining, Quarrying, and Oil and Gas Extraction) 
jobs_22_utilities INTEGER, --CNS03   Num     Number of jobs in NAICS sector 22 (Utilities) 
jobs_23_construction INTEGER, --CNS04   Num     Number of jobs in NAICS sector 23 (Construction) 
jobs_31_33_manufacturing INTEGER, --CNS05   Num     Number of jobs in NAICS sector 31-33 (Manufacturing) 
jobs_42_wholesale_trade INTEGER, -- CNS06   Num     Number of jobs in NAICS sector 42 (Wholesale Trade) 
jobs_44_45_retail_trade INTEGER, -- CNS07   Num     Number of jobs in NAICS sector 44-45 (Retail Trade) 
jobs_48_49_transport_warehousing INTEGER,  -- CNS08   Num     Number of jobs in NAICS sector 48-49 (Transportation and Warehousing) 
jobs_51_information INTEGER, -- CNS09   Num     Number of jobs in NAICS sector 51 (Information) 
jobs_52_finance_and_insurance INTEGER, -- CNS10   Num     Number of jobs in NAICS sector 52 (Finance and Insurance) 
jobs_53_real_estate_rental_leasing INTEGER, --CNS11   Num     Number of jobs in NAICS sector 53 (Real Estate and Rental and Leasing) 
jobs_54_professional_scientific_tech_services INTEGER, -- CNS12   Num     Number of jobs in NAICS sector 54 (Professional, Scientific, and Technical Services) 
jobs_55_management_of_companies_enterprises INTEGER, -- CNS13   Num     Number of jobs in NAICS sector 55 (Management of Companies and Enterprises) 
jobs_56_admin_support_waste_management INTEGER, -- CNS14   Num     Number of jobs in NAICS sector 56 (Administrative and Support and Waste Management and Remediation Services) 
jobs_61_educational_services INTEGER, -- CNS15   Num     Number of jobs in NAICS sector 61 (Educational Services) 
jobs_62_healthcare_social_assistance INTEGER, --CNS16   Num     Number of jobs in NAICS sector 62 (Health Care and Social Assistance) 
jobs_71_arts_entertainment_recreation INTEGER, -- CNS17   Num     Number of jobs in NAICS sector 71 (Arts, Entertainment, and Recreation) 
jobs_72_accommodation_and_food INTEGER, -- CNS18   Num     Number of jobs in NAICS sector 72 (Accommodation and Food Services) 
jobs_81_other_services_except_public_admin INTEGER, -- CNS19   Num     Number of jobs in NAICS sector 81 (Other Services [except Public Administration]) 
jobs_92_public_administration INTEGER, -- CNS20   Num     Number of jobs in NAICS sector 92 (Public Administration) 
jobs_white INTEGER, --CR01    Num     Number of jobs for workers with Race: White, Alone12 
jobs_black INTEGER, --CR02    Num     Number of jobs for workers with Race: Black or African American Alone12 
jobs_amerindian INTEGER, --CR03    Num     Number of jobs for workers with Race: American Indian or Alaska Native Alone12 
jobs_asian INTEGER, --CR04    Num     Number of jobs for workers with Race: Asian Alone12 
jobs_hawaiian INTEGER, -- 33      CR05    Num     Number of jobs for workers with Race: Native Hawaiian or Other Pacific Islander Alone12 
jobs_two_or_more_races INTEGER, --34      CR07    Num     Number of jobs for workers with Race: Two or More Race Groups12 
jobs_not_hispanic INTEGER, --35      CT01    Num     Number of jobs for workers with Ethnicity: Not Hispanic or Latino12 
jobs_hispanic INTEGER, --36      CT02    Num     Number of jobs for workers with Ethnicity: Hispanic or Latino12 
jobs_less_than_high_school INTEGER, --37      CD01    Num     Number of jobs for workers with Educational Attainment: Less than high school12,13 
jobs_high_school INTEGER, --38      CD02    Num     Number of jobs for workers with Educational Attainment: High school or equivalent, no college12,13 
jobs_some_college INTEGER, -- 39      CD03    Num     Number of jobs for workers with Educational Attainment: Some college or Associate degree12,13 
jobs_bachelors_or_advanced INTEGER, -- 40      CD04    Num     Number of jobs for workers with Educational Attainment: Bachelor's degree or advanced degree12,13 
jobs_male INTEGER, --41      CS01    Num     Number of jobs for workers with Sex: Male12 
jobs_female INTEGER, --42      CS02    Num     Number of jobs for workers with Sex: Female12 
43      CFA01   Num     Number of jobs for workers at firms with Firm Age: 0-1 Years14 
44      CFA02   Num     Number of jobs for workers at firms with Firm Age: 2-3 Years14 
45      CFA03   Num     Number of jobs for workers at firms with Firm Age: 4-5 Years14 
46      CFA04   Num     Number of jobs for workers at firms with Firm Age: 6-10 Years14 
47      CFA05   Num     Number of jobs for workers at firms with Firm Age: 11+ Years14 
48      CFS01   Num     Number of jobs for workers at firms with Firm Size: 0-19 Employees14,15 
49      CFS02   Num     Number of jobs for workers at firms with Firm Size: 20-49 Employees14,15 
50      CFS03   Num     Number of jobs for workers at firms with Firm Size: 50-249 Employees14,15 
51      CFS04   Num     Number of jobs for workers at firms with Firm Size: 250-499 Employees14,15 
52      CFS05   Num     Number of jobs for workers at firms with Firm Size: 500+ Employees14,15 
53      createdate      Char    Date on which data was created, formatted as YYYYMMDD 

'''

    def requires(self):
        for state in STATES - MISSING_STATES.get(self.year, set()):
            for part in ('main', 'aux',):
                yield DownloadLODESFile(filetype='wac', year=self.year,
                                        state=state, part=part)


class ResidenceAreaCharacteristics(Task):

    force = BooleanParameter(default=False)
    year = Parameter(default=2013)


    def requires(self):
        for state in STATES - MISSING_STATES.get(self.year, set()):
            for part in ('main', 'aux',):
                yield DownloadLODESFile(filetype='rac', year=self.year,
                                        state=state, part=part)


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
