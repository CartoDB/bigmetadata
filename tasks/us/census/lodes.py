'''
tasks to download LODES origin-destination, residence, and workplace
characteristics files
'''

import os
import subprocess

from collections import OrderedDict
from tasks.meta import OBSColumn, current_session, OBSTag
from tasks.base_tasks import (ColumnsTask, CSV2TempTableTask, TableTask, DownloadGUnzipTask,
                              TagsTask, MetaWrapper)
from tasks.util import shell, classpath
from tasks.tags import SectionTags, SubsectionTags, LicenseTags
from tasks.us.census.tiger import GeoidColumns, SumLevel, GEOID_SUMLEVEL_COLUMN, GEOID_SHORELINECLIPPED_COLUMN

from luigi import (Task, Parameter, LocalTarget, IntParameter)


STATES = set(["al", "ak", "az", "ar", "ca", "co", "ct", "de", "dc", "fl", "ga",
              "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md", "ma",
              "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny",
              "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd", "tn",
              "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy"])

MISSING_STATES = {
    2013: set(['ks', 'ma', 'pr'])
}

COLUMN_MAPPING = OrderedDict([
    ('total_jobs', 'C000'),
    ('jobs_age_29_or_younger', 'CA01'),
    ('jobs_age_30_to_54', 'CA02'),
    ('jobs_age_55_or_older', 'CA03'),
    ('jobs_earning_15000_or_less', 'CE01'),
    ('jobs_earning_15001_to_40000', 'CE02'),
    ('jobs_earning_40001_or_more', 'CE03'),
    ('jobs_11_agriculture_forestry_fishing', 'CNS01'),
    ('jobs_21_mining_quarrying_oil_gas', 'CNS02'),
    ('jobs_22_utilities', 'CNS03'),
    ('jobs_23_construction', 'CNS04'),
    ('jobs_31_33_manufacturing', 'CNS05'),
    ('jobs_42_wholesale_trade', 'CNS06'),
    ('jobs_44_45_retail_trade', 'CNS07'),
    ('jobs_48_49_transport_warehousing', 'CNS08'),
    ('jobs_51_information', 'CNS09'),
    ('jobs_52_finance_and_insurance', 'CNS10'),
    ('jobs_53_real_estate_rental_leasing', 'CNS11'),
    ('jobs_54_professional_scientific_tech_services', 'CNS12'),
    ('jobs_55_management_of_companies_enterprises', 'CNS13'),
    ('jobs_56_admin_support_waste_management', 'CNS14'),
    ('jobs_61_educational_services', 'CNS15'),
    ('jobs_62_healthcare_social_assistance', 'CNS16'),
    ('jobs_71_arts_entertainment_recreation', 'CNS17'),
    ('jobs_72_accommodation_and_food', 'CNS18'),
    ('jobs_81_other_services_except_public_admin', 'CNS19'),
    ('jobs_92_public_administration', 'CNS20'),
    ('jobs_white', 'CR01'),
    ('jobs_black', 'CR02'),
    ('jobs_amerindian', 'CR03'),
    ('jobs_asian', 'CR04'),
    ('jobs_hawaiian', 'CR05'),
    ('jobs_two_or_more_races', 'CR07'),
    ('jobs_not_hispanic', 'CT01'),
    ('jobs_hispanic', 'CT02'),
    ('jobs_less_than_high_school', 'CD01'),
    ('jobs_high_school', 'CD02'),
    ('jobs_some_college', 'CD03'),
    ('jobs_bachelors_or_advanced', 'CD04'),
    ('jobs_male', 'CS01'),
    ('jobs_female', 'CS02'),
])


class SourceTags(TagsTask):

    def version(self):
        return 1

    def tags(self):
        return [OBSTag(id='lehd-lodes',
                       name='',
                       type='source',
                       description='')]


class WorkplaceAreaCharacteristicsColumns(ColumnsTask):

    def requires(self):
        return {
            'tags': SubsectionTags(),
            'sections': SectionTags(),
            'license': LicenseTags(),
            'source': SourceTags(),
        }

    def version(self):
        return 3

    def columns(self):
        input_ = self.input()
        tags = input_['tags']
        source = input_['source']['lehd-lodes']
        license = input_['license']['no-restrictions']

        total_jobs = OBSColumn(
            type='Integer',
            name='Total Jobs',
            description='Total number of jobs',
            weight=8,
            aggregate='sum',
            tags=[tags['employment']]
        )
        cols = OrderedDict([
            #work_census_block TEXT, --w_geocode Char15 Workplace Census Block Code
            ('total_jobs', total_jobs),
            ('jobs_age_29_or_younger', OBSColumn(
                type='Integer',
                name='Jobs for workers age 29 or younger',
                description='Number of jobs of workers age 29 or younger',
                weight=3,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment'], tags['age_gender']]
            )),
            ('jobs_age_30_to_54', OBSColumn(
                type='Integer',
                name='Jobs for workers age 30 to 54',
                description='Number of jobs for workers age 30 to 54',
                weight=3,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment'], tags['age_gender']]
            )),
            ('jobs_age_55_or_older', OBSColumn(
                type='Integer',
                name='Jobs for workers age 55 or older',
                description='Number of jobs for workers age 55 or older',
                weight=3,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment'], tags['age_gender']]
            )),
            ('jobs_earning_15000_or_less', OBSColumn(
                type='Integer',
                name='Jobs earning up to $15,000 per year',
                description='Number of jobs with earnings $1250/month or less ($15,000 per year)',
                weight=3,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment'], tags['income']]
            )),
            ('jobs_earning_15001_to_40000', OBSColumn(
                type='Integer',
                name='Jobs earning $15,000 to $40,000 per year',
                description='Number of jobs with earnings $1251/month to $3333/month ($15,000 to $40,000 per year)',
                weight=5,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment'], tags['income']]
            )),
            ('jobs_earning_40001_or_more', OBSColumn(
                type='Integer',
                name='Jobs with earnings greater than $40,000 per year',
                description='Number of Jobs with earnings greater than $3333/month',
                weight=5,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment'], tags['income']]
            )),
            ('jobs_11_agriculture_forestry_fishing', OBSColumn(
                type='Integer',
                name='Agriculture, Forestry, Fishing and Hunting jobs',
                description='Number of jobs in NAICS sector 11 (Agriculture, Forestry, Fishing and Hunting)',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment']]
            )),
            ('jobs_21_mining_quarrying_oil_gas', OBSColumn(
                type='Integer',
                name='Mining, Quarrying, and Oil and Gas Extraction jobs',
                description='Number of jobs in NAICS sector 21 (Mining, Quarrying, and Oil and Gas Extraction) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment']]
            )),
            ('jobs_22_utilities', OBSColumn(
                type='Integer',
                name='Utilities Jobs',
                description='Number of jobs in NAICS sector 22 (Utilities) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment']]
            )),
            ('jobs_23_construction', OBSColumn(
                type='Integer',
                name='Construction Jobs',
                description='Number of jobs in NAICS sector 23 (Construction) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment']]
            )),
            ('jobs_31_33_manufacturing', OBSColumn(
                type='Integer',
                name='Manufacturing Jobs',
                description='Number of jobs in NAICS sector 31-33 (Manufacturing) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment']]
            )),
            ('jobs_42_wholesale_trade', OBSColumn(
                type='Integer',
                name='Wholesale Trade Jobs',
                description='Number of jobs in NAICS sector 42 (Wholesale Trade) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment']]
            )),
            ('jobs_44_45_retail_trade', OBSColumn(
                type='Integer',
                name='Retail Trade Jobs',
                description='Number of jobs in NAICS sector 44-45 (Retail Trade) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment']]
            )),
            ('jobs_48_49_transport_warehousing', OBSColumn(
                type='Integer',
                name='Transport and Warehousing Jobs',
                description='Number of jobs in NAICS sector 48-49 (Transportation and Warehousing) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment']]
            )),
            ('jobs_51_information', OBSColumn(
                type='Integer',
                name='Information Jobs',
                description='Number of jobs in NAICS sector 51 (Information) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment']]
            )),
            ('jobs_52_finance_and_insurance', OBSColumn(
                type='Integer',
                name='Finance and Insurance Jobs',
                description='Number of jobs in NAICS sector 52 (Finance and Insurance)',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment']]
            )),
            ('jobs_53_real_estate_rental_leasing', OBSColumn(
                type='Integer',
                name='Real Estate and Rental and Leasing Jobs',
                description='Number of jobs in NAICS sector 53 (Real Estate and Rental and Leasing) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment']]
            )),
            ('jobs_54_professional_scientific_tech_services', OBSColumn(
                type='Integer',
                name='Professional, Scientific, and Technical Services Jobs',
                description='Number of jobs in NAICS sector 54 (Professional, Scientific, and Technical Services) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment']]
            )),
            ('jobs_55_management_of_companies_enterprises', OBSColumn(
                type='Integer',
                name='Management of Companies and Enterprises Jobs',
                description='Number of jobs in NAICS sector 55 (Management of Companies and Enterprises) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment']]
            )),
            ('jobs_56_admin_support_waste_management', OBSColumn(
                type='Integer',
                name='Administrative and Support and Waste Management and Remediation Services Jobs',
                description='Number of jobs in NAICS sector 56 (Administrative and Support and Waste Management and Remediation Services) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment']]
            )),
            ('jobs_61_educational_services', OBSColumn(
                type='Integer',
                name='Educational Services Jobs',
                description='Number of jobs in NAICS sector 61 (Educational Services) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment']]
            )),
            ('jobs_62_healthcare_social_assistance', OBSColumn(
                type='Integer',
                name='Health Care and Social Assistance Jobs',
                description='Number of jobs in NAICS sector 62 (Health Care and Social Assistance) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment']]
            )),
            ('jobs_71_arts_entertainment_recreation', OBSColumn(
                type='Integer',
                name='Arts, Entertainment, and Recreation jobs',
                description='Number of jobs in NAICS sector 71 (Arts, Entertainment, and Recreation) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment']]
            )),
            ('jobs_72_accommodation_and_food', OBSColumn(
                type='Integer',
                name='Accommodation and Food Services jobs',
                description='Number of jobs in NAICS sector 72 (Accommodation and Food Services) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment']]
            )),
            ('jobs_81_other_services_except_public_admin', OBSColumn(
                type='Integer',
                name='Other Services (except Public Administration) jobs',
                description='Jobs in NAICS sector 81 (Other Services [except Public Administration])',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment']]
            )),
            ('jobs_92_public_administration', OBSColumn(
                type='Integer',
                name='Public Administration jobs',
                description='Number of jobs in NAICS sector 92 (Public Administration) ',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment']]
            )),
            ('jobs_white', OBSColumn(
                type='Integer',
                name='Jobs held by workers who are white',
                description='Number of jobs for workers with Race: White, Alone',
                weight=2,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment'], tags['race_ethnicity']]
            )),
            ('jobs_black', OBSColumn(
                type='Integer',
                name='Jobs held by workers who are black',
                description='Number of jobs for workers with Race: Black or African American Alone',
                weight=2,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment'], tags['race_ethnicity']]
            )),
            ('jobs_amerindian', OBSColumn(
                type='Integer',
                name='Jobs held by workers who are American Indian or Alaska Native Alone',
                description='Number of jobs for workers with Race: American Indian or Alaska Native Alone',
                weight=0,
                aggregate='sum',
            )),
            ('jobs_asian', OBSColumn(
                type='Integer',
                name='Jobs held by workers who are Asian',
                description='Number of jobs for workers with Race: Asian Alone',
                weight=2,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment'], tags['race_ethnicity']]
            )),
            ('jobs_hawaiian', OBSColumn(
                type='Integer',
                name='Jobs held by workers who are Native Hawaiian or Other Pacific Islander Alone',
                description='Number of jobs for workers with Race: Native Hawaiian or Other Pacific Islander Alone',
                weight=0,
                aggregate='sum',
            )),
            ('jobs_two_or_more_races', OBSColumn(
                type='Integer',
                name='Jobs held by workers who reported Two or More Race Groups',
                description='Number of jobs for workers with Race: Two or More Race Groups',
                weight=0,
                aggregate='sum',
            )),
            ('jobs_not_hispanic', OBSColumn(
                type='Integer',
                name='Jobs held by workers who are Not Hispanic or Latino',
                description='Number of jobs for workers with Ethnicity: Not Hispanic or Latino',
                weight=0,
                aggregate='sum',
            )),
            ('jobs_hispanic', OBSColumn(
                type='Integer',
                name='Jobs held by workers who are Hispanic or Latino',
                description='Number of jobs for workers with Ethnicity: Hispanic or Latino',
                weight=1,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment'], tags['race_ethnicity']]
            )),
            ('jobs_less_than_high_school', OBSColumn(
                type='Integer',
                name='Jobs held by workers who did not complete high school',
                description='Number of jobs for workers with Educational Attainment: Less than high school',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment'], tags['education']]
            )),
            ('jobs_high_school', OBSColumn(
                type='Integer',
                name='Jobs held by workers who completed high school',
                description='Number of jobs for workers with Educational Attainment: High school or equivalent, no college',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment'], tags['education']]
            )),
            ('jobs_some_college', OBSColumn(
                type='Integer',
                name='Jobs held by workers who completed some college or Associate degree',
                description='Number of jobs for workers with Educational Attainment: Some college or Associate degree',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment'], tags['education']]
            )),
            ('jobs_bachelors_or_advanced', OBSColumn(
                type='Integer',
                name='Jobs held by workers who obtained a Bachelor\'s degree or advanced degree',
                description='Number of jobs for workers with Educational Attainment: Bachelor\'s degree or advanced degree',
                weight=4,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['employment'], tags['education']]
            )),
            ('jobs_male', OBSColumn(
                type='Integer',
                name='Jobs held by men',
                description='Number of jobs for male workers',
                weight=2,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['age_gender'], tags['employment']]
            )),
            ('jobs_female', OBSColumn(
                type='Integer',
                name='Jobs held by women',
                description='Number of jobs for female workers',
                weight=2,
                aggregate='sum',
                targets={total_jobs: 'denominator'},
                tags=[tags['age_gender'], tags['employment']]
            )),
        ])
        for colname, col in cols.items():
            col.tags.append(source)
            col.tags.append(license)
        return cols


class DownloadUnzipLodes(DownloadGUnzipTask):
    year = IntParameter(default=2013)
    filetype = Parameter(default='rac')
    state = Parameter()
    partorsegment = Parameter(default='S000')
    jobtype = Parameter(default='JT00')

    def filename_lodes(self):
        #   [STATE]_[FILETYPE]_[PART/SEG]_[TYPE]_[YEAR].csv.gz   where
        return '{}_{}_{}_{}_{}.csv.gz'.format(self.state, self.filetype,
                                              self.partorsegment,
                                              self.jobtype, self.year)

    def url(self):
        return 'http://lehd.ces.census.gov/data/lodes/LODES7/{}/{}/{}'.format(
            self.state, self.filetype, self.filename_lodes())

    def download(self):
        shell('wget -O {output}.gz {url}'.format(output=self.output().path, url=self.url()))


class WorkplaceAreaCharacteristicsTemp(CSV2TempTableTask):

    year = IntParameter(default=2013)

    FILE_EXTENSION = 'csv'

    def requires(self):
        return [DownloadUnzipLodes(file_extension=self.FILE_EXTENSION,
                                   filetype='rac', year=self.year,
                                   state=state, partorsegment='S000',
                                   jobtype='JT00') for state in STATES - MISSING_STATES.get(self.year, set())]

    def input_csv(self):
        csvs = []
        for folder in self.input():
            for file in os.listdir(folder.path):
                if file.endswith('.{}'.format(self.FILE_EXTENSION)):
                    csvs.append(os.path.join(folder.path, file))
        return csvs

    def coldef(self):
        '''
        We are receiving a list of files so we need to implement the coldef method
        We are assuming that all the files have the same format (columns).
        '''
        csv = self.input_csv()[0]
        header_row = shell('head -n 1 "{csv}"'.format(csv=csv), encoding=self.encoding).strip()
        return [(h.replace('"', ''), 'Text') for h in header_row.split(self.delimiter)]


class WorkplaceAreaCharacteristics(TableTask):

    year = IntParameter(default=2013)

    def version(self):
        return 3

    def requires(self):
        return {
            'data_meta': WorkplaceAreaCharacteristicsColumns(),
            'tiger_meta': GeoidColumns(),
            'data': WorkplaceAreaCharacteristicsTemp(year=self.year),
        }

    def timespan(self):
        return str(self.year)

    def columns(self):
        data_columns = self.input()['data_meta']
        tiger_columns = self.input()['tiger_meta']
        cols = OrderedDict([
            ('w_geocode_sl', tiger_columns['block' + GEOID_SUMLEVEL_COLUMN]),
            ('w_geocode_sc', tiger_columns['block' + GEOID_SHORELINECLIPPED_COLUMN]),
        ])
        cols.update(data_columns)
        return cols

    def populate(self):
        input_ = self.input()
        insert = '''INSERT INTO {output} (w_geocode_sl, w_geocode_sc, {colnames})
                    SELECT h_geocode AS w_geocode_sl, h_geocode AS w_geocode_sc, {select_colnames}
                    FROM {input}'''.format(
                        output=self.output().table,
                        input=input_['data'].table,
                        colnames=', '.join(COLUMN_MAPPING.keys()),
                        select_colnames=', '.join(['"{column}"::integer'.format(column=column)
                                                  for column in COLUMN_MAPPING.values()]))
        current_session().execute(insert)


class LODESMetaWrapper(MetaWrapper):
    geography = Parameter()
    year = IntParameter()

    params = {
        'geography': ['block'],
        'year': [2013]
    }

    def tables(self):
        yield WorkplaceAreaCharacteristics()
        yield SumLevel(geography=self.geography, year=str(2015))
