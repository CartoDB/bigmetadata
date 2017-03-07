
'''
Define special segments for the census
'''

import os
import subprocess

from collections import OrderedDict
from tasks.meta import OBSColumn, OBSColumnToColumn, OBSColumnTag, current_session
from tasks.util import shell, classpath, ColumnsTask, TableTask, MetaWrapper
from tasks.us.census.tiger import GeoidColumns, SumLevel
from tasks.us.census.acs import ACSTags
from tasks.tags import SectionTags, SubsectionTags, UnitTags


from luigi import Task, Parameter, LocalTarget, BooleanParameter


class DownloadSpielmanSingletonFile(Task):

    def filename(self):
        return 'understanding-americas-neighborhoods-using-uncertain-data-from-the-american-community-survey-output-data.zip'

    def url(self):
        return 'https://www.openicpsr.org/repoEntity/download'

    def curl_request(self):
        docid = 41528
        email = 'slynn@cartodb.com'
        return 'curl -L {url} --data "downloaderEmail={email}&agree=I Agree&id={docid}"'.format(
            docid = docid,
            email=email,
            url= self.url());

    def run(self):
        self.output().makedirs()
        try:
            self.download();
        except subprocess.CalledProcessError:
            shell('rm -f {target}'.format(target=self.output().path))
            raise

    def download(self):
        print('running ')
        print('curl -L {url} -o {target}'.format(url=self.url(), target=self.output().path))
        shell('{curl} -o {target}'.format(
            curl=self.curl_request(),
            target=self.output().path))

    def output(self):
        return LocalTarget(path=os.path.join(classpath(self), self.filename()))

class ProcessSpielmanSingletonFile(Task):

    def requires(self):
        return {'downloaded_file' :DownloadSpielmanSingletonFile()}

    def run(self):
        try:
            print 'decompressing'
            self.decompress()
            print 'renaming'
            self.rename_files()
            print 'converting to csv'
            self.convert_to_csv()
            print 'parsing'
            self.grab_relevant_columns()
            print 'cleanup'
            self.cleanup()

        except subprocess.CalledProcessError:
            print 'problem encountered exiting  '
            self.cleanup()
            raise

    def cleanup(self):
        shell('rm -rf {decompressed_folder}'.format(decompressed_folder=self.decompressed_folder()))
        shell('rm -rf {decompressed_folder}'.format(decompressed_folder=self.decompressed_folder()))
        shell('rm -f  {target}'.format(target=self.output().path.replace('.csv','_tmp.csv')))

    def filename(self):
        return self.input()['downloaded_file'].path.split("/")[-1].replace('.zip','.csv')

    def grab_relevant_columns(self):
        shell("awk -F',' 'BEGIN{{OFS=\",\"}}{{print $5, $159, $160, $161, $162}}' {source}  |  sed -e '1s/^.*$/GEOID10,X10,X31,X55,X2/' > {target}".format(
            source =self.output().path.replace('.csv','_tmp.csv') , target=self.output().path))

    def decompressed_folder(self):
        return self.input()['downloaded_file'].path.replace('.zip', '')

    def decompress(self):
        shell('unzip {target} -d {dest}'.format(target=self.input()['downloaded_file'].path,dest=self.decompressed_folder()))

    def convert_to_csv(self):
        shell('ogr2ogr -f "CSV" {target} {decompressed_folder}/US_tract_clusters_new.shp'.format(
            target=self.output().path.replace('.csv','_tmp.csv'),
            decompressed_folder=self.decompressed_folder()))

    def rename_files(self):
        rename = {
            'ustractclustersnew-41530.bin' : 'US_tract_clusters_new.dbf',
            'ustractclustersnew-41538.txt' : 'US_tract_clusters_new.prj',
            'ustractclustersnew-41563.bin' : 'US_tract_clusters_new.shp',
            'ustractclustersnew-41555.bin' : 'US_tract_clusters_new.shx'
        }
        for old_name, new_name in rename.iteritems():
            shell('mv {folder}/{old_name} {folder}/{new_name}'.format(
                old_name=old_name,
                new_name=new_name,
                folder=self.decompressed_folder()))

    def output(self):
        return LocalTarget(path=os.path.join('tmp', classpath(self), self.filename()))

class SpielmanSingletonTable(TableTask):

    def requires(self):
        return {
            'columns'   : SpielmanSingletonColumns(),
            'data_file' : ProcessSpielmanSingletonFile(),
            'tiger'     : GeoidColumns()
        }

    def version(self):
        return 8

    def timespan(self):
        return '2010 - 2014'

    def populate(self):
        table_name = self.output().table
        shell(r"psql -c '\copy {table} FROM {file_path} WITH CSV HEADER'".format(
            table=table_name,
            file_path=self.input()['data_file'].path
        ))
        for name, segment_id in SpielmanSingletonColumns.x10_mapping.iteritems():
            current_session().execute("update {table} set X10 = '{name}' "
                                      "where X10 ='{segment_id}'; ".format(
                                          table=table_name,
                                          name=name,
                                          segment_id=segment_id
                                      ))

        for name, segment_id in SpielmanSingletonColumns.x55_mapping.iteritems():
            current_session().execute("update {table} set X55 = '{name}' "
                                      "where X55 ='{segment_id}'; ".format(
                                          table=table_name,
                                          name=name,
                                          segment_id=segment_id
                                      ))

    def columns(self):
        columns = OrderedDict({
            'geoid': self.input()['tiger']['census_tract_geoid']
        })
        columns.update(self.input()['columns'])
        return columns

class SpielmanSingletonColumns(ColumnsTask):

    x10_mapping = {
        'Hispanic and Young' : 1,
        'Wealthy Nuclear Families' : 2,
        'Middle Income, Single Family Home' : 3,
        'Native American' : 4,
        'Wealthy, urban without Kids' : 5,
        'Low income and diverse' : 6,
        'Wealthy Old Caucasian ' : 7,
        'Low income, mix of minorities' : 8,
        'Low income, African American' : 9,
        'Residential Institutions' : 10
    }

    x55_mapping = {
        "Middle Class, Educated, Suburban, Mixed Race": 1,
        "Low Income on Urban Periphery": 3,
        "Suburban, Young and Low-income": 19,
        "low-income, urban, young, unmarried": 32,
        "Low education, mainly suburban": 34,
        "Young, working class and rural": 38,
        "Low-Income with gentrification": 54,
        "High school education Long Commuters, Black, White Hispanic mix": 10,
        "Non-urban, Bachelors or college degree, Rent owned mix": 25,
        "Rural,High School Education, Owns property": 28,
        "Young, City based renters in Sparse neighborhoods, Low poverty": 50,
        "Predominantly black, high high school attainment, home owners": 16,
        "White and minority mix multilingual, mixed income / education. Married": 29,
        "Hispanic Black mix multilingual, high poverty, renters, uses public transport": 33,
        "Predominantly black renters, rent own mix": 41,
        "Lower Middle Income with higher rent burden": 5,
        "Black and mixed community with rent burden": 6,
        "Lower Middle Income with affordable housing": 9,
        "Relatively affordable, satisfied lower middle class": 12,
        "Satisfied Lower Middle Income Higher Rent Costs": 20,
        "Suburban/Rural Satisfied, decently educated lower middle class": 23,
        "Struggling lower middle class with rent burden": 24,
        "Older white home owners, less comfortable financially": 43,
        "Older home owners, more financially comfortable, some diversity": 55,
        "Younger, poorer,single parent family Native Americans": 7,
        "Older, middle income Native Americans once married and Educated": 17,
        "Older, mixed race professionals": 14,
        "Works from home, Highly Educated, Super Wealthy": 21,
        "Retired Grandparents": 35,
        "Wealthy and Rural Living": 40,
        "Wealthy, Retired Mountains/Coasts": 48,
        "Wealthy Diverse Suburbanites On the Coasts": 49,
        "Retirement Communitties": 53,
        "Urban - Inner city": 26,
        "Rural families": 31,
        "College towns": 30,
        "College town with poverty": 44,
        "University campus wider area": 46,
        "City Outskirt University Campuses": 51,
        "City Center University Campuses": 52,
        "Lower educational attainment, Homeowner, Low rent": 2,
        "Younger, Long Commuter in dense neighborhood": 4,
        "Long commuters White black mix": 13,
        "Low rent in built up neighborhoods": 18,
        "Renters within cities, mixed income areas, White/Hispanic mix, Unmarried": 22,
        "Mix of older home owners with middle income and farmers": 27,
        "Older home owners and very high income": 36,
        "White Asian Mix Big City Burbs Dwellers": 37,
        "Bachelors degree Mid income With Mortgages": 39,
        "Asian Hispanic Mix, Mid income": 45,
        "Bachelors degree Higher income Home Owners": 47,
        "Wealthy city commuters": 8,
        "New Developments": 11,
        "Wealthy transplants displacing long-term local residents": 15,
        "High rise, dense urbanites": 42
    }

    x55_categories = OrderedDict([
        ("Middle Class, Educated, Suburban, Mixed Race", {'description' :'', 'details' : {}}),
        ("Low Income on Urban Periphery", {'description' :'', 'details' : {}}),
        ("Suburban, Young and Low-income", {'description' :'', 'details' : {}}),
        ("low-income, urban, young, unmarried", {'description' :'', 'details' : {}}),
        ("Low education, mainly suburban", {'description' :'', 'details' : {}}),
        ("Young, working class and rural", {'description' :'', 'details' : {}}),
        ("Low-Income with gentrification", {'description' :'', 'details' : {}}),
        ("High school education Long Commuters, Black, White Hispanic mix", {'description' :'', 'details' : {}}),
        ("Rural, Bachelors or college degree, Rent owned mix", {'description' :'', 'details' : {}}),
        ("Rural,High School Education, Owns property", {'description' :'', 'details' : {}}),
        ("Young, City based renters in Sparse neighborhoods, Low poverty", {'description' :'', 'details' : {}}),
        ("Predominantly black, high high school attainment, home owners", {'description' :'', 'details' : {}}),
        ("White and minority mix multilingual, mixed income / education. Married", {'description' :'', 'details' : {}}),
        ("Hispanic Black mix multilingual, high poverty, renters, uses public transport", {'description' :'', 'details' : {}}),
        ("Predominantly black renters, rent own mix", {'description' :'', 'details' : {}}),
        ("Lower Middle Income with higher rent burden", {'description' :'', 'details' : {}}),
        ("Black and mixed community with rent burden", {'description' :'', 'details' : {}}),
        ("Lower Middle Income with affordable housing", {'description' :'', 'details' : {}}),
        ("Relatively affordable, satisfied lower middle class", {'description' :'', 'details' : {}}),
        ("Satisfied Lower Middle Income Higher Rent Costs", {'description' :'', 'details' : {}}),
        ("Suburban/Rural Satisfied, decently educated lower middle class", {'description' :'', 'details' : {}}),
        ("Struggling lower middle class with rent burden", {'description' :'', 'details' : {}}),
        ("Older white home owners, less comfortable financially", {'description' :'', 'details' : {}}),
        ("Older home owners, more financially comfortable, some diversity", {'description' :'', 'details' : {}}),
        ("Younger, poorer,single parent family Native Americans", {'description' :'', 'details' : {}}),
        ("Older, middle income Native Americans once married and Educated", {'description' :'', 'details' : {}}),
        ("Older, mixed race professionals", {'description' :'', 'details' : {}}),
        ("Works from home, Highly Educated, Super Wealthy", {'description' :'', 'details' : {}}),
        ("Retired Grandparents", {'description' :'', 'details' : {}}),
        ("Wealthy and Rural Living", {'description' :'', 'details' : {}}),
        ("Wealthy, Retired Mountains/Coasts", {'description' :'', 'details' : {}}),
        ("Wealthy Diverse Suburbanites On the Coasts", {'description' :'', 'details' : {}}),
        ("Retirement Communitties", {'description' :'', 'details' : {}}),
        ("Urban - Inner city", {'description' :'', 'details' : {}}),
        ("Rural families", {'description' :'', 'details' : {}}),
        ("College towns", {'description' :'', 'details' : {}}),
        ("College town with poverty", {'description' :'', 'details' : {}}),
        ("University campus wider area", {'description' :'', 'details' : {}}),
        ("City Outskirt University Campuses", {'description' :'', 'details' : {}}),
        ("City Center University Campuses", {'description' :'', 'details' : {}}),
        ("Lower educational attainment, Homeowner, Low rent", {'description' :'', 'details' : {}}),
        ("Younger, Long Commuter in dense neighborhood", {'description' :'', 'details' : {}}),
        ("Long commuters White black mix", {'description' :'', 'details' : {}}),
        ("Low rent in built up neighborhoods", {'description' :'', 'details' : {}}),
        ("Renters within cities, mixed income areas, White/Hispanic mix, Unmarried", {'description' :'', 'details' : {}}),
        ("Older Home owners with high income", {'description' :'', 'details' : {}}),
        ("Older home owners and very high income", {'description' :'', 'details' : {}}),
        ("White, Asian Mix Big City Burbs Dwellers", {'description' :'', 'details' : {}}),
        ("Bachelors degree Mid income With Mortgages", {'description' :'', 'details' : {}}),
        ("Asian Hispanic Mix, Mid income", {'description' :'', 'details' : {}}),
        ("Bachelors degree Higher income Home Owners", {'description' :'', 'details' : {}}),
        ("Wealthy city commuters", {'description' :'', 'details' : {}}),
        ("New Developments", {'description' :'', 'details' : {}}),
        ("Very wealthy, multiple million dollar homes", {'description' :'', 'details' : {}}),
        ("High rise, dense urbanites", {'description' :'', 'details' : {}}),
    ])

    x10_categories = OrderedDict([
        ('Hispanic and Young', {
            'description' : 'Predominantly Hispanic, tends to have at most high school education, '
                'lots of married couples and single mothers, high mobility within cities, '
                'typically takes public transit and tends to rent.',
            'details' : {
                "Education":{
                  "Less than highschool": "High",
                  "High School": "Average",
                  "Some College / Assc degree" :"Low",
                  "Bachelor Degree" :"Low",
                  "Grad Degree" :"Low"
                },
                "Family Structure":{
                  "Single Mothers" : "High",
                  "Married Couples" : "Average",
                  "Male-Male Couples": "Low",
                  "Female-Female Couples" : "Low"
                },
                "Residential Stability":{
                  "Moved home within city in the last year" : "High",
                  "Moved city in the last year" : "Low"
                },
                "Language":{
                  "Speaks english only" : "Low",
                  "Speaks Spanish" : "High",
                  "Speaks Spanish Low English" : "High"
                },
                "Ethnicity":{
                  "White" : "Low",
                  "Black" : "Low",
                  "American Indian": "Average",
                  "Asian": "Low",
                  "Hispanic Latino" : "High",
                  "Not a Citizen" : "High"
                },
                "Wealth":{
                  "Income Gini": "Average",
                  "Public Assistance": "High",
                  "Retirement Income" : "Low"
                },
                "Commuting":{
                  "No Car" : "High",
                  "Public Transport" : "High",
                  "Commute 5-9 mins":"Low",
                  "Commute 10-14 mins":"Average",
                  "Commute 15-19 mins":"Avarage",
                  "Commute 20-24 mins":"Average",
                  "Commute 25-29 mins":"Low",
                  "Commute 30-34 mins":"High",
                  "Commute 35-39 mins":"Low",
                  "Commute 40-44 mins":"Average",
                  "Commute 45-59 mins":"Average",
                  "Commute 60-89 mins":"High",
                  "Commute 90+ mins":"High"
                },
                "Housing":{
                  "Median rent": "Average",
                  "Housing lower value quantile" : "Low",
                  "Housing median value" : "Low",
                  "Housing upper value quantile" : "Low",
                  "Vacancy" : "Low",
                  "Housing Upper Value Quantile":"Low",
                  "Renter Occupied" : "High",
                  "Single Family Detached" : "Low",
                  "Single Family Attached" : "Low",
                  "Building with 2 Units" : "High",
                  "3-4 Units" : "High",
                  "5-9 Units" : "High",
                  "10-20 Units" : "High",
                  "20-49 Units": "High",
                  "50+ Units" : "Low",
                  "Mobile Homes" : "Average",
                  "Built 2005 or later" : "Low",
                  "Built 2000-2004": "Low",
                  "Built earlier than 1939" : "Low"
                  }
               }
            }
         ),
        ('Wealthy Nuclear Families', {
         'description' : 'Highly educated, high levels of bachelor or grad degrees, '
            'married couples and some same sex couples, tends to stay put, white or asian, '
            'high income, tends to own a car and lives in single family homes with high rents.',
         'details':{
             "Education":{
              "Less than highschool": "Low",
              "High School": "Low",
              "Some College / Assc degree" :"Average",
              "Bachelor Degree" :"High",
              "Grad Degree" :"High"
            },
            "Family Structure":{
              "Single Mothers" : "Low",
              "Married Couples" : "High",
              "Male-Male Couples": "Average",
              "Female-Female Couples" : "Average"
            },
            "Residential Stability":{
              "Moved home within city in the last year" : "Low",
              "Moved city in the last year" : "Average"
            },
            "Language":{
              "Speaks english only" : "Average",
              "Speaks Spanish" : "Low",
              "Speaks Spanish Low English" : "Average"
            },
            "Ethnicity":{
              "White" : "Average",
              "Black" : "Low",
              "American Indian": "Low",
              "Asian": "High",
              "Hispanic Latino" : "Low",
              "Not a Citizen" : "Low"
            },
            "Wealth":{
              "Income Gini": "Average",
              "Public Assistance": "Low",
              "Retirement Income" : "Average"
            },
            "Commuting":{
              "No Car" : "Low",
              "Public Transport" : "Low",
              "Commute 5-9 mins":"Low",
              "Commute 10-14 mins":"Low",
              "Commute 15-19 mins":"Low",
              "Commute 20-24 mins":"Average",
              "Commute 25-29 mins":"High",
              "Commute 30-34 mins":"Average",
              "Commute 35-39 mins":"High",
              "Commute 40-44 mins":"High",
              "Commute 45-59 mins":"High",
              "Commute 60-89 mins":"High",
              "Commute 90+ mins":"Average"
            },
            "Housing":{
              "Median rent": "High",
              "Housing lower value quantile" : "High",
              "Housing median value" : "High",
              "Housing upper value quantile" : "High",
              "Vacancy" : "Low",
              "Housing Upper Value Quantile":"Low" ,
              "Renter Occupied" : "Low",
              "Single Family Detached" : "High",
              "Single Family Attached" : "High",
              "Building with 2 Units" : "Low",
              "3-4 Units" : "Low",
              "5-9 Units" : "Low",
              "10-20 Units" : "Low",
              "20-49 Units": "Low",
              "50+ Units" : "Low",
              "Mobile Homes" : "Low",
              "Built 2005 or later" : "High",
              "Built 2000-2004": "High",
              "Built earlier than 1939" : "Low"
              }
           }
        }),
        ('Middle Income, Single Family Home', {
            'description' : 'High school and some college education, mix of '
            'single parents and married couples, tends to stay put and only '
            'moves within cities, English speaking, white, good retirement '
            'funds, owns their own car, lives in lower rent homes of mobile homes.',
            'details' : {
            "Education":{
              "Less than highschool": "Average",
              "High School": "High",
              "Some College / Assc degree" :"Average",
              "Bachelor Degree" :"Low",
              "Grad Degree" :"Low"
            },
            "Family Structure":{
              "Single Mothers" : "Average",
              "Married Couples" : "Average",
              "Male-Male Couples": "Low",
              "Female-Female Couples" : "Average"
            },
            "Residential Stability":{
              "Moved home within city in the last year" : "Low",
              "Moved city in the last year" : "Average"
            },
            "Language":{
              "Speaks english only" : "High",
              "Speaks Spanish" : "Low",
              "Speaks Spanish Low English" : "Low"
            },
            "Ethnicity":{
              "White" : "High",
              "Black" : "Low",
              "American Indian": "Low",
              "Asian": "Low",
              "Hispanic Latino" : "Low",
              "Not a Citizen" : "Low"
            },
            "Wealth":{
              "Income Gini": "Average",
              "Public Assistance": "Average",
              "Retirement Income" : "High"
            },
            "Commuting":{
              "No Car" : "Low",
              "Public Transport" : "Low",
              "Commute 5-9 mins":"High",
              "Commute 10-14 mins":"Average",
              "Commute 15-19 mins":"Average",
              "Commute 20-24 mins":"Average",
              "Commute 25-29 mins":"Average",
              "Commute 30-34 mins":"Average",
              "Commute 35-39 mins":"Low",
              "Commute 40-44 mins":"Average",
              "Commute 45-59 mins":"Low",
              "Commute 60-89 mins":"Low",
              "Commute 90+ mins":"Low",
            },
            "Housing":{
              "Median rent": "Low",
              "Housing lower value quantile" : "Low",
              "Housing median value" : "Low",
              "Housing upper value quantile" : "Low",
              "Vacancy" : "Average",
              "Housing Upper Value Quantile":"Low" ,
              "Renter Occupied" : "High",
              "Single Family Detached" : "Low",
              "Single Family Attached" : "Low",
              "Building with 2 Units" : "Low",
              "3-4 Units" : "Low",
              "5-9 Units" : "Low",
              "10-20 Units" : "Low",
              "20-49 Units": "Low",
              "50+ Units" : "Low",
              "Mobile Homes" : "High",
              "Built 2005 or later" : "Low",
              "Built 2000-2004": "Low",
              "Built earlier than 1939" : "Average"
              }
           }
        }),
        ('Native American', {
            'description' : 'Native Amerian, high school educated with some '
            'college, commutes for work, single parent families, typical '
            'doesn\'t own a car and lives in low cost rental housing.',
            'details' : {
            "Education":{
              "Less than highschool": "High",
              "High School": "High",
              "Some College / Assc degree" :"Average",
              "Bachelor Degree" :"Low",
              "Grad Degree" :"Low"
            },
            "Family Structure":{
              "Single Mothers" : "High",
              "Married Couples" : "Low",
              "Male-Male Couples": "Low",
              "Female-Female Couples" : "Low"
            },
            "Residential Stability":{
              "Moved home within city in the last year" : "Low",
              "Moved city in the last year" : "Average"
            },
            "Language":{
              "Speaks english only" : "Average",
              "Speaks Spanish" : "Low",
              "Speaks Spanish Low English" : "Low"
            },
            "Ethnicity":{
              "White" : "Low",
              "Black" : "Low",
              "American Indian": "High",
              "Asian": "Low",
              "Hispanic Latino" : "Low",
              "Not a Citizen" : "Low"
            },
            "Wealth":{
              "Income Gini": "Average",
              "Public Assistance": "High",
              "Retirement Income" : "Low"
            },
            "Commuting":{
              "No Car" : "High",
              "Public Transport" : "Low",
              "Commute 5-9 mins":"High",
              "Commute 10-14 mins":"Average",
              "Commute 15-19 mins":"Low",
              "Commute 20-24 mins":"Low",
              "Commute 25-29 mins":"Low",
              "Commute 30-34 mins":"Low",
              "Commute 35-39 mins":"Low",
              "Commute 40-44 mins":"Low",
              "Commute 45-59 mins":"Low",
              "Commute 60-89 mins":"Low",
              "Commute 90+ mins":"High"
            },
            "Housing":{
              "Median rent": "Low",
              "Housing lower value quantile" : "Low",
              "Housing median value" : "Low",
              "Housing upper value quantile" : "Low",
              "Vacancy" : "High",
              "Housing Upper Value Quantile":"Average" ,
              "Renter Occupied" : "High",
              "Single Family Detached" : "Low",
              "Single Family Attached" : "Low",
              "Building with 2 Units" : "Low",
              "3-4 Units" : "Low",
              "5-9 Units" : "Low",
              "10-20 Units" : "Low",
              "20-49 Units": "Low",
              "50+ Units" : "Low",
              "Mobile Homes" : "High",
              "Built 2005 or later" : "Low",
              "Built 2000-2004": "Low",
              "Built earlier than 1939" : "Low"
            }
         }
      }),
        ('Wealthy, urban without Kids',{
            'description' : 'Typically has an advanced degree, single parents '
            'or same sex couples, highly mobile, White, Black or Asian, '
            'doesn\'t own a car and lives in high rental housing.',
            'details' : {
             "Education":{
              "Less than highschool": "Low",
              "High School": "Low",
              "Some College / Assc degree" :"Low",
              "Bachelor Degree" :"High",
              "Grad Degree" :"High"
            },
            "Family Structure":{
              "Single Mothers" : "High",
              "Married Couples" : "Low",
              "Male-Male Couples": "High",
              "Female-Female Couples" : "High"
            },
            "Residential Stability":{
              "Moved home within city in the last year" : "High",
              "Moved city in the last year" : "High"
            },
            "Language":{
              "Speaks english only" : "Average",
              "Speaks Spanish" : "Average",
              "Speaks Spanish Low English" : "Low"
            },
            "Ethnicity":{
              "White" : "Average",
              "Black" : "Average",
              "American Indian": "Low",
              "Asian": "High",
              "Hispanic Latino" : "Low",
              "Not a Citizen" : "High"
            },
            "Wealth":{
              "Income Gini": "High",
              "Public Assistance": "Low",
              "Retirement Income" : "Low"
            },
            "Commuting":{
              "No Car" : "High",
              "Public Transport" : "High",
              "Commute 5-9 mins":"Low",
              "Commute 10-14 mins":"Average",
              "Commute 15-19 mins":"Average",
              "Commute 20-24 mins":"Average",
              "Commute 25-29 mins":"Average",
              "Commute 30-34 mins":"High",
              "Commute 35-39 mins":"Average",
              "Commute 40-44 mins":"High",
              "Commute 45-59 mins":"High",
              "Commute 60-89 mins":"Average",
              "Commute 90+ mins":"Low",
            },
            "Housing":{
              "Median rent": "High",
              "Housing lower value quantile" : "High",
              "Housing median value" : "High",
              "Housing upper value quantile" : "High",
              "Vacancy" : "Average",
              "Renter Occupied" : "High",
              "Single Family Detached" : "Low",
              "Single Family Attached" : "High",
              "Building with 2 Units" : "High",
              "3-4 Units" : "High",
              "5-9 Units" : "High",
              "10-20 Units" : "High",
              "20-49 Units": "High",
              "50+ Units" : "High",
              "Mobile Homes" : "Low",
              "Built 2005 or later" : "High",
              "Built 2000-2004": "Average",
              "Built earlier than 1939" : "High"
             }
          }
        }),
        ('Low income and diverse', {
            'description' : 'High school education, single parent families, '
            'mobile within cities, multilingual, Black or Asian, typically '
            'doesn\'t own a car, uses public transit, high median rent. ',
            'details' : {
            "Education":{
              "Less than highschool": "High",
              "High School": "Average",
              "Some College / Assc degree" :"Low",
              "Bachelor Degree" :"Low",
              "Grad Degree" :"Low"
            },
            "Family Structure":{
              "Single Mothers" : "High",
              "Married Couples" : "Low",
              "Male-Male Couples": "Low",
              "Female-Female Couples" : "Low"
            },
            "Residential Stability":{
              "Moved home within city in the last year" : "High",
              "Moved city in the last year" : "Low"
            },
            "Language":{
              "Speaks english only" : "Low",
              "Speaks Spanish" : "High",
              "Speaks Spanish Low English" : "High"
            },
            "Ethnicity":{
              "White" : "Low",
              "Black" : "High",
              "American Indian": "Low",
              "Asian": "High",
              "Hispanic Latino" : "High",
              "Not a Citizen" : "High"
            },
            "Wealth":{
              "Income Gini": "Average",
              "Public Assistance": "High",
              "Retirement Income" : "Low"
            },
            "Commuting":{
              "No Car" : "High",
              "Public Transport" : "High",
              "Commute 5-9 mins":"Low",
              "Commute 10-14 mins":"Low",
              "Commute 15-19 mins":"Low",
              "Commute 20-24 mins":"Low",
              "Commute 25-29 mins":"Low",
              "Commute 30-34 mins":"High",
              "Commute 35-39 mins":"Average",
              "Commute 40-44 mins":"High",
              "Commute 45-59 mins":"High",
              "Commute 60-89 mins":"High",
              "Commute 90+ mins":"High"
            },
            "Housing":{
              "Median rent": "Average",
              "Housing lower value quantile" : "Low",
              "Housing median value" : "Low",
              "Housing upper value quantile" : "Low",
              "Vacancy" : "High",
              "Renter Occupied" : "High",
              "Single Family Detached" : "Low",
              "Single Family Attached" : "High",
              "Building with 2 Units" : "Average",
              "3-4 Units" : "High",
              "5-9 Units" : "High",
              "10-20 Units" : "High",
              "20-49 Units": "High",
              "50+ Units" : "High",
              "Mobile Homes" : "Low",
              "Built 2005 or later" : "Average",
              "Built 2000-2004": "Average",
              "Built earlier than 1939" : "High"
              }
           }
        }),
        ('Wealthy Old Caucasion', {
            'description' : 'Highly educated, married couples and same sex '
            'couples, tend to stay put, English speaking, White or Asian, high '
            'income and high rent.',
            'details' : {
             "Education":{
              "Less than highschool": "Low",
              "High School": "Low",
              "Some College / Assc degree" :"Average",
              "Bachelor Degree" :"High",
              "Grad Degree" :"High"
            },
            "Family Structure":{
              "Single Mothers" : "Low",
              "Married Couples" : "Average",
              "Male-Male Couples": "High",
              "Female-Female Couples" : "Average"
            },
            "Residential Stability":{
              "Moved home within city in the last year" : "Low",
              "Moved city in the last year" : "Average"
            },
            "Language":{
              "Speaks english only" : "High",
              "Speaks Spanish" : "Low",
              "Speaks Spanish Low English" : "Low"
            },
            "Ethnicity":{
              "White" : "High",
              "Black" : "Low",
              "American Indian": "Low",
              "Asian": "Average",
              "Hispanic Latino" : "Low",
              "Not a Citizen" : "Low"
            },
            "Wealth":{
              "Income Gini": "Average",
              "Public Assistance": "Low",
              "Retirement Income" : "High"
            },
            "Commuting":{
              "No Car" : "Average",
              "Public Transport" : "High",
              "Commute 5-9 mins":"Average",
              "Commute 10-14 mins":"Average",
              "Commute 15-19 mins":"Average",
              "Commute 20-24 mins":"Average",
              "Commute 25-29 mins":"Average",
              "Commute 30-34 mins":"Average",
              "Commute 35-39 mins":"Average",
              "Commute 40-44 mins":"Average",
              "Commute 45-59 mins":"Average",
              "Commute 60-89 mins":"Average",
              "Commute 90+ mins":"Average",
            },
            "Housing":{
              "Median rent": "High",
              "Housing lower value quantile" : "High",
              "Housing median value" : "High",
              "Housing upper value quantile" : "High",
              "Vacancy" : "High",
              "Renter Occupied" : "Low",
              "Single Family Detached" : "Low",
              "Single Family Attached" : "High",
              "Building with 2 Units" : "Low",
              "3-4 Units" : "Low",
              "5-9 Units" : "Average",
              "10-20 Units" : "Average",
              "20-49 Units": "High",
              "50+ Units" : "High",
              "Mobile Homes" : "Low",
              "Built 2005 or later" : "Average",
              "Built 2000-2004": "High",
              "Built earlier than 1939" : "Low"
              }
           }
        }),
        ('Low income, mix of minorities', {
            'description' : 'Highschool education, single parent familes, '
            'mobile within cities, diverse minorities, uses public transit '
            'and low rent.',
            'details' : {
            "Education":{
              "Less than highschool": "High",
              "High School": "Average",
              "Some College / Assc degree" :"Low",
              "Bachelor Degree" :"Low",
              "Grad Degree" :"Low"
            },
            "Family Structure":{
              "Single Mothers" : "High",
              "Married Couples" : "Low",
              "Male-Male Couples": "Low",
              "Female-Female Couples" : "Low"
            },
            "Residential Stability":{
              "Moved home within city in the last year" : "High",
              "Moved city in the last year" : "Low"
            },
            "Language":{
              "Speaks english only" : "Low",
              "Speaks Spanish" : "High",
              "Speaks Spanish Low English" : "High"
            },
            "Ethnicity":{
              "White" : "Low",
              "Black" : "High",
              "American Indian": "Low",
              "Asian": "High",
              "Hispanic Latino" : "High",
              "Not a Citizen" : "High"
            },
            "Wealth":{
              "Income Gini": "Average",
              "Public Assistance": "High",
              "Retirement Income" : "Low"
            },
            "Commuting":{
              "No Car" : "Average",
              "Public Transport" : "High",
              "Commute 5-9 mins":"Low",
              "Commute 10-14 mins":"Low",
              "Commute 15-19 mins":"Low",
              "Commute 20-24 mins":"Low",
              "Commute 25-29 mins":"Low",
              "Commute 30-34 mins":"High",
              "Commute 35-39 mins":"Average",
              "Commute 40-44 mins":"High",
              "Commute 45-59 mins":"High",
              "Commute 60-89 mins":"High",
              "Commute 90+ mins":"High"
            },
            "Housing":{
              "Median rent": "Low",
              "Housing lower value quantile" : "Low",
              "Housing median value" : "Low",
              "Housing upper value quantile" : "Low",
              "Vacancy" : "High",
              "Renter Occupied" : "High",
              "Single Family Detached" : "Low",
              "Single Family Attached" : "High",
              "Building with 2 Units" : "High",
              "3-4 Units" : "High",
              "5-9 Units" : "High",
              "10-20 Units" : "High",
              "20-49 Units": "High",
              "50+ Units" : "High",
              "Mobile Homes" : "Low",
              "Built 2005 or later" : "Average",
              "Built 2000-2004": "Average",
              "Built earlier than 1939" : "Low"
             }
           }
        }),
        ('Low income, African American', {
            'description' : 'African American, low income, high school '
            'education, single parent famalies, mobile within cities, uses '
            'public transport.',
            "Education":{
              "Less than highschool": "High",
              "High School": "High",
              "Some College / Assc degree" :"Low",
              "Bachelor Degree" :"Low",
              "Grad Degree" :"Low"
            },
            "Family Structure":{
              "Single Mothers" : "High",
              "Married Couples" : "Low",
              "Male-Male Couples": "Low",
              "Female-Female Couples" : "Low"
            },
            "Residential Stability":{
              "Moved home within city in the last year" : "High",
              "Moved city in the last year" : "Low"
            },
            "Language":{
              "Speaks english only" : "High",
              "Speaks Spanish" : "Low",
              "Speaks Spanish Low English" : "Low"
            },
            "Ethnicity":{
              "White" : "Low",
              "Black" : "High",
              "American Indian": "Low",
              "Asian": "Low",
              "Hispanic Latino" : "Low",
              "Not a Citizen" : "Low"
            },
            "Wealth":{
              "Income Gini": "High",
              "Public Assistance": "High",
              "Retirement Income" : "Low"
            },
            "Commuting":{
              "No Car" : "High",
              "Public Transport" : "High",
              "Commute 5-9 mins":"High",
              "Commute 10-14 mins":"High",
              "Commute 15-19 mins":"High",
              "Commute 20-24 mins":"Average",
              "Commute 25-29 mins":"Low",
              "Commute 30-34 mins":"Average",
              "Commute 35-39 mins":"Low",
              "Commute 40-44 mins":"Low",
              "Commute 45-59 mins":"Low",
              "Commute 60-89 mins":"Low",
              "Commute 90+ mins":"Low"
            },
            "Housing":{
              "Median rent": "Low",
              "Housing lower value quantile" : "Low",
              "Housing median value" : "Low",
              "Housing upper value quantile" : "Low",
              "Vacancy" : "High",
              "Renter Occupied" : "High",
              "Single Family Detached" : "Average",
              "Single Family Attached" : "Low",
              "Building with 2 Units" : "High",
              "3-4 Units" : "High",
              "5-9 Units" : "High",
              "10-20 Units" : "Low",
              "20-49 Units": "Low",
              "50+ Units" : "Average",
              "Mobile Homes" : "Low",
              "Built 2005 or later" : "Low",
              "Built 2000-2004": "Low",
              "Built earlier than 1939" : "High"
           }
        }),
        ('Residential Institutions', {
            'description' : 'Advanced education, highly mobile, White, Black '
            'or Asian, public transit, low rent.',
            'details' : {
            "Education":{
              "Less than highschool": "Low",
              "High School": "Low",
              "Some College / Assc degree" :"Average",
              "Bachelor Degree" :"High",
              "Grad Degree" :"High"
            },
            "Family Structure":{
              "Single Mothers" : "High",
              "Married Couples" : "Low",
              "Male-Male Couples": "Low",
              "Female-Female Couples" : "High"
            },
            "Residential Stability":{
              "Moved home within city in the last year" : "High",
              "Moved city in the last year" : "High"
            },
            "Language":{
              "Speaks english only" : "Average",
              "Speaks Spanish" : "Low",
              "Speaks Spanish Low English" : "Low"
            },
            "Ethnicity":{
              "White" : "Average",
              "Black" : "Average",
              "American Indian": "Low",
              "Asian": "High",
              "Hispanic Latino" : "Low",
              "Not a Citizen" : "Low"
            },
            "Wealth":{
              "Income Gini": "High",
              "Public Assistance": "Average",
              "Retirement Income" : "Low"
            },
            "Commuting":{
              "No Car" : "High",
              "Public Transport" : "Low",
              "Commute 5-9 mins":"High",
              "Commute 10-14 mins":"High",
              "Commute 15-19 mins":"Average",
              "Commute 20-24 mins":"Low",
              "Commute 25-29 mins":"Low",
              "Commute 30-34 mins":"Low",
              "Commute 35-39 mins":"Low",
              "Commute 40-44 mins":"Low",
              "Commute 45-59 mins":"Low",
              "Commute 60-89 mins":"Low",
              "Commute 90+ mins":"Low"
            },
            "Housing":{
              "Median rent": "Low",
              "Housing lower value quantile" : "Average",
              "Housing median value" : "Average",
              "Housing upper value quantile" : "Average",
              "Vacancy" : "Average",
              "Renter Occupied" : "High",
              "Single Family Detached" : "Low",
              "Single Family Attached" : "Average",
              "Building with 2 Units" : "High",
              "3-4 Units" : "High",
              "5-9 Units" : "High",
              "10-20 Units" : "High",
              "20-49 Units": "High",
              "50+ Units" : "High",
              "Mobile Homes" : "Low",
              "Built 2005 or later" : "Low",
              "Built 2000-2004": "Low",
              "Built earlier than 1939" : "High"
            }
         }
      })
    ])

    def version(self):
        return 12

    def requires(self):
        return {
            'acs_tags': ACSTags(),
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'units': UnitTags(),
        }

    def columns(self):
        segments_tag = self.input()['acs_tags']['segments']
        usa = self.input()['sections']['united_states']
        segmentation = self.input()['units']['segmentation']
        x2 = OBSColumn(
            id='X2',
            type='Text',
            name="Spielman-Singleton Segments: 2 Clusters",
            description="Sociodemographic classes from Spielman and Singleton 2015, 2 clusters"
        )
        x10 = OBSColumn(
            id='X10',
            type='Text',
            name="Spielman-Singleton Segments: 10 Clusters",
            description='Sociodemographic classes from Spielman and Singleton 2015, 10 clusters',
            weight=4,
            extra={'categories': self.x10_categories},
            tags=[segments_tag, usa, segmentation],
        )
        x31 = OBSColumn(
            id='X31',
            type='Text',
            name="Spielman-Singleton Segments: 31 Clusters",
            description='Sociodemographic classes from Spielman and Singleton 2015, 10 clusters'
        )
        x55 = OBSColumn(
            id='X55',
            type='Text',
            name="Spielman-Singleton Segments: 55 Clusters",
            description="Sociodemographic classes from Spielman and Singleton 2015, 55 clusters",
            weight=7,
            extra={'categories': self.x55_categories},
            tags=[segments_tag, usa, segmentation],
        )

        return OrderedDict([
            ('X10', x10),
            ('X31', x31),
            ('X55', x55),
            ('X2', x2)
        ])

class SpielmanSingletonMetaWrapper(MetaWrapper):
    def tables(self):
        yield SpielmanSingletonTable()
        yield SumLevel(year='2014', geography='census_tract')
#
# class LoadSpielmanSingletonToDB(self):
#     def requires(self):
#         reutrn
#
#     def run(self):
#         session = current_session()
