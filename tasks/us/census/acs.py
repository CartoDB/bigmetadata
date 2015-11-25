#!/usr/bin/env python

'''
Bigmetadata tasks

tasks to download and create metadata
'''

#import requests
import datetime
#import json
#import csv
#import os
from luigi import Task, Parameter, DateParameter
from tasks.util import DefaultPostgresTarget, LoadPostgresDumpFromURL


# STEPS:
#
# 1. load ACS dump into postgres
# 2. extract usable metadata from the imported tables, persist as json
#

class DownloadACS(Task):

    # http://censusreporter.tumblr.com/post/73727555158/easier-access-to-acs-data
    url_template = 'https://s3.amazonaws.com/census-backup/acs/{year}/' \
            'acs{year}_{sample}/acs{year}_{sample}_backup.sql.gz'

    year = Parameter()
    sample = Parameter()

    def requires(self):
        table = 'acs{year}_{sample}.census_table_metadata'.format(
            year=self.year,
            sample=self.sample
        )
        url = self.url_template.format(year=self.year, sample=self.sample)
        return LoadPostgresDumpFromURL(url=url, table=table)

    #def __init__(self, *args, **kwargs):
    #    url = self.url_template.format(year=kwargs['year'], sample=kwargs['sample'])
    #    super(DownloadACS, self).__init__(*args, url=url, **kwargs)


#class DownloadTask(Task):
#    url = Parameter()
#    name = Parameter()
#    date_checked = DateParameter(default=datetime.date.today())
#
#    def run(self):
#        out = self.output().open('w')
#        out.write(requests.get(self.url).content)
#        out.close()
#
#    def output(self):
#        return LocalTarget('tmp/{}.csv'.format(self.name))





class ProcessACS(Task):
    year = Parameter()
    sample = Parameter()

    def requires(self):
        return DownloadACS(year=self.year, sample=self.sample)

#     RESOLUTIONS_LOOKUP = {
#         '010': 'United States',
#         '020': 'Region',
#         '030': 'Division',
#         '040': 'State',
#         '050': 'State-County',
#         '060': 'State-County-County Subdivision',
#         '067': 'State-County-County Subdivision-Subminor Civil Division',
#         '140': 'State-County-Census Tract',
#         '150': 'State-County-Census Tract-Block Group',
#         '160': 'State-Place',
#         '170': 'State-Consolidated City',
#         #'230': 'State-Alaska Native Regional Corporation',
#         #'250': 'American Indian Area/Alaska Native Area/Hawaiian Home Land',
#         #'251': 'American Indian Area-Tribal Subdivision/Remainder',
#         #'252': 'American Indian Area/Alaska Native Area (Reservation or Statistical Entity Only)',
#         #'254': 'American Indian Area (Off-Reservation Trust Land Only)/Hawaiian Home Land',
#         #'256': 'American Indian Area-Tribal Census Tract',
#         #'258': 'American Indian Area-Tribal Census Tract-Tribal Block Group',
#         '310': 'Metropolitan Statistical Area/Micropolitan Statistical Area',
#         '314': 'Metropolitan Statistical Area-Metropolitan Division',
#         '330': 'Combined Statistical Area',
#         '332': 'Combined Statistical Area-Metropolitan'
#                'Statistical Area/Micropolitan Statistical Area',
#         #'335': 'Combined New England City and Town Area',
#         #'337': 'Combined New England City and Town Area-New England City and Town Area',
#         #'350': 'New England City and Town Area',
#         #'352': 'New England City and Town Area-State-Principal City',
#         #'355': 'New England City and Town Area (NECTA)-NECTA Division',
#         #'361': 'State-New England City and Town Area-Principal City',
#         '500': 'State-Congressional District (111th)',
#         '610': 'State-State Legislative District (Upper Chamber)',
#         '620': 'State-State Legislative District (Lower Chamber)',
#         '700': 'State-County-Voting District/Remainder',
#         '860': '5-Digit ZIP code Tabulation Area',
#         #'950': 'State-School District (Elementary)/Remainder',
#         #'960': 'State-School District (Secondary)/Remainder',
#         #'970': 'State-School District (Unified)/Remainder',
#     }
# 
#     RESOLUTIONS = {
#         '1yr': [
#             RESOLUTIONS_LOOKUP[sumlevel] for sumlevel in [
#                 '010', '020', '030', '040', '050', '060', '160',
#                 '230', '250', '310', '312', '314', '330', '335',
#                 '350', '352', '355', '400', '500', '795', '950',
#                 '960', '970'] if sumlevel in RESOLUTIONS_LOOKUP
#         ],
#         '5yr': [
#             RESOLUTIONS_LOOKUP[sumlevel] for sumlevel in [
#                 '067', '070', '140', '150', '155', '170',
#                 '172', '251', '252', '254', '256', '258',
#                 '260', '269', '270', '280', '283', '286',
#                 '290', '291', '292', '293', '294', '311',
#                 '313', '315', '316', '320', '321', '322',
#                 '323', '324', '331', '332', '333', '336',
#                 '337', '338', '340', '341', '345', '346',
#                 '351', '353', '354', '356', '357', '358',
#                 '360', '361', '362', '363', '364', '365',
#                 '366', '410', '430', '510', '550', '610',
#                 '612', '620', '622', '860'] if sumlevel in RESOLUTIONS_LOOKUP
#         ]
#     }
#     RESOLUTIONS['3yr'] = RESOLUTIONS['1yr']
#     RESOLUTIONS['5yr'].extend(RESOLUTIONS['1yr'])
# 
#     @property
#     def name(self):
#         return 'acs{year}_{sample}'.format(year=self.year, sample=self.sample)
# 
#     def requires(self):
#         return DownloadTask(name=self.name,
#             url='http://www2.census.gov/{name}/summaryfile/Sequence_Number_and_Table_Number_Lookup.txt'.format(name=self.name))
# 
#     def run(self):
# 
#         # Make sure output folders exist
#         for p in ('columns', 'tables'):
#             try:
#                 os.makedirs('{}/acs'.format(p))
#             except OSError:
#                 pass
# 
#         for i, line in enumerate(csv.reader(self.input().open())):
# 
#             # Skip header
#             if i == 0:
#                 continue
# 
#             _, table_id, seqnum, line_no, start_pos, _, _, table_title, \
#                     subject_area = [_.strip() for _ in line]
# 
#             # Handle metadata rows
#             if not line_no or line_no == '.':
#                 parent = None
#                 parent_table_title = None
#                 if start_pos and start_pos != '.':
#                     table_description = table_title
#                 else:
#                     universe = table_title
#                 continue
# 
#             seqnum = '{:04d}'.format(int(seqnum))
#             try:
#                 column_id = 'census_acs_{}{:03d}'.format(table_id, int(line_no)).lower()
#             except ValueError:
#                 column_id = 'census_acs_{}{:03.1f}'.format(table_id, float(line_no)).lower()
#             source_table = '{}.seq{}'.format(self.name, seqnum)
# 
#             column_file_path = 'colums/acs/{}.json'.format(column_id)
#             table_file_path = 'tables/acs/{}.json'.format(seqnum)
# 
#             try:
#                 with open(column_file_path, 'r') as fhandle:
#                     column_obj = json.load(fhandle)
#             except IOError:
#                 column_obj = {}
# 
#             try:
#                 with open(table_file_path, 'r') as fhandle:
#                     table_obj = json.load(fhandle)
#             except IOError:
#                 table_obj = {}
# 
#             if 'columns' not in table_obj:
#                 column_obj['columns'] = {}
# 
#             if 'tables' not in column_obj:
#                 column_obj['tables'] = {}
# 
#             column_name = table_title
#             if parent:
#                 column_obj['relationships'] = [{
#                     "type": "parent",
#                     "link": parent
#                 }, {
#                     "type": "margin of error",
#                     "link": "{}_moe".format(column_id)
#                 }]
#                 if parent and not parent.lower().startswith('total'):
#                     column_name = '{} {}'.format(parent_table_title, table_title)
# 
#             column_obj['description'] ='{table} {column} in {universe}'.format(
#                 table=table_description,
#                 column=column_name,
#                 universe=universe
# 
#             table_obj['columns'][column_id] = {
#                 "column": column_id,
#                 #"resolutions": self.RESOLUTIONS[self.sample],
#                 #"dates": [self.year]
#             }
# 
#             with open(json_file_path, 'w') as fhandle:
#                 json.dump(obj, fhandle, indent=4)
# 
#             # set parent
#             if table_title.endswith(':'):
#                 parent = column_id
#                 parent_table_title = table_title


class AllACS(Task):

    date_checked = DateParameter(default=datetime.date.today())
    def requires(self):
        for year in xrange(2010, 2014):
            for sample in ('1yr', '3yr', '5yr'):
                #return ProcessACS(year=year, sample=sample)
                yield ProcessACS(year=year, sample=sample)


#if __name__ == '__main__':
#    run()
