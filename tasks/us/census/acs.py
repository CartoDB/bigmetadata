#!/usr/bin/env python

'''
Bigmetadata tasks

tasks to download and create metadata
'''

#import requests
#import datetime
#import json
#import csv
import json
import os
from luigi import Parameter, WrapperTask, LocalTarget
from tasks.util import LoadPostgresFromURL, MetadataTask, DefaultPostgresTarget
#from tasks.us.census.tiger import Tiger


# STEPS:
#
# 1. load ACS SQL into postgres
# 2. extract usable metadata from the imported tables, persist as json
#

class DownloadACS(WrapperTask):

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
        return LoadPostgresFromURL(url=url, table=table)


class DumpACS(WrapperTask):
    '''
    Dump a table in postgres compressed format
    '''
    #TODO
    year = Parameter()
    sample = Parameter()

    def requires(self):
        pass


class ACSColumn(MetadataTask):

    column_id = Parameter()
    column_title = Parameter()
    table_title = Parameter()

    @property
    def name(self):
        return '{table}: {column}'.format(table=self.table_title,
                                          column=self.column_title)

    def run(self):
        data = {
            'name': self.name
        }
        with self.output().open('w') as outfile:
            json.dump(data, outfile, indent=2)

    def output(self):
        return LocalTarget(os.path.join('columns', self.path, self.column_id) + '.json')


class ACSTable(MetadataTask):

    source = Parameter()
    seqnum = Parameter()
    denominators = Parameter()
    table_titles = Parameter()
    column_titles = Parameter()
    column_ids = Parameter()
    indents = Parameter()
    parent_column_ids = Parameter()

    def requires(self):
        for i, column_id in enumerate(self.column_ids):
            yield ACSColumn(column_id=column_id, column_title=self.column_titles[i],
                            table_title=self.table_titles[i])

    def run(self):
        data = {
            'columns': [column_id for column_id in self.column_ids]
        }
        with self.output().open('w') as outfile:
            json.dump(data, outfile, indent=2)

    def output(self):
        return LocalTarget(os.path.join('tables', self.path, self.source, self.seqnum) + '.json')


class ProcessACS(WrapperTask):
    year = Parameter()
    sample = Parameter()

    def requires(self):
        yield DownloadACS(year=self.year, sample=self.sample)
        cursor = self.cursor()
        cursor.execute(
            ' SELECT isc.table_name as seqnum, ARRAY_AGG(table_title) as table_titles,'
            '   ARRAY_AGG(denominator_column_id) as denominators,'
            '   ARRAY_AGG(column_id) as column_ids, ARRAY_AGG(column_title) AS column_titles,'
            '   ARRAY_AGG(indent) as indents, ARRAY_AGG(parent_column_id) AS parent_column_ids'
            ' FROM {schema}.census_table_metadata ctm'
            ' JOIN {schema}.census_column_metadata ccm USING (table_id)'
            ' JOIN information_schema.columns isc ON isc.column_name = LOWER(ccm.column_id)'
            ' WHERE isc.table_schema = \'{schema}\''
            '  AND isc.table_name LIKE \'seq%\''
            ' GROUP BY isc.table_name'
            ' ORDER BY isc.table_name'
            ' LIMIT 10'.format(schema=self.schema))
        for seqnum, table_titles, denominators, column_ids, column_titles, indents, parent_column_ids in cursor:
            yield ACSTable(seqnum=seqnum, source=self.schema,
                           table_titles=table_titles,
                           denominators=denominators, column_titles=column_titles,
                           column_ids=column_ids, indents=indents,
                           parent_column_ids=parent_column_ids)

    def cursor(self):
        if not hasattr(self, '_connection'):
            target = DefaultPostgresTarget(table='foo', update_id='bar')
            self._connection = target.connect()
        return self._connection.cursor()

    @property
    def schema(self):
        return 'acs{year}_{sample}'.format(year=self.year, sample=self.sample)


class AllACS(WrapperTask):

    def requires(self):
        #for year in xrange(2010, 2014):
        #    for sample in ('1yr', '3yr', '5yr'):
        for year in xrange(2010, 2011):
            for sample in ('1yr',):
                #return ProcessACS(year=year, sample=sample)
                yield ProcessACS(year=year, sample=sample)


#if __name__ == '__main__':
#    run()
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

