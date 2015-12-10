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
from luigi import Parameter, BooleanParameter, Task, WrapperTask, LocalTarget
from tasks.util import (LoadPostgresFromURL, classpath, pg_cursor)
from psycopg2 import ProgrammingError
from tasks.us.census.tiger import Tiger, SUMLEVELS, load_sumlevels


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
        return self.schema + '.census_table_metadata'

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
        self.output().touch()


class DumpACS(WrapperTask):
    '''
    Dump a table in postgres compressed format
    '''
    #TODO
    year = Parameter()
    sample = Parameter()

    def requires(self):
        pass


class ACSColumn(LocalTarget):

    def __init__(self, **kwargs):
        self.force = False
        self.column_id = kwargs['column_id']
        self.column_title = kwargs['column_title'].decode('utf8')
        self.column_parent_path = kwargs['column_parent_path']
        self.parent_column_id = kwargs['parent_column_id']
        self.table_title = kwargs['table_title'].decode('utf8')
        self.universe = kwargs['universe'].decode('utf8')
        self.denominator = kwargs['denominator']
        self.moe = kwargs['moe']

        super(ACSColumn, self).__init__(
            path=os.path.join('columns', classpath(self), self.column_id) + '.json')

    @property
    def name(self):
        '''
        Attempt a human-readable name for this column
        '''
        if self.column_title == u'Total:' and not self.column_parent_path:
            #name = self.table_title.split(' by ')[0] + u' in ' + self.universe
            name = self.universe
        else:
            name = self.column_title
            if self.column_parent_path:
                path = [par.decode('utf8').replace(u':', u'') for par in self.column_parent_path if par]
                name += u' within ' + u' within '.join(path)
            name += u' in ' + self.universe
        if self.moe:
            return u'Margin of error for ' + name
        return name

    def generate(self):
        data = {
            'name': self.name,
            'extra': {
                'title': self.column_title,
                'table': self.table_title,
                'universe': self.universe,
            }
        }
        data['relationships'] = {}
        if self.moe:
            data['extra']['margin_of_error'] = True
            data['relationships']['parent'] = self.path.replace(u'_moe.json', u'')
        else:
            if 'median' in self.table_title.lower():
                data['aggregate'] = 'median'
            else:
                data['aggregate'] = 'sum'
            if self.column_parent_path:
                data['extra']['ancestors'] = self.column_parent_path
            if self.denominator:
                data['relationships']['denominator'] = \
                        os.path.join(classpath(self), self.denominator)
            if self.parent_column_id:
                data['relationships']['parent'] = \
                        os.path.join(classpath(self), self.parent_column_id)
        with self.open('w') as outfile:
            json.dump(data, outfile, indent=2, sort_keys=True)


class ACSTable(LocalTarget):

    def __init__(self, force=False, **kwargs):
        self.force = force
        self.source = kwargs['source']
        self.seqnum = kwargs['seqnum']
        self.denominators = kwargs['denominators']
        self.table_titles = kwargs['table_titles']
        self.column_titles = kwargs['column_titles']
        self.column_ids = sorted(kwargs['column_ids'])
        self.indents = kwargs['indents']
        self.parent_column_ids = kwargs['parent_column_ids']
        self.universes = kwargs['universes']
        self.moe = kwargs.get('moe', False)
        self.schema = kwargs['schema']
        self.year = kwargs['year']
        if self.moe:
            self.seqnum += '_moe'
            for i, colid in enumerate(self.column_ids):
                self.column_ids[i] = colid + '_moe'

        super(ACSTable, self).__init__(
            path=os.path.join('tables', classpath(self), self.source, self.seqnum) + '.json')

    def generate(self, cursor, force=False):
        moe_columns = ', '.join([
            #'(avg({column}_moe/nullif({column}, 0)) * 100)::NUMERIC(5, 2), count({column})'.format(
            '(avg({column}_moe/nullif({column}, 0)) * 100), count({column})'.format(
                column=column)
            for column in self.column_ids
        ])

        # Grab approximate margin of error for everything
        if not self.moe:
            cursor.execute(
                ' SELECT SUBSTR(data.geoid, 1, 3) resolution, '
                '        COUNT(*) resolution_sample, '
                '        {moe_columns} '
                ' FROM {schema}.{seqnum} as data, '
                '      {schema}.{seqnum}_moe as moe '
                ' WHERE data.geoid = moe.geoid AND '
                '       SUBSTR(data.geoid, 4, 2) = \'00\' '
                ' GROUP BY SUBSTR(data.geoid, 1, 3) '
                ' ORDER BY SUBSTR(data.geoid, 1, 3) '.format(schema=self.schema,
                                                             seqnum=self.seqnum,
                                                             moe_columns=moe_columns
                                                            ))
            sample_and_moe = cursor.fetchall()
        column_parent_path = []
        columns = []

        for i, column_id in enumerate(self.column_ids):
            resolutions = []
            if self.moe:
                columns.append({
                    'id': os.path.join(classpath(self), column_id),
                })
            else:
                for sam in sample_and_moe:
                    sumlevel = sam[0]
                    if sumlevel not in SUMLEVELS:
                        continue
                    resolution = os.path.join(classpath(load_sumlevels),
                                              self.year,
                                              SUMLEVELS[sumlevel]['slug'])
                    #sumlevel_sample = d[1]
                    moe = sam[(i * 2) + 2]
                    sample = sam[(i * 2) + 3]
                    if sample > 0:
                        resolutions.append({
                            'id': resolution,
                            'error': moe,
                            'sample': sample
                        })
                columns.append({
                    'id': os.path.join(classpath(self), column_id),
                    'resolutions': resolutions
                })
            indent = (self.indents[i] or 0) - 1
            column_title = self.column_titles[i]
            if indent >= 0:

                while len(column_parent_path) < indent:
                    column_parent_path.append(None)

                column_parent_path = column_parent_path[0:indent]
                column_parent_path.append(column_title)

            col = ACSColumn(column_id=column_id, column_title=column_title,
                            column_parent_path=column_parent_path[:-1],
                            parent_column_id=self.parent_column_ids[i],
                            denominator=self.denominators[i],
                            table_title=self.table_titles[i],
                            universe=self.universes[i], moe=self.moe)
            if not col.exists() or force:
                col.generate()

        data = {
            'columns': columns
        }
        with self.open('w') as outfile:
            json.dump(data, outfile, indent=2, sort_keys=True)


class ProcessACS(Task):
    year = Parameter()
    sample = Parameter()
    force = BooleanParameter(default=False)

    def requires(self):
        yield DownloadACS(year=self.year, sample=self.sample)
        yield Tiger()

    @property
    def schema(self):
        return 'acs{year}_{sample}'.format(year=self.year, sample=self.sample)

    def run(self):
        cursor = pg_cursor()
        for output in self.output():
            output.generate(cursor, force=self.force)
        self.force = False

    def complete(self):
        '''
        We can't run output() without hitting an ACS table that may not exist
        yet.  This wraps the default complete() to return `False` in those
        instances.
        '''
        if self.force:
            return False
        try:
            return super(ProcessACS, self).complete()
        except ProgrammingError:
            return False

    def output(self):
        cursor = pg_cursor()
        # Grab all table and column info
        cursor.execute(
            ' SELECT isc.table_name as seqnum, ARRAY_AGG(table_title) as table_titles,'
            '   ARRAY_AGG(denominator_column_id) as denominators,'
            '   ARRAY_AGG(column_id) as column_ids, ARRAY_AGG(column_title) AS column_titles,'
            '   ARRAY_AGG(indent) as indents, ARRAY_AGG(parent_column_id) AS parent_column_ids,'
            '   ARRAY_AGG(universe) as universes'
            ' FROM {schema}.census_table_metadata ctm'
            ' JOIN {schema}.census_column_metadata ccm USING (table_id)'
            ' JOIN information_schema.columns isc ON isc.column_name = LOWER(ccm.column_id)'
            ' WHERE isc.table_schema = \'{schema}\''
            '  AND isc.table_name LIKE \'seq%\''
            ' GROUP BY isc.table_name'
            ' ORDER BY isc.table_name'
            ' '.format(schema=self.schema))
        tables = cursor.fetchall()
        for seqnum, table_titles, denominators, column_ids, column_titles, indents, \
                             parent_column_ids, universes in tables:

            yield ACSTable(seqnum=seqnum, source=self.schema,
                           table_titles=table_titles, universes=universes,
                           denominators=denominators, column_titles=column_titles,
                           column_ids=column_ids, indents=indents,
                           schema=self.schema, year=self.year,
                           parent_column_ids=parent_column_ids)

            yield ACSTable(seqnum=seqnum, source=self.schema,
                           table_titles=table_titles, universes=universes,
                           denominators=denominators, column_titles=column_titles,
                           column_ids=column_ids, indents=indents,
                           schema=self.schema, year=self.year,
                           parent_column_ids=parent_column_ids, moe=True)


class AllACS(WrapperTask):

    def requires(self):
        for year in xrange(2010, 2014):
            for sample in ('1yr', '3yr', '5yr'):
                yield ProcessACS(year=year, sample=sample)
        #for year in xrange(2010, 2011):
        #    for sample in ('1yr',):
        #        yield ProcessACS(year=year, sample=sample)


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

