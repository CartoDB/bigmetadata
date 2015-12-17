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
from tasks.us.census.tiger import SUMLEVELS, load_sumlevels


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
        self.tags = kwargs['tags'].split(',')
        self.moe = kwargs['moe']

        super(ACSColumn, self).__init__(
            path=os.path.join('data', 'columns', classpath(self), self.column_id) + '.json')

    @property
    def name(self):
        '''
        Attempt a human-readable name for this column
        '''
        if self.table_title.startswith('Unweighted'):
            name = self.table_title
        elif self.column_title in (u'Total:', u'Total') and not self.column_parent_path:
            #name = self.table_title.split(' by ')[0] + u' in ' + self.universe
            name = self.universe
        else:
            table_title = self.table_title.split(' for ')[0]
            dimensions = table_title.split(' by ')
            table_title = table_title.split(' by ')[0]
            #if table_title.lower() not in ('sex',):
            #    name = table_title + u': '
            #else:

            column_title = self.column_title.replace(u':', u'')
            if dimensions[-1].lower() == u'race' and column_title.endswith(u' alone'):
                # These extra "alone"s are confusing
                column_title = column_title.replace(u' alone', u'')
            name = column_title

            if self.column_parent_path:
                for i, par in enumerate(self.column_parent_path):
                    if par:
                        par = par.decode('utf8').replace(u':', u'')
                        if par.lower() in (u'total', u'not hispanic or latino',
                                           'enrolled in school', 'employment status', ):
                            # These dimension values are not interesting
                            continue

                        if i >= len(dimensions):
                            dimension_name = dimensions[-1]
                        elif len(dimensions) > 1:
                            dimension_name = dimensions[i]
                        else:
                            dimension_name = dimensions[0]

                        if dimension_name.lower() in ('employment status', ):
                            # These dimensions are not interesting
                            continue
                        if dimension_name.lower() in ('sex', 'race', ):
                            # We want to show dimension value here for these,
                            # but don't need to name the dimension
                            name += u' ' + par
                        else:
                            name += u' ' + dimension_name + u' ' + par
            universe = self.universe
            if 'median' in table_title.lower() or 'aggregate' in table_title.lower():
                # We may want to use something other than 'in' but still indicate
                # the universe sometimes
                pass
            elif universe.lower().strip('s') in name.lower():
                # Universe is redundant with the existing name
                pass
            elif universe.lower() in ('total population', ):
                name += ' Population'
            else:
                name += u' in ' + self.universe
        if self.moe:
            return u'Margin of error for ' + name
        return name

    def generate(self):
        try:
            with self.open('r') as infile:
                data = json.load(infile)
        except IOError:
            data = {}

        data.update({
            'name': self.name,
            'tags': self.tags,
        })
        data['extra'] = data.get('extra', {})
        data['extra'].update({
            'title': self.column_title,
            'table': self.table_title,
            'universe': self.universe,
        })
        data['relationships'] = data.get('relationships', {})
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
        self.sample = kwargs['sample']
        self.tags = kwargs['tags']
        if self.moe:
            self.seqnum += '_moe'
            for i, colid in enumerate(self.column_ids):
                self.column_ids[i] = colid + '_moe'

        super(ACSTable, self).__init__(
            path=os.path.join('data', 'tables', classpath(self), self.source, self.seqnum) + '.json')

    def generate(self, cursor, force=False):
        moe_columns = ', '.join([
            'SQRT(SUM(POWER(NULLIF({column}_moe, -1), 2)))/SUM(NULLIF({column}, 0)) * 100, COUNT({column})'.format(
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
                            tags=self.tags[i],
                            universe=self.universes[i], moe=self.moe)
            if not col.exists() or force:
                col.generate()

        if self.sample == '1yr':
            timespan = self.year
        elif self.sample == '3yr':
            timespan = '{} - {}'.format(int(self.year) - 2, self.year)
        elif self.sample == '5yr':
            timespan = '{} - {}'.format(int(self.year) - 4, self.year)
        else:
            raise Exception('Unrecognized sample {}'.format(self.sample))

        data = {
            'title': self.schema + u' ' + self.seqnum,
            'dct_temporal_sm': timespan,
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
            ' SELECT isc.table_name as seqnum, ARRAY_AGG(table_title ORDER BY column_id) as table_titles,'
            '   ARRAY_AGG(denominator_column_id ORDER BY column_id) as denominators,'
            '   ARRAY_AGG(column_id ORDER BY column_id) as column_ids, '
            '   ARRAY_AGG(column_title ORDER BY column_id) AS column_titles, '
            '   ARRAY_AGG(indent ORDER BY column_id) as indents, '
            '   ARRAY_AGG(parent_column_id ORDER BY column_id) AS parent_column_ids, '
            '   ARRAY_AGG(universe ORDER BY column_id) as universes, '
            '   ARRAY_AGG(array_to_string(topics, \',\') ORDER BY column_id) as tags '
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
                             parent_column_ids, universes, tags in tables:

            yield ACSTable(seqnum=seqnum, source=self.schema,
                           table_titles=table_titles, universes=universes,
                           denominators=denominators, column_titles=column_titles,
                           column_ids=column_ids, indents=indents,
                           schema=self.schema, year=self.year,
                           sample=self.sample,
                           parent_column_ids=parent_column_ids, tags=tags)

            yield ACSTable(seqnum=seqnum, source=self.schema,
                           table_titles=table_titles, universes=universes,
                           denominators=denominators, column_titles=column_titles,
                           column_ids=column_ids, indents=indents,
                           schema=self.schema, year=self.year,
                           sample=self.sample,
                           parent_column_ids=parent_column_ids, moe=True,
                           tags=tags)


class AllACS(WrapperTask):

    def requires(self):
        for year in xrange(2010, 2014):
            for sample in ('1yr', '3yr', '5yr'):
                yield ProcessACS(year=year, sample=sample)
