#!/usr/bin/env python

'''
Tiger
'''

import json
import os
from tasks.util import LoadPostgresFromURL, classpath, pg_cursor
from luigi import Task, WrapperTask, Parameter, LocalTarget, BooleanParameter
from psycopg2 import ProgrammingError


class TigerSumLevel(LocalTarget):

    def __init__(self, sumlevel):
        self.sumlevel = sumlevel
        self.data = SUMLEVELS[sumlevel]
        super(TigerSumLevel, self).__init__(
            path=os.path.join('columns', classpath(self),
                              self.data['slug']) + '.json')

    def generate(self):
        relationships = {}
        if self.data['ancestors']:
            relationships['ancestors'] = self.data['ancestors']
        if self.data['parent']:
            relationships['parent'] = self.data['parent']
        obj = {
            'name': self.data['name'],
            'description': self.data['census_description'],
            'extra': {
                'summary_level': self.data['summary_level'],
                'source': self.data['source']
            }
        }
        if relationships:
            obj['relationships'] = relationships
        with self.open('w') as outfile:
            json.dump(obj, outfile, indent=2)


class DownloadTiger(LoadPostgresFromURL):

    url_template = 'https://s3.amazonaws.com/census-backup/tiger/{year}/tiger{year}_backup.sql.gz'
    year = Parameter()

    @property
    def schema(self):
        return 'tiger{year}'.format(year=self.year)

    def identifier(self):
        return self.schema + '.census_names'

    def run(self):
        cursor = pg_cursor()
        try:
            cursor.execute('DROP SCHEMA {schema} CASCADE'.format(schema=self.schema))
            cursor.connection.commit()
        except ProgrammingError:
            cursor.connection.rollback()
        url = self.url_template.format(year=self.year)
        self.load_from_url(url)
        self.output().touch()


class ProcessTiger(Task):

    force = BooleanParameter(default=False)
    year = Parameter()

    def requires(self):
        yield DownloadTiger(year=self.year)

    def output(self):
        for sumlevel in SUMLEVELS:
            yield TigerSumLevel(sumlevel)

    def complete(self):
        if self.force:
            return False
        else:
            return super(ProcessTiger, self).complete()

    def run(self):
        for output in self.output():
            output.generate()
        self.force = False


class Tiger(WrapperTask):

    force = BooleanParameter(default=False)

    def requires(self):
        yield ProcessTiger(year=2013, force=self.force)


def load_sumlevels():
    '''
    Load summary levels from JSON. Returns a dict by sumlevel number.
    '''
    with open(os.path.join(os.path.dirname(__file__), 'summary_levels.json')) as fhandle:
        sumlevels_list = json.load(fhandle)
    sumlevels = {}
    for slevel in sumlevels_list:
        # Replace pkey ancestors with paths to columns
        # We subtract 1 from the pkey because it's 1-indexed, unlike python
        fields = slevel['fields']
        for i, ancestor in enumerate(fields['ancestors']):
            colpath = os.path.join('columns', classpath(load_sumlevels),
                                   sumlevels_list[ancestor - 1]['fields']['slug'])
            fields['ancestors'][i] = colpath
        if fields['parent']:
            fields['parent'] = os.path.join(
                'columns', classpath(load_sumlevels),
                sumlevels_list[fields['parent'] - 1]['fields']['slug'])

        sumlevels[fields['summary_level']] = fields
    return sumlevels


SUMLEVELS = load_sumlevels()
