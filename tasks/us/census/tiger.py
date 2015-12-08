#!/usr/bin/env python

'''
Tiger
'''

import json
import os
import subprocess
from tasks.util import (LoadPostgresFromURL, classpath, pg_cursor,
                        DefaultPostgresTarget)
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


class DownloadTigerGeography(Task):

    year = Parameter()
    geography = Parameter()
    force = BooleanParameter() # TODO

    url_format = 'ftp://ftp2.census.gov/geo/tiger/TIGER{year}/{geography}/'

    @property
    def url(self):
        return self.url_format.format(year=self.year, geography=self.geography)

    @property
    def directory(self):
        return os.path.join('tmp', classpath(self), self.year)

    def run(self):
        subprocess.check_call('wget --recursive --continue --accept=*.zip '
                              '--no-parent --cut-dirs=3 --no-host-directories '
                              '--directory-prefix={directory} '
                              '{url}'.format(directory=self.directory, url=self.url), shell=True)

    def output(self):
        filenames = subprocess.check_output('wget --recursive --accept=*.zip --reject *.zip '
                                            '--no-parent --cut-dirs=3 --no-host-directories '
                                            '{url} 2>&1 | grep Rejecting'.format(url=self.url), shell=True)
        for fname in filenames.split('\n'):
            if not fname:
                continue
            path = os.path.join(self.directory, self.geography,
                                fname.replace("Rejecting '", '').replace("'.", ''))
            yield LocalTarget(path)


class UnzipTigerGeography(Task):
    '''
    Unzip tiger geography
    '''

    year = Parameter()
    geography = Parameter()
    force = BooleanParameter() # TODO

    def requires(self):
        return DownloadTigerGeography(year=self.year, geography=self.geography)

    def run(self):
        for infile in self.input():
            subprocess.check_call("unzip -n -q -d $(dirname {zippath}) '{zippath}'".format(
                zippath=infile.path), shell=True)

    def output(self):
        for infile in self.input():
            yield LocalTarget(infile.path.replace('.zip', '.shp'))


class TigerGeographyShapefileToSQL(Task):
    '''
    Take downloaded shapefiles and turn them into a SQL dump.
    '''

    year = Parameter()
    geography = Parameter()
    force = BooleanParameter()

    @property
    def table(self):
        return str(self.geography).lower()

    @property
    def schema(self):
        return os.path.join(classpath(self), self.year)

    @property
    def qualified_table(self):
        return '"{schema}"."{table}"'.format(schema=self.schema, table=self.table)

    def complete(self):
        if self.force == True:
            return False
        return super(TigerGeographyShapefileToSQL, self).complete()

    def requires(self):
        return UnzipTigerGeography(year=self.year, geography=self.geography)

    def run(self):
        cursor = pg_cursor()
        cursor.execute('CREATE SCHEMA IF NOT EXISTS "{schema}"'.format(schema=self.schema))
        cursor.execute('DROP TABLE IF EXISTS {qualified_table}'.format(
            qualified_table=self.qualified_table))
        cursor.connection.commit()

        shapefiles = self.input()
        subprocess.check_call(
            'shp2pgsql -D -W "latin1" -s 4326 -c {shp_path} '
            '{qualified_table} | psql'.format(
                shp_path=shapefiles.next().path,
                qualified_table=self.qualified_table),
            shell=True)

        subprocess.check_call(
            'export PG_USE_COPY=yes PGCLIENTENCODING=latin1; '
            'echo \'{shapefiles}\' | xargs -P 16 -I shpfile_path '
            'ogr2ogr -f PostgreSQL PG:dbname=census -append -nlt MultiPolygon '
            '-nln {qualified_table} shpfile_path '.format(
                shapefiles='\n'.join([shp.path for shp in shapefiles]),
                qualified_table=self.qualified_table),
            shell=True)

        # Spatial index
        cursor.execute('CREATE INDEX ON {qualified_table} USING GIST (geom)'.format(
            qualified_table=self.qualified_table))

        self.output().touch()

        self.force = False

    def output(self):
        return DefaultPostgresTarget(table=self.qualified_table,
                                     update_id=self.qualified_table)


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


class ShorelineClipTiger(Task):
    '''
    Clip the provided geography to shoreline.
    '''

    # MTFCC meanings:
    # http://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp2009/TGRSHP09AF.pdf

    year = Parameter()
    geography = Parameter()
    force = BooleanParameter()

    @property
    def schema(self):
        return os.path.join(classpath(self), self.year)

    @property
    def name(self):
        return str(self.geography).lower() + '_shoreline_clipped'

    @property
    def negative_shape(self):
        return self.input()['water'].table

    @property
    def positive_shape(self):
        return '"tiger{year}"."{geography}"'.format(
            #year=self.year,
            year=2012,
            geography=str(self.geography).lower())

    @property
    def qualified_table(self):
        return '"{}"."{}"'.format(self.schema, self.name)

    def requires(self):
        return {
            'tiger': ProcessTiger(year=self.year),
            'water': TigerGeographyShapefileToSQL(geography='AREAWATER', year=self.year)
        }

    def complete(self):
        if self.force:
            return False
        return super(ShorelineClipTiger, self).complete()

    def run(self):
        cursor = pg_cursor()
        cursor.execute('DROP TABLE IF EXISTS {qualified_table}'.format(
            qualified_table=self.qualified_table))

        # copy positive table
        cursor.execute('CREATE TABLE {qualified_table} AS '
                       'SELECT * FROM {positive_shape}'.format(
                           qualified_table=self.qualified_table,
                           positive_shape=self.positive_shape
                       ))

        # update geometries in positive table
        cursor.execute('CREATE INDEX ON {qualified_table} USING GIST '
                       '(the_geom)'.format(qualified_table=self.qualified_table))
        cursor.execute('UPDATE {qualified_table} pos '
                       'SET the_geom = ST_DIFFERENCE(pos.the_geom, neg.geom) '
                       'FROM {negative_shape} neg '
                       'WHERE pos.the_geom && neg.geom '
                       'AND neg.mtfcc IN ( \'H2040\', \'H2053\', \'H2051\', \'H3010\' )'.format(
                           qualified_table=self.qualified_table,
                           negative_shape=self.negative_shape
                       ))
        cursor.connection.commit()
        try:
            self.output().touch()
        except:
            if self.force:
                pass
            else:
                raise
        self.force = False

    def output(self):
        return DefaultPostgresTarget(table=self.qualified_table,
                                     update_id=self.qualified_table)


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
