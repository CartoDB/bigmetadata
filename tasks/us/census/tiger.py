#!/usr/bin/env python

'''
Tiger
'''

import json
import os
import subprocess
from tasks.util import (LoadPostgresFromURL, classpath, pg_cursor,
                        DefaultPostgresTarget, CartoDBTarget,
                        sql_to_cartodb_table, grouper, shell)
from luigi import Task, WrapperTask, Parameter, LocalTarget, BooleanParameter
from psycopg2 import ProgrammingError

HIGH_WEIGHT_COLUMNS = set([
    "block-group",
    "block",
    "census-tract",
    "congressional-district",
    "county",
    "nation",
    "puma",
    "state",
    "zcta5"
])


class TigerSumLevel(LocalTarget):

    def __init__(self, sumlevel):
        self.sumlevel = sumlevel
        self.data = SUMLEVELS[sumlevel]
        super(TigerSumLevel, self).__init__(
            path=os.path.join('data', 'columns', classpath(self),
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
            },
            'tags': ['boundary']
        }
        if self.data['slug'] in HIGH_WEIGHT_COLUMNS:
            obj['weight'] = 2
        else:
            obj['weight'] = 0
        if relationships:
            obj['relationships'] = relationships
        with self.open('w') as outfile:
            json.dump(obj, outfile, indent=2, sort_keys=True)


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
        cmd = 'PG_USE_COPY=yes PGCLIENTENCODING=latin1 ' \
                'ogr2ogr -f PostgreSQL PG:dbname=$PGDATABASE ' \
                '-t_srs "EPSG:4326" -nlt MultiPolygon -nln {qualified_table} ' \
                '{shpfile_path} '.format(
                    qualified_table=self.qualified_table,
                    shpfile_path=shapefiles.next().path)
        shell(cmd)

        # chunk into 500 shapefiles at a time.
        for shape_group in grouper(shapefiles, 500):
            subprocess.check_call(
                'export PG_USE_COPY=yes PGCLIENTENCODING=latin1; '
                'echo \'{shapefiles}\' | xargs -P 16 -I shpfile_path '
                'ogr2ogr -f PostgreSQL PG:dbname=$PGDATABASE -append '
                '-t_srs "EPSG:4326" -nlt MultiPolygon -nln {qualified_table} '
                'shpfile_path '.format(
                    shapefiles='\n'.join([shp.path for shp in shape_group if shp]),
                    qualified_table=self.qualified_table),
                shell=True)

        # Spatial index
        cursor.execute('ALTER TABLE {qualified_table} RENAME COLUMN '
                       'wkb_geometry TO geom'.format(qualified_table=self.qualified_table))
        cursor.execute('CREATE INDEX ON {qualified_table} USING GIST (geom)'.format(
            qualified_table=self.qualified_table))
        cursor.connection.commit()
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


class SimpleShoreline(Task):

    force = BooleanParameter(default=False)
    year = Parameter()

    def requires(self):
        return TigerGeographyShapefileToSQL(geography='AREAWATER', year=self.year)

    def run(self):
        cursor = pg_cursor()
        cursor.execute('DROP TABLE IF EXISTS {output}'.format(
            output=self.output().table))
        cursor.execute('CREATE TABLE {output} AS SELECT * FROM {input} '
                       "WHERE mtfcc IN ('H2040', 'H2053', 'H2051', 'H3010') ".format(
                           input=self.input().table,
                           output=self.output().table
                       ))
        cursor.execute('ALTER TABLE {output} ALTER COLUMN geom SET DATA TYPE GEOMETRY'.format(
            output=self.output().table
        ))
        cursor.execute('UPDATE {output} SET geom = ST_SimplifyPreserveTopology(geom, 0.01)'.format(
            output=self.output().table
        ))
        cursor.execute('CREATE INDEX ON {output} USING GIST (geom)'.format(
            output=self.output().table
        ))
        cursor.connection.commit()
        self.output().touch()

    def output(self):
        output = DefaultPostgresTarget(table='"' + classpath(self) + '/' + str(self.year) + '".simple_shoreline')
        if self.force:
            output.untouch()
            self.force = False
        return output


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
            year=self.year,
            geography=str(self.geography).lower())

    @property
    def qualified_table(self):
        return '"{}"."{}"'.format(self.schema, self.name)

    def requires(self):
        return {
            'tiger': ProcessTiger(year=self.year),
            'water': SimpleShoreline(year=self.year)
        }

    def complete(self):
        if self.force:
            return False
        return super(ShorelineClipTiger, self).complete()

    def run(self):
        cursor = pg_cursor()
        cursor.execute('DROP TABLE IF EXISTS {qualified_table}'.format(
            qualified_table=self.qualified_table))

        # create merged table
        #cursor.execute('CREATE TABLE {qualified_table} AS '
        #               'SELECT pos.*, ST_DIFFERENCE(pos.geom, neg.geom) '
        #               'FROM {positive_shape}

        # copy positive table
        cursor.execute('CREATE TABLE {qualified_table} AS '
                       'SELECT * FROM {positive_shape}'.format(
                           qualified_table=self.output().table,
                           positive_shape=self.positive_shape
                       ))

        # update geometries in positive table
        cursor.execute('CREATE UNIQUE INDEX ON {qualified_table} '
                       '(geoid)'.format(qualified_table=self.qualified_table))
        cursor.execute('CREATE INDEX ON {qualified_table} USING GIST '
                       '(geom)'.format(qualified_table=self.qualified_table))
        cursor.connection.commit()
        tmp_table = 'intersection_tmp'
        cursor.execute('DROP TABLE IF EXISTS {tmp}'.format(tmp=tmp_table))
        query = 'CREATE TEMPORARY TABLE {tmp} AS (' \
                '  SELECT pos.geoid, ' \
                '    ST_MakeValid(ST_Union(ST_MakeValid(neg.geom))) geom ' \
                '  FROM {qualified_table} pos, {negative_shape} neg ' \
                '  WHERE pos.geom && neg.geom ' \
                '  GROUP BY pos.geoid)'.format(
                    tmp=tmp_table,
                    qualified_table=self.output().table,
                    negative_shape=self.negative_shape
                )
        cursor.execute(query)
        cursor.connection.commit()
        query = 'UPDATE {qualified_table} pos ' \
                'SET geom = ST_DIFFERENCE(pos.geom, tmp.geom) ' \
                'FROM {tmp} tmp WHERE tmp.geoid = pos.geoid'.format(
                    qualified_table=self.output().table,
                    tmp=tmp_table
                )
        cursor.execute(query)
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


class ExtractTiger(Task):
    force = BooleanParameter(default=False)
    year = Parameter()
    sumlevel = Parameter()

    def run(self):
        tiger_id = 'tiger' + self.year + '.' + load_sumlevels()[self.sumlevel]['table']
        query = u'SELECT * FROM {table}'.format(
            table=tiger_id
        )
        sql_to_cartodb_table(self.tablename(), query)

    def tablename(self):
        resolution = load_sumlevels()[self.sumlevel]['slug'].replace('-', '_')
        return 'us_census_tiger{year}_{resolution}'.format(
            year=self.year, resolution=resolution)

    def output(self):
        target = CartoDBTarget(self.tablename())
        if self.force and target.exists():
            target.remove()
        return target


class ExtractClippedTiger(Task):
    # TODO this should be merged with ExtractTiger

    force = BooleanParameter(default=False)
    year = Parameter()
    sumlevel = Parameter()

    def requires(self):
        return ShorelineClipTiger(
            year=self.year, geography=load_sumlevels()[self.sumlevel]['table'])

    def run(self):
        query = u'SELECT * FROM {table}'.format(
            table=self.input().table
        )
        sql_to_cartodb_table(self.tablename(), query)

    def tablename(self):
        return self.input().table.replace('-', '_').replace('/', '_').replace('"', '').replace('.', '_')

    def output(self):
        target = CartoDBTarget(self.tablename())
        if self.force and target.exists():
            target.remove()
        return target



class ExtractAllTiger(Task):
    force = BooleanParameter(default=False)
    year = Parameter()

    def requires(self):
        for sumlevel in ('040', '050', '140', '150', '795', '860',):
            yield ExtractTiger(sumlevel=sumlevel, year=self.year,
                               force=self.force)

SUMLEVELS = load_sumlevels()
