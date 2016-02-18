#!/usr/bin/env python

'''
Tiger
'''

import json
import os
import subprocess
from tasks.util import (LoadPostgresFromURL, classpath, pg_cursor,
                        DefaultPostgresTarget, CartoDBTarget,
                        sql_to_cartodb_table, grouper, shell, slug_column)
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
        return classpath(self)

    @property
    def qualified_table(self):
        return '"{schema}"."{table}{year}"'.format(schema=self.schema,
                                                   table=self.table, year=self.year)

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
        cursor.execute('CREATE TABLE {output} AS '
                       'SELECT ST_Subdivide(geom) geom FROM {input} '
                       "WHERE mtfcc != 'H2030' OR awater > 3000000".format(
                           input=self.input().table,
                           output=self.output().table
                       ))
        cursor.execute('CREATE INDEX ON {output} USING GIST (geom)'.format(
            output=self.output().table
        ))
        cursor.connection.commit()
        self.output().touch()

    def output(self):
        output = DefaultPostgresTarget(
            table='"' + classpath(self) +'".simple_shoreline' + str(self.year))
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

    def requires(self):
        return {
            'tiger': ProcessTiger(year=self.year),
            'water': SimpleShoreline(year=self.year)
        }

    def run(self):
        cursor = pg_cursor()
        #tiger = [t for t in self.input()['tiger'] if t.data['slug'] == self.geography.lower()][0]
        pos = 'tiger{}.{}'.format(self.year, self.geography)
        neg = self.input()['water'].table
        if self.geography in ('puma', 'zcta5'):
            geoid = 'geoid10'
        else:
            geoid = 'geoid'
        pos_split = pos.split('.')[-1] + '_split'
        pos_neg_joined = pos_split + '_' + neg.split('.')[-1] + '_joined'
        pos_neg_joined_diffed = pos_neg_joined + '_diffed'
        pos_neg_joined_diffed_merged = pos_neg_joined_diffed + '_merged'
        output = self.output().table

        cursor.execute('CREATE SCHEMA IF NOT EXISTS "{schema}"'.format(
            schema=classpath(self)))

        # Split the positive table into geoms with a reasonable number of
        # vertices.
        cursor.execute('DROP TABLE IF EXISTS {pos_split}'.format(
            pos_split=pos_split))
        cursor.execute('CREATE TEMPORARY TABLE {pos_split} '
                       '(id serial primary key, {geoid} text, geom geometry)'.format(
                           geoid=geoid, pos_split=pos_split))
        cursor.execute('INSERT INTO {pos_split} ({geoid}, geom) '
                       'SELECT {geoid}, ST_Subdivide(geom) geom '
                       'FROM {pos}'.format(pos=pos, pos_split=pos_split, geoid=geoid))

        cursor.execute('CREATE INDEX ON {pos_split} USING GIST (geom)'.format(
            pos_split=pos_split))

        # Join the split up pos to the split up neg, then union the geoms based
        # off the split pos id (technically the union on pos geom is extraneous)
        cursor.execute('DROP TABLE IF EXISTS {pos_neg_joined}'.format(
            pos_neg_joined=pos_neg_joined))
        cursor.execute('CREATE TEMPORARY TABLE {pos_neg_joined} AS '
                       'SELECT id, {geoid}, ST_Union(neg.geom) neg_geom, '
                       '       ST_Union(pos.geom) pos_geom '
                       'FROM {pos_split} pos, {neg} neg '
                       'WHERE ST_Intersects(pos.geom, neg.geom) '
                       'GROUP BY id'.format(geoid=geoid, neg=neg,
                                            pos_split=pos_split,
                                            pos_neg_joined=pos_neg_joined))

        # Calculate the difference between the pos and neg geoms
        cursor.execute('DROP TABLE IF EXISTS {pos_neg_joined_diffed}'.format(
            pos_neg_joined_diffed=pos_neg_joined_diffed))
        cursor.execute('CREATE TEMPORARY TABLE {pos_neg_joined_diffed} '
                       'AS SELECT {geoid}, id, ST_Difference( '
                       'ST_MakeValid(pos_geom), ST_MakeValid(neg_geom)) geom '
                       'FROM {pos_neg_joined}'.format(
                           geoid=geoid,
                           pos_neg_joined=pos_neg_joined,
                           pos_neg_joined_diffed=pos_neg_joined_diffed))

        # Create new table with both diffed and non-diffed (didn't intersect with
        # water) geoms
        cursor.execute('DROP TABLE IF EXISTS {pos_neg_joined_diffed_merged}'.format(
            pos_neg_joined_diffed_merged=pos_neg_joined_diffed_merged))
        cursor.execute('CREATE TEMPORARY TABLE {pos_neg_joined_diffed_merged} '
                       'AS SELECT * FROM {pos_neg_joined_diffed}'.format(
                           pos_neg_joined_diffed=pos_neg_joined_diffed,
                           pos_neg_joined_diffed_merged=pos_neg_joined_diffed_merged))
        cursor.execute('INSERT INTO {pos_neg_joined_diffed_merged} '
                       'SELECT {geoid}, id, geom FROM {pos_split} '
                       'WHERE id NOT IN (SELECT id from {pos_neg_joined_diffed})'.format(
                           geoid=geoid,
                           pos_split=pos_split,
                           pos_neg_joined_diffed=pos_neg_joined_diffed,
                           pos_neg_joined_diffed_merged=pos_neg_joined_diffed_merged))
        cursor.execute('CREATE INDEX ON {pos_neg_joined_diffed_merged} '
                       'USING GIST (geom)'.format(
                           pos_neg_joined_diffed_merged=pos_neg_joined_diffed_merged))

        # Re-union the pos table based off its geoid
        cursor.execute('DROP TABLE IF EXISTS {output}'.format(output=output))
        cursor.execute('CREATE TABLE {output} AS '
                       'SELECT {geoid} AS geoid, ST_UNION(geom) AS geom '
                       'FROM {pos_neg_joined_diffed_merged} '
                       'GROUP BY {geoid}'.format(
                           geoid=geoid,
                           output=output,
                           pos_neg_joined_diffed_merged=pos_neg_joined_diffed_merged))

        cursor.connection.commit()
        self.output().touch()

    def output(self):
        tablename = '"{schema}".{geography}_{year}_shoreline_clipped'.format(
            schema=classpath(self),
            geography=str(self.geography).lower(),
            year=self.year)

        target = DefaultPostgresTarget(table=tablename)
        if self.force:
            target.untouch()
            self.force = False
        return target


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
        return slug_column(self.output().table)

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
    clipped = BooleanParameter(default=True)

    def requires(self):
        for sumlevel in ('040', '050', '140', '150', '795', '860',):
            if self.clipped:
                yield ExtractClippedTiger(sumlevel=sumlevel, year=self.year,
                                          force=self.force)
            else:
                yield ExtractTiger(sumlevel=sumlevel, year=self.year,
                                   force=self.force)

SUMLEVELS = load_sumlevels()
