#!/usr/bin/env python

'''
Tiger
'''

import json
import os
import subprocess
from collections import OrderedDict
from tasks.util import (LoadPostgresFromURL, classpath, DefaultPostgresTarget,
                        sql_to_cartodb_table, grouper, shell,
                        underscore_slugify, TableTask, ColumnTarget,
                        ColumnsTask
                       )
from tasks.meta import (OBSColumnTable, OBSColumn, current_session,
                        OBSColumnTag, OBSColumnToColumn, current_session)
from tasks.tags import CategoryTags

from luigi import (Task, WrapperTask, Parameter, LocalTarget, BooleanParameter,
                   IntParameter)
from psycopg2 import ProgrammingError


class GeomColumns(ColumnsTask):

    def version(self):
        return 3

    def requires(self):
        return {
            'tags': CategoryTags(),
        }

    def columns(self):
        tags = self.input()['tags']
        desc = lambda sumlevel: SUMLEVELS_BY_SLUG[sumlevel]['census_description']
        return {
            'block_group': OBSColumn(
                type='Geometry',
                name='US Census Block Groups',
                description=desc("block_group"),
                weight=10,
                tags=[tags['boundary']]
            ),
            'block': OBSColumn(
                type='Geometry',
                name='US Census Blocks',
                description=desc("block"),
                weight=3,
                tags=[tags['boundary']]
            ),
            'census_tract': OBSColumn(
                type='Geometry',
                name='US Census Tracts',
                description=desc("census_tract"),
                weight=9,
                tags=[tags['boundary']]
            ),
            'congressional_district': OBSColumn(
                type='Geometry',
                name='US Congressional Districts',
                description=desc("congressional_district"),
                weight=5,
                tags=[tags['boundary']]
            ),
            'county': OBSColumn(
                type='Geometry',
                name='US County',
                description=desc("county"),
                weight=7,
                tags=[tags['boundary']]
            ),
            'puma': OBSColumn(
                type='Geometry',
                name='US Census Public Use Microdata Areas',
                description=desc("puma"),
                weight=6,
                tags=[tags['boundary']]
            ),
            'state': OBSColumn(
                type='Geometry',
                name='US States',
                description=desc("state"),
                weight=8,
                tags=[tags['boundary']]
            ),
            'zcta5': OBSColumn(
                type='Geometry',
                name='US Census Zip Code Tabulation Areas',
                description=desc('zcta5'),
                weight=6,
                tags=[tags['boundary']]
            ),
            'school_district_elementary': OBSColumn(
                type='Geometry',
                name='Elementary School District',
                description=desc('school_district_elementary'),
                weight=3,
                tags=[tags['boundary']]
            ),
            'school_district_secondary': OBSColumn(
                type='Geometry',
                name='Secondary School District',
                description=desc('school_district_secondary'),
                weight=3,
                tags=[tags['boundary']]
            ),
            'school_district_unified': OBSColumn(
                type='Geometry',
                name='Unified School District',
                description=desc('school_district_unified'),
                weight=5,
                tags=[tags['boundary']]
            ),
        }


class GeoidColumns(ColumnsTask):

    def version(self):
        return 3

    def requires(self):
        return GeomColumns()

    def columns(self):
        geoms = self.input()
        return {
            'block_group_geoid': OBSColumn(
                type='Text',
                name='US Census Block Group Geoids',
                weight=0,
                targets={
                    geoms['block_group']: 'geom_ref'
                }
            ),
            'block_geoid': OBSColumn(
                type='Text',
                name='US Census Block Geoids',
                weight=0,
                targets={
                    geoms['block']: 'geom_ref'
                }
            ),
            'census_tract_geoid': OBSColumn(
                type='Text',
                name='US Census Tract Geoids',
                description="",
                weight=0,
                targets={
                    geoms['census_tract']: 'geom_ref'
                }
            ),
            'congressional_district_geoid': OBSColumn(
                type='Text',
                name='US Congressional District Geoids',
                description="",
                weight=0,
                targets={
                    geoms['congressional_district']: 'geom_ref'
                }
            ),
            'county_geoid': OBSColumn(
                type='Text',
                name='US County Geoids',
                description="",
                weight=0,
                targets={
                    geoms['county']: 'geom_ref'
                }
            ),
            'puma_geoid': OBSColumn(
                type='Text',
                name='US Census Public Use Microdata Area Geoids',
                description="",
                weight=0,
                targets={
                    geoms['puma']: 'geom_ref'
                }
            ),
            'state_geoid': OBSColumn(
                type='Text',
                name='US State Geoids',
                description="",
                weight=0,
                targets={
                    geoms['state']: 'geom_ref'
                }
            ),
            'zcta5_geoid': OBSColumn(
                type='Text',
                name='US Census Zip Code Tabulation Area Geoids',
                description="",
                weight=0,
                targets={
                    geoms['zcta5']: 'geom_ref'
                }
            ),
            'school_district_elementary_geoid': OBSColumn(
                type='Text',
                name='Elementary School District Geoids',
                description="",
                weight=0,
                targets={
                    geoms['school_district_elementary']: 'geom_ref'
                }
            ),
            'school_district_secondary_geoid': OBSColumn(
                type='Text',
                name='Secondary School District Geoids',
                description="",
                weight=0,
                targets={
                    geoms['school_district_secondary']: 'geom_ref'
                }
            ),
            'school_district_unified_geoid': OBSColumn(
                type='Text',
                name='Unified School District Geoids',
                description="",
                weight=0,
                targets={
                    geoms['school_district_unified']: 'geom_ref'
                }
            ),
        }


class DownloadTigerGeography(Task):

    year = IntParameter()
    geography = Parameter()
    force = BooleanParameter() # TODO

    url_format = 'ftp://ftp2.census.gov/geo/tiger/TIGER{year}/{geography}/'

    @property
    def url(self):
        return self.url_format.format(year=self.year, geography=self.geography)

    @property
    def directory(self):
        return os.path.join('tmp', classpath(self), str(self.year))

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

    def requires(self):
        return DownloadTigerGeography(year=self.year, geography=self.geography)

    def run(self):
        for infile in self.input():
            subprocess.check_call("unzip -n -q -d $(dirname {zippath}) '{zippath}'".format(
                zippath=infile.path), shell=True)

    def output(self):
        for infile in self.input():
            yield LocalTarget(infile.path.replace('.zip', '.shp'))


class TigerGeographyShapefileToSQL(TableTask):
    '''
    Take downloaded shapefiles and load them into Postgres
    '''

    year = Parameter()
    geography = Parameter()

    def requires(self):
        return UnzipTigerGeography(year=self.year, geography=self.geography)

    def version(self):
        return 2

    def columns(self):
        return {}

    def bounds(self):
        return 'BOX(0 0,0 0)'

    def timespan(self):
        return str(self.year)

    def populate(self):
        session = current_session()

        shapefiles = self.input()
        cmd = 'PG_USE_COPY=yes PGCLIENTENCODING=latin1 ' \
                'ogr2ogr -f PostgreSQL PG:dbname=$PGDATABASE ' \
                '-t_srs "EPSG:4326" -nlt MultiPolygon -nln {qualified_table} ' \
                '-overwrite {shpfile_path} '.format(
                    qualified_table=self.output().table,
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
                    qualified_table=self.output().table),
                shell=True)

        # Spatial index
        session.execute('ALTER TABLE {qualified_table} RENAME COLUMN '
                        'wkb_geometry TO geom'.format(
                            qualified_table=self.output().table))
        session.execute('CREATE INDEX ON {qualified_table} USING GIST (geom)'.format(
            qualified_table=self.output().table))


class DownloadTiger(LoadPostgresFromURL):

    url_template = 'https://s3.amazonaws.com/census-backup/tiger/{year}/tiger{year}_backup.sql.gz'
    year = Parameter()

    @property
    def schema(self):
        return 'tiger{year}'.format(year=self.year)

    def identifier(self):
        return self.schema

    def run(self):
        shell("psql -c 'DROP SCHEMA \"{schema}\" CASCADE'".format(schema=self.schema))
        url = self.url_template.format(year=self.year)
        self.load_from_url(url)
        self.output().touch()


class SimpleShorelineColumns(ColumnsTask):

    def columns(self):
        return {
            'geom': OBSColumn(type='Geometry')
        }


class SimpleShoreline(TableTask):

    force = BooleanParameter(default=False)
    year = Parameter()

    def requires(self):
        return {
            'meta': SimpleShorelineColumns(),
            'data': TigerGeographyShapefileToSQL(geography='AREAWATER', year=self.year)
        }

    def columns(self):
        return self.input()['meta']

    def timespan(self):
        return str(self.year)

    def bounds(self):
        return 'BOX(0 0,0 0)'

    def populate(self):
        session = current_session()
        session.execute('INSERT INTO {output} '
                        'SELECT ST_Subdivide(geom) geom FROM {input} '
                        "WHERE mtfcc != 'H2030' OR awater > 3000000".format(
                            input=self.input()['data'].table,
                            output=self.output().table
                        ))
        session.execute('CREATE INDEX ON {output} USING GIST (geom)'.format(
            output=self.output().table
        ))


class ShorelineClipTiger(TableTask):
    '''
    Clip the provided geography to shoreline.
    '''

    # MTFCC meanings:
    # http://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp2009/TGRSHP09AF.pdf

    year = Parameter()
    geography = Parameter()

    def requires(self):
        return {
            'tiger': SumLevel(year=self.year, geography=self.geography),
            'water': SimpleShoreline(year=self.year)
        }

    def timespan(self):
        return str(self.year)

    def bounds(self):
        return 'BOX(0 0,0 0)'

    def columns(self):
        return []

    def populate(self):
        session = current_session()
        #tiger = [t for t in self.input()['tiger'] if t.data['slug'] == self.geography.lower()][0]
        pos = self.input()['tiger'].table
        neg = self.input()['water'].table
        pos_split = pos.split('.')[-1] + '_split'
        pos_neg_joined = pos_split + '_' + neg.split('.')[-1] + '_joined'
        pos_neg_joined_diffed = pos_neg_joined + '_diffed'
        pos_neg_joined_diffed_merged = pos_neg_joined_diffed + '_merged'
        output = self.output().table

        # Split the positive table into geoms with a reasonable number of
        # vertices.
        session.execute('DROP TABLE IF EXISTS {pos_split}'.format(
            pos_split=pos_split))
        session.execute('CREATE TEMPORARY TABLE {pos_split} '
                        '(id serial primary key, geoid text, geom geometry)'.format(
                            pos_split=pos_split))
        session.execute('INSERT INTO {pos_split} (geoid, geom) '
                        'SELECT geoid, ST_Subdivide(geom) geom '
                        'FROM {pos}'.format(pos=pos, pos_split=pos_split))

        session.execute('CREATE INDEX ON {pos_split} USING GIST (geom)'.format(
            pos_split=pos_split))

        # Join the split up pos to the split up neg, then union the geoms based
        # off the split pos id (technically the union on pos geom is extraneous)
        session.execute('DROP TABLE IF EXISTS {pos_neg_joined}'.format(
            pos_neg_joined=pos_neg_joined))
        session.execute('CREATE TEMPORARY TABLE {pos_neg_joined} AS '
                        'SELECT id, geoid, ST_Union(neg.geom) neg_geom, '
                        '       ST_Union(pos.geom) pos_geom '
                        'FROM {pos_split} pos, {neg} neg '
                        'WHERE ST_Intersects(pos.geom, neg.geom) '
                        'GROUP BY id'.format(neg=neg,
                                             pos_split=pos_split,
                                             pos_neg_joined=pos_neg_joined))

        # Calculate the difference between the pos and neg geoms
        session.execute('DROP TABLE IF EXISTS {pos_neg_joined_diffed}'.format(
            pos_neg_joined_diffed=pos_neg_joined_diffed))
        session.execute('CREATE TEMPORARY TABLE {pos_neg_joined_diffed} '
                        'AS SELECT geoid, id, ST_Difference( '
                        'ST_MakeValid(pos_geom), ST_MakeValid(neg_geom)) geom '
                        'FROM {pos_neg_joined}'.format(
                            pos_neg_joined=pos_neg_joined,
                            pos_neg_joined_diffed=pos_neg_joined_diffed))

        # Create new table with both diffed and non-diffed (didn't intersect with
        # water) geoms
        session.execute('DROP TABLE IF EXISTS {pos_neg_joined_diffed_merged}'.format(
            pos_neg_joined_diffed_merged=pos_neg_joined_diffed_merged))
        session.execute('CREATE TEMPORARY TABLE {pos_neg_joined_diffed_merged} '
                        'AS SELECT * FROM {pos_neg_joined_diffed}'.format(
                            pos_neg_joined_diffed=pos_neg_joined_diffed,
                            pos_neg_joined_diffed_merged=pos_neg_joined_diffed_merged))
        session.execute('INSERT INTO {pos_neg_joined_diffed_merged} '
                        'SELECT geoid, id, geom FROM {pos_split} '
                        'WHERE id NOT IN (SELECT id from {pos_neg_joined_diffed})'.format(
                            pos_split=pos_split,
                            pos_neg_joined_diffed=pos_neg_joined_diffed,
                            pos_neg_joined_diffed_merged=pos_neg_joined_diffed_merged))
        session.execute('CREATE INDEX ON {pos_neg_joined_diffed_merged} '
                        'USING GIST (geom)'.format(
                            pos_neg_joined_diffed_merged=pos_neg_joined_diffed_merged))

        # Re-union the pos table based off its geoid
        session.execute('DROP TABLE IF EXISTS {output}'.format(output=output))
        session.execute('CREATE TABLE {output} AS '
                        'SELECT geoid, ST_UNION(geom) AS geom '
                        'FROM {pos_neg_joined_diffed_merged} '
                        'GROUP BY geoid'.format(
                            output=output,
                            pos_neg_joined_diffed_merged=pos_neg_joined_diffed_merged))


class SumLevel(TableTask):

    clipped = BooleanParameter(default=False)
    geography = Parameter()
    year = Parameter()

    @property
    def geoid(self):
        return 'geoid10' if self.geography in ('zcta5', 'puma', ) else 'geoid'

    @property
    def input_tablename(self):
        return SUMLEVELS_BY_SLUG[self.geography]['table']

    def version(self):
        return 4

    def requires(self):
        if self.clipped:
            tiger = ShorelineClipTiger(
                year=self.year, geography=self.input_tablename)
        else:
            tiger = DownloadTiger(year=self.year)
        return {
            'data': tiger,
            'geoids': GeoidColumns(),
            'geoms': GeomColumns()
        }

    def columns(self):
        return OrderedDict([
            ('geoid', self.input()['geoids'][self.geography + '_geoid']),
            ('the_geom', self.input()['geoms'][self.geography])
        ])

    def timespan(self):
        return self.year

    def bounds(self):
        if not self.input()['data'].exists():
            return
        if self.clipped:
            from_clause = self.input()['data'].table
        else:
            from_clause = '{inputschema}.{input_tablename}'.format(
                inputschema=self.input()['data'].table,
                input_tablename=self.input_tablename,
            )
        session = current_session()
        return session.execute('SELECT ST_EXTENT(geom) FROM '
                               '{from_clause}'.format(
                                   from_clause=from_clause
                               )).first()[0]

    def populate(self):
        session = current_session()
        if self.clipped:
            from_clause = self.input()['data'].table
        else:
            from_clause = '{inputschema}.{input_tablename}'.format(
                inputschema=self.input()['data'].table,
                input_tablename=self.input_tablename,
            )
        session.execute('INSERT INTO {output} (geoid, the_geom) '
                        'SELECT {geoid}, geom the_geom  '
                        'FROM {from_clause}'.format(
                            geoid=self.geoid,
                            output=self.output().table,
                            from_clause=from_clause
                        ))


class AllSumLevels(WrapperTask):
    '''
    Compute all sumlevels
    '''

    year = Parameter(default=2013)

    def requires(self):
        #for clipped in (True, False):
        for clipped in (False, ):
            for geo in ('state', 'county', 'census_tract', 'block_group',
                        'puma', 'zcta5', 'school_district_elementary',
                        'school_district_secondary', 'school_district_unified'):
                yield SumLevel(year=self.year, geography=geo, clipped=clipped)


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
SUMLEVELS_BY_SLUG = dict([(v['slug'], v) for k, v in SUMLEVELS.iteritems()])
