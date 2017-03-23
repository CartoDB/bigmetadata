#!/usr/bin/env python

'''
Tiger
'''

import json
import os
import subprocess
from collections import OrderedDict
from tasks.util import (LoadPostgresFromURL, classpath, TempTableTask,
                        sql_to_cartodb_table, grouper, shell,
                        underscore_slugify, TableTask, ColumnTarget,
                        ColumnsTask, TagsTask, Carto2TempTableTask
                       )
from tasks.meta import (OBSColumnTable, OBSColumn, current_session, GEOM_REF, GEOM_NAME,
                        OBSColumnTag, OBSTag, OBSColumnToColumn, current_session)
from tasks.tags import SectionTags, SubsectionTags, LicenseTags

from luigi import (Task, WrapperTask, Parameter, LocalTarget, BooleanParameter,
                   IntParameter)
from psycopg2 import ProgrammingError
from decimal import Decimal

class SourceTags(TagsTask):
    def version(self):
        return 1

    def tags(self):
        return [
            OBSTag(id='tiger-source',
                   name='US Census TIGER/Line Shapefiles',
                   type='source',
                   description='`TIGER/Line Shapefiles <https://www.census.gov/geo/maps-data/data/tiger-line.html>`_')
        ]



class ClippedGeomColumns(ColumnsTask):

    def version(self):
        return 11

    def requires(self):
        return {
            'geom_columns': GeomColumns(),
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'source': SourceTags(),
            'license': LicenseTags(),
        }

    def columns(self):
        cols = OrderedDict()
        session = current_session()
        input_ = self.input()
        sections = input_['sections']
        subsections = input_['subsections']
        source = input_['source']['tiger-source']
        license = input_['license']['no-restrictions']

        for colname, coltarget in self.input()['geom_columns'].iteritems():
            col = coltarget.get(session)
            cols[colname + '_clipped'] = OBSColumn(
                type='Geometry',
                name='Shoreline clipped ' + col.name,
                weight=Decimal(col.weight) + Decimal(0.01),
                description='A cartography-ready version of {name}'.format(
                    name=col.name),
                targets={col: 'cartography'},
                tags=[sections['united_states'], subsections['boundary'],
                      source, license]
            )

        return cols


class GeomColumns(ColumnsTask):

    def version(self):
        return 15

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'source': SourceTags(),
            'license': LicenseTags(),
        }

    def _generate_desc(self, sumlevel):
        '''
        Add figure to the description
        '''
        return SUMLEVELS_BY_SLUG[sumlevel]['census_description']

    def columns(self):
        input_ = self.input()
        sections = input_['sections']
        subsections = input_['subsections']
        source = input_['source']['tiger-source']
        license = input_['license']['no-restrictions']
        columns = {
            'block_group': OBSColumn(
                type='Geometry',
                name='US Census Block Groups',
                description=self._generate_desc("block_group"),
                weight=10,
                tags=[sections['united_states'], subsections['boundary']]
            ),
            'block': OBSColumn(
                type='Geometry',
                name='US Census Blocks',
                description=self._generate_desc("block"),
                weight=0,
                tags=[sections['united_states'], subsections['boundary']]
            ),
            'census_tract': OBSColumn(
                type='Geometry',
                name='US Census Tracts',
                description=self._generate_desc("census_tract"),
                weight=9,
                tags=[sections['united_states'], subsections['boundary']]
            ),
            'congressional_district': OBSColumn(
                type='Geometry',
                name='US Congressional Districts',
                description=self._generate_desc("congressional_district"),
                weight=5.4,
                tags=[sections['united_states'], subsections['boundary']]
            ),
            'county': OBSColumn(
                type='Geometry',
                name='US County',
                description=self._generate_desc("county"),
                weight=7,
                tags=[sections['united_states'], subsections['boundary']]
            ),
            'puma': OBSColumn(
                type='Geometry',
                name='US Census Public Use Microdata Areas',
                description=self._generate_desc("puma"),
                weight=5.5,
                tags=[sections['united_states'], subsections['boundary']]
            ),
            'state': OBSColumn(
                type='Geometry',
                name='US States',
                description=self._generate_desc("state"),
                weight=8,
                tags=[sections['united_states'], subsections['boundary']]
            ),
            'zcta5': OBSColumn(
                type='Geometry',
                name='US Census Zip Code Tabulation Areas',
                description=self._generate_desc('zcta5'),
                weight=6,
                tags=[sections['united_states'], subsections['boundary']]
            ),
            'school_district_elementary': OBSColumn(
                type='Geometry',
                name='Elementary School District',
                description=self._generate_desc('school_district_elementary'),
                weight=2.8,
                tags=[sections['united_states'], subsections['boundary']]
            ),
            'school_district_secondary': OBSColumn(
                type='Geometry',
                name='Secondary School District',
                description=self._generate_desc('school_district_secondary'),
                weight=2.9,
                tags=[sections['united_states'], subsections['boundary']]
            ),
            'school_district_unified': OBSColumn(
                type='Geometry',
                name='Unified School District',
                description=self._generate_desc('school_district_unified'),
                weight=5,
                tags=[sections['united_states'], subsections['boundary']]
            ),
            'cbsa': OBSColumn(
                type='Geometry',
                name='Core Based Statistical Area (CBSA)',
                description=self._generate_desc("cbsa"),
                weight=1,
                tags=[sections['united_states'], subsections['boundary']]
            ),
            'place': OBSColumn(
                type='Geometry',
                name='Incorporated Places',
                description=self._generate_desc("place"),
                weight=1.1,
                tags=[sections['united_states'], subsections['boundary']]
            ),
        }

        for _,col in columns.iteritems():
            col.tags.append(source)
            col.tags.append(license)
        return columns


class Attributes(ColumnsTask):

    def version(self):
        return 2

    def requires(self):
        return SectionTags()

    def columns(self):
        united_states = self.input()['united_states']
        return OrderedDict([
            ('aland', OBSColumn(
                type='Numeric',
                name='Land area',
                aggregate='sum',
                weight=0,
            )),
            ('awater', OBSColumn(
                type='Numeric',
                name='Water area',
                aggregate='sum',
                weight=0,
            )),
            ('name', OBSColumn(
                type='Text',
                name='Name of feature',
                weight=3,
                tags=[united_states]
                targets={geom: GEOM_NAME}
}
            ))
        ])


class GeoidColumns(ColumnsTask):

    def version(self):
        return 6

    def requires(self):
        return {
            'raw': GeomColumns(),
            'clipped': ClippedGeomColumns()
        }

    def columns(self):
        cols = OrderedDict()
        clipped = self.input()['clipped']
        for colname, coltarget in self.input()['raw'].iteritems():
            col = coltarget._column
            cols[colname + '_geoid'] = OBSColumn(
                type='Text',
                name=col.name + ' Geoids',
                weight=0,
                targets={
                    col: GEOM_REF,
                    clipped[colname + '_clipped']._column: GEOM_REF
                }
            )

        return cols


class DownloadTigerGeography(Task):

    year = IntParameter()
    geography = Parameter()

    url_format = 'ftp://ftp2.census.gov/geo/tiger/TIGER{year}/{geography}/'

    @property
    def url(self):
        return self.url_format.format(year=self.year, geography=self.geography)

    @property
    def directory(self):
        return os.path.join('tmp', classpath(self), str(self.year))

    def run(self):
        shell('wget --recursive --continue --accept=*.zip '
              '--no-parent --cut-dirs=3 --no-host-directories '
              '--directory-prefix={directory} '
              '{url}'.format(directory=self.directory, url=self.url))

    def output(self):
        filenames = shell('ls {}'.format(os.path.join(
            self.directory, self.geography, '*.zip'))).split('\n')
        for path in filenames:
            yield LocalTarget(path)

    def complete(self):
        try:
            exists = shell('ls {}'.format(os.path.join(self.directory, self.geography, '*.zip')))
            return exists != ''
        except subprocess.CalledProcessError as err:
            return False


class UnzipTigerGeography(Task):
    '''
    Unzip tiger geography
    '''

    year = Parameter()
    geography = Parameter()

    def requires(self):
        return DownloadTigerGeography(year=self.year, geography=self.geography)

    @property
    def directory(self):
        return os.path.join('tmp', classpath(self), str(self.year), self.geography)

    def run(self):
        #for infile in self.input():
        cmd = "cd {path} && find -iname '*.zip' -print0 | xargs -0 -n1 unzip -n -q ".format(
            path=self.directory)
        shell(cmd)

    def output(self):
        shps = shell('ls {}'.format(os.path.join(self.directory, '*.shp')))
        for path in shps:
            yield LocalTarget(path)

    def complete(self):
        try:
            exists = shell('ls {}'.format(os.path.join(self.directory, '*.shp')))
            return exists != ''
        except subprocess.CalledProcessError:
            return False


class TigerGeographyShapefileToSQL(TempTableTask):
    '''
    Take downloaded shapefiles and load them into Postgres
    '''

    year = Parameter()
    geography = Parameter()

    def requires(self):
        return UnzipTigerGeography(year=self.year, geography=self.geography)

    def run(self):
        shapefiles = shell('ls {dir}/*.shp'.format(
            dir=os.path.join('tmp', classpath(self), str(self.year), self.geography)
        )).strip().split('\n')

        cmd = 'ogrinfo {shpfile_path}'.format(shpfile_path=shapefiles[0])
        resp = shell(cmd)
        if 'Polygon' in resp:
            nlt = '-nlt MultiPolygon'
        else:
            nlt = ''

        cmd = 'PG_USE_COPY=yes PGCLIENTENCODING=latin1 ' \
                'ogr2ogr -f PostgreSQL "PG:dbname=$PGDATABASE active_schema={schema}" ' \
                '-t_srs "EPSG:4326" {nlt} -nln {tablename} ' \
                '-lco OVERWRITE=yes ' \
                '-lco SCHEMA={schema} {shpfile_path} '.format(
                    tablename=self.output().tablename,
                    schema=self.output().schema, nlt=nlt,
                    shpfile_path=shapefiles.pop())
        shell(cmd)

        # chunk into 500 shapefiles at a time.
        for i, shape_group in enumerate(grouper(shapefiles, 500)):
            shell(
                'export PG_USE_COPY=yes PGCLIENTENCODING=latin1; '
                'echo \'{shapefiles}\' | xargs -P 16 -I shpfile_path '
                'ogr2ogr -f PostgreSQL "PG:dbname=$PGDATABASE '
                'active_schema={schema}" -append '
                '-t_srs "EPSG:4326" {nlt} -nln {tablename} '
                'shpfile_path '.format(
                    shapefiles='\n'.join([shp for shp in shape_group if shp]),
                    tablename=self.output().tablename, nlt=nlt,
                    schema=self.output().schema))
            print 'imported {} shapefiles'.format((i + 1) * 500)

        session = current_session()
        # Spatial index
        session.execute('ALTER TABLE {qualified_table} RENAME COLUMN '
                        'wkb_geometry TO geom'.format(
                            qualified_table=self.output().table))
        session.execute('CREATE INDEX ON {qualified_table} USING GIST (geom)'.format(
            qualified_table=self.output().table))


class DownloadTiger(LoadPostgresFromURL):

    url_template = 'https://s3.amazonaws.com/census-backup/tiger/{year}/tiger{year}_backup.sql.gz'
    year = Parameter()

    def run(self):
        schema = 'tiger{year}'.format(year=self.year)
        shell("psql -c 'DROP SCHEMA IF EXISTS \"{schema}\" CASCADE'".format(schema=schema))
        shell("psql -c 'CREATE SCHEMA \"{schema}\"'".format(schema=schema))
        url = self.url_template.format(year=self.year)
        self.load_from_url(url)


class SimpleShoreline(TempTableTask):

    year = Parameter()

    def requires(self):
        return {
            'data': TigerGeographyShapefileToSQL(geography='AREAWATER', year=self.year),
            'us_landmask': Carto2TempTableTask(table='us_landmask_union'),
        }

    def run(self):
        session = current_session()
        session.execute('CREATE TABLE {output} AS '
                        'SELECT ST_Subdivide(geom) geom, false in_landmask, '
                        '       aland, awater, mtfcc '
                        'FROM {input} '
                        "WHERE mtfcc != 'H2030' OR awater > 300000".format(
                            input=self.input()['data'].table,
                            output=self.output().table
                        ))
        session.execute('CREATE INDEX ON {output} USING GIST (geom)'.format(
            output=self.output().table
        ))

        session.execute('UPDATE {output} data SET in_landmask = True '
                        'FROM {landmask} landmask '
                        'WHERE ST_WITHIN(data.geom, landmask.the_geom)'.format(
                            landmask=self.input()['us_landmask'].table,
                            output=self.output().table
                        ))


class SplitSumLevel(TempTableTask):
    '''
    Split the positive table into geoms with a reasonable number of
    vertices.  Assumes there is a geoid and the_geom column.
    '''

    year = Parameter()
    geography = Parameter()

    def requires(self):
        return SumLevel(year=self.year, geography=self.geography)

    def run(self):
        session = current_session()
        session.execute('CREATE TABLE {output} '
                        '(id serial primary key, geoid text, the_geom geometry, '
                        'aland NUMERIC, awater NUMERIC)'.format(
                            output=self.output().table))
        session.execute('INSERT INTO {output} (geoid, the_geom, aland, awater) '
                        'SELECT geoid, ST_Subdivide(the_geom) the_geom, '
                        '       aland, awater '
                        'FROM {input} '
                        'WHERE aland > 0 '.format(output=self.output().table,
                                                  input=self.input().table))

        session.execute('CREATE INDEX ON {output} USING GIST (the_geom)'.format(
            output=self.output().table))


class JoinTigerWaterGeoms(TempTableTask):
    '''
    Join the split up pos to the split up neg, then union the geoms based
    off the split pos id (technically the union on pos geom is extraneous)
    '''

    year = Parameter()
    geography = Parameter()

    def requires(self):
        return {
            'pos': SplitSumLevel(year=self.year, geography=self.geography),
            'neg': SimpleShoreline(year=self.year),
        }

    def use_mask(self):
        '''
        Returns true if we should not clip interior geometries, False otherwise.
        '''
        return self.geography.lower() in ('state', 'county', )

    def run(self):
        session = current_session()
        stmt = ('CREATE TABLE {output} AS '
                'SELECT id, geoid, ST_Union(ST_MakeValid(neg.geom)) neg_geom, '
                '       MAX(pos.the_geom) pos_geom '
                'FROM {pos} pos, {neg} neg '
                'WHERE ST_Intersects(pos.the_geom, neg.geom) '
                '      AND pos.awater > 0 '
                '      {mask_clause} '
                'GROUP BY id '.format(
                    neg=self.input()['neg'].table,
                    mask_clause=' AND in_landmask = false' if self.use_mask() else '',
                    pos=self.input()['pos'].table,
                    output=self.output().table), )[0]
        session.execute(stmt)


class DiffTigerWaterGeoms(TempTableTask):
    '''
    Calculate the difference between the pos and neg geoms
    '''

    year = Parameter()
    geography = Parameter()

    def requires(self):
        return JoinTigerWaterGeoms(year=self.year, geography=self.geography)

    def run(self):
        session = current_session()
        stmt = ('CREATE TABLE {output} '
                'AS SELECT geoid, id, ST_Difference( '
                'ST_MakeValid(pos_geom), ST_MakeValid(neg_geom)) the_geom '
                #'pos_geom, neg_geom) the_geom '
                'FROM {input}'.format(
                    output=self.output().table,
                    input=self.input().table), )[0]
        session.execute(stmt)


class PreunionTigerWaterGeoms(TempTableTask):
    '''
    Create new table with both diffed and non-diffed (didn't intersect with
    water) geoms
    '''

    year = Parameter()
    geography = Parameter()

    def requires(self):
        return {
            'diffed': DiffTigerWaterGeoms(year=self.year, geography=self.geography),
            'split': SplitSumLevel(year=self.year, geography=self.geography)
        }

    def run(self):
        session = current_session()
        session.execute('CREATE TABLE {output} '
                        'AS SELECT geoid::text, id::int, the_geom::geometry, '
                        'aland::numeric, awater::Numeric '
                        'FROM {split} LIMIT 0 '.format(
                            output=self.output().table,
                            split=self.input()['split'].table))
        session.execute('INSERT INTO {output} (geoid, id, the_geom) '
                        'SELECT geoid, id, the_geom FROM {diffed} '
                        'WHERE ST_Area(ST_Transform(the_geom, 3857)) > 5000'
                        '  AND ST_NPoints(the_geom) > 10 '.format(
                            output=self.output().table,
                            diffed=self.input()['diffed'].table))
        session.execute('INSERT INTO {output} '
                        'SELECT geoid, id, the_geom, aland, awater FROM {split} '
                        'WHERE id NOT IN (SELECT id from {diffed})'.format(
                            split=self.input()['split'].table,
                            diffed=self.input()['diffed'].table,
                            output=self.output().table))
        session.execute('CREATE INDEX ON {output} (geoid) '.format(
            output=self.output().table))


class UnionTigerWaterGeoms(TempTableTask):
    '''
    Re-union the pos table based off its geoid, this includes holes in
    the output geoms
    '''

    year = Parameter()
    geography = Parameter()

    def requires(self):
        return PreunionTigerWaterGeoms(year=self.year, geography=self.geography)

    def run(self):
        session = current_session()
        session.execute('CREATE TABLE {output} AS '
                        'SELECT geoid, ST_Union(ST_MakeValid(the_geom)) AS the_geom, '
                        '       MAX(aland) aland, MAX(awater) awater '
                        'FROM {input} '
                        'GROUP BY geoid'.format(
                            output=self.output().table,
                            input=self.input().table))


class ShorelineClip(TableTask):
    '''
    Clip the provided geography to shoreline.
    '''

    # MTFCC meanings:
    # http://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp2009/TGRSHP09AF.pdf

    year = Parameter()
    geography = Parameter()

    def version(self):
        return 6

    def requires(self):
        return {
            'data': UnionTigerWaterGeoms(year=self.year, geography=self.geography),
            'geoms': ClippedGeomColumns(),
            'geoids': GeoidColumns(),
            'attributes': Attributes(),
        }

    def columns(self):
        return OrderedDict([
            ('geoid', self.input()['geoids'][self.geography + '_geoid']),
            ('the_geom', self.input()['geoms'][self.geography + '_clipped']),
            ('aland', self.input()['attributes']['aland']),
            ('name', self.input()['attributes']['name']),
        ])

    def timespan(self):
        return self.year

    def populate(self):
        session = current_session()
        stmt = ('INSERT INTO {output} '
                'SELECT geoid, ST_Union(ST_MakePolygon(ST_ExteriorRing(the_geom))) AS the_geom, '
                '       MAX(aland) AS aland, cdb_observatory.FIRST(name) AS name '
                'FROM ( '
                '    SELECT geoid, (ST_Dump(the_geom)).geom AS the_geom, '
                '           aland, name '
                '    FROM {input} '
                ") holes WHERE GeometryType(the_geom) = 'POLYGON' "
                'GROUP BY geoid'.format(
                    output=self.output().table,
                    input=self.input()['data'].table), )[0]
        session.execute(stmt)


class SumLevel(TableTask):

    geography = Parameter()
    year = Parameter()

    def has_10_suffix(self):
        return self.geography.lower() in ('puma', 'zcta5', 'block', )

    @property
    def geoid(self):
        return 'geoid10' if self.has_10_suffix() else 'geoid'

    @property
    def aland(self):
        return 'aland10' if self.has_10_suffix() else 'aland'

    @property
    def awater(self):
        return 'awater10' if self.has_10_suffix() else 'awater'

    @property
    def name(self):
        if self.geography in ('state', 'county', 'census_tract', 'place',
                              'school_district_elementary', 'cbsa', 'metdiv',
                              'school_district_secondary',
                              'school_district_unified'):
            return 'name'
        elif self.geography in ('congressional_district', 'block_group'):
            return 'namelsad'
        elif self.geography in ('block'):
            return 'name10'
        elif self.geography in ('puma'):
            return 'namelsad10'

    @property
    def input_tablename(self):
        return SUMLEVELS_BY_SLUG[self.geography]['table']

    def version(self):
        return 10

    def requires(self):
        tiger = DownloadTiger(year=self.year)
        return {
            'data': tiger,
            'attributes': Attributes(),
            'geoids': GeoidColumns(),
            'geoms': GeomColumns(),
        }

    def columns(self):
        cols = OrderedDict([
            ('geoid', self.input()['geoids'][self.geography + '_geoid']),
            ('the_geom', self.input()['geoms'][self.geography]),
            ('aland', self.input()['attributes']['aland']),
            ('awater', self.input()['attributes']['awater']),
        ])
        if self.name:
            cols['name'] = self.input()['attributes']['name']

        return cols

    def timespan(self):
        return self.year

    def populate(self):
        session = current_session()
        from_clause = '{inputschema}.{input_tablename}'.format(
            inputschema='tiger' + str(self.year),
            input_tablename=self.input_tablename,
        )
        in_colnames = [self.geoid, 'geom', self.aland, self.awater]
        if self.name:
            in_colnames.append(self.name)
        out_colnames = self.columns().keys()
        session.execute('INSERT INTO {output} ({out_colnames}) '
                        'SELECT {in_colnames} '
                        'FROM {from_clause} '.format(
                            output=self.output().table,
                            in_colnames=', '.join(in_colnames),
                            out_colnames=', '.join(out_colnames),
                            from_clause=from_clause
                        ))


class AllSumLevels(WrapperTask):
    '''
    Compute all sumlevels
    '''

    year = Parameter()

    def requires(self):
        for geo in ('state', 'county', 'census_tract', 'block_group', 'place',
                    'puma', 'zcta5', 'school_district_elementary', 'cbsa',
                    'school_district_secondary', 'school_district_unified',
                    'block', 'congressional_district'):
            yield SumLevel(year=self.year, geography=geo)
            yield ShorelineClip(year=self.year, geography=geo)


class SharedTigerColumns(ColumnsTask):

    def version(self):
        return 2

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'source': SourceTags(),
            'license': LicenseTags(),
        }

    def columns(self):
        input_ = self.input()
        return OrderedDict([
            ('fullname', OBSColumn(
                type='Text',
                name='Name of the feature',
                weight=3,
                tags=[input_['sections']['united_states'],
                      input_['source']['tiger-source'],
                      input_['license']['no-restrictions']]
            )),
            ('mtfcc', OBSColumn(
                type='Text',
                name='MAF/TIGER Feature Class Code Definitions',
                description='''The MAF/TIGER Feature Class Code (MTFCC) is
                a 5-digit code assigned by the Census Bureau intended to
                classify and describe geographic objects or features. These
                codes can be found in the TIGER/Line products.  A full list of
                code meanings can be found `here
                <https://www.census.gov/geo/reference/mtfcc.html>`_.''',
                weight=3,
                tags=[input_['sections']['united_states'],
                      input_['source']['tiger-source'],
                      input_['license']['no-restrictions']]
            ))
        ])


class PointLandmarkColumns(ColumnsTask):
    '''
    Point landmark column definitions
    '''

    def version(self):
        return 8

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'source': SourceTags(),
            'license': LicenseTags(),
        }

    def columns(self):
        input_ = self.input()
        geom = OBSColumn(
            id='pointlm_geom',
            type='Geometry(Point)',
            weight=5,
            tags=[input_['sections']['united_states'],
                  input_['subsections']['poi'],
                  input_['source']['tiger-source'],
                  input_['license']['no-restrictions']]
        )
        cols = OrderedDict([
            ('pointlm_id', OBSColumn(
                type='Text',
                weight=0,
                targets={geom: GEOM_REF}
            )),
            ('pointlm_geom', geom)
        ])
        return cols


class PointLandmark(TableTask):
    '''
    Point landmark data from the census
    '''

    year = Parameter()

    def version(self):
        return 2

    def requires(self):
        return {
            'data': TigerGeographyShapefileToSQL(year=self.year,
                                                 geography='POINTLM'),
            'meta': PointLandmarkColumns(),
            'shared': SharedTigerColumns()
        }

    def timespan(self):
        return self.year

    def columns(self):
        shared = self.input()['shared']
        cols = self.input()['meta']
        return OrderedDict([
            ('pointid', cols['pointlm_id']),
            ('fullname', shared['fullname']),
            ('mtfcc', shared['mtfcc']),
            ('geom', cols['pointlm_geom']),
        ])

    def populate(self):
        session = current_session()
        session.execute('''
            INSERT INTO {output}
            SELECT pointid, fullname, mtfcc, geom
            FROM {input}'''.format(output=self.output().table,
                                   input=self.input()['data'].table))

class PriSecRoadsColumns(ColumnsTask):
    '''
    Primary & secondary roads column definitions
    '''

    def version(self):
        return 5

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'source': SourceTags(),
            'license': LicenseTags(),
        }

    def columns(self):
        input_ = self.input()
        geom = OBSColumn(
            id='prisecroads_geom',
            type='Geometry(LineString)',
            weight=5,
            tags=[input_['sections']['united_states'],
                  input_['subsections']['roads'],
                  input_['source']['tiger-source'],
                  input_['license']['no-restrictions']]
        )
        cols = OrderedDict([
            ('prisecroads_id', OBSColumn(
                type='Text',
                weight=0,
                targets={geom: GEOM_REF}
            )),
            ('rttyp', OBSColumn(
                type='Text'
            )),
            ('prisecroads_geom', geom)
        ])
        return cols


class PriSecRoads(TableTask):
    '''
    Primary & Secondary roads from the census
    '''

    year = Parameter()

    def requires(self):
        return {
            'data': TigerGeographyShapefileToSQL(year=self.year,
                                                 geography='PRISECROADS'),
            'meta': PriSecRoadsColumns(),
            'shared': SharedTigerColumns()
        }

    def version(self):
        return 2

    def timespan(self):
        return self.year

    def columns(self):
        shared = self.input()['shared']
        cols = self.input()['meta']
        return OrderedDict([
            ('linearid', cols['prisecroads_id']),
            ('fullname', shared['fullname']),
            ('rttyp', cols['rttyp']),
            ('mtfcc', shared['mtfcc']),
            ('geom', cols['prisecroads_geom']),
        ])

    def populate(self):
        session = current_session()
        session.execute('''
            INSERT INTO {output}
            SELECT linearid, fullname, rttyp, mtfcc, geom
            FROM {input}'''.format(output=self.output().table,
                                   input=self.input()['data'].table))


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
