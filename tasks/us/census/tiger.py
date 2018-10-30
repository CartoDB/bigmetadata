#!/usr/bin/env python

'''
Tiger
'''

import json
import os
import subprocess
import time
import re
from collections import OrderedDict
from lib.timespan import get_timespan
from lib.logger import get_logger
from tasks.base_tasks import (ColumnsTask, TempTableTask, TableTask, TagsTask, Carto2TempTableTask,
                              SimplifiedTempTableTask, RepoFile, LoadPostgresFromZipFile)
from tasks.util import classpath, grouper, shell
from tasks.meta import OBSTable, OBSColumn, GEOM_REF, GEOM_NAME, OBSTag, current_session
from tasks.tags import SectionTags, SubsectionTags, LicenseTags, BoundaryTags
from tasks.targets import PostgresTarget
from tasks.simplification import SIMPLIFIED_SUFFIX
from tasks.simplify import Simplify
from lib.logger import get_logger

from luigi import (Task, WrapperTask, Parameter, LocalTarget, IntParameter)
from decimal import Decimal

LOGGER = get_logger(__name__)

GEOID_SUMLEVEL_COLUMN = "_geoidsl"
GEOID_SHORELINECLIPPED_COLUMN = "_geoidsc"
BLOCK = 'block'


class TigerSourceTags(TagsTask):
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
    year = IntParameter()

    def version(self):
        return 16

    def requires(self):
        return {
            'geom_columns': GeomColumns(year=self.year),
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'source': TigerSourceTags(),
            'license': LicenseTags(),
            'boundary': BoundaryTags(),
        }

    def columns(self):
        cols = OrderedDict()
        session = current_session()
        input_ = self.input()
        sections = input_['sections']
        subsections = input_['subsections']
        source = input_['source']['tiger-source']
        license = input_['license']['no-restrictions']
        boundary_type = input_['boundary']

        for colname, coltarget in self.input()['geom_columns'].items():
            col = coltarget.get(session)

            level = SUMLEVELS['_'.join(colname.split('_')[:-1])]
            additional_tags = []
            if level['cartographic']:
                additional_tags.append(boundary_type['cartographic_boundary'])
            if level['interpolated']:
                additional_tags.append(boundary_type['interpolation_boundary'])

            try:
                cols[colname + '_clipped'] = OBSColumn(
                    type='Geometry',
                    name='Shoreline clipped ' + '_{}'.format(self.year) + col.name,
                    weight=Decimal(col.weight) + Decimal(0.01),
                    description='A cartography-ready version of {name}'.format(
                        name=col.name),
                    targets={col: 'cartography'},
                    tags=[sections['united_states'],
                          subsections['boundary'],
                          source, license] + additional_tags
                )
            except Exception as e:
                LOGGER.error(
                    "Error loading column {}, {}, {}".format(colname, col, coltarget._id))
                raise e


        return cols


class GeomColumns(ColumnsTask):
    year = IntParameter()

    def version(self):
        return 18

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'source': TigerSourceTags(),
            'license': LicenseTags(),
            'boundary': BoundaryTags(),
        }

    def _generate_desc(self, sumlevel):
        '''
        Add figure to the description
        '''
        return SUMLEVELS[sumlevel]['census_description']

    def columns(self):
        input_ = self.input()
        sections = input_['sections']
        subsections = input_['subsections']
        source = input_['source']['tiger-source']
        license = input_['license']['no-restrictions']

        columns = {}
        for level in list(SUMLEVELS.values()):
            columns[level['slug'] + '_{}'.format(self.year)] = OBSColumn(
                type='Geometry',
                name=level['name'] + '_{}'.format(self.year),
                description=level['census_description'],
                weight=level['weight'],
                tags=[sections['united_states'], subsections['boundary'], source, license]
            )

        return columns


class Attributes(ColumnsTask):

    def version(self):
        return 2

    def requires(self):
        return SectionTags()

    def columns(self):
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
        ])


class GeoidColumns(ColumnsTask):
    '''
    Used for external dependencies on Tiger.

    This creates two different geoid columns
    (GEOID_SUMLEVEL_COLUMN for the SumLevels geometries and
    GEOID_SHORELINECLIPPED_COLUMN for the ShorelineClipped geometries).

    This allows external tables to depend on both shoreline clipped and non-shoreline clipped geometries.
    '''

    year = IntParameter()

    def version(self):
        return 8

    def requires(self):
        return {
            'raw': GeomColumns(year=self.year),
            'clipped': ClippedGeomColumns(year=self.year)
        }

    def columns(self):
        cols = OrderedDict()
        clipped = self.input()['clipped']
        for colname, coltarget in self.input()['raw'].items():
            col = coltarget._column
            cols[colname + GEOID_SHORELINECLIPPED_COLUMN] = OBSColumn(
                type='Text',
                name=col.name + ' Geoids',
                weight=0,
                targets={
                    clipped[colname + '_clipped']._column: GEOM_REF
                }
            )
            cols[colname + GEOID_SUMLEVEL_COLUMN] = OBSColumn(
                type='Text',
                name=col.name + ' Geoids',
                weight=0,
                targets={
                    col: GEOM_REF,
                }
            )

        return cols


class GeonameColumns(ColumnsTask):

    year = IntParameter()

    def version(self):
        return 3

    def requires(self):
        return {
            'raw': GeomColumns(year=self.year),
            'clipped': ClippedGeomColumns(year=self.year),
            'subsections': SubsectionTags(),
            'sections': SectionTags(),
        }

    def columns(self):
        cols = OrderedDict()
        clipped = self.input()['clipped']
        subsection = self.input()['subsections']
        sections = self.input()['sections']
        for colname, coltarget in self.input()['raw'].items():
            col = coltarget._column
            cols[colname + '_geoname'] = OBSColumn(
                type='Text',
                name=col.name + ' Proper Name',
                weight=1,
                tags=[subsection['names'], sections['united_states']],
                targets={
                    col: GEOM_NAME,
                    clipped[colname + '_clipped']._column: GEOM_NAME
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
        except subprocess.CalledProcessError:
            return False


class UnzipTigerGeography(Task):
    '''
    Unzip tiger geography
    '''

    year = IntParameter()
    geography = Parameter()

    def requires(self):
        return DownloadTigerGeography(year=self.year, geography=self.geography)

    @property
    def directory(self):
        return os.path.join('tmp', classpath(self), str(self.year), self.geography)

    def run(self):
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

    year = IntParameter()
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
            print('imported {} shapefiles'.format((i + 1) * 500))

        session = current_session()
        # Spatial index
        session.execute('ALTER TABLE {qualified_table} RENAME COLUMN '
                        'wkb_geometry TO geom'.format(
                            qualified_table=self.output().table))
        session.execute('CREATE INDEX ON {qualified_table} USING GIST (geom)'.format(
            qualified_table=self.output().table))


class DownloadTiger(LoadPostgresFromZipFile):
    url_template = 'https://s3.amazonaws.com/census-backup/tiger/{year}/tiger{year}_backup.sql.gz'
    year = IntParameter()

    def version(self):
        return 1

    def requires(self):
        return RepoFile(resource_id=self.task_id,
                        version=self.version(),
                        url=self.url_template.format(year=self.year))

    def run(self):
        schema = 'tiger{year}'.format(year=self.year)
        shell("psql -c 'DROP SCHEMA IF EXISTS \"{schema}\" CASCADE'".format(schema=schema))
        shell("psql -c 'CREATE SCHEMA \"{schema}\"'".format(schema=schema))
        self.load_from_zipfile(self.input().path)


class SimplifiedDownloadTiger(Task):
    year = IntParameter()
    geography = Parameter()

    def requires(self):
        return DownloadTiger(year=self.year)

    def run(self):
        yield Simplify(schema='tiger{year}'.format(year=self.year),
                       table=SUMLEVELS[self.geography]['table'],
                       table_id='.'.join(['tiger{year}'.format(year=self.year), self.geography]))

    def output(self):
        return PostgresTarget('tiger{year}'.format(year=self.year),
                              SUMLEVELS[self.geography]['table'] + SIMPLIFIED_SUFFIX)


class SimplifyByState(Task):
    year = IntParameter()
    geography = Parameter()

    def run(self):
        session = current_session()
        for _, table in self.input().items():
            query = '''
                    CREATE TABLE IF NOT EXISTS {output} AS
                    SELECT *
                    FROM "{schema_input}".{table_input}
                    WHERE 1=0
                    '''.format(schema_input=table.schema,
                               table_input=table.tablename,
                               output=self.output().table)
            session.execute(query)

            query = '''
                    INSERT INTO {output}
                    SELECT *
                    FROM "{schema_input}".{table_input}
                    '''.format(schema_input=table.schema,
                               table_input=table.tablename,
                               output=self.output().table)
            session.execute(query)
            session.commit()


class SplitByState(Task):
    year = IntParameter()
    geography = Parameter()
    state = Parameter()

    def requires(self):
        return DownloadTiger(year=self.year)

    def run(self):
        session = current_session()
        sql_index = '''
                    CREATE INDEX IF NOT EXISTS {table_input}_statefp10_idx
                    ON "{schema_input}".{table_input} (statefp10)
                    '''
        session.execute(sql_index.format(
            schema_input='tiger{year}'.format(year=self.year),
            table_input=SUMLEVELS[self.geography]['table']))

        query = '''
                CREATE TABLE {table_output} AS
                SELECT *
                FROM "{schema_input}".{table_input}
                WHERE statefp10 = '{state}'
                '''.format(schema_input='tiger{year}'.format(year=self.year),
                           table_input=SUMLEVELS[self.geography]['table'],
                           table_output=self.output().table,
                           state=self.state)
        session.execute(query)
        session.commit()

    def output(self):
        return PostgresTarget('tiger{year}'.format(year=self.year),
                              '{name}_state{state}'.format(name=SUMLEVELS[self.geography]['table'],
                                                           state=self.state))


class SimplifyGeoChunkByState(Task):
    year = IntParameter()
    geography = Parameter()
    state = Parameter()

    def requires(self):
        return SplitByState(year=self.year, geography=self.geography, state=self.state)

    def run(self):
        yield Simplify(schema=self.input().schema,
                       table=self.input().tablename,
                       table_id='.'.join(['tiger{year}'.format(year=self.year),
                                          '{geo}_by_state'.format(geo=self.geography)]))

    def output(self):
        return PostgresTarget('tiger{year}'.format(year=self.year),
                              '{name}_state{state}{suffix}'.format(name=SUMLEVELS[self.geography]['table'],
                                                                   state=self.state,
                                                                   suffix=SIMPLIFIED_SUFFIX))


class SimplifyGeoByState(SimplifyByState):

    def requires(self):
        simplifications = {}
        for state_code, _ in STATES.items():
            simplifications[state_code] = SimplifyGeoChunkByState(year=self.year,
                                                                  geography=self.geography,
                                                                  state=state_code)
        return simplifications

    def output(self):
        return PostgresTarget('tiger{year}'.format(year=self.year),
                              SUMLEVELS[self.geography]['table'] + SIMPLIFIED_SUFFIX)


class SimpleShoreline(TempTableTask):

    year = IntParameter()

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

    year = IntParameter()
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

    year = IntParameter()
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

    year = IntParameter()
    geography = Parameter()

    def requires(self):
        return JoinTigerWaterGeoms(year=self.year, geography=self.geography)

    def run(self):
        session = current_session()
        stmt = ('CREATE TABLE {output} '
                'AS SELECT geoid, id, ST_Difference( '
                'ST_MakeValid(pos_geom), ST_MakeValid(neg_geom)) the_geom '
                'FROM {input}'.format(
                    output=self.output().table,
                    input=self.input().table), )[0]
        session.execute(stmt)


class PreunionTigerWaterGeoms(TempTableTask):
    '''
    Create new table with both diffed and non-diffed (didn't intersect with
    water) geoms
    '''

    year = IntParameter()
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

    year = IntParameter()
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


class SimplifiedUnionTigerWaterGeoms(SimplifiedTempTableTask):
    year = IntParameter()
    geography = Parameter()

    def requires(self):
        return UnionTigerWaterGeoms(year=self.year, geography=self.geography)


class SplitUnionTigerWaterGeomsByState(Task):
    year = IntParameter()
    geography = Parameter()
    state = Parameter()

    def requires(self):
        return UnionTigerWaterGeoms(year=self.year, geography=self.geography)

    def run(self):
        session = current_session()
        query = '''
                CREATE TABLE {table_output} AS
                SELECT *
                FROM "{schema_input}".{table_input}
                WHERE geoid like '{state}%'
                '''.format(schema_input=self.input().schema,
                           table_input=self.input().tablename,
                           table_output=self.output().table,
                           state=self.state)
        session.execute(query)
        session.commit()

    def output(self):
        return PostgresTarget('us.census.tiger',
                              'UnionTigerWaterGeoms_{name}_state{state}'.format(name=SUMLEVELS[self.geography]['table'],
                                                                                state=self.state))


class SimplifyUnionTigerWaterGeomsChunkByState(Task):
    year = IntParameter()
    geography = Parameter()
    state = Parameter()

    def requires(self):
        return SplitUnionTigerWaterGeomsByState(year=self.year, geography=self.geography, state=self.state)

    def run(self):
        yield Simplify(schema=self.input().schema,
                       table=self.input().tablename,
                       table_id='.'.join(['us.census.tiger',
                                          'UnionTigerWaterGeoms_{geo}_by_state_{year}'.format(geo=self.geography,
                                                                                              year=self.year)]))

    def output(self):
        return PostgresTarget('us.census.tiger',
                              'UnionTigerWaterGeoms_{name}_state{state}{suffix}'.format(name=SUMLEVELS[self.geography]['table'],
                                                                                        state=self.state,
                                                                                        suffix=SIMPLIFIED_SUFFIX))


class SimplifiedUnionTigerWaterGeomsByState(SimplifyByState):
    def requires(self):
        simplifications = {}
        for state_code, _ in STATES.items():
            simplifications[state_code] = SimplifyUnionTigerWaterGeomsChunkByState(year=self.year,
                                                                                   geography=self.geography,
                                                                                   state=state_code)
        return simplifications

    def output(self):
        return PostgresTarget('us.census.tiger',
                              'UnionTigerWaterGeom_' + SUMLEVELS[self.geography]['table'] + SIMPLIFIED_SUFFIX)


class ShorelineClip(TableTask):
    '''
    Clip the provided geography to shoreline.
    '''

    # MTFCC meanings:
    # http://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp2009/TGRSHP09AF.pdf

    year = IntParameter()
    geography = Parameter()

    def version(self):
        return 10

    def requires(self):
        if self.geography == BLOCK:
            tiger = SimplifiedUnionTigerWaterGeomsByState(year=self.year, geography=self.geography)
        else:
            tiger = SimplifiedUnionTigerWaterGeoms(year=self.year, geography=self.geography)
        return {
            'data': tiger,
            'geoms': ClippedGeomColumns(year=self.year),
            'geoids': GeoidColumns(year=self.year),
            'attributes': Attributes(),
            'geonames': GeonameColumns(year=self.year),
        }

    def columns(self):
        return OrderedDict([
            ('geoid', self.input()['geoids'][self.geography + '_{}'.format(self.year) + GEOID_SHORELINECLIPPED_COLUMN]),
            ('the_geom', self.input()['geoms'][self.geography + '_{}'.format(self.year) + '_clipped']),
            ('aland', self.input()['attributes']['aland'])
        ])

    def table_timespan(self):
        return get_timespan(str(self.year))

    # TODO: https://github.com/CartoDB/bigmetadata/issues/435
    def targets(self):
        return {
            OBSTable(id='.'.join([self.schema(), self.name()])): GEOM_REF,
        }

    def populate(self):
        session = current_session()

        stmt = ('''INSERT INTO {output}
                   SELECT
                     geoid,
                     ST_Union(ARRAY(
                       SELECT ST_MakePolygon(ST_ExteriorRing(
                         (ST_Dump(ST_CollectionExtract(the_geom, 3))).geom
                       ))
                     )),
                     aland
                   FROM {input}'''.format(
                    output=self.output().table,
                    input=self.input()['data'].table), )[0]
        session.execute(stmt)


class SumLevel(TableTask):

    geography = Parameter()
    year = IntParameter()

    @property
    def geoid(self):
        return SUMLEVELS[self.geography]['fields']['geoid']

    @property
    def aland(self):
        return SUMLEVELS[self.geography]['fields']['aland']

    @property
    def awater(self):
        return SUMLEVELS[self.geography]['fields']['awater']

    @property
    def input_tablename(self):
        return SUMLEVELS[self.geography]['table'] + SIMPLIFIED_SUFFIX

    def version(self):
        return 15

    def requires(self):
        if self.geography == BLOCK:
            tiger = SimplifyGeoByState(geography=self.geography, year=self.year)
        else:
            tiger = SimplifiedDownloadTiger(geography=self.geography, year=self.year)
        return {
            'attributes': Attributes(),
            'geoids': GeoidColumns(year=self.year),
            'geoms': GeomColumns(year=self.year),
            'data': tiger,
        }

    def columns(self):
        input_ = self.input()
        return OrderedDict([
            ('geoid', input_['geoids'][self.geography + '_{}'.format(self.year) + GEOID_SUMLEVEL_COLUMN]),
            ('the_geom', input_['geoms'][self.geography + '_{}'.format(self.year)]),
            ('aland', input_['attributes']['aland']),
            ('awater', input_['attributes']['awater']),
        ])

    def table_timespan(self):
        return get_timespan(str(self.year))

    # TODO: https://github.com/CartoDB/bigmetadata/issues/435
    def targets(self):
        return {
            OBSTable(id='.'.join([self.schema(), self.name()])): GEOM_REF,
        }

    def populate(self):
        session = current_session()
        from_clause = '{inputschema}.{input_tablename}'.format(
            inputschema='tiger' + str(self.year),
            input_tablename=self.input_tablename,
        )
        in_colnames = [self.geoid, 'geom', self.aland, self.awater]

        out_colnames = list(self.columns().keys())
        session.execute('INSERT INTO {output} ({out_colnames}) '
                        'SELECT {in_colnames} '
                        'FROM {from_clause} '.format(
                            output=self.output().table,
                            in_colnames=', '.join(in_colnames),
                            out_colnames=', '.join(out_colnames),
                            from_clause=from_clause
                        ))


class GeoNamesTable(TableTask):

    geography = Parameter()
    year = IntParameter()

    def version(self):
        return 5

    def requires(self):
        if self.geography == BLOCK:
            tiger = SimplifyGeoByState(geography=self.geography, year=self.year)
        else:
            tiger = SimplifiedDownloadTiger(geography=self.geography, year=self.year)
        return {
            'data': tiger,
            'geoids': GeoidColumns(year=self.year),
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'geonames': GeonameColumns(year=self.year),
            'shoreline': ShorelineClip(year=self.year, geography=self.geography),
            'sumlevel': SumLevel(year=self.year, geography=self.geography),
        }

    def targets(self):
        return {self.input()['shoreline'].obs_table: GEOM_REF,
                self.input()['sumlevel'].obs_table: GEOM_REF}

    def columns(self):
        return OrderedDict([
            ('geoidsl', self.input()['geoids'][self.geography + '_{}'.format(self.year) + GEOID_SUMLEVEL_COLUMN]),
            ('geoidsc', self.input()['geoids'][self.geography + '_{}'.format(self.year) + GEOID_SHORELINECLIPPED_COLUMN]),
            ('geoname', self.input()['geonames'][self.geography + '_{}'.format(self.year) + '_geoname'])
        ])

    def table_timespan(self):
        return get_timespan(str(self.year))

    def populate(self):

        session = current_session()
        from_clause = '{inputschema}.{input_tablename}'.format(
            inputschema='tiger' + str(self.year),
            input_tablename=SUMLEVELS[self.geography]['table'] + SIMPLIFIED_SUFFIX,
        )

        field_names = SUMLEVELS[self.geography]['fields']
        assert field_names['name'], "Geonames are not available for {geog} geographies".format(geog=self.geography)
        in_colnames = [field_names['geoid'], field_names['geoid'], field_names['name']]

        session.execute('INSERT INTO {output} (geoidsl, geoidsc, geoname) '
                        'SELECT {in_colnames} '
                        'FROM {from_clause} '.format(
                            output=self.output().table,
                            in_colnames=', '.join(in_colnames),
                            from_clause=from_clause
                        ))


class SumLevel4Geo(WrapperTask):
    '''
    Compute the sumlevel for a given geography
    '''

    year = Parameter()
    geography = Parameter()

    def requires(self):
        config = dict(SUMLEVELS.items()).get(self.geography)
        if config['fields']['name']:
            yield GeoNamesTable(year=self.year, geography=self.geography)
        yield SumLevel(year=self.year, geography=self.geography)
        yield ShorelineClip(year=self.year, geography=self.geography)


class SharedTigerColumns(ColumnsTask):

    def version(self):
        return 2

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'source': TigerSourceTags(),
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
            'source': TigerSourceTags(),
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
        return 4

    def requires(self):
        return {
            'data': TigerGeographyShapefileToSQL(year=self.year,
                                                 geography='POINTLM'),
            'meta': PointLandmarkColumns(),
            'shared': SharedTigerColumns()
        }

    def table_timespan(self):
        return get_timespan(str(self.year))

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
            'source': TigerSourceTags(),
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
        return 4

    def table_timespan(self):
        return get_timespan(str(self.year))

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


class TigerBlocksInterpolation(Task):
    '''
    Task used to create a table with the block and blockgroups geoid and the
    percentage of the block in the block group
    '''
    year = Parameter()

    def requires(self):
        return {
            'shoreline_block': ShorelineClip(year=self.year, geography='block'),
            'shoreline_blockgroup': ShorelineClip(year=self.year, geography='block_group'),
        }

    def run(self):
        session = current_session()
        with session.no_autoflush:
            tiger_tables = {}
            tiger_tables_query = '''SELECT id,tablename
                                    FROM observatory.obs_table
                                    WHERE id ilike 'us.census.tiger.shoreline_clip_block%'
                                 '''

            tiger_tables_result = session.execute(tiger_tables_query)
            if tiger_tables_result:
                for tiger_table in tiger_tables_result.fetchall():
                    if re.search('block_group_{}'.format(self.year), tiger_table['id']):
                        tiger_tables['block_group'] = tiger_table['tablename']
                    elif re.search('block_{}'.format(self.year), tiger_table['id']):
                        tiger_tables['block'] = tiger_table['tablename']

                # Create the table with block/blockgroups and percentage field empty
                start_time = time.time()
                LOGGER.info("Start creating the interpolation table...")
                query = '''
                        CREATE TABLE {table_output} AS
                        SELECT geoid blockid, left(geoid,12) blockgroupid, 0::float percentage, the_geom block_geom
                        FROM "{schema_input}".{block_table} b
                        '''.format(schema_input='observatory',
                                   block_table=tiger_tables['block'],
                                   table_output=self.output().table)
                session.execute(query)
                end_time = time.time()
                LOGGER.info("Time creating the table {}".format((end_time - start_time)))
                # Creating indexes
                LOGGER.info("Start creating the indexes for the interpolation table...")
                start_time = time.time()
                indexes_query = '''
                    CREATE INDEX blocks_idx ON {table_output} (blockid);
                    CREATE INDEX block_groups_idx ON {table_output} (blockgroupid);
                '''.format(table_output=self.output().table)
                session.execute(indexes_query)
                end_time = time.time()
                LOGGER.info("Indexes created in {}".format((end_time - start_time)))
                # Set the interpolation percentages in the table
                LOGGER.info("Start updating the table...")
                start_time = time.time()
                update_percentage_query = '''
                        UPDATE {table_output} b
                        SET percentage = (
                            SELECT (ST_Area(b.block_geom)/ST_Area(bg.the_geom))::float*100.00
                            FROM "{schema_input}".{bg_table} bg
                            WHERE b.blockgroupid = bg.geoid
                        )
                        '''.format(schema_input='observatory',
                                   bg_table=tiger_tables['block_group'],
                                   table_output=self.output().table)
                session.execute(update_percentage_query)
                session.commit()
                end_time = time.time()
                LOGGER.info("Time creating the table {}".format((end_time - start_time)))
            else:
                LOGGER.error('Cant retrieve tiger tables for block and block group')

    def output(self):
        schema = 'tiger{year}'.format(year=self.year)
        return PostgresTarget(schema, 'blocks_interpolation')

def load_sumlevels():
    '''
    Load summary levels from JSON. Returns a dict by sumlevel number.
    '''
    with open(os.path.join(os.path.dirname(__file__), 'summary_levels.json')) as fhandle:
        return json.load(fhandle)


def load_states():
    '''
    Load states from JSON. Returns a dict by state number.
    '''
    with open(os.path.join(os.path.dirname(__file__), 'states.json')) as fhandle:
        return json.load(fhandle)


SUMLEVELS = load_sumlevels()
STATES = load_states()
