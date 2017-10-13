#!/usr/bin/env python

'''
Tiger
'''

import json
import os
import subprocess
from collections import OrderedDict
from tasks.util import (LoadPostgresFromURL, classpath, TempTableTask, grouper,
                        shell, TableTask, ColumnsTask, TagsTask,
                        Carto2TempTableTask)
from tasks.meta import (OBSColumn, GEOM_REF, GEOM_NAME, OBSTag, current_session)
from tasks.tags import SectionTags, SubsectionTags, LicenseTags, BoundaryTags

from luigi import (Task, WrapperTask, Parameter, LocalTarget, IntParameter)
from decimal import Decimal


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

    def version(self):
        return 15

    def requires(self):
        return {
            'geom_columns': GeomColumns(),
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

        for colname, coltarget in input_['geom_columns'].iteritems():
            col = coltarget.get(session)

            level = SUMLEVELS[colname]
            additional_tags = []
            if level['cartographic']:
                additional_tags.append(boundary_type['cartographic_boundary'])
            if level['interpolated']:
                additional_tags.append(boundary_type['interpolation_boundary'])

            cols[colname + '_clipped'] = OBSColumn(
                type='Geometry',
                name='Shoreline clipped ' + col.name,
                weight=Decimal(col.weight) + Decimal(0.01),
                description='A cartography-ready version of {name}'.format(
                    name=col.name),
                targets={col: 'cartography'},
                tags=[sections['united_states'],
                      subsections['boundary'],
                      source, license] + additional_tags
            )

        return cols


class GeomColumns(ColumnsTask):

    def version(self):
        return 17

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
        for level in SUMLEVELS.values():
            columns[level['slug']] = OBSColumn(
                type='Geometry',
                name=level['name'],
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

    def version(self):
        return 6

    def requires(self):
        return {
            'raw': GeomColumns(),
            'clipped': ClippedGeomColumns()
        }

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()
        clipped = input_['clipped']
        for colname, coltarget in input_['raw'].iteritems():
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


class GeonameColumns(ColumnsTask):

    def version(self):
        return 2

    def requires(self):
        return {
            'raw': GeomColumns(),
            'clipped': ClippedGeomColumns(),
            'subsections': SubsectionTags(),
            'sections': SectionTags(),
        }

    def columns(self):
        cols = OrderedDict()
        clipped = self.input()['clipped']
        subsection = self.input()['subsections']
        sections = self.input()['sections']
        for colname, coltarget in self.input()['raw'].iteritems():
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

    year = Parameter()
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
        input_ = self.input()
        session = current_session()
        session.execute('CREATE TABLE {output} AS '
                        'SELECT ST_Subdivide(geom) geom, false in_landmask, '
                        '       aland, awater, mtfcc '
                        'FROM {input} '
                        "WHERE mtfcc != 'H2030' OR awater > 300000".format(
                            input=input_['data'].table,
                            output=self.output().table
                        ))
        session.execute('CREATE INDEX ON {output} USING GIST (geom)'.format(
            output=self.output().table
        ))

        session.execute('UPDATE {output} data SET in_landmask = True '
                        'FROM {landmask} landmask '
                        'WHERE ST_WITHIN(data.geom, landmask.the_geom)'.format(
                            landmask=input_['us_landmask'].table,
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
        input_ = self.input()
        stmt = ('CREATE TABLE {output} AS '
                'SELECT id, geoid, ST_Union(ST_MakeValid(neg.geom)) neg_geom, '
                '       MAX(pos.the_geom) pos_geom '
                'FROM {pos} pos, {neg} neg '
                'WHERE ST_Intersects(pos.the_geom, neg.geom) '
                '      AND pos.awater > 0 '
                '      {mask_clause} '
                'GROUP BY id '.format(
                    neg=input_['neg'].table,
                    mask_clause=' AND in_landmask = false' if self.use_mask() else '',
                    pos=input_['pos'].table,
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
        input_ = self.input()
        session.execute('CREATE TABLE {output} '
                        'AS SELECT geoid::text, id::int, the_geom::geometry, '
                        'aland::numeric, awater::Numeric '
                        'FROM {split} LIMIT 0 '.format(
                            output=self.output().table,
                            split=input_['split'].table))
        session.execute('INSERT INTO {output} (geoid, id, the_geom) '
                        'SELECT geoid, id, the_geom FROM {diffed} '
                        'WHERE ST_Area(ST_Transform(the_geom, 3857)) > 5000'
                        '  AND ST_NPoints(the_geom) > 10 '.format(
                            output=self.output().table,
                            diffed=input_['diffed'].table))
        session.execute('INSERT INTO {output} '
                        'SELECT geoid, id, the_geom, aland, awater FROM {split} '
                        'WHERE id NOT IN (SELECT id from {diffed})'.format(
                            split=input_['split'].table,
                            diffed=input_['diffed'].table,
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
        return 8

    def requires(self):
        return {
            'data': UnionTigerWaterGeoms(year=self.year, geography=self.geography),
            'geoms': ClippedGeomColumns(),
            'geoids': GeoidColumns(),
            'attributes': Attributes(),
            'geonames': GeonameColumns()
        }

    def columns(self):
        input_ = self.input()
        return OrderedDict([
            ('geoid', input_['geoids'][self.geography + '_geoid']),
            ('the_geom', input_['geoms'][self.geography + '_clipped']),
            ('aland', input_['attributes']['aland'])
        ])

    def timespan(self):
        return self.year

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
    year = Parameter()

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
        return SUMLEVELS[self.geography]['table']

    def version(self):
        return 12

    def requires(self):
        tiger = DownloadTiger(year=self.year)
        return {
            'data': tiger,
            'attributes': Attributes(),
            'geoids': GeoidColumns(),
            'geoms': GeomColumns(),
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'geonames': GeonameColumns(),
        }

    def columns(self):
        input_ = self.input()
        return OrderedDict([
            ('geoid', input_['geoids'][self.geography + '_geoid']),
            ('the_geom', input_['geoms'][self.geography]),
            ('aland', input_['attributes']['aland']),
            ('awater', input_['attributes']['awater']),
        ])

    def timespan(self):
        return self.year

    def populate(self):
        session = current_session()
        from_clause = '{inputschema}.{input_tablename}'.format(
            inputschema='tiger' + str(self.year),
            input_tablename=self.input_tablename,
        )
        in_colnames = [self.geoid, 'geom', self.aland, self.awater]

        out_colnames = self.columns().keys()
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
    year = Parameter()

    def version(self):
        return 1

    def requires(self):
        tiger = DownloadTiger(year=self.year)
        return {
            'data': tiger,
            'geoids': GeoidColumns(),
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'geonames': GeonameColumns(),
        }

    def columns(self):
        input_ = self.input()
        return OrderedDict([
            ('geoid', input_['geoids'][self.geography + '_geoid']),
            ('geoname', input_['geonames'][self.geography + '_geoname'])
        ])

    def timespan(self):
        return self.year

    def populate(self):

        session = current_session()
        from_clause = '{inputschema}.{input_tablename}'.format(
            inputschema='tiger' + str(self.year),
            input_tablename=SUMLEVELS[self.geography]['table'],
        )

        field_names = SUMLEVELS[self.geography]['fields']
        assert field_names['name'], "Geonames are not available for {geog} geographies".format(geog=self.geography)
        in_colnames = [field_names['geoid'], field_names['name']]

        session.execute('INSERT INTO {output} (geoid, geoname) '
                        'SELECT {in_colnames} '
                        'FROM {from_clause} '.format(
                            output=self.output().table,
                            in_colnames=', '.join(in_colnames),
                            from_clause=from_clause
                        ))


class AllSumLevels(WrapperTask):
    '''
    Compute all sumlevels
    '''

    year = Parameter()

    def requires(self):
        for geo, config in SUMLEVELS.items():
            yield SumLevel(year=self.year, geography=geo)
            yield ShorelineClip(year=self.year, geography=geo)
            if config['fields']['name']:
                yield GeoNamesTable(year=self.year, geography=geo)


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
        input_ = self.input()
        shared = input_['shared']
        cols = input_['meta']
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
        return 2

    def timespan(self):
        return self.year

    def columns(self):
        input_ = self.input()
        shared = input_['shared']
        cols = input_['meta']
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
        return json.load(fhandle)


SUMLEVELS = load_sumlevels()
