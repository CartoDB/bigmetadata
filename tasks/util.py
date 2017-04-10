'''
Util functions for luigi bigmetadata tasks.
'''

from collections import OrderedDict

import json
import os
import subprocess
import logging
import sys
import time
import re
import importlib
import inspect

from hashlib import sha1
from itertools import izip_longest
from datetime import date
from urllib import quote_plus

from slugify import slugify
import requests

from luigi import (Task, Parameter, LocalTarget, Target, BooleanParameter,
                   ListParameter, DateParameter, WrapperTask, Event)
from luigi.s3 import S3Target

from sqlalchemy import Table, types, Column
from sqlalchemy.dialects.postgresql import JSON

from tasks.meta import (OBSColumn, OBSTable, metadata, Geometry, Point,
                        Linestring, OBSColumnTable, OBSTag, current_session,
                        session_commit, session_rollback, OBSColumnTag)


def get_logger(name):
    '''
    Obtain a logger outputing to stderr with specified name. Defaults to INFO
    log level.
    '''
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(message)s'))
    logger.addHandler(handler)
    return logger

LOGGER = get_logger(__name__)


def shell(cmd):
    '''
    Run a shell command, uses :py:func:`subprocess.check_output(cmd,
    shell=True)` under the hood.

    Returns the ``STDOUT`` output, and raises an error if there is a
    none-zero exit code.
    '''
    try:
        return subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as err:
        LOGGER.error(err.output)
        raise


def underscore_slugify(txt):
    '''
    Given a string, converts from camelcase to underscore style, and then
    slugifies.

    >>> underscore_slugify('FooBarBaz')
    'foo_bar_baz'

    >>> underscore_slugify('Foo, bar, baz')
    'foo_bar_baz'

    >>> underscore_slugify('Foo_Bar_Baz')
    'foo_bar_baz'

    >>> underscore_slugify('Foo:barBaz')
    'foo_bar_baz'
    '''
    return slugify(camel_to_underscore(re.sub(
        r'[^a-zA-Z0-9]+', '_', txt))).replace('-', '_')


def classpath(obj):
    '''
    Returns the path to this object's class, relevant to current working dir.
    Excludes the first element of the path.  If there is only one element,
    returns ``tmp``.

    >>> classpath(object)
    'tmp'
    >>> from tasks.util import ColumnsTask
    >>> classpath(ColumnsTask())
    'util'
    >>> from tasks.es.ine import FiveYearPopulation
    >>> classpath(FiveYearPopulation())
    'es.ine'
    '''
    classpath_ = '.'.join(obj.__module__.split('.')[1:])
    return classpath_ if classpath_ else 'tmp'


def query_cartodb(query, carto_url=None, api_key=None):
    '''
    Convenience function to query CARTO's SQL API with an arbitrary SQL string.
    The account connected via ``.env`` is queried.

    Returns the raw ``Response`` object.  Will not raise an exception in case
    of non-200 response.

    :param query: The query to execute on CARTO.
    '''
    #carto_url = 'https://{}/api/v2/sql'.format(os.environ['CARTODB_DOMAIN'])
    carto_url = (carto_url or os.environ['CARTODB_URL']) + '/api/v2/sql'
    resp = requests.post(carto_url, data={
        'api_key': api_key or os.environ['CARTODB_API_KEY'],
        'q': query
    })
    #assert resp.status_code == 200
    #if resp.status_code != 200:
    #    import pdb
    #    pdb.set_trace()
    #    raise Exception(u'Non-200 response ({}) from carto: {}'.format(
    #        resp.status_code, resp.text))
    return resp


def upload_via_ogr2ogr(outname, localname, schema, api_key=None):
    api_key = api_key or os.environ['CARTODB_API_KEY']
    cmd = u'''
ogr2ogr --config CARTODB_API_KEY {api_key} \
        -f CartoDB "CartoDB:observatory" \
        -overwrite \
        -nlt GEOMETRY \
        -nln "{private_outname}" \
        PG:dbname=$PGDATABASE' active_schema={schema}' '{tablename}'
    '''.format(private_outname=outname, tablename=localname,
               schema=schema, api_key=api_key)
    print cmd
    shell(cmd)


def import_api(request, json_column_names=None, api_key=None, carto_url=None):
    '''
    Run CARTO's `import API <https://carto.com/docs/carto-engine/import-api/importing-geospatial-data/>`_
    The account connected via ``.env`` will be the target.

    Although the import API is asynchronous, this function will block until
    the request is complete, and raise an exception if it fails.

    :param request: A ``dict`` that will be the body of the request to the
                    import api.
    :param json_column_names:  Optional iterable of column names that will
                               be converted to ``JSON`` type after the fact.
                               Otherwise those columns would be ``Text``.
    '''
    carto_url = carto_url or os.environ['CARTODB_URL']
    api_key = api_key or os.environ['CARTODB_API_KEY']
    json_column_names = json_column_names or []
    resp = requests.post('{url}/api/v1/imports/?api_key={api_key}'.format(
        url=carto_url,
        api_key=api_key
    ), json=request)
    assert resp.status_code == 200

    import_id = resp.json()["item_queue_id"]
    while True:
        resp = requests.get('{url}/api/v1/imports/{import_id}?api_key={api_key}'.format(
            url=carto_url,
            import_id=import_id,
            api_key=api_key
        ))
        if resp.json()['state'] == 'complete':
            break
        elif resp.json()['state'] == 'failure':
            raise Exception('Import failed: {}'.format(resp.json()))
        print resp.json()['state']
        time.sleep(1)

    # if failing below, try reloading https://observatory.cartodb.com/dashboard/datasets
    assert resp.json()['table_name'] == request['table_name'] # the copy should not have a
                                                              # mutilated name (like '_1', '_2' etc)

    for colname in json_column_names:
        query = 'ALTER TABLE {outname} ALTER COLUMN {colname} ' \
                'SET DATA TYPE json USING NULLIF({colname}, '')::json'.format(
                    outname=resp.json()['table_name'], colname=colname
                )
        print query
        resp = query_cartodb(query)
        assert resp.status_code == 200


def sql_to_cartodb_table(outname, localname, json_column_names=None,
                         schema='observatory'):
    '''
    Move a table to CARTO using the `import API <https://carto.com/docs/carto-engine/import-api/importing-geospatial-data/>`_

    :param outname: The destination name of the table.
    :param localname: The local name of the table, exclusive of schema.
    :param json_column_names:  Optional iterable of column names that will
                               be converted to ``JSON`` type after the fact.
                               Otherwise those columns would be ``Text``.
    :param schema: Optional schema for the local table.  Defaults to
                   ``observatory``.
    '''
    private_outname = outname + '_private'
    upload_via_ogr2ogr(private_outname, localname, schema)

    # populate the_geom_webmercator
    # if you try to use the import API to copy a table with the_geom populated
    # but not the_geom_webmercator, we get an error for unpopulated column
    resp = query_cartodb(
        'UPDATE {tablename} '
        'SET the_geom_webmercator = CDB_TransformToWebmercator(the_geom) '.format(
            tablename=private_outname
        )
    )
    assert resp.status_code == 200

    print 'copying via import api'
    import_api({
        'table_name': outname,
        'table_copy': private_outname,
        'create_vis': False,
        'type_guessing': False,
        'privacy': 'public'
    }, json_column_names=json_column_names)
    resp = query_cartodb('DROP TABLE "{}" CASCADE'.format(private_outname))
    assert resp.status_code == 200


def generate_tile_summary(session, table_id, column_id, tablename, colname):
    '''
    Add entries to obs_column_table_tile for the given table and column.
    '''
    tablename_ns = tablename.split('.')[-1]

    query = '''
        DELETE FROM observatory.obs_column_table_tile_simple
        WHERE table_id = '{table_id}'
          AND column_id = '{column_id}';'''.format(
              table_id=table_id, column_id=column_id)
    resp = session.execute(query)

    query = '''
        DELETE FROM observatory.obs_column_table_tile
        WHERE table_id = '{table_id}'
          AND column_id = '{column_id}';'''.format(
              table_id=table_id, column_id=column_id)
    resp = session.execute(query)

    query = '''
        DROP TABLE IF EXISTS raster_empty_{tablename_ns};
        CREATE TEMPORARY TABLE raster_empty_{tablename_ns} AS
        SELECT ROW_NUMBER() OVER () AS id, rast FROM (
          WITH tilesize AS (SELECT
            CASE WHEN SUM(ST_Area({colname})) > 5000 THEN 2.5
                 ELSE 0.5 END AS tilesize,
            ST_SetSRID(ST_Extent({colname}), 4326) extent
            FROM {tablename}
          ), summaries AS (
            SELECT ST_XMin(extent) xmin, ST_XMax(extent) xmax,
                   ST_YMin(extent) ymin, ST_YMax(extent) ymax
            FROM tilesize
          ) SELECT
              ST_Tile(ST_SetSRID(
                ST_AddBand(
                  ST_MakeEmptyRaster(
                    ((xmax - xmin) / tilesize)::Integer + 1,
                    ((ymax - ymin) / tilesize)::Integer + 1,
                    ((xmin / tilesize)::Integer)::Numeric * tilesize,
                    ((ymax / tilesize)::Integer)::Numeric * tilesize,
                    tilesize
                  ), ARRAY[
                    (1, '32BF', -1, 0)::addbandarg,
                    (2, '32BF', -1, 0)::addbandarg,
                    (3, '32BF', -1, 0)::addbandarg
                  ])
              , 4326)
          , ARRAY[1, 2, 3], 25, 25) rast
          FROM summaries, tilesize
          ) foo;
        '''.format(colname=colname,
                   tablename=tablename,
                   tablename_ns=tablename_ns)
    resp = session.execute(query)
    assert resp.rowcount > 0

    query = '''
      DROP TABLE IF EXISTS raster_pap_{tablename_ns};
      CREATE TEMPORARY TABLE raster_pap_{tablename_ns} As
      SELECT id, (ST_PixelAsPolygons(FIRST(rast), 1, False)).*
           FROM raster_empty_{tablename_ns} rast,
                {tablename} vector
           WHERE rast.rast && vector.{colname}
           GROUP BY id;
    '''.format(tablename_ns=tablename_ns, tablename=tablename, colname=colname)
    resp = session.execute(query)
    assert resp.rowcount > 0

    query = '''
      CREATE UNIQUE INDEX ON raster_pap_{tablename_ns} (id, x, y);
      CREATE INDEX ON raster_pap_{tablename_ns} using gist (geom);
    '''.format(tablename_ns=tablename_ns, colname=colname)
    resp = session.execute(query)

    # Travis doesn't support ST_ClipByBox2D because of old GEOS version, but
    # our Docker container supports this optimization
    if os.environ.get('TRAVIS'):
        st_clip = 'ST_Intersection'
    else:
        st_clip = 'ST_ClipByBox2D'

    query = '''
      DROP TABLE IF EXISTS raster_vals_{tablename_ns};
      CREATE TEMPORARY TABLE raster_vals_{tablename_ns} AS
      WITH vector AS (SELECT CASE
                        WHEN ST_GeometryType({colname}) IN ('ST_Polygon', 'ST_MultiPolygon')
                          THEN ST_CollectionExtract(ST_MakeValid(
                                 ST_SimplifyVW({colname}, 0.0005)), 3)
                          ELSE {colname}
                        END the_geom
                      FROM {tablename} vector)
      SELECT id
             , (null::geometry, null::numeric)::geomval median
             , (FIRST(geom),
               -- determine number of geoms, including fractions
               Nullif(SUM(CASE ST_GeometryType(vector.the_geom)
                 WHEN 'ST_Point' THEN 1
                 WHEN 'ST_LineString' THEN
                   ST_Length({st_clip}(vector.the_geom, ST_Envelope(geom))) /
                       ST_Length(vector.the_geom)
                 ELSE
                   CASE WHEN ST_Within(geom, vector.the_geom) THEN
                             ST_Area(geom) / ST_Area(vector.the_geom)
                        WHEN ST_Within(vector.the_geom, geom) THEN 1
                        ELSE ST_Area({st_clip}(vector.the_geom, ST_Envelope(geom))) /
                             ST_Area(vector.the_geom)
                   END
                END), 0)
              )::geomval cnt
             , (FIRST(geom),
               -- determine % pixel area filled with geoms
               SUM(CASE WHEN geom @ vector.the_geom THEN 1
                        WHEN vector.the_geom @ geom THEN
                             ST_Area(vector.the_geom) / ST_Area(geom)
                        ELSE ST_Area({st_clip}(vector.the_geom, ST_Envelope(geom))) /
                             ST_Area(geom)
               END)
              )::geomval percent_fill
         FROM raster_pap_{tablename_ns}, vector
         WHERE geom && vector.the_geom
         GROUP BY id, x, y;
    '''.format(tablename_ns=tablename_ns, tablename=tablename, colname=colname,
               st_clip=st_clip)
    resp = session.execute(query)
    assert resp.rowcount > 0

    query = '''
        INSERT INTO observatory.obs_column_table_tile
        WITH pixelspertile AS (SELECT id,
          ARRAY_AGG(median) medians,
          ARRAY_AGG(cnt) counts,
          ARRAY_AGG(percent_fill) percents
        FROM raster_vals_{tablename_ns}
        GROUP BY id)
        SELECT '{table_id}', '{column_id}', er.id,
                 ST_SetValues(ST_SetValues(ST_SetValues(er.rast,
                 1, medians),
                 2, counts),
                 3, percents) geom
          FROM raster_empty_{tablename_ns} er, pixelspertile ppt
          WHERE er.id = ppt.id
       '''.format(tablename_ns=tablename_ns, tablename=tablename,
                  table_id=table_id, column_id=column_id)
    resp = session.execute(query)
    assert resp.rowcount > 0

    resp = session.execute('''
        UPDATE observatory.obs_column_table_tile
        SET tile = st_setvalues(st_setvalues(st_setvalues(tile,
                    1, geomvals, false),
                    2, geomvals, false),
                    3, geomvals, false)
        FROM (
            SELECT table_id, column_id, tile_id, array_agg(((geomval).geom, 0)::geomval) geomvals FROM (
                SELECT table_id, column_id, tile_id, st_dumpaspolygons(tile, 2, false) geomval
                FROM observatory.obs_column_table_tile
                WHERE column_id = '{column_id}'
                  AND table_id = '{table_id}'
            ) bar
            WHERE (geomval).val = -1
            GROUP BY table_id, column_id, tile_id
        ) foo
          WHERE obs_column_table_tile.table_id = foo.table_id
            AND obs_column_table_tile.column_id = foo.column_id
            AND obs_column_table_tile.tile_id = foo.tile_id
       ; '''.format(table_id=table_id, column_id=column_id,
                    colname=colname, tablename=tablename))
    resp = session.execute('''
       INSERT INTO observatory.obs_column_table_tile_simple
       SELECT table_id, column_id, tile_id, ST_Reclass(
         ST_Band(tile, ARRAY[2, 3]),
         ROW(2, '[0-1]::0-255, (1-100]::255-255', '8BUI', 0)::reclassarg
       ) AS tile
       FROM observatory.obs_column_table_tile
       WHERE table_id='{table_id}'
         AND column_id='{column_id}';
       '''.format(table_id=table_id, column_id=column_id,
                  colname=colname, tablename=tablename))

    real_num_geoms = session.execute('''
        SELECT COUNT({colname}) FROM {tablename}
    '''.format(colname=colname,
               tablename=tablename)).fetchone()[0]

    est_num_geoms = session.execute('''
        SELECT (ST_SummaryStatsAgg(tile, 1, false)).sum
        FROM observatory.obs_column_table_tile_simple
        WHERE column_id = '{column_id}'
          AND table_id = '{table_id}'
    '''.format(column_id=column_id,
               table_id=table_id)).fetchone()[0]

    assert abs(real_num_geoms - est_num_geoms) / real_num_geoms < 0.05, \
            "Estimate of {} total geoms more than 5% off real {} num geoms " \
            "for column '{}' in table '{}' (tablename '{}')".format(
                est_num_geoms, real_num_geoms, column_id, table_id, tablename)


class PostgresTarget(Target):
    '''
    PostgresTarget which by default uses command-line specified login.
    '''

    def __init__(self, schema, tablename):
        self._schema = schema
        self._tablename = tablename

    @property
    def table(self):
        return '"{schema}".{tablename}'.format(schema=self._schema,
                                               tablename=self._tablename)

    @property
    def tablename(self):
        return self._tablename

    @property
    def schema(self):
        return self._schema

    def _existenceness(self):
        '''
        Returns 0 if the table does not exist, 1 if it exists but has no
        rows (is empty), and 2 if it exists and has one or more rows.
        '''
        session = current_session()
        resp = session.execute('SELECT COUNT(*) FROM information_schema.tables '
                               "WHERE table_schema ILIKE '{schema}'  "
                               "  AND table_name ILIKE '{tablename}' ".format(
                                   schema=self._schema,
                                   tablename=self._tablename))
        if int(resp.fetchone()[0]) == 0:
            return 0
        resp = session.execute(
            'SELECT row_number() over () FROM "{schema}".{tablename} LIMIT 1'.format(
                schema=self._schema, tablename=self._tablename))
        if resp.fetchone() is None:
            return 1
        else:
            return 2

    def empty(self):
        '''
        Returns True if the table exists but has no rows in it.
        '''
        return self._existenceness() == 1

    def exists(self):
        '''
        Returns True if the table exists and has at least one row in it.
        '''
        return self._existenceness() == 2


class CartoDBTarget(Target):
    '''
    Target which is a CartoDB table
    '''

    def __init__(self, tablename, carto_url=None, api_key=None):
        self.tablename = tablename
        self.carto_url = carto_url
        self.api_key = api_key
        #resp = requests.get('{url}/dashboard/datasets'.format(
        #    url=os.environ['CARTODB_URL']
        #), cookies={
        #    '_cartodb_session': os.environ['CARTODB_SESSION']
        #}).content

    def __str__(self):
        return self.tablename

    def exists(self):
        resp = query_cartodb(
            'SELECT row_number() over () FROM "{tablename}" LIMIT 1'.format(
                tablename=self.tablename),
            api_key=self.api_key,
            carto_url=self.carto_url)
        if resp.status_code != 200:
            return False
        return resp.json()['total_rows'] > 0

    def remove(self, carto_url=None, api_key=None):
        api_key = api_key or os.environ['CARTODB_API_KEY']
        url = carto_url or os.environ['CARTODB_URL']

        try:
            while True:
                resp = requests.get('{url}/api/v1/tables/{tablename}?api_key={api_key}'.format(
                    url=carto_url,
                    tablename=self.tablename,
                    api_key=api_key
                ))
                viz_id = resp.json()['id']
                # delete dataset by id DELETE https://observatory.cartodb.com/api/v1/viz/ed483a0b-7842-4610-9f6c-8591273b8e5c
                try:
                    requests.delete('{url}/api/v1/viz/{viz_id}?api_key={api_key}'.format(
                        url=carto_url,
                        viz_id=viz_id,
                        api_key=api_key
                    ), timeout=1)
                except requests.Timeout:
                    pass
        except ValueError:
            pass
        query_cartodb('DROP TABLE IF EXISTS {tablename}'.format(tablename=self.tablename))
        assert not self.exists()
        #resp = requests.get('{url}/dashboard/datasets'.format(
        #    url=os.environ['CARTODB_URL']
        #), cookies={
        #    '_cartodb_session': os.environ['CARTODB_SESSION']
        #}).content


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


class ColumnTarget(Target):
    '''
    '''

    def __init__(self, column, task):
        self._id = column.id
        self._task = task
        self._column = column

    def get(self, session):
        '''
        Return a copy of the underlying OBSColumn in the specified session.
        '''
        with session.no_autoflush:
            return session.query(OBSColumn).get(self._id)

    def update_or_create(self):
        self._column = current_session().merge(self._column)

    def exists(self):
        existing = self.get(current_session())
        new_version = float(self._column.version or 0.0)
        if existing:
            existing_version = float(existing.version or 0.0)
            current_session().expunge(existing)
        else:
            existing_version = 0.0
        if existing and existing_version == new_version:
            return True
        elif existing and existing_version > new_version:
            raise Exception('Metadata version mismatch: cannot run task {task} '
                            '(id "{id}") '
                            'with ETL version ({etl}) older than what is in '
                            'DB ({db})'.format(task=self._task.task_id,
                                               id=self._id,
                                               etl=new_version,
                                               db=existing_version))
        return False


class TagTarget(Target):
    '''
    '''

    def __init__(self, tag, task):
        self._id = tag.id
        self._tag = tag
        self._task = task

    def get(self, session):
        '''
        Return a copy of the underlying OBSColumn in the specified session.
        '''
        with session.no_autoflush:
            return session.query(OBSTag).get(self._id)

    def update_or_create(self):
        self._tag = current_session().merge(self._tag)

    def exists(self):
        session = current_session()
        existing = self.get(session)
        new_version = float(self._tag.version or 0.0)
        if existing:
            existing_version = float(existing.version or 0.0)
            current_session().expunge(existing)
        else:
            existing_version = 0.0
        if existing and existing_version == new_version:
            return True
        elif existing and existing_version > new_version:
            raise Exception('Metadata version mismatch: cannot run task {task} '
                            '(id "{id}") '
                            'with ETL version ({etl}) older than what is in '
                            'DB ({db})'.format(task=self._task.task_id,
                                               id=self._id,
                                               etl=new_version,
                                               db=existing_version))
        return False


class TableTarget(Target):

    def __init__(self, schema, name, obs_table, columns, task):
        '''
        columns: should be an ordereddict if you want to specify columns' order
        in the table
        '''
        self._id = '.'.join([schema, name])
        obs_table.id = self._id
        obs_table.tablename = 'obs_' + sha1(underscore_slugify(self._id)).hexdigest()
        self.table = 'observatory.' + obs_table.tablename
        self._tablename = obs_table.tablename
        self._schema = schema
        self._name = name
        self._obs_table = obs_table
        self._obs_dict = obs_table.__dict__.copy()
        self._columns = columns
        self._task = task
        if obs_table.tablename in metadata.tables:
            self._table = metadata.tables[obs_table.tablename]
        else:
            self._table = None

    def sync(self):
        '''
        Whether this data should be synced to carto. Defaults to True.
        '''
        return True

    def exists(self):
        '''
        We always want to run this at least once, because we can always
        regenerate tabular data from scratch.
        '''
        session = current_session()
        existing = self.get(session)
        new_version = float(self._obs_table.version or 0.0)
        if existing:
            existing_version = float(existing.version or 0.0)
            if existing in session:
                session.expunge(existing)
        else:
            existing_version = 0.0
        if existing and existing_version == new_version:
            resp = session.execute(
                'SELECT COUNT(*) FROM information_schema.tables '
                "WHERE table_schema = '{schema}'  "
                "  AND table_name = '{tablename}' ".format(
                    schema='observatory',
                    tablename=existing.tablename))
            if int(resp.fetchone()[0]) == 0:
                return False
            resp = session.execute(
                'SELECT row_number() over () '
                'FROM "{schema}".{tablename} LIMIT 1 '.format(
                    schema='observatory',
                    tablename=existing.tablename))
            return resp.fetchone() is not None
        elif existing and existing_version > new_version:
            raise Exception('Metadata version mismatch: cannot run task {task} '
                            '(id "{id}") '
                            'with ETL version ({etl}) older than what is in '
                            'DB ({db})'.format(task=self._task.task_id,
                                               id=self._id,
                                               etl=new_version,
                                               db=existing_version))
        return False

    def get(self, session):
        '''
        Return a copy of the underlying OBSTable in the specified session.
        '''
        with session.no_autoflush:
            return session.query(OBSTable).get(self._id)

    def update_or_create_table(self):
        session = current_session()

        # create new local data table
        columns = []
        for colname, coltarget in self._columns.items():
            colname = colname.lower()
            col = coltarget.get(session)

            # Column info for sqlalchemy's internal metadata
            if col.type.lower() == 'geometry':
                coltype = Geometry
            elif col.type.lower().startswith('geometry(point'):
                coltype = Point
            elif col.type.lower().startswith('geometry(linestring'):
                coltype = Linestring

            # For enum type, pull keys from extra["categories"]
            elif col.type.lower().startswith('enum'):
                cats = col.extra['categories'].keys()
                coltype = types.Enum(*cats, name=col.id + '_enum')
            else:
                coltype = getattr(types, col.type.capitalize())
            columns.append(Column(colname, coltype))

        obs_table = self.get(session) or self._obs_table
        # replace local data table
        if obs_table.id in metadata.tables:
            metadata.tables[obs_table.id].drop()
        self._table = Table(obs_table.tablename, metadata, *columns,
                            extend_existing=True, schema='observatory')
        session.commit()
        self._table.drop(checkfirst=True)
        self._table.create()

    def update_or_create_metadata(self, _testmode=False):
        session = current_session()

        colinfo = {}

        if not _testmode:
            postgres_max_cols = 1664
            query_width = 7
            maxsize = postgres_max_cols / query_width
            for groupnum, group in enumerate(grouper(self._columns.iteritems(), maxsize)):
                select = []
                for i, colname_coltarget in enumerate(group):
                    if colname_coltarget is None:
                        continue
                    colname, coltarget = colname_coltarget
                    col = coltarget.get(session)
                    coltype = col.type.lower()
                    i = i + (groupnum * maxsize)
                    if coltype == 'numeric':
                        select.append('sum(case when {colname} is not null then 1 else 0 end) col{i}_notnull, '
                                      'max({colname}) col{i}_max, '
                                      'min({colname}) col{i}_min, '
                                      'avg({colname}) col{i}_avg, '
                                      'percentile_cont(0.5) within group (order by {colname}) col{i}_median, '
                                      'mode() within group (order by {colname}) col{i}_mode, '
                                      'stddev_pop({colname}) col{i}_stddev'.format(
                                          i=i, colname=colname.lower()))
                    elif coltype == 'geometry':
                        select.append('sum(case when {colname} is not null then 1 else 0 end) col{i}_notnull, '
                                      'max(st_area({colname}::geography)) col{i}_max, '
                                      'min(st_area({colname}::geography)) col{i}_min, '
                                      'avg(st_area({colname}::geography)) col{i}_avg, '
                                      'percentile_cont(0.5) within group (order by st_area({colname}::geography)) col{i}_median, '
                                      'mode() within group (order by st_area({colname}::geography)) col{i}_mode, '
                                      'stddev_pop(st_area({colname}::geography)) col{i}_stddev'.format(
                                          i=i, colname=colname.lower()))

                if select:
                    stmt = 'SELECT COUNT(*) cnt, {select} FROM {output}'.format(
                        select=', '.join(select), output=self.table)
                    resp = session.execute(stmt)
                    colinfo.update(dict(zip(resp.keys(), resp.fetchone())))

        # replace metadata table
        self._obs_table = session.merge(self._obs_table)
        obs_table = self._obs_table

        for i, colname_coltarget in enumerate(self._columns.iteritems()):
            colname, coltarget = colname_coltarget
            colname = colname.lower()
            col = coltarget.get(session)

            if _testmode:
                coltable = OBSColumnTable(colname=colname, table=obs_table,
                                          column=col)
            else:
                # Column info for obs metadata
                coltable = session.query(OBSColumnTable).filter_by(
                    column_id=col.id, table_id=obs_table.id).first()
                if coltable:
                    coltable_existed = True
                    coltable.colname = colname
                else:
                    # catch the case where a column id has changed
                    coltable = session.query(OBSColumnTable).filter_by(
                        table_id=obs_table.id, colname=colname).first()
                    if coltable:
                        coltable_existed = True
                        coltable.column = col
                    else:
                        coltable_existed = False
                        coltable = OBSColumnTable(colname=colname, table=obs_table,
                                                  column=col)

                # include analysis
                if col.type.lower() in ('numeric', 'geometry',):
                    # do not include linkage for any column that is 100% null
                    # unless we are in test mode
                    stats = {
                        'count': colinfo.get('cnt'),
                        'notnull': colinfo.get('col%s_notnull' % i),
                        'max': colinfo.get('col%s_max' % i),
                        'min': colinfo.get('col%s_min' % i),
                        'avg': colinfo.get('col%s_avg' % i),
                        'median': colinfo.get('col%s_median' % i),
                        'mode': colinfo.get('col%s_mode' % i),
                        'stddev': colinfo.get('col%s_stddev' % i),
                    }
                    if stats['notnull'] == 0:
                        if coltable_existed:
                            session.delete(coltable)
                        elif coltable in session:
                            session.expunge(coltable)
                        continue
                    for k in stats.keys():
                        if stats[k] is not None:
                            stats[k] = float(stats[k])
                    coltable.extra = {
                        'stats': stats
                    }
            session.add(coltable)


class ColumnsTask(Task):
    '''
    The ColumnsTask provides a structure for generating metadata. The only
    required method is :meth:`~.ColumnsTask.columns`.
    The keys may be used as human-readable column names in tables based off
    this metadata, although that is not always the case. If the id of the
    :class:`OBSColumn <tasks.meta.OBSColumn>` is left blank, the dict's key will be used to
    generate it (qualified by the module).

    Also, conventionally there will be a requires method that brings in our
    standard tags: :class:`SectionTags <tasks.tags.SectionTags>`,
    :class:`SubsectionTags <tasks.tags.SubsectionTags>`, and
    :class:`UnitTags <tasks.tags.UnitTags>`.
    This is an example of defining several tasks as prerequisites: the outputs
    of those tasks will be accessible via ``self.input()[<key>]`` in other
    methods.

    This will update-or-create columns defined in it when run.  Whether a
    column is updated or not depends on whether it exists in the database with
    the same :meth:`~.ColumnsTask.version` number.
    '''

    def columns(self):
        '''
        This method must be overriden in subclasses.  It must return a
        :py:class:`collections.OrderedDict` whose values are all instances of
        :class:`OBSColumn <tasks.meta.OBSColumn>` and whose keys are all strings.
        '''
        raise NotImplementedError('Must return iterable of OBSColumns')

    def on_failure(self, ex):
        session_rollback(self, ex)
        super(ColumnsTask, self).on_failure(ex)

    def on_success(self):
        session_commit(self)

    def run(self):
        for _, coltarget in self.output().iteritems():
            coltarget.update_or_create()

    def version(self):
        '''
        Returns a number that will be linked to the all
        :py:attr:`tasks.meta.OBSColumn.version` objects resulting from this
        task.  If this is identical to what's already in the database, the
        :class:`OBSColumn <tasks.meta.OBSColumn>` will not be replaced.
        '''
        return 0

    def output(self):
        #if self.deps() and not all([d.complete() for d in self.deps()]):
        #    raise Exception('Must run prerequisites first')
        session = current_session()

        # Return columns from database if the task is already finished
        if hasattr(self, '_colids') and self.complete():
            return OrderedDict([
                (colkey, ColumnTarget(session.query(OBSColumn).get(cid), self))
                for colkey, cid in self.colids.iteritems()
            ])

        # Otherwise, run `columns` (slow!) to generate output
        already_in_session = [obj for obj in session]
        output = OrderedDict()

        input_ = self.input()
        for col_key, col in self.columns().iteritems():
            if not isinstance(col, OBSColumn):
                raise RuntimeError(
                    'Values in `.columns()` must be of type OBSColumn, but '
                    '"{col}" is type {type}'.format(col=col_key, type=type(col)))
            if not col.version:
                col.version = self.version()
            col.id = '.'.join([classpath(self), col.id or col_key])
            tags = self.tags(input_, col_key, col)
            if isinstance(tags, TagTarget):
                col.tags.append(tags)
            else:
                col.tags.extend(tags)

            output[col_key] = ColumnTarget(col, self)
        now_in_session = [obj for obj in session]
        for obj in now_in_session:
            if obj not in already_in_session:
                if obj in session:
                    session.expunge(obj)
        return output

    @property
    def colids(self):
        '''
        Return colids for the output columns, this can be cached
        '''
        if not hasattr(self, '_colids'):
            self._colids = OrderedDict([
                (colkey, ct._id) for colkey, ct in self.output().iteritems()
            ])
        return self._colids

    def complete(self):
        '''
        Custom complete method that attempts to check if output exists, as is
        default, but in case of failure allows attempt to run dependencies (a
        missing dependency could result in exception on `output`).
        '''
        deps = self.deps()
        if deps and not all([d.complete() for d in deps]):
            return False
        else:
            #_complete = super(ColumnsTask, self).complete()
            # bulk check that all columns exist at proper version
            cnt = current_session().execute(
                '''
                SELECT COUNT(*)
                FROM observatory.obs_column
                WHERE id IN ('{ids}') AND version = '{version}'
                '''.format(
                    ids="', '".join(self.colids.values()),
                    version=self.version()
                )).fetchone()[0]
            return cnt == len(self.colids.values())

    def tags(self, input_, col_key, col):
        '''
        Replace with an iterable of :class:`OBSColumn <tasks.meta.OBSColumn>`
        that should be applied to each column

        :param input_: A saved version of this class's :meth:`input <luigi.Task.input>`
        :param col_key: The key of the column this will be applied to.
        :param column: The :class:`OBSColumn <tasks.meta.OBSColumn>` these tags
                       will be applied to.
        '''
        return []


class TagsTask(Task):
    '''
    This will update-or-create :class:`OBSTag <tasks.meta.OBSTag>` objects
    int the database when run.

    The :meth:`~.TagsTask.tags` method must be overwritten.

    :meth:`~TagsTask.version` is used to control updates to the database.
    '''

    def tags(self):
        '''
        This method must be overwritten in subclasses.

        The return value must be an iterable of instances of
        :class:`OBSTag <tasks.meta.OBSTag>`.
        '''
        raise NotImplementedError('Must return iterable of OBSTags')

    def on_failure(self, ex):
        session_rollback(self, ex)
        super(TagsTask, self).on_failure(ex)

    def on_success(self):
        session_commit(self)

    def run(self):
        for _, tagtarget in self.output().iteritems():
            tagtarget.update_or_create()

    def version(self):
        return 0

    def output(self):
        #if self.deps() and not all([d.complete() for d in self.deps()]):
        #    raise Exception('Must run prerequisites first')
        output = {}
        for tag in self.tags():
            orig_id = tag.id
            tag.id = '.'.join([classpath(self), orig_id])
            if not tag.version:
                tag.version = self.version()
            output[orig_id] = TagTarget(tag, self)
        return output

    def complete(self):
        '''
        Custom complete method that attempts to check if output exists, as is
        default, but in case of failure allows attempt to run dependencies (a
        missing dependency could result in exception on `output`).
        '''
        deps = self.deps()
        if deps and not all([d.complete() for d in deps]):
            return False
        else:
            return super(TagsTask, self).complete()


class TableToCartoViaImportAPI(Task):
    '''
    This task wraps :func:`~.util.sql_to_cartodb_table` to upload a table to
    a CARTO account specified in the ``.env`` file quickly using the Import
    API.

    :param table: The name of the table to upload, exclusive of schema.
    :param schema: Optional. The schema of the table to upload, defaults to
                   ``observatory``.
    :param force: Optional boolean.  Defaults to ``False``.  If ``True``, a
                  table of the same name existing remotely will be overwritten.
    '''

    force = BooleanParameter(default=False, significant=False)
    schema = Parameter(default='observatory')
    username = Parameter(default=None, significant=False)
    api_key = Parameter(default=None, significant=False)
    outname = Parameter(default=None, significant=False)
    table = Parameter()
    columns = ListParameter(default=[])

    def run(self):
        carto_url = 'https://{}.carto.com'.format(self.username) if self.username else os.environ['CARTODB_URL']
        api_key = self.api_key if self.api_key else os.environ['CARTODB_API_KEY']
        try:
            os.makedirs(os.path.join('tmp', classpath(self)))
        except OSError:
            pass
        outname = self.outname or self.table
        tmp_file_path = os.path.join('tmp', classpath(self), outname + '.csv')
        if not self.columns:
            shell(r'''psql -c '\copy "{schema}".{tablename} TO '"'"{tmp_file_path}"'"'
                  WITH CSV HEADER' '''.format(
                      schema=self.schema,
                      tablename=self.table,
                      tmp_file_path=tmp_file_path,
                  ))
        else:
            shell(r'''psql -c '\copy (SELECT {columns} FROM "{schema}".{tablename}) TO '"'"{tmp_file_path}"'"'
                  WITH CSV HEADER' '''.format(
                      schema=self.schema,
                      tablename=self.table,
                      tmp_file_path=tmp_file_path,
                      columns=', '.join(self.columns),
                  ))
        curl_resp = shell(
            'curl -s -F privacy=public -F type_guessing=false '
            '  -F file=@{tmp_file_path} "{url}/api/v1/imports/?api_key={api_key}"'.format(
                tmp_file_path=tmp_file_path,
                url=carto_url,
                api_key=api_key
            ))
        try:
            import_id = json.loads(curl_resp)["item_queue_id"]
        except ValueError:
            raise Exception(curl_resp)
        while True:
            resp = requests.get('{url}/api/v1/imports/{import_id}?api_key={api_key}'.format(
                url=carto_url,
                import_id=import_id,
                api_key=api_key
            ))
            if resp.json()['state'] == 'complete':
                LOGGER.info("Waiting for import %s for %s", import_id, outname)
                break
            elif resp.json()['state'] == 'failure':
                raise Exception('Import failed: {}'.format(resp.json()))

            print resp.json()['state']
            time.sleep(1)

        # If CARTO still renames our table to _1, just force alter it
        if resp.json()['table_name'] != outname:
            query_cartodb('ALTER TABLE {oldname} RENAME TO {newname}'.format(
                oldname=resp.json()['table_name'],
                newname=outname,
                carto_url=carto_url,
                api_key=api_key,
            ))
            assert resp.status_code == 200

        # fix broken column data types -- alter everything that's not character
        # varying back to it
        try:
            session = current_session()
            resp = session.execute(
                '''
                SELECT att.attname,
                       pg_catalog.format_type(atttypid, NULL) AS display_type,
                       att.attndims
                FROM pg_attribute att
                  JOIN pg_class tbl ON tbl.oid = att.attrelid
                  JOIN pg_namespace ns ON tbl.relnamespace = ns.oid
                WHERE tbl.relname = '{tablename}'
                  AND pg_catalog.format_type(atttypid, NULL) NOT IN
                      ('character varying', 'text', 'user-defined', 'geometry')
                  AND att.attname IN (SELECT column_name from information_schema.columns
                                      WHERE table_schema='{schema}'
                                        AND table_name='{tablename}')
                  AND ns.nspname = '{schema}';
                '''.format(schema=self.schema,
                           tablename=self.table.lower())).fetchall()
            alter = ', '.join([
                " ALTER COLUMN {colname} SET DATA TYPE {data_type} "
                " USING NULLIF({colname}, '')::{data_type}".format(
                    colname=colname, data_type=data_type
                ) for colname, data_type, _ in resp])
            if alter:
                alter_stmt = 'ALTER TABLE {tablename} {alter}'.format(
                    tablename=outname,
                    alter=alter)
                LOGGER.info(alter_stmt)
                resp = query_cartodb(alter_stmt, api_key=api_key, carto_url=carto_url)
                if resp.status_code != 200:
                    raise Exception('could not alter columns for "{tablename}":'
                                    '{err}'.format(tablename=outname,
                                                   err=resp.text))
        except Exception as err:
            # in case of error, delete the uploaded but not-yet-properly typed
            # table
            self.output().remove(carto_url=carto_url, api_key=api_key)
            raise err

    def output(self):
        carto_url = 'https://{}.carto.com'.format(self.username) if self.username else os.environ['CARTODB_URL']
        api_key = self.api_key if self.api_key else os.environ['CARTODB_API_KEY']
        target = CartoDBTarget(self.outname or self.table, api_key=api_key, carto_url=carto_url)
        if self.force:
            target.remove(carto_url=carto_url, api_key=api_key)
            self.force = False
        return target


class TableToCarto(Task):

    force = BooleanParameter(default=False, significant=False)
    schema = Parameter(default='observatory')
    table = Parameter()
    outname = Parameter(default=None)

    def run(self):
        json_colnames = []
        table = '.'.join([self.schema, self.table])
        if table in metadata.tables:
            cols = metadata.tables[table].columns
            for colname, coldef in cols.items():
                coltype = coldef.type
                if isinstance(coltype, JSON):
                    json_colnames.append(colname)

        sql_to_cartodb_table(self.output().tablename, self.table, json_colnames,
                             schema=self.schema)
        self.force = False

    def output(self):
        if self.schema != 'observatory':
            table = '.'.join([self.schema, self.table])
        else:
            table = self.table
        if self.outname is None:
            self.outname = underscore_slugify(table)
        target = CartoDBTarget(self.outname)
        if self.force and target.exists():
            target.remove()
            self.force = False
        return target


# https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-camel-case
def camel_to_underscore(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()



class DownloadUnzipTask(Task):
    '''
    Download a zip file to location {output}.zip and unzip it to the folder
    {output}.  Subclasses only need to define a
    :meth:`~.util.DownloadUnzipTask.download` method.
    '''

    def download(self):
        '''
        Subclasses must override this.  A good starting point is:

        .. code:: python

            shell('wget -O {output}.zip {url}'.format(
              output=self.output().path,
              url=<URL>
            ))
        '''
        raise NotImplementedError('DownloadUnzipTask must define download()')

    def run(self):
        os.makedirs(self.output().path)
        try:
            self.download()
            shell('unzip -d {output} {output}.zip'.format(output=self.output().path))
        except Exception as err:
            os.rmdir(self.output().path)
            raise

    def output(self):
        '''
        The default output location is in the ``tmp`` folder, in a subfolder
        derived from the subclass's :meth:`~.util.classpath` and its
        :attr:`~.task_id`.
        '''
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))


class TempTableTask(Task):
    '''
    A Task that generates a table that will not be referred to in metadata.

    This is useful for intermediate processing steps that can benefit from the
    session guarantees of the ETL, as well as automatic table naming.

    :param force: Optional Boolean, ``False`` by default.  If ``True``, will
                  overwrite output table even if it exists already.
    '''

    force = BooleanParameter(default=False, significant=False)

    def on_failure(self, ex):
        session_rollback(self, ex)
        super(TempTableTask, self).on_failure(ex)

    def on_success(self):
        session_commit(self)

    def run(self):
        '''
        Must be overriden by subclass.  Should create and populate a table
        named from ``self.output().table``

        If this completes without exceptions, the :func:`~.util.current_session
        will be committed; if there is an exception, it will be rolled back.
        '''
        raise Exception('Must override `run`')

    def output(self):
        '''
        By default, returns a :class:`~.util.TableTarget` whose associated
        table lives in a special-purpose schema in Postgres derived using
        :func:`~.util.classpath`.
        '''
        return PostgresTarget(classpath(self), self.task_id)


@TempTableTask.event_handler(Event.START)
def clear_temp_table(task):
    target = task.output()
    shell("psql -c 'CREATE SCHEMA IF NOT EXISTS \"{schema}\"'".format(
        schema=classpath(task)))
    if task.force or target.empty():
        session = current_session()
        session.execute('DROP TABLE IF EXISTS "{schema}".{tablename}'.format(
            schema=classpath(task), tablename=task.task_id))
        session.flush()


class GdbFeatureClass2TempTableTask(TempTableTask):
    '''
    A task that extracts one vector shape layer from a geodatabase to a
    TempTableTask.
    '''

    feature_class = Parameter()

    def input_gdb(self):
        '''
        This method must be implemented by subclasses.  Should return a path
        to a GDB to convert to shapes.
        '''
        raise NotImplementedError("Must define `input_gdb` method")

    def run(self):
        shell('''
              PG_USE_COPY=yes ogr2ogr -f "PostgreSQL" PG:"dbname=$PGDATABASE \
              active_schema={schema}" -t_srs "EPSG:4326" -nlt MultiPolygon \
              -nln {tablename} {infile}
              '''.format(schema=self.output().schema,
                         infile=self.input_gdb(),
                         tablename=self.output().tablename))


class Shp2TempTableTask(TempTableTask):
    '''
    A task that loads :meth:`~.util.Shp2TempTableTask.input_shp()` into a
    temporary Postgres table.  That method must be overriden.
    '''

    encoding = Parameter(default='latin1', significant=False)

    def input_shp(self):
        '''
        This method must be implemented by subclasses.  Should return either
        a path to a single shapefile, or an iterable of paths to shapefiles.
        In that case, the first in the list will determine the schema.
        '''
        raise NotImplementedError("Must specify `input_shp` method")

    def run(self):
        if isinstance(self.input_shp(), basestring):
            shps = [self.input_shp()]
        else:
            shps = self.input_shp()
        schema = self.output().schema
        tablename = self.output().tablename
        operation = '-overwrite -lco OVERWRITE=yes -lco SCHEMA={schema} -lco PRECISION=no '.format(
            schema=schema)
        for shp in shps:
            cmd = 'PG_USE_COPY=yes PGCLIENTENCODING={encoding} ' \
                    'ogr2ogr -f PostgreSQL PG:"dbname=$PGDATABASE ' \
                    'active_schema={schema}" -t_srs "EPSG:4326" ' \
                    '-nlt MultiPolygon -nln {table} ' \
                    '{operation} \'{input}\' '.format(
                        encoding=self.encoding,
                        schema=schema,
                        table=tablename,
                        input=shp,
                        operation=operation
                    )
            shell(cmd)
            operation = '-append '.format(schema=schema)


class CSV2TempTableTask(TempTableTask):
    '''
    A task that loads :meth:`~.util.CSV2TempTableTask.input_csv` into a
    temporary Postgres table.  That method must be overriden.

    Optionally, :meth:`~util.CSV2TempTableTask.coldef` can be overriden.

    Under the hood, uses postgres's ``COPY``.

    :param delimiter: Delimiter separating fields in the CSV.  Defaults to
                      ``,``.  Must be one character.
    :param has_header: Boolean as to whether first row has column names.
                       Defaults to ``True``.
    :param force: Boolean as to whether the task should be run even if the
                  temporary output already exists.
    '''

    delimiter = Parameter(default=',', significant=False)
    has_header = BooleanParameter(default=True, significant=False)
    encoding = Parameter(default='utf8', significant=False)

    def input_csv(self):
        '''
        Must be overriden with a method that returns either a path to a CSV
        or an iterable of paths to CSVs.
        '''
        raise NotImplementedError("Must specify `input_csv` method")

    def coldef(self):
        '''
        Override this function to customize the column definitions in the table.
        Expected is an iterable of two-tuples.

        If not overriden:

        * All column types will be ``Text``
        * If :attr:`~.util.CSV2TempTableTask.has_header` is ``True``, then
          column names will come from the headers.
        * If :attr:`~.util.CSV2TempTableTask.has_header` is ``False``, then
          column names will be the postgres defaults.
        '''
        if isinstance(self.input_csv(), basestring):
            csv = self.input_csv()
        else:
            raise NotImplementedError("Cannot automatically determine colnames "
                                      "if several input CSVs.")
        header_row = shell('head -n 1 "{csv}"'.format(csv=csv)).strip()
        return [(h.replace('"', ''), 'Text') for h in header_row.split(self.delimiter)]

    def read_method(self, fname):
        return 'cat "{input}"'.format(input=fname)

    def run(self):
        if isinstance(self.input_csv(), basestring):
            csvs = [self.input_csv()]
        else:
            csvs = self.input_csv()

        session = current_session()
        session.execute(u'CREATE TABLE {output} ({coldef})'.format(
            output=self.output().table,
            coldef=u', '.join([u'"{}" {}'.format(c[0].decode(self.encoding), c[1]) for c in self.coldef()])
        ))
        session.commit()
        options = ['''
           DELIMITER '"'{delimiter}'"' ENCODING '"'{encoding}'"'
        '''.format(delimiter=self.delimiter,
                   encoding=self.encoding)]
        if self.has_header:
            options.append('CSV HEADER')
        try:
            for csv in csvs:
                shell(r'''{read_method} | psql -c '\copy {table} FROM STDIN {options}' '''.format(
                    read_method=self.read_method(csv),
                    table=self.output().table,
                    options=' '.join(options)
                ))
            self.after_copy()
        except:
            session.rollback()
            session.execute('DROP TABLE IF EXISTS {output}'.format(
                output=self.output().table))
            session.commit()
            raise

    def after_copy(self):
        pass


class LoadPostgresFromURL(TempTableTask):

    def load_from_url(self, url):
        '''
        Load psql at a URL into the database.

        Ignores tablespaces assigned in the SQL.
        '''
        shell('curl {url} | gunzip -c | grep -v default_tablespace | psql'.format(
            url=url))
        self.mark_done()

    def mark_done(self):
        session = current_session()
        session.execute('DROP TABLE IF EXISTS {table}'.format(
            table=self.output().table))
        session.execute('CREATE TABLE {table} AS SELECT now() creation_time'.format(
            table=self.output().table))


class TableTask(Task):
    '''
    This task creates a single output table defined by its name, path, and
    defined columns.

    :meth:`~.TableTask.timespan`, :meth:`~.TableTask.columns`, and
    :meth:`~.TableTask.populate` must be overwritten in subclasses.

    When run, this task will automatically create the table with a schema
    corresponding to the defined columns, with a unique name.  It will also
    generate all relevant metadata for the table, and link it to the columns.
    '''

    def _requires(self):
        reqs = super(TableTask, self)._requires()
        if self._testmode:
            return [r for r in reqs if isinstance(r, (TagsTask, TableTask, ColumnsTask,))]
        else:
            return reqs

    def version(self):
        '''
        Must return a version control number, which is useful for forcing a
        re-run/overwrite without having to track down and delete output
        artifacts.
        '''
        return 0

    def on_failure(self, ex):
        session_rollback(self, ex)
        super(TableTask, self).on_failure(ex)

    def on_success(self):
        session_commit(self)
        super(TableTask, self).on_success()

    def columns(self):
        '''
        Must be overriden by subclasses.  Columns returned from this function
        determine the schema of the resulting :class:`~tasks.util.TableTarget`.

        The return for this function should be constructed by selecting the
        desired columns from :class:`~tasks.util.ColumnTask`s, specified as
        inputs in :meth:`~.util.TableTask.requires()`

        Must return a :py:class:`~collections.OrderedDict` of
        (colname, :class:`~tasks.util.ColumnTarget`) pairs.

        '''
        raise NotImplementedError('Must implement columns method that returns '
                                   'a dict of ColumnTargets')

    def populate(self):
        '''
        This method must populate (most often via ``INSERT``) the output table.

        For example:
        '''
        raise NotImplementedError('Must implement populate method that '
                                   'populates the table')

    def fake_populate(self, output):
        '''
        Put one empty row in the table
        '''
        session = current_session()
        session.execute('INSERT INTO {output} ({col}) VALUES (NULL)'.format(
            output=output.table,
            col=self._columns.keys()[0]
        ))

    def description(self):
        '''
        Optional description for the :class:`~tasks.util.OBSTable`.  Not
        currently used anywhere.
        '''
        return None

    def timespan(self):
        '''
        Must return an arbitrary string timespan (for example, ``2014``, or
        ``2012Q4``) that identifies the date range or point-in-time for this
        table.  Must be implemented by subclass.
        '''
        raise NotImplementedError('Must define timespan for table')

    def the_geom(self, output, colname):
        session = current_session()
        return session.execute(
            'SELECT ST_AsText( '
            '  ST_Intersection( '
            '    ST_MakeEnvelope(-179.999, -89.999, 179.999, 89.999, 4326), '
            '    ST_Multi( '
            '      ST_CollectionExtract( '
            '        ST_MakeValid( '
            '          ST_SnapToGrid( '
            '            ST_Buffer( '
            '              ST_Union( '
            '                ST_MakeValid( '
            '                  ST_Simplify( '
            '                    ST_SnapToGrid({geom_colname}, 0.3) '
            '                  , 0) '
            '                ) '
            '              ) '
            '            , 0.3, 2) '
            '          , 0.3) '
            '        ) '
            '      , 3) '
            '    ) '
            '  ) '
            ') the_geom '
            'FROM {output}'.format(
                geom_colname=colname,
                output=output.table
            )).fetchone()['the_geom']

    @property
    def _testmode(self):
        if os.environ.get('ENVIRONMENT') == 'test':
            return True
        return getattr(self, '_test', False)

    def run(self):
        LOGGER.info('getting output()')
        before = time.time()
        output = self.output()
        after = time.time()
        LOGGER.info('time: %s', after - before)

        LOGGER.info('update_or_create_table')
        before = time.time()
        output.update_or_create_table()
        after = time.time()
        LOGGER.info('time: %s', after - before)

        if self._testmode:
            LOGGER.info('fake_populate')
            before = time.time()
            self.fake_populate(output)
            after = time.time()
            LOGGER.info('time: %s', after - before)
        else:
            LOGGER.info('populate')
            self.populate()

        before = time.time()
        LOGGER.info('update_or_create_metadata')
        output.update_or_create_metadata(_testmode=self._testmode)
        after = time.time()
        LOGGER.info('time: %s', after - before)

        if not self._testmode:
            LOGGER.info('create_indexes')
            self.create_indexes(output)
            current_session().flush()

            LOGGER.info('create_geom_summaries')
            self.create_geom_summaries(output)

    def create_indexes(self, output):
        session = current_session()
        tablename = output.table
        for colname, coltarget in self._columns.iteritems():
            col = coltarget._column
            index_type = col.index_type
            if index_type:
                index_name = '{}_{}_idx'.format(tablename.split('.')[-1], colname)
                session.execute('CREATE {unique} INDEX IF NOT EXISTS {index_name} ON {table} '
                                'USING {index_type} ({colname})'.format(
                                    #unique='UNIQUE' if index_type == 'btree' else '',
                                    unique='',
                                    index_type=index_type,
                                    index_name=index_name,
                                    table=tablename, colname=colname))

    def create_geom_summaries(self, output):
        geometry_columns = [
            (colname, coltarget._id) for colname, coltarget in
            self.columns().iteritems() if coltarget._column.type.lower().startswith('geometry')
        ]

        if len(geometry_columns) == 0:
            return
        elif len(geometry_columns) > 1:
            raise Exception('Having more than one geometry column in one table '
                            'could lead to problematic behavior ')
        colname, colid = geometry_columns[0]
        # Use SQL directly instead of SQLAlchemy because we need the_geom set
        # on obs_table in this session
        current_session().execute("UPDATE observatory.obs_table "
                                  "SET the_geom = ST_GeomFromText('{the_geom}', 4326) "
                                  "WHERE id = '{id}'".format(
                                      the_geom=self.the_geom(output, colname),
                                      id=output._id
                                  ))
        generate_tile_summary(current_session(),
                              output._id, colid, output.table, colname)

    def output(self):
        #if self.deps() and not all([d.complete() for d in self.deps()]):
        #    raise Exception('Must run prerequisites first')
        if not hasattr(self, '_columns'):
            self._columns = self.columns()

        tt = TableTarget(classpath(self),
                           underscore_slugify(self.task_id),
                           OBSTable(description=self.description(),
                                    version=self.version(),
                                    timespan=self.timespan()),
                           self._columns, self)
        return tt

    def complete(self):
        return TableTarget(classpath(self),
                           underscore_slugify(self.task_id),
                           OBSTable(description=self.description(),
                                    version=self.version(),
                                    timespan=self.timespan()),
                           [], self).exists()


class RenameTables(Task):
    '''
    A one-time use task that renames all ID-instantiated data tables to their
    tablename.
    '''

    def run(self):
        session = current_session()
        for table in session.query(OBSTable):
            table_id = table.id
            tablename = table.tablename
            schema = '.'.join(table.id.split('.')[0:-1]).strip('"')
            table = table.id.split('.')[-1]
            resp = session.execute('SELECT COUNT(*) FROM information_schema.tables '
                                   "WHERE table_schema ILIKE '{schema}'  "
                                   "  AND table_name ILIKE '{table}' ".format(
                                       schema=schema,
                                       table=table))
            if int(resp.fetchone()[0]) > 0:
                resp = session.execute('SELECT COUNT(*) FROM information_schema.tables '
                                       "WHERE table_schema ILIKE 'observatory'  "
                                       "  AND table_name ILIKE '{table}' ".format(
                                           table=tablename))
                # new table already exists -- just drop it
                if int(resp.fetchone()[0]) > 0:
                    cmd = 'DROP TABLE {table_id} CASCADE'.format(table_id=table_id)
                    session.execute(cmd)
                else:
                    cmd = 'ALTER TABLE {old} RENAME TO {new}'.format(
                        old=table_id, new=tablename)
                    print cmd
                    session.execute(cmd)
                    cmd = 'ALTER TABLE "{schema}".{new} SET SCHEMA observatory'.format(
                        new=tablename, schema=schema)
                    print cmd
                    session.execute(cmd)
            else:
                resp = session.execute('SELECT COUNT(*) FROM information_schema.tables '
                                       "WHERE table_schema ILIKE 'public'  "
                                       "  AND table_name ILIKE '{table}' ".format(
                                           table=tablename))
                if int(resp.fetchone()[0]) > 0:
                    cmd = 'ALTER TABLE public.{new} SET SCHEMA observatory'.format(
                        new=tablename)
                    print cmd
                    session.execute(cmd)

        session.commit()
        self._complete = True

    def complete(self):
        return hasattr(self, '_complete')


class CreateGeomIndexes(Task):
    '''
    Make sure every table has a `the_geom` index.
    '''

    def run(self):
        session = current_session()
        resp = session.execute('SELECT DISTINCT geom_colname, geom_tablename '
                               'FROM observatory.obs_meta ')
        for colname, tablename in resp:
            index_name = '{}_{}_idx'.format(tablename, colname)
            resp = session.execute("SELECT to_regclass('observatory.{}')".format(
                index_name))
            print index_name
            if not resp.fetchone()[0]:
                session.execute('CREATE INDEX {index_name} ON observatory.{tablename} '
                                'USING GIST ({colname})'.format(
                                    index_name=index_name,
                                    tablename=tablename,
                                    colname=colname))
        session.commit()
        self._complete = True

    def complete(self):
        return getattr(self, '_complete', False)


class DropOrphanTables(Task):
    '''
    Remove tables that aren't documented anywhere in metadata.  Cleaning.
    '''

    force = BooleanParameter(default=False)

    def run(self):
        session = current_session()
        resp = session.execute('''
SELECT table_name
FROM information_schema.tables
WHERE table_name LIKE 'obs_%'
  AND table_schema = 'observatory'
  AND table_name NOT IN (SELECT tablename FROM observatory.obs_table)
  AND LENGTH(table_name) = 44
''')
        for row in resp:
            tablename = row[0]
            if not self.force:
                cnt = session.execute(
                    'select count(*) from observatory.{}'.format(tablename)).fetchone()[0]
                if cnt > 0:
                    LOGGER.warn('not automatically dropping {}, it has {} rows'.format(
                        tablename, cnt))
                    continue
            session.execute('drop table observatory.{}'.format(tablename))
        session.commit()


class Carto2TempTableTask(TempTableTask):
    '''
    Import a table from a CARTO account into a temporary table.

    :param subdomain: Optional. The subdomain the table resides in. Defaults
                       to ``observatory``.
    :param table: The name of the table to be imported.
    '''

    subdomain = Parameter(default='observatory')
    table = Parameter()

    TYPE_MAP = {
        'string': 'TEXT',
        'number': 'NUMERIC',
        'geometry': 'GEOMETRY',
        'date': 'TIMESTAMP',
    }

    @property
    def _url(self):
        return 'https://{subdomain}.carto.com/api/v2/sql'.format(
            subdomain=self.subdomain
        )

    def _query(self, **params):
        return requests.get(self._url, params=params)

    def _create_table(self):
        resp = self._query(
            q='SELECT * FROM {table} LIMIT 0'.format(table=self.table)
        )
        coltypes = dict([
            (k, self.TYPE_MAP[v['type']]) for k, v in resp.json()['fields'].iteritems()
        ])
        resp = self._query(
            q='SELECT * FROM {table} LIMIT 0'.format(table=self.table),
            format='csv'
        )
        colnames = resp.text.strip().split(',')
        columns = ', '.join(['{colname} {type}'.format(
            colname=c,
            type=coltypes[c]
        ) for c in colnames])
        stmt = 'CREATE TABLE {table} ({columns})'.format(table=self.output().table,
                                                         columns=columns)
        shell("psql -c '{stmt}'".format(stmt=stmt))

    def _load_rows(self):
        url = self._url + '?q={q}&format={format}'.format(
            q=quote_plus('SELECT * FROM {table}'.format(table=self.table)),
            format='csv'
        )
        shell(r"curl '{url}' | "
              r"psql -c '\copy {table} FROM STDIN WITH CSV HEADER'".format(
                  table=self.output().table,
                  url=url))

    def run(self):
        self._create_table()
        self._load_rows()
        shell("psql -c 'CREATE INDEX ON {table} USING gist (the_geom)'".format(
            table=self.output().table,
        ))


class CustomTable(TempTableTask):

    measures = ListParameter()
    boundary = Parameter()

    def run(self):

        session = current_session()
        meta = '''
        SELECT numer_colname, numer_type, numer_geomref_colname, numer_tablename,
               geom_colname, geom_type, geom_geomref_colname, geom_tablename
        FROM observatory.obs_meta
        WHERE numer_id IN ('{measures}')
          AND geom_id = '{boundary}'
        '''.format(measures="', '".join(self.measures),
                   boundary=self.boundary)

        colnames = set()
        tables = set()
        where = set()
        coldefs = set()

        for row in session.execute(meta):
            numer_colname, numer_type, numer_geomref_colname, numer_tablename, \
                    geom_colname, geom_type, geom_geomref_colname, geom_tablename = row
            colnames.add('{}.{}'.format(numer_tablename, numer_colname))
            colnames.add('{}.{}'.format(geom_tablename, geom_colname))
            coldefs.add('{} {}'.format(numer_colname, numer_type))
            coldefs.add('{} {}'.format(geom_colname, geom_type))
            tables.add('observatory."{}"'.format(numer_tablename))
            tables.add('observatory."{}"'.format(geom_tablename))
            where.add('{}.{} = {}.{}'.format(numer_tablename, numer_geomref_colname,
                                             geom_tablename, geom_geomref_colname))

        create = '''
        CREATE TABLE {output} AS
        SELECT {colnames}
        FROM {tables}
        WHERE {where}
        '''.format(
            output=self.output().table,
            colnames=', '.join(colnames),
            tables=', '.join(tables),
            where=' AND '.join(where),
        )
        session.execute(create)


class ArchiveIPython(Task):

    timestamp = DateParameter(default=date.today())

    def run(self):
        self.output().makedirs()
        shell('tar -zcvf {output} --exclude=*.pdf --exclude=*.xml '
              '--exclude=*.gz --exclude=*.zip --exclude=*/tmp/* '
              'tmp/ipython'.format(
                  output=self.output().path))

    def output(self):
        return LocalTarget(os.path.join(
            'tmp', classpath(self), underscore_slugify(self.task_id) + '.gz'))


class BackupIPython(Task):

    timestamp = DateParameter(default=date.today())

    def requires(self):
        return ArchiveIPython(timestamp=self.timestamp)

    def run(self):
        shell('aws s3 cp {input} {output}'.format(
            input=self.input().path,
            output=self.output().path
        ))

    def output(self):
        path = 's3://cartodb-observatory-data/ipython/{}'.format(
            self.input().path.split(os.path.sep)[-1])
        print path
        return S3Target(path)


class GenerateRasterTiles(Task):

    table_id = Parameter()
    column_id = Parameter()

    force = BooleanParameter(default=False, significant=False)

    def run(self):
        self._ran = True
        session = current_session()
        try:
            resp = session.execute('''SELECT tablename, colname
                               FROM observatory.obs_table t,
                                    observatory.obs_column_table ct,
                                    observatory.obs_column c
                               WHERE c.type ILIKE 'geometry%'
                                 AND c.id = '{column_id}'
                                 AND t.id = '{table_id}'
                                 AND c.id = ct.column_id
                                 AND t.id = ct.table_id'''.format(
                                     column_id=self.column_id,
                                     table_id=self.table_id
                                 ))
            tablename, colname = resp.fetchone()
            LOGGER.info('table_id: %s, column_id: %s, tablename: %s, colname: %s',
                        self.table_id, self.column_id, tablename, colname)
            generate_tile_summary(session, self.table_id, self.column_id,
                                  'observatory.' + tablename, colname)
            session.commit()
        except:
            session.rollback()
            raise

    def complete(self):
        if self.force and not hasattr(self, '_ran'):
            return False
        session = current_session()
        resp = session.execute('''
            SELECT COUNT(*)
            FROM observatory.obs_column_table_tile
            WHERE table_id = '{table_id}'
              AND column_id = '{column_id}'
        '''.format(table_id=self.table_id, column_id=self.column_id))
        numrows = resp.fetchone()[0]
        return numrows > 0


class GenerateAllRasterTiles(WrapperTask):

    force = BooleanParameter(default=False, significant=False)

    def requires(self):
        session = current_session()
        resp = session.execute('''
            SELECT DISTINCT table_id, column_id
            FROM observatory.obs_table t,
                 observatory.obs_column_table ct,
                 observatory.obs_column c
            WHERE t.id = ct.table_id
              AND c.id = ct.column_id
              AND c.type ILIKE 'geometry%'
        ''')
        for table_id, column_id in resp:
            yield GenerateRasterTiles(table_id=table_id, column_id=column_id,
                                      force=self.force)


class MetaWrapper(WrapperTask):
    '''
    End-product wrapper for a set of tasks that should yield entries into the
    `obs_meta` table.
    '''

    params = {}

    def tables(self):
        raise NotImplementedError('''
          Must override `tables` with a function that yields TableTasks
                                  ''')

    def requires(self):
        for t in self.tables():
            assert isinstance(t, TableTask)
            yield t


def cross(orig_list, b_name, b_list):
    result = []
    for orig_dict in orig_list:
        for b_val in b_list:
            new_dict = orig_dict.copy()
            new_dict[b_name] = b_val
            result.append(new_dict)
    return result


class RunDiff(WrapperTask):
    '''
    Run MetaWrapper for all tasks that changed compared to master.
    '''

    compare = Parameter()

    def requires(self):
        resp = shell("git diff '{compare}' --name-only | grep '^tasks'".format(
            compare=self.compare
        ))
        for line in resp.split('\n'):
            if not line:
                continue
            module = line.replace('.py', '')
            LOGGER.info(module)
            for task_klass, params in collect_meta_wrappers(test_module=module, test_all=True):
                yield task_klass(**params)


def collect_tasks(task_klass, test_module=None):
    '''
    Returns a set of task classes whose parent is the passed `TaskClass`.

    Can limit to scope of tasks within module.
    '''
    tasks = set()
    for dirpath, _, files in os.walk('tasks'):
        for filename in files:
            if test_module:
                if not os.path.join(dirpath, filename).startswith(test_module):
                    continue
            if filename.endswith('.py'):
                modulename = '.'.join([
                    dirpath.replace(os.path.sep, '.'),
                    filename.replace('.py', '')
                ])
                module = importlib.import_module(modulename)
                for _, obj in inspect.getmembers(module):
                    if inspect.isclass(obj) and issubclass(obj, task_klass) and obj != task_klass:
                        if test_module and obj.__module__ != modulename:
                            continue
                        else:
                            tasks.add((obj, ))
    return tasks


def collect_requirements(task):
    requirements = task._requires()
    for r in requirements:
        requirements.extend(collect_requirements(r))
    return requirements


def collect_meta_wrappers(test_module=None, test_all=True):
    '''
    Yield MetaWrapper and associated params for every MetaWrapper that has been
    touched by changes in test_module.

    Does not collect meta wrappers that have been affected by a change in a
    superclass.
    '''
    affected_task_classes = set([t[0] for t in collect_tasks(Task, test_module)])

    for t, in collect_tasks(MetaWrapper):
        outparams = [{}]
        for key, val in t.params.iteritems():
            outparams = cross(outparams, key, val)
        req_types = None
        for params in outparams:
            # if the metawrapper itself is not affected, look at its requirements
            if t not in affected_task_classes:

                # generating requirements separately for each cross product is
                # too slow, just use the first one
                if req_types is None:
                    req_types = set([type(r) for r in collect_requirements(t(**params))])
                print 'checking {t} with {params}'.format(t=t, params=params)
                if not affected_task_classes.intersection(req_types):
                    continue
            yield t, params
            if not test_all:
                break
