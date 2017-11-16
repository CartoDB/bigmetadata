'''
Util functions for luigi bigmetadata tasks.
'''
import os
import time
import re
import subprocess

from lib.logger import get_logger

from itertools import zip_longest

from slugify import slugify
import requests

OBSERVATORY_PREFIX = 'obs_'
OBSERVATORY_SCHEMA = 'observatory'

LOGGER = get_logger(__name__)


def shell(cmd, encoding='utf-8'):
    '''
    Run a shell command, uses :py:func:`subprocess.check_output(cmd,
    shell=True)` under the hood.

    Returns the ``STDOUT`` output, and raises an error if there is a
    none-zero exit code.
    '''
    try:
        return subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT).decode(encoding)
    except subprocess.CalledProcessError as err:
        LOGGER.error(err.output)
        raise


# https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-camel-case
def camel_to_underscore(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


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
    >>> from tasks.base_tasks import ColumnsTask
    >>> classpath(ColumnsTask())
    'util'
    >>> from tasks.es.ine import FiveYearPopulation
    >>> classpath(FiveYearPopulation())
    'es.ine'
    '''
    classpath_ = '.'.join(obj.__module__.split('.')[1:])
    return classpath_ if classpath_ else 'tmp'


def unqualified_task_id(task_id):
    '''
    Returns the name of the task from the task_id.
    '''
    return task_id.split('.')[-1]


def query_cartodb(query, carto_url=None, api_key=None):
    '''
    Convenience function to query CARTO's SQL API with an arbitrary SQL string.
    The account connected via ``.env`` is queried.

    Returns the raw ``Response`` object.  Will not raise an exception in case
    of non-200 response.

    :param query: The query to execute on CARTO.
    '''
    carto_url = (carto_url or os.environ['CARTODB_URL']) + '/api/v2/sql'
    resp = requests.post(carto_url, data={
        'api_key': api_key or os.environ['CARTODB_API_KEY'],
        'q': query
    })
    return resp


def upload_via_ogr2ogr(outname, localname, schema, api_key=None):
    api_key = api_key or os.environ['CARTODB_API_KEY']
    cmd = '''
ogr2ogr --config CARTODB_API_KEY {api_key} \
        -f CartoDB "CartoDB:observatory" \
        -overwrite \
        -nlt GEOMETRY \
        -nln "{private_outname}" \
        PG:dbname=$PGDATABASE' active_schema={schema}' '{tablename}'
    '''.format(private_outname=outname, tablename=localname,
               schema=schema, api_key=api_key)
    print(cmd)
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
        print(resp.json()['state'])
        time.sleep(1)

    # if failing below, try reloading https://observatory.cartodb.com/dashboard/datasets
    assert resp.json()['table_name'] == request['table_name']  # the copy should not have a
                                                               # mutilated name (like '_1', '_2' etc)

    for colname in json_column_names:
        query = 'ALTER TABLE {outname} ALTER COLUMN {colname} ' \
                'SET DATA TYPE json USING NULLIF({colname}, '')::json'.format(
                    outname=resp.json()['table_name'], colname=colname
                )
        print(query)
        resp = query_cartodb(query)
        assert resp.status_code == 200


def sql_to_cartodb_table(outname, localname, json_column_names=None,
                         schema='observatory'):
    '''
    Move a table to CARTO using the
    `import API <https://carto.com/docs/carto-engine/import-api/importing-geospatial-data/>`_

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

    print('copying via import api')
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
                        WHEN ST_GeometryType({colname}) IN ('ST_Polygon', 'ST_MultiPolygon') THEN
                             ST_CollectionExtract(ST_MakeValid(
                             ST_SimplifyVW({colname}, 0.00001)), 3)
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
               SUM(CASE WHEN ST_Within(geom, vector.the_geom) THEN 1
                    WHEN ST_Within(vector.the_geom, geom) THEN
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
         -- regenerate band 2 setting 1 as the minimum value to include very small geometries
         -- that would be considered spurious due to rounding
         ROW(2, '[0-0]::0, (0-1]::1-255, (1-100]::255-255', '8BUI', 0)::reclassarg
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


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)


def create_temp_schema(task):
    shell("psql -c 'CREATE SCHEMA IF NOT EXISTS \"{schema}\"'".format(schema=classpath(task)))
