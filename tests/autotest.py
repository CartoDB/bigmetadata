from nose.tools import assert_equal, assert_is_not_none
from nose_parameterized import parameterized

from tasks.meta import current_session

import os
import re
import requests

#HOSTNAME = os.environ['OBS_HOSTNAME']
#API_KEY = os.environ['OBS_API_KEY']
#META_HOSTNAME = os.environ.get('OBS_META_HOSTNAME', HOSTNAME)
#META_API_KEY = os.environ.get('OBS_META_API_KEY', API_KEY)
#USE_SCHEMA = 'OBS_USE_SCHEMA' in os.environ

USE_SCHEMA = True

SESSION = current_session()

def query(q, **options):
    '''
    Query the account.  Returned is the response, wrapped by the requests
    library.
    '''
    #url = 'https://{hostname}/api/v2/sql'.format(
    #    hostname=META_HOSTNAME if is_meta else HOSTNAME)
    #params = options.copy()
    #params['q'] = re.sub(r'\s+', ' ', q)
    #params['api_key'] = META_API_KEY if is_meta else API_KEY
    #return requests.get(url, params=params)
    return SESSION.execute(q)

MEASURE_COLUMNS = [(r['numer_id'], r['point_only'], ) for r in query('''
SELECT distinct numer_id, numer_aggregate NOT ILIKE 'sum' as point_only
FROM observatory.obs_meta
WHERE numer_type ILIKE 'numeric'
AND numer_weight > 0
''').fetchall()]

CATEGORY_COLUMNS = [(r['numer_id'], ) for r in query('''
SELECT distinct numer_id
FROM observatory.obs_meta
WHERE numer_type ILIKE 'text'
AND numer_weight > 0
''').fetchall()]

BOUNDARY_COLUMNS = [(r['id'], ) for r in query('''
SELECT id FROM observatory.obs_column
WHERE type ILIKE 'geometry'
AND weight > 0
''', is_meta=True).fetchall()]

US_CENSUS_MEASURE_COLUMNS = [(r['numer_name'], ) for r in query('''
SELECT distinct numer_name
FROM observatory.obs_meta
WHERE numer_type ILIKE 'numeric'
AND 'us.census.acs.acs' = ANY (subsection_tags)
AND numer_weight > 0
''').fetchall()]


def default_geometry_id(column_id):
    '''
    Returns default test point for the column_id.
    '''
    if column_id == 'whosonfirst.wof_disputed_geom':
        return 'ST_SetSRID(ST_MakePoint(76.57, 33.78), 4326)'
    elif column_id == 'whosonfirst.wof_marinearea_geom':
        return 'ST_SetSRID(ST_MakePoint(-68.47, 43.33), 4326)'
    elif column_id in ('us.census.tiger.school_district_elementary',
                       'us.census.tiger.school_district_secondary',
                       'us.census.tiger.school_district_elementary_clipped',
                       'us.census.tiger.school_district_secondary_clipped'):
        return 'ST_SetSRID(ST_MakePoint(-73.7067, 40.7025), 4326)'
    elif column_id.startswith('es.ine'):
        return 'ST_SetSRID(ST_MakePoint(-2.51141249535454, 42.8226119029222), 4326)'
    elif column_id.startswith('us.zillow'):
        return 'ST_SetSRID(ST_MakePoint(-81.3544048197256, 28.3305906291771), 4326)'
    else:
        return 'ST_SetSRID(ST_MakePoint(-73.9, 40.7), 4326)'


def default_point(column_id):
    '''
    Returns default test point for the column_id.
    '''
    if column_id == 'whosonfirst.wof_disputed_geom':
        return 'ST_SetSRID(ST_MakePoint(76.57, 33.78), 4326)'
    elif column_id == 'whosonfirst.wof_marinearea_geom':
        return 'ST_SetSRID(ST_MakePoint(-68.47, 43.33), 4326)'
    elif column_id in ('us.census.tiger.school_district_elementary',
                       'us.census.tiger.school_district_secondary',
                       'us.census.tiger.school_district_elementary_clipped',
                       'us.census.tiger.school_district_secondary_clipped'):
        return 'ST_SetSRID(ST_MakePoint(-73.7067, 40.7025), 4326)'
    elif column_id.startswith('uk'):
        if 'WA' in column_id:
            return 'ST_SetSRID(ST_MakePoint(-3.184833526611328, 51.46844551219723), 4326)'
        else:
            return 'ST_SetSRID(ST_MakePoint(-0.08883476257324219, 51.51461834694225), 4326)'
    elif column_id.startswith('es'):
        return 'ST_SetSRID(ST_MakePoint(-2.51141249535454, 42.8226119029222), 4326)'
    elif column_id.startswith('us.zillow'):
        return 'ST_SetSRID(ST_MakePoint(-81.3544048197256, 28.3305906291771), 4326)'
    elif column_id.startswith('mx.'):
        return 'ST_SetSRID(ST_MakePoint(-99.17019367218018, 19.41347699386547), 4326)'
    else:
        return 'ST_SetSRID(ST_MakePoint(-73.9, 40.7), 4326)'


def default_area(column_id):
    '''
    Returns default test area for the column_id
    '''
    point = default_point(column_id)
    area = 'ST_Transform(ST_Buffer(ST_Transform({point}, 3857), 1000), 4326)'.format(
        point=point)
    return area

@parameterized(US_CENSUS_MEASURE_COLUMNS)
def test_get_us_census_measure_points(name):
    print 'test_get_us_census_measure_points, ', name
    resp = query('''
SELECT * FROM {schema}OBS_GetUSCensusMeasure({point}, '{name}')
                 '''.format(name=name.replace("'", "''"),
                            schema='cdb_observatory.' if USE_SCHEMA else '',
                            point=default_point('')))
    rows = resp.fetchall()
    assert_equal(1, len(rows))
    assert_is_not_none(rows[0].values()[0])


@parameterized(MEASURE_COLUMNS)
def test_get_measure_areas(column_id, point_only):
    print 'test_get_measure_areas, ', column_id, point_only
    if point_only:
        return
    resp = query('''
SELECT * FROM {schema}OBS_GetMeasure({area}, '{column_id}')
                 '''.format(column_id=column_id,
                            schema='cdb_observatory.' if USE_SCHEMA else '',
                            area=default_area(column_id)))
    rows = resp.fetchall()
    assert_equal(1, len(rows))
    assert_is_not_none(rows[0].values()[0])


@parameterized(MEASURE_COLUMNS)
def test_get_measure_points(column_id, point_only):
    print 'test_get_measure_points, ', column_id, point_only
    resp = query('''
SELECT * FROM {schema}OBS_GetMeasure({point}, '{column_id}')
                 '''.format(column_id=column_id,
                            schema='cdb_observatory.' if USE_SCHEMA else '',
                            point=default_point(column_id)))
    rows = resp.fetchall()
    assert_equal(1, len(rows))
    assert_is_not_none(rows[0].values()[0])

#@parameterized(CATEGORY_COLUMNS)
#def test_get_category_areas(column_id):
#    resp = query('''
#SELECT * FROM {schema}OBS_GetCategory({area}, '{column_id}')
#                 '''.format(column_id=column_id,
#                            schema='cdb_observatory.' if USE_SCHEMA else '',
#                            area=default_area(column_id)))
#    assert_equal(resp.status_code, 200)
#    rows = resp.json()['rows']
#    assert_equal(1, len(rows))
#    assert_is_not_none(rows[0].values()[0])

@parameterized(CATEGORY_COLUMNS)
def test_get_category_points(column_id):
    print 'test_get_category_points, ', column_id
    resp = query('''
SELECT * FROM {schema}OBS_GetCategory({point}, '{column_id}')
                 '''.format(column_id=column_id,
                            schema='cdb_observatory.' if USE_SCHEMA else '',
                            point=default_point(column_id)))
    rows = resp.fetchall()
    assert_equal(1, len(rows))
    assert_is_not_none(rows[0].values()[0])

#@parameterized(BOUNDARY_COLUMNS)
#def test_get_boundaries_by_geometry(column_id):
#    resp = query('''
#SELECT * FROM {schema}OBS_GetBoundariesByGeometry({area}, '{column_id}')
#                 '''.format(column_id=column_id,
#                            schema='cdb_observatory.' if USE_SCHEMA else '',
#                            area=default_area(column_id)))
#    assert_equal(resp.status_code, 200)
#    rows = resp.json()['rows']
#    assert_equal(1, len(rows))
#    assert_is_not_none(rows[0].values()[0])

#@parameterized(BOUNDARY_COLUMNS)
#def test_get_points_by_geometry(column_id):
#    resp = query('''
#SELECT * FROM {schema}OBS_GetPointsByGeometry({area}, '{column_id}')
#                 '''.format(column_id=column_id,
#                            schema='cdb_observatory.' if USE_SCHEMA else '',
#                            area=default_area(column_id)))
#    assert_equal(resp.status_code, 200)
#    rows = resp.json()['rows']
#    assert_equal(1, len(rows))
#    assert_is_not_none(rows[0].values()[0])

#@parameterized(BOUNDARY_COLUMNS)
#def test_get_boundary_points(column_id):
#    resp = query('''
#SELECT * FROM {schema}OBS_GetBoundary({point}, '{column_id}')
#                 '''.format(column_id=column_id,
#                            schema='cdb_observatory.' if USE_SCHEMA else '',
#                            point=default_point(column_id)))
#    assert_equal(resp.status_code, 200)
#    rows = resp.json()['rows']
#    assert_equal(1, len(rows))
#    assert_is_not_none(rows[0].values()[0])

#@parameterized(BOUNDARY_COLUMNS)
#def test_get_boundary_id(column_id):
#    resp = query('''
#SELECT * FROM {schema}OBS_GetBoundaryId({point}, '{column_id}')
#                 '''.format(column_id=column_id,
#                            schema='cdb_observatory.' if USE_SCHEMA else '',
#                            point=default_point(column_id)))
#    assert_equal(resp.status_code, 200)
#    rows = resp.json()['rows']
#    assert_equal(1, len(rows))
#    assert_is_not_none(rows[0].values()[0])

#@parameterized(BOUNDARY_COLUMNS)
#def test_get_boundary_by_id(column_id):
#    resp = query('''
#SELECT * FROM {schema}OBS_GetBoundaryById({geometry_id}, '{column_id}')
#                 '''.format(column_id=column_id,
#                            schema='cdb_observatory.' if USE_SCHEMA else '',
#                            geometry_id=default_geometry_id(column_id)))
#    assert_equal(resp.status_code, 200)
#    rows = resp.json()['rows']
#    assert_equal(1, len(rows))
#    assert_is_not_none(rows[0].values()[0])
