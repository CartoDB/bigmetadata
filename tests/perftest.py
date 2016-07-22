from nose.tools import assert_equal, assert_is_not_none
from nose_parameterized import parameterized

from tasks.meta import current_session

import os
import re
import requests
from time import time

USE_SCHEMA = True

SESSION = current_session()


def query(q, **options):
    '''
    Query the account.  Returned is the response, wrapped by the requests
    library.
    '''
    return SESSION.execute(q)


#tmp_tablename = 'obs_censustest'
#'UPDATE untitled_table_3 SET measure = OBS_GetMeasureByID(name, 'us.census.acs.B01001002', 'us.census.tiger.block_group_clipped') WHERE cartodb_id < 11'

for q in (
    'DROP TABLE IF EXISTS obs_censustest',
    '''CREATE TABLE obs_censustest (cartodb_id SERIAL PRIMARY KEY,
       the_geom GEOMETRY, name TEXT, measure NUMERIC, category TEXT)''',
    '''INSERT INTO obs_censustest (the_geom, name)
       SELECT * FROM {schema}OBS_GetBoundariesByGeometry(
       st_makeenvelope(-74.05437469482422,40.66319159533881,-73.81885528564453,40.745696344339564, 4326),
       'us.census.tiger.block_group_clipped') As m(the_geom, geoid)'''
):
    query(q.format(
        schema='cdb_observatory.' if USE_SCHEMA else '',
    ))
    SESSION.commit()


ARGS = {
    'OBS_GetMeasureByID': "name, 'us.census.acs.B01001002', '{}'",
    'OBS_GetMeasure': "{}, 'us.census.acs.B01001002'",
    'OBS_GetCategory': "{}, 'us.census.spielman_singleton_segments.X10'",
}

GEOMS = {
    'point': 'ST_PointOnSurface(the_geom)',
    'polygon_match': 'the_geom',
    'polygon_buffered': 'ST_Buffer(the_geom::GEOGRAPHY, 1000)::GEOMETRY(GEOMETRY, 4326)',
}


@parameterized([
    ('OBS_GetMeasureByID', 'us.census.tiger.block_group_clipped'),
    ('OBS_GetMeasureByID', 'us.census.tiger.county'),
    ('OBS_GetMeasure', GEOMS['point']),
    ('OBS_GetMeasure', GEOMS['polygon_match']),
    ('OBS_GetMeasure', GEOMS['polygon_buffered']),
    ('OBS_GetCategory', GEOMS['point']),
    ('OBS_GetCategory', GEOMS['polygon_match']),
    ('OBS_GetCategory', GEOMS['polygon_buffered']),
])
def test_performance(api_method, arg):
    print api_method, arg
    col = 'measure' if 'measure' in api_method.lower() else 'category'
    for rows in (1, 10, 50, 100):
        q = 'UPDATE obs_censustest SET {col} = {schema}{api_method}({args}) WHERE cartodb_id < {n}'.format(
            col=col,
            schema='cdb_observatory.' if USE_SCHEMA else '',
            api_method=api_method,
            args=ARGS[api_method].format(arg),
            n=rows+1)
        start = time()
        resp = query(q)
        end = time()
        print rows, ': ', (rows / (end - start)), ' QPS'

