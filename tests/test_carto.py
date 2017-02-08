from collections import OrderedDict
from luigi import Parameter
from nose.tools import (assert_equals, with_setup, assert_raises, assert_in,
                        assert_is_none, assert_true, assert_false)
from tests.util import runtask, setup, teardown, FakeTask

from tasks.meta import current_session

import tasks.carto


@with_setup(setup, teardown)
def test_empty_obs_meta_to_local():
    '''
    OBSMetaToLocal should work even if tables are empty.  Should result in
    creation of blank obs_meta, obs_meta_numer, obs_meta_denom, obs_meta_geom,
    obs_meta_timespan tables.
    '''
    reload(tasks.carto)
    task = tasks.carto.OBSMetaToLocal()
    runtask(task)
    session = current_session()
    assert_equals(session.execute('SELECT COUNT(*) FROM observatory.obs_meta').fetchone()[0], 0)
    assert_equals(session.execute('SELECT COUNT(*) FROM observatory.obs_meta_numer').fetchone()[0], 0)
    assert_equals(session.execute('SELECT COUNT(*) FROM observatory.obs_meta_denom').fetchone()[0], 0)
    assert_equals(session.execute('SELECT COUNT(*) FROM observatory.obs_meta_geom').fetchone()[0], 0)
    assert_equals(session.execute('SELECT COUNT(*) FROM observatory.obs_meta_timespan').fetchone()[0], 0)

@with_setup(setup, teardown)
def test_obs_meta_to_local_overwrites_changes():
    '''
    If OBSMetaToLocal is run when obs_meta* already exists, it replaces the
    existing data.
    '''
    reload(tasks.carto)
    task = tasks.carto.OBSMetaToLocal()
    runtask(task)
    session = current_session()
    session.commit()
    session.execute('''
                    INSERT INTO observatory.obs_meta (numer_id, geom_id, denom_id, numer_timespan)
                    VALUES ('foo', 'bar', 'baz', 'boo')
                    ''')
    session.execute("INSERT INTO observatory.obs_meta_numer (numer_id) VALUES ('foo')")
    session.execute("INSERT INTO observatory.obs_meta_geom (geom_id) VALUES ('bar')")
    session.execute("INSERT INTO observatory.obs_meta_denom (denom_id) VALUES ('baz')")
    session.execute("INSERT INTO observatory.obs_meta_timespan (timespan_id) VALUES ('boo')")
    session.commit()
    assert_equals(session.execute(
        'SELECT COUNT(*) FROM observatory.obs_meta').fetchone()[0], 1)
    assert_equals(session.execute(
        'SELECT COUNT(*) FROM observatory.obs_meta_numer').fetchone()[0], 1)
    assert_equals(session.execute(
        'SELECT COUNT(*) FROM observatory.obs_meta_denom').fetchone()[0], 1)
    assert_equals(session.execute(
        'SELECT COUNT(*) FROM observatory.obs_meta_geom').fetchone()[0], 1)
    assert_equals(session.execute(
        'SELECT COUNT(*) FROM observatory.obs_meta_timespan').fetchone()[0], 1)

    reload(tasks.carto)
    task = tasks.carto.OBSMetaToLocal()
    runtask(task)
    assert_equals(session.execute('SELECT COUNT(*) FROM observatory.obs_meta').fetchone()[0], 0)
    assert_equals(session.execute(
        'SELECT COUNT(*) FROM observatory.obs_meta').fetchone()[0], 0)
    assert_equals(session.execute(
        'SELECT COUNT(*) FROM observatory.obs_meta_numer').fetchone()[0], 0)
    assert_equals(session.execute(
        'SELECT COUNT(*) FROM observatory.obs_meta_denom').fetchone()[0], 0)
    assert_equals(session.execute(
        'SELECT COUNT(*) FROM observatory.obs_meta_geom').fetchone()[0], 0)
    assert_equals(session.execute(
        'SELECT COUNT(*) FROM observatory.obs_meta_timespan').fetchone()[0], 0)


@with_setup(setup, teardown)
def test_obs_meta_to_local_works_twice():
    '''
    Should be possible to run OBSMetaToLocal twice in a row.
    '''
    reload(tasks.carto)
    task = tasks.carto.OBSMetaToLocal()
    runtask(task)
    runtask(task)
    session = current_session()
    assert_equals(session.execute('SELECT COUNT(*) FROM observatory.obs_meta').fetchone()[0], 0)
    assert_equals(session.execute('SELECT COUNT(*) FROM observatory.obs_meta_numer').fetchone()[0], 0)
    assert_equals(session.execute('SELECT COUNT(*) FROM observatory.obs_meta_denom').fetchone()[0], 0)
    assert_equals(session.execute('SELECT COUNT(*) FROM observatory.obs_meta_geom').fetchone()[0], 0)
    assert_equals(session.execute('SELECT COUNT(*) FROM observatory.obs_meta_timespan').fetchone()[0], 0)
