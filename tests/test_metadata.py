from tests.util import runtask, setup, teardown, collect_tasks

from tasks.util import TableTask

# Monkeypatch TableTask
TableTask._test = True

import tasks.carto
from tasks.meta import current_session
from tasks.util import MetaWrapper, TagsTask, ColumnsTask

from nose_parameterized import parameterized
from nose.tools import assert_greater, with_setup

import os


def cross(orig_list, b_name, b_list):
    result = []
    for orig_dict in orig_list:
        for b_val in b_list:
            new_dict = orig_dict.copy()
            new_dict[b_name] = b_val
            result.append(new_dict)
    return result


def collect_meta_wrappers():
    test_all = os.environ.get('TEST_ALL', '') != ''
    for t, in collect_tasks(MetaWrapper):
        outparams = [{}]
        for key, val in t.params.iteritems():
            outparams = cross(outparams, key, val)
        for params in outparams:
            yield t, params
            if not test_all:
                break


@with_setup(setup, teardown)
@parameterized(collect_meta_wrappers())
def test_table_task(klass, params):
    '''
    Test {} task with {}.

    This does not clear out all database artifacts between runs, for
    performance reasons.  The order of decorators could be switched to enforce
    cleaner behavior, but running all necessary ColumnsTasks repeatedly tends
    to be very slow.
    '''.format(klass, params)

    task = klass(**params)

    runtask(task, superclasses=[TagsTask, ColumnsTask, TableTask])

    reload(tasks.carto)
    obs_meta_to_local = tasks.carto.OBSMetaToLocal()

    runtask(obs_meta_to_local)

    session = current_session()
    assert_greater(session.execute('SELECT COUNT(*) FROM observatory.obs_meta').fetchone()[0], 0)

    session.execute('DROP TABLE observatory.obs_meta')
    session.execute('DELETE FROM observatory.obs_table')
    session.commit()
