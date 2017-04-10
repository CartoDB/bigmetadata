from tests.util import runtask, setup, teardown

from tasks.util import TableTask

# Monkeypatch TableTask
TableTask._test = True

import tasks.carto
from tasks.meta import current_session
from tasks.util import TagsTask, ColumnsTask, collect_meta_wrappers

from nose_parameterized import parameterized
from nose.tools import assert_greater, with_setup

import os


@with_setup(setup, teardown)
@parameterized(collect_meta_wrappers(
    test_module=os.environ.get('TEST_MODULE', '').replace('.', os.path.sep),
    test_all=os.environ.get('TEST_ALL', '') != ''
))
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
