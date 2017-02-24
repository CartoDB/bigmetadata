from tests.util import runtask, setup, teardown, collect_tasks

from tasks.util import TableTask
# Monkeypatch TableTask
TableTask._test = True

from tasks.carto import OBSMetaToLocal
from tasks.meta import current_session
from tasks.util import MetaWrapper, TagsTask, ColumnsTask

from nose_parameterized import parameterized
from nose.tools import assert_greater, with_setup


@parameterized(collect_tasks(MetaWrapper))
@with_setup(setup, teardown)
def test_table_task(klass):

    task = klass()
    runtask(task, superclasses=[TagsTask, ColumnsTask, TableTask])

    runtask(OBSMetaToLocal())

    session = current_session()
    assert_greater(session.execute('SELECT COUNT(*) FROM observatory.obs_meta').fetchone()[0], 0)
