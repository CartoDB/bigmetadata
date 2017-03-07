from tests.util import runtask, setup, teardown, collect_meta_wrappers

from tasks.util import TableTask

# Monkeypatch TableTask
TableTask._test = True

from tasks.carto import OBSMetaToLocal
from tasks.meta import current_session
from tasks.util import MetaWrapper, TagsTask, ColumnsTask

from nose_parameterized import parameterized
from nose.tools import assert_greater, with_setup


def cross(orig_list, b_name, b_list):
    result = []
    for orig_dict in orig_list:
        for b_val in b_list:
            new_dict = orig_dict.copy()
            new_dict[b_name] = b_val
            result.append(new_dict)
    return result


@with_setup(setup, teardown)
@parameterized(collect_meta_wrappers())
def test_table_task(klass, params):

    task = klass(**params)

    runtask(task, superclasses=[TagsTask, ColumnsTask, TableTask])

    runtask(OBSMetaToLocal())

    session = current_session()
    assert_greater(session.execute('SELECT COUNT(*) FROM observatory.obs_meta').fetchone()[0], 0)
