from tests.util import runtask, setup, teardown, collect_tasks

from tasks.util import TableTask
# Monkeypatch TableTask
TableTask._test = True

from tasks.carto import OBSMetaToLocal
from tasks.meta import current_session
from tasks.util import MetaWrapper, TagsTask, ColumnsTask

from nose_parameterized import parameterized
from nose.tools import assert_greater, with_setup


# {
#   'geo': ['state', 'zcta', 'tract'],
#   'table': ['B01', 'B02', 'B03', 'B04']
# }

# [{'geo': 'state', 'table': 'B01'},
#  {'geo': 'zcta',  'table': 'B01'},
#  {'geo': 'tract', 'table': 'B01'},
#  {'geo': 'state', 'table': 'B02'},
#  {'geo': 'zcta',  'table': 'B02'},
#  {'geo': 'tract', 'table': 'B02'},
#  {'geo': 'state', 'table': 'B03'},
#  {'geo': 'zcta',  'table': 'B03'},
#  {'geo': 'tract', 'table': 'B03'},
#  {'geo': 'state', 'table': 'B04'},
#  {'geo': 'zcta',  'table': 'B04'},
#  {'geo': 'tract', 'table': 'B04'}]


def cross(orig_list, b_name, b_list):
    result = []
    for orig_dict in orig_list:
        for b_val in b_list:
            new_dict = orig_dict.copy()
            new_dict[b_name] = b_val
            result.append(new_dict)
    return result


def collect_meta_wrappers():
    tasks = collect_tasks(MetaWrapper)
    for t, in tasks:
        outparams = [{}]
        for key, val in t.params.iteritems():
            outparams = cross(outparams, key, val)
        for params in outparams:
            yield t, params


@with_setup(setup, teardown)
@parameterized(collect_meta_wrappers())
def test_table_task(klass, params):

    task = klass(**params)

    runtask(task, superclasses=[TagsTask, ColumnsTask, TableTask])

    runtask(OBSMetaToLocal())

    session = current_session()
    assert_greater(session.execute('SELECT COUNT(*) FROM observatory.obs_meta').fetchone()[0], 0)
