from tests.util import runtask, setup, teardown

from tasks.util import TableTask

# Monkeypatch TableTask
TableTask._test = True

from tasks.carto import OBSMetaToLocal
from tasks.meta import current_session
from tasks.util import TagsTask, ColumnsTask, collect_meta_wrappers
from tasks.sphinx import Catalog

from nose.tools import with_setup, assert_greater

import os


@with_setup(setup, teardown)
def test_catalog():
    meta_wrappers = collect_meta_wrappers(
        test_module=os.environ.get('TEST_MODULE', '').replace('.', os.path.sep),
        test_all=os.environ.get('TEST_ALL', '') != ''
    )
    for klass, params in meta_wrappers:
        task = klass(**params)
        runtask(task, superclasses=[TagsTask, ColumnsTask, TableTask])

    runtask(OBSMetaToLocal())
    session = current_session()
    assert_greater(session.execute('SELECT COUNT(*) FROM observatory.obs_meta').fetchone()[0], 0,
      'No data in obs_meta to build catalog from.  Did you specify a module with no MetaWrapper classes?')
    runtask(Catalog(force=True))
