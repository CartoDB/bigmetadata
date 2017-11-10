from tests.util import runtask, setup, teardown

from tasks.tasks import TableTask

# Monkeypatch TableTask
TableTask._test = True

from tasks.carto import OBSMetaToLocal
from tasks.tasks import ColumnsTask, TagsTask, collect_meta_wrappers
from tasks.sphinx import Catalog

from nose.tools import with_setup

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

    runtask(OBSMetaToLocal(force=True))
    runtask(Catalog(force=True))
