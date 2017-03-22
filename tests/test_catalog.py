from tests.util import runtask, setup, teardown
from tests.test_metadata import collect_meta_wrappers

from tasks.carto import OBSMetaToLocal
from tasks.meta import current_session
from tasks.util import MetaWrapper, TagsTask, ColumnsTask, TableTask
from tasks.sphinx import Catalog

from nose_parameterized import parameterized
from nose.tools import assert_greater, with_setup


@with_setup(setup, teardown)
def test_catalog():

    for klass, params in collect_meta_wrappers():
        task = klass(**params)
        runtask(task, superclasses=[TagsTask, ColumnsTask, TableTask])

    runtask(OBSMetaToLocal())
    runtask(Catalog(force=True))
