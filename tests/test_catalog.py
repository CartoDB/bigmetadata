from tests.util import runtask, setup, teardown, collect_meta_wrappers

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
    session = current_session()
    assert_greater(session.execute('SELECT COUNT(*) FROM observatory.obs_meta').fetchone()[0], 0,
      'No data in obs_meta to build catalog from.  Did you specify a module with no MetaWrapper classes?')
    runtask(Catalog(force=True))
