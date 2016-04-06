'''
Test ACS columns
'''

from tasks.util import shell

# TODO clean this up in a more general init script

try:
    shell('createdb test')
except:
    pass

from nose.tools import assert_equals, with_setup, assert_false, assert_true

from tasks.meta import (OBSColumnTable, OBSColumn, OBSColumnToColumn, OBSTable,
                        OBSTag, OBSColumnTag, Base, current_session)
from tasks.us.census.acs import Columns, Extract

from tests.util import runtask, setup, teardown


@with_setup(setup, teardown)
def test_acs_columns_run():
    task = Columns()
    assert_equals(False, task.complete())
    assert_equals(0, len(current_session().dirty))
    worker = runtask(task)
    assert_equals(True, task.complete())
    assert_equals(True, worker.run_succeeded)
    assert_equals(0, len(current_session().dirty))
    #import pdb
    #pdb.set_Trace()
    #assert_equals(False, results.empty())
