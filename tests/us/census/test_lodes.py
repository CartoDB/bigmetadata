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

from tasks.meta import (BMDColumnTable, BMDColumn, BMDColumnToColumn, BMDTable,
                        BMDTag, BMDColumnTag, Base, session_scope)

from tasks.us.census.lodes import WorkplaceAreaCharacteristicsColumns


def setup():
    Base.metadata.drop_all()
    Base.metadata.create_all()


def teardown():
    Base.metadata.drop_all()


@with_setup(setup, teardown)
def test_wac_columns_run():
    task = WorkplaceAreaCharacteristicsColumns()
    for dep in task.deps():
        dep.run()
    task.run()

