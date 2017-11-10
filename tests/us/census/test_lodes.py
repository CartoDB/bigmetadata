'''
Test ACS columns
'''

from tasks.util import shell

# TODO clean this up in a more general init script

try:
    shell('createdb test')
except:
    pass

from nose.tools import with_setup

from tasks.us.census.lodes import WorkplaceAreaCharacteristicsColumns

from tests.util import runtask, setup, teardown


@with_setup(setup, teardown)
def test_wac_columns_run():
    runtask(WorkplaceAreaCharacteristicsColumns())

