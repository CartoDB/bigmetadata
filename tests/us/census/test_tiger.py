'''
Test Tiger columns
'''

from tasks.util import shell

# TODO clean this up in a more general init script

try:
    shell('createdb test')
except:
    pass

from nose.tools import with_setup

from tasks.us.census.tiger import GeoidColumns, GeomColumns

from tests.util import runtask, setup, teardown


@with_setup(setup, teardown)
def test_geom_columns_run():
    runtask(GeomColumns())


@with_setup(setup, teardown)
def test_geoid_columns_run():
    runtask(GeoidColumns())

