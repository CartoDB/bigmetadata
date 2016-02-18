'''
Test metadata functions
'''

from nose.tools import assert_equals, with_setup
from tasks.meta import (BMDColumnTable, BMDColumn, BMDColumnToColumn, BMDTable,
                        BMDTag, BMDColumnTag, Base)
from tasks.util import session_scope


def setup():
    Base.metadata.drop_all()
    Base.metadata.create_all()
    with session_scope() as session:
        datacols = {
            'median_rent': BMDColumn(id='"us.census.acs".median_rent', type='numeric'),
            'total_pop': BMDColumn(id='"us.census.acs".total_pop', type='numeric'),
            'male_pop': BMDColumn(id='"us.census.acs".male_pop', type='numeric'),
            'female_pop': BMDColumn(id='"us.census.acs".female_pop', type='numeric'),
        }
        tract_geoid = BMDColumn(id='"us.census.acs".tract_2013_geoid', type='text')
        puma_geoid = BMDColumn(id='"us.census.acs".puma_2013_geoid', type='text')
        tables = {
            'tract': BMDTable(id='"us.census.acs".extract_2013_5yr_tract',
                              tablename='us_census_acs2013_5yr_tract'),
            'puma': BMDTable(id='"us.census.acs".extract_2013_5yr_puma',
                             tablename='us_census_acs2013_5yr_puma')
        }
        session.add(BMDColumnTable(table=tables['tract'],
                                   column=tract_geoid,
                                   colname='geoid'))
        session.add(BMDColumnTable(table=tables['puma'],
                                   column=puma_geoid,
                                   colname='geoid'))
        for colname, datacol in datacols.iteritems():
            for table in tables.values():
                coltable = BMDColumnTable(column=datacol,
                                          table=table,
                                          colname=colname)
                session.add(coltable)
            session.add(datacol)
        for table in tables.values():
            session.add(table)


def teardown():
    Base.metadata.drop_all()


@with_setup(setup, teardown)
def test_columns_in_tables():
    '''
    Tables can refer to columns.
    '''
    with session_scope() as session:
        table = session.query(BMDTable).get('"us.census.acs".extract_2013_5yr_puma')
        assert_equals(5, len(table.columns))


@with_setup(setup, teardown)
def test_tables_in_columns():
    '''
    Columns can refer to tables.
    '''
    with session_scope() as session:
        column = session.query(BMDColumn).get('"us.census.acs".median_rent')
        assert_equals(2, len(column.tables))


@with_setup(setup, teardown)
def test_tags_in_columns():
    '''
    Columns can refer to tags.
    '''
    pass


@with_setup(setup, teardown)
def test_columns_in_tags():
    '''
    Tags can refer to columns.
    '''
    pass


@with_setup(setup, teardown)
def test_column_to_column():
    '''
    Columns can refer to other columns.
    '''
    pass
