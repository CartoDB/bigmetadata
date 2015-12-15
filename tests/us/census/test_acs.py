from nose.tools import assert_equals
from tasks.us.census.acs import ACSColumn


def test_acs_column_b00001001():
    col = ACSColumn(column_id='B00001001',
                    column_title='Total',
                    column_parent_path=[],
                    parent_column_id=None,
                    table_title='Unweighted Sample Count of the Population',
                    universe='Total Population',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, 'Unweighted Sample Count of the Population')


def test_acs_column_b00002001():
    col = ACSColumn(column_id='B00002001',
                    column_title='Total',
                    column_parent_path=[],
                    parent_column_id=None,
                    table_title='Unweighted Sample Housing Units',
                    universe='Housing Units',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, 'Unweighted Sample Housing Units')


def test_acs_column_b01001001():
    col = ACSColumn(column_id='B01001001',
                    column_title='Total:',
                    column_parent_path=[],
                    parent_column_id=None,
                    table_title='Sex by Age',
                    universe='Total Population',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, 'Total Population')


def test_acs_column_b01001002():
    col = ACSColumn(column_id='B01001002',
                    column_title='Male:',
                    column_parent_path=['Total:'],
                    parent_column_id=None,
                    table_title='Sex by Age',
                    universe='Total Population',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, 'Male Population')


def test_acs_column_b01001003():
    col = ACSColumn(column_id='B01001003',
                    column_title='Under 5 years',
                    column_parent_path=['Total:', 'Male:'],
                    parent_column_id=None,
                    table_title='Sex by Age',
                    universe='Total Population',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, 'Male Population')


