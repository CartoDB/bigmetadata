'''
Test ACS columns
'''

#from tasks.util import shell
#
## TODO clean this up in a more general init script
#try:
#    shell('createdb test')
#except:
#    pass
#
#from nose.tools import assert_equals, with_setup, assert_false, assert_true
#
#from tasks.meta import (BMDColumnTable, BMDColumn, BMDColumnToColumn, BMDTable,
#                        BMDTag, BMDColumnTag, Base, session_scope)
#from tasks.us.census.acs import Columns, Extract
#from tasks.us.census.tiger import GeoidColumns


#def setup():
#    Base.metadata.drop_all()
#    Base.metadata.create_all()
#
#
#def teardown():
#    Base.metadata.drop_all()
#    Base.metadata.clear()
#
#
#@with_setup(setup, teardown)
#def test_acs_extract_columns():
#    geography = 'state'
#    extract = Extract(year=2013, sample='1yr', geography=geography)
#
#    # TODO assert every column from here is a columntable, with a column
#    # pointing to an ACS_COLUMNS column
#    columns = extract.columns()
#    for col in columns:
#        assert_true(isinstance(col, BMDColumnTable))
#        if col.colname != 'geoid':
#            assert_true(hasattr(Columns, col.colname))
#            assert_equals(getattr(Columns, col.colname), col.column)
#        else:
#            assert_equals(getattr(GeoidColumns, geography), col.column)
#
#    # this doesn't raise any exception to run twice
#    columns = extract.columns()
#
#
#@with_setup(setup, teardown)
#def test_acs_extract_output():
#    '''
#    ACS extract should create an output table based off of columns
#    '''
#    geography = 'state'
#    extract = Extract(year=2013, sample='1yr', geography=geography)
#    output = extract.output()
#    with session_scope() as session:
#        assert_equals(session.query(BMDTable).count(), 0)
#        assert_equals(session.query(BMDColumn).count(), 0)
#        assert_equals(session.query(BMDColumnTable).count(), 0)
#    with session_scope() as session:
#        output.create(session)
#    with session_scope() as session:
#        assert_equals(session.query(BMDTable).count(), 1)
#        assert_true(session.query(BMDColumn).count() > 0)
#        assert_true(session.query(BMDColumnTable).count() > 0)
#    # Can run twice without error
#    output = extract.output()
#
#
#@with_setup(setup, teardown)
#def test_acs_extract_output_2():
#    '''
#    Can do this a second time without crashing
#    '''
#    geography = 'state'
#    extract = Extract(year=2013, sample='1yr', geography=geography)
#    output = extract.output()
#    with session_scope() as session:
#        assert_equals(session.query(BMDTable).count(), 0)
#        assert_equals(session.query(BMDColumn).count(), 0)
#        assert_equals(session.query(BMDColumnTable).count(), 0)
#    with session_scope() as session:
#        output.create(session)
#    with session_scope() as session:
#        assert_equals(session.query(BMDTable).count(), 1)
#        assert_true(session.query(BMDColumn).count() > 0)
#        assert_true(session.query(BMDColumnTable).count() > 0)
#    # Can run twice without error
#    output = extract.output()
#
#
#@with_setup(setup, teardown)
#def test_acs_extract_run():
#    geography = 'state'
#    extract = Extract(year=2013, sample='1yr', geography=geography)
#    with session_scope() as session:
#        session.execute('CREATE SCHEMA IF NOT EXISTS acs2013_1yr')
#    extract.run()

#@with_setup(setup, teardown)
#def test_acs_extract_target():
#    extract = Extract(year=2013, sample='1yr', geography='state')
#    target = extract.output()
#    assert_false(target.exists())
#    target.create()
#    assert_true(target.exists())


#from nose.tools import assert_equals
#from tasks.us.census.acs import ACSColumn
#
#
#def test_acs_column_b00001001():
#    col = ACSColumn(column_id='B00001001',
#                    table_id='B00001',
#                    column_title='Total',
#                    column_parent_path=[],
#                    parent_column=None,
#                    is_denominator=False,
#                    table_title='Unweighted Sample Count of the Population',
#                    universe='Total Population',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, 'Unweighted Sample Count of the Population')
#
#
#def test_acs_column_b00002001():
#    col = ACSColumn(column_id='B00002001',
#                    table_id='B00002',
#                    column_title='Total',
#                    column_parent_path=[],
#                    is_denominator=False,
#                    parent_column=None,
#                    table_title='Unweighted Sample Housing Units',
#                    universe='Housing Units',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, 'Unweighted Sample Housing Units')
#
#
#def test_acs_column_b01001001():
#    col = ACSColumn(column_id='B01001001',
#                    table_id='B01001',
#                    column_title='Total:',
#                    column_parent_path=[],
#                    is_denominator=True,
#                    parent_column=None,
#                    table_title='Sex by Age',
#                    universe='Total Population',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, 'Total Population')
#
#
#def test_acs_column_b01001002():
#    col = ACSColumn(column_id='B01001002',
#                    table_id='B01001',
#                    column_title='Male:',
#                    is_denominator=False,
#                    column_parent_path=['Total:'],
#                    parent_column=None,
#                    table_title='Sex by Age',
#                    universe='Total Population',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, 'Male Population')
#
#
#def test_acs_column_b01001003():
#    col = ACSColumn(column_id='B01001003',
#                    table_id='B01001',
#                    column_title='Under 5 years',
#                    is_denominator=False,
#                    column_parent_path=['Total:', 'Male:'],
#                    parent_column=None,
#                    table_title='Sex by Age',
#                    universe='Total Population',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, 'Under 5 years Male Population')
#
#
#def test_acs_column_b01001026():
#    col = ACSColumn(column_id='B01001026',
#                    table_id='B01001',
#                    column_title='Female:',
#                    is_denominator=False,
#                    column_parent_path=['Total:'],
#                    parent_column=None,
#                    table_title='Sex by Age',
#                    universe='Total Population',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, 'Female Population')
#
#
#def test_acs_column_b03002012():
#    col = ACSColumn(column_id='B03002012',
#                    table_id='B03002',
#                    column_title='Hispanic or Latino:',
#                    is_denominator=False,
#                    column_parent_path=['Total:'],
#                    parent_column='B03002001',
#                    table_title='Hispanic or Latino Origin by Race',
#                    universe='Total Population',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, 'Hispanic or Latino Population')
#
#
#def test_acs_column_b03002006():
#    col = ACSColumn(column_id='B03002006',
#                    table_id='B03002',
#                    is_denominator=False,
#                    column_title='Asian alone',
#                    column_parent_path=['Total:', 'Not Hispanic or Latino:'],
#                    parent_column='B03002002',
#                    table_title='Hispanic or Latino Origin by Race',
#                    universe='Total Population',
#                    denominator='B03002001',
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, 'Asian Population')
#
#
#def test_acs_column_b03002004():
#    col = ACSColumn(column_id='B03002004',
#                    table_id='B03002',
#                    is_denominator=False,
#                    column_title='Black or African American alone',
#                    column_parent_path=['Total:', 'Not Hispanic or Latino:'],
#                    parent_column='B03002002',
#                    table_title='Hispanic or Latino Origin by Race',
#                    universe='Total Population',
#                    denominator='B03002001',
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, 'Black or African American Population')
#
#
#def test_acs_column_b03002003():
#    col = ACSColumn(column_id='B03002003',
#                    table_id='B03002',
#                    is_denominator=False,
#                    column_title='White alone',
#                    column_parent_path=['Total:', 'Not Hispanic or Latino:'],
#                    parent_column='B03002002',
#                    table_title='Hispanic or Latino Origin by Race',
#                    universe='Total Population',
#                    denominator='B03002001',
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, 'White Population')
#
#
#def test_acs_column_b09001001():
#    col = ACSColumn(column_id='B09001001',
#                    table_id='B09001',
#                    is_denominator=True,
#                    column_title='Total:',
#                    column_parent_path=[],
#                    parent_column=None,
#                    table_title='Population Under 18 Years by Age',
#                    universe='Population Under 18 Years',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, 'Population Under 18 Years')
#
#
#def test_acs_column_b09020001():
#    col = ACSColumn(column_id='B09020001',
#                    table_id='B09020',
#                    is_denominator=True,
#                    column_title='Total:',
#                    column_parent_path=[],
#                    parent_column=None,
#                    table_title='Relationship by Household Type (Including '
#                                'Living Alone) for the Population 65 Years and Over',
#                    universe='Population 65 Years and Over',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, 'Population 65 Years and Over')
#
#
#def test_acs_column_b11001001():
#    col = ACSColumn(column_id='B11001001',
#                    table_id='B11001',
#                    is_denominator=True,
#                    column_title='Total:',
#                    column_parent_path=[],
#                    parent_column=None,
#                    table_title='Household Type (Including Living Alone)',
#                    universe='Households',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, 'Households')
#
#
#def test_acs_column_b14001002():
#    col = ACSColumn(column_id='B14001002',
#                    table_id='B14001',
#                    is_denominator=False,
#                    column_title='Enrolled in school:',
#                    column_parent_path=['Total:'],
#                    parent_column=None,
#                    table_title='School Enrollment by Level of School for the '
#                                'Population 3 Years and Over',
#                    universe='Population 3 Years and Over',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, 'Enrolled in school Population 3 Years and Over')
#
#def test_acs_column_b14001002():
#    col = ACSColumn(column_id='B14001008',
#                    table_id='B14001',
#                    is_denominator=False,
#                    column_title='Enrolled in college, undergraduate years',
#                    column_parent_path=['Total:', 'Enrolled in school:'],
#                    parent_column=None,
#                    table_title='School Enrollment by Level of School for the '
#                                'Population 3 Years and Over',
#                    universe='Population 3 Years and Over',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, 'Enrolled in college, undergraduate years in Population 3 Years and Over')
#
#
#def test_acs_column_b15003017():
#    col = ACSColumn(column_id='B15003017',
#                    table_id='B15003',
#                    is_denominator=False,
#                    column_title='Regular high school diploma',
#                    column_parent_path=['Total:'],
#                    parent_column='B15003001',
#                    table_title='Educational Attainment for the Population 25 Years and Over',
#                    universe='Population 25 Years and Over',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, "Regular high school diploma in Population 25 Years and Over")
#
#
#def test_acs_column_b15003022():
#    col = ACSColumn(column_id='B15003022',
#                    table_id='B15003',
#                    is_denominator=False,
#                    column_title='Bachelor\'s degree',
#                    column_parent_path=['Total:'],
#                    parent_column='B15003001',
#                    table_title='Educational Attainment for the Population 25 Years and Over',
#                    universe='Population 25 Years and Over',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, "Bachelor's degree in Population 25 Years and Over")
#
#
#def test_acs_column_b17001002():
#    col = ACSColumn(column_id='B17001002',
#                    table_id='B17001',
#                    is_denominator=False,
#                    column_title='Income in the past 12 months below poverty level:',
#                    column_parent_path=['Total:'],
#                    parent_column='B17001001',
#                    table_title='Poverty Status in the Past 12 Months by Sex by Age',
#                    universe='Population for Whom Poverty Status Is Determined',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, "Income in the past 12 months below poverty level in Population for Whom Poverty Status Is Determined")
#
#
#def test_acs_column_b19013001():
#    col = ACSColumn(column_id='B19013001',
#                    table_id='B19013',
#                    is_denominator=True,
#                    column_title='Median household income in the past 12 months (in 2012 inflation-adjusted dollars)',
#                    column_parent_path=[],
#                    parent_column=None,
#                    table_title='Median Household Income in the Past 12 Months (In 2012 Inflation-adjusted Dollars)',
#                    universe='Households',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, "Median household income in the past 12 months (in 2012 inflation-adjusted dollars)")
#
#
#def test_acs_column_b22003002():
#    col = ACSColumn(column_id='B22003002',
#                    table_id='B22003',
#                    is_denominator=False,
#                    column_title='Household received Food Stamps/SNAP in the past 12 months:',
#                    column_parent_path=['Total:'],
#                    parent_column='B22003001',
#                    table_title='Receipt of Food Stamps/SNAP in the Past 12 Months by Poverty Status in the Past 12 Months for Households',
#                    universe='Households',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, "Household received Food Stamps/SNAP in the past 12 months")
#
#
#def test_acs_column_b23025002():
#    col = ACSColumn(column_id='B23025002',
#                    table_id='B23022',
#                    is_denominator=False,
#                    column_title='In labor force:',
#                    column_parent_path=['Total:'],
#                    parent_column='B23025001',
#                    table_title='Employment Status for the Population 16 Years and Over',
#                    universe='Population 16 Years and Over',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, "In labor force in Population 16 Years and Over")
#
#
#def test_acs_column_b23025003():
#    col = ACSColumn(column_id='B23025003',
#                    table_id='B23022',
#                    is_denominator=False,
#                    column_title='Civilian labor force:',
#                    column_parent_path=['Total:', 'In labor force:'],
#                    parent_column='B23025002',
#                    table_title='Employment Status for the Population 16 Years and Over',
#                    universe='Population 16 Years and Over',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, "Civilian labor force in Population 16 Years and Over")
#
#
#def test_acs_column_b23025005():
#    col = ACSColumn(column_id='B23025005',
#                    table_id='B23022',
#                    is_denominator=False,
#                    column_title='Unemployed',
#                    column_parent_path=['Total:', 'In labor force:', 'Unemployed', ],
#                    parent_column='B23025003',
#                    table_title='Employment Status for the Population 16 Years and Over',
#                    universe='Population 16 Years and Over',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, "Unemployed in Population 16 Years and Over")
#
#
#def test_acs_column_b23025007():
#    col = ACSColumn(column_id='B23025007',
#                    table_id='B23025',
#                    is_denominator=False,
#                    column_title='Not in labor force',
#                    column_parent_path=['Total:', ],
#                    parent_column='B23025001',
#                    table_title='Employment Status for the Population 16 Years and Over',
#                    universe='Population 16 Years and Over',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, "Not in labor force in Population 16 Years and Over")
#
#
#def test_acs_column_b23025007():
#    col = ACSColumn(column_id='B23025007',
#                    table_id='B23025',
#                    is_denominator=False,
#                    column_title='Not in labor force',
#                    column_parent_path=['Total:', ],
#                    parent_column='B23025001',
#                    table_title='Employment Status for the Population 16 Years and Over',
#                    universe='Population 16 Years and Over',
#                    denominator=None,
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, "Not in labor force in Population 16 Years and Over")
#
#
##def test_acs_column_c19037001():
##    col = ACSColumn(column_id='C19037001',
##                    column_title='Less than $10,000',
##                    column_parent_path=['Householder under 25 years', ],
##                    parent_column='B19037002',
##                    table_title='Age of Householder by Household Income in the Past 12 Months (In 2010 Inflation-adjusted Dollars)',
##                    universe='Households',
##                    denominator='B19037001',
##                    tags='',
##                    moe=None)
##    assert_equals(col.name, "Less than $10,000 Household Income in the Past 12 Months (In 2010 Inflation-adjusted Dollars) Householder under 25 years")
#
#
#def test_acs_column_b05001001():
#    col = ACSColumn(column_id='B05001001',
#                    column_title='Total:',
#                    is_denominator=True,
#                    column_parent_path=[],
#                    table_id='B05001',
#                    parent_column='',
#                    table_title='Citizenship Status in the United States',
#                    universe='Total Population in the United States',
#                    denominator='B05001001',
#                    tags='',
#                    moe=None)
#    assert_equals(col.name, "Total Population")
