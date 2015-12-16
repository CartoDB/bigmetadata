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
    assert_equals(col.name, 'Under 5 years Age Male Population')


def test_acs_column_b01001026():
    col = ACSColumn(column_id='B01001026',
                    column_title='Female:',
                    column_parent_path=['Total:'],
                    parent_column_id=None,
                    table_title='Sex by Age',
                    universe='Total Population',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, 'Female Population')


def test_acs_column_b03002012():
    col = ACSColumn(column_id='B03002012',
                    column_title='Hispanic or Latino:',
                    column_parent_path=['Total:'],
                    parent_column_id='B03002001',
                    table_title='Hispanic or Latino Origin by Race',
                    universe='Total Population',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, 'Hispanic or Latino Population')


def test_acs_column_b03002006():
    col = ACSColumn(column_id='B03002006',
                    column_title='Asian alone',
                    column_parent_path=['Total:', 'Not Hispanic or Latino:'],
                    parent_column_id='B03002002',
                    table_title='Hispanic or Latino Origin by Race',
                    universe='Total Population',
                    denominator='B03002001',
                    tags='',
                    moe=None)
    assert_equals(col.name, 'Asian Population')


def test_acs_column_b03002004():
    col = ACSColumn(column_id='B03002004',
                    column_title='Black or African American alone',
                    column_parent_path=['Total:', 'Not Hispanic or Latino:'],
                    parent_column_id='B03002002',
                    table_title='Hispanic or Latino Origin by Race',
                    universe='Total Population',
                    denominator='B03002001',
                    tags='',
                    moe=None)
    assert_equals(col.name, 'Black or African American Population')


def test_acs_column_b03002003():
    col = ACSColumn(column_id='B03002003',
                    column_title='White alone',
                    column_parent_path=['Total:', 'Not Hispanic or Latino:'],
                    parent_column_id='B03002002',
                    table_title='Hispanic or Latino Origin by Race',
                    universe='Total Population',
                    denominator='B03002001',
                    tags='',
                    moe=None)
    assert_equals(col.name, 'White Population')


def test_acs_column_b09001001():
    col = ACSColumn(column_id='B09001001',
                    column_title='Total:',
                    column_parent_path=[],
                    parent_column_id=None,
                    table_title='Population Under 18 Years by Age',
                    universe='Population Under 18 Years',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, 'Population Under 18 Years')


def test_acs_column_b09020001():
    col = ACSColumn(column_id='B09020001',
                    column_title='Total:',
                    column_parent_path=[],
                    parent_column_id=None,
                    table_title='Relationship by Household Type (Including '
                                'Living Alone) for the Population 65 Years and Over',
                    universe='Population 65 Years and Over',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, 'Population 65 Years and Over')


def test_acs_column_b11001001():
    col = ACSColumn(column_id='B11001001',
                    column_title='Total:',
                    column_parent_path=[],
                    parent_column_id=None,
                    table_title='Household Type (Including Living Alone)',
                    universe='Households',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, 'Households')


def test_acs_column_b14001002():
    col = ACSColumn(column_id='B14001002',
                    column_title='Enrolled in school:',
                    column_parent_path=['Total:'],
                    parent_column_id=None,
                    table_title='School Enrollment by Level of School for the '
                                'Population 3 Years and Over',
                    universe='Population 3 Years and Over',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, 'Enrolled in school Population 3 Years and Over')

def test_acs_column_b14001002():
    col = ACSColumn(column_id='B14001008',
                    column_title='Enrolled in college, undergraduate years',
                    column_parent_path=['Total:', 'Enrolled in school:'],
                    parent_column_id=None,
                    table_title='School Enrollment by Level of School for the '
                                'Population 3 Years and Over',
                    universe='Population 3 Years and Over',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, 'Enrolled in college, undergraduate years Population 3 Years and Over')


def test_acs_column_b15003017():
    col = ACSColumn(column_id='B15003017',
                    column_title='Regular high school diploma',
                    column_parent_path=['Total:'],
                    parent_column_id='B15003001',
                    table_title='Educational Attainment for the Population 25 Years and Over',
                    universe='Population 25 Years and Over',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, "Regular high school diploma Population 25 Years and Over")


def test_acs_column_b15003022():
    col = ACSColumn(column_id='B15003022',
                    column_title='Bachelor\'s degree',
                    column_parent_path=['Total:'],
                    parent_column_id='B15003001',
                    table_title='Educational Attainment for the Population 25 Years and Over',
                    universe='Population 25 Years and Over',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, "Bachelor's degree Population 25 Years and Over")


#    b17001002 AS poverty,  -- 0.520%
#    b19013001 AS hhi,      -- 0.128%
#    b22003002 AS food_stamps, -- 0.432% -- denominator of this is # of households
#    b23025003 AS civilian_labor_force, -- 
#    b23025005 AS unemployment -- denominator of this is civilian_labor_force
