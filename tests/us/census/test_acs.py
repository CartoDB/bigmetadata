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
    assert_equals(col.name, 'Enrolled in college, undergraduate years in Population 3 Years and Over')


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
    assert_equals(col.name, "Regular high school diploma in Population 25 Years and Over")


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
    assert_equals(col.name, "Bachelor's degree in Population 25 Years and Over")


def test_acs_column_b17001002():
    col = ACSColumn(column_id='B17001002',
                    column_title='Income in the past 12 months below poverty level:',
                    column_parent_path=['Total:'],
                    parent_column_id='B17001001',
                    table_title='Poverty Status in the Past 12 Months by Sex by Age',
                    universe='Population for Whom Poverty Status Is Determined',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, "Income in the past 12 months below poverty level in Population for Whom Poverty Status Is Determined")


def test_acs_column_b19013001():
    col = ACSColumn(column_id='B19013001',
                    column_title='Median household income in the past 12 months (in 2012 inflation-adjusted dollars)',
                    column_parent_path=[],
                    parent_column_id=None,
                    table_title='Median Household Income in the Past 12 Months (In 2012 Inflation-adjusted Dollars)',
                    universe='Households',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, "Median household income in the past 12 months (in 2012 inflation-adjusted dollars)")


def test_acs_column_b22003002():
    col = ACSColumn(column_id='B22003002',
                    column_title='Household received Food Stamps/SNAP in the past 12 months:',
                    column_parent_path=['Total:'],
                    parent_column_id='B22003001',
                    table_title='Receipt of Food Stamps/SNAP in the Past 12 Months by Poverty Status in the Past 12 Months for Households',
                    universe='Households',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, "Household received Food Stamps/SNAP in the past 12 months")


def test_acs_column_b23025002():
    col = ACSColumn(column_id='B23025002',
                    column_title='In labor force:',
                    column_parent_path=['Total:'],
                    parent_column_id='B23025001',
                    table_title='Employment Status for the Population 16 Years and Over',
                    universe='Population 16 Years and Over',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, "In labor force in Population 16 Years and Over")


def test_acs_column_b23025003():
    col = ACSColumn(column_id='B23025003',
                    column_title='Civilian labor force:',
                    column_parent_path=['Total:', 'In labor force:'],
                    parent_column_id='B23025002',
                    table_title='Employment Status for the Population 16 Years and Over',
                    universe='Population 16 Years and Over',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, "Civilian labor force in Population 16 Years and Over")


def test_acs_column_b23025005():
    col = ACSColumn(column_id='B23025005',
                    column_title='Unemployed',
                    column_parent_path=['Total:', 'In labor force:', 'Unemployed', ],
                    parent_column_id='B23025003',
                    table_title='Employment Status for the Population 16 Years and Over',
                    universe='Population 16 Years and Over',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, "Unemployed in Population 16 Years and Over")


def test_acs_column_b23025007():
    col = ACSColumn(column_id='B23025007',
                    column_title='Not in labor force',
                    column_parent_path=['Total:', ],
                    parent_column_id='B23025001',
                    table_title='Employment Status for the Population 16 Years and Over',
                    universe='Population 16 Years and Over',
                    denominator=None,
                    tags='',
                    moe=None)
    assert_equals(col.name, "Not in labor force in Population 16 Years and Over")
