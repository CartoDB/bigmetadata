from nose.tools import assert_equals
from tasks.util import slug_column

def test_slug_column():
    assert_equals(slug_column('Population'), 'pop')
    assert_equals(slug_column('Population 5 Years and Over'), 'pop_5_years_and_over')
    assert_equals(slug_column('Workers 16 Years and Over'), 'workers_16_years_and_over')
    assert_equals(slug_column('Population for Whom Poverty Status Is Determined'),
                  'pop_poverty_status_determined')
    assert_equals(slug_column('Commuters by Car, truck, or van'), 'commuters_by_car_truck_or_van')
    assert_equals(slug_column('Aggregate travel time to work (in minutes)'),
                  'aggregate_travel_time_to_work_in_minutes')

