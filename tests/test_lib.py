from nose.tools import assert_equals, assert_raises
from lib.timespan import parse_timespan


def test_parse_invalid_timespan():
    timespan_id = 'SomeInvalidTimespan'

    with assert_raises(ValueError):
        ts_id, ts_alias, ts_name, ts_description, ts_timespan = parse_timespan(timespan_id)


def test_parse_year_timespan():
    timespan_id = '2018'

    ts_id, ts_alias, ts_name, ts_description, ts_timespan = parse_timespan(timespan_id)

    assert_equals(ts_id, '2018')
    assert_equals(ts_alias, '2018')
    assert_equals(ts_name, '2018')
    assert_equals(ts_description, 'Year 2018')
    assert_equals(ts_timespan, '[2018-01-01, 2018-12-31]')


def test_parse_range_of_years_timespan():
    timespan_id = '2016 - 2018'

    ts_id, ts_alias, ts_name, ts_description, ts_timespan = parse_timespan(timespan_id)

    assert_equals(ts_id, '2016 - 2018')
    assert_equals(ts_alias, '2016 - 2018')
    assert_equals(ts_name, '2016 - 2018')
    assert_equals(ts_description, 'From 2016 to 2018')
    assert_equals(ts_timespan, '[2016-01-01, 2018-12-31]')


def test_parse_year_and_month_timespan():
    timespan_id = '2018-02'

    ts_id, ts_alias, ts_name, ts_description, ts_timespan = parse_timespan(timespan_id)

    assert_equals(ts_id, '2018-02')
    assert_equals(ts_alias, '2018-02')
    assert_equals(ts_name, '2018-02')
    assert_equals(ts_description, 'February 2018')
    assert_equals(ts_timespan, '[2018-02-01, 2018-02-28]')


def test_parse_year_and_quarter_timespan():
    timespan_id = '2018Q1'

    ts_id, ts_alias, ts_name, ts_description, ts_timespan = parse_timespan(timespan_id)

    assert_equals(ts_id, '2018Q1')
    assert_equals(ts_alias, '2018Q1')
    assert_equals(ts_name, '2018Q1')
    assert_equals(ts_description, '2018, first quarter')
    assert_equals(ts_timespan, '[2018-01-01, 2018-03-31]')


def test_parse_day_month_year_timespan():
    timespan_id = '20180201'

    ts_id, ts_alias, ts_name, ts_description, ts_timespan = parse_timespan(timespan_id)

    assert_equals(ts_id, '20180201')
    assert_equals(ts_alias, '20180201')
    assert_equals(ts_name, '20180201')
    assert_equals(ts_description, '02/01/2018')
    assert_equals(ts_timespan, '[2018-02-01, 2018-02-01]')
