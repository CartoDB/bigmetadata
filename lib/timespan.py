import re
import calendar
from dateutil.parser import parse
from tasks.meta import OBSTimespan, current_session

QUARTERS = {1: 'first',
            2: 'second',
            3: 'third',
            4: 'fourth'}


def get_timespan(timespan_id):
    ts_id, ts_alias, ts_name, ts_description, ts_timespan = parse_timespan(timespan_id)
    session = current_session()
    with session.no_autoflush:
        timespan = session.query(OBSTimespan).get(ts_id)
        if not timespan:
            timespan = OBSTimespan(id=ts_id, alias=ts_alias, name=ts_name, description=ts_description, timespan=ts_timespan)
        return session.merge(timespan)


def parse_timespan(timespan_id):
    timespan_str = str(timespan_id).replace(' ', '')

    ts_id, ts_alias, ts_name, ts_description, ts_timespan = None, None, None, None, None

    # Year, for example '2017'
    if re.match('\d{4}$', timespan_str):
        ts_id = timespan_str
        ts_alias = timespan_str
        ts_name = timespan_str
        ts_description = 'Year {year}'.format(year=timespan_str)
        ts_timespan = '[{year}-01-01, {year}-12-31]'.format(year=timespan_str)
    # Range of years, for example '2017-2018'
    elif re.match('\d{4}-\d{4}$', timespan_str):
        year1, year2 = list(map(int, timespan_str.split('-')))
        if year1 > year2:
            raise ValueError("Invalid timespan '{timespan}' (invalid range of years)".format(timespan=timespan_id))

        ts_id = '{year1} - {year2}'.format(year1=year1, year2=year2)
        ts_alias = '{year1} - {year2}'.format(year1=year1, year2=year2)
        ts_name = '{year1} - {year2}'.format(year1=year1, year2=year2)
        ts_description = 'From {year1} to {year2}'.format(year1=year1, year2=year2)
        ts_timespan = '[{year1}-01-01, {year2}-12-31]'.format(year1=year1, year2=year2)
    # Year and month, for example '2017-03'
    elif re.match('\d{4}-\d{2}$', timespan_str):
        year, month = list(map(int, timespan_str.split('-')))
        if month < 1 or month > 12:
            raise ValueError("Invalid timespan '{timespan}' (invalid month)".format(timespan=timespan_id))

        last_day_of_month = calendar.monthrange(year, month)[1]
        month_name = calendar.month_name[month]

        ts_id = '{year}-{month:02d}'.format(year=year, month=month)
        ts_alias = '{year}-{month:02d}'.format(year=year, month=month)
        ts_name = '{year}-{month:02d}'.format(year=year, month=month)
        ts_description = '{month_name} {year}'.format(month_name=month_name, year=year)
        ts_timespan = '[{year}-{month:02d}-01, {year}-{month:02d}-{last_day}]'.format(year=year, month=month,
                                                                                      last_day=last_day_of_month)
    # Year and quarter, for example '2017Q3'
    elif re.match('\d{4}Q\d{1}$', timespan_str):
        year, quarter = list(map(int, timespan_str.split('Q')))
        if quarter < 1 or quarter > 4:
            raise ValueError("Invalid timespan '{timespan}' (invalid quarter)".format(timespan=timespan_id))

        last_month = quarter * 3
        initial_mont = last_month - 2
        last_day = calendar.monthrange(year, last_month)[1]

        ts_id = '{year}Q{quarter}'.format(year=year, quarter=quarter)
        ts_alias = '{year}Q{quarter}'.format(year=year, quarter=quarter)
        ts_name = '{year}Q{quarter}'.format(year=year, quarter=quarter)
        ts_description = '{year}, {quarter} quarter'.format(year=year, quarter=QUARTERS[quarter])
        ts_timespan = '[{year}-{ini_month:02d}-01, {year}-{end_month:02d}-{last_day}]'.format(year=year,
                                                                                              ini_month=initial_mont,
                                                                                              end_month=last_month,
                                                                                              last_day=last_day)
    # Day, month, year, for example '20171231'
    elif re.match('\d{8}$', timespan_str):
        date = parse(timespan_str)

        last_day_of_month = calendar.monthrange(date.year, date.month)[1]

        ts_id = date.strftime('%Y%m%d')
        ts_alias = date.strftime('%Y%m%d')
        ts_name = date.strftime('%Y%m%d')
        ts_description = date.strftime('%m/%d/%Y')
        ts_timespan = '[{date}, {date}]'.format(date=date.strftime('%Y-%m-%d'))
    else:
        raise ValueError('Unparseable timespan {timespan}'.format(timespan=timespan_id))

    return ts_id, ts_alias, ts_name, ts_description, ts_timespan
