import os
import urllib.request
import math
from datetime import datetime
from luigi import Task, IntParameter, LocalTarget
from lib.logger import get_logger
from tasks.meta import current_session
from tasks.util import (classpath, unqualified_task_id)
from tasks.targets import PostgresTarget

LOGGER = get_logger(__name__)
DATA_COLUMN_PREFIX = 'ft_'
HOURS_IN_A_DAY = 24
DAYS_IN_A_WEEK = 7
MONDAY = 0
TUESDAY = 1
WEDNESDAY = 2
THURSDAY = 3
FRIDAY = 4
SATURDAY = 5
SUNDAY = 6

def quadkey2tile(quadkey):
    z = len(quadkey)

    x = 0
    y = 0
    for i in range(len(quadkey)):
        x = x * 2
        if quadkey[i] in ['1', '3']:
            x = x + 1

        y = y * 2
        if quadkey[i] in ['2', '3']:
            y = y + 1

    y = (1 << z) - y - 1

    return z, x, y


def tile2lnglat(z, x, y):
    n = 2.0 ** z

    lon = x / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    lat = - math.degrees(lat_rad)

    return lon, lat


def latlng2tile(lng, lat, z):
    x = math.floor((lng + 180) / 360 * math.pow(2, z))
    y = math.floor((1 - math.log(math.tan(lat * math.pi / 180) + 1 / math.cos(lat * math.pi / 180)) / math.pi) / 2 * math.pow(2, z))
    y = (1 << z) - y - 1

    return z, x, y


def tile2bounds(z, x, y):
    lon0, lat0 = tile2lnglat(z, x, y)
    lon1, lat1 = tile2lnglat(z, x+1, y+1)

    return lon0, lat0, lon1, lat1


def tile2quadkey(z, x, y):
    quadkey = ''
    y = (2**z - 1) - y
    for i in range(z, 0, -1):
        digit = 0
        mask = 1 << (i-1)
        if (x & mask) != 0:
            digit += 1
        if (y & mask) != 0:
            digit += 2
        quadkey += str(digit)

    return quadkey


class DownloadData(Task):

    URL = 'YOUR_URL_HERE'

    def run(self):
        self.output().makedirs()
        urllib.request.urlretrieve(self.URL, self.output().path)

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self),
                                        unqualified_task_id(self.task_id) + '.csv'))


class ImportData(Task):
    def requires(self):
        return DownloadData()

    def _create_table(self):
        query = '''
                CREATE SCHEMA IF NOT EXISTS "{schema}";
                '''.format(
                    schema=self.output().schema
                )
        self._session.execute(query)

        query = '''
                CREATE TABLE "{schema}".{table} (
                    quadkey TEXT,
                    the_geom GEOMETRY(GEOMETRY, 4326),
                    PRIMARY KEY (quadkey)
                )
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                )
        self._session.execute(query)

        self._session.commit()

    def _create_spatial_index(self):
        query = '''
                CREATE INDEX {schema}_{table}_idx
                ON "{schema}".{table} USING GIST (the_geom)
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                )
        self._session.execute(query)

        query = '''
                CLUSTER "{schema}".{table}
                USING {schema}_{table}_idx
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                )
        self._session.execute(query)
        self._session.commit()

    def _insert(self, quadkey, lon, lat):
        query = '''
                INSERT INTO "{schema}".{table} (quadkey, the_geom)
                SELECT '{quadkey}', ST_SnapToGrid(ST_SetSRID(ST_Point({lon}, {lat}), 4326), 0.000001)
                ON CONFLICT DO NOTHING
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                    quadkey=quadkey,
                    lon=lon,
                    lat=lat
                )
        self._session.execute(query)

    def _create_date_column(self, date):
        query = '''
                ALTER TABLE "{schema}".{table}
                ADD COLUMN {column_prefix}{datecolumn} INTEGER[24]
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                    column_prefix=DATA_COLUMN_PREFIX,
                    datecolumn=date
                )
        self._session.execute(query)
        self._session.commit()

    def _update_hour_value(self, quadkey, date, hour, value):
        query = '''
                UPDATE "{schema}".{table} SET {column_prefix}{datecolumn} = array_fill(0, ARRAY[24])
                WHERE quadkey = '{quadkey}'
                  AND {column_prefix}{datecolumn} IS NULL
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                    column_prefix=DATA_COLUMN_PREFIX,
                    datecolumn=date,
                    quadkey=quadkey
                )
        self._session.execute(query)
        query = '''
                UPDATE "{schema}".{table} SET {column_prefix}{datecolumn}[{hour}] = {value}
                WHERE quadkey = '{quadkey}'
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                    column_prefix=DATA_COLUMN_PREFIX,
                    datecolumn=date,
                    hour=int(hour) + 1,  # Postgres arrays are 1-based
                    value=value,
                    quadkey=quadkey
                )
        self._session.execute(query)

    def _point_position(self, z, x, y):
        lon0, lat0, lon1, lat1 = tile2bounds(z, x, y)
        return (lon0 + lon1) / 2, (lat0 + lat1) / 2

    def run(self):
        self._session = current_session()
        self._create_table()

        with open(self.input().path, "r") as ins:
            dates = set()
            i = 0
            for line in ins:
                line = line.split(',')

                quadkey = line[0]
                date = line[1].replace('-', '')
                hour = line[2]
                value = line[3]

                # Insert (empty) quadkey
                z, x, y = quadkey2tile(quadkey)
                lon, lat = self._point_position(z, x, y)
                self._insert(quadkey, lon, lat)

                # Create date column if needed
                if date not in dates:
                    self._create_date_column(date)
                    dates.add(date)

                # Save quadkey/date/hour value
                self._update_hour_value(quadkey, date, hour, value)

                i = i + 1
                if i % 10000 == 0:
                    self._session.commit()
                    LOGGER.info('Inserted {i} rows'.format(i=i))

        self._session.commit()
        self._create_spatial_index()

    def output(self):
        return PostgresTarget(classpath(self), unqualified_task_id(self.task_id))


class AggregateData(Task):
    def requires(self):
        return ImportData()

    def _create_table(self):
        query = '''
                CREATE TABLE "{schema}".{table} (
                    quadkey TEXT,
                    monday INTEGER[24] DEFAULT array_fill(0, ARRAY[24]),
                    tuesday INTEGER[24] DEFAULT array_fill(0, ARRAY[24]),
                    wednesday INTEGER[24] DEFAULT array_fill(0, ARRAY[24]),
                    thursday INTEGER[24] DEFAULT array_fill(0, ARRAY[24]),
                    friday INTEGER[24] DEFAULT array_fill(0, ARRAY[24]),
                    saturday INTEGER[24] DEFAULT array_fill(0, ARRAY[24]),
                    sunday INTEGER[24] DEFAULT array_fill(0, ARRAY[24]),
                    oneday INTEGER[24] DEFAULT array_fill(0, ARRAY[24]),
                    workday INTEGER[24] DEFAULT array_fill(0, ARRAY[24]),
                    weekend INTEGER[24] DEFAULT array_fill(0, ARRAY[24]),
                    h00 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h01 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h02 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h03 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h04 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h05 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h06 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h07 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h08 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h09 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h10 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h11 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h12 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h13 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h14 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h15 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h16 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h17 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h18 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h19 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h20 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h21 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h22 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    h23 INTEGER[7] DEFAULT array_fill(0, ARRAY[7]),
                    the_geom GEOMETRY(GEOMETRY, 4326),
                    PRIMARY KEY (quadkey)
                )
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                )
        self._session.execute(query)

        self._session.commit()

    def _create_spatial_index(self):
        query = '''
                CREATE INDEX {schema}_{table}_idx
                ON "{schema}".{table} USING GIST (the_geom)
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                )
        self._session.execute(query)

        query = '''
                CLUSTER "{schema}".{table}
                USING {schema}_{table}_idx
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                )
        self._session.execute(query)
        self._session.commit()

    def _find_date_columns(self):
        query = '''
                SELECT column_name
                  FROM information_schema.columns
                 WHERE table_schema = '{schema}'
                   AND table_name = lower('{table}')
                   AND column_name like '{column_prefix}%';
                '''.format(
                    schema=self.input().schema,
                    table=self.input().tablename,
                    column_prefix=DATA_COLUMN_PREFIX,
                )
        return [c[0] for c in self._session.execute(query).fetchall()]

    def _find_day_of_week(self, columns, day):
        return [d for d in columns
                if datetime.strptime(d, '{column_prefix}%Y%m%d'.format(column_prefix=DATA_COLUMN_PREFIX)).weekday() == day]

    def run(self):
        self._session = current_session()
        self._create_table()

        columns = self._find_date_columns()
        mondays = self._find_day_of_week(columns, MONDAY)
        tuesdays = self._find_day_of_week(columns, TUESDAY)
        wednesdays = self._find_day_of_week(columns, WEDNESDAY)
        thursdays = self._find_day_of_week(columns, THURSDAY)
        fridays = self._find_day_of_week(columns, FRIDAY)
        saturdays = self._find_day_of_week(columns, SATURDAY)
        sundays = self._find_day_of_week(columns, SUNDAY)
        workdays = mondays + tuesdays + wednesdays + thursdays + fridays
        weekend = saturdays + sundays

        aggregations = 'CEIL(({unnesting})::FLOAT / {numdates})'
        sum_unnest = 'UNNEST(COALESCE("{date}", array_fill(0, ARRAY[{size}])))'
        hour_aggregation = 'NULLIF(ARRAY[(ARRAY_AGG(monday))[{hour}]::integer, (ARRAY_AGG(tuesday))[{hour}]::integer, (ARRAY_AGG(wednesday))[{hour}]::integer, (ARRAY_AGG(thursday))[{hour}]::integer, (ARRAY_AGG(friday))[{hour}]::integer, (ARRAY_AGG(saturday))[{hour}]::integer, (ARRAY_AGG(sunday))[{hour}]::integer]::integer[], array_fill(0, ARRAY[{size}])) h{hour_name}'

        view = '''
               CREATE MATERIALIZED VIEW "{output_schema}".{output_table}_view AS
               '''.format(
                    output_schema=self.output().schema, output_table=self.output().tablename,
               )
        insert = '''
                 INSERT INTO "{output_schema}".{output_table}
                 (quadkey,
                  oneday, workday, weekend, monday, tuesday, wednesday, thursday, friday, saturday, sunday,
                  h00, h01, h02, h03, h04, h05, h06, h07, h08, h09, h10, h11, h12, h13, h14, h15, h16, h17, h18, h19, h20, h21, h22, h23,
                  the_geom)
                 '''.format(
                    output_schema=self.output().schema, output_table=self.output().tablename,
                 )
        select = '''
                 SELECT quadkey,
                       NULLIF(ARRAY_AGG(oneday)::integer[], array_fill(0, ARRAY[24])) oneday,
                       NULLIF(ARRAY_AGG(workday)::integer[], array_fill(0, ARRAY[24])) workday,
                       NULLIF(ARRAY_AGG(weekend)::integer[], array_fill(0, ARRAY[24])) weekend,
                       NULLIF(ARRAY_AGG(monday)::integer[], array_fill(0, ARRAY[24])) monday,
                       NULLIF(ARRAY_AGG(tuesday)::integer[], array_fill(0, ARRAY[24])) tuesday,
                       NULLIF(ARRAY_AGG(wednesday)::integer[], array_fill(0, ARRAY[24])) wednesday,
                       NULLIF(ARRAY_AGG(thursday)::integer[], array_fill(0, ARRAY[24])) thursday,
                       NULLIF(ARRAY_AGG(friday)::integer[], array_fill(0, ARRAY[24])) friday,
                       NULLIF(ARRAY_AGG(saturday)::integer[], array_fill(0, ARRAY[24])) saturday,
                       NULLIF(ARRAY_AGG(sunday)::integer[], array_fill(0, ARRAY[24])) sunday,
                       {hour_aggregations},
                       the_geom
                 FROM (
                    SELECT quadkey,
                           {oneday} oneday, {workday} workday, {weekend} weekend,
                           {monday} monday, {tuesday} tuesday, {wednesday} wednesday, {thursday} thursday, {friday} friday, {saturday} saturday, {sunday} sunday,
                           the_geom
                      FROM "{input_schema}".{input_table}
                     --WHERE quadkey = '0320101101202210321302'
                    ) a
                 GROUP BY quadkey, the_geom;
                 '''.format(
                    input_schema=self.input().schema, input_table=self.input().tablename,
                    oneday=aggregations.format(
                        unnesting='+'.join([sum_unnest.format(date=d, size=HOURS_IN_A_DAY) for d in columns]),
                        numdates=len(columns)),
                    workday=aggregations.format(
                        unnesting='+'.join([sum_unnest.format(date=d, size=HOURS_IN_A_DAY) for d in workdays]),
                        numdates=len(workdays)),
                    weekend=aggregations.format(
                        unnesting='+'.join([sum_unnest.format(date=d, size=HOURS_IN_A_DAY) for d in weekend]),
                        numdates=len(weekend)),
                    monday=aggregations.format(
                        unnesting='+'.join([sum_unnest.format(date=d, size=HOURS_IN_A_DAY) for d in mondays]),
                        numdates=len(mondays)),
                    tuesday=aggregations.format(
                        unnesting='+'.join([sum_unnest.format(date=d, size=HOURS_IN_A_DAY) for d in tuesdays]),
                        numdates=len(tuesdays)),
                    wednesday=aggregations.format(
                        unnesting='+'.join([sum_unnest.format(date=d, size=HOURS_IN_A_DAY) for d in wednesdays]),
                        numdates=len(wednesdays)),
                    thursday=aggregations.format(
                        unnesting='+'.join([sum_unnest.format(date=d, size=HOURS_IN_A_DAY) for d in thursdays]),
                        numdates=len(thursdays)),
                    friday=aggregations.format(
                        unnesting='+'.join([sum_unnest.format(date=d, size=HOURS_IN_A_DAY) for d in fridays]),
                        numdates=len(fridays)),
                    saturday=aggregations.format(
                        unnesting='+'.join([sum_unnest.format(date=d, size=HOURS_IN_A_DAY) for d in saturdays]),
                        numdates=len(saturdays)),
                    sunday=aggregations.format(
                        unnesting='+'.join([sum_unnest.format(date=d, size=HOURS_IN_A_DAY) for d in sundays]),
                        numdates=len(sundays)),
                    hour_aggregations=','.join([hour_aggregation.format(hour=i, size=7,
                                                                        hour_name='{num:02d}'.format(num=(i - 1))) for i in range(1, 25)])
                 )
        self._session.execute(insert + select)
        self._session.commit()
        self._create_spatial_index()

        self._session.execute(view + select)
        self._session.commit()

        query = '''
                CREATE INDEX {schema}_{table}_view_idx
                ON "{schema}".{table}_view USING GIST (the_geom)
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                )
        self._session.execute(query)

        query = '''
                CLUSTER "{schema}".{table}_view
                USING {schema}_{table}_view_idx
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                )
        self._session.execute(query)
        self._session.commit()

    def output(self):
        return PostgresTarget(classpath(self), unqualified_task_id(self.task_id))
