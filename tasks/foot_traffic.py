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
QUADKEY_FIELD = 'quadkey'
GEOM_FIELD = 'the_geom'
MAX_ZOOM_LEVEL = 22
IDX_SUFFIX = '_idx'
VIEW_IDX_SUFFIX = '_view_idx'
META_TABLE = 'ft_table'
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


def find_columns_with_prefix(session, schema, tablename, column_prefix):
    query = '''
            SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = lower('{table}')
                AND column_name like '{column_prefix}%';
            '''.format(
                schema=schema,
                table=tablename,
                column_prefix=column_prefix,
            )
    return [c[0] for c in session.execute(query).fetchall()]


def create_spatial_index(session, schema, tablename, suffix):
    query = '''
            CREATE INDEX {schema}_{table}{suffix}
            ON "{schema}".{table} USING GIST (the_geom)
            '''.format(
                schema=schema,
                table=tablename,
                suffix=suffix
            )
    session.execute(query)

    query = '''
            CLUSTER "{schema}".{table}
            USING {schema}_{table}{suffix}
            '''.format(
                schema=schema,
                table=tablename,
                suffix=suffix
            )
    session.execute(query)
    session.commit()


class BaseFTTable(Task):
    def _populate(self):
        raise NotImplementedError

    @property
    def _id(self):
        return '{schema}.{table}'.format(schema=self.output().schema,
                                         table=self.output().tablename)

    @property
    def _schemaname(self):
        return self.output().schema

    @property
    def _tablename(self):
        return self.output().tablename

    @property
    def _zoom(self):
        return self.zoom

    @property
    def _timerange(self):
        columns = find_columns_with_prefix(self._session, self.input().schema, self.input().tablename,
                                           DATA_COLUMN_PREFIX)
        mindate, maxdate, first = None, None, True

        for column in columns:
            current = datetime.strptime(column, '{column_prefix}%Y%m%d'.format(column_prefix=DATA_COLUMN_PREFIX))
            if first:
                mindate, maxdate, first = current, current, False

            mindate = current if current < mindate else mindate
            maxdate = current if current > maxdate else maxdate

        return '[{mindate}, {maxdate}]'.format(mindate=mindate.strftime('%Y-%m-%d'),
                                               maxdate=maxdate.strftime('%Y-%m-%d'))

    @property
    def _bbox(self):
        query = '''
                SELECT ST_Envelope(ST_Union(the_geom)) FROM "{schema}".{table}
                '''.format(
                    schema=self.input().schema,
                    table=self.input().tablename,
                )

        return self._session.execute(query).fetchone()[0]

    def _create_metadata_table(self):
        query = '''
                CREATE TABLE IF NOT EXISTS "{schema}".{table} (
                    id TEXT,
                    schema TEXT,
                    tablename TEXT,
                    zoom INTEGER,
                    timerange DATERANGE,
                    bbox GEOMETRY(GEOMETRY, 4326),
                    PRIMARY KEY (id)
                )
                '''.format(
                    schema=self.output().schema,
                    table=META_TABLE,
                )
        self._session.execute(query)

        query = '''
            CREATE INDEX {schema}_{table}_{suffix}
            ON "{schema}".{table} (zoom, timerange)
            '''.format(
                schema=self.output().schema,
                table=META_TABLE,
                suffix=IDX_SUFFIX
            )
        self._session.execute(query)

        query = '''
            CREATE INDEX {schema}_{table}_bbox_{suffix}
            ON "{schema}".{table} USING GIST (bbox)
            '''.format(
                schema=self.output().schema,
                table=META_TABLE,
                suffix=IDX_SUFFIX
            )
        self._session.execute(query)

        self._session.commit()

    def _populate_metadata_table(self):
        query = '''
                INSERT INTO "{schema}".{table} (id, schema, tablename, zoom, bbox, timerange)
                VALUES ('{id}', '{schemaname}', '{tablename}', {zoom}, '{bbox}', '{timerange}')
                ON CONFLICT (id)
                DO UPDATE SET tablename = EXCLUDED.tablename,
                              zoom = EXCLUDED.zoom,
                              bbox = EXCLUDED.bbox,
                              timerange = EXCLUDED.timerange
                '''.format(
                    schema=self.output().schema,
                    table=META_TABLE,
                    id=self._id,
                    schemaname=self._schemaname,
                    tablename=self._tablename,
                    zoom=self._zoom,
                    bbox=self._bbox,
                    timerange=self._timerange,
                )
        self._session.execute(query)

        self._session.commit()

    def run(self):
        self._session = current_session()
        self._populate()
        self._create_metadata_table()
        self._populate_metadata_table()


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
                    {quadkey_field} TEXT,
                    {geom_field} GEOMETRY(GEOMETRY, 4326),
                    PRIMARY KEY (quadkey)
                )
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                    quadkey_field=QUADKEY_FIELD,
                    geom_field=GEOM_FIELD,
                )
        self._session.execute(query)

        self._session.commit()

    def _insert(self, quadkey, lon, lat):
        query = '''
                INSERT INTO "{schema}".{table} ({quadkey_field}, {geom_field})
                SELECT '{quadkey}', ST_SnapToGrid(ST_SetSRID(ST_Point({lon}, {lat}), 4326), 0.000001)
                ON CONFLICT DO NOTHING
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                    quadkey_field=QUADKEY_FIELD,
                    geom_field=GEOM_FIELD,
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
                WHERE {quadkey_field} = '{quadkey}'
                  AND {column_prefix}{datecolumn} IS NULL
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                    column_prefix=DATA_COLUMN_PREFIX,
                    datecolumn=date,
                    quadkey_field=QUADKEY_FIELD,
                    quadkey=quadkey
                )
        self._session.execute(query)
        query = '''
                UPDATE "{schema}".{table} SET {column_prefix}{datecolumn}[{hour}] = {value}
                WHERE {quadkey_field} = '{quadkey}'
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                    column_prefix=DATA_COLUMN_PREFIX,
                    datecolumn=date,
                    hour=int(hour) + 1,  # Postgres arrays are 1-based
                    value=value,
                    quadkey_field=QUADKEY_FIELD,
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
        create_spatial_index(self._session, self.output().schema, self.output().tablename, IDX_SUFFIX)

    def output(self):
        return PostgresTarget(classpath(self), unqualified_task_id(self.task_id))


class ZoomLevel(Task):
    zoom = IntParameter()

    def requires(self):
        return ImportData()

    def _create_table(self):
        query = '''
                CREATE TABLE "{schema}".{table} AS
                SELECT * FROM "{input_schema}".{input_table}
                WHERE 1=0
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                    input_schema=self.input().schema,
                    input_table=self.input().tablename,
                )
        self._session.execute(query)

        self._session.commit()

    def run(self):
        if self.zoom > MAX_ZOOM_LEVEL:
            raise ValueError('Zoom level {zoom} cannot be greater than the maximum available zoom level ({maxzoom})'.format(zoom=self.zoom, maxzoom=MAX_ZOOM_LEVEL))

        self._session = current_session()
        self._create_table()

        data_columns = find_columns_with_prefix(self._session, self.input().schema, self.input().tablename,
                                                DATA_COLUMN_PREFIX)

        field_column = 'NULLIF(ARRAY[{sumfield}]::INTEGER[] , ARRAY_FILL(0, ARRAY[{size}])) {field}'
        sumfield = 'SUM((COALESCE({field}, array_fill(0, ARRAY[{size}])))[{i}])'
        query = '''
                INSERT INTO "{schema}".{table}
                SELECT SUBSTRING({quadkey_field} FROM 1 FOR {zoom}) {quadkey_field}, ST_Centroid(ST_Union({geom_field})) {geom_field},
                {fields}
                FROM "{input_schema}".{input_table}
                GROUP BY SUBSTRING({quadkey_field} from 1 for {zoom});
                '''.format(
                    fields=','.join([field_column.format(sumfield=','.join([sumfield.format(field=c,
                                                                                            size=HOURS_IN_A_DAY,
                                                                                            i=i) for i in range(1, HOURS_IN_A_DAY + 1)]), size=HOURS_IN_A_DAY,
                                                                                                                                          field=c) for c in data_columns]),
                    schema=self.output().schema,
                    table=self.output().tablename,
                    quadkey_field=QUADKEY_FIELD,
                    geom_field=GEOM_FIELD,
                    input_schema=self.input().schema,
                    input_table=self.input().tablename,
                    zoom=self.zoom
                )

        self._session.execute(query)
        self._session.commit()
        create_spatial_index(self._session, self.output().schema, self.output().tablename, IDX_SUFFIX)

    def output(self):
        return PostgresTarget(classpath(self), unqualified_task_id(self.task_id))


class AggregateData(BaseFTTable):
    zoom = IntParameter(default=MAX_ZOOM_LEVEL)

    def requires(self):
        return ZoomLevel(zoom=self.zoom)

    def _create_table(self):
        query = '''
                CREATE TABLE "{schema}".{table} (
                    {quadkey_field} TEXT,
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
                    {geom_field} GEOMETRY(GEOMETRY, 4326),
                    PRIMARY KEY (quadkey)
                );
                COMMENT ON COLUMN "{schema}".{table}.monday IS 'Aggregated 0 to 24 hour data for a typical monday';
                COMMENT ON COLUMN "{schema}".{table}.tuesday IS 'Aggregated 0 to 24 hour data for a typical tuesday';
                COMMENT ON COLUMN "{schema}".{table}.wednesday IS 'Aggregated 0 to 24 hour data for a typical wednesday';
                COMMENT ON COLUMN "{schema}".{table}.thursday IS 'Aggregated 0 to 24 hour data for a typical thursday';
                COMMENT ON COLUMN "{schema}".{table}.friday IS 'Aggregated 0 to 24 hour data for a typical friday';
                COMMENT ON COLUMN "{schema}".{table}.saturday IS 'Aggregated 0 to 24 hour data for a typical saturday';
                COMMENT ON COLUMN "{schema}".{table}.sunday IS 'Aggregated 0 to 24 hour data for a typical sunday';
                COMMENT ON COLUMN "{schema}".{table}.oneday IS 'Aggregated 0 to 24 hour data for a typical day';
                COMMENT ON COLUMN "{schema}".{table}.workday IS 'Aggregated 0 to 24 hour data for a typical workday';
                COMMENT ON COLUMN "{schema}".{table}.weekend IS 'Aggregated 0 to 24 hour data for a typical weekend day';
                COMMENT ON COLUMN "{schema}".{table}.h00 IS 'Aggregated week data for a typical 00:00 to 01:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h01 IS 'Aggregated week data for a typical 01:00 to 02:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h02 IS 'Aggregated week data for a typical 02:00 to 03:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h03 IS 'Aggregated week data for a typical 03:00 to 04:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h04 IS 'Aggregated week data for a typical 04:00 to 05:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h05 IS 'Aggregated week data for a typical 05:00 to 06:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h06 IS 'Aggregated week data for a typical 06:00 to 07:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h07 IS 'Aggregated week data for a typical 07:00 to 08:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h08 IS 'Aggregated week data for a typical 08:00 to 09:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h09 IS 'Aggregated week data for a typical 09:00 to 10:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h10 IS 'Aggregated week data for a typical 10:00 to 11:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h11 IS 'Aggregated week data for a typical 11:00 to 12:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h12 IS 'Aggregated week data for a typical 12:00 to 13:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h13 IS 'Aggregated week data for a typical 13:00 to 14:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h14 IS 'Aggregated week data for a typical 14:00 to 15:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h15 IS 'Aggregated week data for a typical 15:00 to 16:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h16 IS 'Aggregated week data for a typical 16:00 to 17:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h17 IS 'Aggregated week data for a typical 17:00 to 18:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h18 IS 'Aggregated week data for a typical 18:00 to 19:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h19 IS 'Aggregated week data for a typical 19:00 to 20:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h20 IS 'Aggregated week data for a typical 20:00 to 21:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h21 IS 'Aggregated week data for a typical 21:00 to 22:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h22 IS 'Aggregated week data for a typical 22:00 to 23:00 hour';
                COMMENT ON COLUMN "{schema}".{table}.h23 IS 'Aggregated week data for a typical 23:00 to 00:00 hour';
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                    quadkey_field=QUADKEY_FIELD,
                    geom_field=GEOM_FIELD
                )
        self._session.execute(query)

        self._session.commit()

    def _find_day_of_week(self, columns, day):
        return [d for d in columns
                if datetime.strptime(d, '{column_prefix}%Y%m%d'.format(column_prefix=DATA_COLUMN_PREFIX)).weekday() == day]

    def _populate(self):
        self._create_table()

        columns = find_columns_with_prefix(self._session, self.input().schema, self.input().tablename,
                                           DATA_COLUMN_PREFIX)
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
                 ({quadkey_field},
                  oneday, workday, weekend, monday, tuesday, wednesday, thursday, friday, saturday, sunday,
                  h00, h01, h02, h03, h04, h05, h06, h07, h08, h09, h10, h11, h12, h13, h14, h15, h16, h17, h18, h19, h20, h21, h22, h23,
                  {geom_field})
                 '''.format(
                    output_schema=self.output().schema, output_table=self.output().tablename,
                    quadkey_field=QUADKEY_FIELD, geom_field=GEOM_FIELD,
                 )
        select = '''
                 SELECT {quadkey_field},
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
                       {geom_field}
                 FROM (
                    SELECT {quadkey_field},
                           {oneday} oneday, {workday} workday, {weekend} weekend,
                           {monday} monday, {tuesday} tuesday, {wednesday} wednesday, {thursday} thursday, {friday} friday, {saturday} saturday, {sunday} sunday,
                           {geom_field}
                      FROM "{input_schema}".{input_table}
                    ) a
                 GROUP BY {quadkey_field}, {geom_field};
                 '''.format(
                    input_schema=self.input().schema, input_table=self.input().tablename,
                    quadkey_field=QUADKEY_FIELD, geom_field=GEOM_FIELD,
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
        create_spatial_index(self._session, self.output().schema, self.output().tablename, IDX_SUFFIX)

        # self._session.execute(view + select)
        # self._session.commit()
        # create_spatial_index(self._session, self.output().schema, self.output().tablename + '_view', VIEW_IDX_SUFFIX)

    def output(self):
        return PostgresTarget(classpath(self), unqualified_task_id(self.task_id))
