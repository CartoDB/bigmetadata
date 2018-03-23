import os
import urllib.request
import math
from luigi import Task, IntParameter, LocalTarget
from lib.logger import get_logger
from tasks.meta import current_session
from tasks.util import (classpath, unqualified_task_id)
from tasks.targets import PostgresTarget

LOGGER = get_logger(__name__)


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


class FTPoints(Task):
    def requires(self):
        return DownloadData()

    def _point_position(self, z, x, y):
        lon0, lat0, lon1, lat1 = tile2bounds(z, x, y)
        return (lon0 + lon1) / 2, (lat0 + lat1) / 2

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
                    val NUMERIC,
                    the_geom GEOMETRY(GEOMETRY, 4326),
                    PRIMARY KEY (quadkey)
                )
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                )
        self._session.execute(query)

        self._session.commit()

    def _insert(self, quadkey, value, lon, lat):
        query = '''
                INSERT INTO "{schema}".{table} (quadkey, val, the_geom)
                SELECT '{quadkey}', {value}, ST_SnapToGrid(ST_SetSRID(ST_Point({lon}, {lat}), 4326), 0.000001)
                ON CONFLICT DO NOTHING
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                    quadkey=quadkey,
                    value=value,
                    lon=lon,
                    lat=lat
                )
        self._session.execute(query)

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

    def run(self):
        self._session = current_session()
        self._create_table()
        with open(self.input().path, "r") as ins:
            i = 0
            for line in ins:
                line = line.split(',')

                quadkey = line[0]
                value = line[3]

                z, x, y = quadkey2tile(quadkey)
                lon, lat = self._point_position(z, x, y)

                self._insert(quadkey, value, lon, lat)
                i = i + 1
                if i % 10000 == 0:
                    self._session.commit()
                    LOGGER.info('Inserted {i} rows'.format(i=i))

        self._session.commit()
        self._create_spatial_index()

    def output(self):
        return PostgresTarget(classpath(self), unqualified_task_id(self.task_id))


class FTPolygons(Task):
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
                    val NUMERIC,
                    the_geom GEOMETRY(GEOMETRY, 4326),
                    PRIMARY KEY (quadkey)
                )
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                )
        self._session.execute(query)

        self._session.commit()

    def _insert(self, quadkey, value, envelope):
        query = '''
                INSERT INTO "{schema}".{table} (quadkey, val, the_geom)
                SELECT '{quadkey}', {value}, ST_SnapToGrid(ST_SetSRID(ST_Envelope('LINESTRING({x1} {y1}, {x2} {y2})'::geometry), 4326), 0.000001)
                ON CONFLICT DO NOTHING
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                    quadkey=quadkey,
                    value=value,
                    x1=envelope[0],
                    y1=envelope[1],
                    x2=envelope[2],
                    y2=envelope[3]
                )
        self._session.execute(query)

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

    def run(self):
        self._session = current_session()
        self._create_table()
        with open(self.input().path, "r") as ins:
            i = 0
            for line in ins:
                line = line.split(',')

                quadkey = line[0]
                value = line[3]

                z, x, y = quadkey2tile(quadkey)
                envelope = tile2bounds(z, x, y)

                self._insert(quadkey, value, envelope)
                i = i + 1
                if i % 10000 == 0:
                    self._session.commit()
                    LOGGER.info('Inserted {i} rows'.format(i=i))

        self._session.commit()
        self._create_spatial_index()

    def output(self):
        return PostgresTarget(classpath(self), unqualified_task_id(self.task_id))


class FTRaster(Task):
    zoom_shift = IntParameter(default=5)

    def requires(self):
        return FTPoints()

    def _create_table(self):
        query = '''
                CREATE SCHEMA IF NOT EXISTS "{schema}";
                '''.format(
                    schema=self.output().schema
                )
        self._session.execute(query)

        query = '''
                CREATE TABLE "{schema}".{table} (
                    rid TEXT,
                    rast RASTER,
                    PRIMARY KEY (rid)
                )
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                )
        self._session.execute(query)

        self._session.commit()

    def _zoom_level(self):
        query = '''
                SELECT quadkey FROM {schema}.{table} LIMIT 1
                '''.format(
                    schema=self.input().schema,
                    table=self.input().tablename,
                )
        z, x, y = quadkey2tile(self._session.execute(query).fetchone()[0])
        return z

    def _raster_zoom_level(self, z):
        return z - 3 if z - 3 > 1 else 1

    def _input_envelope(self):
        query = '''
                WITH env as (
                    SELECT ST_Extent(the_geom) as the_geom FROM {schema}.{table}
                )
                SELECT ST_XMin(e.the_geom), ST_YMax(e.the_geom),
                       ST_XMax(e.the_geom), ST_YMin(e.the_geom)
                FROM env e
                '''.format(
                    schema=self.input().schema,
                    table=self.input().tablename,
                )
        envelope = self._session.execute(query).fetchone()
        return envelope[0], envelope[1], envelope[2], envelope[3]

    def _has_points(self, envelope):
        query = '''
                WITH env AS (
                    SELECT ST_SetSRID(ST_Envelope('LINESTRING({x1} {y1}, {x2} {y2})'::geometry),4326) the_geom
                )
                SELECT 1
                FROM "{schema}".{table} i, env e
                WHERE ST_Contains(e.the_geom, i.the_geom)
                LIMIT 1
                '''.format(
                    schema=self.input().schema,
                    table=self.input().tablename,
                    x1=envelope[0],
                    y1=envelope[1],
                    x2=envelope[2],
                    y2=envelope[3]
                )
        return self._session.execute(query).fetchone() is not None

    def _insert_raster(self, quadkey, pixels, envelope):
        query = '''
                INSERT INTO "{schema_o}".{table_o} (rid, rast)
                SELECT '{quadkey}', ST_AddBand(ST_MakeEmptyRaster({pixels}, {pixels}, {x1}, {y1}, ABS(({x1})-({x2}))/{pixels}, -ABS(({y1})-({y2}))/{pixels}, 0, 0, 4326), 1, '16BUI'::text, 0, 0) rast
                '''.format(
                    quadkey=quadkey,
                    pixels=pixels,
                    schema_o=self.output().schema,
                    table_o=self.output().tablename,
                    x1=envelope[0],
                    y1=envelope[1],
                    x2=envelope[2],
                    y2=envelope[3]
                )
        self._session.execute(query)

        query = '''
                WITH env AS (
                    SELECT ST_SetSRID(ST_Envelope('LINESTRING({x1} {y1}, {x2} {y2})'::geometry),4326) the_geom
                )
                UPDATE "{schema_o}".{table_o}
                    SET rast = ST_SetValues(rast, 1,
                    (
                        SELECT ARRAY(
                            SELECT (i.the_geom, i.val)::geomval
                            FROM "{schema_i}".{table_i} i, env e
                            WHERE ST_Contains(e.the_geom, i.the_geom)
                        )
                    )
                )
                WHERE rid = '{quadkey}';
                '''.format(
                    quadkey=quadkey,
                    schema_i=self.input().schema,
                    table_i=self.input().tablename,
                    schema_o=self.output().schema,
                    table_o=self.output().tablename,
                    x1=envelope[0],
                    y1=envelope[1],
                    x2=envelope[2],
                    y2=envelope[3]
                )
        self._session.execute(query)

        self._session.commit()

    def _clean_raster(self):
        query = '''
                DELETE FROM {schema_o}.{table_o}
                WHERE rast IS NULL;
                '''.format(
                    schema_o=self.output().schema,
                    table_o=self.output().tablename
                )
        self._session.execute(query)
        self._session.commit()

    def _create_spatial_index(self):
        query = '''
                CREATE INDEX {schema}_{table}_idx
                ON "{schema}".{table} USING GIST (ST_Envelope(rast))
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

    def run(self):
        self._session = current_session()
        self._create_table()

        env_lon0, env_lat0, env_lon1, env_lat1 = self._input_envelope()
        z = self._zoom_level()
        z, x, y = latlng2tile(env_lon0, env_lat0, z)
        quadkey = tile2quadkey(z, x, y)
        pixels = 2**(self.zoom_shift + 1)

        r_quadkey = quadkey[:-self.zoom_shift]
        r_z, r_x, r_y = quadkey2tile(r_quadkey)

        r_lon0, r_lat1, r_lon1, r_lat0 = tile2bounds(r_z, r_x, r_y)

        i = 0
        r_x_ini = r_x
        while r_lat0 > env_lat1:
            while r_lon0 < env_lon1:
                if self._has_points((r_lon0, r_lat0, r_lon1, r_lat1)):
                    self._insert_raster(r_quadkey, pixels,
                                        (r_lon0, r_lat0,
                                         r_lon1, r_lat1))
                r_x = r_x + 1
                r_quadkey = tile2quadkey(r_z, r_x, r_y)
                r_lon0, r_lat1, r_lon1, r_lat0 = tile2bounds(r_z, r_x, r_y)
                i = i + 1
            r_x = r_x_ini
            r_y = r_y - 1
            r_quadkey = tile2quadkey(r_z, r_x, r_y)
            r_lon0, r_lat1, r_lon1, r_lat0 = tile2bounds(r_z, r_x, r_y)

        self._clean_raster()
        self._session.commit()
        self._create_spatial_index()

    def output(self):
        return PostgresTarget(classpath(self), unqualified_task_id(self.task_id))
