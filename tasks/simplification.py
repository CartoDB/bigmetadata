import os
from luigi import Task, Parameter, LocalTarget
from .util import shell
from .targets import PostgresTarget
from .meta import CurrentSession

OBSERVATORY_SCHEMA = 'observatory'
DEFAULT_GEOMFIELD = 'the_geom'
TMP_DIRECTORY = 'tmp'
SIMPLIFICATION_DIRECTORY = 'simplification'
SIMPLIFIED_SUFFIX = '_simpl'
SKIPFAILURES_NO = 'no'  # Avoids https://trac.osgeo.org/gdal/ticket/6803
SKIPFAILURES_YES = 'yes'
DEFAULT_P_RETAIN_FACTOR_MAPSHAPER = '10'
DEFAULT_P_RETAIN_FACTOR_POSTGIS = '50'  # Retain factors used for simplification (this is NOT a percentage) \
# The higher the retain factor, the lower the simplification
DEFAULT_MAX_MEMORY = '8192'


def tmp_directory(schema, table):
    return os.path.join(TMP_DIRECTORY, SIMPLIFICATION_DIRECTORY,
                        '{schema}.{table}'.format(schema=schema, table=table))


def shp_filename(table, suffix=''):
    return '{table}{suffix}.shp'.format(table=table, suffix=suffix)


class ExportShapefile(Task):
    schema = Parameter()
    table = Parameter()
    skipfailures = Parameter(default=SKIPFAILURES_NO)

    def run(self):
        with self.output().temporary_path() as temp_path:
            self.output().fs.mkdir(temp_path)
            cmd = 'ogr2ogr -f "ESRI Shapefile" {shapefile} ' \
                '{skipfailures} ' \
                '"PG:dbname=$PGDATABASE active_schema={schema}" {table} -nlt MultiPolygon'.format(
                    shapefile=os.path.join(temp_path, shp_filename(self.table)), schema=self.schema, table=self.table,
                    skipfailures='-skipfailures' if self.skipfailures.lower() == SKIPFAILURES_YES else '')
            shell(cmd)

    def output(self):
        return LocalTarget(tmp_directory(self.schema, self.table))


class SimplifyShapefile(Task):
    schema = Parameter()
    table_input = Parameter()
    table_output = Parameter()
    geomfield = Parameter(default=DEFAULT_GEOMFIELD)
    retainfactor = Parameter(default=DEFAULT_P_RETAIN_FACTOR_MAPSHAPER)
    skipfailures = Parameter(default=SKIPFAILURES_NO)
    maxmemory = Parameter(default=DEFAULT_MAX_MEMORY)

    def requires(self):
        return ExportShapefile(schema=self.schema, table=self.table_input, skipfailures=self.skipfailures)

    def run(self):
        factor = simplification_factor(self.schema, self.table_input, self.geomfield, self.retainfactor)

        with self.output().temporary_path() as temp_path:
            self.output().fs.mkdir(temp_path)
            cmd = 'node --max-old-space-size={maxmemory} `which mapshaper` ' \
                '{input} snap -simplify interval={interval} planar keep-shapes -o {output}'.format(
                    maxmemory=self.maxmemory,
                    input=os.path.join(self.input().path, shp_filename(self.table_input)),
                    interval=factor,
                    output=os.path.join(temp_path, shp_filename(self.table_output)))
            shell(cmd)

    def output(self):
        return LocalTarget(tmp_directory(self.schema, self.table_output))


class SimplifyGeometriesMapshaper(Task):
    schema = Parameter()
    table_input = Parameter()
    table_output = Parameter(default='')
    geomfield = Parameter(default=DEFAULT_GEOMFIELD)
    retainfactor = Parameter(default=DEFAULT_P_RETAIN_FACTOR_MAPSHAPER)
    skipfailures = Parameter(default=SKIPFAILURES_NO)
    maxmemory = Parameter(default=DEFAULT_MAX_MEMORY)

    def __init__(self, *args, **kwargs):
        super(SimplifyGeometriesMapshaper, self).__init__(*args, **kwargs)

    def requires(self):
        return SimplifyShapefile(schema=self.schema, table_input=self.table_input, table_output=self.table_output,
                                 geomfield=self.geomfield, retainfactor=self.retainfactor,
                                 skipfailures=self.skipfailures, maxmemory=self.maxmemory)

    def run(self):
        cmd = 'PG_USE_COPY=yes ' \
              'ogr2ogr -f PostgreSQL "PG:dbname=$PGDATABASE active_schema={schema}" ' \
              '-t_srs "EPSG:4326" -nlt MultiPolygon -nln {table} ' \
              '-lco OVERWRITE=yes -lco PRECISION=no -lco GEOMETRY_NAME={geomfield} ' \
              '-lco SCHEMA={schema} {shp_path} '.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                    geomfield=self.geomfield,
                    shp_path=os.path.join(self.input().path, shp_filename(self.table_output)))
        shell(cmd)

        session = CurrentSession().get()
        session.execute('UPDATE "{schema}".{table} '
                        'SET {geomfield}=ST_CollectionExtract(ST_MakeValid({geomfield}), 3)'.format(
                                schema=self.output().schema, table=self.output().tablename, geomfield=self.geomfield))
        session.commit()

    def output(self):
        return PostgresTarget(self.schema, self.table_output)


def simplification_factor(schema, table, geomfield, divisor_power):
    session = CurrentSession().get()
    return session.execute('SELECT '
                           'AVG(ST_Perimeter({geomfield}) / ST_NPoints({geomfield})) / 10 ^ ({divisor}::Decimal / 10) '
                           'FROM "{schema}".{table} WHERE ST_NPoints({geomfield}) > 0 AND ST_isValid({geomfield})'.format(
                            schema=schema, table=table, geomfield=geomfield, divisor=divisor_power
                           )).fetchone()[0]


class SimplifyGeometriesPostGIS(Task):
    schema = Parameter()
    table_input = Parameter()
    table_output = Parameter()
    geomfield = Parameter(default=DEFAULT_GEOMFIELD)
    retainfactor = Parameter(default=DEFAULT_P_RETAIN_FACTOR_POSTGIS)

    def __init__(self, *args, **kwargs):
        super(SimplifyGeometriesPostGIS, self).__init__(*args, **kwargs)

    def run(self):
        session = CurrentSession().get()

        columns = session.execute("SELECT column_name "
                                  "FROM information_schema.columns "
                                  "WHERE table_schema = '{schema}' "
                                  "AND table_name   = '{table}'".format(
                                    schema=self.schema, table=self.table_input.lower())).fetchall()

        factor = simplification_factor(self.schema, self.table_input, self.geomfield, self.retainfactor)

        simplified_geomfield = 'ST_CollectionExtract(ST_MakeValid(ST_SimplifyVW({geomfield}, {factor})), 3) ' \
                               '{geomfield}'.format(geomfield=self.geomfield, factor=factor)

        session.execute('CREATE TABLE "{schema}".{table_output} '
                        'AS SELECT {fields} '
                        'FROM "{schema}".{table_in} '.format(
                            schema=self.output().schema, table_in=self.table_input,
                            table_output=self.output().tablename,
                            fields=', '.join([x[0] if x[0] != self.geomfield else simplified_geomfield
                                             for x in columns])))
        session.commit()
        session.execute('CREATE INDEX {table_output}_{geomfield}_geo ON '
                        '"{schema}".{table_output} USING GIST ({geomfield})'.format(
                            table_output=self.output().tablename, geomfield=self.geomfield, schema=self.output().schema))

    def output(self):
        return PostgresTarget(self.schema, self.table_output)
