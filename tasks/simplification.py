import os
from luigi import Task, Parameter, LocalTarget
from util import PostgresTarget, shell
from meta import CurrentSession

OBSERVATORY_SCHEMA = 'observatory'
DEFAULT_GEOMFIELD = 'the_geom'
TMP_DIRECTORY = 'tmp'
SIMPLIFICATION_DIRECTORY = 'simplification'
SIMPLIFIED_SUFFIX = '_simpl'
SKIPFAILURES_NO = 'no'  # Avoids https://trac.osgeo.org/gdal/ticket/6803
SKIPFAILURES_YES = 'yes'
DEFAULT_M_RETAIN_PERCENTAGE = '50'  # [0-100] Percentage of removable vertices retained by mapshaper
DEFAULT_P_RETAIN_FACTOR = '50'  # Retain factor used for PostGIS simplification (this is NOT a percentage) \
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
        self.output().makedirs()
        cmd = 'ogr2ogr -f "ESRI Shapefile" {shapefile} ' \
              '{skipfailures} ' \
              '"PG:dbname=$PGDATABASE active_schema={schema}" {table} '.format(
                  shapefile=self.output().path, schema=self.schema, table=self.table,
                  skipfailures='-skipfailures' if self.skipfailures.lower() == SKIPFAILURES_YES else '')
        shell(cmd)

    def output(self):
        return LocalTarget(os.path.join(tmp_directory(self.schema, self.table),
                                        shp_filename(self.table)))


class SimplifyShapefile(Task):
    schema = Parameter()
    table_input = Parameter()
    table_output = Parameter()
    retainpercentage = Parameter(default=DEFAULT_M_RETAIN_PERCENTAGE)
    skipfailures = Parameter(default=SKIPFAILURES_NO)
    maxmemory = Parameter(default=DEFAULT_MAX_MEMORY)

    def requires(self):
        return ExportShapefile(schema=self.schema, table=self.table_input, skipfailures=self.skipfailures)

    def run(self):
        cmd = 'node --max-old-space-size={maxmemory} `which mapshaper` ' \
              '{input} snap -simplify {retainpercentage}% keep-shapes -o {output}'.format(
                maxmemory=self.maxmemory,
                input=os.path.join(tmp_directory(self.schema, self.table_input),
                                   shp_filename(self.table_input)),
                retainpercentage=self.retainpercentage,
                output=self.output().path)
        shell(cmd)

    def output(self):
        return LocalTarget(os.path.join(tmp_directory(self.schema, self.table_input),
                                        shp_filename(self.table_output)))


class SimplifyGeometriesMapshaper(Task):
    schema = Parameter()
    table_input = Parameter()
    table_output = Parameter()
    geomfield = Parameter(default=DEFAULT_GEOMFIELD)
    retainpercentage = Parameter(default=DEFAULT_M_RETAIN_PERCENTAGE)
    skipfailures = Parameter(default=SKIPFAILURES_NO)
    maxmemory = Parameter(default=DEFAULT_MAX_MEMORY)

    def requires(self):
        return SimplifyShapefile(schema=self.schema, table_input=self.table_input, table_output=self.table_output,
                                 retainpercentage=self.retainpercentage, skipfailures=self.skipfailures,
                                 maxmemory=self.maxmemory)

    def run(self):
        cmd = 'PG_USE_COPY=yes ' \
              'ogr2ogr -f PostgreSQL "PG:dbname=$PGDATABASE active_schema={schema}" ' \
              '-t_srs "EPSG:4326" -nlt MultiPolygon -nln {table} ' \
              '-lco OVERWRITE=yes -lco PRECISION=no -lco GEOMETRY_NAME={geomfield} ' \
              '-lco SCHEMA={schema} {shp_path} '.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                    geomfield=self.geomfield,
                    shp_path=os.path.join(tmp_directory(self.schema, self.table_input),
                                          shp_filename(self.table_output)))
        shell(cmd)

    def output(self):
        return PostgresTarget(self.schema, self.table_output)


def postgis_simplification_factor(schema, table, geomfield, divisor_power):
    session = CurrentSession().get()
    return session.execute('SELECT AVG(ST_Perimeter({geomfield}) / ST_NPoints({geomfield})) / 10 ^ ({divisor} / 10) '
                           'from "{schema}".{table}'.format(
                            schema=schema, table=table, geomfield=geomfield, divisor=divisor_power
                           )).fetchone()[0]


class SimplifyGeometriesPostGIS(Task):
    schema = Parameter()
    table_input = Parameter()
    table_output = Parameter()
    geomfield = Parameter(default=DEFAULT_GEOMFIELD)
    retainfactor = Parameter(default=DEFAULT_P_RETAIN_FACTOR)

    def run(self):
        session = CurrentSession().get()

        columns = session.execute("SELECT column_name "
                                  "FROM information_schema.columns "
                                  "WHERE table_schema = '{schema}'"
                                  "AND table_name   = '{table}'".format(
                                    schema=self.schema, table=self.table_input)).fetchall()

        factor = postgis_simplification_factor(self.schema, self.table_input, self.geomfield, self.retainfactor)

        simplified_geomfield = 'ST_MakeValid(ST_SimplifyVW({geomfield}, {factor})) {geomfield}'.format(
                                geomfield=self.geomfield, factor=factor)

        session.execute('CREATE TABLE "{schema}".{table_out} '
                        'AS SELECT {fields} '
                        'FROM "{schema}".{table_in} '.format(
                            schema=self.output().schema, table_in=self.table_input, table_out=self.output().tablename,
                            fields=', '.join([x[0] if x[0] != self.geomfield else simplified_geomfield
                                             for x in columns])))
        session.execute('COMMIT')

    def output(self):
        return PostgresTarget(self.schema, self.table_output)
