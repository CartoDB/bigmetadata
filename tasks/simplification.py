import os
from luigi import Task, WrapperTask, Parameter, LocalTarget
from util import PostgresTarget, shell
from meta import CurrentSession

DEFAULT_GEOMFIELD = 'the_geom'
TMP_DIRECTORY = 'tmp'
SIMPLIFICATION_DIRECTORY = 'simplification'
SIMPLIFIED_SUFFIX = '_simpl'
SKIPFAILURES_NO = 'no'  # Avoids https://trac.osgeo.org/gdal/ticket/6803
SKIPFAILURES_YES = 'yes'
DEFAULT_M_RETAIN_PERCENTAGE = '50'  # [0-100] Percentage of removable vertices retained by mapshaper
DEFAULT_P_RETAIN_FACTOR = '50'  # Retain factor used for PostGIS simplification (this is NOT a percentage)
                                # The higher the retain factor, the lower the simplification


def tmp_directory(schema, table):
    return os.path.join(TMP_DIRECTORY, SIMPLIFICATION_DIRECTORY,
                        '{schema}.{table}'.format(schema=schema, table=table))


def shp_filename(schema, table, suffix=''):
    return '{schema}.{table}{suffix}.shp'.format(schema=schema, table=table, suffix=suffix)


class ExportShapefile(Task):
    schema = Parameter()
    table = Parameter()
    skipfailures = Parameter(default=SKIPFAILURES_NO)

    def run(self):
        self.output().makedirs()
        cmd = 'ogr2ogr -f "ESRI Shapefile" {shapefile} ' \
              '{skipfailures} ' \
              '"PG:dbname=$PGDATABASE active_schema={schema}" {table}'.format(
                  shapefile=self.output().path, schema=self.schema, table=self.table,
                  skipfailures='-skipfailures' if self.skipfailures.lower() == SKIPFAILURES_YES else '')
        shell(cmd)

    def output(self):
        return LocalTarget(os.path.join(tmp_directory(self.schema, self.table), shp_filename(self.schema, self.table)))


class SimplifyShapefile(Task):
    schema = Parameter()
    table = Parameter()
    retainpercentage = Parameter(default=DEFAULT_M_RETAIN_PERCENTAGE)
    skipfailures = Parameter(default=SKIPFAILURES_NO)

    def requires(self):
        yield ExportShapefile(schema=self.schema, table=self.table, skipfailures=self.skipfailures)

    def run(self):
        cmd = 'node --max-old-space-size=8192 `which mapshaper` ' \
              '{input} snap -simplify {retainpercentage}% keep-shapes -o {output}'.format(
                input=os.path.join(tmp_directory(self.schema, self.table),
                                   shp_filename(self.schema, self.table)),
                retainpercentage=self.retainpercentage,
                output=self.output().path)
        shell(cmd)

    def output(self):
        return LocalTarget(os.path.join(tmp_directory(self.schema, self.table),
                                        shp_filename(self.schema, self.table, SIMPLIFIED_SUFFIX)))


class ImportSimplifiedShapefile(Task):
    schema = Parameter()
    table = Parameter()
    retainpercentage = Parameter(default=DEFAULT_M_RETAIN_PERCENTAGE)
    skipfailures = Parameter(default=SKIPFAILURES_NO)

    executed = False

    def requires(self):
        yield SimplifyShapefile(schema=self.schema, table=self.table, retainpercentage=self.retainpercentage,
                                skipfailures=self.skipfailures)

    def run(self):
        session = CurrentSession().get()

        session.execute('TRUNCATE TABLE "{schema}".{table} '.format(
                         schema=self.output().schema, table=self.output().tablename))
        session.execute("COMMIT")

        cmd = 'PG_USE_COPY=yes ' \
              'ogr2ogr -f PostgreSQL "PG:dbname=$PGDATABASE active_schema={schema}" ' \
              '-t_srs "EPSG:4326" -nlt MultiPolygon -nln {table} ' \
              '-lco OVERWRITE=yes -lco PRECISION=no -lco GEOMETRY_NAME= ' \
              '-lco SCHEMA={schema} {shp_path} '.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                    shp_path=os.path.join(tmp_directory(self.schema, self.table),
                                          shp_filename(self.schema, self.table, SIMPLIFIED_SUFFIX)))
        shell(cmd)
        self.executed = True

    def output(self):
        return PostgresTarget(self.schema, self.table)

    def complete(self):
        if not self.executed:
            return False
        return self.output().exists()


class SimplifyGeometriesMapshaper(WrapperTask):
    schema = Parameter()
    table = Parameter()
    retainpercentage = Parameter(default=DEFAULT_M_RETAIN_PERCENTAGE)
    skipfailures = Parameter(default=SKIPFAILURES_NO)

    def requires(self):
        yield ImportSimplifiedShapefile(schema=self.schema, table=self.table, retainpercentage=self.retainpercentage,
                                        skipfailures=self.skipfailures)


def postgis_simplification_factor(schema, table, geomfield, divisor_power):
    session = CurrentSession().get()
    return session.execute('SELECT AVG(ST_Perimeter({geomfield}) / ST_NPoints({geomfield})) / 10 ^ ({divisor} / 10) '
                           'from "{schema}".{table}'.format(
                            schema=schema, table=table, geomfield=geomfield, divisor=divisor_power
                           )).fetchone()[0]


class SimplifyGeometriesPostGIS(Task):
    schema = Parameter()
    table = Parameter()
    geomfield = Parameter(default=DEFAULT_GEOMFIELD)
    retainfactor = Parameter(default='?')

    executed = False

    def run(self):
        session = CurrentSession().get()

        if self.retainfactor == '?':
            self.retainfactor = DEFAULT_P_RETAIN_FACTOR

        factor = postgis_simplification_factor(self.schema, self.table, self.geomfield, self.retainfactor)

        session.execute('UPDATE "{schema}".{table} '
                        'SET {geomfield} = ST_MakeValid(ST_SimplifyVW({geomfield}, {factor})); '.format(
                            schema=self.schema, table=self.table, geomfield=self.geomfield, factor=factor))
        session.execute("COMMIT")

        self.executed = True

    def output(self):
        return PostgresTarget(self.schema, self.table)

    def complete(self):
        if not self.executed:
            return False
        return self.output().exists()
