import os
from luigi import Task, WrapperTask, Parameter, LocalTarget
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
        with self.output().temporary_path() as tempfile:
            cmd = 'ogr2ogr -f "ESRI Shapefile" {shapefile} ' \
                  '{skipfailures} ' \
                  '"PG:dbname=$PGDATABASE active_schema={schema}" {table}'.format(
                      shapefile=tempfile, schema=self.schema, table=self.table,
                      skipfailures='-skipfailures' if self.skipfailures.lower() == SKIPFAILURES_YES else '')
            shell(cmd)

    def output(self):
        return LocalTarget(tmp_directory(self.schema, self.table))


class SimplifyShapefile(Task):
    schema = Parameter()
    table = Parameter()
    retainpercentage = Parameter(default=DEFAULT_M_RETAIN_PERCENTAGE)
    skipfailures = Parameter(default=SKIPFAILURES_NO)
    maxmemory = Parameter(default=DEFAULT_MAX_MEMORY)

    def requires(self):
        return ExportShapefile(schema=self.schema, table=self.table, skipfailures=self.skipfailures)

    def run(self):
        cmd = 'node --max-old-space-size={maxmemory} `which mapshaper` ' \
              '{input} snap -simplify {retainpercentage}% keep-shapes -o {output}'.format(
                maxmemory=self.maxmemory,
                input=os.path.join(tmp_directory(self.schema, self.table),
                                   shp_filename(self.table)),
                retainpercentage=self.retainpercentage,
                output=self.output().path)
        shell(cmd)

    def output(self):
        return LocalTarget(os.path.join(tmp_directory(self.schema, self.table),
                                        shp_filename(self.table, SIMPLIFIED_SUFFIX)))


class ImportSimplifiedShapefile(Task):
    schema = Parameter()
    table = Parameter()
    retainpercentage = Parameter(default=DEFAULT_M_RETAIN_PERCENTAGE)
    skipfailures = Parameter(default=SKIPFAILURES_NO)
    geomname = Parameter(default=DEFAULT_GEOMFIELD)
    maxmemory = Parameter(default=DEFAULT_MAX_MEMORY)

    def requires(self):
        return SimplifyShapefile(schema=self.schema, table=self.table, retainpercentage=self.retainpercentage,
                                 skipfailures=self.skipfailures, maxmemory=self.maxmemory)

    def run(self):
        cmd = 'PG_USE_COPY=yes ' \
              'ogr2ogr -f PostgreSQL "PG:dbname=$PGDATABASE active_schema={schema}" ' \
              '-t_srs "EPSG:4326" -nlt MultiPolygon -nln {table} ' \
              '-lco OVERWRITE=yes -lco PRECISION=no -lco GEOMETRY_NAME={geomname} ' \
              '-lco SCHEMA={schema} {shp_path} '.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                    geomname=self.geomname,
                    shp_path=os.path.join(tmp_directory(self.schema, self.table),
                                          shp_filename(self.table, SIMPLIFIED_SUFFIX)))
        shell(cmd)

    def output(self):
        return PostgresTarget(self.schema, self.table + SIMPLIFIED_SUFFIX)


class SimplifyGeometriesMapshaper(WrapperTask):
    schema = Parameter()
    table = Parameter()
    retainpercentage = Parameter(default=DEFAULT_M_RETAIN_PERCENTAGE)
    skipfailures = Parameter(default=SKIPFAILURES_NO)
    geomname = Parameter(default=DEFAULT_GEOMFIELD)
    maxmemory = Parameter(default=DEFAULT_MAX_MEMORY)

    def requires(self):
        return ImportSimplifiedShapefile(schema=self.schema, table=self.table, retainpercentage=self.retainpercentage,
                                         skipfailures=self.skipfailures, geomname=self.geomname,
                                         maxmemory=self.maxmemory)


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
    retainfactor = Parameter(default=DEFAULT_P_RETAIN_FACTOR)

    def run(self):
        session = CurrentSession().get()

        columns = session.execute("SELECT column_name "
                                  "FROM information_schema.columns "
                                  "WHERE table_schema = '{schema}'"
                                  "AND table_name   = '{table}'".format(
                                    schema=self.schema, table=self.table)).fetchall()

        factor = postgis_simplification_factor(self.schema, self.table, self.geomfield, self.retainfactor)

        simplified_geomfield = 'ST_MakeValid(ST_SimplifyVW({geomfield}, {factor})) {geomfield}'.format(
                                geomfield=self.geomfield, factor=factor)

        session.execute('CREATE TABLE "{schema}".{table_out} '
                        'AS SELECT {fields} '
                        'FROM "{schema}".{table_in} '.format(
                            schema=self.output().schema, table_in=self.table, table_out=self.output().tablename,
                            fields=', '.join([x[0] if x[0] != self.geomfield else simplified_geomfield
                                             for x in columns])))
        session.execute('COMMIT')

    def output(self):
        return PostgresTarget(self.schema, self.table + SIMPLIFIED_SUFFIX)


class MapshaperSimplification():
    def __init__(self, retainpercentage=DEFAULT_M_RETAIN_PERCENTAGE, skipfailures=SKIPFAILURES_NO,
                 maxmemory=DEFAULT_MAX_MEMORY):
        self.retainpercentage = retainpercentage
        self.skipfailures = skipfailures
        self.maxmemory = maxmemory


class PostGISSimplification():
    def __init__(self, geomfield=DEFAULT_GEOMFIELD, retainfactor=DEFAULT_P_RETAIN_FACTOR):
        self.geomfield = geomfield
        self.retainfactor = retainfactor


class Simplify(Task):
    schema = Parameter()

    dont_simplify = None
    '''
    Array of names of tables in the schema that we don't want to simplify
    Example: dont_simplify = ['mytable1', 'mytable2']
    '''

    override_defaults = []
    '''
    Array of Dict objects to override default simplification
    Example: [('mytable', PostGISSimplification(retainfactor=60))]
    (simplify 'mytable' using a PostGIS simplification with a retain factor of 60)
    '''

    executed = False

    def requires(self):
        tasks = []
        for _tableid, _tablename in self.find_tables(self.schema):
            tasks.append(self.find_simplification(_tableid, _tablename))
        return tasks

    def run(self):
        self.executed = True

    def complete(self):
        return self.executed

    def find_tables(self, schema):
        session = CurrentSession().get()

        return session.execute("SELECT id, tablename FROM observatory.obs_table "
                               "WHERE the_geom IS NOT NULL AND id LIKE '{schema}%' "
                               "{dont_simplify} ".format(
                                    schema=schema,
                                    dont_simplify="" if self.dont_simplify is None
                                                  else "AND id NOT SIMILAR TO '" + self.schema + ".("
                                                  + "|".join([x for x in self.dont_simplify]) + ")%'"
                                )).fetchall()

    def find_simplification(self, tableid, tablename):
        simplification = SimplifyGeometriesMapshaper(OBSERVATORY_SCHEMA, tablename)

        for _table, _simplification in self.override_defaults:
            if tableid.startswith('{schema}.{table}'.format(schema=self.schema, table=_table)):
                if isinstance(_simplification, MapshaperSimplification):
                    simplification = SimplifyGeometriesMapshaper(OBSERVATORY_SCHEMA, tablename,
                                                                 _simplification.retainpercentage,
                                                                 _simplification.skipfailures,
                                                                 _simplification.maxmemory)
                elif isinstance(_simplification, PostGISSimplification):
                    simplification = SimplifyGeometriesPostGIS(OBSERVATORY_SCHEMA, tablename,
                                                               _simplification.geomfield,
                                                               _simplification.retainfactor)

        return simplification
