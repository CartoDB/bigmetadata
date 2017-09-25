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
UNKNOWN_RETAIN_FACTOR = '?'


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
    retainfactor = Parameter(default=UNKNOWN_RETAIN_FACTOR)

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


class MapshaperSimplification():
    def __init__(self, retainpercentage=DEFAULT_M_RETAIN_PERCENTAGE, skipfailures=SKIPFAILURES_NO):
        self.retainpercentage = retainpercentage
        self.skipfailures = skipfailures


class PostGISSimplification():
    def __init__(self, geomfield=DEFAULT_GEOMFIELD, retainfactor=UNKNOWN_RETAIN_FACTOR):
        self.geomfield = geomfield
        self.retainfactor = retainfactor


class Simplify(Task):
    schema = Parameter()

    dont_simplify = None
    '''
    Array of names of tables in the schema that we don't want to simplify
    Example: dont_simplify = ['mytable1', 'mytable2']
    '''

    override_defaults = None
    '''
    Array of Dict objects to override default simplification
    Example: [('mytable', PostGISSimplification(retainfactor=60))]
    (simplify 'mytable' using a PostGIS simplification with a retain factor of 60)
    '''

    executed = False

    def requires(self):
        for _tableid, _tablename in self.find_tables(self.schema):
            yield self.find_simplification(_tableid, _tablename)

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

        if self.override_defaults is not None:
            for _table, _simplification in self.override_defaults:
                if tableid.startswith('{schema}.{table}'.format(schema=self.schema, table=_table)):
                    if isinstance(_simplification, MapshaperSimplification):
                        simplification = SimplifyGeometriesMapshaper(OBSERVATORY_SCHEMA, tablename,
                                                                     _simplification.retainpercentage,
                                                                     _simplification.skipfailures)
                    elif isinstance(_simplification, PostGISSimplification):
                        simplification = SimplifyGeometriesPostGIS(OBSERVATORY_SCHEMA, tablename,
                                                                   _simplification.geomfield,
                                                                   _simplification.retainfactor)

        return simplification
