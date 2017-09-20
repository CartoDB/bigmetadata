import os
from luigi import Task, WrapperTask, Parameter, LocalTarget
from util import PostgresTarget, shell

TMP_DIRECTORY = 'tmp'
SIMPLIFICATION_DIRECTORY = 'simplification'
SIMPLIFIED_SUFFIX = '_simpl'
RETAIN_PERCENTAGE = '50'  # [0-100] Percentage of removable vertices retained


def tmp_directory(schema, table):
    return os.path.join(TMP_DIRECTORY, SIMPLIFICATION_DIRECTORY,
                        '{schema}.{table}'.format(schema=schema, table=table))


def shp_filename(schema, table, suffix=''):
    return '{schema}.{table}{suffix}.shp'.format(schema=schema, table=table, suffix=suffix)


class ExportShapefile(Task):
    schema = Parameter()
    table = Parameter()

    def run(self):
        self.output().makedirs()
        cmd = 'ogr2ogr -f "ESRI Shapefile" {shapefile} ' \
              '"PG:dbname=$PGDATABASE active_schema={schema}" {table}'.format(
                  shapefile=self.output().path, schema=self.schema, table=self.table)
        shell(cmd)

    def output(self):
        return LocalTarget(os.path.join(tmp_directory(self.schema, self.table), shp_filename(self.schema, self.table)))


class SimplifyShapefile(Task):
    schema = Parameter()
    table = Parameter()
    factor = Parameter(default=RETAIN_PERCENTAGE)

    def requires(self):
        yield ExportShapefile(schema=self.schema, table=self.table)

    def run(self):
        cmd = 'mapshaper {input} -simplify {factor}% -o {output}'.format(
                input=os.path.join(tmp_directory(self.schema, self.table),
                                   shp_filename(self.schema, self.table)),
                factor=self.factor,
                output=self.output().path)
        shell(cmd)

    def output(self):
        return LocalTarget(os.path.join(tmp_directory(self.schema, self.table),
                                        shp_filename(self.schema, self.table, SIMPLIFIED_SUFFIX)))


class ImportSimplifiedShapefile(Task):
    schema = Parameter()
    table = Parameter()
    outsuffix = Parameter(default='')
    factor = Parameter(default=RETAIN_PERCENTAGE)

    executed = False

    def requires(self):
        yield SimplifyShapefile(schema=self.schema, table=self.table, factor=self.factor)

    def run(self):
        cmd = 'PG_USE_COPY=yes ' \
              'ogr2ogr -f PostgreSQL "PG:dbname=$PGDATABASE active_schema={schema}" -overwrite ' \
              '-t_srs "EPSG:4326" -nlt MultiPolygon -nln {table} ' \
              '-lco OVERWRITE=yes -lco PRECISION=no ' \
              '-lco SCHEMA={schema} {shp_path} '.format(
                    table=self.output().tablename,
                    schema=self.output().schema,
                    shp_path=os.path.join(tmp_directory(self.schema, self.table),
                                          shp_filename(self.schema, self.table, SIMPLIFIED_SUFFIX)))
        shell(cmd)
        self.executed = True

    def output(self):
        return PostgresTarget(self.schema, self.table + self.outsuffix)

    def complete(self):
        if not self.executed:
            return False
        return self.output().exists()


class SimplifyGeometries(WrapperTask):
    schema = Parameter()
    table = Parameter()
    outsuffix = Parameter(default='')
    factor = Parameter(default=RETAIN_PERCENTAGE)

    def requires(self):
        yield ImportSimplifiedShapefile(schema=self.schema, table=self.table, outsuffix=self.outsuffix,
                                        factor=self.factor)
