#!/usr/bin/env python

'''
Bigmetadata tasks

tasks to download and create metadata
'''

from luigi import Task, Parameter, LocalTarget, BooleanParameter
from tasks.util import (DefaultPostgresTarget, classpath, pg_cursor, shell,
                        CartoDBTarget, sql_to_cartodb_table)


class DMA(Task):

    URL = 'https://andrew.cartodb.com/api/v2/sql?q=select+*+from+andrew.dma_master_polygons&format=csv'

    def tablename(self):
        return '"{schema}".dma'.format(schema=classpath(self))

    def columns(self):
        return '''
"cartodb_id" int,
"the_geom" geometry,
"the_geom_webmercator" geometry,
"dma_code" int,
"dma_name" text
    '''

    def run(self):
        cursor = pg_cursor()
        cursor.execute('CREATE SCHEMA IF NOT EXISTS "{schema}"'.format(
            schema=classpath(self)))
        cursor.execute('''
DROP TABLE IF EXISTS {tablename};
CREATE TABLE {tablename} (
    {columns}
)
                       '''.format(tablename=self.tablename(),
                                  columns=self.columns()))
        cursor.connection.commit()
        shell('wget \'{url}\' -O - | '
              r"psql -c '\copy {tablename} FROM STDIN WITH CSV HEADER'".format(
                  url=self.URL,
                  tablename=self.tablename()))
        self.output().touch()

    def output(self):
        return DefaultPostgresTarget(table=self.tablename())


class ExtractDMA(Task):

    force = BooleanParameter(default=False)

    def tablename(self):
        return "us_dma_digital_marketing_areas"

    def requires(self):
        return DMA()

    def run(self):
        sql_to_cartodb_table(self.tablename(), self.input().table)

    def output(self):
        target = CartoDBTarget(self.tablename())
        if self.force and target.exists():
            target.remove()
        return target
