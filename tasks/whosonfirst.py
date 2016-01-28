#!/usr/bin/env python

'''
Bigmetadata tasks
'''

from tasks.util import (classpath, DefaultPostgresTarget, pg_cursor, shell,
                        CartoDBTarget, sql_to_cartodb_table)

from csv import DictReader
from luigi import Task, Parameter, BooleanParameter

import requests
import subprocess


class ImportWhosOnFirstResolution(Task):

    force = BooleanParameter(default=False)
    resolution = Parameter()
    URL = 'https://raw.githubusercontent.com/whosonfirst/whosonfirst-data/master/meta/wof-{resolution}-latest.csv'

    def run(self):
        resp = requests.get(self.URL.format(resolution=self.resolution))
        reader = DictReader(resp.text.split('\r\n'))
        cursor = pg_cursor()
        cursor.execute('CREATE SCHEMA IF NOT EXISTS "{}"'.format(classpath(self)))
        cursor.connection.commit()

        created_table = False
        for line in reader:
            # TODO would be much, much faster in parallel...
            url = 'https://whosonfirst.mapzen.com/data/{path}'.format(path=line['path'])
            lfs_url = 'https://github.com/whosonfirst/whosonfirst-data/raw/master/data/{path}'.format(
                path=line['path'])
            cmd = 'wget \'{url}\' -O - | ogr2ogr -{operation} -nlt MULTIPOLYGON -nln \'{table}\' ' \
                    '-f PostgreSQL PG:"dbname=$PGDATABASE" /vsistdin/'.format(
                        url=url,
                        operation='append' if created_table else 'overwrite',
                        table=self.output().table
                    )
            try:
                shell(cmd)
            except subprocess.CalledProcessError:
                cmd = cmd.replace(url, lfs_url)
                shell(cmd)
            created_table = True
        self.output().touch()

    def output(self):
        target = DefaultPostgresTarget(table=classpath(self) + '.' + self.resolution)
        if self.force:
            target.untouch()
        return target


class ExportWhosOnFirstResolution(Task):

    force = BooleanParameter(default=False)
    resolution = Parameter()

    def requires(self):
        return ImportWhosOnFirstResolution(resolution=self.resolution)

    def tablename(self):
        return self.input().table.replace('.', '_')

    def run(self):
        query = u'SELECT wkb_geometry as geom, ' \
                u'"wof:placetype" as placetype, ' \
                u'"wof:name" as name FROM {table}'.format(
                    table=self.input().table)
        sql_to_cartodb_table(self.tablename(), query)

    def output(self):
        target = CartoDBTarget(self.tablename())
        if self.force and target.exists():
            target.remove()
        return target
