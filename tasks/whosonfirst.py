#!/usr/bin/env python

'''
Bigmetadata tasks
'''

from tasks.meta import current_session
from tasks.util import (classpath, shell, TempTableTask)

from csv import DictReader
from luigi import Task, Parameter, BooleanParameter, WrapperTask

import requests
import subprocess


class ImportWOFResolution(TempTableTask):

    force = BooleanParameter(default=False)
    resolution = Parameter()
    URL = 'https://raw.githubusercontent.com/whosonfirst/whosonfirst-data/master/meta/wof-{resolution}-latest.csv'

    def run(self):
        resp = requests.get(self.URL.format(resolution=self.resolution))
        encoded = resp.text.encode(resp.headers['Content-Type'].split('charset=')[1])
        reader = DictReader(encoded.split('\r\n'))
        cursor = current_session()
        cursor.execute('CREATE SCHEMA IF NOT EXISTS "{}"'.format(classpath(self)))
        cursor.connection.commit()

        created_table = False
        for i, line in enumerate(reader):
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


class WOFColumns(Task):
    pass
