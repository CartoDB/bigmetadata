#!/usr/bin/env python

'''
Bigmetadata tasks
'''

import subprocess
from csv import DictReader
from collections import OrderedDict

import requests
from luigi import Task, Parameter, BooleanParameter, WrapperTask

from tasks.meta import current_session, OBSColumn, OBSTag
from tasks.util import (classpath, shell, TempTableTask, TableTask,
                        ColumnsTask, TagsTask)


class GlobalBoundaries(TagsTask):

    def version(self):
        return 2

    def tags(self):
        return [OBSTag(id='global',
                       type='catalog',
                       name='Global Boundaries',
                       description='',
                      )]


class DownloadWOF(TempTableTask):

    resolution = Parameter()
    URL = 'https://raw.githubusercontent.com/whosonfirst/whosonfirst-data/master/meta/wof-{resolution}-latest.csv'

    def run(self):
        resp = requests.get(self.URL.format(resolution=self.resolution))
        encoded = resp.text.encode(resp.headers['Content-Type'].split('charset=')[1])
        reader = DictReader(encoded.split('\r\n'))

        for i, line in enumerate(reader):
            # TODO would be much, much faster in parallel...
            url = 'https://whosonfirst.mapzen.com/data/{path}'.format(path=line['path'])
            lfs_url = 'https://github.com/whosonfirst/whosonfirst-data/raw/master/data/{path}'.format(
                path=line['path'])
            cmd = 'wget \'{url}\' -O - | ogr2ogr -{operation} ' \
                    '-nlt MULTIPOLYGON -nln \'{table}\' ' \
                    '-f PostgreSQL PG:"dbname=$PGDATABASE ' \
                    'active_schema={schema}" /vsistdin/'.format(
                        url=url,
                        schema=self.output().schema,
                        table=self.output().tablename,
                        operation='append' if i > 0 else 'overwrite -lco OVERWRITE=yes'
                    )
            try:
                shell(cmd)
            except subprocess.CalledProcessError:
                cmd = cmd.replace(url, lfs_url)
                shell(cmd)


class WOFColumns(ColumnsTask):

    resolution = Parameter()

    def version(self):
        return 2

    def requires(self):
        return {
            'global': GlobalBoundaries()
        }

    def columns(self):
        global_tag = self.input()['global']['global']

        geom_names = {
            'continent': 'Continents',
            'country': 'Countries',
            'disputed': 'Disputed Areas',
            'marinearea': 'Marine Areas',
            'region': 'Regions (First-level Administrative)',
        }

        geom_descriptions = {
            'continent': 'Continents of the world.',
            'country': ' ',
            'disputed': ' ',
            'marinearea': ' ',
            'region': ' ',
        }

        return OrderedDict([
            ('wof_id', OBSColumn(
                id='wof_' + self.resolution + '_id',
                name="Who's on First ID",
                type="Numeric",
                weight=0,
            )),
            ('the_geom', OBSColumn(
                id='wof_' + self.resolution + '_geom',
                name=geom_names[self.resolution],
                type="Geometry",
                weight=5,
                description=geom_descriptions[self.resolution],
                tags=[global_tag],
            )),
            ('name', OBSColumn(
                id='wof_' + self.resolution + '_name',
                #name=name_names[self.resolution],
                type="Text",
                weight=0
                #description=name_descriptions[self.resolution],
            )),
            #('placetype', OBSColumn(
            #    id='wof:placetype'
            #)),
        ])


class WOF(TableTask):

    resolution = Parameter()

    def bounds(self):
        return 'BOX(0 0,0 0)'

    def timespan(self):
        return '2016'

    def version(self):
        return 2

    def requires(self):
        return {
            'columns': WOFColumns(resolution=self.resolution),
            'data': DownloadWOF(resolution=self.resolution),
        }

    def columns(self):
        return self.input()['columns']

    def populate(self):
        session = current_session()

        session.execute('INSERT INTO {output} '
                        'SELECT "wof:id", '
                        'CASE WHEN ST_Npoints(wkb_geometry) > 1000000 '
                        '     THEN ST_Simplify(wkb_geometry, 0.0001) '
                        '     ELSE wkb_geometry '
                        'END, '
                        '"wof:name" '
                        'FROM {input} '.format(
                            output=self.output().table,
                            input=self.input()['data'].table
                        ))


class AllWOF(WrapperTask):

    def requires(self):
        for resolution in ('continent', 'country', 'disputed', 'marinearea',
                           'region', ):
            yield WOFColumns(resolution=resolution)
            yield WOF(resolution=resolution)
