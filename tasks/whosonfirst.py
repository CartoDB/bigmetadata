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
                        ColumnsTask)
from tasks.us.census.tiger import ShorelineClip, DownloadTiger
from tasks.tags import SectionTags, SubsectionTags


class DownloadWOF(TempTableTask):

    resolution = Parameter()
    URL = 'https://media.githubusercontent.com/media/whosonfirst-data/whosonfirst-data/master/meta/wof-{resolution}-latest.csv'

    def run(self):
        resp = requests.get(self.URL.format(resolution=self.resolution))
        #encoded = resp.text.encode(resp.headers['Content-Type'].split('charset=')[1])
        reader = DictReader(resp.text.encode('utf8').split('\r\n'))

        for i, line in enumerate(reader):
            # TODO would be much, much faster in parallel...
            url = 'https://whosonfirst.mapzen.com/data/{path}'.format(path=line['path'])
            lfs_url = 'https://github.com/whosonfirst/whosonfirst-data/raw/master/data/{path}'.format(
                path=line['path'])
            cmd = 'wget \'{url}\' -O - | ogr2ogr -{operation} ' \
                    '-nlt Geometry -nln \'{table}\' ' \
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
        return 6

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
        }

    def columns(self):
        glob = self.input()['sections']['global']
        boundaries = self.input()['subsections']['boundary']

        geom_names = {
            'continent': 'Continents',
            'country': 'Countries',
            'county': 'Counties',
            'disputed': 'Disputed Areas',
            'marinearea': 'Marine Areas',
            'region': 'Regions (First-level Administrative)',
        }

        #geom_descriptions = {
        #    'continent': 'Continents of the world',
        #    'country': 'Countries of the world',
        #    'disputed': 'Disputed territories of the world',
        #    'marinearea': 'Marine ',
        #    'region': ' ',
        #}
        the_geom = OBSColumn(
            id='wof_' + self.resolution + '_geom',
            name=geom_names[self.resolution],
            type="Geometry",
            weight=5,
            description='',
            tags=[glob, boundaries],
        )

        return OrderedDict([
            ('wof_id', OBSColumn(
                id='wof_' + self.resolution + '_id',
                name="Who's on First ID",
                type="Numeric",
                weight=0,
                targets={the_geom: 'geom_ref'},
            )),
            ('the_geom', the_geom),
            ('name', OBSColumn(
                id='wof_' + self.resolution + '_name',
                #name=name_names[self.resolution],
                type="Text",
                weight=1,
                tags=[glob]
                #description=name_descriptions[self.resolution],
            )),
            #('placetype', OBSColumn(
            #    id='wof:placetype'
            #)),
        ])


class WOF(TableTask):

    resolution = Parameter()

    def timespan(self):
        return '2016'

    def version(self):
        return 11

    def requires(self):
        requirements = {
            'columns': WOFColumns(resolution=self.resolution),
            'data': DownloadWOF(resolution=self.resolution),
        }
        if self.resolution == 'region':
            requirements['shoreline'] = ShorelineClip(year=2014, geography='state')
            requirements['tiger'] = DownloadTiger(year=2014)

        return requirements

    def columns(self):
        return self.input()['columns']

    def populate(self):
        session = current_session()

        session.execute('INSERT INTO {output} '
                        'SELECT "wof:id", '
                        'CASE WHEN ST_Npoints(wkb_geometry) > 100000 '
                        '     THEN ST_MakeValid(ST_SimplifyVW(wkb_geometry, 0.0001)) '
                        '     ELSE wkb_geometry '
                        'END, '
                        '"wof:name" '
                        'FROM {input} '.format(
                            output=self.output().table,
                            input=self.input()['data'].table
                        ))

        # replace default WOF US states with our clipped versions
        if self.resolution == 'region':
            for geoid, statename in session.execute('SELECT geoid, name FROM tiger2014.state'):
                session.execute('UPDATE {output} out '
                                'SET the_geom = shoreline.the_geom '
                                'FROM {shoreline} shoreline '
                                'WHERE shoreline.geoid = \'{geoid}\' '
                                '  AND out.name ILIKE \'{statename}\' '.format(
                                    shoreline=self.input()['shoreline'].table,
                                    output=self.output().table,
                                    geoid=geoid,
                                    statename=statename))


class AllWOF(WrapperTask):

    def requires(self):
        for resolution in ('continent', 'country', 'disputed', 'marinearea',
                           'region', 'county', ):
            yield WOFColumns(resolution=resolution)
            yield WOF(resolution=resolution)
