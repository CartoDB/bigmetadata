'''
Bigmetadata tasks

tasks to download and create metadata
'''

import os
import requests

from collections import OrderedDict
from luigi import Task, IntParameter, LocalTarget, BooleanParameter, Parameter
from tasks.util import (TableTarget, shell, classpath, underscore_slugify,
                        CartoDBTarget, sql_to_cartodb_table,
                        TableTask, ColumnsTask)
from tasks.meta import OBSColumn, current_session
from tasks.us.census.tiger import GeoidColumns
from psycopg2 import ProgrammingError


class DownloadZillow(Task):
    
    geography = Parameter(default='Zip') # example: Zip
    hometype = Parameter(default='SingleFamilyResidence') # example: SingleFamilyResidence
    
    URL = 'http://files.zillowstatic.com/research/public/{geography}/{geography}_Zhvi_{hometype}.csv'
    
    def run(self):
        self.output().makedirs()
        shell('wget \'{url}\' -O {output}'.format(
            url=self.URL.format(geography=self.geography, hometype=self.hometype), output=self.output().path))
        
    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id) + '.csv')


class ZillowColumns(ColumnsTask):

    def columns(self):
        # TODO manually generate columns before value columns

        columns = OrderedDict([
            ('RegionID', OBSColumn(type='Text',
                                   name='Zillow geography identifier',
                                   description="",
                                   weight=0)),
            ('RegionName', OBSColumn(type='Text',
                                     name='ZCTA5',
                                     description="",
                                     weight=0)),
            ('City', OBSColumn(type='Text',
                               name='City Name',
                               description="",
                               weight=0)),
            ('State', OBSColumn(type='Text',
                                name='State Name',
                                description="",
                                weight=0)),
            ('Metro', OBSColumn(type='Text',
                                name='Metro Area Name',
                                description="",
                                weight=0)),
            ('CountyName', OBSColumn(type='Text',
                                     name='County Name',
                                     description="",
                                     weight=0)),
            ('SizeRank', OBSColumn(type='Text',
                                   name='Size Rank',
                                   description="",
                                   weight=0)),
            ('State', OBSColumn(type='Text',
                                name='State Name',
                                description="",
                                weight=0))
        ])
        # TODO generate value columns
        for yr in xrange(1996, 2030):
            for mo in xrange(1, 13):
                yr_str = str(yr).zfill(2)
                mo_str = str(mo).zfill(2)
                
                columns['{yr}-{mo}'.format(yr=yr_str, mo=mo_str)] = OBSColumn(
                    type='Numeric',
                    name='ZHVI Value',
                    description='Zillow Home Value Index (ZHVI) for year {yr}, month {mo}'.format(yr=yr_str, mo=mo_str),
                    weight=0)
        return columns


class Zillow(TableTask):

    geography = Parameter(default='Zip') # example: Zip
    hometype = Parameter(default='SingleFamilyResidence') # example: SingleFamilyResidence
    
    def bounds(self):
        return 'BOX(0 0,0 0)'

    def timespan(self):
        return None

    def requires(self):
        return {
            'data': DownloadZillow(geography=self.geography, hometype=self.hometype),
            'metadata': ZillowColumns(),
            'geoids': GeoidColumns()
        }
    
    def columns(self):
        if self.geography == 'Zip':
            tiger_geo = 'zcta5'
        # elif self.geography == 'State':
        #     tiger_geo = 'geoid'
        else:
            raise Exception('unrecognized geography {}'.format(self.geography))
        
        columns = OrderedDict()

        with self.input()['data'].open() as fhandle:
            first_row = fhandle.next().strip().split(',')

        for headercell in first_row:
            headercell = headercell.strip('"')
            if headercell == 'RegionName':
                columns['region_name'] = self.input()['geoids'][tiger_geo + '_geoid']
            else:
                colname = underscore_slugify(headercell)
                if colname[0:2] in ('19', '20'):
                    colname = 'value_' + colname
                columns[colname] = self.input()['metadata'][headercell]
        
        return columns
    
    def populate(self):
        shell(r"psql -c '\copy {table} FROM {file_path} WITH CSV HEADER'".format(
            table     = self.output().table,
            file_path = self.input()['data'].path
        ))
