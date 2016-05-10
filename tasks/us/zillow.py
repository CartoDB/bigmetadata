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


class ExtractAllZillow(WrapperTask):
    
    def requires(self):            
        ## go across all types
        geographies = ('State', 'Metro', 'County', 'City', 'Zip', 'Neighborhood',)
        
        ## cherry-picked datasets
        hometypes = ('AllHomes', 'SingleFamilyResidence',)
        rentaltypes = ('AllHomesPlusMultifamily','SingleFamilyResidenceRental', 'Sfr','AllHomes',)
        
        ## Zillow Measures
        home_measure = 'Zhvi'
        rental_measure = 'Zri'
        
        ## Median Value per SqFt
        hometypes_medians =   ('AllHomes',)
        rentaltypes_medians = ('AllHomes', 'Sfr', 'CondoCoop',)
        
        ## Measures for amount per square foot
        median_sqft_houses = 'MedianValuePerSqft'
        median_sqft_rental = 'MedianRentalPricePerSqft'
        
        for g in geographies:
            for h in hometypes:
                yield DownloadZillow(geography=g, hometype=h, measure=home_measure)
            for r in rentaltypes:
                yield DownloadZillow(geography=g, hometype=h, measure=home_measure)
            for hm in hometypes_medians:
                yield DownloadZillow(geography=g, hometype=hm, measure=median_sqft_houses)
            for rm in rentaltypes_medians:
                yield DownloadZillow(geography=g, hometype=rm, measure=median_sqft_rental)
            

class DownloadZillow(Task):
    
    geography = Parameter(default='Zip')
    hometype = Parameter(default='SingleFamilyResidence')
    measure = Parameter(default='Zhvi')
    
    
    URL = 'http://files.zillowstatic.com/research/public/{geography}/{geography}_{measure}_{hometype}.csv'
    
    def run(self):
        self.output().makedirs()
        shell('wget \'{url}\' -O {output}'.format(
            url=self.URL.format(geography=self.geography, hometype=self.hometype), measure=self.measure, output=self.output().path))
        
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
                                     description="Zip Code Tabulation Area id",
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
        elif self.geography == 'State':
            tiger_geo = 'geoid'
        elif self.geography == 'County':
            tiger_geom = 'county'
        else:
            ## will happen for metro areas, cities, neighborhoods
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
