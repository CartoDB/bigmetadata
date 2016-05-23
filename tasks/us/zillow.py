'''
Bigmetadata tasks

tasks to download and create metadata
'''

import os
import requests

from collections import OrderedDict
from luigi import (Task, IntParameter, LocalTarget, BooleanParameter, Parameter,
                   WrapperTask)
from tasks.util import (TableTarget, shell, classpath, underscore_slugify,
                        CartoDBTarget, sql_to_cartodb_table,
                        TableTask, ColumnsTask, TagsTask)
from tasks.tags import SectionTags, SubsectionTags
from tasks.meta import OBSColumn, current_session, OBSTag
from tasks.us.census.tiger import GeoidColumns
from psycopg2 import ProgrammingError

## cherry-picked datasets
HOMETYPES = {
    'AllHomes': 'All homes',
    'SingleFamilyResidence': 'Single Family Homes',
    'AllHomesPlusMultifamily': 'All homes plus multifamily',
    'SingleFamilyResidenceRental': 'Single Family residence rental',
    'Sfr': 'Single Family residence rental',
}

MEASURES_HUMAN = {
    'Zhvi': 'Zillow Home Value Index',
    'Zri': 'Zillow Rental Index',
    'MedianValuePerSqft': 'Median value per square foot',
    'MedianRentalPricePerSqft': 'Median rental price per square foot'
}

HOMETYPES_DESCRIPTION = {
    'AllHomes': 'Zillow defines all homes as single-family, condominium and '
                'co-operative homes with a county record. Unless specified, '
                'all series cover this segment of the housing stock.',
    'SingleFamilyResidence': 'Single family residences are detached, '
                             'free-standing residential buildings. ',
    'AllHomesPlusMultifamily': 'In addition to "All homes", which Zillow defines '
                               'as single-family, condominium and co-operative '
                               'homes with a county record, this group includes '
                               'units in buildings with 5 or more housing '
                               'units that are not a condominiums or co-ops.',
    'SingleFamilyResidenceRental': 'Single Family residence rental is defined '
                                   'as detached, free-standing residential '
                                   'buildings which are rented out.',
}

HOMETYPES_DESCRIPTION['Sfr'] = HOMETYPES_DESCRIPTION['SingleFamilyResidence']

MEASURES_DESCRIPTION = {
    'Zhvi': 'The Zillow Home Value Index (ZHVI) is a time series tracking the '
            'monthly median home value (in US Dollars) in a particular '
            'geographical region. In general, each ZHVI time series begins in '
            'April 1996. See `Zillow\'s methodology '
            '<http://www.zillow.com/research/zhvi-methodology-6032/>`_ for '
            'more information.',
    'Zri': 'Similar to Zillow\'s ZHVI, the Zillow Rent Index (ZRI) tracks the '
           'monthly median rent (in US Dollars) in different geographical '
           'regions. In general, each ZRI time series beginds in November, '
           '2010.  See `Zillow\'s methodology '
           '<http://www.zillow.com/research/zillow-rent-index-methodology-2393/>`_ '
           'for more information.',
    'MedianValuePerSqft': 'Median of the estimated monthly rent price (US '
                          'Dollars) of all homes, per square foot. This is '
                          'calculated by taking the estimated rent price for '
                          'a home and dividing it by the homes square footage.',
    'MedianRentalPricePerSqft': 'Median of the value (US Dollars) of all homes '
                                'per square foot. This number is calculated by '
                                'taking the estimated home value for each home '
                                'in a given region and dividing it by the '
                                'home\'s square footage.'
}

def measures_for_hometype(hometype):
    homes = ('AllHomes', 'SingleFamilyResidence', )
    rentals = ('AllHomesPlusMultifamily', 'SingleFamilyResidenceRental',
               'AllHomes', 'Sfr', )

    if hometype in homes:
        measures = ['Zhvi']
    elif hometype == 'Sfr':
        measures = []
    elif hometype in rentals:
        measures = ['Zri']
    else:
        raise Exception('Unknown hometype "{hometype}"'.format(hometype=hometype))

    if hometype in ('AllHomes',):
        measures.append('MedianValuePerSqft')

    if hometype in ('AllHomes', 'Sfr', 'CondoCoop',):
        measures.append('MedianRentalPricePerSqft')

    return measures


def hometype_measures():
    for hometype, hometype_human in HOMETYPES.iteritems():
        for measure in measures_for_hometype(hometype):
            measure_human = MEASURES_HUMAN[measure]
            yield hometype, hometype_human, measure, measure_human


class ZillowTags(TagsTask):

    def version(self):
        return 2

    def tags(self):
        return [
            OBSTag(id='indexes',
                   name='Zillow Home Value and Rental Indexes',
                   type='subsection',
                   description='Zillow home value and rental indexes.'),
        ]


class DownloadZillow(Task):

    geography = Parameter()
    hometype = Parameter()
    measure = Parameter()

    URL = 'http://files.zillowstatic.com/research/public/{geography}/{geography}_{measure}_{hometype}.csv'

    def run(self):
        self.output().makedirs()
        shell('wget \'{url}\' -O {output}'.format(
            url=self.URL.format(geography=self.geography, hometype=self.hometype,
                                measure=self.measure), output=self.output().path))

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id) + '.csv')


class ZillowValueColumns(ColumnsTask):

    def requires(self):
        return {
            'tags': ZillowTags(),
            'subsections': SubsectionTags(),
            'sections': SectionTags(),
        }

    def version(self):
        return 5

    def columns(self):
        input_ = self.input()
        tag = input_['tags']['indexes']
        united_states = input_['sections']['united_states']
        housing = input_['subsections']['housing']

        columns = OrderedDict()

        for hometype, hometype_human, measure, measure_human in hometype_measures():
            aggregate = 'median' if 'median' in measure.lower() else 'index'
            col_id = '{hometype}_{measure}'.format(hometype=hometype,
                                                   measure=measure)
            col = OBSColumn(type='Numeric',
                            name='{measure} for {hometype}'.format(
                                measure=measure_human,
                                hometype=hometype_human),
                            aggregate=aggregate,
                            weight=1,
                            description='{measure_description} {hometype_description}'.format(
                                measure_description=MEASURES_DESCRIPTION[measure],
                                hometype_description=HOMETYPES_DESCRIPTION[hometype],
                                ),
                            tags=[tag, united_states, housing])
            columns[col_id] = col
        return columns


class ZillowTimeValueColumns(ColumnsTask):

    def columns(self):
        columns = OrderedDict()

        # TODO generate value columns
        for year in xrange(1996, 2017):
            for month in xrange(1, 13):
                yr_str = str(year).zfill(2)
                mo_str = str(month).zfill(2)

                columns['{yr}_{mo}'.format(
                    yr=yr_str, mo=mo_str)] = OBSColumn(
                        type='Numeric',
                        name='',
                        description='',
                        weight=0)
        return columns

class ZillowGeoColumns(ColumnsTask):

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

        return columns


class WideZillow(TableTask):

    geography = Parameter() # example: Zip
    hometype = Parameter() # example: SingleFamilyResidence
    measure = Parameter()

    def requires(self):
        return {
            'data': DownloadZillow(geography=self.geography, hometype=self.hometype,
                                   measure=self.measure),
            'zillow_geo': ZillowGeoColumns(),
            'zillow_time_value': ZillowTimeValueColumns(),
            'geoids': GeoidColumns()
        }

    def bounds(self):
        return 'BOX(0 0,0 0)'

    def timespan(self):
        return None

    def version(self):
        return 2

    def columns(self):
        if self.geography == 'Zip':
            tiger_geo = 'zcta5'
        #elif self.geography == 'State':
        #    tiger_geo = 'geoid'
        #elif self.geography == 'County':
        #    tiger_geom = 'county'
        else:
            ## will happen for metro areas, cities, neighborhoods, state, county
            raise Exception('unrecognized geography {}'.format(self.geography))

        columns = OrderedDict()

        input_ = self.input()
        with input_['data'].open() as fhandle:
            first_row = fhandle.next().strip().split(',')

        for headercell in first_row:
            headercell = headercell.strip('"').replace('-', '_')
            if headercell == 'RegionName':
                columns['region_name'] = input_['geoids'][tiger_geo + '_geoid']
            else:
                colname = underscore_slugify(headercell)
                if colname[0:2] in ('19', '20'):
                    colname = 'value_' + colname
                    columns[colname] = input_['zillow_time_value'][headercell]
                else:
                    columns[colname] = input_['zillow_geo'][headercell]

        return columns

    def populate(self):
        shell(r"psql -c '\copy {table} FROM {file_path} WITH CSV HEADER'".format(
            table=self.output().table,
            file_path=self.input()['data'].path
        ))
        session = current_session()
        session.execute('ALTER TABLE {output} ADD PRIMARY KEY (region_name)'.format(
            output=self.output().table))


class Zillow(TableTask):

    year = Parameter()
    month = Parameter()
    geography = Parameter() # example: Zip

    def version(self):
        return 3

    def requires(self):
        requirements = {
            'metadata': ZillowValueColumns(),
            'geoids': GeoidColumns()
        }
        for hometype, _, measure, _ in hometype_measures():
            table_id = '{hometype}_{measure}'.format(hometype=hometype,
                                                     measure=measure)
            requirements[table_id] = WideZillow(
                geography=self.geography, hometype=hometype, measure=measure)

        return requirements

    def bounds(self):
        return 'BOX(0 0,0 0)'

    def timespan(self):
        return '{year}-{month}'.format(year=str(self.year).zfill(2),
                                       month=str(self.month).zfill(2))

    def columns(self):
        input_ = self.input()
        if self.geography == 'Zip':
            tiger_geo = 'zcta5'
        else:
            ## will happen for metro areas, cities, neighborhoods, state, county
            raise Exception('unrecognized geography {}'.format(self.geography))

        columns = OrderedDict([
            ('region_name', input_['geoids'][tiger_geo + '_geoid']),
        ])
        for hometype, _, measure, _ in hometype_measures():
            col_id = hometype + '_' + measure
            columns[col_id] = input_['metadata'][col_id]
        return columns

    def populate(self):
        session = current_session()

        insert = True
        for hometype, _, measure, _ in hometype_measures():
            col_id = hometype + '_' + measure
            input_table = self.input()[col_id].table
            if insert:
                stmt = 'INSERT INTO {output} (region_name, {col_id}) ' \
                        'SELECT region_name, value_{year}_{month} ' \
                        'FROM {input_table} '
            else:
                stmt = 'UPDATE {output} ' \
                        'SET {col_id} = value_{year}_{month} ' \
                        'FROM {input_table} WHERE ' \
                        '{input_table}.region_name = {output}.region_name '
            session.execute(stmt.format(
                output=self.output().table,
                year=str(self.year).zfill(2),
                month=str(self.month).zfill(2),
                col_id=col_id,
                input_table=input_table))
            if insert:
                session.execute('ALTER TABLE {output} ADD PRIMARY KEY (region_name)'.format(
                    output=self.output().table))
            insert = False


class AllZillow(WrapperTask):

    def requires(self):
        for geography in ('Zip', ):
            for year in xrange(1996, 2017):
                for month in xrange(1, 13):
                    yield Zillow(geography=geography, year=year, month=month)
