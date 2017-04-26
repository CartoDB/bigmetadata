'''
Bigmetadata tasks

tasks to download and create metadata
'''

import os
import requests

from collections import OrderedDict
from datetime import datetime
from luigi import (Task, IntParameter, LocalTarget, BooleanParameter, Parameter,
                   WrapperTask)
from tasks.util import (TableTarget, shell, classpath, underscore_slugify,
                        CartoDBTarget, sql_to_cartodb_table,
                        TableTask, ColumnsTask, TagsTask, CSV2TempTableTask, MetaWrapper)
from tasks.tags import SectionTags, SubsectionTags, UnitTags
from tasks.meta import OBSColumn, current_session, OBSTag
from tasks.us.census.tiger import GeoidColumns, SumLevel
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

MEASURES_UNITS = {
    'Zhvi': 'index',
    'Zri': 'index',
    'MedianValuePerSqft': 'money',
    'MedianRentalPricePerSqft': 'money'
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
            measure_unit = MEASURES_UNITS[measure]
            yield hometype, hometype_human, measure, measure_human, measure_unit


class ZillowSourceTags(TagsTask):

    def tags(self):
        return [OBSTag(id='zillow-source',
                       name='Zillow Data',
                       type='source',
                       description='Zillow makes available data free for reuse `here <http://www.zillow.com/research/data/>`_.', )]


class ZillowLicenseTags(TagsTask):

    def version(self):
        return 2

    def tags(self):
        return [OBSTag(id='zillow-license',
                       name='Zillow Terms of Use for "Aggregate Data"',
                       type='license',
                       description='May be used for non-personal uses, e.g., real estate market analysis. More information `here <http://www.zillow.com/corp/Terms.htm>`_', )]


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
    last_year = IntParameter()
    last_month = IntParameter()

    URL = 'http://files.zillowstatic.com/research/public/{geography}/{geography}_{measure}_{hometype}.csv'

    def url(self):
        return self.URL.format(geography=self.geography, hometype=self.hometype,
                               measure=self.measure)

    @property
    def last_time(self):
        if not hasattr(self, '_last_time'):
            last_time = shell('curl -s {url} | head -n 1'.format(url=self.url()))
            self._last_time = last_time.strip().split(',')[-1].strip('"')
        return self._last_time

    def run(self):
        self.output().makedirs()
        shell('wget \'{url}\' -O {output}'.format(url=self.url(),
                                                  output=self.output().path))

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id) +
                           '_' + underscore_slugify(self.last_time) + '.csv')


class ZillowValueColumns(ColumnsTask):

    def requires(self):
        return {
            'tags': ZillowTags(),
            'subsections': SubsectionTags(),
            'sections': SectionTags(),
            'units': UnitTags(),
            'source': ZillowSourceTags(),
            'license': ZillowLicenseTags(),
        }

    def version(self):
        return 7

    def columns(self):
        input_ = self.input()
        tag = input_['tags']['indexes']
        united_states = input_['sections']['united_states']
        housing = input_['subsections']['housing']
        units = input_['units']
        source = input_['source']['zillow-source']
        license = input_['license']['zillow-license']

        columns = OrderedDict()

        for hometype, hometype_human, measure, measure_human, measure_unit in hometype_measures():
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
                            tags=[tag, united_states, housing, units[measure_unit], license, source])
            columns[col_id] = col
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


class WideZillow(CSV2TempTableTask):

    geography = Parameter() # example: Zip
    hometype = Parameter() # example: SingleFamilyResidence
    measure = Parameter()
    last_year = IntParameter()
    last_month = IntParameter()

    def requires(self):
        return DownloadZillow(geography=self.geography, hometype=self.hometype,
                               measure=self.measure, last_year=self.last_year,
                               last_month = self.last_month)

    def input_csv(self):
        return self.input().path


class Zillow(TableTask):

    year = IntParameter()
    month = IntParameter()
    geography = Parameter() # example: Zip

    def version(self):
        return 4

    def requires(self):
        requirements = {
            'metadata': ZillowValueColumns(),
            'geoids': GeoidColumns()
        }
        for hometype, _, measure, _, _ in hometype_measures():
            table_id = '{hometype}_{measure}'.format(hometype=hometype,
                                                     measure=measure)
            requirements[table_id] = WideZillow(
                geography=self.geography, hometype=hometype, measure=measure,
                last_month=datetime.now().month, last_year=datetime.now().year)

        return requirements

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
        for hometype, _, measure, _, _ in hometype_measures():
            col_id = hometype + '_' + measure
            columns[col_id] = input_['metadata'][col_id]
        return columns

    def populate(self):
        session = current_session()

        insert = True
        input_ = self.input()
        output = self.output()
        session.execute('ALTER TABLE {output} ADD PRIMARY KEY (region_name)'.format(
            output=output.table))
        session.flush()
        for hometype, _, measure, _, _ in hometype_measures():
            col_id = hometype + '_' + measure
            input_table = input_[col_id].table
            stmt = '''INSERT INTO {output} (region_name, {col_id})
                      SELECT "RegionName", "{year}-{month}"::NUMERIC
                      FROM {input_table}
                      ON CONFLICT (region_name)
                         DO UPDATE SET {col_id} = EXCLUDED.{col_id}'''
            session.execute(stmt.format(
                output=output.table,
                year=str(self.year).zfill(2),
                month=str(self.month).zfill(2),
                col_id=col_id,
                input_table=input_table))
            if insert:
                insert = False


class AllZillow(WrapperTask):

    def requires(self):
        now = datetime.now()
        for geography in ('Zip', ):
            for year in xrange(2011, now.year + 1):
                for month in xrange(1, 13):
                    if now.year == year and now.month <= month - 1:
                        continue
                    yield Zillow(geography=geography, year=year, month=month)


class ZillowMetaWrapper(MetaWrapper):
    geography = Parameter()
    year = IntParameter()
    month = IntParameter()

    now = datetime.now()

    params = {
        'geography': ['Zip'],
        'year': range(2011, now.year + 1),
        'month': range(1, 13)
    }

    def tables(self):
        now = datetime.now()
        if now.year == self.year and now.month - 1 <= self.month:
            return
        yield Zillow(geography=self.geography, year=self.year, month=self.month)
        yield SumLevel(year=str(2015), geography='zcta5')
