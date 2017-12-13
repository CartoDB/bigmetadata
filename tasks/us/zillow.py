'''
Bigmetadata tasks

tasks to download and create metadata
'''

import os

from collections import OrderedDict
from datetime import datetime
from luigi import (Task, IntParameter, LocalTarget, Parameter, WrapperTask)
from tasks.base_tasks import ColumnsTask, TableTask, TagsTask, CSV2TempTableTask, MetaWrapper
from tasks.util import shell, classpath, underscore_slugify
from tasks.tags import SectionTags, SubsectionTags, UnitTags
from tasks.meta import OBSColumn, current_session, OBSTag, UNIVERSE
from tasks.us.census.tiger import GeoidColumns, SumLevel


# cherry-picked datasets
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

HOMES = ('AllHomes', 'SingleFamilyResidence', )
RENTALS = ('AllHomesPlusMultifamily', 'SingleFamilyResidenceRental',
           'AllHomes', 'Sfr', )
MEDIAN_VALUE_HOMETYPES = ('AllHomes',)
MEDIAN_RENTAL_HOMETYPES = ('AllHomes', 'Sfr', 'CondoCoop',)


def measure_name(hometype):
    if hometype in HOMES:
        measure_name = 'Zhvi'
    elif hometype == 'Sfr':
        measure_name = None
    elif hometype in RENTALS:
        measure_name = 'Zri'
    else:
        raise Exception('Unknown hometype "{hometype}"'.format(hometype=hometype))

    return measure_name


def measure_aggregation(hometype):
    aggregation = []

    if hometype in MEDIAN_VALUE_HOMETYPES:
        aggregation.append('MedianValuePerSqft')

    if hometype in MEDIAN_RENTAL_HOMETYPES:
        aggregation.append('MedianRentalPricePerSqft')

    return aggregation


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

    def build_column(self, input_, hometype, measure, hometype_human, aggregate, targets={}):
        tag = input_['tags']['indexes']
        united_states = input_['sections']['united_states']
        housing = input_['subsections']['housing']
        units = input_['units']
        source = input_['source']['zillow-source']
        license = input_['license']['zillow-license']

        measure_human = MEASURES_HUMAN[measure]
        measure_unit = MEASURES_UNITS[measure]

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
                        targets=targets,
                        tags=[tag, united_states, housing, units[measure_unit], license, source])

        return col

    def columns(self):
        input_ = self.input()

        columns = OrderedDict()

        for hometype, hometype_human in HOMETYPES.items():
            measure = measure_name(hometype)

            if measure:
                col_id = '{hometype}_{measure}'.format(hometype=hometype,
                                                       measure=measure)
                columns[col_id] = self.build_column(input_, hometype, measure, hometype_human, 'index')

                aggregations = measure_aggregation(hometype)
                for aggregation in aggregations:
                    targets = {columns[col_id]: UNIVERSE}

                    col_id = '{hometype}_{aggregation}'.format(hometype=hometype,
                                                               aggregation=aggregation)
                    columns[col_id] = self.build_column(input_, hometype, aggregation, hometype_human,
                                                        'median', targets)

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

    geography = Parameter()  # example: Zip
    hometype = Parameter()  # example: SingleFamilyResidence
    measure = Parameter()
    last_year = IntParameter()
    last_month = IntParameter()

    def requires(self):
        return DownloadZillow(geography=self.geography, hometype=self.hometype,
                              measure=self.measure, last_year=self.last_year,
                              last_month=self.last_month)

    def input_csv(self):
        return self.input().path


class Zillow(TableTask):

    year = IntParameter()
    month = IntParameter()
    geography = Parameter()  # example: Zip

    def version(self):
        return 5

    def requires(self):
        requirements = {
            'metadata': ZillowValueColumns(),
            'geoids': GeoidColumns()
        }
        for hometype, hometype_human in HOMETYPES.items():
            measure = measure_name(hometype)

            if measure:
                table_id = '{hometype}_{measure}'.format(hometype=hometype,
                                                         measure=measure)
                requirements[table_id] = WideZillow(
                    geography=self.geography, hometype=hometype, measure=measure,
                    last_month=datetime.now().month, last_year=datetime.now().year)

                aggregations = measure_aggregation(hometype)
                for aggregation in aggregations:
                    table_id = '{hometype}_{measure}'.format(hometype=hometype,
                                                             measure=aggregation)
                    requirements[table_id] = WideZillow(
                        geography=self.geography, hometype=hometype, measure=aggregation,
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
            # will happen for metro areas, cities, neighborhoods, state, county
            raise Exception('unrecognized geography {}'.format(self.geography))

        columns = OrderedDict([
            ('region_name', input_['geoids'][tiger_geo + '_geoid']),
        ])
        columns.update(input_['metadata'])

        return columns

    def populate(self):
        session = current_session()

        insert = True
        input_ = self.input()
        output = self.output()
        session.execute('ALTER TABLE {output} ADD PRIMARY KEY (region_name)'.format(
            output=output.table))
        session.flush()
        for key, value in input_['metadata'].items():
            input_table = input_[key].table
            stmt = '''INSERT INTO {output} (region_name, {col_id})
                      SELECT "RegionName", "{year}-{month}"::NUMERIC
                      FROM {input_table}
                      ON CONFLICT (region_name)
                         DO UPDATE SET {col_id} = EXCLUDED.{col_id}'''
            session.execute(stmt.format(
                output=output.table,
                year=str(self.year).zfill(2),
                month=str(self.month).zfill(2),
                col_id=key,
                input_table=input_table))
            if insert:
                insert = False


class AllZillow(WrapperTask):

    def requires(self):
        now = datetime.now()
        for geography in ('Zip', ):
            for year in range(2010, now.year + 1):
                for month in range(1, 13):
                    if now.year == year and now.month <= month:
                        continue
                    if year == 2010 and month <= 10:
                        continue
                    yield Zillow(geography=geography, year=year, month=month)


class ZillowMetaWrapper(MetaWrapper):
    geography = Parameter()
    year = IntParameter()
    month = IntParameter()

    now = datetime.now()

    params = {
        'geography': ['Zip'],
        'year': list(range(2010, now.year + 1)),
        'month': list(range(1, 13))
    }

    def tables(self):
        now = datetime.now()
        if now.year == self.year and now.month <= self.month:
            return
        if self.year == 2010 and self.month <= 10:
            return
        yield Zillow(geography=self.geography, year=self.year, month=self.month)
        yield SumLevel(year=str(2015), geography='zcta5')
