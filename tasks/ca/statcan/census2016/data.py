import csv
import os
import glob
import logging
from collections import OrderedDict
from luigi import Parameter, WrapperTask
from tasks.ca.statcan.geo import (GEOGRAPHIES, GeographyColumns, Geography,
                                  GEO_CT, GEO_PR, GEO_CD, GEO_CSD, GEO_CMA, GEO_DA, GEO_FSA, GEO_DB)
from tasks.ca.statcan.census2016.cols_census import (CensusColumns, COLUMNS_DEFINITION, TOPICS,
                                                     SEGMENT_ALL, SEGMENT_TOTAL, SEGMENT_FEMALE, SEGMENT_MALE)

from tasks.base_tasks import RepoFileUnzipTask, TempTableTask, TableTask, ReverseCoupledInterpolationTask
from tasks.meta import current_session, GEOM_REF
from lib.logger import get_logger
from lib.timespan import get_timespan

LOGGER = get_logger(__name__)

GEOGRAPHY_CODES = {
    GEO_PR: '044',
    GEO_CD: '044',
    GEO_CSD: '044',
    GEO_DA: '044',
    GEO_CMA: '043',
    GEO_CT: '043',
    GEO_FSA: '046',
}

GEOLEVEL_FROM_GEOGRAPHY = {
    GEO_PR: '1',
    GEO_CD: '2',
    GEO_CSD: '3',
    GEO_DA: '4',
    GEO_CMA: '1',
    GEO_CT: '2',
    GEO_FSA: '2',
}

# https://www12.statcan.gc.ca/census-recensement/2016/dp-pd/prof/details/download-telecharger/comp/page_dl-tc.cfm?Lang=E
URL = 'https://www12.statcan.gc.ca/census-recensement/2016/dp-pd/prof/details/download-telecharger/comp/GetFile.cfm?Lang=E&TYPE=CSV&GEONO={geo_code}'
NUM_MEASUREMENTS = 2247


def safe_float_cast(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return 'x'


class DownloadData(RepoFileUnzipTask):
    geocode = Parameter()

    def get_url(self):
        return URL.format(geo_code=self.geocode)


class ImportData(TempTableTask):
    geocode = Parameter()
    topic = Parameter()
    segment = Parameter()

    def requires(self):
        return {
            'data': DownloadData(geocode=self.geocode),
        }

    def create_table(self):
        cols = []
        for key, column in COLUMNS_DEFINITION.items():
            if column['subsection'] in TOPICS[self.topic]:
                if self.segment in [SEGMENT_ALL, SEGMENT_TOTAL]:
                    cols.append(key + '_t')
                if column.get('gender_split', 'no') == 'yes':
                    if self.segment in [SEGMENT_ALL, SEGMENT_FEMALE]:
                        cols.append(key + '_f')
                    if self.segment in [SEGMENT_ALL, SEGMENT_MALE]:
                        cols.append(key + '_m')

        columns = ['{} NUMERIC'.format(col) for col in cols]
        session = current_session()
        query = '''
                CREATE TABLE {output} (
                    geom_id TEXT,
                    geolevel NUMERIC,
                    {columns},
                    PRIMARY KEY (geom_id)
                )
                '''.format(
                    output=self.output().table,
                    columns=','.join(columns),
                )

        LOGGER.debug(query)
        session.execute(query)

    def populate_from_csv(self):
        input_ = self.input()
        session = current_session()
        is_logger_debug = LOGGER.isEnabledFor(logging.DEBUG)

        i, col_id, col_total, col_male, col_female = 0, -1, -1, -1, -1
        path = glob.glob(os.path.join(input_['data'].path, '*data.csv'))[0]
        with open(path) as f:
            reader = csv.reader(f, delimiter=",")
            for num, line in enumerate(reader):
                if i == 0:
                    col_id = line.index('DIM: Profile of Dissemination Areas (2247)')
                    col_total = line.index('Dim: Sex (3): Member ID: [1]: Total - Sex')
                    col_female = line.index('Dim: Sex (3): Member ID: [3]: Female')
                    col_male = line.index('Dim: Sex (3): Member ID: [2]: Male')

                colname = 'c{}'.format(str(i).zfill(4))
                coldef = COLUMNS_DEFINITION.get(colname)

                if coldef and coldef['subsection'] in TOPICS[self.topic]:
                    f_geom_id = line[1]
                    f_geolevel = line[2]
                    f_column_name = line[col_id]
                    f_measurement_total = safe_float_cast(line[col_total])

                    if is_logger_debug:
                        LOGGER.debug('Reading line {} ::: {} ({}) | {} | {} '.format(
                            num, f_geom_id, f_geolevel, f_column_name, f_measurement_total
                        ))

                    if coldef['source_name'] != f_column_name:
                        raise ValueError("Line {line} (geoid={geoid}): The name for {column} doesn't match ('{f_name}' / '{json_name}')".format(
                            line=num, geoid=f_geom_id, column=colname,
                            f_name=f_column_name, json_name=coldef['source_name']
                        ))

                    measurements = {}
                    if self.segment in [SEGMENT_ALL, SEGMENT_TOTAL]:
                        measurements[colname + '_t'] = f_measurement_total

                    if coldef.get('gender_split', 'no') == 'yes':
                        if self.segment in [SEGMENT_ALL, SEGMENT_FEMALE]:
                            measurements[colname + '_f'] = safe_float_cast(line[col_female])
                        if self.segment in [SEGMENT_ALL, SEGMENT_MALE]:
                            measurements[colname + '_m'] = safe_float_cast(line[col_male])

                    stmt = '''
                           INSERT INTO {output} (geom_id, geolevel, {measure_names})
                           SELECT '{geoid}', {geolevel}, {measure_values}
                           ON CONFLICT (geom_id)
                           DO UPDATE SET {upsert}
                           '''.format(
                                output=self.output().table,
                                measure_names=','.join(measurements.keys()),
                                geoid=f_geom_id,
                                geolevel=f_geolevel,
                                measure_values=','.join(["NULLIF('{}', 'x')::NUMERIC".format(x)
                                                         for x in measurements.values()]),
                                upsert=','.join(['{colname} = EXCLUDED.{colname}'.format(colname=x)
                                                 for x in measurements.keys()])
                           )
                    session.execute(stmt)

                i += 1
                if (i > NUM_MEASUREMENTS):
                    i = 1

    def run(self):
        self.create_table()
        self.populate_from_csv()


class CensusData(TableTask):
    resolution = Parameter()
    topic = Parameter()
    segment = Parameter()

    def version(self):
        return 1

    def requires(self):
        return {
            'meta': CensusColumns(resolution=self.resolution, topic=self.topic),
            'data': ImportData(geocode=GEOGRAPHY_CODES[self.resolution], topic=self.topic, segment=self.segment),
            'geometa': GeographyColumns(resolution=self.resolution, year=2016),
            'geo': Geography(resolution=self.resolution, year=2016),
        }

    def targets(self):
        return {
            self.input()['geo'].obs_table: GEOM_REF,
        }

    def table_timespan(self):
        return get_timespan('2016')

    def columns(self):
        input_ = self.input()
        cols = OrderedDict()
        cols['geom_id'] = input_['geometa']['geom_id']
        for colname, coltarget in input_['meta'].items():
            colid, segment = colname.split('_')
            if COLUMNS_DEFINITION[colid]['subsection'] in TOPICS[self.topic]:
                if segment == self.segment or self.segment == SEGMENT_ALL:
                    cols[colname] = coltarget

        return cols

    def populate(self):
        session = current_session()
        input_ = self.input()
        colnames = list(self.columns().keys())

        query = '''
                INSERT INTO {output} ({out_colnames})
                SELECT {in_colnames} FROM {input}
                WHERE geolevel = {geolevel}
                '''.format(output=self.output().table,
                           input=input_['data'].table,
                           in_colnames=', '.join(colnames),
                           out_colnames=', '.join(colnames),
                           geolevel=GEOLEVEL_FROM_GEOGRAPHY[self.resolution])

        LOGGER.debug(query)
        session.execute(query)


class CensusDBFromDA(ReverseCoupledInterpolationTask):
    topic = Parameter()
    segment = Parameter()

    def requires(self):
        deps = {
            'source_geom_columns': GeographyColumns(resolution=GEO_DA, year=2016),
            'source_geom': Geography(resolution=GEO_DA, year=2016),
            'source_data_columns': CensusColumns(resolution=GEO_DA, topic=self.topic),
            'source_data': CensusData(resolution=GEO_DA, topic=self.topic, segment=self.segment),
            'target_geom_columns': GeographyColumns(resolution=GEO_DB, year=2016),
            'target_geom': Geography(resolution=GEO_DB, year=2016),
            'target_data_columns': CensusColumns(resolution=GEO_DB, topic=self.topic),
        }

        return deps

    def table_timespan(self):
        return get_timespan('2016')

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()
        cols['geom_id'] = input_['target_geom_columns']['geom_id']
        for colname, coltarget in input_['target_data_columns'].items():
            colid, segment = colname.split('_')
            if COLUMNS_DEFINITION[colid]['subsection'] in TOPICS[self.topic]:
                if segment == self.segment or self.segment == SEGMENT_ALL:
                    cols[colname] = coltarget
        return cols

    def get_interpolation_parameters(self):
        params = {
            'source_data_geoid': 'geom_id',
            'source_geom_geoid': 'geom_id',
            'target_data_geoid': 'geom_id',
            'target_geom_geoid': 'geom_id',
            'source_geom_geomfield': 'the_geom',
            'target_geom_geomfield': 'the_geom',
        }

        return params


class CensusDataWrapper(WrapperTask):
    resolution = Parameter()
    topic = Parameter()

    def requires(self):
        segments = [SEGMENT_ALL]
        if self.topic == 't003':
            segments = [SEGMENT_TOTAL, SEGMENT_FEMALE, SEGMENT_MALE]

        if self.resolution == GEO_DB:
            return [CensusDBFromDA(topic=self.topic, segment=s) for s in segments]
        else:
            return [CensusData(resolution=self.resolution, topic=self.topic, segment=s) for s in segments]


class AllCensusTopics(WrapperTask):
    resolution = Parameter()

    def requires(self):
        return [CensusDataWrapper(resolution=self.resolution, topic=topic) for topic in TOPICS.keys()]


class AllCensusResolutions(WrapperTask):
    def requires(self):
        return [AllCensusTopics(resolution=resolution) for resolution in GEOGRAPHIES]
