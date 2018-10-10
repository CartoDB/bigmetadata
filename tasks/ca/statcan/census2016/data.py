import os
import glob
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


class CopyData(TempTableTask):
    geocode = Parameter()

    def requires(self):
        return DownloadData(geocode=self.geocode)

    def _create_table(self, session):
        query = '''
                CREATE TABLE {output} (
                    year TEXT,
                    geocode TEXT,
                    geolevel TEXT,
                    geoname TEXT,
                    gnr TEXT,
                    gnrlf TEXT,
                    quality TEXT,
                    altgeocode TEXT,
                    name TEXT,
                    profileid NUMERIC,
                    profiledesc TEXT,
                    total TEXT,
                    male TEXT,
                    female TEXT,
                    PRIMARY KEY(geocode, geolevel, profileid)
                )
                '''.format(
                    output=self.output().table,
                )

        LOGGER.debug(query)
        session.execute(query)
        session.commit()

    def run(self):
        session = current_session()

        self._create_table(session)

        path = glob.glob(os.path.join(self.input().path, '*data.csv'))[0]
        with open(path, 'rb') as f:
            cursor = session.connection().connection.cursor()
            sql_copy = """
                        COPY {table} FROM STDIN WITH CSV HEADER DELIMITER AS ','
                       """.format(table=self.output().table)
            cursor.copy_expert(sql=sql_copy, file=f)
            session.commit()
            cursor.close()


class ImportData(TempTableTask):
    resolution = Parameter()
    topic = Parameter()
    segment = Parameter()

    def requires(self):
        return {
            'data': CopyData(geocode=GEOGRAPHY_CODES[self.resolution]),
        }

    def _create_table(self):
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
                    {columns},
                    PRIMARY KEY (geom_id)
                )
                '''.format(
                    output=self.output().table,
                    columns=','.join(columns),
                )

        LOGGER.debug(query)
        session.execute(query)

    def _upsert_data(self, ids, column_suffix):
        session = current_session()

        AGG_COLUMN = {
            't': 'total',
            'f': 'female',
            'm': 'male',
        }

        stmt = '''
               INSERT INTO {output} (geom_id, {measure_names})
               SELECT geom_id, {measure_names_num}
               FROM crosstab('SELECT geocode, profileid, {agg_column}
                    FROM {input}
                    WHERE geolevel = ''{geolevel}''
                    ORDER BY 1, 2')
                    AS ct(geom_id text, {measure_ids})
               ON CONFLICT (geom_id)
               DO UPDATE SET {upsert}
               '''.format(
                output=self.output().table,
                input=self.input()['data'].table,
                geolevel=GEOLEVEL_FROM_GEOGRAPHY[self.resolution],
                agg_column=AGG_COLUMN[column_suffix],
                measure_names=','.join(['c{}_{}'.format(str(x).zfill(4), column_suffix) for x in ids]),
                measure_names_num=','.join(["nullif(nullif(nullif(nullif(c{}_{}, '..'), '...'), 'F'), 'x')::numeric".format(str(x).zfill(4), column_suffix) for x in ids]),
                measure_ids=','.join(['c{}_{} text'.format(str(x).zfill(4), column_suffix) for x in ids]),
                upsert=','.join(['{colname} = EXCLUDED.{colname}'.format(colname=x)
                                 for x in ['c{}_{}'.format(str(x).zfill(4), column_suffix) for x in ids]])
                )
        LOGGER.debug(stmt)
        session.execute(stmt)

    def _populate_from_copy(self):
        t_ids = []
        m_ids = []
        f_ids = []
        for key, column in COLUMNS_DEFINITION.items():
            if column['subsection'] in TOPICS[self.topic]:
                if self.segment in [SEGMENT_ALL, SEGMENT_TOTAL]:
                    t_ids.append(int(key[1:]))
                if column.get('gender_split', 'no') == 'yes':
                    if self.segment in [SEGMENT_ALL, SEGMENT_FEMALE]:
                        f_ids.append(int(key[1:]))
                    if self.segment in [SEGMENT_ALL, SEGMENT_MALE]:
                        m_ids.append(int(key[1:]))

        if t_ids:
            self._upsert_data(t_ids, 't')
        if f_ids:
            self._upsert_data(f_ids, 'f')
        if m_ids:
            self._upsert_data(m_ids, 'm')

    def run(self):
        self._create_table()
        self._populate_from_copy()


class CensusData(TableTask):
    resolution = Parameter()
    topic = Parameter()
    segment = Parameter()

    def version(self):
        return 1

    def requires(self):
        return {
            'meta': CensusColumns(resolution=self.resolution, topic=self.topic),
            'data': ImportData(resolution=self.resolution, topic=self.topic, segment=self.segment),
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
