import os
import urllib.request
import csv
import glob
import pandas as pd
import numpy as np

from luigi.local_target import LocalTarget
from luigi import Parameter, IntParameter, Task, WrapperTask
from tasks.base_tasks import (DownloadUnzipTask, CSV2TempTableTask, TempTableTask)
from tasks.meta import current_session
from tasks.util import shell

from lib.logger import get_logger

LOGGER = get_logger(__name__)

BLOCK = 'block'
BLOCK_GROUP = 'block group'
CENSUS_TRACT = 'tract'
COUNTY = 'county'
STATE = 'state'
GEOGRAPHIES = [
    BLOCK,
    BLOCK_GROUP,
    CENSUS_TRACT,
    COUNTY,
    STATE
]

# (file_source_column, db_target_column)
MONTH_COLUMN = 'month'
CATEGORY_COLUMN = 'category'
REGION_TYPE_COLUMN = 'region_type'
REGION_ID_COLUMN = 'region_id'
TOTAL_MERCHANTS_COLUMN = 'total_merchants'
TICKET_SIZE_SCORE_COLUMN = 'ticket_size_score'
TICKET_SIZE_COUNTRY_SCORE_COLUMN = 'ticket_size_country_score'
TICKET_SIZE_METRO_SCORE_COLUMN = 'ticket_size_metro_score'
TICKET_SIZE_STATE_SCORE_COLUMN = 'ticket_size_state_score'
GROWTH_SCORE_COLUMN = 'growth_score'
GROWTH_COUNTRY_SCORE_COLUMN = 'growth_country_score'
GROWTH_METRO_SCORE_COLUMN = 'growth_metro_score'
GROWTH_STATE_SCORE_COLUMN = 'growth_state_score'
STABILITY_SCORE_COLUMN = 'stability_score'
STABILITY_COUNTRY_SCORE_COLUMN = 'stability_country_score'
STABILITY_METRO_SCORE_COLUMN = 'stability_metro_score'
STABILITY_STATE_SCORE_COLUMN = 'stability_state_score'
TRANSACTIONS_SCORE_COLUMN = 'transactions_score'
TRANSACTIONS_COUNTRY_SCORE_COLUMN = 'transactions_country_score'
TRANSACTIONS_METRO_SCORE_COLUMN = 'transactions_metro_score'
TRANSACTIONS_STATE_SCORE_COLUMN = 'transactions_state_score'
SALES_SCORE_COLUMN = 'sales_score'
SALES_COUNTRY_SCORE_COLUMN = 'sales_country_score'
SALES_METRO_SCORE_COLUMN = 'sales_metro_score'
SALES_STATE_SCORE_COLUMN = 'sales_state_score'

DATA_COLUMNS = {
    TOTAL_MERCHANTS_COLUMN,
    TICKET_SIZE_COUNTRY_SCORE_COLUMN,
    TICKET_SIZE_METRO_SCORE_COLUMN,
    TICKET_SIZE_STATE_SCORE_COLUMN,
    GROWTH_COUNTRY_SCORE_COLUMN,
    GROWTH_METRO_SCORE_COLUMN,
    GROWTH_STATE_SCORE_COLUMN,
    STABILITY_COUNTRY_SCORE_COLUMN,
    STABILITY_METRO_SCORE_COLUMN,
    STABILITY_STATE_SCORE_COLUMN,
    TRANSACTIONS_COUNTRY_SCORE_COLUMN,
    TRANSACTIONS_METRO_SCORE_COLUMN,
    TRANSACTIONS_STATE_SCORE_COLUMN,
    SALES_COUNTRY_SCORE_COLUMN,
    SALES_METRO_SCORE_COLUMN,
    SALES_STATE_SCORE_COLUMN,
}

CATEGORIES = {
    'non eating places': 'NEP',
    'eating places': 'EP',
    'apparel': 'APP',
    'small business': 'SB',
    'total retail': 'TR',
}


class DownloadUnzipMasterCard(DownloadUnzipTask):
    URL = 'http://172.17.0.1/mastercard/my_fake_data.zip'

    def download(self):
        urllib.request.urlretrieve(self.URL, self.output().path + '.zip')


class GatherMasterCardData(Task):
    geography = Parameter()
    month = Parameter()
    minyear = IntParameter(default=2012, significant=False)
    maxyear = IntParameter(default=2018, significant=False)

    FILE_EXTENSION = 'csv'

    def requires(self):
        return DownloadUnzipMasterCard()

    def _build_row(self, header, monthfilter, row):
        region_type = row[header.index(REGION_TYPE_COLUMN)]
        category = CATEGORIES.get(row[header.index(CATEGORY_COLUMN)])
        month = row[header.index(MONTH_COLUMN)]

        if region_type == self.geography and month in monthfilter and category:
            month = month.split('/')
            month = ''.join([month[2], month[0], month[1]])
            return {
                REGION_ID_COLUMN: row[header.index(REGION_ID_COLUMN)],
                '_'.join([TOTAL_MERCHANTS_COLUMN, category, month]).lower():
                    row[header.index(TOTAL_MERCHANTS_COLUMN)],
                '_'.join([TICKET_SIZE_COUNTRY_SCORE_COLUMN, category, month]).lower():
                    row[header.index(TICKET_SIZE_SCORE_COLUMN)],
                '_'.join([TICKET_SIZE_METRO_SCORE_COLUMN, category, month]).lower():
                    row[header.index(TICKET_SIZE_METRO_SCORE_COLUMN)],
                '_'.join([TICKET_SIZE_STATE_SCORE_COLUMN, category, month]).lower():
                    row[header.index(TICKET_SIZE_STATE_SCORE_COLUMN)],
                '_'.join([GROWTH_COUNTRY_SCORE_COLUMN, category, month]).lower():
                    row[header.index(GROWTH_SCORE_COLUMN)],
                '_'.join([GROWTH_METRO_SCORE_COLUMN, category, month]).lower():
                    row[header.index(GROWTH_METRO_SCORE_COLUMN)],
                '_'.join([GROWTH_STATE_SCORE_COLUMN, category, month]).lower():
                    row[header.index(GROWTH_STATE_SCORE_COLUMN)],
                '_'.join([STABILITY_COUNTRY_SCORE_COLUMN, category, month]).lower():
                    row[header.index(STABILITY_SCORE_COLUMN)],
                '_'.join([STABILITY_METRO_SCORE_COLUMN, category, month]).lower():
                    row[header.index(STABILITY_METRO_SCORE_COLUMN)],
                '_'.join([STABILITY_STATE_SCORE_COLUMN, category, month]).lower():
                    row[header.index(STABILITY_STATE_SCORE_COLUMN)],
                '_'.join([TRANSACTIONS_COUNTRY_SCORE_COLUMN, category, month]).lower():
                    row[header.index(TRANSACTIONS_SCORE_COLUMN)],
                '_'.join([TRANSACTIONS_METRO_SCORE_COLUMN, category, month]).lower():
                    row[header.index(TRANSACTIONS_METRO_SCORE_COLUMN)],
                '_'.join([TRANSACTIONS_STATE_SCORE_COLUMN, category, month]).lower():
                    row[header.index(TRANSACTIONS_STATE_SCORE_COLUMN)],
                '_'.join([SALES_COUNTRY_SCORE_COLUMN, category, month]).lower():
                    row[header.index(SALES_SCORE_COLUMN)],
                '_'.join([SALES_METRO_SCORE_COLUMN, category, month]).lower():
                    row[header.index(SALES_METRO_SCORE_COLUMN)],
                '_'.join([SALES_STATE_SCORE_COLUMN, category, month]).lower():
                    row[header.index(SALES_STATE_SCORE_COLUMN)],
            }

        return None

    def run(self):
        self.output().makedirs()
        months = ['/'.join([self.month, '01', str(x)]) for x in range(self.minyear, self.maxyear + 1)]

        with(open(glob.glob(os.path.join(self.input().path, '*.csv'))[0], 'r')) as infile:
            df = pd.DataFrame(columns=[REGION_ID_COLUMN])
            df = df.iloc[0:0]  # Empty the DataFrame

            reader = csv.reader(infile)
            first = True
            inheader = []

            for row in reader:
                if first:
                    inheader = row
                    first = False
                else:
                    f_row = self._build_row(inheader, months, row)
                    if f_row:
                        df = df.append(f_row, ignore_index=True)

            df = df.set_index(REGION_ID_COLUMN)
            df = df.groupby(REGION_ID_COLUMN).first()  # Any grouping function will do the trick

            df.to_csv(self.output().path, sep=',', index=True)

    def output(self):
        csv_filename = os.path.join('tmp', 'mc', '{}.csv'.format('_'.join(['mc', self.geography, self.month])))
        return LocalTarget(path=csv_filename, format='csv')


class MasterCardDataCOPY(TempTableTask):
    geography = Parameter()
    month = Parameter()
    minyear = IntParameter(default=2012, significant=False)
    maxyear = IntParameter(default=2018, significant=False)

    def requires(self):
        return GatherMasterCardData(geography=self.geography, month=self.month,
                                    minyear=self.minyear, maxyear=self.maxyear)

    def run(self):
        session = current_session()

        try:
            data_fields = []
            months = [str(x) + self.month+'01' for x in range(self.minyear, self.maxyear + 1)]
            for column in DATA_COLUMNS:
                for category in CATEGORIES.values():
                    for month in months:
                        data_fields.append('_'.join([column, category, month]).lower() + ' NUMERIC')

            query = '''
                    CREATE TABLE {table} (
                        {region_id} TEXT,
                        {data_fields}
                    )
                    '''.format(
                        table=self.output().table,
                        region_id=REGION_ID_COLUMN,
                        data_fields=','.join(data_fields)
                    )
            LOGGER.info('Creating table...')
            session.execute(query)
            session.commit()

            cmd = "cat '{infile}' | psql -c 'COPY {table} FROM STDIN WITH CSV HEADER'".format(
                table=self.output().table,
                infile=self.input().path,
            )
            LOGGER.info('COPYing...')
            shell(cmd)

            query = 'ALTER TABLE {table} ADD PRIMARY KEY ({region_id})'.format(
                table=self.output().table,
                region_id=REGION_ID_COLUMN,
            )
            LOGGER.info('Indexing...')
            session.execute(query)
            session.commit()
        except Exception as e:
            LOGGER.error('Error creating/populating {table}: {error}'.format(
                table=self.output().table,
                error=str(e)
            ))
            session.rollback()
            raise e


class MasterCardDataCOPYAllMonths(WrapperTask):
    geography = Parameter()
    minyear = IntParameter(default=2012)
    maxyear = IntParameter(default=2018)

    def requires(self):
        for month in [str(x).zfill(2) for x in range(1, 13)]:
            yield MasterCardDataCOPY(geography=self.geography, month=month,
                                     minyear=self.minyear, maxyear=self.maxyear)


class ImportMasterCardData(CSV2TempTableTask):
    FILE_EXTENSION = 'csv'

    def requires(self):
        return DownloadUnzipMasterCard()

    def coldef(self):
        '''
        :return: Lowercased column names
        '''
        uppercased = super(ImportMasterCardData, self).coldef()
        return [(t[0].lower(), t[1]) for t in uppercased]

    def input_csv(self):
        for file in os.listdir(self.input().path):
            if file.endswith('.{}'.format(self.FILE_EXTENSION)):
                return os.path.join(self.input().path, file)


class MasterCardData(TempTableTask):
    geography = Parameter()

    def requires(self):
        return ImportMasterCardData()

    def run(self):
        session = current_session()

        try:
            query = '''
                    CREATE TABLE {table} (
                        {region_id} TEXT,
                        {month} TEXT,
                        {category} TEXT,
                        {total_merchants_column} NUMERIC,
                        {ticket_size_country_score_column} NUMERIC,
                        {ticket_size_metro_score_column} NUMERIC,
                        {ticket_size_state_score_column} NUMERIC,
                        {growth_country_score_column} NUMERIC,
                        {growth_metro_score_column} NUMERIC,
                        {growth_state_score_column} NUMERIC,
                        {stability_country_score_column} NUMERIC,
                        {stability_metro_score_column} NUMERIC,
                        {stability_state_score_column} NUMERIC,
                        {transactions_country_score_column} NUMERIC,
                        {transactions_metro_score_column} NUMERIC,
                        {transactions_state_score_column} NUMERIC,
                        {sales_country_score_column} NUMERIC,
                        {sales_metro_score_column} NUMERIC,
                        {sales_state_score_column} NUMERIC,
                        PRIMARY KEY ({region_id}, {month}, {category})
                    )
                    '''.format(
                        table=self.output().table,
                        region_id=REGION_ID_COLUMN,
                        month=MONTH_COLUMN,
                        category=CATEGORY_COLUMN,
                        total_merchants_column=TOTAL_MERCHANTS_COLUMN,
                        ticket_size_country_score_column=TICKET_SIZE_COUNTRY_SCORE_COLUMN,
                        ticket_size_metro_score_column=TICKET_SIZE_METRO_SCORE_COLUMN,
                        ticket_size_state_score_column=TICKET_SIZE_STATE_SCORE_COLUMN,
                        growth_country_score_column=GROWTH_COUNTRY_SCORE_COLUMN,
                        growth_metro_score_column=GROWTH_METRO_SCORE_COLUMN,
                        growth_state_score_column=GROWTH_STATE_SCORE_COLUMN,
                        stability_country_score_column=STABILITY_COUNTRY_SCORE_COLUMN,
                        stability_metro_score_column=STABILITY_METRO_SCORE_COLUMN,
                        stability_state_score_column=STABILITY_STATE_SCORE_COLUMN,
                        transactions_country_score_column=TRANSACTIONS_COUNTRY_SCORE_COLUMN,
                        transactions_metro_score_column=TRANSACTIONS_METRO_SCORE_COLUMN,
                        transactions_state_score_column=TRANSACTIONS_STATE_SCORE_COLUMN,
                        sales_country_score_column=SALES_COUNTRY_SCORE_COLUMN,
                        sales_metro_score_column=SALES_METRO_SCORE_COLUMN,
                        sales_state_score_column=SALES_STATE_SCORE_COLUMN,
                    )
            session.execute(query)

            query = '''
                    INSERT INTO {output_table}
                    SELECT {region_id}, {month}, {category},
                           {total_merchants_column}::NUMERIC,
                           {ticket_size_country_score_column}::NUMERIC,
                           {ticket_size_metro_score_column}::NUMERIC,
                           {ticket_size_state_score_column}::NUMERIC,
                           {growth_country_score_column}::NUMERIC,
                           {growth_metro_score_column}::NUMERIC,
                           {growth_state_score_column}::NUMERIC,
                           {stability_country_score_column}::NUMERIC,
                           {stability_metro_score_column}::NUMERIC,
                           {stability_state_score_column}::NUMERIC,
                           {transactions_country_score_column}::NUMERIC,
                           {transactions_metro_score_column}::NUMERIC,
                           {transactions_state_score_column}::NUMERIC,
                           {sales_country_score_column}::NUMERIC,
                           {sales_metro_score_column}::NUMERIC,
                           {sales_state_score_column}::NUMERIC
                          FROM {input_table}
                          WHERE {region_type} = '{geography}'
                    '''.format(
                        output_table=self.output().table,
                        input_table=self.input().table,
                        region_id=REGION_ID_COLUMN,
                        month=MONTH_COLUMN,
                        category=CATEGORY_COLUMN,
                        total_merchants_column=TOTAL_MERCHANTS_COLUMN,
                        ticket_size_country_score_column=TICKET_SIZE_SCORE_COLUMN,
                        ticket_size_metro_score_column=TICKET_SIZE_METRO_SCORE_COLUMN,
                        ticket_size_state_score_column=TICKET_SIZE_STATE_SCORE_COLUMN,
                        growth_country_score_column=GROWTH_SCORE_COLUMN,
                        growth_metro_score_column=GROWTH_METRO_SCORE_COLUMN,
                        growth_state_score_column=GROWTH_STATE_SCORE_COLUMN,
                        stability_country_score_column=STABILITY_SCORE_COLUMN,
                        stability_metro_score_column=STABILITY_METRO_SCORE_COLUMN,
                        stability_state_score_column=STABILITY_STATE_SCORE_COLUMN,
                        transactions_country_score_column=TRANSACTIONS_SCORE_COLUMN,
                        transactions_metro_score_column=TRANSACTIONS_METRO_SCORE_COLUMN,
                        transactions_state_score_column=TRANSACTIONS_STATE_SCORE_COLUMN,
                        sales_country_score_column=SALES_SCORE_COLUMN,
                        sales_metro_score_column=SALES_METRO_SCORE_COLUMN,
                        sales_state_score_column=SALES_STATE_SCORE_COLUMN,
                        region_type=REGION_TYPE_COLUMN,
                        geography=self.geography
                    )
            session.execute(query)

            session.commit()
        except Exception as e:
            LOGGER.error('Error creating/populating {table}: {error}'.format(
                table=self.output().table,
                error=str(e)
            ))
            session.rollback()
            raise e


class AllMasterCardData(WrapperTask):
    def requires(self):
        for geography in GEOGRAPHIES:
            yield MasterCardData(geography=geography)
