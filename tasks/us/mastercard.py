import os
import urllib.request
from luigi import Parameter, WrapperTask
from tasks.base_tasks import (DownloadUnzipTask, CSV2TempTableTask, TempTableTask)
from tasks.meta import current_session

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
MONTH_COLUMN = ('month', 'month')
CATEGORY_COLUMN = ('category', 'category')
REGION_TYPE_COLUMN = ('region_type', 'region_type')
REGION_ID_COLUMN = ('region_id', 'region_id')
TOTAL_MERCHANTS_COLUMN = ('total_merchants', 'total_merchants')
TICKET_SIZE_COUNTRY_SCORE_COLUMN = ('ticket_size_score', 'ticket_size_country_score')
TICKET_SIZE_METRO_SCORE_COLUMN = ('ticket_size_metro_score', 'ticket_size_metro_score')
TICKET_SIZE_STATE_SCORE_COLUMN = ('ticket_size_state_score', 'ticket_size_state_score')
GROWTH_COUNTRY_SCORE_COLUMN = ('growth_score', 'growth_country_score')
GROWTH_METRO_SCORE_COLUMN = ('growth_metro_score', 'growth_metro_score')
GROWTH_STATE_SCORE_COLUMN = ('growth_state_score', 'growth_state_score')
STABILITY_COUNTRY_SCORE_COLUMN = ('stability_score', 'stability_country_score')
STABILITY_METRO_SCORE_COLUMN = ('stability_metro_score', 'stability_metro_score')
STABILITY_STATE_SCORE_COLUMN = ('stability_state_score', 'stability_state_score')
TRANSACTIONS_COUNTRY_SCORE_COLUMN = ('transactions_score', 'transactions_country_score')
TRANSACTIONS_METRO_SCORE_COLUMN = ('transactions_metro_score', 'transactions_metro_score')
TRANSACTIONS_STATE_SCORE_COLUMN = ('transactions_state_score', 'transactions_state_score')
SALES_COUNTRY_SCORE_COLUMN = ('sales_score', 'sales_country_score')
SALES_METRO_SCORE_COLUMN = ('sales_metro_score', 'sales_metro_score')
SALES_STATE_SCORE_COLUMN = ('sales_state_score', 'sales_state_score')


class DownloadUnzipMasterCard(DownloadUnzipTask):
    URL = 'http://172.17.0.1/mastercard/my_fake_data.zip'

    def download(self):
        urllib.request.urlretrieve(self.URL, self.output().path + '.zip')


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
                        region_id=REGION_ID_COLUMN[1],
                        month=MONTH_COLUMN[1],
                        category=CATEGORY_COLUMN[1],
                        total_merchants_column=TOTAL_MERCHANTS_COLUMN[1],
                        ticket_size_country_score_column=TICKET_SIZE_COUNTRY_SCORE_COLUMN[1],
                        ticket_size_metro_score_column=TICKET_SIZE_METRO_SCORE_COLUMN[1],
                        ticket_size_state_score_column=TICKET_SIZE_STATE_SCORE_COLUMN[1],
                        growth_country_score_column=GROWTH_COUNTRY_SCORE_COLUMN[1],
                        growth_metro_score_column=GROWTH_METRO_SCORE_COLUMN[1],
                        growth_state_score_column=GROWTH_STATE_SCORE_COLUMN[1],
                        stability_country_score_column=STABILITY_COUNTRY_SCORE_COLUMN[1],
                        stability_metro_score_column=STABILITY_METRO_SCORE_COLUMN[1],
                        stability_state_score_column=STABILITY_STATE_SCORE_COLUMN[1],
                        transactions_country_score_column=TRANSACTIONS_COUNTRY_SCORE_COLUMN[1],
                        transactions_metro_score_column=TRANSACTIONS_METRO_SCORE_COLUMN[1],
                        transactions_state_score_column=TRANSACTIONS_STATE_SCORE_COLUMN[1],
                        sales_country_score_column=SALES_COUNTRY_SCORE_COLUMN[1],
                        sales_metro_score_column=SALES_METRO_SCORE_COLUMN[1],
                        sales_state_score_column=SALES_STATE_SCORE_COLUMN[1],
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
                        region_id=REGION_ID_COLUMN[0],
                        month=MONTH_COLUMN[0],
                        category=CATEGORY_COLUMN[0],
                        total_merchants_column=TOTAL_MERCHANTS_COLUMN[0],
                        ticket_size_country_score_column=TICKET_SIZE_COUNTRY_SCORE_COLUMN[0],
                        ticket_size_metro_score_column=TICKET_SIZE_METRO_SCORE_COLUMN[0],
                        ticket_size_state_score_column=TICKET_SIZE_STATE_SCORE_COLUMN[0],
                        growth_country_score_column=GROWTH_COUNTRY_SCORE_COLUMN[0],
                        growth_metro_score_column=GROWTH_METRO_SCORE_COLUMN[0],
                        growth_state_score_column=GROWTH_STATE_SCORE_COLUMN[0],
                        stability_country_score_column=STABILITY_COUNTRY_SCORE_COLUMN[0],
                        stability_metro_score_column=STABILITY_METRO_SCORE_COLUMN[0],
                        stability_state_score_column=STABILITY_STATE_SCORE_COLUMN[0],
                        transactions_country_score_column=TRANSACTIONS_COUNTRY_SCORE_COLUMN[0],
                        transactions_metro_score_column=TRANSACTIONS_METRO_SCORE_COLUMN[0],
                        transactions_state_score_column=TRANSACTIONS_STATE_SCORE_COLUMN[0],
                        sales_country_score_column=SALES_COUNTRY_SCORE_COLUMN[0],
                        sales_metro_score_column=SALES_METRO_SCORE_COLUMN[0],
                        sales_state_score_column=SALES_STATE_SCORE_COLUMN[0],
                        region_type=REGION_TYPE_COLUMN[0],
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
