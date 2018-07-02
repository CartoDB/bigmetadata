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

MONTH_COLUMN = 'month'
CATEGORY_COLUMN = 'category'
REGION_TYPE_COLUMN = 'region_type'
REGION_ID_COLUMN = 'region_id'
TICKET_SIZE_SCORE_COLUMN = 'ticket_size_score'
GROWTH_SCORE_COLUMN = 'growth_score'
STABILITY_SCORE_COLUMN = 'stability_score'
TRANSACTIONS_SCORE_COLUMN = 'transactions_score'
SALES_SCORE_COLUMN = 'sales_score'
TICKET_SIZE_COUNTRY_PCTILE_COLUMN = 'ticket_size_country_pctile'
TICKET_SIZE_METRO_PCTILE_COLUMN = 'ticket_size_metro_pctile'
TICKET_SIZE_STATE_PCTILE_COLUMN = 'ticket_size_state_pctile'
GROWTH_COUNTRY_PCTILE_COLUMN = 'growth_country_pctile'
GROWTH_METRO_PCTILE_COLUMN = 'growth_metro_pctile'
GROWTH_STATE_PCTILE_COLUMN = 'growth_state_pctile'
STABILITY_COUNTRY_PCTILE_COLUMN = 'stability_country_pctile'
STABILITY_METRO_PCTILE_COLUMN = 'stability_metro_pctile'
STABILITY_STATE_PCTILE_COLUMN = 'stability_state_pctile'
TRANSACTIONS_COUNTRY_PCTILE_COLUMN = 'transactions_country_pctile'
TRANSACTIONS_METRO_PCTILE_COLUMN = 'transactions_metro_pctile'
TRANSACTIONS_STATE_PCTILE_COLUMN = 'transactions_state_pctile'
SALES_COUNTRY_PCTILE_COLUMN = 'sales_country_pctile'
SALES_METRO_PCTILE_COLUMN = 'sales_metro_pctile'
SALES_STATE_PCTILE_COLUMN = 'sales_state_pctile'


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

    def columns(self):
        return [
            REGION_ID_COLUMN,
            MONTH_COLUMN,
            CATEGORY_COLUMN,
            TICKET_SIZE_SCORE_COLUMN,
            GROWTH_SCORE_COLUMN,
            STABILITY_SCORE_COLUMN,
            TRANSACTIONS_SCORE_COLUMN,
            SALES_SCORE_COLUMN,
            TICKET_SIZE_COUNTRY_PCTILE_COLUMN,
            TICKET_SIZE_METRO_PCTILE_COLUMN,
            TICKET_SIZE_STATE_PCTILE_COLUMN,
            GROWTH_COUNTRY_PCTILE_COLUMN,
            GROWTH_METRO_PCTILE_COLUMN,
            GROWTH_STATE_PCTILE_COLUMN,
            STABILITY_COUNTRY_PCTILE_COLUMN,
            STABILITY_METRO_PCTILE_COLUMN,
            STABILITY_STATE_PCTILE_COLUMN,
            TRANSACTIONS_COUNTRY_PCTILE_COLUMN,
            TRANSACTIONS_METRO_PCTILE_COLUMN,
            TRANSACTIONS_STATE_PCTILE_COLUMN,
            SALES_COUNTRY_PCTILE_COLUMN,
            SALES_METRO_PCTILE_COLUMN,
            SALES_STATE_PCTILE_COLUMN
        ]

    def run(self):
        session = current_session()

        try:
            query = '''
                    CREATE TABLE {table} (
                        {region_id} TEXT,
                        {month} TEXT,
                        {category} TEXT,
                        {ticker_size} NUMERIC,
                        {growth_score} NUMERIC,
                        {stability_score} NUMERIC,
                        {transactions_score} NUMERIC,
                        {sales_score} NUMERIC,
                        {ticket_size_country_pctile_column} NUMERIC,
                        {ticket_size_metro_pctile_column} NUMERIC,
                        {ticket_size_state_pctile_column} NUMERIC,
                        {growth_country_pctile_column} NUMERIC,
                        {growth_metro_pctile_column} NUMERIC,
                        {growth_state_pctile_column} NUMERIC,
                        {stability_country_pctile_column} NUMERIC,
                        {stability_metro_pctile_column} NUMERIC,
                        {stability_state_pctile_column} NUMERIC,
                        {transactions_country_pctile_column} NUMERIC,
                        {transactions_metro_pctile_column} NUMERIC,
                        {transactions_state_pctile_column} NUMERIC,
                        {sales_country_pctile_column} NUMERIC,
                        {sales_metro_pctile_column} NUMERIC,
                        {sales_state_pctile_column} NUMERIC,
                        PRIMARY KEY ({region_id}, {month}, {category})
                    )
                    '''.format(
                        table=self.output().table,
                        region_id=REGION_ID_COLUMN,
                        month=MONTH_COLUMN,
                        category=CATEGORY_COLUMN,
                        ticker_size=TICKET_SIZE_SCORE_COLUMN,
                        growth_score=GROWTH_SCORE_COLUMN,
                        stability_score=STABILITY_SCORE_COLUMN,
                        transactions_score=TRANSACTIONS_SCORE_COLUMN,
                        sales_score=SALES_SCORE_COLUMN,
                        ticket_size_country_pctile_column=TICKET_SIZE_COUNTRY_PCTILE_COLUMN,
                        ticket_size_metro_pctile_column=TICKET_SIZE_METRO_PCTILE_COLUMN,
                        ticket_size_state_pctile_column=TICKET_SIZE_STATE_PCTILE_COLUMN,
                        growth_country_pctile_column=GROWTH_COUNTRY_PCTILE_COLUMN,
                        growth_metro_pctile_column=GROWTH_METRO_PCTILE_COLUMN,
                        growth_state_pctile_column=GROWTH_STATE_PCTILE_COLUMN,
                        stability_country_pctile_column=STABILITY_COUNTRY_PCTILE_COLUMN,
                        stability_metro_pctile_column=STABILITY_METRO_PCTILE_COLUMN,
                        stability_state_pctile_column=STABILITY_STATE_PCTILE_COLUMN,
                        transactions_country_pctile_column=TRANSACTIONS_COUNTRY_PCTILE_COLUMN,
                        transactions_metro_pctile_column=TRANSACTIONS_METRO_PCTILE_COLUMN,
                        transactions_state_pctile_column=TRANSACTIONS_STATE_PCTILE_COLUMN,
                        sales_country_pctile_column=SALES_COUNTRY_PCTILE_COLUMN,
                        sales_metro_pctile_column=SALES_METRO_PCTILE_COLUMN,
                        sales_state_pctile_column=SALES_STATE_PCTILE_COLUMN,
                    )
            session.execute(query)

            query = '''
                    INSERT INTO {output_table}
                    SELECT {region_id}, {month}, {category},
                           {ticker_size}::NUMERIC, {growth_score}::NUMERIC, {stability_score}::NUMERIC,
                           {transactions_score}::NUMERIC,
                           {sales_score}::NUMERIC,
                           {ticket_size_country_pctile_column}::NUMERIC,
                           {ticket_size_metro_pctile_column}::NUMERIC,
                           {ticket_size_state_pctile_column}::NUMERIC,
                           {growth_country_pctile_column}::NUMERIC,
                           {growth_metro_pctile_column}::NUMERIC,
                           {growth_state_pctile_column}::NUMERIC,
                           {stability_country_pctile_column}::NUMERIC,
                           {stability_metro_pctile_column}::NUMERIC,
                           {stability_state_pctile_column}::NUMERIC,
                           {transactions_country_pctile_column}::NUMERIC,
                           {transactions_metro_pctile_column}::NUMERIC,
                           {transactions_state_pctile_column}::NUMERIC,
                           {sales_country_pctile_column}::NUMERIC,
                           {sales_metro_pctile_column}::NUMERIC,
                           {sales_state_pctile_column}::NUMERIC
                          FROM {input_table}
                          WHERE {region_type} = '{geography}'
                    '''.format(
                        output_table=self.output().table,
                        input_table=self.input().table,
                        region_id=REGION_ID_COLUMN,
                        month=MONTH_COLUMN,
                        category=CATEGORY_COLUMN,
                        ticker_size=TICKET_SIZE_SCORE_COLUMN,
                        growth_score=GROWTH_SCORE_COLUMN,
                        stability_score=STABILITY_SCORE_COLUMN,
                        transactions_score=TRANSACTIONS_SCORE_COLUMN,
                        sales_score=SALES_SCORE_COLUMN,
                        ticket_size_country_pctile_column=TICKET_SIZE_COUNTRY_PCTILE_COLUMN,
                        ticket_size_metro_pctile_column=TICKET_SIZE_METRO_PCTILE_COLUMN,
                        ticket_size_state_pctile_column=TICKET_SIZE_STATE_PCTILE_COLUMN,
                        growth_country_pctile_column=GROWTH_COUNTRY_PCTILE_COLUMN,
                        growth_metro_pctile_column=GROWTH_METRO_PCTILE_COLUMN,
                        growth_state_pctile_column=GROWTH_STATE_PCTILE_COLUMN,
                        stability_country_pctile_column=STABILITY_COUNTRY_PCTILE_COLUMN,
                        stability_metro_pctile_column=STABILITY_METRO_PCTILE_COLUMN,
                        stability_state_pctile_column=STABILITY_STATE_PCTILE_COLUMN,
                        transactions_country_pctile_column=TRANSACTIONS_COUNTRY_PCTILE_COLUMN,
                        transactions_metro_pctile_column=TRANSACTIONS_METRO_PCTILE_COLUMN,
                        transactions_state_pctile_column=TRANSACTIONS_STATE_PCTILE_COLUMN,
                        sales_country_pctile_column=SALES_COUNTRY_PCTILE_COLUMN,
                        sales_metro_pctile_column=SALES_METRO_PCTILE_COLUMN,
                        sales_state_pctile_column=SALES_STATE_PCTILE_COLUMN,
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
