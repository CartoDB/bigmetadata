import os
import urllib.request
import itertools
from luigi import Parameter, WrapperTask
from tasks.base_tasks import (RepoFileUnzipTask, CSV2TempTableTask, TempTableTask)
from tasks.meta import current_session
from tasks.targets import PostgresTarget

from lib.logger import get_logger

LOGGER = get_logger(__name__)

MC_SCHEMA = 'us.mastercard'
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

MONTH = 'month'
CATEGORY = 'category'
REGION_TYPE = 'region_type'
REGION_ID = 'region_id'
TOTAL_MERCHANTS = 'total_merchants'
TICKET_SIZE_SCORE = 'ticket_size_score'
TICKET_SIZE_COUNTRY_SCORE = 'ticket_size_country_score'
TICKET_SIZE_METRO_SCORE = 'ticket_size_metro_score'
TICKET_SIZE_STATE_SCORE = 'ticket_size_state_score'
GROWTH_SCORE = 'growth_score'
GROWTH_COUNTRY_SCORE = 'growth_country_score'
GROWTH_METRO_SCORE = 'growth_metro_score'
GROWTH_STATE_SCORE = 'growth_state_score'
STABILITY_SCORE = 'stability_score'
STABILITY_COUNTRY_SCORE = 'stability_country_score'
STABILITY_METRO_SCORE = 'stability_metro_score'
STABILITY_STATE_SCORE = 'stability_state_score'
TRANSACTIONS_SCORE = 'transactions_score'
TRANSACTIONS_COUNTRY_SCORE = 'transactions_country_score'
TRANSACTIONS_METRO_SCORE = 'transactions_metro_score'
TRANSACTIONS_STATE_SCORE = 'transactions_state_score'
SALES_SCORE = 'sales_score'
SALES_COUNTRY_SCORE = 'sales_country_score'
SALES_METRO_SCORE = 'sales_metro_score'
SALES_STATE_SCORE = 'sales_state_score'

# (file_source_column, db_target_column)
MONTH_COLUMN = (MONTH, MONTH)
CATEGORY_COLUMN = (CATEGORY, CATEGORY)
REGION_TYPE_COLUMN = (REGION_TYPE, REGION_TYPE)
REGION_ID_COLUMN = (REGION_ID, REGION_ID)
TOTAL_MERCHANTS_COLUMN = (TOTAL_MERCHANTS, TOTAL_MERCHANTS)
TICKET_SIZE_COUNTRY_SCORE_COLUMN = (TICKET_SIZE_SCORE, TICKET_SIZE_COUNTRY_SCORE)
TICKET_SIZE_METRO_SCORE_COLUMN = (TICKET_SIZE_METRO_SCORE, TICKET_SIZE_METRO_SCORE)
TICKET_SIZE_STATE_SCORE_COLUMN = (TICKET_SIZE_STATE_SCORE, TICKET_SIZE_STATE_SCORE)
GROWTH_COUNTRY_SCORE_COLUMN = (GROWTH_SCORE, GROWTH_COUNTRY_SCORE)
GROWTH_METRO_SCORE_COLUMN = (GROWTH_METRO_SCORE, GROWTH_METRO_SCORE)
GROWTH_STATE_SCORE_COLUMN = (GROWTH_STATE_SCORE, GROWTH_STATE_SCORE)
STABILITY_COUNTRY_SCORE_COLUMN = (STABILITY_SCORE, STABILITY_COUNTRY_SCORE)
STABILITY_METRO_SCORE_COLUMN = (STABILITY_METRO_SCORE, STABILITY_METRO_SCORE)
STABILITY_STATE_SCORE_COLUMN = (STABILITY_STATE_SCORE, STABILITY_STATE_SCORE)
TRANSACTIONS_COUNTRY_SCORE_COLUMN = (TRANSACTIONS_SCORE, TRANSACTIONS_COUNTRY_SCORE)
TRANSACTIONS_METRO_SCORE_COLUMN = (TRANSACTIONS_METRO_SCORE, TRANSACTIONS_METRO_SCORE)
TRANSACTIONS_STATE_SCORE_COLUMN = (TRANSACTIONS_STATE_SCORE, TRANSACTIONS_STATE_SCORE)
SALES_COUNTRY_SCORE_COLUMN = (SALES_SCORE, SALES_COUNTRY_SCORE)
SALES_METRO_SCORE_COLUMN = (SALES_METRO_SCORE, SALES_METRO_SCORE)
SALES_STATE_SCORE_COLUMN = (SALES_STATE_SCORE, SALES_STATE_SCORE)

MEASUREMENT_COLUMNS = [
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
]

CATEGORIES = {
    'NEP': 'non eating places',
    'EP': 'eating places',
    'APP': 'apparel',
    'SB': 'small business',
    'TR': 'total retail',
}


class DownloadUnzipMasterCard(RepoFileUnzipTask):
    URL = 'http://172.17.0.1/mastercard/my_fake_data.zip'

    def get_url(self):
        return self.URL


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


class MasterCardDataBaseTable(TempTableTask):
    geography = Parameter()

    def requires(self):
        return ImportMasterCardData()

    def run(self):
        session = current_session()

        geography = self.geography
        if self.geography == 'block_group':
            geography = 'block group'

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
                        geography=geography
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


class MasterCardData(TempTableTask):
    geography = Parameter()

    def requires(self):
        return MasterCardDataBaseTable(geography=self.geography)

    def _create_table(self, session):
        LOGGER.info('Creating table {}'.format(self.output().table))

        fields = ','.join(['{}_{} NUMERIC'.format(x[0].lower(), x[1].lower()) for x in
                           itertools.product([x[1] for x in MEASUREMENT_COLUMNS],
                                             CATEGORIES.keys())])
        query = '''
                CREATE TABLE "{schema}".{table} (
                    {region_id} text NOT NULL,
                    {month} text NOT NULL,
                    {fields}
                );
                '''.format(
                        schema=self.output().schema,
                        table=self.output().tablename,
                        region_id=REGION_ID_COLUMN[1],
                        month=MONTH_COLUMN[1],
                        fields=fields,
                    )
        session.execute(query)
        session.commit()

    def _insert_tr(self, session):
        LOGGER.info('Inserting "total retail" data into {}'.format(self.output().table))

        output_fields = ','.join(['{}_tr'.format(x[1].lower()) for x in MEASUREMENT_COLUMNS])
        input_fields = ','.join(['mc.{field} as {field}_tr'.format(field=x[1].lower())
                                 for x in MEASUREMENT_COLUMNS])

        query = '''
                INSERT INTO "{output_schema}".{output_table} (
                    {region_id},
                    {month},
                    {output_fields}
                )
                SELECT
                    mc.{region_id},
                    mc.{month},
                    {input_fields}
                    FROM
                        "{input_schema}".{input_table} mc
                    WHERE mc.category = 'total retail';
                '''.format(
                        output_schema=self.output().schema,
                        output_table=self.output().tablename,
                        region_id=REGION_ID_COLUMN[1],
                        month=MONTH_COLUMN[1],
                        output_fields=output_fields,
                        input_fields=input_fields,
                        input_schema=self.input().schema,
                        input_table=self.input().tablename,
                    )
        session.execute(query)
        session.commit()

    def _create_constraints(self, session):
        LOGGER.info('Creating constraints for {}'.format(self.output().table))

        query = '''
                ALTER TABLE "{schema}".{table}
                ADD PRIMARY KEY({region_id}, {month});
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                    region_id=REGION_ID_COLUMN[1],
                    month=MONTH_COLUMN[1],
                )
        session.execute(query)
        session.commit()

    def _update_category(self, session, category):
        LOGGER.info('Updating {} with "{}" category'.format(self.output().table, category[1]))

        fields = ','.join(['{field}_{category} = cat.{field}'.format(
                                field=x[1].lower(), category=category[0].lower())
                           for x in MEASUREMENT_COLUMNS])

        query = '''
                UPDATE "{output_schema}".{output_table} as mcc SET
                    {fields}
                FROM
                    "{input_schema}".{input_table} cat
                WHERE mcc.{region_id} = cat.{region_id}
                  AND mcc.{month} = cat.{month}
                  AND cat.category = '{category}';
                '''.format(
                    output_schema=self.output().schema,
                    output_table=self.output().tablename,
                    region_id=REGION_ID_COLUMN[1],
                    month=MONTH_COLUMN[1],
                    input_schema=self.input().schema,
                    input_table=self.input().tablename,
                    fields=fields,
                    category=category[1]
                )
        session.execute(query)
        session.commit()

    def run(self):
        session = current_session()
        try:
            self._create_table(session)
            self._insert_tr(session)  # Insert the 'total retail' data
            self._create_constraints(session)
            for catid, catname in CATEGORIES.items():  # Insert the rest of the categories
                if catid != 'TR':
                    self._update_category(session, (catid, catname))
        except Exception as e:
            LOGGER.error('Error creating/populating {table}: {error}'.format(
                table=self.output().table,
                error=str(e)
            ))
            session.rollback()
            session.execute('DROP TABLE "{schema}".{table};'.format(schema=self.output().schema,
                                                                    table=self.output().tablename,))
            raise e

    def output(self):
        return PostgresTarget(MC_SCHEMA, 'mc_' + self.geography)


class AllMasterCardData(WrapperTask):
    def requires(self):
        for geography in GEOGRAPHIES:
            yield MasterCardData(geography=geography)
