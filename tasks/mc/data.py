import os, re, sqlalchemy
import urllib.request
import itertools
from luigi import Parameter, WrapperTask
from tasks.base_tasks import (RepoFileGUnzipTask, CSV2TempTableTask, TempTableTask)
from tasks.meta import current_session
from tasks.targets import PostgresTarget

from lib.logger import get_logger

LOGGER = get_logger(__name__)

MONTH = 'month'
CATEGORY = 'category'
REGION_TYPE = 'region_type'
REGION_ID = 'region_id'
TOTAL_MERCHANTS = 'total_merchants'
TICKET_SIZE_SCORE = 'ticket_size_score'
TICKET_SIZE_COUNTRY_SCORE = 'ticket_size_{country_level_name}_score'
TICKET_SIZE_METRO_SCORE = 'ticket_size_{metro_level_name}_score'
TICKET_SIZE_STATE_SCORE = 'ticket_size_{state_level_name}_score'
GROWTH_SCORE = 'growth_score'
GROWTH_COUNTRY_SCORE = 'growth_{country_level_name}_score'
GROWTH_METRO_SCORE = 'growth_{metro_level_name}_score'
GROWTH_STATE_SCORE = 'growth_{state_level_name}_score'
STABILITY_SCORE = 'stability_score'
STABILITY_COUNTRY_SCORE = 'stability_{country_level_name}_score'
STABILITY_METRO_SCORE = 'stability_{metro_level_name}_score'
STABILITY_STATE_SCORE = 'stability_{state_level_name}_score'
TRANSACTIONS_SCORE = 'transactions_score'
TRANSACTIONS_COUNTRY_SCORE = 'transactions_{country_level_name}_score'
TRANSACTIONS_METRO_SCORE = 'transactions_{metro_level_name}_score'
TRANSACTIONS_STATE_SCORE = 'transactions_{state_level_name}_score'
SALES_SCORE = 'sales_score'
SALES_COUNTRY_SCORE = 'sales_{country_level_name}_score'
SALES_METRO_SCORE = 'sales_{metro_level_name}_score'
SALES_STATE_SCORE = 'sales_{state_level_name}_score'

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

GEOGRAPHIES = {
    'us': ['block',  'block group', 'tract', 'zcta5', 'county', 'state'],
    'ca': ['province', 'census division', 'dissemination area', 'dissemination block'],
    'au': ['state', 'sa1', 'sa2', 'sa3', 'sa4', 'mesh block'],
    'uk': ['postcode area', 'postcode district', 'postcode sector', 'postcode unit'],
}

STATE_LEVEL_NAME = {
    'ca': 'prov',
    'uk': 'pc_area',
}

METRO_LEVEL_NAME = {
    'uk': 'pc_district',
}


def geoname_format(country, name):
    return name.format(country_level_name='country',
                       state_level_name=STATE_LEVEL_NAME.get(country, 'state'),
                       metro_level_name=METRO_LEVEL_NAME.get(country, 'metro'))


# You can override this host with MC_DOWNLOAD_PATH
# Example: MC_DOWNLOAD_PATH=http://host.docker.internal:8000/mc
MC_PATH = 'http://172.17.0.1:8000/mc'
COMPLETE_URL = '{host}/carto_{country}_mrli_scores.csv.gz'
UNTIL_URL = '{host}/carto_{country}_mrli_scores_until_{month}.csv.gz'
MONTH_URL = '{host}/carto_{country}_mrli_scores_{month}.csv.gz'

INPUT_FILE_GEOGRAPHY_ALIAS = {
    'zcta5': 'zip code'
}

INPUT_FILE_COUNTRY_ALIAS = {
    'au': 'aus',
    'ca': 'can',
    'uk': 'gbr',
    'us': 'usa'
}


class DownloadGUnzipMC(RepoFileGUnzipTask):
    country = Parameter()
    until_month = Parameter(default=None)
    month = Parameter(default=None)
    # Needed if the file is not the full month but a subset (for example,
    # it only contains zip codes)
    content = Parameter(default=None)

    def get_url(self):
        path = os.environ.get('MC_DOWNLOAD_PATH', MC_PATH)
        if self.until_month:
            return UNTIL_URL.format(
                host=path,
                country=INPUT_FILE_COUNTRY_ALIAS[self.country],
                month=self.until_month)
        elif self.month:
            return MONTH_URL.format(
                host=path,
                country=INPUT_FILE_COUNTRY_ALIAS[self.country],
                month=self.month)
        else:
            return COMPLETE_URL.format(
                host=path,
                country=INPUT_FILE_COUNTRY_ALIAS[self.country])


class ImportMCData(CSV2TempTableTask):
    country = Parameter()
    until_month = Parameter(default=None)
    month = Parameter(default=None)
    # Needed if the file is not the full month but a subset (for example,
    # it only contains zip codes)
    content = Parameter(default=None)

    FILE_EXTENSION = 'csv'

    def requires(self):
        return DownloadGUnzipMC(country=self.country,
                                until_month=self.until_month,
                                month=self.month,
                                content=self.content)

    def coldef(self):
        '''
        :return: Lowercased column names
        '''
        uppercased = super(ImportMCData, self).coldef()
        return [(t[0].lower(), t[1]) for t in uppercased]

    def input_csv(self):
        for file in os.listdir(self.input().path):
            if file.endswith('.{}'.format(self.FILE_EXTENSION)):
                return os.path.join(self.input().path, file)


class MCDataBaseTable(TempTableTask):
    country = Parameter()
    geography = Parameter()
    until_month = Parameter(default=None)
    month = Parameter(default=None)
    # Needed if the file is not the full month but a subset (for example,
    # it only contains zip codes)
    content = Parameter(default=None)

    def requires(self):
        return ImportMCData(country=self.country, until_month=self.until_month,
                            month=self.month, content=self.content)

    def get_geography_name(self):
        return self.geography.replace('_', ' ')

    def run(self):
        session = current_session()
        geography = self.get_geography_name()

        if geography not in GEOGRAPHIES[self.country]:
            raise ValueError('Invalid geography: "{}"'.format(geography))

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
                        ticket_size_country_score_column=geoname_format(self.country,
                                                                        TICKET_SIZE_COUNTRY_SCORE_COLUMN[1]),
                        ticket_size_metro_score_column=geoname_format(self.country,
                                                                      TICKET_SIZE_METRO_SCORE_COLUMN[1]),
                        ticket_size_state_score_column=geoname_format(self.country,
                                                                      TICKET_SIZE_STATE_SCORE_COLUMN[1]),
                        growth_country_score_column=geoname_format(self.country,
                                                                   GROWTH_COUNTRY_SCORE_COLUMN[1]),
                        growth_metro_score_column=geoname_format(self.country,
                                                                 GROWTH_METRO_SCORE_COLUMN[1]),
                        growth_state_score_column=geoname_format(self.country,
                                                                 GROWTH_STATE_SCORE_COLUMN[1]),
                        stability_country_score_column=geoname_format(self.country,
                                                                      STABILITY_COUNTRY_SCORE_COLUMN[1]),
                        stability_metro_score_column=geoname_format(self.country,
                                                                    STABILITY_METRO_SCORE_COLUMN[1]),
                        stability_state_score_column=geoname_format(self.country,
                                                                    STABILITY_STATE_SCORE_COLUMN[1]),
                        transactions_country_score_column=geoname_format(self.country,
                                                                         TRANSACTIONS_COUNTRY_SCORE_COLUMN[1]),
                        transactions_metro_score_column=geoname_format(self.country,
                                                                       TRANSACTIONS_METRO_SCORE_COLUMN[1]),
                        transactions_state_score_column=geoname_format(self.country,
                                                                       TRANSACTIONS_STATE_SCORE_COLUMN[1]),
                        sales_country_score_column=geoname_format(self.country,
                                                                  SALES_COUNTRY_SCORE_COLUMN[1]),
                        sales_metro_score_column=geoname_format(self.country,
                                                                SALES_METRO_SCORE_COLUMN[1]),
                        sales_state_score_column=geoname_format(self.country,
                                                                SALES_STATE_SCORE_COLUMN[1]),
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
                        ticket_size_country_score_column=geoname_format(self.country,
                                                                        TICKET_SIZE_COUNTRY_SCORE_COLUMN[0]),
                        ticket_size_metro_score_column=geoname_format(self.country,
                                                                      TICKET_SIZE_METRO_SCORE_COLUMN[0]),
                        ticket_size_state_score_column=geoname_format(self.country,
                                                                      TICKET_SIZE_STATE_SCORE_COLUMN[0]),
                        growth_country_score_column=geoname_format(self.country,
                                                                   GROWTH_COUNTRY_SCORE_COLUMN[0]),
                        growth_metro_score_column=geoname_format(self.country,
                                                                 GROWTH_METRO_SCORE_COLUMN[0]),
                        growth_state_score_column=geoname_format(self.country,
                                                                 GROWTH_STATE_SCORE_COLUMN[0]),
                        stability_country_score_column=geoname_format(self.country,
                                                                      STABILITY_COUNTRY_SCORE_COLUMN[0]),
                        stability_metro_score_column=geoname_format(self.country,
                                                                    STABILITY_METRO_SCORE_COLUMN[0]),
                        stability_state_score_column=geoname_format(self.country,
                                                                    STABILITY_STATE_SCORE_COLUMN[0]),
                        transactions_country_score_column=geoname_format(self.country,
                                                                         TRANSACTIONS_COUNTRY_SCORE_COLUMN[0]),
                        transactions_metro_score_column=geoname_format(self.country,
                                                                       TRANSACTIONS_METRO_SCORE_COLUMN[0]),
                        transactions_state_score_column=geoname_format(self.country,
                                                                       TRANSACTIONS_STATE_SCORE_COLUMN[0]),
                        sales_country_score_column=geoname_format(self.country,
                                                                  SALES_COUNTRY_SCORE_COLUMN[0]),
                        sales_metro_score_column=geoname_format(self.country,
                                                                SALES_METRO_SCORE_COLUMN[0]),
                        sales_state_score_column=geoname_format(self.country,
                                                                SALES_STATE_SCORE_COLUMN[0]),
                        region_type=REGION_TYPE_COLUMN[0],
                        geography=INPUT_FILE_GEOGRAPHY_ALIAS.get(geography, geography)
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


class MCData(TempTableTask):
    country = Parameter()
    geography = Parameter()
    until_month = Parameter(default=None)
    month = Parameter(default=None)
    # Needed if the file is not the full month but a subset (for example,
    # it only contains zip codes)
    content = Parameter(default=None)

    @property
    def mc_schema(self):
        return '{country}.mastercard'.format(country=self.country)

    def requires(self):
        return MCDataBaseTable(country=self.country, geography=self.geography,
                               until_month=self.until_month, month=self.month,
                               content=self.content)

    def _create_table(self, session):
        LOGGER.info('Creating table {}'.format(self.output().table))

        fields = ','.join(['{}_{} NUMERIC'.format(geoname_format(self.country, x[0].lower()), x[1].lower())
                           for x in itertools.product([x[1] for x in MEASUREMENT_COLUMNS],
                           CATEGORIES.keys())])

        query = '''
                CREATE SCHEMA IF NOT EXISTS "{schema}"
                '''.format(schema=self.output().schema)
        session.execute(query)

        query = '''
                CREATE TABLE IF NOT EXISTS "{schema}".{table} (
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

        output_fields = ','.join(['{}_tr'.format(geoname_format(self.country, x[1].lower()))
                                  for x in MEASUREMENT_COLUMNS])
        input_fields = ','.join(['mc.{field} as {field}_tr'.format(field=geoname_format(self.country, x[1].lower()))
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
                    WHERE mc.category = 'total retail'
                    AND NOT EXISTS
                      (
                       select 1
                       from "{output_schema}".{output_table} mc2
                       where mc.{region_id} = mc2.{region_id}
                       and mc.{month} = mc2.{region_id}
                       limit 1
                       );
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
        output = self.output()
        LOGGER.info('Creating constraints for {}'.format(output.table))

        query = '''
                ALTER TABLE "{schema}".{table}
                ADD PRIMARY KEY({region_id}, {month});
                '''.format(
                    schema=output.schema,
                    table=output.tablename,
                    region_id=REGION_ID_COLUMN[1],
                    month=MONTH_COLUMN[1],
                )
        try:
            session.execute(query)
        except sqlalchemy.exc.ProgrammingError as e:
            if e.orig.pgcode == '42P16':
                LOGGER.info('PK already exists at {}'.format(output.tablename))
            else:
                raise e
        session.commit()

    def _update_category(self, session, category):
        LOGGER.info('Updating {} with "{}" category'.format(self.output().table, category[1]))

        fields = ','.join(['{field}_{category} = cat.{field}'.format(
                                field=geoname_format(self.country, x[1].lower()), category=category[0].lower())
                           for x in MEASUREMENT_COLUMNS])

        query = '''
                UPDATE "{output_schema}".{output_table} as mcc SET
                    {fields}
                FROM
                    "{input_schema}".{input_table} cat
                WHERE mcc.{region_id} = cat.{region_id}
                  AND mcc.{month} = cat.{month}
                  AND cat.category = '{category}'
                  AND NOT EXISTS
                      (
                       select 1
                       from "{output_schema}".{output_table} mc2
                       where cat.{region_id} = mc2.{region_id}
                       and cat.{month} = mc2.{region_id}
                       limit 1
                       );
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
        return PostgresTarget(self.mc_schema, 'mc_' + self.geography)

    @staticmethod
    def _month_code_to_date(yyyymm):
        return '{month}/01/{year}'.format(month=yyyymm[4:6], year=yyyymm[0:4])

    def complete(self):
        return super(MCData, self).complete() and self.is_month_loaded()

    def is_month_loaded(self):
        session = current_session()
        month = self.month if self.month else self.until_month
        if not month:  # in case of initial full load there's no month check
            return True

        month_string = self._month_code_to_date(month)
        query = "select 1 from {table} where month = '{month}' limit 1"
        result = session.execute(query.format(table=self.output().table,
                                              month=month_string))
        return len(result.fetchall()) > 0

class AllMCData(WrapperTask):
    country = Parameter()
    until_month = Parameter(default=None)
    month = Parameter(default=None)
    # Needed if the file is not the full month but a subset (for example,
    # it only contains zip codes)
    content = Parameter(default=None)

    def requires(self):
        return [MCData(geography=geography, country=self.country,
                until_month=self.until_month, month=self.month,
                content=self.content)
         for geography in [x.replace(' ', '_')
                           for x in GEOGRAPHIES[self.country]]]


class AllMCCountries(WrapperTask):
    until_month = Parameter(default=None)
    month = Parameter(default=None)

    def requires(self):
        return [AllMCData(country=country, until_month=self.until_month, month=self.month)
            for country in ['us', 'ca', 'uk', 'au']]

