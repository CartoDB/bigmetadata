import requests
from luigi import Task
from lib.logger import get_logger
from tasks.foot_traffic.data_file import AddLatLngData
from tasks.util import shell

LOGGER = get_logger(__name__)
CLICKHOUSE_HOST = 'clickhouse-server'
DATABASE_NAME = 'foot_traffic'
TABLE_NAME = 'foot_traffic'
QUADKEY_FIELD = 'quadkey'
LONGITUDE_FIELD = 'lon'
LATITUDE_FIELD = 'lat'
DATE_FIELD = 'ftdate'
HOUR_FIELD = 'fthour'
VALUE_FIELD = 'val'


def execute_query(query):
    cmd = 'clickhouse-client --host {host} --query="{query}";'.format(
        host=CLICKHOUSE_HOST,
        query=query,
    )
    return shell(cmd)


class ImportData(Task):
    def requires(self):
        return AddLatLngData()

    def _create_table(self):
        query = '''
                CREATE DATABASE IF NOT EXISTS "{database}";
                '''.format(
                    database=DATABASE_NAME
                )
        execute_query(query)

        query = '''
                CREATE TABLE IF NOT EXISTS {database}.{table} (
                    {quadkey} String,
                    {longitude} Float64,
                    {latitude} Float64,
                    {date} Date,
                    {hour} UInt8,
                    {value} UInt16
                ) ENGINE = MergeTree({date}, ({quadkey}, {longitude}, {latitude}, {date}, {hour}), 8192)
                '''.format(
                    database=DATABASE_NAME,
                    table=TABLE_NAME,
                    quadkey=QUADKEY_FIELD,
                    longitude=LONGITUDE_FIELD,
                    latitude=LATITUDE_FIELD,
                    date=DATE_FIELD,
                    hour=HOUR_FIELD,
                    value=VALUE_FIELD,
                )
        execute_query(query)

    def _import_data(self):
        cmd = 'cat {file} | clickhouse-client --host {host} --query="INSERT INTO {database}.{table} FORMAT CSV"'.format(
            file=self.input().path,
            host=CLICKHOUSE_HOST,
            database=DATABASE_NAME,
            table=TABLE_NAME,
        )
        return shell(cmd)

    def run(self):
        self._create_table()
        self._import_data()

    def complete(self):
        query = '''
                EXISTS TABLE  {database}.{table}
                '''.format(
                    database=DATABASE_NAME,
                    table=TABLE_NAME,
                )
        exists = execute_query(query)
        return exists == '1'
