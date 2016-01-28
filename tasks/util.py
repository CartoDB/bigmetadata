'''
Util functions for luigi bigmetadata tasks.
'''

import os
import subprocess
import logging
import sys
import time
import re
import requests

import elasticsearch

from luigi import Task, Parameter, LocalTarget, Target
from luigi.postgres import PostgresTarget


def elastic_conn(index_name='bigmetadata', logger=None):
    '''
    Obtain an index with specified name.  Waits for elasticsearch to start.
    Returns elasticsearch.
    '''
    elastic = elasticsearch.Elasticsearch([{
        'host': os.environ.get('ES_HOST', 'localhost'),
        'port': os.environ.get('ES_PORT', '9200')
    }])
    while True:
        try:
            elastic.indices.create(index=index_name, ignore=400)  # pylint: disable=unexpected-keyword-arg
            break
        except elasticsearch.exceptions.ConnectionError:
            if logger:
                logger.info('waiting for elasticsearch')
            time.sleep(1)
    return elastic


def get_logger(name):
    '''
    Obtain a logger outputing to stderr with specified name. Defaults to INFO
    log level.
    '''
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(message)s'))
    logger.addHandler(handler)
    return logger


def pg_cursor():
    '''
    Obtain a cursor on a fresh connection to postgres.
    '''
    target = DefaultPostgresTarget(table='foo', update_id='bar')
    return target.connect().cursor()


def shell(cmd):
    '''
    Run a shell command. Returns the STDOUT output.
    '''
    return subprocess.check_output(cmd, shell=True)


def slug_column(column_name):
    '''
    Turn human-readable column name into a decent one for a SQL table
    '''
    translations = {
        'population': 'pop',
        'for_whom': '',
        'u_s': 'us',
        '_is_': '_',
        'in_the_past_12_months': '',
        'black_or_african_american': 'black',
        'percentage_of': 'percent'
    }
    # TODO handle accents etc properly
    column_name = re.sub(r'[^a-z0-9]+', '_', column_name.lower())
    for before, after in translations.iteritems():
        column_name = column_name.replace(before, after)
    column_name = re.sub(r'[^a-z0-9]+', '_', column_name).strip('_')
    return column_name

class MetadataTarget(LocalTarget):
    '''
    Target that ensures metadata exists.
    '''
    pass


def classpath(obj):
    '''
    Path to this task, suitable for the current OS.
    '''
    return os.path.join(*obj.__module__.split('.')[1:])


def query_cartodb(query):
    carto_url = 'https://{}/api/v2/sql'.format(os.environ['CARTODB_DOMAIN'])
    resp = requests.post(carto_url, data={
        'api_key': os.environ['CARTODB_API_KEY'],
        'q': query
    })
    #assert resp.status_code == 200
    #if resp.status_code != 200:
    #    raise Exception(u'Non-200 response ({}) from carto: {}'.format(
    #        resp.status_code, resp.text))
    return resp


def sql_to_cartodb_table(tablename, query):
    '''
    Move the results of the specified query to cartodb
    '''
    query = query.replace("'", '\'"\'"\'')
    cmd = u'''
ogr2ogr --config CARTODB_API_KEY $CARTODB_API_KEY \
        -f CartoDB "CartoDB:observatory" \
        -overwrite \
        -nln "{tablename}" \
        PG:"dbname=$PGDATABASE" -sql '{sql}'
    '''.format(tablename=tablename, sql=query)
    shell(cmd)
    query_cartodb(u"SELECT CDB_CartodbfyTable('{tablename}')".format(
        tablename=tablename))


class ColumnTarget(MetadataTarget):
    '''
    Column target for metadata
    '''

    def __init__(self, **kwargs):
        self.filename = kwargs.pop('filename')
        super(ColumnTarget, self).__init__(
            path=os.path.join('data', 'columns', classpath(self),
                              self.filename + '.json'))

    #def filename(self):
    #    '''
    #    Filename for persistence in bigmetadata
    #    '''
    #    raise NotImplementedError('Must implement filename() for ColumnTarget')


class TableTarget(MetadataTarget):
    '''
    Table target for metadata
    '''

    def __init__(self, **kwargs):
        self.filename = kwargs.pop('filename')
        super(ColumnTarget, self).__init__(
            path=os.path.join('data', 'tables', classpath(self),
                              self.filename + '.json'))

    #def filename(self):
    #    '''
    #    Filename for persistence in bigmetadata
    #    '''
    #    raise NotImplementedError('Must implement filename() for TableTarget')


class DefaultPostgresTarget(PostgresTarget):
    '''
    PostgresTarget which by default uses command-line specified login.
    '''

    def __init__(self, *args, **kwargs):
        kwargs['host'] = kwargs.get('host', os.environ.get('PGHOST', 'localhost'))
        kwargs['port'] = kwargs.get('port', os.environ.get('PGPORT', '5432'))
        kwargs['user'] = kwargs.get('user', os.environ.get('PGUSER', 'postgres'))
        kwargs['password'] = kwargs.get('password', os.environ.get('PGPASSWORD'))
        kwargs['database'] = kwargs.get('database', os.environ.get('PGDATABASE', 'postgres'))
        if 'update_id' not in kwargs:
            kwargs['update_id'] = kwargs['table']
        super(DefaultPostgresTarget, self).__init__(*args, **kwargs)

    def untouch(self, connection=None):
        self.create_marker_table()

        if connection is None:
            connection = self.connect()
            connection.autocommit = True  # if connection created here, we commit it here

        connection.cursor().execute(
            """DELETE FROM {marker_table}
               WHERE update_id = %s AND target_table = %s
            """.format(marker_table=self.marker_table),
            (self.update_id, self.table))

        # make sure update is properly marked
        assert not self.exists(connection)


class LoadPostgresFromURL(Task):

    def load_from_url(self, url):
        '''
        Load psql at a URL into the database.

        Ignores tablespaces assigned in the SQL.
        '''
        subprocess.check_call('curl {url} | gunzip -c | '
                              'grep -v default_tablespace | psql'.format(url=url), shell=True)
        self.output().touch()

    def output(self):
        id_ = self.identifier()
        return DefaultPostgresTarget(table=id_, update_id=id_)

    def identifier(self):
        raise NotImplementedError()


class CartoDBTarget(Target):
    '''
    Target which is a CartoDB table
    '''

    def __init__(self, tablename):
        self.tablename = tablename

    def exists(self):
        resp = query_cartodb('SELECT * FROM "{tablename}" LIMIT 0'.format(
            tablename=self.tablename))
        return resp.status_code == 200

    def remove(self):
        resp = query_cartodb('DROP TABLE "{tablename}"'.format(
            tablename=self.tablename))
        assert resp.status_code == 200
