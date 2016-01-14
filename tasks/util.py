'''
Util functions for luigi bigmetadata tasks.
'''

import os
import subprocess
import elasticsearch
import logging
import sys
import time

from luigi import Task, Parameter, LocalTarget
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
    Run a shell command
    '''
    return subprocess.check_call(cmd, shell=True)


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
