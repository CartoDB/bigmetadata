'''
Util functions for luigi bigmetadata tasks.
'''

import os
import subprocess
import logging
import sys
import time
import re
from itertools import izip_longest
from contextlib import contextmanager

import elasticsearch
import requests

from luigi import Task, Parameter, LocalTarget, Target, BooleanParameter
from luigi.postgres import PostgresTarget

from sqlalchemy import Table
from sqlalchemy.schema import CreateSchema

from tasks.meta import (metadata, Session, ColumnDefinition, TableDefinition,
                        ColumnTableDefinition)


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
    return '.'.join(obj.__module__.split('.')[1:])


def query_cartodb(query):
    #carto_url = 'https://{}/api/v2/sql'.format(os.environ['CARTODB_DOMAIN'])
    carto_url = os.environ['CARTODB_API_URL']
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
        -nlt GEOMETRY \
        -nln "{tablename}" \
        PG:"dbname=$PGDATABASE" -sql '{sql}'
    '''.format(tablename=tablename, sql=query)
    shell(cmd)
    query_cartodb(u"SELECT CDB_CartodbfyTable('{tablename}')".format(
        tablename=tablename))


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


class LoadCSVFromURL(Task):
    '''
    Load CSV from a URL into the database.  Requires a schema, URL, and
    tablename.
    '''

    force = BooleanParameter(default=False)

    def url(self):
        raise NotImplementedError()

    def tableschema(self):
        raise NotImplementedError()

    def tablename(self):
        raise NotImplementedError()

    def schemaname(self):
        return classpath(self)

    def run(self):
        cursor = pg_cursor()
        cursor.execute('CREATE SCHEMA IF NOT EXISTS "{schemaname}"'.format(
            schemaname=self.schemaname()
        ))
        cursor.execute('DROP TABLE IF EXISTS {tablename}'.format(
            tablename=self.output().table
        ))
        cursor.execute('CREATE TABLE {tablename} ({schema})'.format(
            tablename=self.output().table, schema=self.tableschema()))
        cursor.connection.commit()
        shell("curl '{url}' | psql -c 'COPY {table} FROM STDIN WITH CSV HEADER'".format(
            table=self.output().table, url=self.url()
        ))
        self.output().touch()

    def output(self):
        qualified_table = '"{}"."{}"'.format(self.schemaname(), self.tablename())
        target = DefaultPostgresTarget(table=qualified_table)
        if self.force:
            target.untouch()
            self.force = False
        return target


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

    def __str__(self):
        return self.tablename

    def exists(self):
        resp = query_cartodb('SELECT * FROM "{tablename}" LIMIT 0'.format(
            tablename=self.tablename))
        return resp.status_code == 200

    def remove(self):
        resp = query_cartodb('DROP TABLE "{tablename}"'.format(
            tablename=self.tablename))
        assert resp.status_code == 200


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


class TableToCarto(Task):

    force = BooleanParameter(default=False)
    table = Parameter()
    outname = Parameter(default=None)

    def run(self):
        sql_to_cartodb_table(self.output().tablename, 'SELECT * FROM {table}'.format(
            table=self.table
        ))

    def output(self):
        if self.outname is None:
            self.outname = slug_column(self.table)
        target = CartoDBTarget(self.outname)
        if self.force:
            target.untouch()
        return target


# https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-camel-case
def camel_to_underscore(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


class TableTarget(Target):

    def __init__(self, task, columns):
        task_id = camel_to_underscore(task.task_id)
        task_id = task_id.replace('force=_false', '')
        task_id = task_id.replace('force=_true', '')
        tablename = re.sub(r'[^a-z0-9]+', '_', task_id).strip('_')
        schema = classpath(task)
        self.table = Table(tablename, metadata, *columns, schema=schema,
                           extend_existing=True)

    def exists(self):
        return self.table.exists()

    def create(self, **kwargs):
        return self.table.create(**kwargs)

    #def touch(self):
    #    with session_scope() as session:
    #        #table = TableDefinition(id=self.table.fullname, **self.table.info)
    #        #for key, col in self.table.columns.iteritems():
    #        #    import pdb
    #        #    pdb.set_trace()
    #        #    column = ColumnDefinition(id=key, **col.info)
    #        #    association = ColumnTable(column=column)
    #        #    table.columns.append(association)
    #        #session.add(table)

    def drop(self, checkfirst=True, **kwargs):
        return self.table.drop(checkfirst=checkfirst, **kwargs)

    def select(self):
        return self.table.select()

    def __str__(self):
        return '"{schema}".{table}'.format(schema=self.table.schema,
                                           table=self.table.name)


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def save_metadata(session, table):
    '''
    Persist the metadata for a new table or update existing metadata.
    '''
    table_id = '"' + table.schema + '".' + table.name
    table_def = session.query(TableDefinition).get(table_id)
    if not table_def:
        table_def = TableDefinition(id=table_id, **table.info)
    else:
        for key, val in table.info.iteritems():
            setattr(table_def, key, val)

    session.add(table_def)
    #for column_name, column in table_meta.columns.iteritems():
    #    column_meta = session.query(ColumnInfoMetadata).get(column_name)
    #    if not column_meta:
    #        column_meta = ColumnInfoMetadata(column_name=column_name, **column.info)
    #    else:
    #        column_meta.update(**column.info)
    #    column_table = ColumnTableInfoMetadata(table=table,
    #                                           column_name=column_name)
    #    column_table_meta = session.query(ColumnTableInfoMetadata).filter_by(
    #        column=column_meta, column_table=column_table).one()
    #    column_meta.columns.append(column_table_meta)
    #    session.add(column_meta)

    #session.add(table_meta)


class SessionTask(Task):
    '''
    A Task whose `runession` and `columns` methods should be overriden, and
    executes creating a single output table defined by its name, path, and
    defined columns.
    '''

    force = BooleanParameter(default=False)

    def columns(self):
        return NotImplementedError('Must implement columns method that returns '
                                   'an iterable sequence of columns for the '
                                   'table generated by this task.')


    def runsession(self, session):
        return NotImplementedError('Must implement runsession method that '
                                   'populates the table')

    def run(self):
        with session_scope() as session:
            self.output().create() # TODO in session
            self.runsession(session)
            save_metadata(session, self.output().table)

    def output(self):
        target = TableTarget(self, self.columns())
        if self.force:
            target.drop()
            self.force = False
        return target


#def test():
#    with session_scope() as session:
#        tabledef = TableDefinition(id='"foo.bar".table')
#        columndef = ColumnDefinition(id='"foo.bar".column')
#        association = ColumnTableDefinition(column=columndef)
#        tabledef.columns.append(association)
#        session.add(tabledef)
#
#        import pdb
#        pdb.set_trace()
