'''
Util functions for luigi bigmetadata tasks.
'''

import os
import subprocess
import logging
import sys
import time
import re
from slugify import slugify
from itertools import izip_longest

import requests

from luigi import Task, Parameter, LocalTarget, Target, BooleanParameter
from luigi.postgres import PostgresTarget

from sqlalchemy import Table, types, Column

from tasks.meta import (BMDColumn, BMDTable, metadata, Geometry,
                        BMDColumnTable, BMDTag, session_scope)


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


def underscore_slugify(txt):
    return slugify(camel_to_underscore(re.sub(
        r'[^a-zA-Z0-9]+', '_', txt))).replace('-', '_')

#def slug_column(column_name):
#    '''
#    Turn human-readable column name into a decent one for a SQL table
#    '''
#    translations = {
#        'population': 'pop',
#        'for_whom': '',
#        'u_s': 'us',
#        '_is_': '_',
#        'in_the_past_12_months': '',
#        'black_or_african_american': 'black',
#        'percentage_of': 'percent'
#    }
#    # TODO handle accents etc properly
#    column_name = re.sub(r'[^a-z0-9]+', '_', column_name.lower())
#    for before, after in translations.iteritems():
#        column_name = column_name.replace(before, after)
#    column_name = re.sub(r'[^a-z0-9]+', '_', column_name).strip('_')
#    return column_name


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
        return DefaultPostgresTarget(table=id_)

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


class ColumnTarget(Target):
    '''
    '''

    def __init__(self, schema, name, column):
        self.schema = schema
        self.name = name
        self._id = '"{schema}".{name}'.format(schema=schema, name=name)
        column.id = self._id
        #self._id = column.id
        self._column = column

    def get(self, session):
        '''
        Return a copy of the underlying BMDColumn in the specified session.
        '''
        return session.query(BMDColumn).get(self._id)

    def update_or_create(self, session):
        existing = self.get(session)
        if existing:
            for key, val in self._column.__dict__.iteritems():
                if not key.startswith('_'):
                    setattr(existing, key, val)
        else:
            session.add(self._column)

    def exists(self):
        with session_scope() as session:
            return self.get(session) is not None


class TagTarget(Target):
    '''
    '''

    def __init__(self, tag):
        self._id = tag.id
        self._tag = tag

    def get(self, session):
        '''
        Return a copy of the underlying BMDColumn in the specified session.
        '''
        return session.query(BMDTag).get(self._id)

    def update_or_create(self, session):
        existing = self.get(session)
        if existing:
            for key, val in self._tag.__dict__.iteritems():
                if not key.startswith('_'):
                    setattr(existing, key, val)
        else:
            session.add(self._tag)

    def exists(self):
        with session_scope() as session:
            return self.get(session) is not None


class TableTarget(Target):

    def __init__(self, schema, name, bmd_table, columns):
        '''
        columns: should be an ordereddict if you want to specify columns' order
        in the table
        '''
        self._id = '"{schema}".{name}'.format(schema=schema, name=name)
        self._id_noquote = '{schema}.{name}'.format(schema=schema, name=name)
        bmd_table.id = self._id
        bmd_table.tablename = underscore_slugify(self._id)
        self._schema = schema
        self._name = name
        self._bmd_table = bmd_table
        self._columns = columns
        if self._id_noquote in metadata.tables:
            self.table = metadata.tables[self._id_noquote]
        else:
            self.table = None
        with session_scope() as session:
            session.execute('CREATE SCHEMA IF NOT EXISTS "{schema}"'.format(
                schema=self._schema))
            session.flush()

    def exists(self):
        '''
        We always want to run this at least once, because we can always
        regenerate tabular data from scratch.
        '''
        with session_scope() as session:
            return self.get(session) is not None

    def get(self, session):
        '''
        Return a copy of the underlying BMDTable in the specified session.
        '''
        return session.query(BMDTable).get(self._id)

    def update_or_create(self, session):

        # replace metadata table
        bmd_table = self.get(session)
        if bmd_table:
            for key, val in self._bmd_table.__dict__.iteritems():
                if not key.startswith('_'):
                    setattr(bmd_table, key, val)
        else:
            session.add(self._bmd_table)
            bmd_table = self._bmd_table

        # create new local data table
        columns = []
        for colname, coltarget in self._columns.items():
            col = coltarget.get(session)

            # Column info for sqlalchemy's internal metadata
            if col.type.lower() == 'geometry':
                coltype = Geometry
            else:
                coltype = getattr(types, col.type)
            columns.append(Column(colname, coltype))

            # Column info for bmd metadata
            coltable = session.query(BMDColumnTable).filter_by(
                column_id=col.id, table_id=bmd_table.id).first()
            if coltable:
                coltable.colname = colname
            else:
                coltable = BMDColumnTable(colname=colname, table=bmd_table,
                                          column=col)
            session.add(coltable)

        # replace local data table
        if bmd_table.id in metadata.tables:
            metadata.tables[bmd_table.id].drop()
        self.table = Table(self._name, metadata, *columns,
                           schema=self._schema, extend_existing=True)
        self.table.drop(checkfirst=True)
        self.table.create()


class ColumnsTask(Task):
    '''
    This will update-or-create columns defined in it when run
    '''

    def columns(self):
        '''
        '''
        raise NotImplementedError('Must return iterable of BMDColumns')

    def run(self):
        with session_scope() as session:
            for _, coltarget in self.output().iteritems():
                coltarget.update_or_create(session)

    def output(self):
        output = {}
        for col in self.columns():
            orig_id = col.id
            #col.id = '"{}".{}'.format(classpath(self), orig_id)
            output[orig_id] = ColumnTarget(classpath(self), orig_id, col)
        return output


class TagsTask(Task):
    '''
    This will update-or-create tags defined in it when run
    '''

    def tags(self):
        '''
        '''
        raise NotImplementedError('Must return iterable of BMDTags')

    def run(self):
        with session_scope() as session:
            for _, tagtarget in self.output().iteritems():
                tagtarget.update_or_create(session)

    def output(self):
        output = {}
        for tag in self.tags():
            orig_id = tag.id
            tag.id = '"{}".{}'.format(classpath(self), orig_id)
            output[orig_id] = TagTarget(tag)
        return output


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


#def save_metadata(session, table):
#    '''
#    Persist the metadata for a new table or update existing metadata.
#    '''
#    table_id = '"' + table.schema + '".' + table.name
#    table_def = session.query(TableDefinition).get(table_id)
#    if not table_def:
#        table_def = TableDefinition(id=table_id, **table.info)
#    else:
#        for key, val in table.info.iteritems():
#            setattr(table_def, key, val)
#
#    session.add(table_def)
#    #for column_name, column in table_meta.columns.iteritems():
#    #    column_meta = session.query(ColumnInfoMetadata).get(column_name)
#    #    if not column_meta:
#    #        column_meta = ColumnInfoMetadata(column_name=column_name, **column.info)
#    #    else:
#    #        column_meta.update(**column.info)
#    #    column_table = ColumnTableInfoMetadata(table=table,
#    #                                           column_name=column_name)
#    #    column_table_meta = session.query(ColumnTableInfoMetadata).filter_by(
#    #        column=column_meta, column_table=column_table).one()
#    #    column_meta.columns.append(column_table_meta)
#    #    session.add(column_meta)
#
#    #session.add(table_meta)

class TableTask(Task):
    '''
    A Task whose `runsession` and `columns` methods should be overriden, and
    executes creating a single output table defined by its name, path, and
    defined columns.
    '''

    def columns(self):
        return NotImplementedError('Must implement columns method that returns '
                                   'a dict of ColumnTargets')

    def runsession(self, session):
        return NotImplementedError('Must implement runsession method that '
                                   'populates the table')

    @property
    def description(self):
        return None

    @property
    def timespan(self):
        return None

    @property
    def table(self):
        '''
        Obtain metadata table for insertion via sqlalchemy or direct postgres
        '''
        return self.output().table

    def run(self):
        with session_scope() as session:
            self.output().update_or_create(session)
            self.runsession(session)

    def output(self):
        return TableTarget(classpath(self),
                           underscore_slugify(self.task_id),
                           BMDTable(description=self.description,
                                    timespan=self.timespan),
                           self.columns())


#def update_or_create(session, obj, predicate):
#    try:
#        with session.no_autoflush:
#            existing_obj = session.query(type(obj)).filter(*predicate(obj)).one()
#        for key, val in obj.__dict__.iteritems():
#            if not key.startswith('__'):
#                setattr(existing_obj, key, val)
#        return existing_obj
#    except NoResultFound:
#        session.add(obj)
#        return obj
