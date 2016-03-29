'''
Util functions for luigi bigmetadata tasks.
'''

from collections import OrderedDict

import os
import subprocess
import logging
import sys
import time
import re
from hashlib import sha1
from itertools import izip_longest

from slugify import slugify
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


def classpath(obj):
    '''
    Path to this task, suitable for the current OS.
    '''
    return '.'.join(obj.__module__.split('.')[1:])


def query_cartodb(query):
    #carto_url = 'https://{}/api/v2/sql'.format(os.environ['CARTODB_DOMAIN'])
    carto_url = os.environ['CARTODB_URL'] + '/api/v2/sql'
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
    api_key = os.environ['CARTODB_API_KEY']
    query = query.replace("'", '\'"\'"\'')
    private_tablename = tablename + '_private'
    cmd = u'''
ogr2ogr --config CARTODB_API_KEY $CARTODB_API_KEY \
        -f CartoDB "CartoDB:observatory" \
        -overwrite \
        -nlt GEOMETRY \
        -nln "{private_tablename}" \
        PG:"dbname=$PGDATABASE" -sql '{sql}'
    '''.format(private_tablename=private_tablename, sql=query)
    print cmd
    shell(cmd)
    print 'copying via import api'
    resp = requests.post('{url}/api/v1/imports/?api_key={api_key}'.format(
        url=os.environ['CARTODB_URL'],
        api_key=api_key
    ), json={
        'table_name': tablename,
        'table_copy': private_tablename,
        'create_vis': False,
        'type_guessing': False,
        'privacy': 'public'
    })
    assert resp.status_code == 200
    import_id = resp.json()["item_queue_id"]
    while True:
        resp = requests.get('{url}/api/v1/imports/{import_id}?api_key={api_key}'.format(
            url=os.environ['CARTODB_URL'],
            import_id=import_id,
            api_key=api_key
        ))
        if resp.json()['state'] == 'complete':
            break
        elif resp.json()['state'] == 'failure':
            raise Exception('Import failed: {}'.format(resp.json()))
        print resp.json()['state']
        time.sleep(1)

    resp = query_cartodb('DROP TABLE "{}"'.format(private_tablename))
    assert resp.status_code == 200


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
        api_key = os.environ['CARTODB_API_KEY']
        # get dataset id: GET https://observatory.cartodb.com/api/v1/tables/bmd_column_table_3?api_key=bf40056ab6e223c07a7aa7731861a7bda1043241
        try:
            while True:
                resp = requests.get('{url}/api/v1/tables/{tablename}?api_key={api_key}'.format(
                    url=os.environ['CARTODB_URL'],
                    tablename=self.tablename,
                    api_key=api_key
                ))
                viz_id = resp.json()['id']
                # delete dataset by id DELETE https://observatory.cartodb.com/api/v1/viz/ed483a0b-7842-4610-9f6c-8591273b8e5c?api_key=bf40056ab6e223c07a7aa7731861a7bda1043241
                try:
                    requests.delete('{url}/api/v1/viz/{viz_id}?api_key={api_key}'.format(
                        url=os.environ['CARTODB_URL'],
                        viz_id=viz_id,
                        api_key=api_key
                    ), timeout=1)
                except requests.Timeout:
                    pass
        except ValueError:
            pass


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
        with session.no_autoflush:
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
        with session.no_autoflush:
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
        bmd_table.tablename = 'bmd_' + sha1(underscore_slugify(self._id)).hexdigest()
        self._schema = schema
        self._name = name
        self._bmd_table = bmd_table
        self._bmd_dict = bmd_table.__dict__.copy()
        self._columns = columns
        if self._id_noquote in metadata.tables:
            self.table = metadata.tables[self._id_noquote]
        else:
            self.table = None
        with session_scope() as session:
            session.execute('CREATE SCHEMA IF NOT EXISTS "{schema}"'.format(
                schema=self._schema))
            session.flush()

    def sync(self):
        '''
        Whether this data should be synced to carto. Defaults to True.
        '''
        return True

    def exists(self):
        '''
        We always want to run this at least once, because we can always
        regenerate tabular data from scratch.
        '''
        with session_scope() as session:
            with session.no_autoflush:
                if self.get(session) is None:
                    return False
                old_dict = self.get(session).__dict__.copy()
                new_dict = self._bmd_dict.copy()
                old_dict.pop('_sa_instance_state')
                new_dict.pop('_sa_instance_state')
                for key, val in old_dict.iteritems():
                    if key == 'id':
                        continue
                    if val is None:
                        new_dict[key] = val
                    if key not in new_dict:
                        new_dict[key] = None
                for key, val in new_dict.items():
                    if isinstance(val, list):
                        new_dict.pop(key)
                return new_dict == old_dict

        #return self._id_noquote in metadata.tables

    def get(self, session):
        '''
        Return a copy of the underlying BMDTable in the specified session.
        '''
        with session.no_autoflush:
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
        output = OrderedDict({})
        for col_key, col in self.columns().iteritems():
            output[col_key] = ColumnTarget(classpath(self), col.id, col)
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
        self.force = False

    def output(self):
        if self.outname is None:
            self.outname = underscore_slugify(self.table)
        target = CartoDBTarget(self.outname)
        if self.force and target.exists():
            target.remove()
            self.force = False
        return target


# https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-camel-case
def camel_to_underscore(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


class TableTask(Task):
    '''
    A Task whose `runsession` and `columns` methods should be overriden, and
    executes creating a single output table defined by its name, path, and
    defined columns.
    '''

    def __init__(self, *args, **kwargs):
        super(TableTask, self).__init__(*args, **kwargs)
        # Make sure everything is defined
        self.columns()
        self.timespan()
        self.bounds()

    def columns(self):
        return NotImplementedError('Must implement columns method that returns '
                                   'a dict of ColumnTargets')

    def runsession(self, session):
        return NotImplementedError('Must implement runsession method that '
                                   'populates the table')

    def description(self):
        return None

    def timespan(self):
        return NotImplementedError('Must define timespan for table')

    def bounds(self):
        return NotImplementedError('Must define bounds for table')

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
                           BMDTable(description=self.description(),
                                    bounds=self.bounds(),
                                    timespan=self.timespan()),
                           self.columns())
