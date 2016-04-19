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
from sqlalchemy.dialects.postgresql import JSON

from tasks.meta import (OBSColumn, OBSTable, metadata, Geometry,
                        OBSColumnTable, OBSTag, current_session,
                        session_commit, session_rollback)


def get_logger(name):
    '''
    Obtain a logger outputing to stderr with specified name. Defaults to INFO
    log level.
    '''
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(message)s'))
    logger.addHandler(handler)
    return logger

LOGGER = get_logger(__name__)


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
    try:
        return subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as err:
        LOGGER.error(err.output)
        raise


def underscore_slugify(txt):
    return slugify(camel_to_underscore(re.sub(
        r'[^a-zA-Z0-9]+', '_', txt))).replace('-', '_')


def classpath(obj):
    '''
    Path to this task, suitable for the current OS.
    '''
    return '.'.join(obj.__module__.split('.')[1:])


def tablize(task):
    '''
    Generate a qualified tablename, properly quoted, for the passed object.
    '''
    return '"{schema}".{tablename}'.format(
        schema=classpath(task),
        tablename=underscore_slugify(task.task_id))


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


def sql_to_cartodb_table(outname, localname, json_column_names=None):
    '''
    Move the specified table to cartodb

    If json_column_names are specified, then those columns will be altered to
    JSON after the fact (they get smushed to TEXT at some point in the import
    process)
    '''
    json_column_names = json_column_names or []
    api_key = os.environ['CARTODB_API_KEY']
    private_outname = outname + '_private'
    schema = '.'.join(localname.split('.')[0:-1]).replace('"', '')
    tablename = localname.split('.')[-1]
    cmd = u'''
ogr2ogr --config CARTODB_API_KEY $CARTODB_API_KEY \
        -f CartoDB "CartoDB:observatory" \
        -overwrite \
        -nlt GEOMETRY \
        -nln "{private_outname}" \
        PG:dbname=$PGDATABASE' active_schema={schema}' '{tablename}'
    '''.format(private_outname=private_outname, tablename=tablename,
               schema=schema)
    print cmd
    shell(cmd)
    print 'copying via import api'
    resp = requests.post('{url}/api/v1/imports/?api_key={api_key}'.format(
        url=os.environ['CARTODB_URL'],
        api_key=api_key
    ), json={
        'table_name': outname,
        'table_copy': private_outname,
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

    resp = query_cartodb('DROP TABLE "{}"'.format(private_outname))
    assert resp.status_code == 200

    for colname in json_column_names:
        query = 'ALTER TABLE {outname} ALTER COLUMN {colname} ' \
                'SET DATA TYPE json USING {colname}::json'.format(
                    outname=outname, colname=colname
                )
        print query
        resp = query_cartodb(query)
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
        # get dataset id: GET https://observatory.cartodb.com/api/v1/tables/obs_column_table_3?api_key=bf40056ab6e223c07a7aa7731861a7bda1043241
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

    def __init__(self, schema, name, column, task):
        self.schema = schema
        self.name = name
        self._id = '"{schema}".{name}'.format(schema=schema, name=name)
        column.id = self._id
        #self._id = column.id
        self._task = task
        self._column = column

    def get(self, session):
        '''
        Return a copy of the underlying OBSColumn in the specified session.
        '''
        with session.no_autoflush:
            return session.query(OBSColumn).get(self._id)

    def update_or_create(self):
        session = current_session()
        in_session = session.identity_map.get((OBSColumn, (self._column.id, )))
        if in_session:
            if None in in_session.srcs:
                col2col = in_session.srcs.pop(None)
                in_session.srcs[col2col.source] = col2col
            if None in in_session.tgts:
                col2col = in_session.tgts.pop(None)
                in_session.tgts[col2col.target] = col2col

        self._column = session.merge(self._column)
        if self._column.targets:
            # fix missing sources in association_proxy... very weird
            # bug
            for target in self._column.tgts.keys():
                if None in target.srcs:
                    col2col = target.srcs.pop(None)
                    target.srcs[col2col.source] = col2col

    def exists(self):
        existing = self.get(current_session())
        new_version = float(self._column.version) or 0.0
        if existing:
            existing_version = float(existing.version)
            current_session().expunge(existing)
        else:
            existing_version = 0.0
        if existing and existing_version == new_version:
            return True
        elif existing and existing_version > new_version:
            raise Exception('Metadata version mismatch: running tasks with '
                            'older version than what is in DB')
        return False


class TagTarget(Target):
    '''
    '''

    def __init__(self, tag, task):
        self._id = tag.id
        self._tag = tag
        self._task = task

    def get(self, session):
        '''
        Return a copy of the underlying OBSColumn in the specified session.
        '''
        with session.no_autoflush:
            return session.query(OBSTag).get(self._id)

    def update_or_create(self):
        self._tag = current_session().merge(self._tag)

    def exists(self):
        session = current_session()
        existing = self.get(session)
        new_version = float(self._tag.version) or 0.0
        if existing:
            existing_version = float(existing.version)
            current_session().expunge(existing)
        else:
            existing_version = 0.0
        if existing and existing_version == new_version:
            return True
        elif existing and existing_version > new_version:
            raise Exception('Metadata version mismatch: running tasks with '
                            'older version than what is in DB')
        return False


class TableTarget(Target):

    def __init__(self, schema, name, obs_table, columns, task):
        '''
        columns: should be an ordereddict if you want to specify columns' order
        in the table
        '''
        self._id = '"{schema}".{name}'.format(schema=schema, name=name)
        self._id_noquote = '{schema}.{name}'.format(schema=schema, name=name)
        obs_table.id = self._id
        obs_table.tablename = 'obs_' + sha1(underscore_slugify(self._id)).hexdigest()
        self._schema = schema
        self._name = name
        self._obs_table = obs_table
        self._obs_dict = obs_table.__dict__.copy()
        self._columns = columns
        self._task = task
        if self._id_noquote in metadata.tables:
            self._table = metadata.tables[self._id_noquote]
        else:
            self._table = None

    @property
    def table(self):
        return self.get(current_session()).id

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


        existing = self.get(current_session())
        new_version = float(self._obs_table.version) or 0.0
        if existing:
            existing_version = float(existing.version)
            current_session().expunge(existing)
        else:
            existing_version = 0.0
        if existing and existing_version == new_version:
            return True
        elif existing and existing_version > new_version:
            raise Exception('Metadata version mismatch: running tasks with '
                            'older version than what is in DB')
        return False

    def get(self, session):
        '''
        Return a copy of the underlying OBSTable in the specified session.
        '''
        with session.no_autoflush:
            return session.query(OBSTable).get(self._id)

    def update_or_create(self):

        session = current_session()

        # create schema out-of-band as we cannot create a table after a schema
        # in a single session
        shell("psql -c 'CREATE SCHEMA IF NOT EXISTS \"{schema}\"'".format(
            schema=self._schema))

        # replace metadata table
        self._obs_table = session.merge(self._obs_table)
        obs_table = self._obs_table

        # create new local data table
        columns = []
        for colname, coltarget in self._columns.items():
            colname = colname.lower()
            col = coltarget.get(session)

            # Column info for sqlalchemy's internal metadata
            if col.type.lower() == 'geometry':
                coltype = Geometry

            # For enum type, pull keys from extra["categories"]
            elif col.type.lower().startswith('enum'):
                cats = col.extra['categories'].keys()
                coltype = types.Enum(*cats, name=col.id + '_enum')
            else:
                coltype = getattr(types, col.type.capitalize())
            columns.append(Column(colname, coltype))

            # Column info for bmd metadata
            coltable = session.query(OBSColumnTable).filter_by(
                column_id=col.id, table_id=obs_table.id).first()
            if coltable:
                coltable.colname = colname
            else:
                # catch the case where a column id has changed
                coltable = session.query(OBSColumnTable).filter_by(
                    table_id=obs_table.id, colname=colname).first()
                if coltable:
                    coltable.column = col
                else:
                    coltable = OBSColumnTable(colname=colname, table=obs_table,
                                              column=col)
            session.add(coltable)

        # replace local data table
        if obs_table.id in metadata.tables:
            metadata.tables[obs_table.id].drop()
        self._table = Table(self._name, metadata, *columns,
                            schema=self._schema, extend_existing=True)
        self._table.drop(checkfirst=True)
        self._table.create()


class ColumnsTask(Task):
    '''
    This will update-or-create columns defined in it when run
    '''

    def columns(self):
        '''
        '''
        raise NotImplementedError('Must return iterable of OBSColumns')

    def on_failure(self, ex):
        session_rollback(self, ex)
        super(ColumnsTask, self).on_failure(ex)

    def on_success(self):
        session_commit(self)

    def run(self):
        for _, coltarget in self.output().iteritems():
            coltarget.update_or_create()

    def version(self):
        return 0

    def output(self):
        output = OrderedDict({})
        session = current_session()
        already_in_session = [obj for obj in session]
        for col_key, col in self.columns().iteritems():
            if not col.version:
                col.version = self.version()
            output[col_key] = ColumnTarget(classpath(self), col.id or col_key, col, self)
        now_in_session = [obj for obj in session]
        for obj in now_in_session:
            if obj not in already_in_session:
                if obj in session:
                    session.expunge(obj)
        return output


class TagsTask(Task):
    '''
    This will update-or-create tags defined in it when run
    '''

    def tags(self):
        '''
        '''
        raise NotImplementedError('Must return iterable of OBSTags')

    def on_failure(self, ex):
        session_rollback(self, ex)
        super(TagsTask, self).on_failure(ex)

    def on_success(self):
        session_commit(self)

    def run(self):
        for _, tagtarget in self.output().iteritems():
            tagtarget.update_or_create()

    def version(self):
        return 0

    def output(self):
        output = {}
        for tag in self.tags():
            orig_id = tag.id
            tag.id = '"{}".{}'.format(classpath(self), orig_id)
            if not tag.version:
                tag.version = self.version()
            output[orig_id] = TagTarget(tag, self)
        return output


class TableToCarto(Task):

    force = BooleanParameter(default=False)
    table = Parameter()
    outname = Parameter(default=None)

    def run(self):
        json_colnames = []
        if self.table in metadata.tables:
            cols = metadata.tables[self.table].columns
            for colname, coldef in cols.items():
                coltype = coldef.type
                if isinstance(coltype, JSON):
                    json_colnames.append(colname)

        sql_to_cartodb_table(self.output().tablename, self.table, json_colnames)
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
    A Task whose `populate` and `columns` methods should be overriden, and
    executes creating a single output table defined by its name, path, and
    defined columns.
    '''

    def version(self):
        return 0

    def on_failure(self, ex):
        session_rollback(self, ex)
        super(TableTask, self).on_failure(ex)

    def on_success(self):
        session_commit(self)

    def columns(self):
        raise NotImplementedError('Must implement columns method that returns '
                                   'a dict of ColumnTargets')

    def populate(self):
        raise NotImplementedError('Must implement populate method that '
                                   'populates the table')

    def description(self):
        return None

    def timespan(self):
        raise NotImplementedError('Must define timespan for table')

    def bounds(self):
        raise NotImplementedError('Must define bounds for table')

    def run(self):
        self.output().update_or_create()
        self.populate()

    def complete(self):
        inputs = self.input()
        if isinstance(inputs, dict):
            for _, input_ in self.input().iteritems():
                if not input_.exists():
                    return False
        elif isinstance(inputs, list):
            for input_ in self.input():
                if not input_.exists():
                    return False
        elif isinstance(inputs, Target):
            if not inputs.exists():
                return False
        else:
            raise Exception('Can only work with inputs that are a dict, '
                            'list or Target')

        return super(TableTask, self).complete()

    def output(self):
        return TableTarget(classpath(self),
                           underscore_slugify(self.task_id),
                           OBSTable(description=self.description(),
                                    bounds=self.bounds(),
                                    version=self.version(),
                                    timespan=self.timespan()),
                           self.columns(), self)
