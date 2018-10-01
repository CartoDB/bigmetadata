import os
import requests

from luigi import Target, LocalTarget
from hashlib import sha1

from tasks.util import (query_cartodb, underscore_slugify, OBSERVATORY_PREFIX, OBSERVATORY_SCHEMA)
from tasks.meta import (OBSColumn, OBSTable, metadata, Geometry, Point,
                        Linestring, OBSColumnTable, OBSTag, current_session)
from sqlalchemy import Table, types, Column

from lib.logger import get_logger

LOGGER = get_logger(__name__)


class PostgresTarget(Target):
    '''
    PostgresTarget which by default uses command-line specified login.
    '''

    def __init__(self, schema, tablename, non_empty=True, where="1 = 1"):
        self._schema = schema
        self._tablename = tablename
        self._non_empty = non_empty
        self._where = where

    @property
    def table(self):
        return '"{schema}".{tablename}'.format(schema=self._schema,
                                               tablename=self._tablename)

    @property
    def tablename(self):
        return self._tablename

    @property
    def schema(self):
        return self._schema

    @property
    def qualified_tablename(self):
        return '"{}"."{}"'.format(self.schema, self.tablename)

    def _existenceness(self):
        '''
        Returns 0 if the table does not exist, 1 if it exists but has no
        rows (is empty), and 2 if it exists and has one or more rows.
        '''
        session = current_session()
        sql = '''
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema ILIKE '{schema}'
                  AND table_name ILIKE '{tablename}'
              '''.format(
            schema=self._schema,
            tablename=self._tablename)
        LOGGER.debug('Existenceness SQL: {}'.format(sql))
        resp = session.execute(sql)
        if int(resp.fetchone()[0]) == 0:
            return 0
        resp = session.execute(
            'SELECT row_number() over () FROM "{schema}"."{tablename}" WHERE {where} LIMIT 1'.format(
                schema=self._schema, tablename=self._tablename,
                where=self._where))
        if resp.fetchone() is None:
            return 1
        else:
            return 2

    def empty(self):
        '''
        Returns True if the table exists but has no rows in it.
        '''
        return self._existenceness() == 1

    def exists(self):
        '''
        Returns True if the table exists and has at least one row in it.
        '''
        if self._non_empty:
            return self._existenceness() == 2
        else:
            return self._existenceness() >= 1

    def exists_or_empty(self):
        '''
        Returns True if the table exists, even if it is empty.
        '''
        return self._existenceness() >= 1


class CartoDBTarget(Target):
    '''
    Target which is a CartoDB table
    '''

    def __init__(self, tablename, carto_url=None, api_key=None):
        self.tablename = tablename
        self.carto_url = carto_url
        self.api_key = api_key

    def __str__(self):
        return self.tablename

    def exists(self):
        resp = query_cartodb(
            'SELECT row_number() over () FROM "{tablename}" LIMIT 1'.format(
                tablename=self.tablename),
            api_key=self.api_key,
            carto_url=self.carto_url)
        if resp.status_code != 200:
            return False
        return resp.json()['total_rows'] > 0

    def remove(self, carto_url=None, api_key=None):
        api_key = api_key or os.environ['CARTODB_API_KEY']

        try:
            while True:
                resp = requests.get('{url}/api/v1/tables/{tablename}?api_key={api_key}'.format(
                    url=carto_url,
                    tablename=self.tablename,
                    api_key=api_key
                ))
                viz_id = resp.json()['id']
                # delete dataset by id DELETE
                # https://observatory.cartodb.com/api/v1/viz/ed483a0b-7842-4610-9f6c-8591273b8e5c
                try:
                    requests.delete('{url}/api/v1/viz/{viz_id}?api_key={api_key}'.format(
                        url=carto_url,
                        viz_id=viz_id,
                        api_key=api_key
                    ), timeout=1)
                except requests.Timeout:
                    pass
        except ValueError:
            pass
        query_cartodb('DROP TABLE IF EXISTS {tablename}'.format(tablename=self.tablename))
        assert not self.exists()


class ColumnTarget(Target):
    '''
    '''

    def __init__(self, column, task):
        self._id = column.id
        self._task = task
        self._column = column

    def get(self, session):
        '''
        Return a copy of the underlying OBSColumn in the specified session.
        '''
        with session.no_autoflush:
            return session.query(OBSColumn).get(self._id)

    def update_or_create(self):
        self._column = current_session().merge(self._column)

    def exists(self):
        existing = self.get(current_session())
        new_version = float(self._column.version or 0.0)
        if existing:
            existing_version = float(existing.version or 0.0)
            current_session().expunge(existing)
        else:
            existing_version = 0.0
        if existing and existing_version == new_version:
            return True
        elif existing and existing_version > new_version:
            raise Exception('Metadata version mismatch: cannot run task {task} '
                            '(id "{id}") '
                            'with ETL version ({etl}) older than what is in '
                            'DB ({db})'.format(task=self._task.task_id,
                                               id=self._id,
                                               etl=new_version,
                                               db=existing_version))
        return False


class TagTarget(Target):
    '''
    '''

    def __init__(self, tag, task):
        self._id = tag.id
        self._tag = tag
        self._task = task

    _tag_cache = {}

    def get(self, session):
        '''
        Return a copy of the underlying OBSColumn in the specified session.
        '''
        if not self._tag_cache.get(self._id, None):
            with session.no_autoflush:
                self._tag_cache[self._id] = session.query(OBSTag).get(self._id)
        return self._tag_cache[self._id]

    def update_or_create(self):
        with current_session().no_autoflush:
            self._tag = current_session().merge(self._tag)

    def exists(self):
        session = current_session()
        existing = self.get(session)
        new_version = self._tag.version or 0.0
        if existing:
            if existing in session:
                session.expunge(existing)
            existing_version = existing.version or 0.0
            if float(existing_version) == float(new_version):
                return True
            if existing_version > new_version:
                raise Exception('Metadata version mismatch: cannot run task {task} '
                                '(id "{id}") '
                                'with ETL version ({etl}) older than what is in '
                                'DB ({db})'.format(task=self._task.task_id,
                                                   id=self._id,
                                                   etl=new_version,
                                                   db=existing_version))
        return False


class TableTarget(Target):

    def __init__(self, schema, name, obs_table, columns, task):
        '''
        columns: should be an ordereddict if you want to specify columns' order
        in the table
        '''
        self._id = '.'.join([schema, name])
        obs_table.id = self._id
        obs_table.tablename = '{prefix}{name}'.format(prefix=OBSERVATORY_PREFIX, name=sha1(
            underscore_slugify(self._id).encode('utf-8')).hexdigest())
        self.table = '{schema}.{table}'.format(schema=OBSERVATORY_SCHEMA, table=obs_table.tablename)
        self.qualified_tablename = '"{schema}"."{table}"'.format(schema=OBSERVATORY_SCHEMA, table=obs_table.tablename)
        self.obs_table = obs_table
        self._tablename = obs_table.tablename
        self._schema = schema
        self._name = name
        self._obs_dict = obs_table.__dict__.copy()
        self._columns = columns
        self._task = task
        if obs_table.tablename in metadata.tables:
            self._table = metadata.tables[obs_table.tablename]
        else:
            self._table = None

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
        session = current_session()
        existing = self.get(session)
        new_version = float(self.obs_table.version or 0.0)
        if existing:
            existing_version = float(existing.version or 0.0)
            if existing in session:
                session.expunge(existing)
        else:
            existing_version = 0.0
        if existing and existing_version == new_version:
            resp = session.execute(
                'SELECT COUNT(*) FROM information_schema.tables '
                "WHERE table_schema = '{schema}'  "
                "  AND table_name = '{tablename}' ".format(
                    schema='observatory',
                    tablename=existing.tablename))
            if int(resp.fetchone()[0]) == 0:
                return False
            resp = session.execute(
                'SELECT row_number() over () '
                'FROM "{schema}".{tablename} LIMIT 1 '.format(
                    schema='observatory',
                    tablename=existing.tablename))
            return resp.fetchone() is not None
        elif existing and existing_version > new_version:
            raise Exception('Metadata version mismatch: cannot run task {task} '
                            '(id "{id}") '
                            'with ETL version ({etl}) older than what is in '
                            'DB ({db})'.format(task=self._task.task_id,
                                               id=self._id,
                                               etl=new_version,
                                               db=existing_version))
        return False

    def get(self, session):
        '''
        Return a copy of the underlying OBSTable in the specified session.
        '''
        with session.no_autoflush:
            return session.query(OBSTable).get(self._id)

    def update_or_create_table(self):
        session = current_session()

        # create new local data table
        columns = []
        for colname, coltarget in list(self._columns.items()):
            colname = colname.lower()
            col = coltarget.get(session)

            # Column info for sqlalchemy's internal metadata
            if col.type.lower() == 'geometry':
                coltype = Geometry
            elif col.type.lower().startswith('geometry(point'):
                coltype = Point
            elif col.type.lower().startswith('geometry(linestring'):
                coltype = Linestring

            # For enum type, pull keys from extra["categories"]
            elif col.type.lower().startswith('enum'):
                cats = list(col.extra['categories'].keys())
                coltype = types.Enum(*cats, name=col.id + '_enum')
            else:
                coltype = getattr(types, col.type.capitalize())
            columns.append(Column(colname, coltype))

        obs_table = self.get(session) or self.obs_table
        # replace local data table
        if obs_table.id in metadata.tables:
            metadata.tables[obs_table.id].drop()
        self._table = Table(obs_table.tablename, metadata, *columns,
                            extend_existing=True, schema='observatory')
        session.commit()
        self._table.drop(checkfirst=True)
        self._table.create()

    def update_or_create_metadata(self, _testmode=False):
        session = current_session()

        # replace metadata table
        self.obs_table = session.merge(self.obs_table)
        obs_table = self.obs_table

        for i, colname_coltarget in enumerate(self._columns.items()):
            colname, coltarget = colname_coltarget
            colname = colname.lower()
            col = coltarget.get(session)

            if _testmode:
                coltable = OBSColumnTable(colname=colname, table=obs_table,
                                          column=col)
            else:
                # Column info for obs metadata
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


class RepoTarget(LocalTarget):
    def __init__(self, schema, tablename, repo_dir, resource_id, version, filename):
        self.format = None
        self.is_tmp = False
        self.schema = schema
        self.tablename = tablename
        self.repo_dir = repo_dir
        self.resource_id = resource_id
        self.version = version
        self.filename = filename

    @property
    def path(self):
        path = self._get_path()
        if path and os.path.isfile(path):
            return path
        else:
            return self._build_path()

    def _build_path(self):
        return os.path.join(self.repo_dir, self.resource_id, str(self.version), self.filename)

    def _get_path(self):
        path = None
        query = '''
                SELECT path FROM "{schema}".{table}
                WHERE id = '{resource_id}'
                  AND version = {version}
                '''.format(schema=self.schema,
                           table=self.tablename,
                           resource_id=self.resource_id,
                           version=self.version)
        try:
            result = current_session().execute(query).fetchone()
            if result:
                path = result[0]
        except:
            path = None

        return path

    def exists(self):
        path = self._get_path()
        return path and os.path.isfile(path)


class ConstraintExistsTarget(Target):
    def __init__(self, schema, table, constraint):
        self.schema = schema
        self.table = table
        self.constraint = constraint
        self.session = current_session()

    def exists(self):
        sql = "SELECT 1 FROM information_schema.constraint_column_usage " \
              "WHERE table_schema = '{schema}' " \
              "  AND table_name = '{table}' " \
              "  AND constraint_name = '{constraint}'"
        check = sql.format(schema=self.schema,
                           table=self.table,
                           constraint=self.constraint)
        return len(self.session.execute(check).fetchall()) > 0
