'''
Util functions for luigi bigmetadata tasks.
'''

from collections import OrderedDict

import json
import os
import subprocess
import logging
import sys
import time
import re
from hashlib import sha1
from itertools import izip_longest
from datetime import date
from urllib import quote_plus

from slugify import slugify
import requests

from luigi import (Task, Parameter, LocalTarget, Target, BooleanParameter,
                   ListParameter, DateParameter)
from luigi.s3 import S3Target

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


def shell(cmd):
    '''
    Run a shell command, uses :py:func:`subprocess.check_output(cmd,
    shell=True)` under the hood.

    Returns the ``STDOUT`` output, and raises an error if there is a
    none-zero exit code.
    '''
    try:
        return subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as err:
        LOGGER.error(err.output)
        raise


def underscore_slugify(txt):
    '''
    Given a string, converts from camelcase to underscore style, and then
    slugifies.

    >>> underscore_slugify('FooBarBaz')
    'foo_bar_baz'

    >>> underscore_slugify('Foo, bar, baz')
    'foo_bar_baz'

    >>> underscore_slugify('Foo_Bar_Baz')
    'foo_bar_baz'

    >>> underscore_slugify('Foo:barBaz')
    'foo_bar_baz'
    '''
    return slugify(camel_to_underscore(re.sub(
        r'[^a-zA-Z0-9]+', '_', txt))).replace('-', '_')


def classpath(obj):
    '''
    Returns the path to this object's class, relevant to current working dir.
    Excludes the first element of the path.  If there is only one element,
    returns ``tmp``.

    >>> classpath(object)
    'tmp'
    >>> from tasks.util import ColumnsTask
    >>> classpath(ColumnsTask())
    'util'
    >>> from tasks.es.ine import FiveYearPopulation
    >>> classpath(FiveYearPopulation())
    'es.ine'
    '''
    classpath_ = '.'.join(obj.__module__.split('.')[1:])
    return classpath_ if classpath_ else 'tmp'


def query_cartodb(query):
    '''
    Convenience function to query CARTO's SQL API with an arbitrary SQL string.
    The account connected via ``.env`` is queried.

    Returns the raw ``Response`` object.  Will not raise an exception in case
    of non-200 response.

    :param query: The query to execute on CARTO.
    '''
    #carto_url = 'https://{}/api/v2/sql'.format(os.environ['CARTODB_DOMAIN'])
    carto_url = os.environ['CARTODB_URL'] + '/api/v2/sql'
    resp = requests.post(carto_url, data={
        'api_key': os.environ['CARTODB_API_KEY'],
        'q': query
    })
    #assert resp.status_code == 200
    #if resp.status_code != 200:
    #    import pdb
    #    pdb.set_trace()
    #    raise Exception(u'Non-200 response ({}) from carto: {}'.format(
    #        resp.status_code, resp.text))
    return resp


def upload_via_ogr2ogr(outname, localname, schema):
    api_key = os.environ['CARTODB_API_KEY']
    cmd = u'''
ogr2ogr --config CARTODB_API_KEY $CARTODB_API_KEY \
        -f CartoDB "CartoDB:observatory" \
        -overwrite \
        -nlt GEOMETRY \
        -nln "{private_outname}" \
        PG:dbname=$PGDATABASE' active_schema={schema}' '{tablename}'
    '''.format(private_outname=outname, tablename=localname,
               schema=schema)
    print cmd
    shell(cmd)


def import_api(request, json_column_names=None):
    '''
    Run CARTO's `import API <https://carto.com/docs/carto-engine/import-api/importing-geospatial-data/>`_
    The account connected via ``.env`` will be the target.

    Although the import API is asynchronous, this function will block until
    the request is complete, and raise an exception if it fails.

    :param request: A ``dict`` that will be the body of the request to the
                    import api.
    :param json_column_names:  Optional iterable of column names that will
                               be converted to ``JSON`` type after the fact.
                               Otherwise those columns would be ``Text``.
    '''
    api_key = os.environ['CARTODB_API_KEY']
    json_column_names = json_column_names or []
    resp = requests.post('{url}/api/v1/imports/?api_key={api_key}'.format(
        url=os.environ['CARTODB_URL'],
        api_key=api_key
    ), json=request)
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

    # if failing below, try reloading https://observatory.cartodb.com/dashboard/datasets
    assert resp.json()['table_name'] == request['table_name'] # the copy should not have a
                                                             # mutilated name (like '_1', '_2' etc)

    for colname in json_column_names:
        query = 'ALTER TABLE {outname} ALTER COLUMN {colname} ' \
                'SET DATA TYPE json USING NULLIF({colname}, '')::json'.format(
                    outname=resp.json()['table_name'], colname=colname
                )
        print query
        resp = query_cartodb(query)
        assert resp.status_code == 200


def sql_to_cartodb_table(outname, localname, json_column_names=None,
                         schema='observatory'):
    '''
    Move a table to CARTO using the `import API <https://carto.com/docs/carto-engine/import-api/importing-geospatial-data/>`_

    :param outname: The destination name of the table.
    :param localname: The local name of the table, exclusive of schema.
    :param json_column_names:  Optional iterable of column names that will
                               be converted to ``JSON`` type after the fact.
                               Otherwise those columns would be ``Text``.
    :param schema: Optional schema for the local table.  Defaults to
                   ``observatory``.
    '''
    private_outname = outname + '_private'
    upload_via_ogr2ogr(private_outname, localname, schema)

    # populate the_geom_webmercator
    # if you try to use the import API to copy a table with the_geom populated
    # but not the_geom_webmercator, we get an error for unpopulated column
    resp = query_cartodb(
        'UPDATE {tablename} '
        'SET the_geom_webmercator = CDB_TransformToWebmercator(the_geom) '.format(
            tablename=private_outname
        )
    )
    assert resp.status_code == 200

    print 'copying via import api'
    import_api({
        'table_name': outname,
        'table_copy': private_outname,
        'create_vis': False,
        'type_guessing': False,
        'privacy': 'public'
    }, json_column_names=json_column_names)
    resp = query_cartodb('DROP TABLE "{}" CASCADE'.format(private_outname))
    assert resp.status_code == 200


class PostgresTarget(Target):
    '''
    PostgresTarget which by default uses command-line specified login.
    '''

    def __init__(self, schema, tablename):
        self._schema = schema
        self._tablename = tablename

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

    def exists(self):
        session = current_session()
        resp = session.execute('SELECT COUNT(*) FROM information_schema.tables '
                               "WHERE table_schema ILIKE '{schema}'  "
                               "  AND table_name ILIKE '{tablename}' ".format(
                                   schema=self._schema,
                                   tablename=self._tablename))
        if int(resp.fetchone()[0]) == 0:
            return False
        resp = session.execute(
            'SELECT row_number() over () FROM "{schema}".{tablename} LIMIT 1'.format(
                schema=self._schema, tablename=self._tablename))
        return resp.fetchone() is not None


class CartoDBTarget(Target):
    '''
    Target which is a CartoDB table
    '''

    def __init__(self, tablename):
        resp = requests.get('{url}/dashboard/datasets'.format(
             url=os.environ['CARTODB_URL']
        ), cookies={
            '_cartodb_session': os.environ['CARTODB_SESSION']
        }).content

        self.tablename = tablename

    def __str__(self):
        return self.tablename

    def exists(self):
        resp = query_cartodb('SELECT * FROM "{tablename}" LIMIT 0'.format(
            tablename=self.tablename))
        return resp.status_code == 200

    def remove(self):
        api_key = os.environ['CARTODB_API_KEY']
        url = os.environ['CARTODB_URL']

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
        query_cartodb('DROP TABLE IF EXISTS {tablename}'.format(tablename=self.tablename))
        assert not self.exists()


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
        self._id = '.'.join([schema, name])
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
        self._column = current_session().merge(self._column)

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
        obs_table.tablename = 'obs_' + sha1(underscore_slugify(self._id)).hexdigest()
        self._schema = schema
        self._name = name
        self._obs_table = obs_table
        self._obs_dict = obs_table.__dict__.copy()
        self._columns = columns
        self._task = task
        if obs_table.tablename in metadata.tables:
            self._table = metadata.tables[obs_table.tablename]
        else:
            self._table = None

    @property
    def table(self):
        return 'observatory.' + self._obs_table.tablename

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
        new_version = float(self._obs_table.version) or 0.0
        if existing:
            existing_version = float(existing.version)
            session.expunge(existing)
        else:
            existing_version = 0.0
        if existing and existing_version == new_version:
            resp = session.execute(
                'SELECT COUNT(*) FROM information_schema.tables '
                "WHERE table_schema = '{schema}'  "
                "  AND table_name = '{tablename}' ".format(
                    schema='observatory',
                    tablename=self._obs_table.tablename))
            if int(resp.fetchone()[0]) == 0:
                return False
            resp = session.execute(
                'SELECT row_number() over () '
                'FROM "{schema}".{tablename} LIMIT 1 '.format(
                    schema='observatory',
                    tablename=self._obs_table.tablename))
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

        obs_table = self._obs_table
        # replace local data table
        if obs_table.id in metadata.tables:
            metadata.tables[obs_table.id].drop()
        self._table = Table(self._obs_table.tablename, metadata, *columns,
                            extend_existing=True, schema='observatory')
        self._table.drop(checkfirst=True)
        self._table.create()

    def update_or_create_metadata(self):
        session = current_session()
        select = []
        for i, colname_coltarget in enumerate(self._columns.iteritems()):
            colname, coltarget = colname_coltarget
            if coltarget._column.type.lower() == 'numeric':
                select.append('sum(case when {colname} is not null then 1 else 0 end) col{i}_notnull, '
                              'max({colname}) col{i}_max, '
                              'min({colname}) col{i}_min, '
                              'avg({colname}) col{i}_avg, '
                              'percentile_cont(0.5) within group (order by {colname}) col{i}_median, '
                              'mode() within group (order by {colname}) col{i}_mode, '
                              'stddev_pop({colname}) col{i}_stddev'.format(
                                  i=i, colname=colname.lower()))
        if select:
            stmt = 'SELECT COUNT(*) cnt, {select} FROM {output}'.format(
                select=', '.join(select), output=self.table)
            resp = session.execute(stmt)
            colinfo = dict(zip(resp.keys(), resp.fetchone()))
        else:
            colinfo = {}

        # replace metadata table
        self._obs_table = session.merge(self._obs_table)
        obs_table = self._obs_table

        for i, colname_coltarget in enumerate(self._columns.iteritems()):
            colname, coltarget = colname_coltarget
            colname = colname.lower()
            col = coltarget.get(session)

            # Column info for obs metadata
            coltable = session.query(OBSColumnTable).filter_by(
                column_id=col.id, table_id=obs_table.id).first()
            if coltable:
                coltable_existed = True
                coltable.colname = colname
            else:
                # catch the case where a column id has changed
                coltable = session.query(OBSColumnTable).filter_by(
                    table_id=obs_table.id, colname=colname).first()
                if coltable:
                    coltable_existed = True
                    coltable.column = col
                else:
                    coltable_existed = False
                    coltable = OBSColumnTable(colname=colname, table=obs_table,
                                              column=col)
            # include analysis
            if col.type.lower() == 'numeric':
                # do not include linkage for any column that is 100% null
                stats = {
                    'count': colinfo.get('cnt'),
                    'notnull': colinfo.get('col%s_notnull' % i),
                    'max': colinfo.get('col%s_max' % i),
                    'min': colinfo.get('col%s_min' % i),
                    'avg': colinfo.get('col%s_avg' % i),
                    'median': colinfo.get('col%s_median' % i),
                    'mode': colinfo.get('col%s_mode' % i),
                    'stddev': colinfo.get('col%s_stddev' % i),
                }
                if stats['notnull'] == 0:
                    if coltable_existed:
                        session.delete(coltable)
                    elif coltable in session:
                        session.expunge(coltable)
                    continue
                for k in stats.keys():
                    if stats[k] is not None:
                        stats[k] = float(stats[k])
                coltable.extra = {
                    'stats': stats
                }
            session.add(coltable)


class ColumnsTask(Task):
    '''
    The ColumnsTask provides a structure for generating metadata. The only
    required method is :meth:`~.ColumnsTask.columns`.
    The keys may be used as human-readable column names in tables based off
    this metadata, although that is not always the case. If the id of the
    :class:`OBSColumn <tasks.meta.OBSColumn>` is left blank, the dict's key will be used to
    generate it (qualified by the module).

    Also, conventionally there will be a requires method that brings in our
    standard tags: :class:`SectionTags <tasks.tags.SectionTags>`,
    :class:`SubsectionTags <tasks.tags.SubsectionTags>`, and
    :class:`UnitTags <tasks.tags.UnitTags>`.
    This is an example of defining several tasks as prerequisites: the outputs
    of those tasks will be accessible via ``self.input()[<key>]`` in other
    methods.

    This will update-or-create columns defined in it when run.  Whether a
    column is updated or not depends on whether it exists in the database with
    the same :meth:`~.ColumnsTask.version` number.
    '''

    def columns(self):
        '''
        This method must be overriden in subclasses.  It must return a
        :py:class:`collections.OrderedDict` whose values are all instances of
        :class:`OBSColumn <tasks.meta.OBSColumn>` and whose keys are all strings.
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
        '''
        Returns a number that will be linked to the all
        :py:attr:`tasks.meta.OBSColumn.version` objects resulting from this
        task.  If this is identical to what's already in the database, the
        :class:`OBSColumn <tasks.meta.OBSColumn>` will not be replaced.
        '''
        return 0

    def output(self):
        if self.deps() and not all([d.complete() for d in self.deps()]):
            raise Exception('Must run prerequisites first')
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

    def complete(self):
        '''
        Custom complete method that attempts to check if output exists, as is
        default, but in case of failure allows attempt to run dependencies (a
        missing dependency could result in exception on `output`).
        '''
        if self.deps() and not all([d.complete() for d in self.deps()]):
            return False
        else:
            return super(ColumnsTask, self).complete()


class TagsTask(Task):
    '''
    This will update-or-create :class:`OBSTag <tasks.meta.OBSTag>` objects
    int the database when run.

    The :meth:`~.TagsTask.tags` method must be overwritten.

    :meth:`~TagsTask.version` is used to control updates to the database.
    '''

    def tags(self):
        '''
        This method must be overwritten in subclasses.

        The return value must be an iterable of instances of
        :class:`OBSTag <tasks.meta.OBSTag>`.
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
        if self.deps() and not all([d.complete() for d in self.deps()]):
            raise Exception('Must run prerequisites first')
        output = {}
        for tag in self.tags():
            orig_id = tag.id
            tag.id = '.'.join([classpath(self), orig_id])
            if not tag.version:
                tag.version = self.version()
            output[orig_id] = TagTarget(tag, self)
        return output

    def complete(self):
        '''
        Custom complete method that attempts to check if output exists, as is
        default, but in case of failure allows attempt to run dependencies (a
        missing dependency could result in exception on `output`).
        '''
        if self.deps() and not all([d.complete() for d in self.deps()]):
            return False
        else:
            return super(TagsTask, self).complete()


class TableToCartoViaImportAPI(Task):
    '''
    This task wraps :func:`~.util.sql_to_cartodb_table` to upload a table to
    a CARTO account specified in the ``.env`` file quickly using the Import
    API.

    :param table: The name of the table to upload, exclusive of schema.
    :param schema: Optional. The schema of the table to upload, defaults to
                   ``observatory``.
    :param force: Optional boolean.  Defaults to ``False``.  If ``True``, a
                  table of the same name existing remotely will be overwritten.
    '''

    force = BooleanParameter(default=False, significant=False)
    schema = Parameter(default='observatory')
    table = Parameter()

    def run(self):
        url = os.environ['CARTODB_URL']
        api_key = os.environ['CARTODB_API_KEY']
        try:
            os.makedirs(os.path.join('tmp', classpath(self)))
        except OSError:
            pass
        tmp_file_path = os.path.join('tmp', classpath(self), self.table + '.csv')
        shell(r'''psql -c '\copy {schema}.{tablename} TO '"'"{tmp_file_path}"'"'
              WITH CSV HEADER' '''.format(
                  schema=self.schema,
                  tablename=self.table,
                  tmp_file_path=tmp_file_path,
              ))
        curl_resp = shell(
            'curl -s -F privacy=public -F type_guessing=false '
            '  -F file=@{tmp_file_path} "{url}/api/v1/imports/?api_key={api_key}"'.format(
                tmp_file_path=tmp_file_path,
                url=url,
                api_key=api_key
            ))
        import_id = json.loads(curl_resp)["item_queue_id"]
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

        # if failing below, try reloading https://observatory.cartodb.com/dashboard/datasets
        assert resp.json()['table_name'] == self.table # the copy should not have a
                                                       # mutilated name (like '_1', '_2' etc)

        # fix broken column data types -- alter everything that's not character
        # varying back to it
        try:
            session = current_session()
            resp = session.execute('SELECT column_name, data_type '
                                   'FROM information_schema.columns '
                                   "WHERE table_schema='{schema}' "
                                   "AND table_name='{tablename}' ".format(
                                       schema=self.schema,
                                       tablename=self.table)).fetchall()
            alter = ', '.join(["ALTER COLUMN {colname} SET DATA TYPE {data_type} USING NULLIF({colname}, '')::{data_type}".format(
                colname=colname, data_type=data_type
            ) for colname, data_type in resp if data_type.lower() not in ('character varying', 'text', 'user-defined')])
            if alter:
                resp = query_cartodb(
                    'ALTER TABLE {tablename} {alter}'.format(
                        tablename=self.table,
                        alter=alter)
                )
                if resp.status_code != 200:
                    raise Exception('could not alter columns for "{tablename}":'
                                    '{err}'.format(tablename=self.table,
                                                   err=resp.text))
        except Exception as err:
            # in case of error, delete the uploaded but not-yet-properly typed
            # table
            self.output().remove()
            raise err

    def output(self):
        target = CartoDBTarget(self.table)
        if self.force:
            target.remove()
            self.force = False
        return target


class TableToCarto(Task):

    force = BooleanParameter(default=False, significant=False)
    schema = Parameter(default='observatory')
    table = Parameter()
    outname = Parameter(default=None)

    def run(self):
        json_colnames = []
        table = '.'.join([self.schema, self.table])
        if table in metadata.tables:
            cols = metadata.tables[table].columns
            for colname, coldef in cols.items():
                coltype = coldef.type
                if isinstance(coltype, JSON):
                    json_colnames.append(colname)

        sql_to_cartodb_table(self.output().tablename, self.table, json_colnames,
                             schema=self.schema)
        self.force = False

    def output(self):
        if self.schema != 'observatory':
            table = '.'.join([self.schema, self.table])
        else:
            table = self.table
        if self.outname is None:
            self.outname = underscore_slugify(table)
        target = CartoDBTarget(self.outname)
        if self.force and target.exists():
            target.remove()
            self.force = False
        return target


# https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-camel-case
def camel_to_underscore(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()



class DownloadUnzipTask(Task):
    '''
    Download a zip file to location {output}.zip and unzip it to the folder
    {output}.  Subclasses only need to define a
    :meth:`~.util.DownloadUnzipTask.download` method.
    '''

    def download(self):
        '''
        Subclasses must override this.  A good starting point is:

        .. code:: python

            shell('wget -O {output}.zip {url}'.format(
              output=self.output().path,
              url=<URL>
            ))
        '''
        raise NotImplementedError('DownloadUnzipTask must define download()')

    def run(self):
        os.makedirs(self.output().path)
        self.download()
        shell('unzip -d {output} {output}.zip'.format(output=self.output().path))

    def output(self):
        '''
        The default output location is in the ``tmp`` folder, in a subfolder
        derived from the subclass's :meth:`~.util.classpath` and its
        :attr:`~.task_id`.
        '''
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))


class TempTableTask(Task):
    '''
    A Task that generates a table that will not be referred to in metadata.

    This is useful for intermediate processing steps that can benefit from the
    session guarantees of the ETL, as well as automatic table naming.

    :param force: Optional Boolean, ``False`` by default.  If ``True``, will
                  overwrite output table even if it exists already.
    '''

    force = BooleanParameter(default=False, significant=False)

    def on_failure(self, ex):
        session_rollback(self, ex)
        super(TempTableTask, self).on_failure(ex)

    def on_success(self):
        session_commit(self)

    def run(self):
        '''
        Must be overriden by subclass.  Should create and populate a table
        named from ``self.output().table``

        If this completes without exceptions, the :func:`~.util.current_session
        will be committed; if there is an exception, it will be rolled back.
        '''
        raise Exception('Must override `run`')

    def output(self):
        '''
        By default, returns a :class:`~.util.TableTarget` whose associated
        table lives in a special-purpose schema in Postgres derived using
        :func:`~.util.classpath`.
        '''
        shell("psql -c 'CREATE SCHEMA IF NOT EXISTS \"{schema}\"'".format(
            schema=classpath(self)))
        target = PostgresTarget(classpath(self), self.task_id)
        if self.force and not getattr(self, 'wiped', False):
            if target.exists():
                shell("psql -c 'DROP TABLE \"{schema}\".{tablename}'".format(
                    schema=classpath(self), tablename=self.task_id))
            self.wiped = True
        return target


class Shp2TempTableTask(TempTableTask):
    '''
    A task that loads :meth:`~.util.Shp2TempTableTask.input_shp()` into a
    temporary Postgres table.  That method must be overriden.
    '''

    def input_shp(self):
        '''
        This method must be implemented by subclasses.  Should return either
        a path to a single shapefile, or an iterable of paths to shapefiles.
        In that case, the first in the list will determine the schema.
        '''
        raise NotImplementedError("Must specify `input_shp` method")

    def run(self):
        if isinstance(self.input_shp(), basestring):
            shps = [self.input_shp()]
        else:
            shps = self.input_shp()
        schema = self.output().schema
        tablename = self.output().tablename
        operation = '-overwrite -lco OVERWRITE=yes -lco SCHEMA={schema} -lco PRECISION=no '.format(
            schema=schema)
        for shp in shps:
            cmd = 'PG_USE_COPY=yes PGCLIENTENCODING=latin1 ' \
                    'ogr2ogr -f PostgreSQL PG:"dbname=$PGDATABASE ' \
                    'active_schema={schema}" -t_srs "EPSG:4326" ' \
                    '-nlt MultiPolygon -nln {table} ' \
                    '{operation} \'{input}\' '.format(
                        schema=schema,
                        table=tablename,
                        input=shp,
                        operation=operation
                    )
            shell(cmd)
            operation = '-append '.format(schema=schema)


class CSV2TempTableTask(TempTableTask):
    '''
    A task that loads :meth:`~.util.CSV2TempTableTask.input_csv` into a
    temporary Postgres table.  That method must be overriden.

    Optionally, :meth:`~util.CSV2TempTableTask.coldef` can be overriden.

    Under the hood, uses postgres's ``COPY``.

    :param delimiter: Delimiter separating fields in the CSV.  Defaults to
                      ``,``.  Must be one character.
    :param has_header: Boolean as to whether first row has column names.
                       Defaults to ``True``.
    :param force: Boolean as to whether the task should be run even if the
                  temporary output already exists.
    '''

    delimiter = Parameter(default=',', significant=False)
    has_header = BooleanParameter(default=True, significant=False)

    def input_csv(self):
        '''
        Must be overriden with a method that returns either a path to a CSV
        or an iterable of paths to CSVs.
        '''
        raise NotImplementedError("Must specify `input_csv` method")

    def coldef(self):
        '''
        Override this function to customize the column definitions in the table.
        Expected is an iterable of two-tuples.

        If not overriden:

        * All column types will be ``Text``
        * If :attr:`~.util.CSV2TempTableTask.has_header` is ``True``, then
          column names will come from the headers.
        * If :attr:`~.util.CSV2TempTableTask.has_header` is ``False``, then
          column names will be the postgres defaults.
        '''
        if isinstance(self.input_csv(), basestring):
            csv = self.input_csv()
        else:
            raise NotImplementedError("Cannot automatically determine colnames "
                                      "if several input CSVs.")
        header_row = shell('head -n 1 {csv}'.format(csv=csv))
        return [(h, 'Text') for h in header_row.split(self.delimiter)]

    def run(self):
        if isinstance(self.input_csv(), basestring):
            csvs = [self.input_csv()]
        else:
            csvs = self.input_csv()

        session = current_session()
        session.execute('CREATE TABLE {output} ({coldef})'.format(
            output=self.output().table,
            coldef=', '.join(['{} {}'.format(*c) for c in self.coldef()])
        ))
        session.commit()
        options = ['''DELIMITER '"'{}'"' '''.format(self.delimiter)]
        if self.has_header:
            options.append('CSV HEADER')
        try:
            for csv in csvs:
                shell(r'''psql -c '\copy {table} FROM '"'{input}'"' {options}' '''.format(
                    input=csv,
                    table=self.output().table,
                    options=' '.join(options)
                ))
        except:
            session.execute('DROP TABLE IF EXISTS {output}'.format(
                output=self.output().table))
            raise


class LoadPostgresFromURL(TempTableTask):

    def load_from_url(self, url):
        '''
        Load psql at a URL into the database.

        Ignores tablespaces assigned in the SQL.
        '''
        shell('curl {url} | gunzip -c | grep -v default_tablespace | psql'.format(
            url=url))
        self.mark_done()

    def mark_done(self):
        session = current_session()
        session.execute('DROP TABLE IF EXISTS {table}'.format(
            table=self.output().table))
        session.execute('CREATE TABLE {table} AS SELECT now() creation_time'.format(
            table=self.output().table))


class TableTask(Task):
    '''
    This task creates a single output table defined by its name, path, and
    defined columns.

    :meth:`~.TableTask.timespan`, :meth:`~.TableTask.columns`, and
    :meth:`~.TableTask.populate` must be overwritten in subclasses.

    When run, this task will automatically create the table with a schema
    corresponding to the defined columns, with a unique name.  It will also
    generate all relevant metadata for the table, and link it to the columns.
    '''

    def version(self):
        '''
        Must return a version control number, which is useful for forcing a
        re-run/overwrite without having to track down and delete output
        artifacts.
        '''
        return 0

    def on_failure(self, ex):
        session_rollback(self, ex)
        super(TableTask, self).on_failure(ex)

    def on_success(self):
        session_commit(self)
        super(TableTask, self).on_success()

    def columns(self):
        '''
        Must be overriden by subclasses.  Columns returned from this function
        determine the schema of the resulting :class:`~tasks.util.TableTarget`.

        The return for this function should be constructed by selecting the
        desired columns from :class:`~tasks.util.ColumnTask`s, specified as
        inputs in :meth:`~.util.TableTask.requires()`

        Must return a :py:class:`~collections.OrderedDict` of
        (colname, :class:`~tasks.util.ColumnTarget`) pairs.

        '''
        raise NotImplementedError('Must implement columns method that returns '
                                   'a dict of ColumnTargets')

    def populate(self):
        '''
        This method must populate (most often via ``INSERT``) the output table.

        For example: 
        '''
        raise NotImplementedError('Must implement populate method that '
                                   'populates the table')

    def description(self):
        '''
        Optional description for the :class:`~tasks.util.OBSTable`.  Not
        currently used anywhere.
        '''
        return None

    def timespan(self):
        '''
        Must return an arbitrary string timespan (for example, ``2014``, or
        ``2012Q4``) that identifies the date range or point-in-time for this
        table.  Must be implemented by subclass.
        '''
        raise NotImplementedError('Must define timespan for table')

    def the_geom(self, output):
        geometry_columns = [(colname, coltarget) for colname, coltarget in
                            self.columns().iteritems() if coltarget._column.type.lower() == 'geometry']
        if len(geometry_columns) == 0:
            return None
        elif len(geometry_columns) == 1:
            session = current_session()
            return session.execute(
                'SELECT ST_AsText( '
                '  ST_Multi( '
                '    ST_CollectionExtract( '
                '      ST_MakeValid( '
                '        ST_SnapToGrid( '
                '          ST_Buffer( '
                '            ST_Union( '
                '              ST_MakeValid( '
                '                ST_Simplify( '
                '                  ST_SnapToGrid({geom_colname}, 0.3) '
                '                , 0) '
                '              ) '
                '            ) '
                '          , 0.3, 2) '
                '        , 0.3) '
                '      ) '
                '    , 3) '
                '  ) '
                ') the_geom '
                'FROM {output}'.format(
                    geom_colname=geometry_columns[0][0],
                    output=output.table
                )).fetchone()['the_geom']
        else:
            raise Exception('Having more than one geometry column in one table '
                            'could lead to problematic behavior ')

    def run(self):
        output = self.output()
        output.update_or_create_table()
        self.populate()
        output.update_or_create_metadata()
        self.create_indexes(output)
        output._obs_table.the_geom = self.the_geom(output)

    def create_indexes(self, output):
        session = current_session()
        tablename = output.table
        for colname, coltarget in self._columns.iteritems():
            col = coltarget._column
            index_type = col.index_type
            if index_type:
                index_name = '{}_{}_idx'.format(tablename.split('.')[-1], colname)
                session.execute('CREATE INDEX {index_name} ON {table} '
                                'USING {index_type} ({colname})'.format(
                                    index_type=index_type,
                                    index_name=index_name,
                                    table=tablename, colname=colname))

    def output(self):
        if self.deps() and not all([d.complete() for d in self.deps()]):
            raise Exception('Must run prerequisites first')
        if not hasattr(self, '_columns'):
            self._columns = self.columns()

        return TableTarget(classpath(self),
                           underscore_slugify(self.task_id),
                           OBSTable(description=self.description(),
                                    version=self.version(),
                                    timespan=self.timespan()),
                           self._columns, self)

    def complete(self):
        if self.deps() and not all([d.complete() for d in self.deps()]):
            return False
        else:
            return super(TableTask, self).complete()


class RenameTables(Task):
    '''
    A one-time use task that renames all ID-instantiated data tables to their
    tablename.
    '''

    def run(self):
        session = current_session()
        for table in session.query(OBSTable):
            table_id = table.id
            tablename = table.tablename
            schema = '.'.join(table.id.split('.')[0:-1]).strip('"')
            table = table.id.split('.')[-1]
            resp = session.execute('SELECT COUNT(*) FROM information_schema.tables '
                                   "WHERE table_schema ILIKE '{schema}'  "
                                   "  AND table_name ILIKE '{table}' ".format(
                                       schema=schema,
                                       table=table))
            if int(resp.fetchone()[0]) > 0:
                resp = session.execute('SELECT COUNT(*) FROM information_schema.tables '
                                       "WHERE table_schema ILIKE 'observatory'  "
                                       "  AND table_name ILIKE '{table}' ".format(
                                           table=tablename))
                # new table already exists -- just drop it
                if int(resp.fetchone()[0]) > 0:
                    cmd = 'DROP TABLE {table_id} CASCADE'.format(table_id=table_id)
                    session.execute(cmd)
                else:
                    cmd = 'ALTER TABLE {old} RENAME TO {new}'.format(
                        old=table_id, new=tablename)
                    print cmd
                    session.execute(cmd)
                    cmd = 'ALTER TABLE "{schema}".{new} SET SCHEMA observatory'.format(
                        new=tablename, schema=schema)
                    print cmd
                    session.execute(cmd)
            else:
                resp = session.execute('SELECT COUNT(*) FROM information_schema.tables '
                                       "WHERE table_schema ILIKE 'public'  "
                                       "  AND table_name ILIKE '{table}' ".format(
                                           table=tablename))
                if int(resp.fetchone()[0]) > 0:
                    cmd = 'ALTER TABLE public.{new} SET SCHEMA observatory'.format(
                        new=tablename)
                    print cmd
                    session.execute(cmd)

        session.commit()
        self._complete = True

    def complete(self):
        return hasattr(self, '_complete')


class CreateGeomIndexes(Task):
    '''
    Make sure every table has a `the_geom` index.
    '''

    def run(self):
        session = current_session()
        resp = session.execute('SELECT DISTINCT geom_colname, geom_tablename '
                               'FROM observatory.obs_meta ')
        for colname, tablename in resp:
            index_name = '{}_{}_idx'.format(tablename, colname)
            resp = session.execute("SELECT to_regclass('observatory.{}')".format(
                index_name))
            print index_name
            if not resp.fetchone()[0]:
                session.execute('CREATE INDEX {index_name} ON observatory.{tablename} '
                                'USING GIST ({colname})'.format(
                                    index_name=index_name,
                                    tablename=tablename,
                                    colname=colname))
        session.commit()
        self._complete = True

    def complete(self):
        return getattr(self, '_complete', False)


class DropOrphanTables(Task):
    '''
    Remove tables that aren't documented anywhere in metadata.
    '''

    def run(self):
        session = current_session()
        resp = session.execute('''
SELECT table_name
FROM information_schema.tables
WHERE table_name LIKE 'obs_%'
  AND table_schema = 'observatory'
  AND table_name NOT IN (SELECT tablename FROM observatory.obs_table)
  AND LENGTH(table_name) = 44
''')
        for row in resp:
            tablename = row[0]
            cnt = session.execute(
                'select count(*) from observatory.{}'.format(tablename)).fetchone()[0]
            if cnt > 0:
                raise Exception('not automatically dropping {}, it has {} rows'.format(
                    tablename, cnt))
            else:
                session.execute('drop table observatory.{}'.format(tablename))


class Carto2TempTableTask(TempTableTask):
    '''
    Import a table from a CARTO account into a temporary table.

    :param subdomain: Optional. The subdomain the table resides in. Defaults
                       to ``observatory``.
    :param table: The name of the table to be imported.
    '''

    subdomain = Parameter(default='observatory')
    table = Parameter()

    TYPE_MAP = {
        'string': 'TEXT',
        'number': 'NUMERIC',
        'geometry': 'GEOMETRY',
    }

    @property
    def _url(self):
        return 'https://{subdomain}.carto.com/api/v2/sql'.format(
            subdomain=self.subdomain
        )

    def _query(self, **params):
        return requests.get(self._url, params=params)

    def _create_table(self):
        resp = self._query(
            q='SELECT * FROM {table} LIMIT 0'.format(table=self.table)
        )
        coltypes = dict([
            (k, self.TYPE_MAP[v['type']]) for k, v in resp.json()['fields'].iteritems()
        ])
        resp = self._query(
            q='SELECT * FROM {table} LIMIT 0'.format(table=self.table),
            format='csv'
        )
        colnames = resp.text.strip().split(',')
        columns = ', '.join(['{colname} {type}'.format(
            colname=c,
            type=coltypes[c]
        ) for c in colnames])
        stmt = 'CREATE TABLE {table} ({columns})'.format(table=self.output().table,
                                                         columns=columns)
        shell("psql -c '{stmt}'".format(stmt=stmt))

    def _load_rows(self):
        url = self._url + '?q={q}&format={format}'.format(
            q=quote_plus('SELECT * FROM {table}'.format(table=self.table)),
            format='csv'
        )
        shell(r"curl '{url}' | "
              r"psql -c '\copy {table} FROM STDIN WITH CSV HEADER'".format(
                  table=self.output().table,
                  url=url))

    def run(self):
        self._create_table()
        self._load_rows()
        shell("psql -c 'CREATE INDEX ON {table} USING gist (the_geom)'".format(
            table=self.output().table,
        ))


class CustomTable(TempTableTask):

    measures = ListParameter()
    boundary = Parameter()

    def run(self):

        session = current_session()
        meta = '''
        SELECT numer_colname, numer_type, numer_geomref_colname, numer_tablename,
               geom_colname, geom_type, geom_geomref_colname, geom_tablename
        FROM observatory.obs_meta
        WHERE numer_id IN ('{measures}')
          AND geom_id = '{boundary}'
        '''.format(measures="', '".join(self.measures),
                   boundary=self.boundary)

        colnames = set()
        tables = set()
        where = set()
        coldefs = set()

        for row in session.execute(meta):
            numer_colname, numer_type, numer_geomref_colname, numer_tablename, \
                    geom_colname, geom_type, geom_geomref_colname, geom_tablename = row
            colnames.add('{}.{}'.format(numer_tablename, numer_colname))
            colnames.add('{}.{}'.format(geom_tablename, geom_colname))
            coldefs.add('{} {}'.format(numer_colname, numer_type))
            coldefs.add('{} {}'.format(geom_colname, geom_type))
            tables.add('observatory."{}"'.format(numer_tablename))
            tables.add('observatory."{}"'.format(geom_tablename))
            where.add('{}.{} = {}.{}'.format(numer_tablename, numer_geomref_colname,
                                             geom_tablename, geom_geomref_colname))

        create = '''
        CREATE TABLE {output} AS
        SELECT {colnames}
        FROM {tables}
        WHERE {where}
        '''.format(
            output=self.output().table,
            colnames=', '.join(colnames),
            tables=', '.join(tables),
            where=' AND '.join(where),
        )
        session.execute(create)


class ArchiveIPython(Task):

    timestamp = DateParameter(default=date.today())

    def run(self):
        self.output().makedirs()
        shell('tar -zcvf {output} --exclude=*.pdf --exclude=*.xml '
              '--exclude=*.gz --exclude=*.zip --exclude=*/tmp/* '
              'tmp/ipython'.format(
                  output=self.output().path))

    def output(self):
        return LocalTarget(os.path.join(
            'tmp', classpath(self), underscore_slugify(self.task_id) + '.gz'))


class BackupIPython(Task):

    timestamp = DateParameter(default=date.today())

    def requires(self):
        return ArchiveIPython(timestamp=self.timestamp)

    def run(self):
        shell('aws s3 cp {input} {output}'.format(
            input=self.input().path,
            output=self.output().path
        ))

    def output(self):
        path = 's3://cartodb-observatory-data/ipython/{}'.format(
            self.input().path.split(os.path.sep)[-1])
        print path
        return S3Target(path)
