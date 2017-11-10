import os
import re
import json
import time
import requests
import subprocess
import importlib
import inspect

from urllib.parse import quote_plus
from collections import OrderedDict
from datetime import date

from luigi import (Task, Parameter, LocalTarget, BoolParameter,
                   ListParameter, DateParameter, WrapperTask, Event)
from luigi.contrib.s3 import S3Target

from sqlalchemy.dialects.postgresql import JSON

from lib.logger import get_logger

from tasks.meta import (OBSColumn, OBSTable, metadata, current_session,
                        session_commit, session_rollback)
from tasks.targets import (ColumnTarget, TagTarget, CartoDBTarget, PostgresTarget, TableTarget)
from tasks.util import (classpath, query_cartodb, sql_to_cartodb_table, underscore_slugify, shell,
                        create_temp_schema, unqualified_task_id, generate_tile_summary)

LOGGER = get_logger(__name__)


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
        for _, coltarget in self.output().items():
            coltarget.update_or_create()

    def version(self):
        '''
        Returns a number that will be linked to the all
        :py:attr:`tasks.meta.OBSColumn.version` objects resulting from this
        task.  If this is identical to what's already in the database, the
        :class:`OBSColumn <tasks.meta.OBSColumn>` will not be replaced.
        '''
        return 0

    def prefix(self):
        return classpath(self)

    def output(self):
        session = current_session()

        # Return columns from database if the task is already finished
        if hasattr(self, '_colids') and self.complete():
            return OrderedDict([
                (colkey, ColumnTarget(session.query(OBSColumn).get(cid), self))
                for colkey, cid in self.colids.items()
            ])

        # Otherwise, run `columns` (slow!) to generate output
        already_in_session = [obj for obj in session]
        output = OrderedDict()

        input_ = self.input()
        for col_key, col in self.columns().items():
            if not isinstance(col, OBSColumn):
                raise RuntimeError(
                    'Values in `.columns()` must be of type OBSColumn, but '
                    '"{col}" is type {type}'.format(col=col_key, type=type(col)))
            if not col.version:
                col.version = self.version()
            col.id = '.'.join([self.prefix(), col.id or col_key])
            tags = self.tags(input_, col_key, col)
            if isinstance(tags, TagTarget):
                col.tags.append(tags)
            else:
                col.tags.extend(tags)

            output[col_key] = ColumnTarget(col, self)
        now_in_session = [obj for obj in session]
        for obj in now_in_session:
            if obj not in already_in_session:
                if obj in session:
                    session.expunge(obj)
        return output

    @property
    def colids(self):
        '''
        Return colids for the output columns, this can be cached
        '''
        if not hasattr(self, '_colids'):
            self._colids = OrderedDict([
                (colkey, ct._id) for colkey, ct in self.output().items()
            ])
        return self._colids

    def complete(self):
        '''
        Custom complete method that attempts to check if output exists, as is
        default, but in case of failure allows attempt to run dependencies (a
        missing dependency could result in exception on `output`).
        '''
        deps = self.deps()
        if deps and not all([d.complete() for d in deps]):
            return False
        else:
            # bulk check that all columns exist at proper version
            cnt = current_session().execute(
                '''
                SELECT COUNT(*)
                FROM observatory.obs_column
                WHERE id IN ('{ids}') AND version = '{version}'
                '''.format(
                    ids="', '".join(list(self.colids.values())),
                    version=self.version()
                )).fetchone()[0]
            return cnt == len(list(self.colids.values()))

    def tags(self, input_, col_key, col):
        '''
        Replace with an iterable of :class:`OBSColumn <tasks.meta.OBSColumn>`
        that should be applied to each column

        :param input_: A saved version of this class's :meth:`input <luigi.Task.input>`
        :param col_key: The key of the column this will be applied to.
        :param column: The :class:`OBSColumn <tasks.meta.OBSColumn>` these tags
                       will be applied to.
        '''
        return []


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
        for _, tagtarget in self.output().items():
            tagtarget.update_or_create()

    def version(self):
        return 0

    def output(self):
        # if self.deps() and not all([d.complete() for d in self.deps()]):
        #    raise Exception('Must run prerequisites first')
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
        deps = self.deps()
        if deps and not all([d.complete() for d in deps]):
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

    force = BoolParameter(default=False, significant=False)
    schema = Parameter(default='observatory')
    username = Parameter(default=None, significant=False)
    api_key = Parameter(default=None, significant=False)
    outname = Parameter(default=None, significant=False)
    table = Parameter()
    columns = ListParameter(default=[])

    def run(self):
        carto_url = 'https://{}.carto.com'.format(self.username) if self.username else os.environ['CARTODB_URL']
        api_key = self.api_key if self.api_key else os.environ['CARTODB_API_KEY']
        try:
            os.makedirs(os.path.join('tmp', classpath(self)))
        except OSError:
            pass
        outname = self.outname or self.table
        tmp_file_path = os.path.join('tmp', classpath(self), outname + '.csv')
        if not self.columns:
            shell(r'''psql -c '\copy "{schema}".{tablename} TO '"'"{tmp_file_path}"'"'
                  WITH CSV HEADER' '''.format(
                      schema=self.schema,
                      tablename=self.table,
                      tmp_file_path=tmp_file_path,
                  ))
        else:
            shell(r'''psql -c '\copy (SELECT {columns} FROM "{schema}".{tablename}) TO '"'"{tmp_file_path}"'"'
                  WITH CSV HEADER' '''.format(
                      schema=self.schema,
                      tablename=self.table,
                      tmp_file_path=tmp_file_path,
                      columns=', '.join(self.columns),
                  ))
        curl_resp = shell(
            'curl -s -F privacy=public -F type_guessing=false '
            '  -F file=@{tmp_file_path} "{url}/api/v1/imports/?api_key={api_key}"'.format(
                tmp_file_path=tmp_file_path,
                url=carto_url,
                api_key=api_key
            ))
        try:
            import_id = json.loads(curl_resp)["item_queue_id"]
        except ValueError:
            raise Exception(curl_resp)
        while True:
            resp = requests.get('{url}/api/v1/imports/{import_id}?api_key={api_key}'.format(
                url=carto_url,
                import_id=import_id,
                api_key=api_key
            ))
            if resp.json()['state'] == 'complete':
                LOGGER.info("Waiting for import %s for %s", import_id, outname)
                break
            elif resp.json()['state'] == 'failure':
                raise Exception('Import failed: {}'.format(resp.json()))

            print(resp.json()['state'])
            time.sleep(1)

        # If CARTO still renames our table to _1, just force alter it
        if resp.json()['table_name'] != outname:
            query_cartodb('ALTER TABLE {oldname} RENAME TO {newname}'.format(
                oldname=resp.json()['table_name'],
                newname=outname,
                carto_url=carto_url,
                api_key=api_key,
            ))
            assert resp.status_code == 200

        # fix broken column data types -- alter everything that's not character
        # varying back to it
        try:
            session = current_session()
            resp = session.execute(
                '''
                SELECT att.attname,
                       pg_catalog.format_type(atttypid, NULL) AS display_type,
                       att.attndims
                FROM pg_attribute att
                  JOIN pg_class tbl ON tbl.oid = att.attrelid
                  JOIN pg_namespace ns ON tbl.relnamespace = ns.oid
                WHERE tbl.relname = '{tablename}'
                  AND pg_catalog.format_type(atttypid, NULL) NOT IN
                      ('character varying', 'text', 'user-defined', 'geometry')
                  AND att.attname IN (SELECT column_name from information_schema.columns
                                      WHERE table_schema='{schema}'
                                        AND table_name='{tablename}')
                  AND ns.nspname = '{schema}';
                '''.format(schema=self.schema,
                           tablename=self.table.lower())).fetchall()
            alter = ', '.join([
                " ALTER COLUMN {colname} SET DATA TYPE {data_type} "
                " USING NULLIF({colname}, '')::{data_type}".format(
                    colname=colname, data_type=data_type
                ) for colname, data_type, _ in resp])
            if alter:
                alter_stmt = 'ALTER TABLE {tablename} {alter}'.format(
                    tablename=outname,
                    alter=alter)
                LOGGER.info(alter_stmt)
                resp = query_cartodb(alter_stmt, api_key=api_key, carto_url=carto_url)
                if resp.status_code != 200:
                    raise Exception('could not alter columns for "{tablename}":'
                                    '{err}'.format(tablename=outname,
                                                   err=resp.text))
        except Exception as err:
            # in case of error, delete the uploaded but not-yet-properly typed
            # table
            self.output().remove(carto_url=carto_url, api_key=api_key)
            raise err

    def output(self):
        carto_url = 'https://{}.carto.com'.format(self.username) if self.username else os.environ['CARTODB_URL']
        api_key = self.api_key if self.api_key else os.environ['CARTODB_API_KEY']
        target = CartoDBTarget(self.outname or self.table, api_key=api_key, carto_url=carto_url)
        if self.force:
            target.remove(carto_url=carto_url, api_key=api_key)
            self.force = False
        return target


class TableToCarto(Task):

    force = BoolParameter(default=False, significant=False)
    schema = Parameter(default='observatory')
    table = Parameter()
    outname = Parameter(default=None)

    def run(self):
        json_colnames = []
        table = '.'.join([self.schema, self.table])
        if table in metadata.tables:
            cols = metadata.tables[table].columns
            for colname, coldef in list(cols.items()):
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


class DownloadUnzipTask(Task):
    '''
    Download a zip file to location {output}.zip and unzip it to the folder
    {output}.  Subclasses only need to define a
    :meth:`~.tasks.DownloadUnzipTask.download` method.
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
        try:
            self.download()
            shell('unzip -d {output} {output}.zip'.format(output=self.output().path))
        except:
            os.rmdir(self.output().path)
            raise

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

    force = BoolParameter(default=False, significant=False)

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
        By default, returns a :class:`~.targets.TableTarget` whose associated
        table lives in a special-purpose schema in Postgres derived using
        :func:`~.util.classpath`.
        '''
        return PostgresTarget(classpath(self), unqualified_task_id(self.task_id))


@TempTableTask.event_handler(Event.START)
def clear_temp_table(task):
    create_temp_schema(task)
    target = task.output()
    if task.force or target.empty():
        session = current_session()
        session.execute('DROP TABLE IF EXISTS "{schema}".{tablename}'.format(
            schema=classpath(task), tablename=unqualified_task_id(task.task_id)))
        session.flush()


class GdbFeatureClass2TempTableTask(TempTableTask):
    '''
    A task that extracts one vector shape layer from a geodatabase to a
    TempTableTask.
    '''

    feature_class = Parameter()

    def input_gdb(self):
        '''
        This method must be implemented by subclasses.  Should return a path
        to a GDB to convert to shapes.
        '''
        raise NotImplementedError("Must define `input_gdb` method")

    def run(self):
        shell('''
              PG_USE_COPY=yes ogr2ogr -f "PostgreSQL" PG:"dbname=$PGDATABASE \
              active_schema={schema}" -t_srs "EPSG:4326" -nlt MultiPolygon \
              -nln {tablename} {infile}
              '''.format(schema=self.output().schema,
                         infile=self.input_gdb(),
                         tablename=self.output().tablename))


class Shp2TempTableTask(TempTableTask):
    '''
    A task that loads :meth:`~.tasks.Shp2TempTableTask.input_shp()` into a
    temporary Postgres table.  That method must be overriden.
    '''

    encoding = Parameter(default='latin1', significant=False)

    def input_shp(self):
        '''
        This method must be implemented by subclasses.  Should return either
        a path to a single shapefile, or an iterable of paths to shapefiles.
        In that case, the first in the list will determine the schema.
        '''
        raise NotImplementedError("Must specify `input_shp` method")

    def run(self):
        if isinstance(self.input_shp(), str):
            shps = [self.input_shp()]
        else:
            shps = self.input_shp()
        schema = self.output().schema
        tablename = self.output().tablename
        operation = '-overwrite -lco OVERWRITE=yes -lco SCHEMA={schema} -lco PRECISION=no'.format(
            schema=schema)
        for shp in shps:
            # First we  try to import using utf8 as encoding if not able we
            # fallback to the encoding passed as parameter
            cmd = self.build_ogr_command(encoding='utf-8',
                                         schema=schema,
                                         tablename=tablename,
                                         shp=shp,
                                         operation=operation)
            output = shell(cmd)
            if self.utf8_error(output):
                cmd = self.build_ogr_command(encoding=self.encoding,
                                             schema=schema,
                                             tablename=tablename,
                                             shp=shp,
                                             operation=operation)
                shell(cmd)
            # We don't add lco (layer creation options) because in the append mode
            # are ignored
            operation = '-append '.format(schema=schema)

    def utf8_error(self, cmd_output):
        regex = re.compile('invalid byte sequence for encoding \"UTF8\"', re.IGNORECASE)
        return re.search(regex, cmd_output) is not None

    def build_ogr_command(self, **args):
        return 'PG_USE_COPY=yes PGCLIENTENCODING=UTF8 ' \
               'ogr2ogr --config SHAPE_ENCODING {encoding} -f PostgreSQL PG:"dbname=$PGDATABASE ' \
               'active_schema={schema}" -t_srs "EPSG:4326" ' \
               '-nlt MultiPolygon -nln {table} ' \
               '{operation} \'{input}\' '.format(encoding=args['encoding'],
                                                 schema=args['schema'],
                                                 table=args['tablename'],
                                                 input=args['shp'],
                                                 operation=args['operation'])


class CSV2TempTableTask(TempTableTask):
    '''
    A task that loads :meth:`~.tasks.CSV2TempTableTask.input_csv` into a
    temporary Postgres table.  That method must be overriden.

    Optionally, :meth:`~tasks.CSV2TempTableTask.coldef` can be overriden.

    Under the hood, uses postgres's ``COPY``.

    :param delimiter: Delimiter separating fields in the CSV.  Defaults to
                      ``,``.  Must be one character.
    :param has_header: Boolean as to whether first row has column names.
                       Defaults to ``True``.
    :param force: Boolean as to whether the task should be run even if the
                  temporary output already exists.
    '''

    delimiter = Parameter(default=',', significant=False)
    has_header = BoolParameter(default=True, significant=False)
    encoding = Parameter(default='utf8', significant=False)

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
        * If :attr:`~.tasks.CSV2TempTableTask.has_header` is ``True``, then
          column names will come from the headers.
        * If :attr:`~.tasks.CSV2TempTableTask.has_header` is ``False``, then
          column names will be the postgres defaults.
        '''
        if isinstance(self.input_csv(), str):
            csv = self.input_csv()
        else:
            raise NotImplementedError("Cannot automatically determine colnames "
                                      "if several input CSVs.")
        header_row = shell('head -n 1 "{csv}"'.format(csv=csv), encoding=self.encoding).strip()
        return [(h.replace('"', ''), 'Text') for h in header_row.split(self.delimiter)]

    def read_method(self, fname):
        return 'cat "{input}"'.format(input=fname)

    def run(self):
        if isinstance(self.input_csv(), str):
            csvs = [self.input_csv()]
        else:
            csvs = self.input_csv()

        session = current_session()
        session.execute('CREATE TABLE {output} ({coldef})'.format(
            output=self.output().table,
            coldef=', '.join(['"{}" {}'.format(c[0], c[1]) for c in self.coldef()])
        ))
        session.commit()
        options = ['''
           DELIMITER '"'{delimiter}'"' ENCODING '"'{encoding}'"'
        '''.format(delimiter=self.delimiter,
                   encoding=self.encoding)]
        if self.has_header:
            options.append('CSV HEADER')
        try:
            for csv in csvs:
                shell(r'''{read_method} | psql -c '\copy {table} FROM STDIN {options}' '''.format(
                    read_method=self.read_method(csv),
                    table=self.output().table,
                    options=' '.join(options)
                ))
            self.after_copy()
        except:
            session.rollback()
            session.execute('DROP TABLE IF EXISTS {output}'.format(
                output=self.output().table))
            session.commit()
            raise

    def after_copy(self):
        pass


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

    def _requires(self):
        reqs = super(TableTask, self)._requires()
        if self._testmode:
            return [r for r in reqs if isinstance(r, (TagsTask, TableTask, ColumnsTask))]
        else:
            return reqs

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
        determine the schema of the resulting :class:`~tasks.targets.TableTarget`.

        The return for this function should be constructed by selecting the
        desired columns from :class:`~tasks.tasks.ColumnTask`s, specified as
        inputs in :meth:`~.tasks.TableTask.requires()`

        Must return a :py:class:`~collections.OrderedDict` of
        (colname, :class:`~tasks.targets.ColumnTarget`) pairs.

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

    def fake_populate(self, output):
        '''
        Put one empty row in the table
        '''
        session = current_session()
        session.execute('INSERT INTO {output} ({col}) VALUES (NULL)'.format(
            output=output.table,
            col=list(self._columns.keys())[0]
        ))

    def description(self):
        '''
        Optional description for the :class:`~tasks.meta.OBSTable`.  Not
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

    def the_geom(self, output, colname):
        session = current_session()
        return session.execute(
            'SELECT ST_AsText( '
            '  ST_Intersection( '
            '    ST_MakeEnvelope(-179.999, -89.999, 179.999, 89.999, 4326), '
            '    ST_Multi( '
            '      ST_CollectionExtract( '
            '        ST_MakeValid( '
            '          ST_SnapToGrid( '
            '            ST_Buffer( '
            '              ST_Union( '
            '                ST_MakeValid( '
            '                  ST_Simplify( '
            '                    ST_SnapToGrid({geom_colname}, 0.3) '
            '                  , 0) '
            '                ) '
            '              ) '
            '            , 0.3, 2) '
            '          , 0.3) '
            '        ) '
            '      , 3) '
            '    ) '
            '  ) '
            ') the_geom '
            'FROM {output}'.format(
                geom_colname=colname,
                output=output.table
            )).fetchone()['the_geom']

    @property
    def _testmode(self):
        if os.environ.get('ENVIRONMENT') == 'test':
            return True
        return getattr(self, '_test', False)

    def run(self):
        LOGGER.info('getting output()')
        before = time.time()
        output = self.output()
        after = time.time()
        LOGGER.info('time: %s', after - before)

        LOGGER.info('update_or_create_table')
        before = time.time()
        output.update_or_create_table()
        after = time.time()
        LOGGER.info('time: %s', after - before)

        if self._testmode:
            LOGGER.info('fake_populate')
            before = time.time()
            self.fake_populate(output)
            after = time.time()
            LOGGER.info('time: %s', after - before)
        else:
            LOGGER.info('populate')
            self.populate()

        before = time.time()
        LOGGER.info('update_or_create_metadata')
        output.update_or_create_metadata(_testmode=self._testmode)
        after = time.time()
        LOGGER.info('time: %s', after - before)

        LOGGER.info('analyzing the table %s', self.output().table)
        session = current_session()
        session.execute('ANALYZE {table}'.format(table=self.output().table))

        if not self._testmode:
            LOGGER.info('checking for null columns on %s', self.output().table)
            self.check_null_columns()

            LOGGER.info('create_indexes')
            self.create_indexes(output)
            current_session().flush()

            LOGGER.info('create_geom_summaries')
            self.create_geom_summaries(output)

    def create_indexes(self, output):
        session = current_session()
        tablename = output.table
        for colname, coltarget in self._columns.items():
            col = coltarget._column
            index_type = col.index_type
            if index_type:
                index_name = '{}_{}_idx'.format(tablename.split('.')[-1], colname)
                session.execute('CREATE {unique} INDEX IF NOT EXISTS {index_name} ON {table} '
                                'USING {index_type} ({colname})'.format(
                                    unique='',
                                    index_type=index_type,
                                    index_name=index_name,
                                    table=tablename, colname=colname))

    def create_geom_summaries(self, output):
        geometry_columns = [
            (colname, coltarget._id) for colname, coltarget in
            self.columns().items() if coltarget._column.type.lower().startswith('geometry')
        ]

        if len(geometry_columns) == 0:
            return
        elif len(geometry_columns) > 1:
            raise Exception('Having more than one geometry column in one table '
                            'could lead to problematic behavior ')
        colname, colid = geometry_columns[0]
        # Use SQL directly instead of SQLAlchemy because we need the_geom set
        # on obs_table in this session
        current_session().execute("UPDATE observatory.obs_table "
                                  "SET the_geom = ST_GeomFromText('{the_geom}', 4326) "
                                  "WHERE id = '{id}'".format(
                                      the_geom=self.the_geom(output, colname),
                                      id=output._id
                                  ))
        generate_tile_summary(current_session(),
                              output._id, colid, output.table, colname)

    def check_null_columns(self):
        session = current_session()
        result = session.execute("SELECT attname FROM pg_stats WHERE schemaname = 'observatory' "
                                 "AND tablename = '{table}' AND null_frac = 1".format(
                                    table=self.output()._tablename)).fetchall()

        if result:
            raise ValueError('The following columns of the table "{table}" contain only NULL values: {columns}'.format(
                table=self.output().table, columns=', '.join([x[0] for x in result])))

    def output(self):
        if not hasattr(self, '_columns'):
            self._columns = self.columns()

        tt = TableTarget(classpath(self),
                         underscore_slugify(unqualified_task_id(self.task_id)),
                         OBSTable(description=self.description(),
                                  version=self.version(),
                                  timespan=self.timespan()),
                         self._columns, self)
        return tt

    def complete(self):
        return TableTarget(classpath(self),
                           underscore_slugify(unqualified_task_id(self.task_id)),
                           OBSTable(description=self.description(),
                                    version=self.version(),
                                    timespan=self.timespan()),
                           [], self).exists()


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
                    print(cmd)
                    session.execute(cmd)
                    cmd = 'ALTER TABLE "{schema}".{new} SET SCHEMA observatory'.format(
                        new=tablename, schema=schema)
                    print(cmd)
                    session.execute(cmd)
            else:
                resp = session.execute('SELECT COUNT(*) FROM information_schema.tables '
                                       "WHERE table_schema ILIKE 'public'  "
                                       "  AND table_name ILIKE '{table}' ".format(
                                           table=tablename))
                if int(resp.fetchone()[0]) > 0:
                    cmd = 'ALTER TABLE public.{new} SET SCHEMA observatory'.format(
                        new=tablename)
                    print(cmd)
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
    Remove tables that aren't documented anywhere in metadata.  Cleaning.
    '''

    force = BoolParameter(default=False)

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
            if not self.force:
                cnt = session.execute(
                    'select count(*) from observatory.{}'.format(tablename)).fetchone()[0]
                if cnt > 0:
                    LOGGER.warn('not automatically dropping {}, it has {} rows'.format(
                        tablename, cnt))
                    continue
            session.execute('drop table observatory.{}'.format(tablename))
        session.commit()


class CleanMetadata(Task):
    '''
    Remove all columns, tables, and tags that have no references to other
    metadata entities.
    '''

    def run(self):
        session = current_session()

        clear_tables = '''
            DELETE FROM observatory.obs_table
            WHERE id NOT IN (SELECT DISTINCT table_id from observatory.obs_column_table)
        '''

        clear_columns = '''
            DELETE FROM observatory.obs_column
            WHERE id NOT IN (SELECT DISTINCT column_id from observatory.obs_column_table)
        '''

        clear_tags = '''
            DELETE FROM observatory.obs_tag
            WHERE id NOT IN (SELECT DISTINCT tag_id from observatory.obs_column_tag)
        '''

        for q in (clear_tables, clear_columns, clear_tags):
            session.execute(q)
        session.commit()

        self._complete = True

    def complete(self):
        return getattr(self, '_complete', False)


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
        'date': 'TIMESTAMP',
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
        if resp.status_code != 200:
            raise Exception('Non-200 code (%s): %s', resp.status_code, resp.text)
        coltypes = dict([
            (k, self.TYPE_MAP[v['type']]) for k, v in resp.json()['fields'].items()
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
        print(path)
        return S3Target(path)


class GenerateRasterTiles(Task):

    table_id = Parameter()
    column_id = Parameter()

    force = BoolParameter(default=False, significant=False)

    def run(self):
        self._ran = True
        session = current_session()
        try:
            resp = session.execute('''SELECT tablename, colname
                               FROM observatory.obs_table t,
                                    observatory.obs_column_table ct,
                                    observatory.obs_column c
                               WHERE c.type ILIKE 'geometry%'
                                 AND c.id = '{column_id}'
                                 AND t.id = '{table_id}'
                                 AND c.id = ct.column_id
                                 AND t.id = ct.table_id'''.format(
                                     column_id=self.column_id,
                                     table_id=self.table_id
                                 ))
            tablename, colname = resp.fetchone()
            LOGGER.info('table_id: %s, column_id: %s, tablename: %s, colname: %s',
                        self.table_id, self.column_id, tablename, colname)
            generate_tile_summary(session, self.table_id, self.column_id,
                                  'observatory.' + tablename, colname)
            session.commit()
        except:
            session.rollback()
            raise

    def complete(self):
        if self.force and not hasattr(self, '_ran'):
            return False
        session = current_session()
        resp = session.execute('''
            SELECT COUNT(*)
            FROM observatory.obs_column_table_tile
            WHERE table_id = '{table_id}'
              AND column_id = '{column_id}'
        '''.format(table_id=self.table_id, column_id=self.column_id))
        numrows = resp.fetchone()[0]
        return numrows > 0


class GenerateAllRasterTiles(WrapperTask):

    force = BoolParameter(default=False, significant=False)

    def requires(self):
        session = current_session()
        resp = session.execute('''
            SELECT DISTINCT table_id, column_id
            FROM observatory.obs_table t,
                 observatory.obs_column_table ct,
                 observatory.obs_column c
            WHERE t.id = ct.table_id
              AND c.id = ct.column_id
              AND c.type ILIKE 'geometry%'
        ''')
        for table_id, column_id in resp:
            yield GenerateRasterTiles(table_id=table_id, column_id=column_id,
                                      force=self.force)


class MetaWrapper(WrapperTask):
    '''
    End-product wrapper for a set of tasks that should yield entries into the
    `obs_meta` table.
    '''

    params = {}

    def tables(self):
        raise NotImplementedError('''
          Must override `tables` with a function that yields TableTasks
                                  ''')

    def requires(self):
        for t in self.tables():
            assert isinstance(t, TableTask)
            yield t


class RunWrapper(WrapperTask):
    '''
    Run MetaWrapper for a specific module.
    '''

    wrapper = Parameter()

    def requires(self):
        module = 'tasks/' + self.wrapper.replace('.', '/')
        for task_klass, params in collect_meta_wrappers(test_module=module,
                                                        test_all=True):
            yield task_klass(**params)


class RunDiff(WrapperTask):
    '''
    Run MetaWrapper for all tasks that changed compared to master.
    '''

    compare = Parameter()
    test_all = BoolParameter(default=False)

    def requires(self):
        try:
            resp = shell("git diff '{compare}' --name-only | grep '^tasks'".format(
                compare=self.compare
            ))
        except subprocess.CalledProcessError:
            # No diffs
            return
        for line in resp.split('\n'):
            if not line:
                continue
            module = line.replace('.py', '')
            LOGGER.info(module)
            for task_klass, params in collect_meta_wrappers(test_module=module, test_all=self.test_all):
                yield task_klass(**params)


def cross(orig_list, b_name, b_list):
    result = []
    for orig_dict in orig_list:
        for b_val in b_list:
            new_dict = orig_dict.copy()
            new_dict[b_name] = b_val
            result.append(new_dict)
    return result


def collect_tasks(task_klass, test_module=None):
    '''
    Returns a set of task classes whose parent is the passed `TaskClass`.

    Can limit to scope of tasks within module.
    '''
    tasks = set()
    for dirpath, _, files in os.walk('tasks'):
        for filename in files:
            if test_module:
                if not os.path.join(dirpath, filename).startswith(test_module):
                    continue
            if filename.endswith('.py'):
                modulename = '.'.join([
                    dirpath.replace(os.path.sep, '.'),
                    filename.replace('.py', '')
                ])
                module = importlib.import_module(modulename)
                for _, obj in inspect.getmembers(module):
                    if inspect.isclass(obj) and issubclass(obj, task_klass) and obj != task_klass:
                        if test_module and obj.__module__ != modulename:
                            continue
                        else:
                            tasks.add((obj, ))
    return tasks


def collect_requirements(task):
    requirements = task._requires()
    for r in requirements:
        requirements.extend(collect_requirements(r))
    return requirements


def collect_meta_wrappers(test_module=None, test_all=True):
    '''
    Yield MetaWrapper and associated params for every MetaWrapper that has been
    touched by changes in test_module.

    Does not collect meta wrappers that have been affected by a change in a
    superclass.
    '''
    affected_task_classes = set([t[0] for t in collect_tasks(Task, test_module)])

    for t, in collect_tasks(MetaWrapper):
        outparams = [{}]
        for key, val in t.params.items():
            outparams = cross(outparams, key, val)
        req_types = None
        for params in outparams:
            # if the metawrapper itself is not affected, look at its requirements
            if t not in affected_task_classes:

                # generating requirements separately for each cross product is
                # too slow, just use the first one
                if req_types is None:
                    req_types = set([type(r) for r in collect_requirements(t(**params))])
                if not affected_task_classes.intersection(req_types):
                    continue
            yield t, params
            if not test_all:
                break
