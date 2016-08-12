'''
Test TableTask classes
'''

import os
from collections import OrderedDict
from nose import with_setup
from nose.tools import assert_equal, assert_is_not_none, assert_is_none, \
                       assert_true, assert_false

from tests.util import session_scope, runtask

from tasks.meta import OBSColumn, OBSTable, current_session
from tasks.util import ColumnsTask, TableTask, Shp2TempTableTask, classpath, \
                       shell, DownloadUnzipTask, CSV2TempTableTask, \
                       Carto2TempTableTask


class TestColumnsTask(ColumnsTask):

    def columns(self):
        return OrderedDict([
            ('the_geom', OBSColumn(name='', type='Geometry')),
            ('col', OBSColumn(name='', type='Text')),
        ])


class BaseTestTableTask(TableTask):

    def timespan(self):
        return '2000'

    def requires(self):
        return TestColumnsTask()


class TestTableTask(BaseTestTableTask):

    def columns(self):
        cols = self.input()
        cols.pop('the_geom')
        return cols

    def populate(self):
        session = current_session()
        session.execute("INSERT INTO {output} VALUES ('foo')".format(
            output=self.output().table
        ))


class TestGeomTableTask(BaseTestTableTask):

    def columns(self):
        return self.input()

    def populate(self):
        session = current_session()
        session.execute("INSERT INTO {output} VALUES (ST_MakePoint(0, 0), 'foo')".format(
            output=self.output().table
        ))


class TestShp2TempTableTask(Shp2TempTableTask):

    def input_shp(self):
        return os.path.join('tests', 'fixtures', 'cartodb-query.shp')


class TestDownloadUnzipTask(DownloadUnzipTask):

    def download(self):
        shell('wget -O {output}.zip "{url}"'.format(
            output=self.output().path,
            url='http://andrew.carto.com/api/v2/sql?q=select%20*%20from%20dma_master_polygons%20limit%201&format=shp'
        ))


class TestCarto2TempTableTask(Carto2TempTableTask):

    subdomain = 'andrew'
    table = 'dma_master_polygons'


class TestCSV2TempTableTask(CSV2TempTableTask):

    def input_csv(self):
        return os.path.join('tests', 'fixtures', 'cartodb-query.csv')


def test_table_task():
    '''
    Very simple table task should run and make an entry in OBSTable, as
    well as a physical table with rows in observatory schema.
    '''
    with session_scope() as session:
        task = TestTableTask()
        runtask(task)
        table = task.output().get(session)
        assert_is_not_none(table)
        assert_is_none(table.the_geom)
        assert_equal(session.execute(
            'SELECT COUNT(*) FROM observatory.{}'.format(
                table.tablename)).fetchone()[0], 1)


def test_geom_table_task():
    '''
    Table task with geoms should run and generate an entry in OBSTable with
    a summary geom, plus a physical table with rows in observatory schema.
    '''
    with session_scope() as session:
        task = TestGeomTableTask()
        runtask(task)
        table = task.output().get(session)
        assert_is_not_none(table)
        assert_is_not_none(table.the_geom)
        assert_equal(session.execute(
            'SELECT COUNT(*) FROM observatory.{}'.format(
                table.tablename)).fetchone()[0], 1)


def test_shp_2_temp_table_task():
    '''
    Shp to temp table task should run and generate no entry in OBSTable, but
    a physical table in the right schema.
    '''
    with session_scope() as session:
        task = TestShp2TempTableTask()
        before_table_count = session.execute(
            'SELECT COUNT(*) FROM observatory.obs_table').fetchone()[0]
        runtask(task)
        table = task.output()
        assert_equal(session.execute(
            'SELECT COUNT(*) FROM {}'.format(
                table.table)).fetchone()[0], 10)
        after_table_count = session.execute(
            'SELECT COUNT(*) FROM observatory.obs_table').fetchone()[0]
        assert_equal(before_table_count, after_table_count)


def test_csv_2_temp_table_task():
    '''
    CSV to temp table task should run and generate no entry in OBSTable, but
    a physical table in the right schema.
    '''
    with session_scope() as session:
        task = TestCSV2TempTableTask()
        before_table_count = session.execute(
            'SELECT COUNT(*) FROM observatory.obs_table').fetchone()[0]
        runtask(task)
        table = task.output()
        assert_equal(session.execute(
            'SELECT COUNT(*) FROM {}'.format(
                table.table)).fetchone()[0], 10)
        after_table_count = session.execute(
            'SELECT COUNT(*) FROM observatory.obs_table').fetchone()[0]
        assert_equal(before_table_count, after_table_count)


def test_download_unzip_task():
    '''
    Download unzip task should download remote assets and unzip them locally.
    '''
    task = TestDownloadUnzipTask()
    if task.output().exists():
        shell('rm -r {}'.format(task.output().path))
    assert_false(task.output().exists())
    runtask(task)
    assert_true(task.output().exists())


def test_carto_2_temp_table_task():
    '''
    Convert a table on CARTO to a temp table.
    '''
    with session_scope() as session:
        task = TestCSV2TempTableTask()
        before_table_count = session.execute(
            'SELECT COUNT(*) FROM observatory.obs_table').fetchone()[0]
        runtask(task)
        table = task.output()
        assert_equal(session.execute(
            'SELECT COUNT(*) FROM {}'.format(
                table.table)).fetchone()[0], 10)
        after_table_count = session.execute(
            'SELECT COUNT(*) FROM observatory.obs_table').fetchone()[0]
        assert_equal(before_table_count, after_table_count)
