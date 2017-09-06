'''
Test TableTask classes
'''

import os
from collections import OrderedDict
from nose import with_setup
from nose.tools import (assert_equal, assert_is_not_none, assert_is_none,
                        assert_true, assert_false, with_setup, assert_greater)

from tests.util import runtask, setup, teardown

from tasks.meta import OBSColumn, OBSTable, OBSColumnTableTile, current_session
from tasks.util import ColumnsTask, TableTask, Shp2TempTableTask, classpath, \
                       shell, DownloadUnzipTask, CSV2TempTableTask, \
                       Carto2TempTableTask


class TestColumnsTask(ColumnsTask):

    def columns(self):
        return OrderedDict([
            ('the_geom', OBSColumn(name='', type='Geometry')),
            ('col', OBSColumn(name='', type='Text')),
            ('measure', OBSColumn(name='', type='Numeric')),
        ])


class BaseTestTableTask(TableTask):

    _test = False

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
        session.execute("INSERT INTO {output} VALUES ('foo', 100)".format(
            output=self.output().table
        ))


class TestGeomTableTask(BaseTestTableTask):

    def columns(self):
        return self.input()

    def populate(self):
        session = current_session()
        session.execute(
            "INSERT INTO {output} VALUES "
            "(ST_SetSRID(ST_MakeEnvelope(-10, 10, 10, -10), 4326), 'foo', 100)".format(
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


@with_setup(setup, teardown)
def test_table_task():
    '''
    Very simple table task should run and make an entry in OBSTable, as
    well as a physical table with rows in observatory schema.
    '''
    task = TestTableTask()
    runtask(task)
    table = task.output().get(current_session())
    assert_is_not_none(table)
    assert_is_none(table.the_geom)
    assert_equal(current_session().execute(
        'SELECT COUNT(*) FROM observatory.{}'.format(
            table.tablename)).fetchone()[0], 1)


@with_setup(setup, teardown)
def test_geom_table_task():
    '''
    Table task with geoms should run and generate an entry in OBSTable with
    a summary geom, plus a physical table with rows in observatory schema.

    It should also generate a raster tile summary.
    '''
    task = TestGeomTableTask()
    runtask(task)
    table = task.output().get(current_session())
    assert_is_not_none(table)
    assert_is_not_none(table.the_geom)
    assert_equal(current_session().execute(
        'SELECT COUNT(*) FROM observatory.{}'.format(
            table.tablename)).fetchone()[0], 1)
    assert_greater(current_session().query(OBSColumnTableTile).count(), 0)


@with_setup(setup, teardown)
def test_geom_table_task_update():
    '''
    Should be possible to generate a new version of a table by incrementing
    the version number.
    '''
    task = TestGeomTableTask()
    runtask(task)
    assert_true(task.complete())
    task = TestGeomTableTask()
    task.version = lambda: 10
    assert_false(task.complete())
    current_session().commit()
    runtask(task)
    assert_true(task.complete())


@with_setup(setup, teardown)
def test_shp_2_temp_table_task():
    '''
    Shp to temp table task should run and generate no entry in OBSTable, but
    a physical table in the right schema.
    '''
    task = TestShp2TempTableTask()
    before_table_count = current_session().execute(
        'SELECT COUNT(*) FROM observatory.obs_table').fetchone()[0]
    runtask(task)
    table = task.output()
    assert_equal(current_session().execute(
        'SELECT COUNT(*) FROM {}'.format(
            table.table)).fetchone()[0], 10)
    after_table_count = current_session().execute(
        'SELECT COUNT(*) FROM observatory.obs_table').fetchone()[0]
    assert_equal(before_table_count, after_table_count)


@with_setup(setup, teardown)
def test_csv_2_temp_table_task():
    '''
    CSV to temp table task should run and generate no entry in OBSTable, but
    a physical table in the right schema.
    '''
    task = TestCSV2TempTableTask()
    before_table_count = current_session().execute(
        'SELECT COUNT(*) FROM observatory.obs_table').fetchone()[0]
    runtask(task)
    table = task.output()
    assert_equal(current_session().execute(
        'SELECT COUNT(*) FROM {}'.format(
            table.table)).fetchone()[0], 10)
    after_table_count = current_session().execute(
        'SELECT COUNT(*) FROM observatory.obs_table').fetchone()[0]
    assert_equal(before_table_count, after_table_count)


@with_setup(setup, teardown)
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


@with_setup(setup, teardown)
def test_carto_2_temp_table_task():
    '''
    Convert a table on CARTO to a temp table.
    '''
    task = TestCSV2TempTableTask()
    before_table_count = current_session().execute(
        'SELECT COUNT(*) FROM observatory.obs_table').fetchone()[0]
    runtask(task)
    table = task.output()
    assert_equal(current_session().execute(
        'SELECT COUNT(*) FROM {}'.format(
            table.table)).fetchone()[0], 10)
    after_table_count = current_session().execute(
        'SELECT COUNT(*) FROM observatory.obs_table').fetchone()[0]
    assert_equal(before_table_count, after_table_count)
