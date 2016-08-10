'''
Test TableTask classes
'''

from collections import OrderedDict
from nose import with_setup
from nose.tools import assert_equal

from tests.util import session_scope, runtask

from tasks.meta import OBSColumn, OBSTable, current_session
from tasks.util import ColumnsTask, TableTask, classpath


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


def test_table_task():
    with session_scope() as session:
        task = TestTableTask()
        runtask(task)
        assert_equal(
            session.query(OBSTable).filter(
                OBSTable.id.startswith('test_tabletasks.test_table_task_')).count(), 1)


def test_geom_table_task():
    with session_scope() as session:
        task = TestGeomTableTask()
        runtask(task)
        assert_equal(
            session.query(OBSTable).filter(
                OBSTable.id.startswith('test_tabletasks.test_geom_table_task_')).count(), 1)
