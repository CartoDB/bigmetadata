'''
Test ColumnTask classes
'''


from tests.util import runtask, setup, teardown

from tasks.es.ine import FiveYearPopulationColumns
from tasks.meta import (get_engine, current_session, Base,
                        session_rollback, OBSColumn, current_session)
from tasks.util import ColumnsTask, TableTask, TagsTask, classpath

# Monkeypatch TableTask
TableTask._test = True

from tasks.carto import OBSMetaToLocal

from nose.tools import assert_greater, with_setup, assert_true
from nose.plugins.skip import SkipTest
from nose_parameterized import parameterized

from tasks.ca.statcan.data import NHS, Census
from tasks.us.census.acs import Extract
from tasks.us.census.tiger import SumLevel

import importlib
import inspect
import os


def collect_tasks(TaskClass):
    tasks = set()
    for dirpath, _, files in os.walk('tasks'):
        for filename in files:
            if filename.endswith('.py'):
                modulename = '.'.join([
                    dirpath.replace(os.path.sep, '.'),
                    filename.replace('.py', '')
                ])
                module = importlib.import_module(modulename)
                for _, obj in inspect.getmembers(module):
                    if inspect.isclass(obj) and issubclass(obj, TaskClass) and obj != TaskClass:
                        tasks.add((obj, ))
    return tasks


@parameterized(collect_tasks(ColumnsTask))
@with_setup(setup, teardown)
def test_column_task(klass):
    # Ensure every column task runs and produces some kind of independent
    # metadata.
    # TODO: test columnstasks that have params
    if klass.get_param_names():
        raise SkipTest('Cannot test ColumnsTask with params')
    task = klass()
    runtask(task)
    assert_greater(
        current_session().query(OBSColumn).filter(
            OBSColumn.id.startswith(classpath(task))).count(), 0)



#@parameterized(collect_tasks(TableTask))
#def test_table_task(klass):
#    # Ensure every column task runs and produces some kind of independent
#    # metadata.
#    with session_scope() as session:
#        # TODO: test tabletasks that have params
#        if klass.get_param_names():
#            raise SkipTest('Cannot test TableTask with params')
#        task = klass()
#        # we don't do a full `run` as that's too expensive
#        # instead, we run any column reqs and make sure the `columns` method
#        # works after that
#        for dep in task.deps():
#            if issubclass(dep, ColumnsTask):
#                runtask(dep)
#        task.columns()
#        #assert_greater(
#        #    session.query(OBSColumn).filter(
#        #        OBSColumn.id.startswith(classpath(task))).count(), 0)
