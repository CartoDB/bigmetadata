'''
Test ColumnTask classes
'''


from tests.util import session_scope, runtask
from tasks.es.ine import FiveYearPopulationColumns
from tasks.meta import (get_engine, current_session, Base, session_commit,
                        session_rollback, OBSColumn)
from tasks.util import ColumnsTask, TableTask, classpath

from nose import with_setup
from nose.tools import assert_greater
from nose.plugins.skip import SkipTest
from nose_parameterized import parameterized

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
def test_column_task(klass):
    # Ensure every column task runs and produces some kind of independent
    # metadata.
    with session_scope() as session:
        # TODO: test columnstasks that have params
        if klass.get_param_names():
            raise SkipTest('Cannot test ColumnsTask with params')
        task = klass()
        runtask(task)
        assert_greater(
            session.query(OBSColumn).filter(
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
