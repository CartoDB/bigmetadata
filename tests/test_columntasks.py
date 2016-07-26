'''
Test ColumnTask classes
'''


from tests.util import session_scope, runtask, setup, teardown
from tasks.es.ine import FiveYearPopulationColumns
from tasks.meta import (get_engine, current_session, Base, session_commit,
                        session_rollback, OBSColumn)
from tasks.util import shell, ColumnsTask, classpath

from nose import with_setup
from nose.tools import assert_greater
from nose.plugins.skip import SkipTest
from nose_parameterized import parameterized

import importlib
import inspect
import os


def column_tasks():
    for dirpath, _, files in os.walk('tasks'):
        for filename in files:
            if filename.endswith('.py'):
                modulename = '.'.join([
                    dirpath.replace(os.path.sep, '.'),
                    filename.replace('.py', '')
                ])
                module = importlib.import_module(modulename)
                for _, obj in inspect.getmembers(module):
                    if inspect.isclass(obj) and issubclass(obj, ColumnsTask) and obj != ColumnsTask:
                        yield obj,



@with_setup(setup, teardown)
@parameterized(column_tasks())
def test_column_task(klass):
    # Ensure every column task runs and produces some kind of independent
    # metadata.
    print klass
    with session_scope() as session:
        # TODO: test columnstasks that have params
        if klass.get_param_names():
            raise SkipTest('Cannot test ColumnsTask with params')
        task = klass()
        runtask(task)
        assert_greater(
            session.query(OBSColumn).filter(
                OBSColumn.id.startswith(classpath(task))).count(), 0)
