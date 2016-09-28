'''
Util functions for tests
'''

from subprocess import check_output


EMPTY_RASTER = '0100000000000000000000F03F000000000000F0BF0000000000000000' \
        '000000000000000000000000000000000000000000000000000000000A000A00'


class FakeTask(object):

    task_id = 'fake'


def recreate_db():
    check_output('''
    psql -d gis -c "SELECT pg_terminate_backend(pg_stat_activity.pid)
             FROM pg_stat_activity
             WHERE pg_stat_activity.datname = 'test'
               AND pid <> pg_backend_pid();"
    ''', shell=True)
    check_output('dropdb --if-exists test', shell=True)
    check_output('createdb test -E UTF8 -T template0', shell=True)
    check_output('psql -d test -c "CREATE EXTENSION IF NOT EXISTS postgis"', shell=True)
    check_output('psql -d test -c "CREATE SCHEMA IF NOT EXISTS observatory"', shell=True)

recreate_db()


from tasks.util import shell
from contextlib import contextmanager
from tasks.meta import (get_engine, current_session, Base, session_commit,
                        session_rollback)
from luigi.worker import Worker
from luigi.scheduler import CentralPlannerScheduler


def setup():
    if Base.metadata.bind.url.database != 'test':
        raise Exception('Can only run tests on database "test"')
    session = current_session()
    session.rollback()
    Base.metadata.create_all()


def teardown():
    if Base.metadata.bind.url.database != 'test':
        raise Exception('Can only run tests on database "test"')
    session = current_session()
    session.rollback()
    Base.metadata.drop_all()



def runtask(task):
    '''
    Run deps of tasks then the task, faking session management
    '''
    if task.complete():
        return
    for dep in task.deps():
        runtask(dep)
        assert dep.complete() is True
    try:
        task.run()
        task.on_success()
    except Exception as exc:
        task.on_failure(exc)
        raise


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    try:
        yield current_session()
        session_commit(None)
    except Exception as e:
        session_rollback(None, e)
        raise
