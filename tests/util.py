'''
Util functions for tests
'''

import os
from subprocess import check_output

from time import time


EMPTY_RASTER = '0100000000000000000000F03F000000000000F0BF0000000000000000' \
        '000000000000000000000000000000000000000000000000000000000A000A00'


class FakeTask(object):

    task_id = 'fake'


def recreate_db(dbname='test'):
    if not os.environ.get('TRAVIS'):
        check_output('''
        psql -d gis -c "SELECT pg_terminate_backend(pg_stat_activity.pid)
                 FROM pg_stat_activity
                 WHERE pg_stat_activity.datname = '{dbname}'
                   AND pid <> pg_backend_pid();"
        '''.format(dbname=dbname), shell=True)
    check_output('dropdb --if-exists {dbname}'.format(dbname=dbname), shell=True)
    check_output('createdb {dbname} -E UTF8 -T template0'.format(dbname=dbname), shell=True)
    check_output('psql -d {dbname} -c "CREATE EXTENSION IF NOT EXISTS postgis"'.format(
        dbname=dbname), shell=True)
    os.environ['PGDATABASE'] = dbname


from contextlib import contextmanager
from luigi.worker import Worker
from luigi.scheduler import CentralPlannerScheduler


def setup():
    from tasks.meta import current_session, Base
    if Base.metadata.bind.url.database != 'test':
        raise Exception('Can only run tests on database "test"')
    session = current_session()
    session.rollback()
    session.execute('DROP SCHEMA IF EXISTS observatory CASCADE')
    session.execute('CREATE SCHEMA observatory')
    session.commit()
    Base.metadata.create_all()


def teardown():
    from tasks.meta import current_session, Base
    if Base.metadata.bind.url.database != 'test':
        raise Exception('Can only run tests on database "test"')
    session = current_session()
    session.rollback()
    Base.metadata.drop_all()
    session.execute('DROP SCHEMA IF EXISTS observatory CASCADE')
    session.commit()


def runtask(task, superclasses=None):
    '''
    Run deps of tasks then the task, faking session management

    superclasses is a list of classes that we will be willing to run as
    pre-reqs, other pre-reqs will be ignored.  Can be useful when testing to
    only run metadata classes, for example.
    '''
    from tasks.util import LOGGER
    if task.complete():
        return
    for dep in task.deps():
        if superclasses:
            for klass in superclasses:
                if isinstance(dep, klass):
                    runtask(dep, superclasses=superclasses)
                    assert dep.complete() is True
        else:
            runtask(dep)
            assert dep.complete() is True
    try:
        before = time()
        for klass, cb_dict in task._event_callbacks.iteritems():
            if isinstance(task, klass):
                start_callbacks = cb_dict.get('event.core.start', [])
                for scb in start_callbacks:
                    scb(task)
        task.run()
        task.on_success()
        after = time()
        LOGGER.warn('runtask timing %s: %s', task, round(after - before, 2))
    except Exception as exc:
        task.on_failure(exc)
        raise


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    from tasks.meta import current_session, session_commit, session_rollback
    try:
        yield current_session()
        session_commit(None)
    except Exception as e:
        session_rollback(None, e)
        raise
