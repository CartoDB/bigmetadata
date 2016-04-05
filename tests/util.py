'''
Util functions for tests
'''

from contextlib import contextmanager
from tasks.meta import (get_engine, current_session, Base, session_commit,
                        session_rollback)
from sqlalchemy.orm import sessionmaker
from luigi.worker import Worker
from luigi.scheduler import CentralPlannerScheduler
from Queue import Queue


def setup():
    Base.metadata.drop_all()
    Base.metadata.create_all()
    #current_session().begin()


def teardown():
    current_session().close()
    Base.metadata.drop_all()
    #current_session().begin()



def runtask(task):
    '''
    Run deps of tasks then the task, faking session management
    '''
    scheduler = CentralPlannerScheduler(retry_delay=100, remove_delay=1000,
                                        worker_disconnect_delay=10)
    with Worker(scheduler=scheduler, worker_id='X') as worker:
        worker.add(task)
        worker.run()
    #q = Queue()
    #worker = TaskProcess(task, 1, Queue())
    #worker.run()
    #return q


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    try:
        yield current_session()
        session_commit(None)
    except Exception as e:
        session_rollback(None, e)
        raise
    finally:
        pass
    #session = sessionmaker(bind=get_engine())()
    #try:
    #    yield session
    #    session.commit()
    #except:
    #    session.rollback()
    #    raise
    #finally:
    #    session.close()
