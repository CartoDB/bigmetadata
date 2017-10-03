from luigi import Task, WrapperTask
import os
from sqlalchemy import create_engine
from sqlalchemy.orm.session import sessionmaker
import collections
import logging

class DatabaseTask(Task):

    def __init__(self, *args, **kwargs):
        super(DatabaseTask, self).__init__(*args, **kwargs)
        self.connection_string = 'postgres://{user}:{password}@{host}:{port}/{db}'.format(
            user=os.environ.get('PGUSER', 'postgres'),
            password=os.environ.get('PGPASSWORD', ''),
            host=os.environ.get('PGHOST', 'localhost'),
            port=os.environ.get('PGPORT', '5432'),
            db=os.environ.get('PGDATABASE', 'postgres')
        )
        self._engine = None
        self._session = None

    def engine(self):
        if not self._engine:
            self._engine = create_engine(
                self.connection_string
            )
        return self._engine

    def current_session(self):
        if not self._session:
            self._session = sessionmaker(bind=self.engine())()
        return self._session

    def on_success(self):
        self.commit()

    def on_failure(self, exception):
        self.rollback(exception)

    def commit(self):
        try:
            logging.info('commit {}'.format(self.task_id))
            self.current_session().commit()
        except:
            self.current_session().rollback()
            self.current_session().expunge_all()
            raise
        finally:
            self.current_session().close()
            self._session = None

    def rollback(self, e):
        try:
            logging.info('rollback {}: {}'.format(self.task_id, e))
            self.current_session().rollback()
        except:
            raise
        finally:
            self.current_session().expunge_all()
            self.current_session().close()
            self._session = None

class DatabaseWrapperTask(WrapperTask):

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapperTask, self).__init__(*args, **kwargs)
        self.connection_string = 'postgres://{user}:{password}@{host}:{port}/{db}'.format(
            user=os.environ.get('PGUSER', 'postgres'),
            password=os.environ.get('PGPASSWORD', ''),
            host=os.environ.get('PGHOST', 'localhost'),
            port=os.environ.get('PGPORT', '5432'),
            db=os.environ.get('PGDATABASE', 'postgres')
        )
        self._engine = None
        self._session = None

    def engine(self):
        if not self._engine:
            self._engine = create_engine(
                self.connection_string
            )
        return self._engine

    def current_session(self):
        if not self._session:
            self._session = sessionmaker(bind=self.engine())()
        return self._session

    def on_success(self):
        self.commit()

    def on_failure(self, exception):
        self.rollback(exception)

    def commit(self):
        try:
            logger.info('commit {}'.format(self.task_id))
            self.current_session().commit()
        except:
            self.current_session().rollback()
            self.current_session().expunge_all()
            raise
        finally:
            self.current_session().close()
            self._session = None

    def rollback(self, e):
        try:
            logging.info('rollback {}: {}'.format(self.task_id, e))
            self.current_session().rollback()
        except:
            raise
        finally:
            self.current_session().expunge_all()
            self.current_session().close()
            self._session = None
