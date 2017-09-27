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
        self.engine = self._engine()
        self.session = sessionmaker(bind=self.engine)()

    def _engine(self):
        return create_engine(
            self.connection_string,
        )

    def current_session(self):
        return self.session

    def commit(self):
        try:
            logging.info('commit {}'.format(self.task_id))
            self.session.commit()
        except:
            self.session.rollback()
            self.session.expunge_all()
            raise
        finally:
            self.session.close()
            self.session = None

    def rollback(self, e):
        try:
            logging.info('rollback {}: {}'.format(self.task_id, e))
            self.session.rollback()
        except:
            raise
        finally:
            self.session.expunge_all()
            self.session.close()
            self.session = None

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
        self.engine = self._engine()
        self.session = sessionmaker(bind=self.engine)()

    def _engine(self):
        return create_engine(
            self.connection_string,
        )

    def current_session(self):
        return self.session

    def commit(self):
        try:
            logger.info('commit {}'.format(self.task_id))
            self.session.commit()
        except:
            self.session.rollback()
            self.session.expunge_all()
            raise
        finally:
            self.session.close()
            self.session = None

    def rollback(self, e):
        try:
            logging.info('rollback {}: {}'.format(self.task_id, e))
            self.session.rollback()
        except:
            raise
        finally:
            self.session.expunge_all()
            self.session.close()
            self.session = None
