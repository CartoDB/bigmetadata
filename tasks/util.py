'''
Util functions for luigi bigmetadata tasks.
'''

import os
import subprocess
from luigi import Task, Parameter
from luigi.postgres import PostgresTarget

class DefaultPostgresTarget(PostgresTarget):
    '''
    PostgresTarget which by default uses command-line specified login.
    '''

    def __init__(self, *args, **kwargs):
        kwargs['host'] = kwargs.get('host', os.environ.get('PGHOST', 'localhost'))
        kwargs['port'] = kwargs.get('port', os.environ.get('PGPORT', '5432'))
        kwargs['user'] = kwargs.get('user', os.environ.get('PGUSER', 'postgres'))
        kwargs['password'] = kwargs.get('password', os.environ.get('PGPASSWORD'))
        kwargs['database'] = kwargs.get('database', os.environ.get('PGDATABASE', 'postgres'))
        super(DefaultPostgresTarget, self).__init__(*args, **kwargs)


class LoadPostgresFromURL(Task):

    url = Parameter()
    table = Parameter()
    #gunzip = Parameter() # TODO

    def run(self):
        subprocess.check_call('curl {url} | gunzip -c | psql'.format(url=self.url), shell=True)
        self.output().touch()

    def output(self):
        return DefaultPostgresTarget(
            table=self.table,
            update_id=self.table
        )


class MetadataPathMixin():
    '''
    Mixin to provide metadata path
    '''

    @property
    def path(self):
        '''
        Path to this task, suitable for the current OS.
        '''
        return os.path.join(*type(self).__module__.split('.')[1:])


class MetadataTask(MetadataPathMixin, Task):
    '''
    A task that generates metadata.  This will ensure that the tables/columns
    folders exist before starting.
    '''

    def __init__(self, *args, **kwargs):
        # Make sure output folders exist
        for folder in ('columns', 'tables'):
            try:
                os.makedirs(os.path.join(folder, self.path))
            except OSError:
                pass

        super(MetadataTask, self).__init__(*args, **kwargs)

