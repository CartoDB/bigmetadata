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


class LoadPostgresDumpFromURL(Task):

    url = Parameter()
    table = Parameter()
    #gunzip = Parameter() # TODO

    def run(self):
        subprocess.check_call('curl {url} | gunzip -c | psql'.format(url=self.url), shell=True)

    def output(self):
        return DefaultPostgresTarget(
            table=self.table,
            update_id=self.table
        )
