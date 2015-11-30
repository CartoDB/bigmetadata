#!/usr/bin/env python

'''
Tiger
'''

from tasks.util import LoadPostgresFromURL
from luigi import WrapperTask, Parameter


class DownloadTiger(WrapperTask):

    url_template = 'https://s3.amazonaws.com/census-backup/tiger/{year}/tiger{year}_backup.sql.gz'
    year = Parameter()

    def requires(self):
        url = self.url_template.format(year=self.year)
        table = 'tiger{year}.census_names'.format(year=self.year)
        return LoadPostgresFromURL(url=url, table=table)


class ProcessTiger(WrapperTask):

    year = Parameter()

    def requires(self):
        yield DownloadTiger(year=self.year)


class Tiger(WrapperTask):

    def requires(self):
        yield ProcessTiger(year=2013)
