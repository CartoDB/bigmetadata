import os

from luigi import LocalTarget, Task
from tasks.util import (CartoDBTarget, ColumnsTask, ColumnTarget,
                        LoadPostgresFromURL, PostgresTarget, TableTask,
                        TagsTask, TempTableTask, classpath, get_logger, shell,
                        underscore_slugify, LocalDirTarget,)

from statcan_util import StatCanParser


class NhsDataDownload(Task):

    def run(self):
        pass

    def output(self):
        pass


class NhsParse(Task):
    '''

    '''

    def requires(self):
        return NhsDataDownload()

    def run(self):
        infiles = shell('ls {input}/*.csv'.format(input=self.input().path))
        in_csv_files = infiles.strip().split('\n')
        StatCanParser().parse_csv_to_files(in_csv_files, self.output().path)

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))
