# http://www.caixabankresearch.com/anuario

from luigi import Task, Parameter, LocalTarget

from tasks.util import shell, classpath, TableTask, ColumnsTask

from collections import OrderedDict
import os

class DownloadAnuario(Task):

    year = Parameter()

    URL = 'http://www.caixabankresearch.com/documents/10180/266550/AE{year2}' \
            '_Datos_estadisticos_municipales-provinciales.zip'

    def run(self):
        self.output().makedirs()
        shell('wget -O {output}.zip {url}'.format(
            output=self.output().path,
            url=self.URL.format(year2=self.year[-2:])
        ))
        os.makedirs(self.output().path)
        shell('unzip -d {output} {output}.zip'.format(
            output=self.output().path))

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self)))


class AnuarioColumns(TableTask):

    def version(self):
        return 1

    def columns(self):
        return OrderedDict([

        ])


class Anuario(TableTask):

    resolution = Parameter()

    def columns(self):
        pass
