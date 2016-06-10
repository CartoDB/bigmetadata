# http://webarchive.nationalarchives.gov.uk/20160105160709/http://ons.gov.uk/ons/guide-method/census/2011/census-data/bulk-data/bulk-data-downloads/index.html

from luigi import Task, Parameter, LocalTarget

from tasks.util import TableTask, ColumnsTask, classpath, shell

import os


class DownloadEnglandWalesLocal(Task):

    URL = 'http://data.statistics.gov.uk/Census/BulkLocalCharacteristicsoaandinfo310713.zip'

    def run(self):
        self.output().makedirs()
        shell('wget -O {output}.zip {url}'.format(output=self.output().path,
                                                  url=self.URL))
        os.makedirs(self.output().path)
        shell('unzip -d {output} {output}.zip'.format(output=self.output().path))

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self)))



