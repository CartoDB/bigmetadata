# http://www.scotlandscensus.gov.uk/ods-web/data-warehouse.html#bulkdatatab

from luigi import Task, Parameter, LocalTarget

from tasks.util import TableTask, ColumnsTask, classpath, shell

import os

class DownloadScotlandLocal(Task):

    URL = 'http://www.scotlandscensus.gov.uk/ods-web/download/getDownloadFile.html?downloadFileIds=Output%20Area%20blk'

    def run(self):
        self.output().makedirs()
        shell('wget -O {output}.zip {url}'.format(output=self.output().path,
                                                  url=self.URL))
        os.makedirs(self.output().path)
        shell('unzip -d {output} {output}.zip'.format(output=self.output().path))

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self)))

