# http://www.scotlandscensus.gov.uk/ods-web/data-warehouse.html#bulkdatatab

from luigi import Task, Parameter, LocalTarget

from tasks.util import (TableTask, TagsTask, ColumnsTask, classpath, shell,
                        DownloadUnzipTask)

import os

class SourceTags(TagsTask):
	def version(self):
		return 1

	def tags(self):
		return [
			OBSTag(id='scotland',
					name="Scotland's Census Data Warehouse by National Records of Scotland",
					description="http://www.scotlandscensus.gov.uk/")]

class DownloadScotlandLocal(DownloadUnzipTask):

    URL = 'http://www.scotlandscensus.gov.uk/ods-web/download/getDownloadFile.html?downloadFileIds=Output%20Area%20blk'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(output=self.output().path,
                                                  url=self.URL))
