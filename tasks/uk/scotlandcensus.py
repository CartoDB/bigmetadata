# http://www.scotlandscensus.gov.uk/ods-web/data-warehouse.html#bulkdatatab

from luigi import Task, Parameter, LocalTarget

from tasks.util import (TableTask, TagsTask, ColumnsTask, classpath, shell,
                        DownloadUnzipTask)
from tasks.meta import OBSTag
import os

class SourceTags(TagsTask):
	def version(self):
		return 1

	def tags(self):
		return [OBSTag(id='scotland-census',
    			name="Scotland's Census Data Warehouse by National Records of Scotland",
                type='source',
    			description="`Scotland Census <http://www.scotlandscensus.gov.uk/`_")]

class DownloadScotlandLocal(DownloadUnzipTask):

    URL = 'http://www.scotlandscensus.gov.uk/ods-web/download/getDownloadFile.html?downloadFileIds=Output%20Area%20blk'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(output=self.output().path,
                                                  url=self.URL))
