import os

from abc import ABCMeta
from luigi import Task, Parameter, WrapperTask, LocalTarget

from tasks.util import (DownloadUnzipTask, shell, Shp2TempTableTask,
                        ColumnsTask, TableTask, TempTableTask, classpath)
from tasks.meta import GEOM_REF, OBSColumn, current_session
# from tasks.mx.inegi_columns import DemographicColumns
from tasks.tags import SectionTags, SubsectionTags, UnitTags

from collections import OrderedDict
from statcan import GEOGRAPHY_CODES
import urllib


SURVEYS = (
    'census',
    'nhs',
)

SURVEY_CODES = {
    'census': '98-316-XWE2011001',
    'nhs': '99-004-XWE2011001',
}

SURVEY_URLS = {
    'census': 'census-recensement',
    'nhs': 'nhs-enm',
}


class BaseParams:
    __metaclass__ = ABCMeta

    resolution = Parameter(default='pr_')
    survey = Parameter(default='census')


class DownloadData(BaseParams, Task):
    URL = 'http://www12.statcan.gc.ca/{survey_url}/2011/dp-pd/prof/details/download-telecharger/comprehensive/comp_download.cfm?CTLG={survey_code}&FMT=CSV{geo_code}'

    def run(self):
        self.output().makedirs()
        urllib.urlretrieve(url=self.URL.format(
                           survey_url=SURVEY_URLS[self.survey],
                           survey_code=SURVEY_CODES[self.survey],
                           geo_code=GEOGRAPHY_CODES[self.resolution],
                           ),
                           filename=self.output().path)

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self),
                                        SURVEY_CODES[self.survey] + '-' + str(GEOGRAPHY_CODES[self.resolution]) + '.zip'))


class UnzipData(BaseParams, Task):
    def requires(self):
        return DownloadData(resolution=self.resolution, survey=self.survey)

    def run(self):
        cmd = 'unzip -o {input} -d {output_dir}'.format(
            input=self.input().path,
            output_dir=self.input().path.replace('.zip', ''))
        shell(cmd)

    def output(self):
        path, folder = os.path.split(self.input().path)
        folder = folder.replace('.zip', '')
        csv_file = folder + '.CSV'
        return LocalTarget(os.path.join(path, folder, csv_file))
