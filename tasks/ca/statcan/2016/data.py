from luigi import Task, Parameter, LocalTarget, WrapperTask
from tasks.ca.statcan.geo import (GEOGRAPHIES, GeographyColumns, Geography,
                                  GEO_CT, GEO_PR, GEO_CD, GEO_CSD, GEO_CMA, GEO_DA, GEO_FSA)
from tasks.base_tasks import DownloadUnzipTask, RepoFile
from tasks.util import copyfile

GEOGRAPHY_CODES = {
    GEO_PR: '044',
    GEO_CD: '044',
    GEO_CSD: '044',
    GEO_DA: '044',
    GEO_CMA: '043',
    GEO_CT: '043',
    GEO_FSA: '046',
}

URL = 'https://www12.statcan.gc.ca/census-recensement/2016/dp-pd/prof/details/download-telecharger/comp/GetFile.cfm?Lang=E&TYPE=CSV&GEONO={geo_code}'


class DownloadData(DownloadUnzipTask):
    geocode = Parameter()

    def version(self):
        return 1

    def requires(self):
        return RepoFile(resource_id=self.task_id,
                        version=self.version(),
                        url=URL.format(geo_code=self.geocode))

    def download(self):
        copyfile(self.input().path, '{output}.zip'.format(output=self.output().path))


class ImportData(WrapperTask):
    resolution = Parameter()

    def requires(self):
        return DownloadData(geocode=GEOGRAPHY_CODES[self.resolution])
