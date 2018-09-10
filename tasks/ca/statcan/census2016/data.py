from luigi import Task, Parameter, LocalTarget, WrapperTask
from tasks.ca.statcan.geo import (GEOGRAPHIES, GeographyColumns, Geography,
                                  GEO_CT, GEO_PR, GEO_CD, GEO_CSD, GEO_CMA, GEO_DA, GEO_FSA)
from tasks.ca.statcan.census2016.cols_census import CensusColumns

from tasks.base_tasks import DownloadUnzipTask, RepoFile, TempTableTask
from tasks.util import copyfile
from tasks.meta import current_session
from lib.logger import get_logger

LOGGER = get_logger(__name__)

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


class ImportData(TempTableTask):
    resolution = Parameter()
    table = Parameter()

    def requires(self):
        return {
            'data': DownloadData(geocode=GEOGRAPHY_CODES[self.resolution]),
            'columns': CensusColumns(table=self.table),
        }

    def run(self):
        _input = self.input()

        columns = ['{} NUMERIC'.format(col) for col in _input['columns'].keys()]
        session = current_session()
        query = '''
                CREATE TABLE {output} (
                    geom_id TEXT,
                    {columns}
                )
                '''.format(
                    output=self.output().table,
                    columns=','.join(columns),
                )

        LOGGER.debug(query)
        session.execute(query)
