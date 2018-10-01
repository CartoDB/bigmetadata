import glob
import os
import csv
from luigi import Parameter, WrapperTask
from tasks.base_tasks import RepoFileUnzipTask, CSV2TempTableTask
from lib.logger import get_logger

LOGGER = get_logger(__name__)

URL = 'http://www.censusdata.abs.gov.au/CensusOutput/copsubdatapacks.nsf/All%20docs%20by%20catNo/{year}_GCP_{resolution}_for_{state}/$File/{year}_GCP_{resolution}_for_{state}_short-header.zip'

STATES = (
    # 'AUST',
    'NSW',
    'Vic',
    'Qld',
    'SA',
    'WA',
    'Tas',
    'NT',
    'ACT',
    'OT',
)

TABLES = ['G01', 'G02', 'G03', 'G04A', 'G04B', 'G05', 'G06', 'G07', 'G08',
          'G09A', 'G09B', 'G09C', 'G09D', 'G09E', 'G09F', 'G09G', 'G09H', 'G10A', 'G10B', 'G10C',
          'G11A', 'G11B', 'G11C', 'G12A', 'G12B', 'G13A', 'G13B', 'G13C', 'G14', 'G15', 'G16A', 'G16B',
          'G17A', 'G17B', 'G17C', 'G18', 'G19', 'G20A', 'G20B', 'G21', 'G22A', 'G22B', 'G23A', 'G23B', 'G24', 'G25',
          'G26', 'G27', 'G28', 'G29', 'G30', 'G31', 'G32', 'G33', 'G34', 'G35', 'G36', 'G37', 'G38', 'G39', 'G40',
          'G41', 'G42', 'G43A', 'G43B', 'G44A', 'G44B', 'G44C', 'G44D', 'G44E', 'G44F', 'G45A', 'G45B', 'G46A', 'G46B',
          'G47A', 'G47B', 'G47C', 'G48A', 'G48B', 'G48C', 'G49A', 'G49B', 'G49C', 'G50A', 'G50B', 'G50C',
          'G51A', 'G51B', 'G51C', 'G51D', 'G52A', 'G52B', 'G52C', 'G52D', 'G53A', 'G53B', 'G54A', 'G54B',
          'G55A', 'G55B', 'G56A', 'G56B', 'G57A', 'G57B', 'G58A', 'G58B', 'G59',]


class DownloadData(RepoFileUnzipTask):
    year = Parameter()
    resolution = Parameter()
    state = Parameter()

    def get_url(self):
        return URL.format(year=self.year,
                          resolution=self.resolution,
                          state=self.state)


class ImportData(CSV2TempTableTask):
    tablename = Parameter()
    year = Parameter()
    resolution = Parameter()
    state = Parameter()

    def requires(self):
        return DownloadData(resolution=self.resolution, state=self.state, year=self.year)

    def input_csv(self):
        return glob.glob(os.path.join(self.input().path, '**',
                                      '{year}Census_{tablename}_{state}_{resolution}.csv'.format(
                                        path=self.input().path,
                                        year=self.year,
                                        tablename=self.tablename,
                                        state=self.state.upper(),
                                        resolution=self.resolution,)))[0]

    def coldef(self):
        with open('{csv}'.format(csv=self.input_csv()), 'r', encoding=self.encoding) as f:
            header_row = next(csv.reader(f, delimiter=self.delimiter))
        return [(h.strip(), 'Text') for h in header_row]


class ImportAllTables(WrapperTask):
    year = Parameter()
    resolution = Parameter()
    state = Parameter()

    def requires(self):
        for table in TABLES:
            yield ImportData(resolution=self.resolution, state=self.state, year=self.year, tablename=table)


class ImportAllStates(WrapperTask):
    year = Parameter()
    resolution = Parameter()

    def requires(self):
        for state in STATES:
            yield ImportAllTables(resolution=self.resolution, state=state, year=self.year)

