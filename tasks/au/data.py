import os
import urllib

from abc import ABCMeta
from luigi import Task, Parameter, WrapperTask, LocalTarget
from collections import OrderedDict
from tasks.util import DownloadUnzipTask, shell, TableTask, TempTableTask, classpath, CSV2TempTableTask
from tasks.meta import current_session
from tasks.au.geo import (
    GEO_STE,
    GEOGRAPHIES, GeographyColumns, Geography)
from tasks.au.cols_bcp import BCPColumns


PROFILE_BCP = 'BCP'

PROFILES = (
    PROFILE_BCP,
)

STATE_AUST = 'AUST'

STATES = (
    STATE_AUST
)


URL = 'http://www.censusdata.abs.gov.au/CensusOutput/copsubdatapacks.nsf/All%20docs%20by%20catNo/{year}_{profile}_{resolution}_for_{state}/$File/{year}_{profile}_{resolution}_for_{state}_{header}-header.zip'


class BaseParams:
    __metaclass__ = ABCMeta

    year = Parameter(default='2011')
    profile = Parameter(default=PROFILE_BCP)
    resolution = Parameter(default=GEO_STE)
    state = Parameter(default=STATE_AUST)


class DownloadData(BaseParams, DownloadUnzipTask):
    def download(self):
        urllib.urlretrieve(url=URL.format(
                               year=self.year,
                               profile=self.profile,
                               resolution=self.resolution,
                               state=self.state,
                               header='short'
                           ),
                           filename=self.output().path + '.zip')


class CopyFiles(BaseParams, Task):
    def requires(self):
        return DownloadData(resolution=self.resolution, profile=self.profile)

    def run(self):
        shell('mkdir {output}'.format(
            output=self.output().path
        ))

        # copy the csvs to a path with no spaces so we can use them in the CSV2TempTableTask
        cmd = 'find '+self.input().path+' -name \'*.csv\' -print0 | xargs -0 -I {} cp {} '+self.output().path
        shell(cmd)

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))


class ImportCSV(BaseParams, CSV2TempTableTask):

    infilepath = Parameter()

    def requires(self):
        return DownloadData(resolution=self.resolution, profile=self.profile)

    def input_csv(self):
        return self.infilepath


class ImportData(BaseParams, Task):
    def requires(self):
        return CopyFiles(resolution=self.resolution, profile=self.profile)

    def run(self):
        infiles = shell('ls {input}/*.csv'.format(
            input=self.input().path
        ))

        file_list = infiles.strip().split('\n')
        fhandle = self.output().open('w')
        for infile in file_list:
            data = yield ImportCSV(resolution=self.resolution, profile=self.profile, infilepath=infile)
            fhandle.write('{table}\n'.format(table=data.table))
        fhandle.close()

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))




#####################################
# COPY TO OBSERVATORY
#####################################
class BCP(BaseParams, TableTask):

    def requires(self):
        return {
            'data': ImportData(resolution=self.resolution, profile=PROFILE_BCP),
            # 'geo': Geography(resolution=self.resolution),
            'geometa': GeographyColumns(resolution=self.resolution),
            'meta': BCPColumns(),
        }

    def timespan(self):
        return unicode(self.year)

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()
        cols['region_id'] = input_['geometa']['geom_id']
        for colname, coltarget in input_['meta'].iteritems():
            # if coltarget._id.split('.')[-1].lower().startswith(self.topic.lower()):
            cols[colname] = coltarget
        return cols

    def populate(self):
        session = current_session()
        columns = self.columns()
        out_colnames = columns.keys()
        in_table = self.input()['data']
        in_colnames = [ct._id.split('.')[-1] for ct in columns.values()]
        in_colnames[0] = 'region_id'
        for i, in_c in enumerate(in_colnames):
            cmd =   "SELECT 'exists' FROM information_schema.columns " \
                    "WHERE table_schema = '{schema}' " \
                    "  AND table_name = '{tablename}' " \
                    "  AND column_name = '{colname}' " \
                    "  LIMIT 1".format(
                        schema=in_table.schema,
                        tablename=in_table.tablename.lower(),
                        colname=in_c.lower())
            # remove columns that aren't in input table
            if session.execute(cmd).fetchone() is None:
                in_colnames[i] = None
                out_colnames[i] = None
        in_colnames = [
            "CASE {ic}::TEXT WHEN '-6' THEN NULL ELSE {ic} END".format(ic=ic) for ic in in_colnames if ic is not None]
        out_colnames = [oc for oc in out_colnames if oc is not None]

        cmd =   'INSERT INTO {output} ({out_colnames}) ' \
                'SELECT {in_colnames} FROM {input} '.format(
                    output=self.output().table,
                    input=in_table.table,
                    in_colnames=', '.join(in_colnames),
                    out_colnames=', '.join(out_colnames))
        session.execute(cmd)


class AllBCPResolutions(BaseParams, WrapperTask):

    def requires(self):
        for resolution in GEOGRAPHIES:
            yield BCP(resolution=resolution, profile=PROFILE_BCP)
