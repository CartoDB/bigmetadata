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
# from tasks.ca.statcan.cols_census import CensusColumns
# from tasks.ca.statcan.cols_nhs import NHSColumns


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


# class ImportAllResolutions(WrapperTask):
#     profile = Parameter(default=PROFILE_BCP)

#     def requires(self):
#         for resolution in GEOGRAPHIES:
#             yield ImportData(resolution=resolution, profile=self.profile)


# class ImportAllSurveys(WrapperTask):
#     resolution = Parameter(default=GEO_STE)

#     def requires(self):
#         for profile in PROFILES:
#             yield ImportData(resolution=self.resolution, profile=profile)


# class ImportAll(WrapperTask):
#     def requires(self):
#         for profile in PROFILES:
#             for resolution in GEOGRAPHIES:
#                 yield ImportData(resolution=resolution, profile=profile)


#####################################
# COPY TO OBSERVATORY
#####################################
# class Survey(BaseParams, TableTask):

#     topic = Parameter(default='t001')

#     def requires(self):
#         '''
#         Subclasses must override this.
#         '''
#         raise NotImplementedError('Survey must define requires()')

#     def timespan(self):
#         '''
#         Subclasses must override this.
#         '''
#         raise NotImplementedError('Survey must define timespan()')

#     def columns(self):
#         cols = OrderedDict()
#         input_ = self.input()
#         cols['geo_code'] = input_['geometa']['geom_id']
#         for colname, coltarget in input_['meta'].iteritems():
#             if coltarget._id.split('.')[-1].lower().startswith(self.topic.lower()):
#                 cols[colname] = coltarget
#         return cols

#     def populate(self):
#         session = current_session()
#         columns = self.columns()
#         out_colnames = columns.keys()
#         in_table = self.input()['data']
#         in_colnames = [ct._id.split('.')[-1] for ct in columns.values()]
#         in_colnames[0] = 'geo_code'
#         for i, in_c in enumerate(in_colnames):
#             cmd =   "SELECT 'exists' FROM information_schema.columns " \
#                     "WHERE table_schema = '{schema}' " \
#                     "  AND table_name = '{tablename}' " \
#                     "  AND column_name = '{colname}' " \
#                     "  LIMIT 1".format(
#                         schema=in_table.schema,
#                         tablename=in_table.tablename.lower(),
#                         colname=in_c.lower())
#             # remove columns that aren't in input table
#             if session.execute(cmd).fetchone() is None:
#                 in_colnames[i] = None
#                 out_colnames[i] = None
#         in_colnames = [
#             "CASE {ic}::TEXT WHEN '-6' THEN NULL ELSE {ic} END".format(ic=ic) for ic in in_colnames if ic is not None]
#         out_colnames = [oc for oc in out_colnames if oc is not None]

#         cmd =   'INSERT INTO {output} ({out_colnames}) ' \
#                 'SELECT {in_colnames} FROM {input} '.format(
#                     output=self.output().table,
#                     input=in_table.table,
#                     in_colnames=', '.join(in_colnames),
#                     out_colnames=', '.join(out_colnames))
#         session.execute(cmd)


# class Census(Survey):
#     def requires(self):
#         return {
#             'data': CopyDataToTable(resolution=self.resolution, profile=PROFILE_BCP, topic=self.topic),
#             'geo': Geography(resolution=self.resolution),
#             'geometa': GeographyColumns(resolution=self.resolution),
#             'meta': CensusColumns(),
#         }

#     def timespan(self):
#         return 2011


# class AllCensusTopics(BaseParams, WrapperTask):
#     def requires(self):
#         topic_range = range(1, 11)   # 1-10

#         for resolution in (GEO_CT, GEO_STE, GEO_CD, GEO_CSD, GEO_CMA):
#             for count in topic_range:
#                 topic = 't{:03d}'.format(count)
#                 yield Census(resolution=resolution, profile=PROFILE_BCP, topic=topic)


# class NHS(Survey):
#     def requires(self):
#         return {
#             'data': CopyDataToTable(resolution=self.resolution, profile=PROFILE_NHS, topic=self.topic),
#             'geo': Geography(resolution=self.resolution),
#             'geometa': GeographyColumns(resolution=self.resolution),
#             'meta': NHSColumns(),
#         }

#     def timespan(self):
#         return 2011


# class AllNHSTopics(BaseParams, WrapperTask):
#     def requires(self):
#         topic_range = range(1, 30)   # 1-29

#         for resolution in (GEO_CT, GEO_STE, GEO_CD, GEO_CSD, GEO_CMA):
#             for count in topic_range:
#                 topic = 't{:03d}'.format(count)
#                 yield NHS(resolution=resolution, profile=PROFILE_NHS, topic=topic)
