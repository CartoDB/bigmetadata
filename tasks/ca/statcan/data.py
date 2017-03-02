import os
import urllib

from abc import ABCMeta
from luigi import Task, Parameter, WrapperTask, LocalTarget
from collections import OrderedDict
from tasks.util import DownloadUnzipTask, shell, TableTask, TempTableTask, classpath, MetaWrapper
from tasks.meta import current_session
from tasks.ca.statcan.geo import (
    GEO_CT, GEO_PR, GEO_CD, GEO_CSD, GEO_CMA,
    GEOGRAPHY_CODES, GEOGRAPHIES, GeographyColumns, Geography)
from tasks.ca.statcan.util import StatCanParser
from tasks.ca.statcan.cols_census import CensusColumns
from tasks.ca.statcan.cols_nhs import NHSColumns


SURVEY_CEN = 'census'
SURVEY_NHS = 'nhs'

SURVEYS = (
    SURVEY_CEN,
    SURVEY_NHS,
)

SURVEY_CODES = {
    SURVEY_CEN: '98-316-XWE2011001',
    SURVEY_NHS: '99-004-XWE2011001',
}

SURVEY_URLS = {
    SURVEY_CEN: 'census-recensement',
    SURVEY_NHS: 'nhs-enm',
}

URL = 'http://www12.statcan.gc.ca/{survey_url}/2011/dp-pd/prof/details/download-telecharger/comprehensive/comp_download.cfm?CTLG={survey_code}&FMT=CSV{geo_code}'


class BaseParams:
    __metaclass__ = ABCMeta

    resolution = Parameter(default=GEO_PR)
    survey = Parameter(default=SURVEY_CEN)


class DownloadData(BaseParams, DownloadUnzipTask):
    def download(self):
        urllib.urlretrieve(url=URL.format(
                           survey_url=SURVEY_URLS[self.survey],
                           survey_code=SURVEY_CODES[self.survey],
                           geo_code=GEOGRAPHY_CODES[self.resolution],
                           ),
                           filename=self.output().path + '.zip')


class SplitAndTransposeData(BaseParams, Task):
    def requires(self):
        return DownloadData(resolution=self.resolution, survey=self.survey)

    def run(self):
        infiles = shell('ls {input}/{survey_code}-{geo_code}*.[cC][sS][vV]'.format(
            input=self.input().path,
            survey_code=SURVEY_CODES[self.survey],
            geo_code=GEOGRAPHY_CODES[self.resolution]
        ))
        in_csv_files = infiles.strip().split('\n')
        os.makedirs(self.output().path)
        StatCanParser().parse_csv_to_files(in_csv_files, self.output().path)

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))


#####################################
# IMPORT TO TEMP TABLES
#####################################
class CopyDataToTable(BaseParams, TempTableTask):

    topic = Parameter()

    def requires(self):
        return SplitAndTransposeData(resolution=self.resolution, survey=self.survey)

    def run(self):
        infile = os.path.join(self.input().path, self.topic + '.csv')
        headers = shell('head -n 1 {csv}'.format(csv=infile))
        cols = ['{} NUMERIC'.format(h) for h in headers.split(',')[1:]]

        session = current_session()
        session.execute('CREATE TABLE {output} (Geo_Code TEXT, {cols})'.format(
            output=self.output().table,
            cols=', '.join(cols)
        ))
        session.commit()
        shell("cat '{infile}' | psql -c 'COPY {output} FROM STDIN WITH CSV HEADER'".format(
            output=self.output().table,
            infile=infile,
        ))
        session.execute('ALTER TABLE {output} ADD PRIMARY KEY (geo_code)'.format(
            output=self.output().table
        ))


class ImportData(BaseParams, Task):
    def requires(self):
        return SplitAndTransposeData(resolution=self.resolution, survey=self.survey)

    def run(self):
        infiles = shell('ls {input}/*.csv'.format(
            input=self.input().path))
        fhandle = self.output().open('w')
        for infile in infiles.strip().split('\n'):
            topic = os.path.split(infile)[-1].split('.csv')[0]
            data = yield CopyDataToTable(resolution=self.resolution, survey=self.survey, topic=topic)
            fhandle.write('{table}\n'.format(table=data.table))
        fhandle.close()

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))


class ImportAllResolutions(WrapperTask):
    survey = Parameter(default=SURVEY_CEN)

    def requires(self):
        for resolution in GEOGRAPHIES:
            yield ImportData(resolution=resolution, survey=self.survey)


class ImportAllSurveys(WrapperTask):
    resolution = Parameter(default=GEO_PR)

    def requires(self):
        for survey in SURVEYS:
            yield ImportData(resolution=self.resolution, survey=survey)


class ImportAll(WrapperTask):
    def requires(self):
        for survey in SURVEYS:
            for resolution in GEOGRAPHIES:
                yield ImportData(resolution=resolution, survey=survey)


#####################################
# COPY TO OBSERVATORY
#####################################
class Survey(BaseParams, TableTask):

    topic = Parameter(default='t001')

    def requires(self):
        '''
        Subclasses must override this.
        '''
        raise NotImplementedError('Survey must define requires()')

    def timespan(self):
        '''
        Subclasses must override this.
        '''
        raise NotImplementedError('Survey must define timespan()')

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()
        cols['geo_code'] = input_['geometa']['geom_id']
        for colname, coltarget in input_['meta'].iteritems():
            if coltarget._id.split('.')[-1].lower().startswith(self.topic.lower()):
                cols[colname] = coltarget
        return cols

    def populate(self):
        session = current_session()
        columns = self.columns()
        out_colnames = columns.keys()
        in_table = self.input()['data']
        in_colnames = [ct._id.split('.')[-1] for ct in columns.values()]
        in_colnames[0] = 'geo_code'
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


class Census(Survey):
    def requires(self):
        return {
            'data': CopyDataToTable(resolution=self.resolution, survey=SURVEY_CEN, topic=self.topic),
            'geo': Geography(resolution=self.resolution),
            'geometa': GeographyColumns(resolution=self.resolution),
            'meta': CensusColumns(),
        }

    def timespan(self):
        return 2011


class AllCensusTopics(BaseParams, WrapperTask):
    def requires(self):
        topic_range = range(1, 11)   # 1-10

        for resolution in (GEO_CT, GEO_PR, GEO_CD, GEO_CSD, GEO_CMA):
            for count in topic_range:
                topic = 't{:03d}'.format(count)
                yield Census(resolution=resolution, survey=SURVEY_CEN, topic=topic)


class NHS(Survey):
    def requires(self):
        return {
            'data': CopyDataToTable(resolution=self.resolution, survey=SURVEY_NHS, topic=self.topic),
            'geo': Geography(resolution=self.resolution),
            'geometa': GeographyColumns(resolution=self.resolution),
            'meta': NHSColumns(),
        }

    def timespan(self):
        return 2011


class AllNHSTopics(BaseParams, WrapperTask):
    def requires(self):
        topic_range = range(1, 30)   # 1-29

        for resolution in (GEO_CT, GEO_PR, GEO_CD, GEO_CSD, GEO_CMA):
            for count in topic_range:
                topic = 't{:03d}'.format(count)
                yield NHS(resolution=resolution, survey=SURVEY_NHS, topic=topic)


class CensusMetaWrapper(MetaWrapper):
    resolution = Parameter()
    topic = Parameter()

    params = {
        'topic': ['t{:03d}'.format(i) for i in range(1,11)],
        'resolution': (GEO_CT, GEO_PR, GEO_CD, GEO_CSD, GEO_CMA)
    }

    def tables(self):
        yield Geography(resolution=self.resolution)
        yield Census(resolution=self.resolution, topic=self.topic, survey=SURVEY_CEN)

class NHSMetaWrapper(MetaWrapper):
    resolution = Parameter()
    topic = Parameter()

    params = {
        'topic': ['t{:03d}'.format(i) for i in range(1,30)],
        'resolution': (GEO_CT, GEO_PR, GEO_CD, GEO_CSD, GEO_CMA)
    }

    def tables(self):
        yield Geography(resolution=self.resolution)
        yield NHS(resolution=self.resolution, topic=self.topic, survey=SURVEY_NHS)
