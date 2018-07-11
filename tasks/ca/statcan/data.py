import os
import urllib.request

from abc import ABCMeta
from luigi import Task, Parameter, WrapperTask, LocalTarget
from collections import OrderedDict

from lib.logger import get_logger
from lib.timespan import get_timespan

from tasks.base_tasks import DownloadUnzipTask, TableTask, TempTableTask, MetaWrapper, RepoFile
from tasks.util import shell, classpath, copyfile
from tasks.meta import current_session, GEOM_REF
from tasks.ca.statcan.geo import (
    GEO_CT, GEO_PR, GEO_CD, GEO_CSD, GEO_CMA, GEO_DA,
    GEOGRAPHY_CODES, GEOGRAPHIES, GeographyColumns, Geography)
from tasks.ca.statcan.util import StatCanParser
from tasks.ca.statcan.cols_census import CensusColumns
from tasks.ca.statcan.cols_nhs import NHSColumns

LOGGER = get_logger(__name__)


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


class BaseParams(metaclass=ABCMeta):
    resolution = Parameter(default=GEO_PR)
    survey = Parameter(default=SURVEY_CEN)


class DownloadData(BaseParams, DownloadUnzipTask):
    def version(self):
        return 1

    def requires(self):
        return RepoFile(resource_id=self.task_id,
                        version=self.version(),
                        url=URL.format(survey_url=SURVEY_URLS[self.survey],
                                       survey_code=SURVEY_CODES[self.survey],
                                       geo_code=GEOGRAPHY_CODES[self.resolution]))

    def download(self):
        copyfile(self.input().path, '{output}.zip'.format(output=self.output().path))


class SplitAndTransposeData(BaseParams, Task):
    IGNORED_FILE_SUFFIXES = ('-DQ',)
    DIVISION_SPLITTED = {
        SURVEY_CEN: {
            GEO_CT: None,
            GEO_PR: None,
            GEO_CD: None,
            GEO_CSD: None,
            GEO_CMA: ('cmaca_name', (r'part\)$',)),
            GEO_DA: None,
        },
        SURVEY_NHS: {
            GEO_CT: None,
            GEO_PR: None,
            GEO_CD: None,
            GEO_CSD: None,
            GEO_CMA: ('cma_ca_name', (r'part\)$',))
        }
    }

    def requires(self):
        return DownloadData(resolution=self.resolution, survey=self.survey)

    def run(self):
        infiles = shell('ls {input}/{survey_code}-{geo_code}*.[cC][sS][vV]'.format(
            input=self.input().path,
            survey_code=SURVEY_CODES[self.survey],
            geo_code=GEOGRAPHY_CODES[self.resolution]
        ))
        in_csv_files = []
        for in_csv_file in infiles.strip().split('\n'):
            if not self._is_ignored_suffix(in_csv_file):
                in_csv_files.append(in_csv_file)
            else:
                LOGGER.warning('Ignoring file %s' % in_csv_file)
        os.makedirs(self.output().path)
        StatCanParser(self.DIVISION_SPLITTED[self.survey][self.resolution]).parse_csv_to_files(in_csv_files, self.output().path)

    def _is_ignored_suffix(self, csv_file):
        if os.path.splitext(csv_file)[0].endswith(self.IGNORED_FILE_SUFFIXES):
            return True
        return False

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))


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


class InterpolateNHSDAFromCD(Task):
    topic = Parameter()
    resolution = Parameter()

    def requires(self):
        return {
            'nhs': NHS(resolution=GEO_CD, topic=self.topic, survey=SURVEY_NHS),
            'geo_cd': Geography(resolution='GEO_CD'),
            'geo_da': Geography(resolution='GEO_DA')
        }

class Survey(BaseParams, TableTask):

    topic = Parameter(default='t001')

    def version(self):
        return 6

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
        for colname, coltarget in input_['meta'].items():
            if coltarget._id.split('.')[-1].lower().startswith(self.topic.lower()):
                cols[colname] = coltarget
        return cols

    def populate(self):
        if self.survey == SURVEY_NHS:
            if self.resolution == GEO_DA:
                self.populate_da_from_cd()
            else:
                self.populate_general()
        else:
            self.populate_general()

    def populate_da_from_cd(self):
        session = current_session()
        columns = self.columns()
        colnames = list(columns.keys())
        out_colnames = [oc for oc in colnames if oc is not None]
        in_colnames = ['da.geom_id']
        for colname in out_colnames:
            if colname != 'geo_code':
                # We reduce the number of decimals to reduce the size of the row to avoid hit
                # the limit which is 8Kb. More info https://github.com/CartoDB/bigmetadata/issues/527
                in_colnames.append('round(cast(float8 ({colname} * (ST_Area(da.the_geom)/ST_Area(cd.the_geom))) as numeric), 2) {colname}'.format(colname=colname))

        insert_query = '''
                INSERT INTO {output} ({out_colnames})
                SELECT {in_colnames} FROM {da_geom} da
                INNER JOIN {cd_geom} cd ON (cd.geom_id = left(da.geom_id,4))
                INNER JOIN {cd_data} data ON (cd.geom_id = data.geo_code)
                '''.format(output=self.output().table,
                           da_geom=self.input()['geo'].table,
                           cd_geom=self.input()['geo_cd'].table,
                           cd_data=self.input()['data_cd'].table,
                           in_colnames=', '.join(in_colnames),
                           out_colnames=', '.join(out_colnames))

        LOGGER.debug(insert_query)
        session.execute(insert_query)

    def populate_general(self):
        session = current_session()
        columns = self.columns()
        out_colnames = list(columns.keys())
        in_table = self.input()['data']
        in_colnames = [ct._id.split('.')[-1] for ct in list(columns.values())]
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
            'meta': CensusColumns(resolution=self.resolution, survey=self.survey, topic=self.topic),
        }

    def targets(self):
        return {
            self.input()['geo'].obs_table: GEOM_REF,
        }

    def table_timespan(self):
        return get_timespan('2011')


class AllCensusTopics(BaseParams, WrapperTask):
    def requires(self):
        topic_range = list(range(1, 11))   # 1-10

        for resolution in (GEO_CT, GEO_PR, GEO_CD, GEO_CSD, GEO_CMA, GEO_DA):
            for count in topic_range:
                topic = 't{:03d}'.format(count)
                yield Census(resolution=resolution, survey=SURVEY_CEN, topic=topic)


class NHS(Survey):

    def requires(self):
        requires = {
            'geo': Geography(resolution=self.resolution),
            'geometa': GeographyColumns(resolution=self.resolution),
            'meta': NHSColumns(),
        }
        # DA interpolate data and there is no data for DA in NHS so we should
        # avoid this step for DA resolution
        if self.resolution == GEO_DA:
            requires['geo_cd'] = Geography(resolution=GEO_CD)
            requires['data_cd'] = NHS(resolution=GEO_CD, survey=self.survey, topic=self.topic)
        else:
            requires['data'] = CopyDataToTable(resolution=self.resolution, survey=SURVEY_NHS, topic=self.topic)

        return requires

    def targets(self):
        return {
            self.input()['geo'].obs_table: GEOM_REF,
        }

    def table_timespan(self):
        return get_timespan('2011')


class AllNHSTopics(BaseParams, WrapperTask):
    def requires(self):
        topic_range = list(range(1, 30))   # 1-29

        for resolution in (GEO_CT, GEO_PR, GEO_CD, GEO_CSD, GEO_CMA, GEO_DA):
            for count in topic_range:
                topic = 't{:03d}'.format(count)
                yield NHS(resolution=resolution, survey=SURVEY_NHS, topic=topic)


class CensusMetaWrapper(MetaWrapper):
    resolution = Parameter()
    topic = Parameter()

    params = {
        'topic': ['t{:03d}'.format(i) for i in range(1,11)],
        'resolution': (GEO_CT, GEO_PR, GEO_CD, GEO_CSD, GEO_CMA, GEO_DA)
    }

    def tables(self):
        yield Geography(resolution=self.resolution)
        yield Census(resolution=self.resolution, topic=self.topic, survey=SURVEY_CEN)


class NHSMetaWrapper(MetaWrapper):
    resolution = Parameter()
    topic = Parameter()

    params = {
        'topic': ['t{:03d}'.format(i) for i in range(1,30)],
        'resolution': (GEO_CT, GEO_PR, GEO_CD, GEO_CSD, GEO_CMA) # NHS not available at Dissemination Area level
    }

    def tables(self):
        yield Geography(resolution=self.resolution)
        yield NHS(resolution=self.resolution, topic=self.topic, survey=SURVEY_NHS)
