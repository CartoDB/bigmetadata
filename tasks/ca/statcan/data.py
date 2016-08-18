import os
import urllib

from abc import ABCMeta
from luigi import Task, Parameter, WrapperTask, LocalTarget
from tasks.util import DownloadUnzipTask, shell, TableTask, TempTableTask, classpath,
from tasks.meta import current_session
from tasks.ca.statcan.geo import (
    GEO_CT, GEO_PR, GEO_CD, GEO_CSD, GEO_CMA,
    GEOGRAPHY_CODES, GEOGRAPHIES, GeographyColumns)
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


class CopyDataToTable(BaseParams, TempTableTask):

    table = Parameter()

    def requires(self):
        return SplitAndTransposeData(resolution=self.resolution, survey=self.survey)

    def run(self):
        infile = os.path.join(self.input().path, self.table + '.csv')
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
            table = os.path.split(infile)[-1].split('.csv')[0]
            data = yield CopyDataToTable(resolution=self.resolution, survey=self.survey, table=table)
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


class Survey(BaseParams, TableTask):
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
        cols['Geo_Code'] = input_['geom_columns']['geom_id']
        cols.update(input_['data_columns'])
        return cols

    def populate(self):
        session = current_session()
        cols = self.columns()
        inserted = False
        for line in self.input()['data'].open():
            intable = line.strip()
            table = intable.split('_')[1]
            cols_for_table = OrderedDict([
                (n, t,) for n, t in cols.iteritems() if table in t._id
            ])
            out_colnames = cols_for_table.keys()
            in_colnames = [t._id.split('.')[-1] for t in cols_for_table.values()]
            assert len(out_colnames) == len(in_colnames)
            if not out_colnames:
                continue
            if not inserted:
                stmt =  'INSERT INTO {output} (geo_code, {out_colnames}) ' \
                        'SELECT geo_code, {in_colnames} ' \
                        'FROM {input} '.format(
                            output=self.output().table,
                            out_colnames=', '.join(out_colnames),
                            in_colnames=', '.join(in_colnames),
                            input=intable)
                session.execute(stmt)
                inserted = True
            else:
                stmt =  'UPDATE {output} out ' \
                        'SET {set} ' \
                        'FROM {input} intable ' \
                        'WHERE intable.geo_code = out.geo_code ' \
                        .format(
                            set=', '.join([
                                '{} = {}'.format(a, b) for a, b in zip(
                                    out_colnames, in_colnames)
                            ]),
                            output=self.output().table,
                            input=intable)
                session.execute(stmt)


class Census(Survey):
    def requires(self):
        return {
            'data': ImportData(resolution=self.resolution, survey=SURVEY_CEN),
            'geom_columns': GeographyColumns(resolution=self.resolution),
            'data_columns': CensusColumns(),
        }

    def timespan(self):
        return 2011


class NHS(Survey):
    def requires(self):
        return {
            'data': ImportData(resolution=self.resolution, survey=SURVEY_NHS),
            'geom_columns': GeographyColumns(resolution=self.resolution),
            'data_columns': NHSColumns(),
        }

    def timespan(self):
        return 2011

