'''
tasks to download LODES origin-destination, residence, and workplace
characteristics files
'''

import os
import subprocess

from tasks.util import shell, DefaultPostgresTarget, pg_cursor, classpath
from luigi import Task, Parameter, LocalTarget, BooleanParameter



class WorkplaceAreaCharacteristics(Task):

    year = Parameter()
    state = Parameter()


class ResidenceAreaCharacteristics(Task):

    year = Parameter()
    state = Parameter()




class DownloadOriginDestination(Task):

    # [YEAR] = Year of job data. Can have the value of 2002-2013 for most states.
    year = Parameter()

    # [ST] =   lowercase, 2-letter postal code for a chosen state
    state = Parameter()

    # [TYPE] = Job Type, can have a value of "JT00" for All Jobs, "JT01" for Primary
    #          Jobs, "JT02" for All Private Jobs, "JT03" for Private Primary Jobs,
    #          "JT04" for All Federal Jobs, or "JT05" for Federal Primary Jobs.
    job_type = Parameter(default="JT00")

    # [PART] = Part of the state file, can have a value of either "main" or "aux".
    #          Complimentary parts of the state file, the main part includes jobs with both
    #          workplace and residence in the state and the aux part includes jobs with the
    #          workplace in the state and the residence outside of the state.
    part = Parameter()

    def filename(self):
        # Filename of the OD datasets are described by the following
        #          templates [ST]_od_[PART]_[TYPE]_[YEAR].csv.gz   where
        return '{}_od_{}_{}_{}.csv.gz'.format(self.state, self.part,
                                              self.job_type, self.year)

    def url(self):
        return 'http://lehd.ces.census.gov/data/lodes/LODES7/{}/od/{}'.format(
            self.state, self.filename())

    def run(self):
        self.output().makedirs()
        try:
            shell('wget {url} -O {target}'.format(url=self.url(), target=self.output().path))
        except subprocess.CalledProcessError:
            shell('rm -f {target}'.format(target=self.output().path))

    def output(self):
        return LocalTarget(path=os.path.join(classpath(self), self.filename()))
        #return DefaultPostgresTarget(self.filename())


class OriginDestination(Task):

    force = BooleanParameter(default=False)
    year = Parameter(default=2013)

    STATES = set(["al", "ak", "az", "ar", "ca", "co", "ct", "de", "dc", "fl", "ga",
                  "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md", "ma",
                  "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny",
                  "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd", "tn",
                  "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy"])

    MISSING_STATES = {
        2013: set(['ks', 'ma', 'pr'])
    }

    def tablename(self):
        return '"{}".od_{}'.format(classpath(self), self.year)

    def columns(self):
        return '''
work_census_block Text, -- Workplace Census Block Code
home_census_block Text, --   Residence Census Block Code
total_jobs INTEGER, -- Total number of jobs
jobs_age_29_or_younger INTEGER, -- Number of jobs of workers age 29 or younger 11
jobs_age_30_to_54 INTEGER, -- Number of jobs for workers age 30 to 54 11
jobs_age_55_or_older INTEGER, -- Number of jobs for workers age 55 or older 11
jobs_earning_15000_or_less INTEGER, --  Number of jobs with earnings $1250/month or less
jobs_earning_15001_to_40000 INTEGER, --  Number of jobs with earnings $1251/month to $3333/month
jobs_earning_40001_or_more INTEGER, --  Number of jobs with earnings greater than $3333/month
jobs_in_goods_production INTEGER, --   Number of jobs in Goods Producing industry sectors
jobs_in_trade_transport_and_util INTEGER, --   Number of jobs in Trade, Tr sportation, and Utilities industry sectors
jobs_in_all_other_service INTEGER, --   Number of jobs in All Other Services industry sectors
createdate DATE -- Date on which da ta was created, formatted as YYYYMMDD
'''

    def requires(self):
        for state in self.STATES - self.MISSING_STATES.get(self.year, set()):
            for part in ('main', 'aux',):
                yield DownloadOriginDestination(year=self.year, state=state, part=part)

    def run(self):
        # make the table
        cursor = pg_cursor()
        cursor.execute('CREATE SCHEMA IF NOT EXISTS "{schema}"'.format(
            schema=classpath(self)))
        cursor.execute('''
DROP TABLE IF EXISTS {tablename};
CREATE TABLE {tablename} (
    {columns}
)
                       '''.format(tablename=self.tablename(),
                                  columns=self.columns()))

        cursor.connection.commit()

        for infile in self.input():
            # gunzip each CSV into the table
            cmd = r"gunzip -c '{input}' | psql -c '\copy {tablename} FROM STDIN " \
                  r"WITH CSV HEADER'".format(input=infile.path, tablename=self.tablename())
            shell(cmd)

        self.output().touch()

    def output(self):
        output = DefaultPostgresTarget(table=self.tablename())
        if self.force:
            output.untouch()
        return output
