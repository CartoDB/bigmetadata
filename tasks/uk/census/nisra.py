# https://www.nisra.gov.uk/statistics/2011-census/results/key-statistics
# https://www.nisra.gov.uk/statistics/2011-census/results/quick-statistics

import os

from luigi import Parameter

from tasks.util import TagsTask, shell, DownloadUnzipTask, TempTableTask
from tasks.meta import OBSTag, current_session

from .metadata import parse_table


class SourceTags(TagsTask):
    def version(self):
        return 1

    def tags(self):
        return [OBSTag(id='nisra',
                name="Northern Ireland Statistics and Research Agency",
                type='source',
                description="`Northern Ireland Statistics and Research Agency <https://www.nisra.gov.uk/`_")]


class DownloadNISRAKeyStats(DownloadUnzipTask):
    URL = 'http://www.ninis2.nisra.gov.uk/Download/Census%202011/Key%20Statistics%20Tables%20(statistical%20geographies).zip'

    def download(self):
        shell('wget -O {output}.zip "{url}"'.format(output=self.output().path, url=self.URL))


class DownloadNISRAQuickStats(DownloadUnzipTask):
    URL = 'http://www.ninis2.nisra.gov.uk/Download/Census%202011/Quick%20Statistics%20Tables%20(statistical%20geographies).zip'

    def download(self):
        shell('wget -O {output}.zip "{url}"'.format(output=self.output().path, url=self.URL))


class ImportNISRA(TempTableTask):
    table = Parameter()

    def requires(self):
        kind, place = parse_table(self.table)
        assert place == 'NI'
        return DownloadNISRAKeyStats() if kind == 'KS' else DownloadNISRAQuickStats()

    def run(self):
        infile = os.path.join(self.input().path, 'SMALL AREAS', self.table + 'DATA0.CSV')
        headers = shell('head -n 1 "{csv}"'.format(csv=infile))
        cols = ['{} NUMERIC'.format(h) for h in headers.split(',')[1:]]

        session = current_session()
        session.execute('CREATE TABLE {output} (GeographyCode TEXT, {cols})'.format(
            output=self.output().table,
            cols=', '.join(cols)
        ))
        session.commit()
        shell("cat '{infile}' | psql -c 'COPY {output} FROM STDIN WITH CSV HEADER'".format(
            output=self.output().table,
            infile=infile,
        ))
        session.execute('ALTER TABLE {output} ADD PRIMARY KEY (geographycode)'.format(
            output=self.output().table
        ))
