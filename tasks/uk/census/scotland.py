# http://www.scotlandscensus.gov.uk/ods-web/data-warehouse.html#bulkdatatab

import csv
import os
import re
import urllib

from luigi import Parameter

from lib.normalization import CSVNormalizerStream
from tasks.util import DownloadUnzipTask, TempTableTask
from tasks.meta import current_session


class DownloadScotlandLocal(DownloadUnzipTask):
    URL = 'http://www.scotlandscensus.gov.uk/ods-web/download/getDownloadFile.html?downloadFileIds=Output%20Area%20blk'

    def download(self):
        urllib.retrieve(self.URL, '{}.zip'.format(self.output().path))


class ImportScotland(TempTableTask):
    table = Parameter()

    def requires(self):
        return DownloadScotlandLocal()

    @staticmethod
    def id_to_column(colid):
        return re.sub(r'[:/, \-\.\(\)]', '_', '_'.join(colid.split(';')[0].split(':')[-2:]))

    def run(self):
        infile = os.path.join(self.input().path, self.table + '.csv')
        with open(infile) as csvfile:
            reader = csv.reader(csvfile)
            header = reader.next()

            datacols = [ImportScotland.id_to_column(c) for c in header[1:]]

        with current_session().connection().connection.cursor() as cursor, open(infile) as csvfile:
            cursor.execute('CREATE TABLE {output} (geographycode TEXT PRIMARY KEY, {cols})'.format(
                output=self.output().table,
                cols=', '.join(['{} NUMERIC'.format(c) for c in datacols])
            ))

            cursor.copy_expert(
                'COPY {table} (geographycode, {cols}) FROM stdin WITH (FORMAT CSV, HEADER)'.format(
                    cols=', '.join(datacols),
                    table=self.output().table),
                CSVNormalizerStream(csvfile, lambda row: ['0' if f == '-' else f.replace(',', '') for f in row]))
