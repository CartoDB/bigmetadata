# http://www.scotlandscensus.gov.uk/ods-web/data-warehouse.html#bulkdatatab

import csv
import io
import os
import re

from luigi import Parameter

from lib.normalization import CSVNormalizerStream
from tasks.util import shell, DownloadUnzipTask, TempTableTask
from tasks.meta import current_session


class DownloadScotlandLocal(DownloadUnzipTask):

    URL = 'http://www.scotlandscensus.gov.uk/ods-web/download/getDownloadFile.html?downloadFileIds=Output%20Area%20blk'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(output=self.output().path, url=self.URL))


class NormalizerStream(io.IOBase):
    def __init__(self, infile):
        self._csvreader = csv.reader(infile)
        self._buffer = ''

    def read(self, nbytes):
        try:
            while len(self._buffer) < nbytes:
                self.getline()
            out, self._buffer = self._buffer[:nbytes], self._buffer[nbytes:]
            return out
        except StopIteration:
            out = self._buffer
            self._buffer = ''
            return out

    def getline(self):
        line = self._csvreader.next()
        clean_line = ['0' if f == '-' else f.replace(',', '') for f in line]
        self._buffer += (','.join(clean_line) + ' \n')


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
