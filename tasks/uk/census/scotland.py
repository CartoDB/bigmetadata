# http://www.scotlandscensus.gov.uk/ods-web/data-warehouse.html#bulkdatatab

from collections import OrderedDict
import csv
import os
import urllib

from luigi import Parameter

from lib.csv_stream import CSVNormalizerStream
from lib.copy import copy_from_csv
from tasks.util import DownloadUnzipTask, TempTableTask
from tasks.meta import current_session

from .metadata import sanitize_identifier


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
        return sanitize_identifier(colid)

    def run(self):
        infile = os.path.join(self.input().path, self.table + '.csv')

        cols = OrderedDict({'geographycode': 'TEXT PRIMARY KEY'})
        with open(infile) as csvfile:
            reader = csv.reader(csvfile)
            header = reader.next()

            for c in header[1:]:
                cols[self.id_to_column(c)] = 'NUMERIC'

        with open(infile) as csvfile:
            copy_from_csv(
                current_session(),
                self.output().table,
                cols,
                CSVNormalizerStream(csvfile, lambda row: ['0' if f == '-' else f.replace(',', '') for f in row])
            )
