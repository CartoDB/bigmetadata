# http://www.scotlandscensus.gov.uk/ods-web/data-warehouse.html#bulkdatatab

import csv
import os
import re

from luigi import Parameter

from tasks.util import shell, DownloadUnzipTask, TempTableTask
from tasks.meta import current_session


class DownloadScotlandLocal(DownloadUnzipTask):

    URL = 'http://www.scotlandscensus.gov.uk/ods-web/download/getDownloadFile.html?downloadFileIds=Output%20Area%20blk'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(output=self.output().path, url=self.URL))


class ImportScotland(TempTableTask):
    table = Parameter()

    def requires(self):
        return DownloadScotlandLocal()

    @staticmethod
    def id_to_column(colid):
        return re.sub(r'[:/, \-\.\(\)]', '_', '_'.join(colid.split(';')[0].split(':')[-2:]))

    def run(self):
        with open(os.path.join(self.input().path, self.table + '.csv')) as csvfile:
            reader = csv.reader(csvfile)
            header = reader.next()

            cols = [ImportScotland.id_to_column(c) for c in header[1:]]

            session = current_session()
            session.execute('CREATE TABLE {output} (GeographyCode TEXT PRIMARY KEY, {cols})'.format(
                output=self.output().table,
                cols=', '.join(['{} NUMERIC'.format(c) for c in cols])
            ))

            rows = []
            for r in reader:
                if r[0].startswith('S92'):
                    continue  # Skip contry-level summary

                binds = {cols[i]: (0 if v == '-' else v.replace(',', '')) for i, v in enumerate(r[1:])}
                binds['geographycode'] = r[0]
                rows.append(binds)

            session.execute('INSERT INTO {output} (geographycode, {cols}) VALUES (:geographycode, {pcols})'.format(
                output=self.output().table,
                cols=', '.join(cols),
                pcols=', '.join([':{}'.format(c) for c in cols])), rows)
