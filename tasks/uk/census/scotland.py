# http://www.scotlandscensus.gov.uk/ods-web/data-warehouse.html#bulkdatatab

import csv
import os

from luigi import Parameter

from tasks.util import TagsTask, shell, DownloadUnzipTask, TempTableTask
from tasks.meta import OBSTag, current_session


class SourceTags(TagsTask):
    def version(self):
        return 1

    def tags(self):
        return [OBSTag(id='scotland-census',
                name="Scotland's Census Data Warehouse by National Records of Scotland",
                type='source',
                description="`Scotland Census <http://www.scotlandscensus.gov.uk/`_")]


class DownloadScotlandLocal(DownloadUnzipTask):

    URL = 'http://www.scotlandscensus.gov.uk/ods-web/download/getDownloadFile.html?downloadFileIds=Output%20Area%20blk'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(output=self.output().path, url=self.URL))


class ImportScotland(TempTableTask):
    table = Parameter()

    def requires(self):
        return DownloadScotlandLocal()

    def run(self):
        with open(os.path.join(self.input().path, self.table + '.csv')) as csvfile:
            reader = csv.reader(csvfile)
            header = reader.next()

            # We are faking the IDs, because Scotland bulk downloads uses the column name instead of the ID
            cols = ['{table}{idx:04}'.format(table=self.table, idx=i) for i in range(1, len(header))]

            session = current_session()
            session.execute('CREATE TABLE {output} (GeographyCode TEXT PRIMARY KEY, {cols})'.format(
                output=self.output().table,
                cols=', '.join(['{} NUMERIC'.format(c) for c in cols])
            ))

            rows = []
            for r in reader:
                if r[0].startswith('S92'):
                    continue  # Skip contry-level summary

                rcols = {'geographycode': r[0]}
                for i, v in enumerate(r[1:], 1):
                    key = '{table}{idx:04}'.format(table=self.table, idx=i)
                    rcols[key] = 0 if v == '-' else v.replace(',', '')
                rows.append(rcols)

            session.execute('INSERT INTO {output} (geographycode, {cols}) VALUES (:geographycode, {pcols})'.format(
                output=self.output().table,
                cols=', '.join(cols),
                pcols=', '.join([':{}'.format(c) for c in cols])), rows)
