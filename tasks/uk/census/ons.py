# http://webarchive.nationalarchives.gov.uk/20160105160709/http://ons.gov.uk/ons/guide-method/census/2011/census-data/bulk-data/bulk-data-downloads/index.html

import csv
import json
import os
import re
import urllib
import shutil
from zipfile import ZipFile

from luigi import Task, Parameter, LocalTarget

from tasks.meta import current_session
from tasks.util import classpath, shell, DownloadUnzipTask, TempTableTask


class DownloadUK(Task):
    API_URL = 'https://www.nomisweb.co.uk/api/v01/dataset/def.sdmx.json?search={}*'
    DOWNLOAD_URL = 'https://www.nomisweb.co.uk/api/v01/dataset/{id}.bulk.csv?time=2011&measures=20100&geography={geo}'

    table = Parameter()

    def run(self):
        # Query API, extract table ID from name
        meta = json.load(urllib.urlopen(self.API_URL.format(self.table)))
        api_id = (meta['structure']['keyfamilies']['keyfamily'][0]['id']).lower()

        # Download for SA (EW,S) and OA (NI) in a single file
        with self.output().temporary_path() as tmp:
            os.mkdir(tmp)
            with open(os.path.join(tmp, '{}.csv'.format(self.table)), 'w') as outcsv:
                skip_header = False
                for geo in ['TYPE258', 'TYPE299']:
                    remote_file = urllib.urlopen(self.DOWNLOAD_URL.format(id=api_id, geo=geo))
                    if skip_header:
                        remote_file.next()
                    else:
                        skip_header = True
                    for l in remote_file:
                        outcsv.write(l)

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))


class ImportUK(TempTableTask):
    table = Parameter()

    def requires(self):
        return DownloadUK(self.table)

    @staticmethod
    def id_to_column(colid):
        return re.sub(r'[:/, \-\.\(\)]', '_', '_'.join(colid.split(';')[0].split(':')[-2:]))

    def run(self):
        with open(os.path.join(self.input().path, self.table + '.csv')) as csvfile:
            reader = csv.reader(csvfile)
            header = reader.next()

            # We are faking the IDs, because Scotland bulk downloads uses the column name instead of the ID
            datacols = [self.id_to_column(x) for x in header[3:]]

            session = current_session()
            with session.connection().connection.cursor() as cursor:
                cursor.execute('CREATE TABLE {output} (date TEXT, geography TEXT, geographycode TEXT PRIMARY KEY, {cols})'.format(
                    output=self.output().table,
                    cols=', '.join(['{} NUMERIC'.format(c) for c in datacols])
                ))
                csvfile.seek(0)

                cursor.copy_expert(
                    'COPY {table} (date, geography, geographycode, {cols}) FROM stdin WITH (FORMAT CSV, HEADER)'.format(
                        cols=', '.join(datacols),
                        table=self.output().table),
                    csvfile)


class DownloadEnglandWalesLocal(DownloadUnzipTask):

    URL = 'https://www.nomisweb.co.uk/output/census/2011/release_4-1_bulk_all_tables.zip'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(output=self.output().path, url=self.URL))

    def run(self):
        super(DownloadEnglandWalesLocal, self).run()
        work_dir = self.output().path
        try:
            for filename in os.listdir(work_dir):
                if filename.endswith('.zip'):
                    ZipFile(os.path.join(work_dir, filename)).extractall(work_dir)
        except:
            shutil.rmtree(work_dir)
            raise


class ImportEnglandWalesLocal(TempTableTask):

    table = Parameter()

    def requires(self):
        return DownloadEnglandWalesLocal()

    @staticmethod
    def id_to_column(colid):
        return colid

    def run(self):
        session = current_session()
        infile = os.path.join(self.input().path, self.table + 'DATA.CSV')
        headers = shell('head -n 1 {csv}'.format(csv=infile))
        cols = ['{} NUMERIC'.format(h) for h in headers.split(',')[1:]]
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

