# http://webarchive.nationalarchives.gov.uk/20160105160709/http://ons.gov.uk/ons/guide-method/census/2011/census-data/bulk-data/bulk-data-downloads/index.html

from collections import OrderedDict
import csv
import json
import os
import urllib
import shutil
from zipfile import ZipFile

from luigi import Task, Parameter, LocalTarget

from tasks.meta import current_session
from tasks.util import TableTask, classpath, shell, DownloadUnzipTask, TempTableTask, create_temp_schema
from tasks.uk.cdrc import OutputAreaColumns
from tasks.uk.census.metadata import CensusColumns
from .common import table_from_id
from .metadata import COLUMNS_DEFINITION


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
    def clean_id(colid):
        return colid.split(';')[0].replace(':', '').replace(' ', '_')

    def run(self):
        with open(os.path.join(self.input().path, self.table + '.csv')) as csvfile:
            reader = csv.reader(csvfile)
            header = reader.next()

            # We are faking the IDs, because Scotland bulk downloads uses the column name instead of the ID
            cols = [self.clean_id(x) for x in header]

            session = current_session()
            with session.connection().connection.cursor() as cursor:
                cursor.execute('CREATE TABLE {output} (date TEXT, geography TEXT, geography_code TEXT PRIMARY KEY, {cols})'.format(
                    output=self.output().table,
                    cols=', '.join(['{} NUMERIC'.format(c) for c in cols[3:]])
                ))
                csvfile.seek(0)

                cursor.copy_expert(
                    'COPY {table} ({cols}) FROM stdin WITH (FORMAT CSV, HEADER)'.format(
                        cols=', '.join(cols),
                        table=self.output().table),
                    csvfile)


class DownloadONS(DownloadUnzipTask):
    URL = 'https://www.nomisweb.co.uk/output/census/2011/{}_2011_oa.zip'

    table = Parameter()

    def download(self):
        shell('wget -O {output}.zip {url}'.format(output=self.output().path, url=self.URL.format(self.table.lower())))


class ImportONS(TempTableTask):
    table = Parameter()

    def requires(self):
        return DownloadONS(table=self.table)

    def run(self):
        infile = os.path.join(self.input().path, os.listdir(self.input().path)[0], self.table + 'DATA.CSV')
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


class EnglandWalesLocal(TableTask):
    def requires(self):
        deps = {
            'geom_columns': OutputAreaColumns(),
            'data_columns': CensusColumns(),
        }
        for t in self.source_tables():
            deps[t] = ImportEnglandWalesLocal(table=t)
        return deps

    def timespan(self):
        return 2011

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()
        cols['GeographyCode'] = input_['geom_columns']['oa_sa']
        cols.update(input_['data_columns'])
        return cols

    def source_tables(self):
        return set([table_from_id(col['fields']['ons']) for col in COLUMNS_DEFINITION.values()])

    def populate(self):
        tables = [self.requires()[table].output().table for table in self.source_tables()]
        in_colnames = [col['fields']['ons'] for col in COLUMNS_DEFINITION.values()]
        out_colnames = COLUMNS_DEFINITION.keys()

        from_part = tables[0]
        for t in tables[1:]:
            from_part += ' FULL JOIN {} USING (geographycode)'.format(t)

        stmt = 'INSERT INTO {output} (geographycode, {out_colnames}) ' \
               'SELECT geographycode, {in_colnames} ' \
               'FROM {from_part} ' \
               'WHERE geographycode LIKE \'E00%\' OR ' \
               '      geographycode LIKE \'W00%\''.format(
                   output=self.output().table,
                   out_colnames=', '.join(out_colnames),
                   in_colnames=', '.join(in_colnames),
                   from_part=from_part)
        current_session().execute(stmt)
