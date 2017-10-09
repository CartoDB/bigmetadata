# http://webarchive.nationalarchives.gov.uk/20160105160709/http://ons.gov.uk/ons/guide-method/census/2011/census-data/bulk-data/bulk-data-downloads/index.html

from collections import OrderedDict
import os
import shutil
from zipfile import ZipFile

from luigi import Task, Parameter, LocalTarget

from tasks.meta import current_session
from tasks.util import TableTask, classpath, shell, DownloadUnzipTask, TempTableTask, create_temp_schema
from tasks.uk.cdrc import OutputAreaColumns
from tasks.uk.census.metadata import CensusColumns
from .common import table_from_id
from .metadata import COLUMNS_DEFINITION


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
