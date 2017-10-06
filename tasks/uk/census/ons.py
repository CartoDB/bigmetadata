# http://webarchive.nationalarchives.gov.uk/20160105160709/http://ons.gov.uk/ons/guide-method/census/2011/census-data/bulk-data/bulk-data-downloads/index.html

from collections import OrderedDict
import os
import shutil
from zipfile import ZipFile

from luigi import Task, Parameter, LocalTarget

from tasks.meta import current_session
from tasks.util import TableTask, classpath, shell, DownloadUnzipTask, TempTableTask, MetaWrapper, create_temp_schema
from tasks.uk.cdrc import OutputAreaColumns, OutputAreas
from tasks.uk.census.metadata import CensusColumns


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


class ImportAllEnglandWalesLocal(Task):
    def requires(self):
        return DownloadEnglandWalesLocal()

    def run(self):
        # Create schema in this wrapper task, avoid race conditions with the dependencies
        create_temp_schema(self)

        infiles = shell('ls {input}/LC*DATA.CSV'.format(
            input=self.input().path))

        tables = [os.path.split(infile)[-1].split('DATA.CSV')[0] for infile in infiles.strip().split('\n')]
        task_results = yield [ImportEnglandWalesLocal(table=table) for table in tables]

        with self.output().open('w') as fhandle:
            for result in task_results:
                fhandle.write('{table}\n'.format(table=result.table))

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))


class EnglandWalesLocal(TableTask):
    def requires(self):
        return {
            'data': ImportAllEnglandWalesLocal(),
            'geom_columns': OutputAreaColumns(),
            'data_columns': CensusColumns(),
        }

    def timespan(self):
        return 2011

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()
        cols['GeographyCode'] = input_['geom_columns']['oa_sa']
        cols.update(input_['data_columns'])
        return cols

    def populate(self):
        session = current_session()
        cols = self.columns()
        inserted = False
        for line in self.input()['data'].open():
            intable = line.strip()
            table = intable.split('_')[1]
            cols_for_table = OrderedDict([
                (n, t,) for n, t in cols.iteritems() if table in t._id
            ])
            out_colnames = cols_for_table.keys()
            in_colnames = [t._id.split('.')[-1] for t in cols_for_table.values()]
            assert len(out_colnames) == len(in_colnames)
            if not out_colnames:
                continue
            if not inserted:
                stmt = 'INSERT INTO {output} (geographycode, {out_colnames}) ' \
                        'SELECT geographycode, {in_colnames} ' \
                        'FROM {input} ' \
                        'WHERE geographycode LIKE \'E00%\' OR ' \
                        '      geographycode LIKE \'W00%\''.format(
                            output=self.output().table,
                            out_colnames=', '.join(out_colnames),
                            in_colnames=', '.join(in_colnames),
                            input=intable)
                session.execute(stmt)
                inserted = True
            else:
                stmt = 'UPDATE {output} out ' \
                        'SET {set} ' \
                        'FROM {input} intable ' \
                        'WHERE intable.geographycode = out.geographycode ' \
                        .format(
                            set=', '.join([
                                '{} = {}'.format(a, b) for a, b in zip(
                                    out_colnames, in_colnames)
                            ]),
                            output=self.output().table,
                            input=intable)
                session.execute(stmt)


class EnglandWalesMetaWrapper(MetaWrapper):
    def tables(self):
        yield EnglandWalesLocal()
        yield OutputAreas()
