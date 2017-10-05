# http://webarchive.nationalarchives.gov.uk/20160105160709/http://ons.gov.uk/ons/guide-method/census/2011/census-data/bulk-data/bulk-data-downloads/index.html

from luigi import Task, Parameter, LocalTarget

from tasks.meta import OBSColumn, DENOMINATOR, current_session, OBSTag
from tasks.util import (TableTask, ColumnsTask, classpath, shell,
                        DownloadUnzipTask, TagsTask, TempTableTask, MetaWrapper, create_temp_schema)
from tasks.uk.cdrc import OutputAreaColumns, OutputAreas
from tasks.tags import UnitTags, SectionTags, SubsectionTags, LicenseTags

from collections import OrderedDict
import os
import shutil
import json


def load_definition():
    with open(os.path.join(os.path.dirname(__file__), 'columns.json')) as json_file:
        return json.load(json_file, object_pairs_hook=OrderedDict)


COLUMNS_DEFINITION = load_definition()


class SourceTags(TagsTask):

    def version(self):
        return 1

    def tags(self):
        return [OBSTag(id='ons',
                       name='Office for National Statistics (ONS)',
                       type='source',
                       description="The UK's largest independent producer of official statistics and the recognised national statistical institute of the UK (`ONS <https://www.ons.gov.uk/>`_)")]


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
                    shell('unzip -d {path} {path}/{zipfile}'.format(path=work_dir, zipfile=filename))
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


class CensusColumns(ColumnsTask):

    def requires(self):
        return {
            'units': UnitTags(),
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'source': SourceTags(),
            'license': LicenseTags(),
        }

    def version(self):
        return 4

    def columns(self):
        input_ = self.input()
        source = input_['source']['ons']
        license = input_['license']['uk_ogl']
        uk = input_['sections']['uk']
        subsections = input_['subsections']
        units = input_['units']

        columns = OrderedDict()
        for key, column in COLUMNS_DEFINITION.items():
            columns[key] = OBSColumn(
                id=column['id'],
                name=column['name'],
                description='',
                type='Numeric',
                weight=column['weight'],
                aggregate='sum',
                targets={columns[denom]: DENOMINATOR for denom in column['denominators']},
                tags=[uk, source, license, units[column['units']], subsections[column['subsection']]]
            )

        return columns


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
