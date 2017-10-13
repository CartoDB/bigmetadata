from tasks.util import (TableTask, TagsTask, ColumnsTask, MetaWrapper, CSV2TempTableTask, shell, classpath)
from tasks.meta import current_session, DENOMINATOR
from collections import OrderedDict
from luigi import Parameter, Task, LocalTarget
import os
import glob
from tasks.meta import OBSColumn, OBSTag
from tasks.tags import SectionTags, SubsectionTags, UnitTags
from tasks.fr.geo import OutputAreaColumns, OutputAreas
import csv
import pandas as pd

SUM_UNITS = ('households', 'people', 'tax_consumption_units')

THEME_DEC_IRIS_2012 = 'BASE_TD_FILO_DEC_IRIS_2012'
THEME_DISP_IRIS_2012 = 'BASE_TD_FILO_DISP_IRIS_2012'
THEMES = [THEME_DEC_IRIS_2012, THEME_DISP_IRIS_2012]


class DownloadFRIncomeIris(Task):

    table_theme = Parameter()

    URL_base = 'https://www.insee.fr/fr/statistiques/fichier/2507751/'

    def download(self):

        themes = {
            THEME_DEC_IRIS_2012: 'BASE_TD_FILO_DEC_IRIS_2012.xls',
            THEME_DISP_IRIS_2012: 'BASE_TD_FILO_DISP_IRIS_2012.xls',
        }

        URL = self.URL_base + themes.get(self.table_theme)

        shell('wget -P {output} {url}'.format(
           output=self.output().path,
           url=URL
        ))

    def run(self):
        self.output().makedirs()
        self.download()

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))


class RawIncomeIrisData(CSV2TempTableTask):

    table_theme = Parameter()

    def requires(self):
        return DownloadFRIncomeIris(table_theme=self.table_theme)

    def xls2csv(self):
        # Read in excel file, it should be the only file in self.input().path
        # xls = pd.ExcelFile(os.path.join(self.input().path, os.listdir(self.input().path)[0]))
        xls = pd.ExcelFile(glob.glob(os.path.join(self.input().path,
                                                  '{table_theme}.xls'.format(table_theme=self.table_theme)))[0])

        # Remove header
        df = xls.parse(skiprows=5, header=0)
        df.to_csv(os.path.join(self.input().path, '{table_theme}.csv'.format(table_theme=self.table_theme)),
                  index=False, encoding='utf8')

    def input_csv(self):
        if not glob.glob(os.path.join(self.input().path, '{table_theme}.csv'.format(table_theme=self.table_theme))):
            self.xls2csv()

        return os.path.join(self.input().path, '{table_theme}.csv'.format(table_theme=self.table_theme))


class SourceTags(TagsTask):
    def version(self):
        return 1

    def tags(self):
        return [
            OBSTag(id='insee',
                   name='INSEE',
                   type='source',
                   description='http://www.insee.fr/fr/bases-de-donnees/default.asp?page=recensement/resultats/2012/donnees-detaillees-recensement-2012.htm')
        ]


class IrisIncomeColumns(ColumnsTask):

    table_theme = Parameter()

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'unittags': UnitTags(),
            'sourcetag': SourceTags()
        }

    def version(self):
        return 2

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()

        subsectiontags = input_['subsections']
        unittags = input_['unittags']
        france = input_['sections']['fr']
        insee_source = input_['sourcetag']['insee']

        filepath = "incomemetadata/Income Variables - {}.tsv".format(self.table_theme)
        with open(os.path.join(os.path.dirname(__file__), filepath)) as tsvfile:
            tsvreader = csv.reader(tsvfile, delimiter="\t")
            # Skip first row (header)
            next(tsvreader, None)
            for line in tsvreader:
                var_code, short_name, long_name, _universes, var_unit, denominators, subsections = line

                targets = self.get_targets(cols, denominators)

                aggregate = ''
                if var_unit in SUM_UNITS:
                    aggregate = 'sum'

                cols[var_code] = OBSColumn(
                    id=var_code,
                    type='Numeric',
                    name=long_name,
                    description=long_name,
                    # Ranking of importance, sometimes used to favor certain measures in auto-selection
                    # Weight of 0 will hide this column from the user.  We generally use between 0 and 10
                    weight=5,
                    aggregate=aggregate,
                    # Tags are our way of noting aspects of this measure like its unit, the country
                    # it's relevant to, and which section(s) of the catalog it should appear in
                    tags=[france, unittags[var_unit], insee_source],
                    targets=targets
                )
                subsections = subsections.split(',')
                for s in subsections:
                    s = s.strip()
                    subsection_tag = subsectiontags[s]
                    cols[var_code].tags.append(subsection_tag)

        return cols

    def get_targets(self, cols, denominators):
        targets_dict = {}
        if denominators:
            for x in denominators.split(','):
                targets_dict[cols.get(x.strip())] = DENOMINATOR
            targets_dict.pop(None, None)

        return targets_dict


class FranceIncome(TableTask):

    table_theme = Parameter()

    def version(self):
        return 2

    def timespan(self):
        return '2012'

    def requires(self):
        requirements = {
            'data': RawIncomeIrisData(table_theme=self.table_theme),
            'meta': IrisIncomeColumns(table_theme=self.table_theme),
            'geometa': OutputAreaColumns(),
        }
        return requirements

    def columns(self):
        cols = OrderedDict()
        cols['IRIS'] = self.input()['geometa']['dcomiris']
        cols.update(self.input()['meta'])
        return cols

    def populate(self):
        session = current_session()

        column_targets = self.columns()
        colnames = ', '.join(list(column_targets.keys()))
        colnames_typed = ','.join(['"{}"::{}'.format(colname, ct.get(session).type)
                                   for colname, ct in column_targets.items()])
        session.execute('INSERT INTO {output} ({ids}) '
                        'SELECT {ids_typed} '
                        'FROM {input} '.format(
                            ids=colnames,
                            ids_typed=colnames_typed,
                            output=self.output().table,
                            input=self.input()['data'].table
                        ))


class IRISIncomeTables(MetaWrapper):
    def requires(self):
        yield OutputAreas()
        for table_theme in THEMES:
            yield FranceIncome(table_theme=table_theme)
