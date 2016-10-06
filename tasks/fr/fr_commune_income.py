from tasks.util import (Shp2TempTableTask, TempTableTask, TableTask, TagsTask, ColumnsTask,
                        DownloadUnzipTask, CSV2TempTableTask,
                        underscore_slugify, shell, classpath)
from tasks.meta import current_session, DENOMINATOR, GEOM_REF, UNIVERSE
from collections import OrderedDict
from luigi import IntParameter, Parameter, WrapperTask, Task, LocalTarget
import os
from tasks.meta import OBSTable, OBSColumn, OBSTag
from tasks.tags import SectionTags, SubsectionTags, UnitTags
from tasks.fr.COMMUNES import OutputCommuneColumns
import csv
import pandas as pd

class DownloadFRCommuneIncome(DownloadUnzipTask):

    URL = 'http://www.insee.fr/fr/ppp/bases-de-donnees/donnees-detaillees/filosofi/filosofi-2012/indic-struct-distrib-revenu/indic-struct-distrib-revenu-communes-2012.zip'

    def download(self):

        shell('wget -O {output}.zip {url}'.format(
           output=self.output().path,
           url=self.URL
        ))


class FRIncomeData(CSV2TempTableTask):

    income_type = Parameter()

    def requires(self):
        return DownloadFRCommuneIncome()

    def infilepath(self):
        base = self.input().path
        fname_base = 'FILO_{}_COM.xls'.format(self.income_type)
        return os.path.join(base, fname_base)

    def input_csv(self):
        # Read in excel file
        xls = pd.ExcelFile(self.infilepath())

        ensembleSheet = xls.parse(skiprows=5,header=0, sheetname=1)
        ensembleSheet.to_csv(os.path.join(self.input().path,'{}_Ensemble.csv'.format(self.income_type)),index=False,encoding='utf8')
        return os.path.join(self.input().path,'{}_Ensemble.csv'.format(self.income_type))


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


class FRCommuneIncomeCols(ColumnsTask):

    income_type = Parameter()

    def requires(self):
        requirements = {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'unittags': UnitTags(),
            'sourcetag': SourceTags()
        }
        return requirements

    def version(self):
        return 1

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()

        subsectiontags = input_['subsections']
        unittags = input_['unittags']
        france = input_['sections']['fr']
        insee_source = input_['sourcetag']['insee']

        filepath = "incomemetadata/INCOME VARIABLES - FILO_{}_COM.tsv".format(self.income_type)
        with open(os.path.join(os.path.dirname(__file__),filepath)) as tsvfile:
            tsvreader = csv.reader(tsvfile, delimiter="\t")
            # Skip first row (header)
            next(tsvreader, None)
            for line in tsvreader:
                # Ignore Universe and Denomintors varible for now
                var_code, short_name, long_name, universe, var_unit, \
                    denominators, subsections = line
                cols[var_code] = OBSColumn(
                    id=var_code,
                    type='Numeric',
                    name=long_name,
                    description =long_name,
                    # Ranking of importance, sometimes used to favor certain measures in auto-selection
                    # Weight of 0 will hide this column from the user.  We generally use between 0 and 10
                    weight=5,
                    aggregate='',
                    # Tags are our way of noting aspects of this measure like its unit, the country
                    # it's relevant to, and which section(s) of the catalog it should appear in
                    tags=[france, unittags[var_unit]],
                )
                subsections = subsections.split(',')
                for s in subsections:
                    s = s.strip()
                    subsection_tag = subsectiontags[s]
                    cols[var_code].tags.append(subsection_tag)
                if var_unit in ('households', 'people', 'tax_consumption_units'):
                    cols[var_code].aggregate = 'sum'
        for _,col in cols.iteritems():
            col.tags.append(insee_source)
        return cols

class FRCommuneIncome(TableTask):

    income_type = Parameter()

    def version(self):
        return 1

    def timespan(self):
        return '2012'

    def requires(self):
        requirements = {
            'data': FRIncomeData(income_type=self.income_type),
            'meta': FRCommuneIncomeCols(income_type=self.income_type),
            'geometa': OutputCommuneColumns(),
        }
        return requirements

    def columns(self):
        cols = OrderedDict()
        cols['CODGEO'] = self.input()['geometa']['insee_com']
        cols.update(self.input()['meta'])
        return cols

    def populate(self):
        session = current_session()

        column_targets = self.columns()
        colnames = ', '.join(column_targets.keys())
        colnames_typed = ','.join(['{}::{}'.format(colname, ct.get(session).type)
                              for colname, ct in column_targets.iteritems()])
        session.execute('INSERT INTO {output} ({ids}) '
                        'SELECT {ids_typed} '
                        'FROM {input} '.format(
                            ids=colnames,
                            ids_typed=colnames_typed,
                            output=self.output().table,
                            input=self.input()['data'].table
                        ))


class CommuneIncomeTables(WrapperTask):
    def requires(self):
        income_types = ['DEC','DISP']
        for income_type in income_types:
            yield FRCommuneIncome(income_type=income_type)
