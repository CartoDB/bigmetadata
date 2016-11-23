import os
import csv

from luigi import Task, Parameter, WrapperTask, LocalTarget

from tasks.util import (DownloadUnzipTask, shell, Shp2TempTableTask,
                        ColumnsTask, TableTask, classpath, CSV2TempTableTask)
from tasks.meta import GEOM_REF, OBSColumn, current_session
from tasks.tags import SectionTags, SubsectionTags, UnitTags
from abc import ABCMeta
from collections import OrderedDict
from tasks.br.geo import BaseParams, GEOGRAPHIES


class DownloadData(BaseParams, DownloadUnzipTask):

    URL = 'ftp://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/Resultados_do_Universo/Agregados_por_Setores_Censitarios/'

    def _get_filename(self):
        regex = self.state

        if regex == 'sp':
            regex = 'sp_capital'    # SP_Capital_20150728.zip

        cmd = 'curl -s {url}'.format(url=self.URL)
        cmd += ' | '
        cmd += 'awk \'{print $9}\''
        cmd += ' | '
        cmd += 'grep -i {state}_'.format(state=regex)

        return shell(cmd)

    def download(self):
        filename = self._get_filename()
        shell('wget -O {output}.zip {url}{filename}'.format(
            output=self.output().path, url=self.URL, filename=filename
        ))


class ImportCSV(BaseParams, CSV2TempTableTask):

    tablename = Parameter(default='Basico')
    encoding = 'latin1'
    delimiter = ';'

    def requires(self):
        return DownloadData(resolution=self.resolution, state=self.state)

    def read_method(self, fname):
        coldef = self.coldef()
        numcols = len(coldef)
        return r"cut -d ';' -f 1-{numcols} {fname} | tr -d '\015'".format(
            fname=fname,
            numcols=numcols
        )

    def input_csv(self):
        return '"{downloadpath}/"*/*/*"/{tablename}_{state}.csv"'.format(
            downloadpath=self.input().path,
            tablename=self.tablename,
            state=self.state.upper(),
        )


class ImportAllCSV(BaseParams, WrapperTask):

    TABLES = ['Basico',
              # Home
              'Domicilio01', 'Domicilio02', 'DomicilioRenda',   # Renda --> Income
              # Environment
              'Entorno01', 'Entorno02', 'Entorno03', 'Entorno04', 'Entorno05',
              # People
              'Pessoa01', 'Pessoa02', 'Pessoa03', 'Pessoa04', 'Pessoa05',
              'Pessoa06', 'Pessoa07', 'Pessoa08', 'Pessoa09', 'Pessoa10',
              'Pessoa11', 'Pessoa12', 'Pessoa13', 'PessoaRenda',
              # Responsible
              'Responsavel01', 'Responsavel02', 'ResponsavelRenda',]

    def requires(self):
        for table in self.TABLES:
            yield ImportCSV(resolution=self.resolution, state=self.state,
                            tablename=table)


class Columns(ColumnsTask):

    tablename = Parameter(default='Domicilio02')

    def requires(self):
        requirements = {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'units': UnitTags(),
        }
        if self.tablename == 'Domicilio02':
            requirements['Domicilio01'] = Columns(tablename='Domicilio01')

        if self.tablename == 'Entorno02':
            requirements['Entorno01'] = Columns(tablename='Entorno01')

        if self.tablename in ('Entorno04', 'Entorno05'):
            requirements['Entorno03'] = Columns(tablename='Entorno03')
        return requirements

    def version(self):
        return 2

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()

        subsectiontags = input_['subsections']
        unittags = input_['units']
        brasil = input_['sections']['br']

        # column req's from other tables
        column_reqs = {}
        column_reqs.update(input_.get('Domicilio01', {}))
        column_reqs.update(input_.get('Entorno01', {}))
        column_reqs.update(input_.get('Entorno03', {}))

        filepath = "meta/{tablename}.csv".format(tablename=self.tablename)

        session = current_session()
        with open(os.path.join(os.path.dirname(__file__),filepath)) as tsvfile:
            reader = csv.reader(tsvfile, delimiter=',', quotechar='"')

            # Skip first row (header)
            # next(reader, None)
            for line in reader:
                col_id, col_name_pt, col_name_en, col_unit, denominators, col_subsections, \
                  data_sample_1, data_sample_2 = line

                # skip header rows
                if not col_id.startswith('V'):
                    continue

                # parse targets
                denominators = denominators.split('|')
                # print(denominators)
                # universes = universes.split('|')

                targets_dict = {}
                for x in denominators:
                    x = x.strip()
                    targets_dict[cols.get(x, column_reqs[x].get(session) if x in column_reqs else None)] = 'denominator'
                # for x in universes:
                #     x = x.strip()
                #     targets_dict[cols.get(x, column_reqs[x].get(session) if x in column_reqs else None)] = 'universe'
                targets_dict.pop(None, None)

                col_id = self.tablename+'_'+col_id

                cols[col_id] = OBSColumn(
                    id=col_id,
                    type='Numeric',
                    name=col_name_en,
                    description ='',
                    # Ranking of importance, sometimes used to favor certain measures in auto-selection
                    # Weight of 0 will hide this column from the user.  We generally use between 0 and 10
                    weight=5,
                    aggregate='sum',
                    # Tags are our way of noting aspects of this measure like its unit, the country
                    # it's relevant to, and which section(s) of the catalog it should appear in
                    tags=[brasil, unittags[col_unit]],
                    targets= targets_dict
                )

                # append the rest of the subsection tags
                col_subsections = col_subsections.split('|')
                for subsection in col_subsections:
                    subsection = subsection.strip()
                    subsection_tag = subsectiontags[subsection]
                    cols[col_id].tags.append(subsection_tag)

        return cols
