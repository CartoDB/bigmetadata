import os
import csv
import pandas as pd

from luigi import Task, Parameter, WrapperTask, LocalTarget

from tasks.util import (DownloadUnzipTask, shell, Shp2TempTableTask,
                        ColumnsTask, TableTask, classpath, CSV2TempTableTask)
from tasks.meta import GEOM_REF, OBSColumn, current_session
from tasks.tags import SectionTags, SubsectionTags, UnitTags
from abc import ABCMeta
from collections import OrderedDict
from tasks.br.geo import (
    BaseParams,
    GEOGRAPHIES,
    STATES,
    Geography,
    GeographyColumns
)


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


class DownloadData(BaseParams, DownloadUnzipTask):

    URL = 'ftp://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/Resultados_do_Universo/Agregados_por_Setores_Censitarios/'

    def _get_filename(self):
        state = self.state.upper()

        if state == 'SP':
            state = 'SP_Capital'    # SP_Capital_20150728.zip

        cmd = 'curl -s {url}'.format(url=self.URL)
        cmd += ' | '
        cmd += 'awk \'{print $9}\''
        cmd += ' | '
        cmd += 'grep {state}_[0-9].*zip$'.format(state=state)

        return shell(cmd)

    def download(self):
        filename = self._get_filename()
        shell('wget -O {output}.zip {url}{filename}'.format(
            output=self.output().path, url=self.URL, filename=filename
        ))


class ImportData(BaseParams, CSV2TempTableTask):

    tablename = Parameter(default='Basico')
    encoding = 'latin1'
    delimiter = ';'

    def requires(self):
        return DownloadData(resolution=self.resolution, state=self.state)

    def version(self):
        return 1

    def input_csv(self):
        if self.state == 'sp':
            state_code = 'SP1'
        else:
            state_code = self.state.upper()

        filename = '{tablename}_{state_code}.[xX][lL][sS]'.format(
            tablename=self.tablename,
            state_code=state_code
        )

        path = shell('find {downloadpath} -name "{filename}"'.format(downloadpath=self.input().path, filename=filename))

        df = pd.read_excel(path.split('\n')[0])
        df.to_csv(
            os.path.join(self.input().path, '{tablename}_{state_code}.csv'.format(tablename=self.tablename,
            state_code=state_code)),
            index=False,
            sep=';',
            encoding='utf8'
        )

        return os.path.join(self.input().path,'{tablename}_{state_code}.csv'.format(tablename=self.tablename,
            state_code=state_code))


class ImportAllTables(BaseParams, WrapperTask):

    def requires(self):
        for table in TABLES:
            yield Columns(tablename=table)
            yield ImportData(resolution=self.resolution, state=self.state,
                            tablename=table)

class ImportAllStates(BaseParams, WrapperTask):

    def requires(self):
        for state in STATES:
            yield ImportAllTables(resolution=self.resolution, state=state)


class ImportAllResolutions(BaseParams, WrapperTask):

    def requires(self):
        for resolution in GEOGRAPHIES:
            yield ImportAllTables(resolution=resolution, state=self.state)


class ImportAll(BaseParams, WrapperTask):

    def requires(self):
        for resolution in GEOGRAPHIES:
            for state in STATES:
                yield ImportAllTables(resolution=resolution, state=state)


class Columns(ColumnsTask):

    tablename = Parameter(default='Basico')

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

        if self.tablename in ('Pessoa11', 'Pessoa12'):
            requirements['Pessoa13'] = Columns(tablename='Pessoa13')

        if self.tablename in ('Pessoa02', 'Pessoa04'):
            requirements['Pessoa01'] = Columns(tablename='Pessoa01')

        if self.tablename in ('Pessoa08'):
            requirements['Pessoa07'] = Columns(tablename='Pessoa07')

        if self.tablename in ('Pessoa05'):
            requirements['Pessoa03'] = Columns(tablename='Pessoa03')

        if self.tablename in ('Responsavel01'):
            requirements['Responsavel02'] = Columns(tablename='Responsavel02')

        if self.tablename in ('Basico'):
            requirements['Pessoa13'] = Columns(tablename='Pessoa13')
            requirements['ResponsavelRenda'] = Columns(tablename='ResponsavelRenda')


        return requirements

    def version(self):
        return 5

    def validate_id(self, col_id):

        # ids start with V
        if not col_id.startswith('V'):
            return False

        # get numeric value
        num = col_id.split('V')[1]
        # should be 3 digits min
        if len(num) < 3:
            return False

        # ensure its an number
        try:
            num = int(num)
        except ValueError:
            return False

        return True


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
        column_reqs.update(input_.get('Pessoa13', {}))
        column_reqs.update(input_.get('Pessoa01', {}))
        column_reqs.update(input_.get('Pessoa07', {}))
        column_reqs.update(input_.get('Pessoa03', {}))
        column_reqs.update(input_.get('Responsavel02', {}))
        column_reqs.update(input_.get('Basico', {}))

        filepath = "meta/{tablename}.csv".format(tablename=self.tablename)

        session = current_session()
        with open(os.path.join(os.path.dirname(__file__),filepath)) as tsvfile:
            reader = csv.reader(tsvfile, delimiter=',', quotechar='"')

            for line in reader:
                # skip headers
                if not line[0].startswith('V'):
                    continue

                col_id = line[0]
                col_name_pt = line[1]
                col_name_en = line[2]
                col_unit = line[3]
                denominators = line[4]
                col_subsections = line[5]

                # validate the col_id (VXXX)
                if not self.validate_id(col_id):
                    print('*******col_id not valid', col_id)
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


class ImportAllColumns(WrapperTask):

    def requires(self):
        for table in TABLES:
            yield Columns(tablename=table)


#####################################
# COPY TO OBSERVATORY
#####################################
class Censos(BaseParams, TableTask):

    tablename = Parameter(default='Basico')

    def requires(self):
        return {
            'data': ImportData(resolution=self.resolution, state=self.state, tablename=self.tablename),
            'geo': Geography(resolution=self.resolution, state=self.state),
            'geometa': GeographyColumns(resolution=self.resolution, state=self.state),
            'meta': Columns(tablename=self.tablename),
        }

    def timespan(self):
        return '2010'

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()
        cols['Cod_setor'] = input_['geometa']['geom_id']
        for colname, coltarget in input_['meta'].iteritems():
            # if coltarget._id.split('.')[-1].lower().startswith(self.topic.lower()):
            cols[colname] = coltarget
        return cols

    def populate(self):
        session = current_session()
        column_targets = self.columns()
        out_colnames = column_targets.keys()
        in_colnames = ['{}::{}'.format(colname.split('_')[1], ct.get(session).type)
                              for colname, ct in column_targets.iteritems()]
        in_colnames[0] = 'Cod_setor'

        cmd =   'INSERT INTO {output} ({out_colnames}) ' \
                'SELECT {in_colnames} FROM {input} '.format(
                    output=self.output().table,
                    input=self.input()['data'].table,
                    in_colnames=', '.join(in_colnames),
                    out_colnames=', '.join(out_colnames))
        session.execute(cmd)


class CensosAllTables(BaseParams, WrapperTask):

    def requires(self):
        for table in TABLES:
            yield Censos(resolution=self.resolution, state=self.state, tablename=table)
