# -*- coding: utf-8 -*-

import os
import glob
import csv
import pandas as pd

from luigi import Parameter, WrapperTask

from lib.timespan import get_timespan

from tasks.base_tasks import (ColumnsTask, DownloadUnzipTask, TagsTask, TableTask, CSV2TempTableTask, MetaWrapper,
                              RepoFile)
from tasks.util import shell, copyfile
from tasks.meta import OBSColumn, OBSTag, current_session, GEOM_REF
from tasks.tags import SectionTags, SubsectionTags, UnitTags
from collections import OrderedDict
from tasks.br.geo import (
    BaseParams,
    DATA_STATES,
    Geography,
    GeographyColumns,
    GEOGRAPHY_CODES,
    GEO_I
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

NO_DATA = {'al': ['Pessoa05', ],
           'pa': ['Pessoa05', ], }


class SourceTags(TagsTask):

    def tags(self):
        return [
            OBSTag(id='ibge-censo',
                   name='Censo Demográfico Instituto Brasileiro de Geografia e Estatística (IBGE)',
                   type='source',
                   description='The `Instituto Brasileiro de Geografia e Estatística <http://ibge.gov.br>`_ `Censo Demográfico <http://censo2010.ibge.gov.br/>`')
        ]


class LicenseTags(TagsTask):

    def tags(self):
        return [
            OBSTag(id='ibge-license',
                   name='Law No. 12,527, OF November 18, 2011',
                   type='license',
                   description='Full text is `here <http://www.planalto.gov.br/ccivil_03/_Ato2011-2014/2011/Lei/L12527.htm>`_')
        ]


class DownloadData(DownloadUnzipTask):

    state = Parameter()
    URL = 'ftp://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/Resultados_do_Universo/Agregados_por_Setores_Censitarios/'

    def version(self):
        return 1

    def requires(self):
        return RepoFile(resource_id=self.task_id,
                        version=self.version(),
                        url='{url}{filename}'.format(url=self.URL, filename=self._get_filename()))

    def _get_filename(self):
        cmd = 'curl -s {url}'.format(url=self.URL)
        cmd += ' | '
        cmd += 'awk \'{print $9}\''
        cmd += ' | '
        cmd += 'grep -i ^{state}_[0-9]*\.zip$'.format(state=self.state)

        return shell(cmd)

    def download(self):
        copyfile(self.input().path, '{output}.zip'.format(output=self.output().path))


class ImportData(CSV2TempTableTask):

    state = Parameter()
    tablename = Parameter()
    encoding = 'latin1'
    delimiter = ';'

    def requires(self):
        return DownloadData(state=self.state)

    def version(self):
        return 1

    def input_csv(self):
        if self.state.lower() == 'sp_capital':
            state_code = 'SP1'
        elif self.state.lower() == 'sp_exceto_a_capital':
            state_code = 'SP2'
        else:
            state_code = self.state.upper()

        # The provided CSV files are not well-formed, so we convert the provided XLS files into CSV
        # All files are {tablename}_{state}.xls (or XLS), except Basico-MG.xls
        filename = '{tablename}[-_]{state_code}.[xX][lL][sS]'.format(tablename=self.tablename,
                                                                     state_code=state_code)

        path = glob.glob(os.path.join(self.input().path, '**', filename), recursive=True)[0]

        df = pd.read_excel(path)
        if self.tablename != 'Basico':
            df = df.apply(pd.to_numeric, errors="coerce")
        df.to_csv(os.path.join(self.input().path, '{tablename}_{state_code}.csv'.format(tablename=self.tablename,
                                                                                        state_code=state_code)),
                  index=False,
                  sep=';',
                  encoding=self.encoding)

        return os.path.join(self.input().path, '{tablename}_{state_code}.csv'.format(tablename=self.tablename,
                                                                                     state_code=state_code))


class ImportAllTables(BaseParams, WrapperTask):

    def requires(self):
        for table in TABLES:
            if table not in NO_DATA.get(self.state, []):
                yield ImportData(state=self.state, tablename=table)


class ImportAllStates(BaseParams, WrapperTask):

    def requires(self):
        for state in DATA_STATES:
            yield ImportAllTables(state=state)


class Columns(ColumnsTask):

    tablename = Parameter(default='Basico')

    def requires(self):
        requirements = {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'units': UnitTags(),
            'source': SourceTags(),
            'license': LicenseTags()
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
        return 6

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
        source = input_['source']['ibge-censo']
        license = input_['license']['ibge-license']

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
        column_reqs.update(input_.get('ResponsavelRenda', {}))

        filepath = "meta/{tablename}.csv".format(tablename=self.tablename)

        session = current_session()
        with open(os.path.join(os.path.dirname(__file__), filepath), encoding='latin1') as tsvfile:
            reader = csv.reader(tsvfile, delimiter=',', quotechar='"')

            for line in reader:
                # skip headers
                if not line[0].startswith('V'):
                    continue

                col_id = line[0]
                col_name_pt = line[1]
                col_unit = line[3]
                denominators = line[4]
                col_subsections = line[5]
                if self.tablename == 'Basico':
                    col_agg = line[6]
                else:
                    col_agg = None

                # validate the col_id (VXXX)
                if not self.validate_id(col_id):
                    print(('*******col_id not valid', col_id))
                    continue

                # parse targets
                denominators = denominators.split('|')

                targets_dict = {}
                for denom in denominators:
                    denom = denom.strip()
                    target_type = 'universe' if col_agg in ['median', 'average'] else 'denominator'
                    targets_dict[cols.get(denom, column_reqs[denom].get(session)
                                 if denom in column_reqs else None)] = target_type

                targets_dict.pop(None, None)

                col_id = self.tablename+'_'+col_id

                cols[col_id] = OBSColumn(
                    id=col_id,
                    type='Numeric',
                    name=col_name_pt,
                    description='',
                    # Ranking of importance, sometimes used to favor certain measures in auto-selection
                    # Weight of 0 will hide this column from the user.  We generally use between 0 and 10
                    weight=5,
                    aggregate=col_agg or 'sum',
                    # Tags are our way of noting aspects of this measure like its unit, the country
                    # it's relevant to, and which section(s) of the catalog it should appear in
                    tags=[source, license, brasil, unittags[col_unit]],
                    targets=targets_dict
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
class Censos(TableTask):

    tablename = Parameter(default='Basico')
    resolution = Parameter()

    def version(self):
        return 7

    def targets(self):
        return {
            self.input()['geo'].obs_table: GEOM_REF,
        }

    def states(self):
        '''
        Exclude Basico/mg, which seems to be missing
        '''
        return DATA_STATES

    def requires(self):
        import_data = {}
        for state in self.states():
            if self.tablename not in NO_DATA.get(state, []):
                import_data[state] = ImportData(state=state, tablename=self.tablename)
        return {
            'data': import_data,
            'geo': Geography(resolution=self.resolution),
            'geometa': GeographyColumns(resolution=self.resolution),
            'meta': Columns(tablename=self.tablename),
        }

    def table_timespan(self):
        return get_timespan('2010')

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()
        cols['Cod_setor'] = input_['geometa']['{}'.format(GEOGRAPHY_CODES[self.resolution])]
        # For some reason, state 'go' is missing columns 843 through 860 in
        # Entorno05
        for colname, coltarget in input_['meta'].items():
            # if coltarget._id.split('.')[-1].lower().startswith(self.topic.lower()):
            #if skip_pre_861 and int(colname.split('V')[-1]) < 861:
            #    continue
            cols[colname] = coltarget
        return cols

    def populate(self):
        if self.resolution != 'setores_censitarios':
            raise Exception('Data only supported for setores_censitarios')
        session = current_session()
        column_targets = self.columns()
        out_colnames = list(column_targets.keys())

        for state, input_ in self.input()['data'].items():
            intable = input_.table

            in_colnames = ['"{}"::{}'.format(colname.split('_')[1], ct.get(session).type)
                           for colname, ct in column_targets.items()]
            # skip_pre_861
            if self.tablename == 'Entorno05' and state.lower() == 'go':
                for i, in_colname in enumerate(in_colnames):
                    if i == 0:
                        continue
                    if int(in_colname.split('::')[0].split('V')[-1].replace('"', '')) < 861:
                        in_colnames[i] = 'NULL'

            in_colnames[0] = '"Cod_setor"'

            cmd = 'INSERT INTO {output} ({out_colnames}) ' \
                  'SELECT {in_colnames} FROM {input} '.format(
                      output=self.output().table,
                      input=intable,
                      in_colnames=', '.join(in_colnames),
                      out_colnames=', '.join(out_colnames))
            session.execute(cmd)


class CensosAllTables(WrapperTask):

    resolution = Parameter()

    def requires(self):
        for table in TABLES:
            yield Censos(resolution=self.resolution, tablename=table)


class CensosAllGeographiesAllTables(WrapperTask):

    def requires(self):
        for resolution in (GEO_I, ):
            yield CensosAllTables(resolution=resolution)


class CensosMetaWrapper(MetaWrapper):

    resolution = Parameter()
    tablename = Parameter()

    params = {
        #'resolution': GEOGRAPHIES,
        'resolution': [GEO_I], # data only for setores_censitarios
        'tablename': TABLES
    }

    def tables(self):
        yield Geography(resolution=self.resolution)
        yield Censos(resolution=self.resolution, tablename=self.tablename)
