import os

from luigi import Task, Parameter, WrapperTask, LocalTarget

from tasks.util import (DownloadUnzipTask, shell, Shp2TempTableTask,
                        ColumnsTask, TableTask, classpath, CSV2TempTableTask)
from tasks.meta import GEOM_REF, OBSColumn, current_session
from tasks.tags import SectionTags, SubsectionTags
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

    tablename = Parameter()
    encoding = 'latin1'
    delimiter = ';'

    def requires(self):
        return DownloadData(resolution=self.resolution, state=self.state)

    def read_method(self, fname):
        return 'sed "s/;.$//" {}'.format(fname)

    def input_csv(self):
        return '"{downloadpath}/"*/*/*"/{tablename}_{state}.csv"'.format(
            downloadpath=self.input().path,
            tablename=self.tablename,
            state=self.state.upper(),
        )


class ImportAllCSV(BaseParams, WrapperTask):

    TABLES = ['Basico', 'Domicilio01', 'Domicilio02', 'DomicilioRenda',
              'Entorno01', 'Entorno02', 'Entorno03', 'Entorno04', 'Entorno05',
              'Pessoa01', 'Pessoa02', 'Pessoa03', 'Pessoa04', 'Pessoa05',
              'Pessoa06', 'Pessoa07', 'Pessoa08', 'Pessoa09', 'Pessoa10',
              'Pessoa11', 'Pessoa12', 'Pessoa13', 'PessoaRenda',
              'Responsavel01', 'Responsavel02', 'ResponsavelRenda',]

    def requires(self):
        for table in self.TABLES:
            yield ImportCSV(resolution=self.resolution, state=self.state,
                            tablename=table)
