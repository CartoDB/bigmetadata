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


class CopyFiles(BaseParams, Task):
    def requires(self):
        return DownloadData(resolution=self.resolution, state=self.state)

    def run(self):
        shell('mkdir {output}'.format(
            output=self.output().path
        ))

        # move the csvs to a path with no spaces so we can use them in the CSV2TempTableTask
        cmd = 'find '+self.input().path+' -name \'*.csv\' -print0 | xargs -0 -I {} cp {} '+self.output().path
        shell(cmd)

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))


class CleanCSVs(BaseParams, Task):
    def requires(self):
        return CopyFiles(resolution=self.resolution, state=self.state)

    def run(self):
        cmd = 'sed -i \'s/;/,/g\' {}/*.csv'.format(self.input().path)
        shell(cmd)

        # create a file to indicate that the task succeeded
        shell('touch {output}'.format(output=self.output().path))

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))


class ImportCSV(BaseParams, CSV2TempTableTask):

    infilepath = Parameter()

    # def requires(self):
    #     return DownloadData(resolution=self.resolution, state=self.state)

    def input_csv(self):
        return self.infilepath


class ImportData(BaseParams, Task):
    def requires(self):
        return {
            'copied': CopyFiles(resolution=self.resolution, state=self.state),
            'cleaned': CleanCSVs(resolution=self.resolution, state=self.state)
        }

    def run(self):
        infiles = shell('ls {input}/*.csv'.format(
            input=self.input()['copied'].path
        ))

        file_list = infiles.strip().split('\n')
        fhandle = self.output().open('w')
        for infile in file_list:
            data = yield ImportCSV(resolution=self.resolution, state=self.state, infilepath=infile)
            fhandle.write('{table}\n'.format(table=data.table))
        fhandle.close()

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))
