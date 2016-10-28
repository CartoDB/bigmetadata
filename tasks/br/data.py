from luigi import Task, Parameter, WrapperTask

from tasks.util import (DownloadUnzipTask, shell, Shp2TempTableTask,
                        ColumnsTask, TableTask)
from tasks.meta import GEOM_REF, OBSColumn, current_session
from tasks.tags import SectionTags, SubsectionTags
from abc import ABCMeta
from collections import OrderedDict
from tasks.br.geo import BaseParams, GEOGRAPHIES


class DownloadData(BaseParams, DownloadUnzipTask):

    URL = 'ftp://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/Resultados_do_Universo/Agregados_por_Setores_Censitarios/'

    def _get_filename(self):
        cmd = 'curl -s {url}'.format(url=self.URL)
        cmd += ' | '
        cmd += 'awk \'{print $9}\''
        cmd += ' | '
        cmd += 'grep -i {state}_'.format(state=self.state)

        return shell(cmd)

    def download(self):
        filename = self._get_filename()
        shell('wget -O {output}.zip {url}/{filename}'.format(
            output=self.output().path, url=self.URL, filename=filename
        ))


