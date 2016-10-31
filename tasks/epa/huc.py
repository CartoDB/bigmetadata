import os

from tasks.util import DownloadUnzipTask, GdbFeatureClass2TempTableTask, shell


class DownloadHUC(DownloadUnzipTask):

    URL = 'ftp://newftp.epa.gov/epadatacommons/ORD/EnviroAtlas/NHDPlusV2_WBDSnapshot_EnviroAtlas_CONUS.gdb.zip'

    def download(self):
        shell('wget -O "{output}".zip "{url}"'.format(
            url=self.URL,
            output=self.output().path
        ))


class ImportHUC(GdbFeatureClass2TempTableTask):

    feature_class = 'nhdplusv2_wbdsnapshot_enviroatlas_conus'

    def requires(self):
        return DownloadHUC()

    def input_gdb(self):
        return os.path.join(self.input().path, 'NHDPlusV2_WBDSnapshot_EnviroAtlas_CONUS.gdb')
