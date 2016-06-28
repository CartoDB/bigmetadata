from luigi import Task

from tasks.util import DownloadUnzipTask, shell

# https://blog.diegovalle.net/2016/01/encuesta-intercensal-2015-shapefiles.html
# 2015 Encuesta Intercensal AGEBs, Manzanas, Municipios, States, etc
class DownloadGeographies(DownloadUnzipTask):
    URL = 'http://data.diegovalle.net/mapsmapas/encuesta_intercensal_2015.zip'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(
            output=self.output().path, url=self.URL
        ))


# https://blog.diegovalle.net/2013/06/shapefiles-of-mexico-agebs-manzanas-etc.html
# 2010 Census AGEBs, Manzanas, Municipios, States, etc
class DownloadDemographicData(DownloadUnzipTask):
    URL = 'http://data.diegovalle.net/mapsmapas/agebsymas.zip'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(
            output=self.output().path, url=self.URL
        ))


# http://blog.diegovalle.net/2013/02/download-shapefiles-of-mexico.html
# Electoral shapefiles of Mexico (secciones and distritos)
class DownloadElectoralDistricts(DownloadUnzipTask):
    URL = 'http://data.diegovalle.net/mapsmapas/eleccion_2010.zip'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(
            output=self.output().path, url=self.URL
        ))
