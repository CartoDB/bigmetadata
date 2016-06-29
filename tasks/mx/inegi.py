from luigi import Task, Parameter, WrapperTask

from tasks.util import (DownloadUnzipTask, shell, Shp2TempTableTask,
                        ColumnsTask, TableTask)
from tasks.meta import GEOM_REF, OBSColumn, current_session

from collections import OrderedDict


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


class ImportGeography(Shp2TempTableTask):
    '''
    Import geographies into postgres by resolution using ogr2ogr
    '''

    resolution = Parameter()

    def requires(self):
        return DownloadGeographies()

    def input_shp(self):
        cmd = 'ls {input}/encuesta_intercensal_2015/shps/'.format(
            input=self.input().path
        )
        for ent in shell(cmd).strip().split('\n'):
            cmd = 'ls {input}/encuesta_intercensal_2015/shps/{ent}/{ent}_{resolution}*.shp'.format(
                input=self.input().path,
                ent=ent,
                resolution=self.resolution
            )
            for shp in shell(cmd).strip().split('\n'):
                yield shp


RESNAMES = {
    'ageb': '',
    'entidad': '',
    'localidad_rural_no_amanzanada': '',
    'localidad_urbana_y_rural_amanzanada': '',
    'manzana': '',
    'municipio': '',
    'servicios_area': '',
    'servicios_puntual': '',
}

RESDESCS = {
    'ageb': '',
    'entidad': '',
    'localidad_rural_no_amanzanada': '',
    'localidad_urbana_y_rural_amanzanada': '',
    'manzana': '',
    'municipio': '',
    'servicios_area': '',
    'servicios_puntual': '',
}

class GeographyColumns(ColumnsTask):

    resolution = Parameter()

    def requires(self):
        pass

    def columns(self):
        geom = OBSColumn(
            id=self.resolution,
            type='Geometry',
            name=RESNAMES[self.resolution],
            description=RESDESCS[self.resolution],
            weight=4,
        )
        geom_ref = OBSColumn(
            type='Text',
            weight=0,
            targets={geom: GEOM_REF},
        )
        name = OBSColumn(
            type='Text',
            weight=0,
        )
        return OrderedDict([
            ('the_geom', geom),
            ('cvegeo', geom_ref),
            ('nomgeo', name),
        ])


class Geography(TableTask):
    '''
    '''

    resolution = Parameter()

    def requires(self):
        return {
            'data': ImportGeography(resolution=self.resolution),
            'columns': GeographyColumns(resolution=self.resolution)
        }

    def timespan(self):
        return 2015

    def columns(self):
        return self.input()['columns']

    def populate(self):
        session = current_session()
        session.execute('INSERT INTO {output} (the_geom, cvegeo) '
                        'SELECT wkb_geometry, cvegeo '
                        'FROM {input} '.format(
                            output=self.output().table,
                            input=self.input()['data'].table))


class AllGeographies(WrapperTask):

    def requires(self):
        for resolution in ('ageb', 'entidad',
                           'localidad_urbana_y_rural_amanzanada', 'manzana',
                           'municipio', 'servicios_area',):
            yield Geography(resolution=resolution)
