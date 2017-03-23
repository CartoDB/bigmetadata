from luigi import Task, Parameter, WrapperTask

from tasks.util import (DownloadUnzipTask, shell, Shp2TempTableTask,
                        ColumnsTask, TableTask, MetaWrapper)
from tasks.meta import GEOM_REF, OBSColumn, current_session
from tasks.mx.inegi_columns import DemographicColumns
from tasks.tags import SectionTags, SubsectionTags, BoundaryTags
from tasks.mx.inegi_columns import SourceTags, LicenseTags

from collections import OrderedDict


RESOLUTIONS = (
    'ageb', 'entidad', 'localidad_urbana_y_rural_amanzanada', 'manzana',
    'municipio', 'servicios_area',
)

RESNAMES = {
    'ageb': 'Census area (urban areas only)',
    'entidad': 'State',
    'localidad_rural_no_amanzanada': 'Localidades (rural)',
    'localidad_urbana_y_rural_amanzanada': 'Localidades (urban)',
    'manzana': 'Census block (urban areas only)',
    'municipio': 'Municipios',
    'servicios_area': 'Service areas',
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

RESTAGS = {
    'ageb': ['cartographic_boundary', 'interpolation_boundary'],
    'entidad': ['cartographic_boundary', 'interpolation_boundary'],
    'localidad_rural_no_amanzanada': ['cartographic_boundary'],
    'localidad_urbana_y_rural_amanzanada': ['cartographic_boundary'],
    'manzana': ['cartographic_boundary', 'interpolation_boundary'],
    'municipio': ['cartographic_boundary', 'interpolation_boundary'],
    'servicios_area': [],
    'servicios_puntual': [],
}

#ags_cpv2010_municipal_mortalidad.dbf 'MOR'
#ags_cpv2010_municipal_desarrollo_social.dbf

#DEMOGRAPHIC_TABLES = (
#    'caracteristicas_economicas', 'migracion', 'caracteristicas_educativas',
#    'mortalidad', 'desarrollo_social', 'religion', 'discapacidad',
#    'servicios_de_salud', 'fecundidad', 'situacion_conyugal',
#    'hogares_censales', 'viviendas', 'lengua_indigena',
#)

DEMOGRAPHIC_TABLES = {
    'mig': 'migracion',
    'indi': 'lengua_indigena',
    'disc': 'discapacidad',
    'edu': 'caracteristicas_educativas',
    'eco': 'caracteristicas_economicas',
    'salud': 'servicios_de_salud',
    'scony': 'situacion_conyugal',
    'relig': 'religion',
    'hogar': 'hogares_censales',
    'viv': 'viviendas',
    'fec': 'fecundidad',
    'pob': ''
}


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


class ImportDemographicData(Shp2TempTableTask):

    resolution = Parameter()
    table = Parameter()

    def requires(self):
        return DownloadDemographicData()

    def input_shp(self):
        cmd = 'ls {input}/scince/shps/'.format(
            input=self.input().path
        )
        # handle differeing file naming conventions between the geographies
        # and the census data
        if self.resolution == 'municipio':
            resolution = 'municipal'
        elif self.resolution == 'entidad':
            resolution = 'estatal'
        elif self.resolution == 'localidad_urbana_y_rural_amanzanada':
            resolution = 'loc_urb'
        else:
            resolution = self.resolution
        for ent in shell(cmd).strip().split('\n'):
            if ent.lower() == 'national':
                continue
            if self.table.lower().startswith('pob'):
                path = 'ls {input}/scince/shps/{ent}/{ent}_{resolution}*.dbf'
            else:
                path = 'ls {{input}}/scince/shps/{{ent}}/tablas/' \
                    '{{ent}}_cpv2010_{{resolution}}*_{table}.dbf'.format(
                        table=DEMOGRAPHIC_TABLES[self.table])
            cmd = path.format(
                input=self.input().path,
                ent=ent,
                resolution=resolution,
            )
            for shp in shell(cmd).strip().split('\n'):
                yield shp


class GeographyColumns(ColumnsTask):

    resolution = Parameter()

    weights = {
        'manzana': 8,
        'ageb': 7,
        'municipio': 6,
        'entidad': 5,
        'localidad_urbana_y_rural_amanzanada': 4,
        'servicios_area': 3,
    }

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'source': SourceTags(),
            'license': LicenseTags(),
            'boundary': BoundaryTags()
        }

    def version(self):
        return 8

    def columns(self):
        input_ = self.input()
        sections = input_['sections']
        subsections = input_['subsections']
        license = input_['license']['inegi-license']
        source = input_['source']['inegi-source']
        boundary_type = input_['boundary']
        geom = OBSColumn(
            id=self.resolution,
            type='Geometry',
            name=RESNAMES[self.resolution],
            description=RESDESCS[self.resolution],
            weight=self.weights[self.resolution],
            tags=[sections['mx'], subsections['boundary'], license, source]
        )
        geom_ref = OBSColumn(
            id=self.resolution + '_cvegeo',
            type='Text',
            weight=0,
            targets={geom: GEOM_REF},
        )
        name = OBSColumn(
            type='Text',
            weight=0,
        )

        geom.tags.extend(boundary_type[i] for i in RESTAGS[self.resolution])

        return OrderedDict([
            ('the_geom', geom),
            ('cvegeo', geom_ref),
            ('nomgeo', name),
        ])


class Geography(TableTask):
    '''
    '''

    resolution = Parameter()

    def version(self):
        return 1

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


class Census(TableTask):

    resolution = Parameter()
    table = Parameter()

    def version(self):
        return 4

    def timespan(self):
        return 2010

    def requires(self):
        return {
            'data': ImportDemographicData(resolution=self.resolution,
                                          table=self.table),
            'meta': DemographicColumns(),
            'geometa': GeographyColumns(resolution=self.resolution)
        }

    def columns(self):
        input_ = self.input()
        cols = OrderedDict()
        cols['cvegeo'] = input_['geometa']['cvegeo']
        for colname, coltarget in input_['meta'].iteritems():
            if coltarget._id.split('.')[-1].lower().startswith(self.table.lower()):
                cols[colname] = coltarget
        return cols

    def populate(self):
        session = current_session()
        columns = self.columns()
        out_colnames = columns.keys()
        in_table = self.input()['data']
        in_colnames = [ct._id.split('.')[-1] for ct in columns.values()]
        in_colnames[0] = 'cvegeo'
        for i, in_c in enumerate(in_colnames):
            cmd = "SELECT 'exists' FROM information_schema.columns " \
                    "WHERE table_schema = '{schema}' " \
                    "  AND table_name = '{tablename}' " \
                    "  AND column_name = '{colname}' " \
                    "  LIMIT 1".format(
                        schema=in_table.schema,
                        tablename=in_table.tablename.lower(),
                        colname=in_c.lower())
            # remove columns that aren't in input table
            if session.execute(cmd).fetchone() is None:
                in_colnames[i] = None
                out_colnames[i] = None
        in_colnames = [
            "CASE {ic}::TEXT WHEN '-6' THEN NULL ELSE {ic} END".format(ic=ic) for ic in in_colnames if ic is not None]
        out_colnames = [oc for oc in out_colnames if oc is not None]

        cmd = 'INSERT INTO {output} ({out_colnames}) ' \
                'SELECT {in_colnames} FROM {input} '.format(
                    output=self.output().table,
                    input=in_table.table,
                    in_colnames=', '.join(in_colnames),
                    out_colnames=', '.join(out_colnames))
        session.execute(cmd)


class AllGeographies(WrapperTask):

    def requires(self):
        for resolution in RESOLUTIONS:
            yield Geography(resolution=resolution)


class AllCensus(WrapperTask):
    # skip localidad_urbana_y_rural_amanzanada

    def requires(self):
        for resolution in RESOLUTIONS:
            if resolution == 'servicios_area':
                continue
            for table in DEMOGRAPHIC_TABLES.keys():
                yield Census(resolution=resolution, table=table)


class CensusWrapper(MetaWrapper):

    resolution = Parameter()
    table = Parameter()

    params = {
        'resolution': set(RESOLUTIONS) - set(['servicios_area']),
        'table': DEMOGRAPHIC_TABLES.keys()
    }

    def tables(self):
        yield Geography(resolution=self.resolution)
        yield Census(resolution=self.resolution, table=self.table)
