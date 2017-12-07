from collections import OrderedDict
from luigi import Parameter, WrapperTask

from lib.timespan import get_timespan
from tasks.base_tasks import (ColumnsTask, DownloadUnzipTask, Shp2TempTableTask, TableTask, MetaWrapper,
                              SimplifiedTempTableTask)
from tasks.util import shell
from tasks.meta import GEOM_REF, GEOM_NAME, OBSColumn, current_session
from tasks.mx.inegi_columns import DemographicColumns
from tasks.tags import SectionTags, SubsectionTags, BoundaryTags
from tasks.mx.inegi_columns import SourceTags, LicenseTags

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

RESPROPNAMES = (
    'entidad',
    'localidad_urbana_y_rural_amanzanada',
    'municipio'
)

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

SCINCE_DIRECTORY = 'scince_2010'

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


class DownloadGeographies(DownloadUnzipTask):
    """
        https://blog.diegovalle.net/2016/01/encuesta-intercensal-2015-shapefiles.html
        2015 Encuesta Intercensal AGEBs, Manzanas, Municipios, States, etc
    """
    URL = 'http://data.diegovalle.net/mapsmapas/encuesta_intercensal_2015.zip'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(
            output=self.output().path, url=self.URL
        ))


class DownloadDemographicData(DownloadUnzipTask):
    """
        https://blog.diegovalle.net/2013/06/shapefiles-of-mexico-agebs-manzanas-etc.html
        2010 Census AGEBs, Manzanas, Municipios, States, etc
    """
    URL = 'http://data.diegovalle.net/mapsmapas/agebsymas.zip'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(
            output=self.output().path, url=self.URL
        ))


class DownloadElectoralDistricts(DownloadUnzipTask):
    """
        http://blog.diegovalle.net/2013/02/download-shapefiles-of-mexico.html
        Electoral shapefiles of Mexico (secciones and distritos)
    """
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


class SimplifiedImportGeography(SimplifiedTempTableTask):
    resolution = Parameter()

    def requires(self):
        return ImportGeography(resolution=self.resolution)


class ImportDemographicData(Shp2TempTableTask):

    resolution = Parameter()
    table = Parameter()

    def requires(self):
        return DownloadDemographicData()

    def input_shp(self):
        cmd = 'ls {input}/{scince}/shps/'.format(
            input=self.input().path,
            scince=SCINCE_DIRECTORY
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
                path = 'ls {input}/{scince}/shps/{ent}/{ent}_{resolution}*.dbf'
            else:
                path = 'ls {{input}}/{{scince}}/shps/{{ent}}/tablas/' \
                    '{{ent}}_cpv2010_{{resolution}}*_{table}.dbf'.format(
                        table=DEMOGRAPHIC_TABLES[self.table])
            cmd = path.format(
                input=self.input().path,
                ent=ent,
                resolution=resolution,
                scince=SCINCE_DIRECTORY,
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
        return 11

    def columns(self):
        input_ = self.input()
        sections = input_['sections']
        subsections = input_['subsections']
        license_data = input_['license']['inegi-license']
        source = input_['source']['inegi-source']
        boundary_type = input_['boundary']
        geom = OBSColumn(
            id=self.resolution,
            type='Geometry',
            name=RESNAMES[self.resolution],
            description=RESDESCS[self.resolution],
            weight=self.weights[self.resolution],
            tags=[sections['mx'], subsections['boundary'], license_data, source]
        )
        geom_ref = OBSColumn(
            id=self.resolution + '_cvegeo',
            type='Text',
            weight=0,
            targets={geom: GEOM_REF},
        )
        cols = OrderedDict([('the_geom', geom),
                            ('cvegeo', geom_ref)])

        if self.resolution in RESPROPNAMES:
            cols['nomgeo'] = OBSColumn(
                id=self.resolution + '_name',
                type='Text',
                weight=1,
                name='Name of {}'.format(RESNAMES[self.resolution]),
                tags=[sections['mx'], subsections['names'], license_data, source],
                targets={geom: GEOM_NAME}
            )

        geom.tags.extend(boundary_type[i] for i in RESTAGS[self.resolution])

        return cols


class Geography(TableTask):

    resolution = Parameter()

    def version(self):
        return 6

    def requires(self):
        return {
            'data': SimplifiedImportGeography(resolution=self.resolution),
            'columns': GeographyColumns(resolution=self.resolution)
        }

    def table_timespan(self):
        return get_timespan('2015')

    def columns(self):
        return self.input()['columns']

    def populate(self):
        session = current_session()
        column_targets = self.columns()
        output_cols = list(column_targets.keys())
        input_cols = ['ST_MakeValid(wkb_geometry)', 'cvegeo']
        if self.resolution in RESPROPNAMES:
            input_cols.append('nomgeo')

        session.execute('INSERT INTO {output} ({output_cols}) '
                        'SELECT {input_cols} '
                        'FROM {input} '.format(
                            output=self.output().table,
                            input=self.input()['data'].table,
                            output_cols=', '.join(output_cols),
                            input_cols=', '.join(input_cols)))


class Census(TableTask):

    resolution = Parameter()
    table = Parameter()

    def version(self):
        return 6

    def targets(self):
        return {
            self.input()['geo'].obs_table: GEOM_REF,
        }

    def table_timespan(self):
        return get_timespan('2010')

    def requires(self):
        return {
            'data': ImportDemographicData(resolution=self.resolution,
                                          table=self.table),
            'meta': DemographicColumns(resolution=self.resolution, table=self.table),
            'geometa': GeographyColumns(resolution=self.resolution),
            'geo': Geography(resolution=self.resolution),
        }

    def columns(self):
        input_ = self.input()
        cols = OrderedDict()
        cols['cvegeo'] = input_['geometa']['cvegeo']
        for colname, coltarget in input_['meta'].items():
            if coltarget._id.split('.')[-1].lower().startswith(self.table.lower()):
                cols[colname] = coltarget
        return cols

    def populate(self):
        session = current_session()
        columns = self.columns()
        out_colnames = list(columns.keys())
        in_table = self.input()['data']
        in_colnames = [ct._id.split('.')[-1] for ct in list(columns.values())]
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
            for key, _ in DEMOGRAPHIC_TABLES.items():
                yield Census(resolution=resolution, table=key)


class CensusWrapper(MetaWrapper):

    resolution = Parameter()
    table = Parameter()

    params = {
        'resolution': set(RESOLUTIONS) - set(['servicios_area']),
        'table': list(DEMOGRAPHIC_TABLES.keys())
    }

    def tables(self):
        yield Geography(resolution=self.resolution)
        yield Census(resolution=self.resolution, table=self.table)
