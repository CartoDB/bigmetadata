from luigi import Task, Parameter, WrapperTask

from tasks.util import (DownloadUnzipTask, shell, Shp2TempTableTask,
                        ColumnsTask, TableTask, TempTableTask)
from tasks.meta import GEOM_REF, OBSColumn, current_session
# from tasks.mx.inegi_columns import DemographicColumns

from collections import OrderedDict


GEOGRAPHIES = (
    'ct',
    'pr',
)


GEOGRAPHY_NAMES = {
    'ct': 'Census Tracts',
    'pr': 'Provinces/Territories',
}

GEOGRAPHY_DESCS = {
    'ct': '',
    'pr': '',
}


# http://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/bound-limit-2011-eng.cfm
# 2011 Boundary Files
class DownloadGeography(DownloadUnzipTask):

    geog_lvl = Parameter()

    URL = 'http://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/g{geog_lvl}_000b11a_e.zip'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(
            output=self.output().path, url=self.URL.format(geog_lvl=self.geog_lvl)
        ))


# class ImportGeography2(TempTableTask):

#     geog_lvl = Parameter()

#     def requires(self):
#         return DownloadGeography(geog_lvl=self.geog_lvl)

#     def run(self):
#         cmd =   'PG_USE_COPY=yes PGCLIENTENCODING=latin1 ' \
#                 'ogr2ogr -f PostgreSQL PG:dbname=$PGDATABASE ' \
#                 '-t_srs "EPSG:4326" -nlt MultiPolygon -nln {table} ' \
#                 '-lco OVERWRITE=yes ' \
#                 '-lco SCHEMA={schema} -lco PRECISION=no ' \
#                 '$(dirname {input})/$(basename {input})/*.shp '.format(
#                     schema=self.output().schema,
#                     table=self.output().tablename,
#                     input=self.input().path)
#         shell(cmd)


class ImportGeography(Shp2TempTableTask):
    '''
    Import geographies into postgres by geography level
    '''

    geog_lvl = Parameter()

    def requires(self):
        return DownloadGeography(geog_lvl=self.geog_lvl)

    def input_shp(self):
        cmd = 'ls {input}/*.shp'.format(
            input=self.input().path
        )
        for shp in shell(cmd).strip().split('\n'):
            print shp
            yield shp


class GeographyColumns(ColumnsTask):

    geog_lvl = Parameter()

    weights = {
        'ct': 5,
        'pr': 4,
    }

    def version(self):
        return 1

    def columns(self):
        geom = OBSColumn(
            id=self.geog_lvl,
            type='Geometry',
            name=GEOGRAPHY_NAMES[self.geog_lvl],
            description=GEOGRAPHY_DESCS[self.geog_lvl],
            weight=self.weights[self.geog_lvl],
        )
        geom_ref = OBSColumn(
            id=self.geog_lvl + '_geom_id',
            type='Text',
            weight=0,
            targets={geom: GEOM_REF},
        )
        name = OBSColumn(
            type='Text',
            weight=0,
        )
        return OrderedDict([
            ('geom', geom),     # the_geom
            ('geom_id', geom_ref),   # cvegeo
            ('geom_name', name),       # nomgeo
        ])


class Geography(TableTask):
    '''
    '''

    geog_lvl = Parameter()

    def version(self):
        return 1

    def requires(self):
        return {
            'data': ImportGeography(geog_lvl=self.geog_lvl),
            'columns': GeographyColumns(geog_lvl=self.geog_lvl)
        }

    def timespan(self):
        return 2011

    def columns(self):
        return self.input()['columns']

    def populate(self):
        session = current_session()
        session.execute('INSERT INTO {output} (geom, geom_id) '
                        'SELECT wkb_geometry, geom_id '
                        'FROM {input} '.format(
                            output=self.output().table,
                            input=self.input()['data'].table))


class AllGeographies(WrapperTask):

    def requires(self):
        for geog_lvl in GEOGRAPHIES:
            yield Geography(geog_lvl=geog_lvl)
