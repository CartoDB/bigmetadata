# -*- coding: utf-8 -*-
# #http://centrodedescargas.cnig.es/CentroDescargas/inicio.do

from luigi import Task, Parameter, LocalTarget, WrapperTask
from tasks.util import (ColumnsTask, TableTask, shell, classpath,
                        Shp2TempTableTask, current_session)

from tasks.tags import SectionTags, SubsectionTags, UnitTags, LicenseTags
from tasks.meta import OBSColumn, GEOM_REF

from collections import OrderedDict
import os


class DownloadGeometry(Task):

    seq = Parameter()

    #http://centrodedescargas.cnig.es/CentroDescargas/downloadFile.do?seq=114023
    URL = 'http://centrodedescargas.cnig.es/CentroDescargas/downloadFile.do?seq={seq}'

    def run(self):
        self.output().makedirs()
        shell('wget -O {output}.zip {url}'.format(output=self.output().path,
                                                  url=self.URL.format(seq=self.seq)))
        os.makedirs(self.output().path)
        shell('unzip -d {output} {output}.zip'.format(output=self.output().path))

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.seq))


class ImportGeometry(Shp2TempTableTask):

    resolution = Parameter()
    timestamp = Parameter()

    def requires(self):
        return DownloadGeometry(seq='114023')

    def input_shp(self):
        path = os.path.join('SIANE_CARTO_BASE_S_3M', 'anual', self.timestamp,
                            'SE89_3_ADMIN_{resolution}_A_X.shp'.format(
                                resolution=self.resolution.upper()))
        return os.path.join(self.input().path, path)


class GeometryColumns(ColumnsTask):

    def version(self):
        return 3

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
        }

    def columns(self):
        sections = self.input()['sections']
        subsections = self.input()['subsections']
        ccaa = OBSColumn(
            type='Geometry',
            name='Autonomous Community',
            weight=6,
            description='The first-level administrative subdivision of Spain. ',
            tags=[sections['spain'], subsections['boundary']],
        )
        prov = OBSColumn(
            type='Geometry',
            name='Province',
            weight=7,
            description='The second-level administrative subdivision of Spain, '
                        'used primarily as electoral districts and geographic '
                        'references.  Provinces do not cross between autonomous '
                        'communities.',
            tags=[sections['spain'], subsections['boundary']],
        )
        muni = OBSColumn(
            type='Geometry',
            name='Municipality',
            weight=8,
            description='The lowest level of territorial organization in Spain. '
                        'Municipal boundaries do not cross between provinces. ',
            tags=[sections['spain'], subsections['boundary']],
        )
        return OrderedDict([
            ('ccaa', ccaa),
            ('prov', prov),
            ('muni', muni),
        ])


class GeomRefColumns(ColumnsTask):

    def version(self):
        return 1

    def requires(self):
        return GeometryColumns()

    def columns(self):
        cols = OrderedDict()
        session = current_session()
        for colname, coltarget in self.input().iteritems():
            cols['id_' + colname] = OBSColumn(
                type='Text',
                name='',
                weight=0,
                targets={coltarget: GEOM_REF},
            )
        return cols


class Geometry(TableTask):

    resolution = Parameter()
    timestamp = Parameter(default='20150101')

    def version(self):
        return 1

    def requires(self):
        return {
            'geom_columns': GeometryColumns(),
            'geomref_columns': GeomRefColumns(),
            'data': ImportGeometry(resolution=self.resolution,
                                   timestamp=self.timestamp)
        }

    def bounds(self):
        return 'BOX(-18.1608741903971 27.6377376448416,4.32788958435346 43.7899939997615)'

    def timespan(self):
        return self.timestamp

    def columns(self):
        return OrderedDict([
            ('geom_ref', self.input()['geomref_columns']['id_' + self.resolution]),
            ('the_geom', self.input()['geom_columns'][self.resolution])
        ])

    def geom_ref_colname(self):
        if self.resolution.lower() == 'ccaa':
            return 'id_ccaa'
        elif self.resolution.lower() == 'prov':
            return 'id_prov'
        elif self.resolution.lower() == 'muni':
            return 'id_ine'
        else:
            raise 'Unknown resolution {resolution}'.format(resolution=self.resolution)

    def populate(self):
        session = current_session()
        query = 'INSERT INTO {output} ' \
                'SELECT {geom_ref_colname} geom_ref, wkb_geometry the_geom ' \
                'FROM {input}'.format(
                    output=self.output().table,
                    input=self.input()['data'].table,
                    geom_ref_colname=self.geom_ref_colname())
        session.execute(query)


class AllGeometries(WrapperTask):

    def requires(self):
        for resolution in ('ccaa', 'muni', 'prov', ):
            yield Geometry(resolution=resolution)

