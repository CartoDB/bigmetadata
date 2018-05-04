# -*- coding: utf-8 -*-

from luigi import Task, Parameter, LocalTarget, WrapperTask

from tasks.base_tasks import (ColumnsTask, TableTask, TagsTask, Shp2TempTableTask, SimplifiedTempTableTask,
                              RepoFile)
from lib.timespan import get_timespan
from tasks.util import shell, classpath

from tasks.tags import SectionTags, SubsectionTags, BoundaryTags
from tasks.meta import OBSTable, OBSColumn, GEOM_REF, GEOM_NAME, OBSTag, current_session

from shutil import copyfile
from collections import OrderedDict
import os


class DownloadGeometry(Task):

    seq = Parameter()
    # request url: http://centrodedescargas.cnig.es/CentroDescargas/descargaDir
    # arguments:
    # secDescDirLA:114023
    # pagActual:1
    # numTotReg:5
    # codSerieSel:CAANE
    URL = 'http://centrodedescargas.cnig.es/CentroDescargas/descargaDir?secDescDirLA={seq}&pagActual=1&numTotReg=5&codSerieSel=CAANE'

    def requires(self):
        return RepoFile(resource_id=self.task_id, version=self.version(), url=self.URL.format(seq=self.seq))

    def version(self):
        return 1

    def run(self):
        self.output().makedirs()
        copyfile(self.input().path, '{output}.zip'.format(output=self.output().path))
        shell('unzip -d {output} {output}.zip'.format(output=self.output().path))

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.seq).lower())


class ImportGeometry(Shp2TempTableTask):

    resolution = Parameter()
    timestamp = Parameter()
    id_aux = Parameter()  # X for Peninsula and Balearic Islands, Y for Canary Islands

    def requires(self):
        return DownloadGeometry(seq='114023')

    def input_shp(self):
        '''
        We don't know precise name of file inside zip archive beforehand, so
        use find to track it down.
        '''
        return shell("find '{dirpath}' -iname *_{resolution}_*_{aux}.shp | grep {timestamp}".format(
            dirpath=self.input().path,
            timestamp=self.timestamp,
            aux=self.id_aux,
            resolution=self.resolution
        )).strip()


class SimplifiedImportGeometry(SimplifiedTempTableTask):
    resolution = Parameter()
    timestamp = Parameter()
    id_aux = Parameter()  # X for Peninsula and Balearic Islands, Y for Canary Islands

    def requires(self):
        return ImportGeometry(resolution=self.resolution, timestamp=self.timestamp, id_aux=self.id_aux)


class LicenseTags(TagsTask):

    def version(self):
        return 1

    def tags(self):
        return [
            OBSTag(id='cnig-license',
                    name='License of CNIG',
                    type='license',
                   description='Royal Decree 663/2007, more information `here <https://www.cnig.es/propiedadIntelectual.do>`_.'
                    )]


class SourceTags(TagsTask):

    def version(self):
        return 1

    def tags(self):
        return [
            OBSTag(id='cnig-source',
                    name='National Center for Geographic Information (CNIG)',
                    type='source',
                    description='`The National Center for Geographic Information (CNIG) <https://www.cnig.es/>`_'
                    )]


class GeometryColumns(ColumnsTask):

    def version(self):
        return 5

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'source': SourceTags(),
            'license': LicenseTags(),
            'boundary': BoundaryTags(),
        }

    def columns(self):
        input_ = self.input()
        sections = input_['sections']
        subsections = input_['subsections']
        source = input_['source']['cnig-source']
        license = input_['license']['cnig-license']
        boundary_type = input_['boundary']

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
            tags=[sections['spain'], subsections['boundary'], boundary_type['cartographic_boundary'], boundary_type['interpolation_boundary']],
        )
        muni = OBSColumn(
            type='Geometry',
            name='Municipality',
            weight=8,
            description='The lowest level of territorial organization in Spain. '
                        'Municipal boundaries do not cross between provinces. ',
            tags=[sections['spain'], subsections['boundary'], boundary_type['cartographic_boundary'], boundary_type['interpolation_boundary']],
        )
        columns = OrderedDict([
            ('ccaa', ccaa),
            ('prov', prov),
            ('muni', muni),
        ])

        for _, col in columns.items():
            col.tags.append(source)
            col.tags.append(license)

        return columns


class GeomRefColumns(ColumnsTask):

    def version(self):
        return 1

    def requires(self):
        return {
            'geom_cols':GeometryColumns(),
            'subsections': SubsectionTags(),
            'sections': SectionTags(),
            }

    def columns(self):
        cols = OrderedDict()
        for colname, coltarget in self.input()['geom_cols'].items():
            cols['id_' + colname] = OBSColumn(
                type='Text',
                name='',
                weight=0,
                targets={coltarget: GEOM_REF},
            )
        return cols


class GeomNameColumns(ColumnsTask):

    def version(self):
        return 1

    def requires(self):
        return {
                'geom_cols':GeometryColumns(),
                'subsections':SubsectionTags(),
                'sections':SectionTags(),
            }

    def columns(self):
        cols = OrderedDict()
        for colname, coltarget in self.input()['geom_cols'].items():
            cols['name_' + colname] = OBSColumn(
                id='name_' + colname,
                type='Text',
                name='Proper name of {}'.format(colname),
                tags=[self.input()['subsections']['names'],self.input()['sections']['spain']],
                weight=1,
                targets={coltarget: GEOM_NAME},
            )
        return cols


class Geometry(TableTask):

    resolution = Parameter()
    timestamp = Parameter(default='20150101')

    def version(self):
        return 13

    # TODO: https://github.com/CartoDB/bigmetadata/issues/435
    def targets(self):
        return {
            OBSTable(id='.'.join([self.schema(), self.name()])): GEOM_REF,
        }

    def requires(self):
        return {
            'geom_columns': GeometryColumns(),
            'geomref_columns': GeomRefColumns(),
            'geomname_columns': GeomNameColumns(),
            'peninsula_data': SimplifiedImportGeometry(resolution=self.resolution,
                                                       timestamp=self.timestamp,
                                                       id_aux='x'),
            'canary_data': SimplifiedImportGeometry(resolution=self.resolution,
                                                    timestamp=self.timestamp,
                                                    id_aux='y')
        }

    def table_timespan(self):
        return get_timespan(str(self.timestamp))

    def columns(self):
        return OrderedDict([
            ('geom_ref', self.input()['geomref_columns']['id_' + self.resolution]),
            ('rotulo', self.input()['geomname_columns']['name_' + self.resolution]),
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
        peninsula_query = 'INSERT INTO {output} ' \
                'SELECT {geom_ref_colname} geom_ref, rotulo, wkb_geometry the_geom ' \
                'FROM {input}'.format(
                    output=self.output().table,
                    input=self.input()['peninsula_data'].table,
                    geom_ref_colname=self.geom_ref_colname())
        canary_query = 'INSERT INTO {output} ' \
                'SELECT {geom_ref_colname} geom_ref, rotulo, wkb_geometry the_geom ' \
                'FROM {input}'.format(
                    output=self.output().table,
                    input=self.input()['canary_data'].table,
                    geom_ref_colname=self.geom_ref_colname())
        session.execute(peninsula_query)
        session.execute(canary_query)


class AllGeometries(WrapperTask):

    def requires(self):
        for resolution in ('ccaa', 'muni', 'prov', ):
            yield Geometry(resolution=resolution)
