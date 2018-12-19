from tasks.base_tasks import (RepoFileUnzipTask, GeoFile2TempTableTask, SimplifiedTempTableTask, TagsTask,
                              ColumnsTask, TableTask)
from tasks.tags import SectionTags, SubsectionTags, LicenseTags, BoundaryTags
from tasks.meta import GEOM_REF, OBSTag, OBSColumn, OBSTable, current_session
from tasks.util import copyfile
from lib.timespan import get_timespan

from luigi import WrapperTask
from collections import OrderedDict
import os


class ODLSourceTags(TagsTask):
    def version(self):
        return 1

    def tags(self):
        return[
            OBSTag(id='uk_os_opendata_source',
                   name='UK OS OpenData',
                   type='source',
                   description='''
                               Contains Ordnance Survey data (c) Crown copyright and database right 2015
                               Contains Royal Mail data (c) Royal Mail copyright and database right 2015
                               Contains National Statistics data (c) Crown copyright and database right 2015
                               ''')
                   ]


class DownloadPostcodeBoundaries(RepoFileUnzipTask):

    # Source: https://www.opendoorlogistics.com/downloads/
    URL = 'https://www.opendoorlogistics.com/wp-content/uploads/Data/UK-postcode-boundaries-Jan-2015.zip'

    def get_url(self):
        return self.URL


class ImportPostcodeDistricts(GeoFile2TempTableTask):

    def requires(self):
        return DownloadPostcodeBoundaries()

    def input_files(self):
        return os.path.join(self.input().path, 'Distribution', 'Districts.shp')


class SimplifiedPostcodeDistricts(SimplifiedTempTableTask):
    def requires(self):
        return ImportPostcodeDistricts()


class PostcodeDistrictsColumns(ColumnsTask):

    def version(self):
        return 1

    def requires(self):
        return {
            'subsections': SubsectionTags(),
            'sections': SectionTags(),
            'source': ODLSourceTags(),
            'license': LicenseTags(),
            'boundary': BoundaryTags(),
        }

    def columns(self):
        input_ = self.input()
        license = input_['license']['uk_ogl']
        source = input_['source']['uk_os_opendata_source']
        boundary_type = input_['boundary']
        geom = OBSColumn(
            id='pd_geo',
            type='Geometry',
            name='Reconstructed UK Postcode Districts',
            description='Reconstructed UK Postcode Districts',
            weight=8,
            tags=[input_['subsections']['boundary'], input_['sections']['uk'], source, license,
                  boundary_type['cartographic_boundary'], boundary_type['interpolation_boundary']]
        )
        geomref = OBSColumn(
            id='pd_id',
            type='Text',
            name='Postcode District ID',
            weight=0,
            targets={geom: GEOM_REF},
        )
        geomname = OBSColumn(
            id='pd_name',
            type='Text',
            name='Postcode District ID',
            weight=1,
            targets={geom: GEOM_REF},
            tags=[input_['sections']['uk'], input_['subsections']['boundary'], source, license]
        )

        return OrderedDict([
            ('the_geom', geom),
            ('geographycode', geomref),
            ('name', geomname)
        ])


class PostcodeDistricts(TableTask):

    def requires(self):
        return {
            'columns': PostcodeDistrictsColumns(),
            'data': SimplifiedPostcodeDistricts(),
        }

    def version(self):
        return 1

    def table_timespan(self):
        return get_timespan('2015')

    # TODO: https://github.com/CartoDB/bigmetadata/issues/435
    def targets(self):
        return {
            OBSTable(id='.'.join([self.schema(), self.name()])): GEOM_REF,
        }

    def columns(self):
        input_ = self.input()
        cols = OrderedDict()
        cols.update(input_['columns'])
        return cols

    def populate(self):
        session = current_session()

        query = '''
                INSERT INTO {output}
                SELECT ST_MakeValid(wkb_geometry), name, name
                FROM {input}
                '''.format(output=self.output().table,
                           input=self.input()['data'].table)

        session.execute(query)


class ImportPostcodeSectors(GeoFile2TempTableTask):

    def requires(self):
        return DownloadPostcodeBoundaries()

    def input_files(self):
        return os.path.join(self.input().path, 'Distribution', 'Sectors.shp')


class SimplifiedPostcodeSectors(SimplifiedTempTableTask):
    def requires(self):
        return ImportPostcodeSectors()


class PostcodeSectorsColumns(ColumnsTask):

    def version(self):
        return 1

    def requires(self):
        return {
            'subsections': SubsectionTags(),
            'sections': SectionTags(),
            'source': ODLSourceTags(),
            'license': LicenseTags(),
            'boundary': BoundaryTags(),
        }

    def columns(self):
        input_ = self.input()
        license = input_['license']['uk_ogl']
        source = input_['source']['uk_os_opendata_source']
        boundary_type = input_['boundary']
        geom = OBSColumn(
            id='ps_geo',
            type='Geometry',
            name='Reconstructed UK Postcode Sectors',
            description='Reconstructed UK Postcode Sectors',
            weight=8,
            tags=[input_['subsections']['boundary'], input_['sections']['uk'], source, license,
                  boundary_type['cartographic_boundary'], boundary_type['interpolation_boundary']]
        )
        geomref = OBSColumn(
            id='ps_id',
            type='Text',
            name='Postcode Sector ID',
            weight=0,
            targets={geom: GEOM_REF},
        )
        geomname = OBSColumn(
            id='ps_name',
            type='Text',
            name='Postcode Sector ID',
            weight=1,
            targets={geom: GEOM_REF},
            tags=[input_['sections']['uk'], input_['subsections']['boundary'], source, license]
        )

        return OrderedDict([
            ('the_geom', geom),
            ('geographycode', geomref),
            ('name', geomname)
        ])


class PostcodeSectors(TableTask):

    def requires(self):
        return {
            'columns': PostcodeSectorsColumns(),
            'data': SimplifiedPostcodeSectors(),
        }

    def version(self):
        return 1

    def table_timespan(self):
        return get_timespan('2015')

    # TODO: https://github.com/CartoDB/bigmetadata/issues/435
    def targets(self):
        return {
            OBSTable(id='.'.join([self.schema(), self.name()])): GEOM_REF,
        }

    def columns(self):
        input_ = self.input()
        cols = OrderedDict()
        cols.update(input_['columns'])
        return cols

    def populate(self):
        session = current_session()

        query = '''
                INSERT INTO {output}
                SELECT ST_MakeValid(wkb_geometry), replace(name, ' ', '_'), name
                FROM {input}
                '''.format(output=self.output().table,
                           input=self.input()['data'].table)

        session.execute(query)


class ODLWrapper(WrapperTask):
    def requires(self):
        yield PostcodeDistricts()
        yield PostcodeSectors()
