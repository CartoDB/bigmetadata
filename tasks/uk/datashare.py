from tasks.base_tasks import (RepoFileUnzipTask, GeoFile2TempTableTask, ColumnsTask, TagsTask,
                              TableTask, SimplifiedTempTableTask, TempTableTask)
from tasks.meta import current_session
from tasks.util import copyfile
from tasks.meta import GEOM_REF, OBSColumn, OBSTag, OBSTable
from tasks.tags import SectionTags, SubsectionTags, LicenseTags, BoundaryTags
from lib.timespan import get_timespan
from tasks.uk.shoreline import Shoreline

from collections import OrderedDict


class DownloadPostcodeAreas(RepoFileUnzipTask):

    # Source: https://datashare.is.ed.ac.uk/handle/10283/2405
    URL = 'https://datashare.is.ed.ac.uk/bitstream/handle/10283/2405/gb_postcode_areas.zip?sequence=1&isAllowed=y'

    def get_url(self):
        return self.URL


class ImportPostcodeAreas(GeoFile2TempTableTask):

    other_options = '-s_srs "EPSG:27700"'

    def requires(self):
        return DownloadPostcodeAreas()

    def input_files(self):
        return self.input().path


class ShorelineClippedPostcodeAreas(TempTableTask):
    def requires(self):
        return {
            'shoreline': Shoreline(),
            'postcode_area': ImportPostcodeAreas(),
        }

    def version(self):
        return 1

    def run(self):
        session = current_session()

        stmt = '''
                CREATE TABLE {output_table} AS
                SELECT name, label, ST_Intersection(pa.wkb_geometry, shore.the_geom) wkb_geometry
                  FROM {postcode_areas_table} pa,
                       {shoreline_table} shore
               '''.format(
                   output_table=self.output().table,
                   postcode_areas_table=self.input()['postcode_area'].table,
                   shoreline_table=self.input()['shoreline'].table,
               )

        session.execute(stmt)


class SimplifiedPostcodeAreas(SimplifiedTempTableTask):
    def requires(self):
        return ShorelineClippedPostcodeAreas()


class SimplifiedUniquePostcodeAreas(TempTableTask):
    def requires(self):
        return SimplifiedPostcodeAreas()

    def run(self):
        session = current_session()
        session.execute('''
            CREATE TABLE {output} AS
            SELECT name, label, st_union(wkb_geometry) wkb_geometry
            FROM {input}
            GROUP BY name, label
            '''.format(
            output=self.output().table,
            input=self.input().table,
        ))

        session.commit()


class SourceTags(TagsTask):

    def version(self):
        return 1

    def tags(self):
        return[
            OBSTag(id='uk_ed_datashare_source',
                   name='Edinburgh DataShare',
                   type='source',
                   description='''
                               Edinburgh DataShare is a digital repository of research data produced at the University of Edinburgh, hosted by Information Services.
                               Edinburgh University researchers who have produced research data associated with an existing or forthcoming publication, or which has potential use for other researchers, are invited to upload their dataset for sharing and safekeeping.
                               A persistent identifier and suggested citation will be provided.
                               ''')
                   ]


class PostcodeAreasColumns(ColumnsTask):

    def version(self):
        return 1

    def requires(self):
        return {
            'subsections': SubsectionTags(),
            'sections': SectionTags(),
            'source': SourceTags(),
            'license': LicenseTags(),
            'boundary': BoundaryTags(),
        }

    def columns(self):
        input_ = self.input()
        license = input_['license']['uk_ed_datashare']
        source = input_['source']['uk_ed_datashare_source']
        boundary_type = input_['boundary']
        geom = OBSColumn(
            id='pa_geo',
            type='Geometry',
            name='GB Postcode Areas',
            description='''
                        Dataset contains polygons corresponding to the top level (Postcode Area) of the GB Postcode Hierarchy.
                        Thiessen polygons were generated from all records in OS OpenData Code-Point Open.
                        Resulting coverage was dissolved up to postcode area level and then clipped to GB extent of the realm using OS OpenData Boundary-Line.
                        Names of postcode areas were added from Wikipedia. GIS vector data.
                        This dataset was first accessioned in the EDINA ShareGeo Open repository on 2010-07-22 and migrated to Edinburgh DataShare on 2017-02-21.
                        ''',
            weight=8,
            tags=[input_['subsections']['boundary'], input_['sections']['uk'], source, license,
                  boundary_type['cartographic_boundary'], boundary_type['interpolation_boundary']]
        )
        geomref = OBSColumn(
            id='pa_id',
            type='Text',
            name='GB Postcode Area ID',
            weight=0,
            targets={geom: GEOM_REF}
        )
        geomname = OBSColumn(
            id='pa_name',
            type='Text',
            name='GB Postcode Area Name',
            weight=1,
            targets={geom: GEOM_REF},
            tags=[input_['sections']['uk'], input_['subsections']['boundary'], source, license]
        )

        return OrderedDict([
            ('the_geom', geom),
            ('pa_id', geomref),
            ('name', geomname),
        ])

    @staticmethod
    def geoname_column():
        return 'name'

    @staticmethod
    def geoid_column():
        return 'pa_id'


class PostcodeAreas(TableTask):

    def requires(self):
        return {
            'geom_columns': PostcodeAreasColumns(),
            'data': SimplifiedUniquePostcodeAreas(),
        }

    def version(self):
        return 2

    def table_timespan(self):
        return get_timespan('2017')

    # TODO: https://github.com/CartoDB/bigmetadata/issues/435
    def targets(self):
        return {
            OBSTable(id='.'.join([self.schema(), self.name()])): GEOM_REF,
        }

    def columns(self):
        input_ = self.input()
        cols = OrderedDict()
        cols.update(input_['geom_columns'])
        return cols

    def populate(self):
        session = current_session()
        session.execute('INSERT INTO {output} '
                        'SELECT ST_MakeValid(wkb_geometry) as the_geom, label pa_id, name '
                        'FROM {input}'.format(
                            output=self.output().table,
                            input=self.input()['data'].table,
                        ))

    @staticmethod
    def geoid_column():
        return PostcodeAreasColumns.geoid_column()

    @staticmethod
    def geoname_column():
        return PostcodeAreasColumns.geoname_column()

