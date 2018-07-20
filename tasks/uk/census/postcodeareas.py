from tasks.base_tasks import (DownloadUnzipTask, RepoFile, Shp2TempTableTask, ColumnsTask, TagsTask,
                              TableTask, SimplifiedTempTableTask)
from tasks.meta import current_session
from tasks.util import copyfile
from tasks.meta import GEOM_REF, OBSColumn, OBSTag, OBSTable
from tasks.tags import SectionTags, SubsectionTags, LicenseTags, BoundaryTags
from lib.timespan import get_timespan

from collections import OrderedDict


class DownloadPostcodeAreas(DownloadUnzipTask):

    # Source: https://datashare.is.ed.ac.uk/handle/10283/2405
    URL = 'https://datashare.is.ed.ac.uk/bitstream/handle/10283/2405/gb_postcode_areas.zip?sequence=1&isAllowed=y'

    def version(self):
        return 1

    def requires(self):
        return RepoFile(resource_id=self.task_id,
                        version=self.version(),
                        url=self.URL)

    def download(self):
        copyfile(self.input().path, '{output}.zip'.format(output=self.output().path))


class ImportPostcodeAreas(Shp2TempTableTask):

    other_options = '-s_srs "EPSG:27700"'

    def requires(self):
        return DownloadPostcodeAreas()

    def input_shp(self):
        return self.input().path


class SimplifiedPostcodeAreas(SimplifiedTempTableTask):
    def requires(self):
        return ImportPostcodeAreas()


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
            type='Text',
            name='GB Postcode Area ID',
            weight=0,
            targets={geom: GEOM_REF}
        )
        geomname = OBSColumn(
            type='Text',
            name='GB Postcode Area Name',
            weight=0,
            targets={geom: GEOM_REF}
        )

        return OrderedDict([
            ('the_geom', geom),
            ('pa_id', geomref),
            ('name', geomname),
        ])


class PostcodeAreas(TableTask):

    def requires(self):
        return {
            'geom_columns': PostcodeAreasColumns(),
            'data': SimplifiedPostcodeAreas(),
        }

    def version(self):
        return 1

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
