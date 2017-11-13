import os

from tasks.base_tasks import ColumnsTask, DownloadUnzipTask, GdbFeatureClass2TempTableTask, TagsTask, TableTask
from tasks.meta import OBSColumn, OBSTag, GEOM_REF, current_session
from tasks.util import shell
from tasks.tags import SubsectionTags, SectionTags, LicenseTags

from collections import OrderedDict


class DownloadHUC(DownloadUnzipTask):

    URL = 'ftp://newftp.epa.gov/epadatacommons/ORD/EnviroAtlas/NHDPlusV2_WBDSnapshot_EnviroAtlas_CONUS.gdb.zip'

    def download(self):
        shell('curl -o "{output}".zip "{url}"'.format(
            url=self.URL,
            output=self.output().path
        ))


class ImportHUC(GdbFeatureClass2TempTableTask):

    feature_class = 'nhdplusv2_wbdsnapshot_enviroatlas_conus'

    def requires(self):
        return DownloadHUC()

    def input_gdb(self):
        return os.path.join(self.input().path, 'NHDPlusV2_WBDSnapshot_EnviroAtlas_CONUS.gdb')


class SourceTags(TagsTask):

    def tags(self):
        return [
            OBSTag(id='epa-enviroatlas',
                   name='United States Environmental Protection Agency (EPA) EnviroAtlas',
                   description='''EnviroAtlas is a collaborative project
developed by researchers at EPA in cooperation with the U.S. Geological Survey
(USGS), the U.S. Department of Agriculture's Forest Service and Natural
Resources Conservation Service (NRCS), and Landscope America.  The data
download page can be found `here <https://www.epa.gov/enviroatlas/forms/enviroatlas-data-download>`_.''',
                   type='source')
        ]


class HUCColumns(ColumnsTask):

    def version(self):
        return 4

    def requires(self):
        return {
            'source': SourceTags(),
            'license': LicenseTags(),
            'subsections': SubsectionTags(),
            'sections': SectionTags(),
        }

    def columns(self):
        input_ = self.input()
        source = input_['source']['epa-enviroatlas']
        license_ = input_['license']['no-restrictions']
        usa = input_['sections']['united_states']
        environmental = input_['subsections']['environmental']
        boundary = input_['subsections']['boundary']

        hydro_unit = OBSColumn(
            id='hydro_unit',
            name='Subwatershed hydrological unit',
            description='Subwatershed areas, which are assigned unique twelve Digit Hydrologic Unit Codes. Numbers were assigned in an upstream to downstream fashion. Where no downstream/upstream relationship could be determined, numbers were assigned in a clockwise fashion.',
            type='Geometry',
            weight=5,
            tags=[source, license_, usa, environmental, boundary]
        )
        huc_12 = OBSColumn(
            weight=0,
            type='Text',
            targets={hydro_unit: GEOM_REF},
        )

        return OrderedDict([
            ('the_geom', hydro_unit),
            ('huc_12', huc_12),
        ])


class HUC(TableTask):

    def version(self):
        return 3

    def requires(self):
        return {
            'columns': HUCColumns(),
            'data': ImportHUC(),
        }

    def columns(self):
        return self.input()['columns']

    def timespan(self):
        return '2015'

    def populate(self):
        session = current_session()
        session.execute('''
            INSERT INTO {output} (the_geom, huc_12)
            SELECT ST_MakeValid(ST_Simplify(wkb_geometry, 0.0005)) the_geom, huc_12
            FROM {input}'''.format(input=self.input()['data'].table,
                                   output=self.output().table))
