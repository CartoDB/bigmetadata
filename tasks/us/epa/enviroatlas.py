from tasks.meta import OBSColumn, current_session
from tasks.tags import SectionTags, SubsectionTags, LicenseTags, UnitTags
from tasks.us.epa.huc import HUCColumns, SourceTags
from tasks.util import (DownloadUnzipTask, shell, ColumnsTask, TableTask,
                        CSV2TempTableTask)
from collections import OrderedDict

import os


class DownloadMetrics(DownloadUnzipTask):

    URL = 'http://edg.epa.gov/data/Public/ORD/EnviroAtlas/National/National_metrics_July2015_CSV.zip'

    def download(self):
        shell('wget -O "{output}".zip "{url}"'.format(
            output=self.output().path,
            url=self.URL
        ))


class LandCoverTempTable(CSV2TempTableTask):

    def requires(self):
        return DownloadMetrics()

    def input_csv(self):
        return os.path.join(self.input().path, 'landcover.csv')


class LandCoverColumns(ColumnsTask):

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'licenses': LicenseTags(),
            'sources': SourceTags(),
            'units': UnitTags(),
        }

    def columns(self):
        input_ = self.input()
        us = input_['sections']['united_states']
        environmental = input_['subsections']['environmental']
        license = input_['licenses']['no-restrictions']
        source = input_['sources']['epa-enviroatlas']
        ratio = input_['units']['ratio']

        pfor = OBSColumn(
            name='Forest land cover',
            description='Percentage of land area within the WBD 12-digit hydrologic unit that is classified as forest land cover (2006 NLCD codes: 41, 42, 43). A value of -1 indicates that no land cover data was located within the hydrologic unit.',
            type='Numeric',
            weight=5,
            tags=[us, environmental, license, source, ratio],
        )
        pwetl = OBSColumn(
            name='Wetland land cover',
            description='Percentage of land area within the WBD 12-digit hydrologic unit that is classified as wetland land cover (2006 NLCD codes: 90, 95). A value of -1 indicates that no land cover data was located within the hydrologic unit.',
            type='Numeric',
            weight=5,
            tags=[us, environmental, license, source, ratio],
        )
        pagt = OBSColumn(
            name='Agricultural/cultivated land cover',
            description='Percentage of land area within the WBD 12-digit hydrologic unit that is classified as agricultural/cultivated land cover (2006 NLCD codes: 21, 81, 82). A value of -1 indicates that no land cover data was located within the hydrologic unit.',
            type='Numeric',
            weight=5,
            tags=[us, environmental, license, source, ratio],
        )
        pagp = OBSColumn(
            name='Agricultural pasture land cover',
            description='Percentage of land area within the WBD 12-digit hydrologic unit that is classified as agricultural pasture land cover (2006 NLCD codes: 81). A value of -1 indicates that no land cover data was located within the hydrologic unit.',
            type='Numeric',
            weight=5,
            tags=[us, environmental, license, source, ratio],
        )
        pagc = OBSColumn(
            name='Agricultural cropland land cover',
            description='Percentage of land area within the WBD 12-digit hydrologic unit that is classified as agricultural cropland land cover (2006 NLCD codes: 82). A value of -1 indicates that no land cover data was located within the hydrologic unit.',
            type='Numeric',
            weight=5,
            tags=[us, environmental, license, source, ratio],
        )
        pfor90 = OBSColumn(
            name='Modified forest land cover',
            description='Percentage of land area within the WBD 12-digit hydrologic unit that is classified as modified forest land cover (2006 NLCD codes: 41, 42, 43, and 90). A value of -1 indicates that no land cover data was located within the hydrologic unit.',
            type='Numeric',
            weight=5,
            tags=[us, environmental, license, source, ratio],
        )
        pwetl95 = OBSColumn(
            name='Modified wetlands land cover',
            description='Percentage of land area within the WBD 12-digit hydrologic unit that is classified as modified wetlands land cover (2006 NLCD codes: 95). A value of -1 indicates that no land cover data was located within the hydrologic unit.',
            type='Numeric',
            weight=5,
            tags=[us, environmental, license, source, ratio],
        )
        return OrderedDict([
            ('pfor', pfor),
            ('pwetl', pwetl),
            ('pagt', pagt),
            ('pagp', pagp),
            ('pagc', pagc),
            ('pfor90', pfor90),
            ('pwetl95', pwetl95),
        ])


class LandCover(TableTask):

    def requires(self):
        return {
            'geom_cols': HUCColumns(),
            'data_cols': LandCoverColumns(),
            'data': LandCoverTempTable(),
        }

    def timespan(self):
        return '2006'

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()
        cols['huc_12'] = input_['geom_cols']['huc_12']
        cols.update(input_['data_cols'])
        return cols

    def populate(self):
        session = current_session()
        session.execute('''
            INSERT INTO {output} (
              huc_12, pfor, pwetl, pagt, pagp, pagc, pfor90, pwetl95
            ) SELECT
              huc_12, pfor::Numeric, pwetl::Numeric, pagt::Numeric,
              pagp::Numeric, pagc::Numeric, pfor90::Numeric, pwetl95::Numeric
            FROM {input}
        '''.format(input=self.input()['data'].table,
                   output=self.output().table))
