from luigi import Parameter, WrapperTask

from tasks.meta import OBSColumn, current_session
from tasks.tags import SectionTags, SubsectionTags, LicenseTags, UnitTags
from tasks.us.epa.huc import HUCColumns, SourceTags, HUC
from tasks.util import (DownloadUnzipTask, shell, ColumnsTask, TableTask,
                        CSV2TempTableTask, MetaWrapper)
from collections import OrderedDict

import os


class DownloadMetrics(DownloadUnzipTask):

    URL = 'http://edg.epa.gov/data/Public/ORD/EnviroAtlas/National/National_metrics_July2015_CSV.zip'

    def download(self):
        shell('wget -O "{output}".zip "{url}"'.format(
            output=self.output().path,
            url=self.URL
        ))


class EnviroAtlasTempTable(CSV2TempTableTask):

    csv_name = Parameter()

    def requires(self):
        return DownloadMetrics()

    def input_csv(self):
        return os.path.join(self.input().path, self.csv_name)


class EnviroAtlasColumns(ColumnsTask):

    table = Parameter()

    def version(self):
        return 2

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'licenses': LicenseTags(),
            'sources': SourceTags(),
            'units': UnitTags(),
        }

    def solar_energy(self, usa, environmental, license_, source, units):
        return OrderedDict([
            ('sole_area', OBSColumn(
                name='Area with solar energy potential',
                tags=[usa, environmental, license_, source, units['km2']],
                weight=5,
                type='Numeric',
                aggregate='sum',
            )),
            ('sole_mean', OBSColumn(
                name='Annual Average direct normal solar resources kWh/m2/day',
                tags=[usa, environmental, license_, source, units['ratio']],
                weight=5,
                type='Numeric',
                aggregate='average',
            ))
        ])

    def avgprecip(self, usa, environmental, license_, source, units):
        inches = units['inches']
        return OrderedDict([
            ('meanprecip', OBSColumn(
                name='Average annual precipitation',
                description='Average annual precipitation in inches.',
                aggregate='average',
                type='Numeric',
                weight=5,
                tags=[usa, environmental, license_, source, inches],))
        ])

    def landcover(self, usa, environmental, license_, source, units):
        ratio = units['ratio']
        pfor = OBSColumn(
            name='Forest land cover',
            description='Percentage of land area within the WBD 12-digit hydrologic unit that is classified as forest land cover (2006 NLCD codes: 41, 42, 43). A value of -1 indicates that no land cover data was located within the hydrologic unit.',
            type='Numeric',
            weight=5,
            tags=[usa, environmental, license_, source, ratio],
        )
        pwetl = OBSColumn(
            name='Wetland land cover',
            description='Percentage of land area within the WBD 12-digit hydrologic unit that is classified as wetland land cover (2006 NLCD codes: 90, 95). A value of -1 indicates that no land cover data was located within the hydrologic unit.',
            type='Numeric',
            weight=5,
            tags=[usa, environmental, license_, source, ratio],
        )
        pagt = OBSColumn(
            name='Agricultural/cultivated land cover',
            description='Percentage of land area within the WBD 12-digit hydrologic unit that is classified as agricultural/cultivated land cover (2006 NLCD codes: 21, 81, 82). A value of -1 indicates that no land cover data was located within the hydrologic unit.',
            type='Numeric',
            weight=5,
            tags=[usa, environmental, license_, source, ratio],
        )
        pagp = OBSColumn(
            name='Agricultural pasture land cover',
            description='Percentage of land area within the WBD 12-digit hydrologic unit that is classified as agricultural pasture land cover (2006 NLCD codes: 81). A value of -1 indicates that no land cover data was located within the hydrologic unit.',
            type='Numeric',
            weight=5,
            tags=[usa, environmental, license_, source, ratio],
        )
        pagc = OBSColumn(
            name='Agricultural cropland land cover',
            description='Percentage of land area within the WBD 12-digit hydrologic unit that is classified as agricultural cropland land cover (2006 NLCD codes: 82). A value of -1 indicates that no land cover data was located within the hydrologic unit.',
            type='Numeric',
            weight=5,
            tags=[usa, environmental, license_, source, ratio],
        )
        pfor90 = OBSColumn(
            name='Modified forest land cover',
            description='Percentage of land area within the WBD 12-digit hydrologic unit that is classified as modified forest land cover (2006 NLCD codes: 41, 42, 43, and 90). A value of -1 indicates that no land cover data was located within the hydrologic unit.',
            type='Numeric',
            weight=5,
            tags=[usa, environmental, license_, source, ratio],
        )
        pwetl95 = OBSColumn(
            name='Modified wetlands land cover',
            description='Percentage of land area within the WBD 12-digit hydrologic unit that is classified as modified wetlands land cover (2006 NLCD codes: 95). A value of -1 indicates that no land cover data was located within the hydrologic unit.',
            type='Numeric',
            weight=5,
            tags=[usa, environmental, license_, source, ratio],
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

    def columns(self):
        input_ = self.input()
        usa = input_['sections']['united_states']
        environmental = input_['subsections']['environmental']
        license_ = input_['licenses']['no-restrictions']
        source = input_['sources']['epa-enviroatlas']
        units = input_['units']

        cols = getattr(self, self.table)(usa, environmental,
                                         license_, source, units)
        for colname, col in cols.iteritems():
            col.id = '{}_{}'.format(self.table, colname)

        return cols


class EnviroAtlas(TableTask):

    table = Parameter()
    time = Parameter()

    def requires(self):
        return {
            'geom_cols': HUCColumns(),
            'data_cols': EnviroAtlasColumns(table=self.table.lower()),
            'data': EnviroAtlasTempTable(csv_name=self.table + '.csv'),
        }

    def timespan(self):
        return self.time

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()
        cols['huc_12'] = input_['geom_cols']['huc_12']
        cols.update(input_['data_cols'])
        return cols

    def populate(self):
        session = current_session()
        cols = self.columns()
        cols.pop('huc_12')
        colnames = cols.keys()
        session.execute('''
            INSERT INTO {output} (huc_12, {colnames})
            SELECT huc_12, {typed_colnames}::Numeric
            FROM {input}
        '''.format(input=self.input()['data'].table,
                   output=self.output().table,
                   colnames=', '.join(colnames),
                   typed_colnames='::Numeric, '.join(colnames)))


class AllTables(WrapperTask):

    TABLES = [
        ('AvgPrecip', '2010'),
        ('landcover', '2006'),
        ('solar_energy', '2012'),
    ]

    def requires(self):
        for table, timespan in self.TABLES:
            yield EnviroAtlas(table=table, time=timespan)

# class HUCMetaWrap(MetaWrapper):
#     table = Parameter()
#     time = Parameter()
#
#     params = {
#         'table': ['AvgPrecip','landcover','solar_energy'],
#         'time': ['2010','2006','2012']
#     }
#
#     def tables(self):
#         yield EnviroAtlas(table=self.table, time=self.time)
#         yield HUC()
