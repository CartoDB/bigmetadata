from tasks.base_tasks import (ColumnsTask, TableTask, TagsTask, DownloadUnzipTask, Shp2TempTableTask,
                              SimplifiedTempTableTask, RepoFile)
from tasks.tags import SectionTags, SubsectionTags, LicenseTags, BoundaryTags
from tasks.meta import GEOM_REF, OBSColumn, OBSTable, OBSTag, current_session
from tasks.util import copyfile
import os
from luigi import WrapperTask
from collections import OrderedDict
from lib.timespan import get_timespan


class LowerLayerSuperOutputAreasSourceTags(TagsTask):

    def version(self):
        return 1

    def tags(self):
        return[
            OBSTag(id='uk-gov-source',
                   name='Office for National Statistics (UK)',
                   type='source',
                   description='Lower Layer Super Output Area (LSOA) and Data Zones boundaries')
                   ]


class DownloadLowerLayerSuperOutputAreas(DownloadUnzipTask):
    # https://data.gov.uk/dataset/fa883558-22fb-4a1a-8529-cffdee47d500/lower-layer-super-output-area-lsoa-boundaries
    # http://geoportal.statistics.gov.uk/datasets/da831f80764346889837c72508f046fa_0
    URL = 'https://opendata.arcgis.com/datasets/da831f80764346889837c72508f046fa_0.zip?outSR=%7B%22wkid%22%3A27700%2C%22latestWkid%22%3A27700%7D'

    def version(self):
        return 1

    def requires(self):
        return RepoFile(resource_id=self.task_id,
                        version=self.version(),
                        url=self.URL)

    def download(self):
        copyfile(self.input().path, '{output}.zip'.format(output=self.output().path))


class ImportLowerLayerSuperOutputAreas(Shp2TempTableTask):

    def requires(self):
        return DownloadLowerLayerSuperOutputAreas()

    def input_shp(self):
        return os.path.join(self.input().path, 'Lower_Layer_Super_Output_Areas_December_2011_Full_Clipped__Boundaries_in_England_and_Wales.shp')


class SimplifiedImportLowerLayerSuperOutputAreas(SimplifiedTempTableTask):
    def requires(self):
        return ImportLowerLayerSuperOutputAreas()


class MiddleLayerSuperOutputAreasSourceTags(TagsTask):

    def version(self):
        return 1

    def tags(self):
        return[
            OBSTag(id='uk-gov-source',
                   name='Office for National Statistics (UK)',
                   type='source',
                   description='Middle Layer Super Output Area (LSOA) and Intermediate Zone boundaries')
                   ]


class DownloadMiddleLayerSuperOutputAreas(DownloadUnzipTask):
    # https://data.gov.uk/dataset/2cf1f346-2f74-4c06-bd4b-30d7e4df5ae7/middle-layer-super-output-area-msoa-boundaries
    # http://geoportal.statistics.gov.uk/datasets/826dc85fb600440889480f4d9dbb1a24_0
    URL = 'https://opendata.arcgis.com/datasets/826dc85fb600440889480f4d9dbb1a24_0.zip?outSR=%7B%22wkid%22%3A27700%2C%22latestWkid%22%3A27700%7D'

    def version(self):
        return 1

    def requires(self):
        return RepoFile(resource_id=self.task_id,
                        version=self.version(),
                        url=self.URL)

    def download(self):
        copyfile(self.input().path, '{output}.zip'.format(output=self.output().path))


class ImportMiddleLayerSuperOutputAreas(Shp2TempTableTask):

    def requires(self):
        return DownloadMiddleLayerSuperOutputAreas()

    def input_shp(self):
        return os.path.join(self.input().path, 'Middle_Layer_Super_Output_Areas_December_2011_Full_Clipped_Boundaries_in_England_and_Wales.shp')


class SimplifiedImportMiddleLayerSuperOutputAreas(SimplifiedTempTableTask):
    def requires(self):
        return ImportMiddleLayerSuperOutputAreas()


# Scotland Data Zones == Lower Level Super Output Areas
class DownloadDataZones(DownloadUnzipTask):
    # https://data.gov.uk/dataset/ab9f1f20-3b7f-4efa-9bd2-239acf63b540/data-zone-boundaries-2011
    URL = 'http://sedsh127.sedsh.gov.uk/Atom_data/ScotGov/ZippedShapefiles/SG_DataZoneBdry_2011.zip'

    def version(self):
        return 1

    def requires(self):
        return RepoFile(resource_id=self.task_id,
                        version=self.version(),
                        url=self.URL)

    def download(self):
        copyfile(self.input().path, '{output}.zip'.format(output=self.output().path))


class ImportDataZones(Shp2TempTableTask):

    def requires(self):
        return DownloadDataZones()

    def input_shp(self):
        return os.path.join(self.input().path, 'SG_DataZone_Bdry_2011.shp')


class SimplifiedImportDataZones(SimplifiedTempTableTask):
    def requires(self):
        return ImportDataZones()


# Scotland Intermediate Zones == Middle Level Super Output Areas
class DownloadIntermediateZones(DownloadUnzipTask):
    # https://data.gov.uk/dataset/133d4983-c57d-4ded-bc59-390c962ea280/intermediate-zone-boundaries-2011
    URL = 'http://sedsh127.sedsh.gov.uk/Atom_data/ScotGov/ZippedShapefiles/SG_IntermediateZoneBdry_2011.zip'

    def version(self):
        return 1

    def requires(self):
        return RepoFile(resource_id=self.task_id,
                        version=self.version(),
                        url=self.URL)

    def download(self):
        copyfile(self.input().path, '{output}.zip'.format(output=self.output().path))


class ImportIntermediateZones(Shp2TempTableTask):

    def requires(self):
        return DownloadIntermediateZones()

    def input_shp(self):
        return os.path.join(self.input().path, 'SG_IntermediateZone_Bdry_2011.shp')


class SimplifiedImportIntermediateZones(SimplifiedTempTableTask):
    def requires(self):
        return ImportIntermediateZones()


# Northern Ireland Super Output Areas == Lower Level Super Output Areas
class DownloadNISuperOutputAreas(DownloadUnzipTask):
    # https://www.nisra.gov.uk/support/geography/northern-ireland-super-output-areas
    URL = 'https://www.nisra.gov.uk/sites/nisra.gov.uk/files/publications/SOA2011_Esri_Shapefile_0.zip'

    def version(self):
        return 1

    def requires(self):
        return RepoFile(resource_id=self.task_id,
                        version=self.version(),
                        url=self.URL)

    def download(self):
        copyfile(self.input().path, '{output}.zip'.format(output=self.output().path))


class ImportNISuperOutputAreas(Shp2TempTableTask):

    def requires(self):
        return DownloadNISuperOutputAreas()

    def input_shp(self):
        return os.path.join(self.input().path, 'SOA2011.shp')


class SimplifiedImportNISuperOutputAreas(SimplifiedTempTableTask):
    def requires(self):
        return ImportNISuperOutputAreas()


class LowerLayerSuperOutputAreasColumns(ColumnsTask):

    def version(self):
        return 1

    def requires(self):
        return {
            'subsections': SubsectionTags(),
            'sections': SectionTags(),
            'source': LowerLayerSuperOutputAreasSourceTags(),
            'license': LicenseTags(),
            'boundary': BoundaryTags(),
        }

    def columns(self):
        input_ = self.input()
        license = input_['license']['uk_ogl']
        source = input_['source']['uk-gov-source']
        boundary_type = input_['boundary']
        geom = OBSColumn(
            id='lsoa_geo',
            type='Geometry',
            name='Lower Layer Super Output Areas (LSOA) and Data Zones',
            description='Full Clipped Lower Layer Super Output Area Boundaries '
                        'in England and Wales. '
                        'Data Zone Boundaries in Scotland. '
                        'Lower Layer Super Output Areas (LSOAs) typically contain '
                        '4 to 6 OAs with a population of around 1500. '
                        '-`Wikipedia <https://en.wikipedia.org/'
                        'wiki/ONS_coding_system#Neighbourhood_Statistics_Geography>`_'
                        'Data zones are the key geography for the dissemination '
                        'of small area statistics in Scotland and are widely used '
                        'across the public and private sector. '
                        'Composed of aggregates of Census Output Areas, data zones '
                        'are large enough that statistics can be presented accurately '
                        'without fear of disclosure and yet small enough that they '
                        'can be used to represent communities. They are designed to '
                        'have roughly standard populations of 500 to 1,000 household '
                        'residents, nest within Local Authorities, have compact '
                        'shapes that respect physical boundaries where possible, '
                        'and to contain households with similar social characteristics. '
                        'Aggregations of data zones are often used to approximate a '
                        'larger area of interest or a higher level geography that '
                        'statistics wouldn’t normally be available for. '
                        'Data zones also represent a relatively stable geography '
                        'that can be used to analyse change over time, '
                        'with changes only occurring after a Census. '
                        'Following the update to data zones using 2011 Census data, '
                        'there are now 6,976 data zones covering the whole of Scotland. '
                        '-`data.gov.uk <https://data.gov.uk/dataset/'
                        'ab9f1f20-3b7f-4efa-9bd2-239acf63b540/data-zone-boundaries-2011>`_'
                        'Super Output Areas (SOAs) were a new geography that were developed '
                        'NISRA to improve the reporting of small area statistics. '
                        'A set of slightly revised Super Output Areas (SOAs) were created '
                        'for the 2011 census outputs. '
                        '-`Northern Ireland Statistics and Research Agency <https://www.nisra.gov.uk/'
                        'support/geography/northern-ireland-super-output-areas>`_',
            weight=8,
            tags=[input_['subsections']['boundary'], input_['sections']['uk'], source, license,
                  boundary_type['cartographic_boundary'], boundary_type['interpolation_boundary']]
        )
        geomref = OBSColumn(
            id='lsoa_id',
            type='Text',
            name='Lower Layer Super Output Area ID',
            weight=0,
            targets={geom: GEOM_REF}
        )
        geomname = OBSColumn(
            id='lsoa_name',
            type='Text',
            name='Lower Layer Super Output Area Name',
            weight=0,
            targets={geom: GEOM_REF}
        )

        return OrderedDict([
            ('the_geom', geom),
            ('geographycode', geomref),
            ('name', geomname),
        ])


class LowerLayerSuperOutputAreas(TableTask):

    def requires(self):
        return {
            'columns': LowerLayerSuperOutputAreasColumns(),
            'lsoa-data': SimplifiedImportLowerLayerSuperOutputAreas(),
            'dz-data': SimplifiedImportDataZones(),
            'ni-data': SimplifiedImportNISuperOutputAreas(),
        }

    def version(self):
        return 1

    def table_timespan(self):
        return get_timespan('2011')

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
                SELECT ST_MakeValid(wkb_geometry), lsoa11cd, lsoa11nm
                FROM {input_lsoa}
                UNION ALL
                SELECT ST_MakeValid(wkb_geometry), datazone, name
                FROM {input_dz}
                UNION ALL
                SELECT ST_MakeValid(wkb_geometry), soa_code, soa_label
                FROM {input_ni}
                '''.format(output=self.output().table,
                           input_lsoa=self.input()['lsoa-data'].table,
                           input_dz=self.input()['dz-data'].table,
                           input_ni=self.input()['ni-data'].table,)

        session.execute(query)


class MiddleLayerSuperOutputAreasColumns(ColumnsTask):

    def version(self):
        return 1

    def requires(self):
        return {
            'subsections': SubsectionTags(),
            'sections': SectionTags(),
            'source': MiddleLayerSuperOutputAreasSourceTags(),
            'license': LicenseTags(),
            'boundary': BoundaryTags(),
        }

    def columns(self):
        input_ = self.input()
        license = input_['license']['uk_ogl']
        source = input_['source']['uk-gov-source']
        boundary_type = input_['boundary']
        geom = OBSColumn(
            id='msoa_geo',
            type='Geometry',
            name='Middle Layer Super Output Areas (LSOA) and Intermediate Zones',
            description='Full Clipped Middle Layer Super Output Area Boundaries '
                        'in England and Wales. '
                        'Middle Layer Super Output Areas (MSOAs) on average '
                        'have a population of 7,200. '
                        '-`Wikipedia <https://en.wikipedia.org/'
                        'wiki/ONS_coding_system#Neighbourhood_Statistics_Geography>`_'
                        'Intermediate zones are a statistical geography that sit '
                        'between data zones and local authorities, created for use '
                        'with the Scottish Neighbourhood Statistics (SNS) programme '
                        'and the wider public sector. Intermediate zones are used '
                        'for the dissemination of statistics that are not suitable '
                        'for release at the data zone level because of the sensitive '
                        'nature of the statistic, or for reasons of reliability. '
                        'Intermediate Zones were designed to meet constraints on '
                        'population thresholds (2,500 - 6,000 household residents), '
                        'to nest within local authorities, and to be built up from '
                        'aggregates of data zones. Intermediate zones also represent '
                        'a relatively stable geography that can be used to analyse '
                        'change over time, with changes only occurring after a Census. '
                        'Following the update to intermediate zones using 2011 Census '
                        'data, there are now 1,279 Intermediate Zones covering the '
                        'whole of Scotland. '
                        '-`data.gov.uk <https://data.gov.uk/dataset/'
                        '133d4983-c57d-4ded-bc59-390c962ea280/intermediate-zone-boundaries-2011>`_',
            weight=8,
            tags=[input_['subsections']['boundary'], input_['sections']['uk'], source, license,
                  boundary_type['cartographic_boundary'], boundary_type['interpolation_boundary']]
        )
        geomref = OBSColumn(
            id='msoa_id',
            type='Text',
            name='Middle Layer Super Output Area ID',
            weight=0,
            targets={geom: GEOM_REF}
        )
        geomname = OBSColumn(
            id='msoa_name',
            type='Text',
            name='Middle Layer Super Output Area Name',
            weight=0,
            targets={geom: GEOM_REF}
        )

        return OrderedDict([
            ('the_geom', geom),
            ('geographycode', geomref),
            ('name', geomname),
        ])


class MiddleLayerSuperOutputAreas(TableTask):

    def requires(self):
        return {
            'columns': MiddleLayerSuperOutputAreasColumns(),
            'msoa-data': SimplifiedImportMiddleLayerSuperOutputAreas(),
            'iz-data': SimplifiedImportIntermediateZones(),
        }

    def version(self):
        return 1

    def table_timespan(self):
        return get_timespan('2011')

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
                SELECT ST_MakeValid(wkb_geometry), msoa11cd, msoa11nm
                FROM {input_msoa}
                UNION ALL
                SELECT ST_MakeValid(wkb_geometry), interzone, name
                FROM {input_iz}
                '''.format(output=self.output().table,
                           input_msoa=self.input()['msoa-data'].table,
                           input_iz=self.input()['iz-data'].table,)

        session.execute(query)


class GovWrapper(WrapperTask):
    def tables(self):
        yield LowerLayerSuperOutputAreas()
        yield MiddleLayerSuperOutputAreas()
