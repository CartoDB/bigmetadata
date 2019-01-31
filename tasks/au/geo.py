from luigi import Parameter, IntParameter, WrapperTask, Task, LocalTarget
import os
import json

from lib.timespan import get_timespan

from tasks.base_tasks import (ColumnsTask, RepoFileUnzipTask, GeoFile2TempTableTask, SimplifiedTempTableTask, TableTask,
                              TagsTask, RepoFile)
from tasks.util import shell, copyfile, classpath, uncompress_file
from tasks.meta import GEOM_REF, GEOM_NAME, OBSColumn, current_session, OBSTag, OBSTable
from tasks.tags import SectionTags, SubsectionTags, BoundaryTags

from collections import OrderedDict
from lib.logger import get_logger

LOGGER = get_logger(__name__)

GEO_STE = 'STE'
GEO_SA4 = 'SA4'
GEO_SA3 = 'SA3'
GEO_SA2 = 'SA2'
GEO_SA1 = 'SA1'
GEO_GCCSA = 'GCCSA'
GEO_LGA = 'LGA'
GEO_SLA = 'SLA'
GEO_SSC = 'SSC'
GEO_POA = 'POA'
GEO_CED = 'CED'
GEO_SED = 'SED'
GEO_SOS = 'SOS'
GEO_SOSR = 'SOSR'
GEO_UCL = 'UCL'
GEO_SUA = 'SUA'
GEO_RA = 'RA'
GEO_MB = 'MB'

GEOGRAPHIES = {
    2011: (GEO_STE, GEO_SA4, GEO_SA3, GEO_SA2, GEO_SA1, GEO_MB, GEO_GCCSA, GEO_LGA, GEO_SLA,
           GEO_SSC, GEO_POA, GEO_CED, GEO_SED, GEO_SOS, GEO_SOSR, GEO_UCL, GEO_SUA, GEO_RA),
    2016: (GEO_STE, GEO_SA4, GEO_SA3, GEO_SA2, GEO_SA1, GEO_MB, GEO_GCCSA, GEO_LGA,
           GEO_SSC, GEO_POA, GEO_CED, GEO_SED, GEO_SOS, GEO_SOSR, GEO_UCL, GEO_SUA, GEO_RA),
}


class GeographyMeta():
    def _get_geo_meta(self, year):
        with open(os.path.join(os.path.dirname(__file__), 'geography_meta_{}.json'.format(year))) as f:
            return json.load(f)


class SourceTags(TagsTask):

    def tags(self):
        return [
            OBSTag(id='au-census',
                   name='Australian Bureau of Statistics (ABS)',
                   type='source',
                   description='The `Australian Bureau of Statistics <http://abs.gov.au/websitedbs/censushome.nsf/home/datapacks>`')
        ]


class LicenseTags(TagsTask):

    def tags(self):
        return [
            OBSTag(id='au-datapacks-license',
                   name='Creative Commons Attribution 2.5 Australia licence',
                   type='license',
                   description='DataPacks is licenced under a `Creative Commons Attribution 2.5 Australia licence <https://creativecommons.org/licenses/by/2.5/au/>`_')
        ]


class DownloadGeography(RepoFileUnzipTask):

    year = IntParameter()
    resolution = Parameter()

    # http://www.censusdata.abs.gov.au/CensusOutput/copsubdatapacks.nsf/All%20docs%20by%20catNo
    URL = 'http://www.censusdata.abs.gov.au/CensusOutput/copsubdatapacks.nsf/All%20docs%20by%20catNo/Boundaries_{year}_{resolution}/\$File/{year}_{resolution}_shape.zip'

    def get_url(self):
        return self.URL.format(resolution=self.resolution, year=self.year)


class DownloadAndMergeMeshBlocks(Task):

    URLS = {
        "nsw": "http://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055001_mb_2016_nsw_shape.zip&1270.0.55.001&Data%20Cubes&E9FA17AFA7EB9FEBCA257FED0013A5F5&0&July%202016&12.07.2016&Latest",
        "vic": "http://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055001_mb_2016_vic_shape.zip&1270.0.55.001&Data%20Cubes&04F12B9E465AE765CA257FED0013B20F&0&July%202016&12.07.2016&Latest",
        "qld": "http://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055001_mb_2016_qld_shape.zip&1270.0.55.001&Data%20Cubes&A17EA45AB7CC5D5CCA257FED0013B7F6&0&July%202016&12.07.2016&LatestA",
        "sa": "http://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055001_mb_2016_sa_shape.zip&1270.0.55.001&Data%20Cubes&793662F7A1C04BD6CA257FED0013BCB0&0&July%202016&12.07.2016&LatestA",
        "wa": "http://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055001_mb_2016_wa_shape.zip&1270.0.55.001&Data%20Cubes&2634B61773C82931CA257FED0013BE47&0&July%202016&12.07.2016&Latest",
        "tas": "http://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055001_mb_2016_tas_shape.zip&1270.0.55.001&Data%20Cubes&854152CB547DE707CA257FED0013C180&0&July%202016&12.07.2016&Latest",
        "nt": "http://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055001_mb_2016_nt_shape.zip&1270.0.55.001&Data%20Cubes&31364C9DFE4CC667CA257FED0013C4F6&0&July%202016&12.07.2016&Latest",
        "act": "http://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055001_mb_2016_act_shape.zip&1270.0.55.001&Data%20Cubes&21B8D5684405A2A7CA257FED0013C567&0&July%202016&12.07.2016&Latest"
    }

    def version(self):
        return 1

    def requires(self):
        requires = {}
        for key, url in self.URLS.items():
            requires[key] = (RepoFile(resource_id=key+"_"+self.task_id,
                                      version=self.version(),
                                      url=url))
        return requires

    def run(self):
        output_dir = os.path.dirname(self.output().path)
        for key in self.URLS.keys():
            self.download(self.input()[key].path, key)
            uncompress_file('{output}/{key}'.format(output=output_dir, key=key))
        self.merge()

    def download(self, shp_file, key):
        output_dir = os.path.dirname(self.output().path)
        copyfile(shp_file, '{output}/{key}.zip'.format(output=output_dir, key=key))

    def merge(self):
        first = True
        output_dir = os.path.dirname(self.output().path)
        for key in self.URLS.keys():
            if first:
                shell('ogr2ogr -f "ESRI Shapefile" {output_file} {output}/{key}/MB_2016_{key_upper}.shp'.format(output_file=self.output().path, output=output_dir, key=key, key_upper=key.upper()))
                first = False
            else:
                shell('ogr2ogr -f "ESRI Shapefile" -update -append {output_file} {output}/{key}/MB_2016_{key_upper}.shp'.format(output_file=self.output().path, output=output_dir, key=key, key_upper=key.upper()))

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id, 'au_mb_all_merged.shp'))


class ImportGeography(GeoFile2TempTableTask):
    '''
    Import geographies into postgres by geography level
    '''

    year = IntParameter()
    resolution = Parameter()

    def requires(self):
        if self.resolution == GEO_MB:
            return DownloadAndMergeMeshBlocks()
        else:
            return DownloadGeography(resolution=self.resolution, year=self.year)

    def input_files(self):
        if self.resolution == GEO_MB:
            yield self.input().path
        else:
            cmd = 'ls {input}/*.shp'.format(
                input=self.input().path
            )
            for shp in shell(cmd).strip().split('\n'):
                yield shp


class SimplifiedRawGeometry(SimplifiedTempTableTask):
    year = IntParameter()
    resolution = Parameter()

    def requires(self):
        return ImportGeography(year=self.year, resolution=self.resolution)


class GeographyColumns(ColumnsTask, GeographyMeta):

    year = IntParameter()
    resolution = Parameter()

    def version(self):
        return 4

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
        source = input_['source']['au-census']
        license = input_['license']['au-datapacks-license']
        boundary_type = input_['boundary']

        geom = OBSColumn(
            id='{}_{}'.format(self.resolution, self.year),
            type='Geometry',
            name=self._get_geo_meta(self.year)[self.resolution]['name'],
            description='',
            weight=self._get_geo_meta(self.year)[self.resolution]['weight'],
            tags=[source, license, sections['au'], subsections['boundary']],
        )
        geom_id = OBSColumn(
            type='Text',
            id='{}_id_{}'.format(self.resolution, self.year),
            weight=0,
            targets={geom: GEOM_REF},
        )
        external_id = OBSColumn(
            type='Text',
            id='{}_external_id_{}'.format(self.resolution, self.year),
            weight=0,
            targets={geom: GEOM_REF},
        )
        parent_id = OBSColumn(
            type='Text',
            id='{}_parent_id_{}'.format(self.resolution, self.year),
            weight=0,
            targets={geom: GEOM_REF},
        )
        geom_name = OBSColumn(
            type='Text',
            name='Proper name of {}'.format(self._get_geo_meta(self.year)[self.resolution]['name']),
            id='{}_name_{}'.format(self.resolution, self.year),
            description='',
            weight=1,
            targets={geom: GEOM_NAME},
            tags=[source, license, sections['au'], subsections['names']],
        )

        cartographic_boundaries = [GEO_LGA, GEO_POA, GEO_CED, GEO_SED, GEO_SSC,
                                   GEO_SA1, GEO_SA2, GEO_SA3, GEO_SA4,
                                   GEO_STE, GEO_GCCSA, GEO_UCL,
                                   GEO_SOS, GEO_SOSR, GEO_SUA, GEO_RA]
        interpolated_boundaries = [GEO_SA1, GEO_SA2, GEO_SA3, GEO_SA4,
                                   GEO_STE, GEO_GCCSA, GEO_UCL,
                                   GEO_SOS, GEO_SOSR, GEO_SUA, GEO_RA]

        cols = OrderedDict([
            ('geom_name', geom_name),
            ('geom_id', geom_id),
            ('external_id', external_id),
            ('parent_id', parent_id),
            ('the_geom', geom),
        ])

        for _, col in cols.items():
            if col.id in interpolated_boundaries:
                col.tags.append(boundary_type['interpolation_boundary'])
            if col.id in cartographic_boundaries:
                col.tags.append(boundary_type['cartographic_boundary'])
        return cols


class Geography(TableTask, GeographyMeta):

    year = IntParameter()
    resolution = Parameter()

    def version(self):
        return 6

    def requires(self):
        return {
            'data': SimplifiedRawGeometry(resolution=self.resolution, year=self.year),
            'columns': GeographyColumns(resolution=self.resolution, year=self.year)
        }

    def table_timespan(self):
        return get_timespan(str(self.year))

    # TODO: https://github.com/CartoDB/bigmetadata/issues/435
    def targets(self):
        return {
            OBSTable(id='.'.join([self.schema(), self.name()])): GEOM_REF,
        }

    def columns(self):
        if self.resolution == GEO_MB:
            return self.input()['columns']
        else:
            cols = OrderedDict()
            for key, value in self.input()['columns'].items():
                if key != 'parent_id':
                    cols[key] = value
            return cols

    def populate(self):
        session = current_session()
        if self.resolution == GEO_MB:
            parent_col = self._get_geo_meta(self.year)[self.resolution]['parent_col'] + ' as parent_id,'
        else:
            parent_col = ''
        query = '''
                INSERT INTO {output}
                SELECT {geom_name} as geom_name,
                       {region_col} as geom_id,
                       {external_id} as external_id,
                       {parent_col}
                       wkb_geometry as the_geom
                FROM {input}
                '''.format(geom_name=self._get_geo_meta(self.year)[self.resolution]['proper_name'],
                           region_col=self._get_geo_meta(self.year)[self.resolution]['region_col'],
                           external_id=self._get_geo_meta(self.year)[self.resolution]['external_id'],
                           parent_col=parent_col,
                           output=self.output().table,
                           input=self.input()['data'].table)
        session.execute(query)


class AllGeographies(WrapperTask):

    year = IntParameter()

    def requires(self):
        for resolution in GEOGRAPHIES[self.year]:
            yield Geography(resolution=resolution, year=self.year)
