# http://webarchive.nationalarchives.gov.uk/20160105160709/http://ons.gov.uk/ons/guide-method/census/2011/census-data/bulk-data/bulk-data-downloads/index.html

from collections import OrderedDict
import csv
import os
import requests
import shutil
from zipfile import ZipFile

from luigi import Task, Parameter

from lib.copy import copy_from_csv
from lib.targets import DirectoryTarget
from lib.timespan import get_timespan
from tasks.base_tasks import RepoFileUnzipTask, TempTableTask, RepoFile, \
    GeoFile2TempTableTask, TableTask, ColumnsTask
from tasks.meta import current_session, GEOM_REF, OBSColumn, OBSTable
from tasks.tags import SectionTags, SubsectionTags, LicenseTags, BoundaryTags

from .metadata import sanitize_identifier, SourceTags

class DownloadUK(Task):
    API_URL = 'https://www.nomisweb.co.uk/api/v01/dataset/def.sdmx.json?search={}*'
    DOWNLOAD_URL = 'https://www.nomisweb.co.uk/api/v01/dataset/{id}.bulk.csv?time=2011&measures=20100&geography={geo}'

    table = Parameter()

    def version(self):
        return 1

    def requires(self):
        requirements = {}
        # Query API, extract table ID from name
        meta = requests.get(self.API_URL.format(self.table)).json()
        api_id = (meta['structure']['keyfamilies']['keyfamily'][0]['id']).lower()
        for geo in self.GEO_TYPES:
            requirements[geo] = RepoFile(resource_id='{task_id}_{geo}'.format(task_id=self.task_id, geo=geo),
                                         version=self.version(),
                                         url=self.DOWNLOAD_URL.format(id=api_id, geo=geo))

        return requirements

    def run(self):
        # Download for SA (EW,S) and OA (NI) in a single file
        with self.output().temporary_path() as tmp, open(os.path.join(tmp, '{}.csv'.format(self.table)), 'wb') as outcsv:
            skip_header = False
            for geo in self.GEO_TYPES:
                with open(self.input()[geo].path, 'rb') as remote_file:
                    if skip_header:
                        next(remote_file)
                    else:
                        skip_header = True
                    for l in remote_file:
                        outcsv.write(l)

    def output(self):
        return DirectoryTarget(self)


class ImportUK(TempTableTask):
    table = Parameter()

    @staticmethod
    def id_to_column(colid):
        return sanitize_identifier(colid)

    def run(self):
        infile = os.path.join(self.input().path, self.table + '.csv')

        cols = OrderedDict([['date', 'TEXT'], ['geography', 'TEXT'], ['geographycode', 'TEXT PRIMARY KEY']])
        with open(infile) as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)

            for c in header[3:]:
                cols[self.id_to_column(c)] = 'NUMERIC'

        with open(infile) as csvfile:
            copy_from_csv(
                current_session(),
                self.output().table,
                cols,
                csvfile
            )


class DownloadUKOutputAreas(DownloadUK):
    GEO_TYPES = [
        'TYPE258',  # Small Areas (Northern Ireland)
        'TYPE299'   # Output Areas (England, Wales, Scotland)
    ]


class ImportUKOutputAreas(ImportUK):
    def requires(self):
        return DownloadUKOutputAreas(self.table)


class DownloadEnglandWalesLocal(RepoFileUnzipTask):
    URL = 'https://www.nomisweb.co.uk/output/census/2011/release_4-1_bulk_all_tables.zip'

    def get_url(self):
        return self.URL

    def run(self):
        super(DownloadEnglandWalesLocal, self).run()
        work_dir = self.output().path
        try:
            for filename in os.listdir(work_dir):
                if filename.endswith('.zip'):
                    ZipFile(os.path.join(work_dir, filename)).extractall(work_dir)
        except:
            shutil.rmtree(work_dir)
            raise


class ImportEnglandWalesLocal(TempTableTask):
    table = Parameter()

    def requires(self):
        return DownloadEnglandWalesLocal()

    @staticmethod
    def id_to_column(colid):
        return colid

    def run(self):
        infile = os.path.join(self.input().path, self.table + 'DATA.CSV')
        cols = OrderedDict({'geographycode': 'TEXT PRIMARY KEY'})
        with open(infile) as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)

            for c in header[1:]:
                cols[self.id_to_column(c)] = 'NUMERIC'

        with open(infile) as csvfile:
            copy_from_csv(
                current_session(),
                self.output().table,
                cols,
                csvfile
            )


class ImportNUTS1Geoms(GeoFile2TempTableTask):
    def requires(self):
        return DownloadNUTS1Geographies()

    def input_files(self):
        return self.input().path


class DownloadNUTS1Geographies(RepoFileUnzipTask):
    URL='https://opendata.arcgis.com/datasets/01fd6b2d7600446d8af768005992f76a_2.zip?outSR=%7B%22wkid%22%3A27700%2C%22latestWkid%22%3A27700%7D'

    def get_url(self):
        return self.URL


class NUTS1Columns(ColumnsTask):
    def version(self):
        return 1

    def requires(self):
        return {
            'subsections': SubsectionTags(),
            'sections': SectionTags(),
            'license': LicenseTags(),
            'boundary': BoundaryTags(),
            'uk_census_sources': SourceTags()
        }

    def columns(self):
        input_ = self.input()

        license = input_['license']['uk_ogl']
        source = input_['uk_census_sources']['ons']
        boundary_type = input_['boundary']
        geom = OBSColumn(
            type='Geometry',
            name='NUTS 1',
            description='The Classification of Territorial Units for '
                        'Statistics, (NUTS, for the French nomenclature '
                        'd\'unités territoriales statistiques), is a geocode '
                        'standard for referencing the administrative '
                        'divisions of countries for statistical purposes. The '
                        'standard was developed by the European Union.'
                        'changed). '
                        '-`Wikipedia <https://en.wikipedia.org/'
                        'wiki/First-level_NUTS_of_the_European_Union>`_',
            weight=8,
            tags=[input_['subsections']['boundary'], input_['sections']['uk'], source, license,
                  boundary_type['cartographic_boundary']]
        )
        geomref = OBSColumn(
            type='Text',
            name='NUTS 1 ID',
            weight=0,
            targets={geom: GEOM_REF}
        )

        geomname = OBSColumn(
            type='Text',
            name='NUTS 1 Name',
            weight=1,
            targets={geom: GEOM_REF}
        )

        return OrderedDict([
            ('the_geom', geom),
            (self.geoid_column(), geomref),
            (self.geoname_column(), geomname)
        ])

    @staticmethod
    def geoid_column():
        return 'nuts1_id'

    @staticmethod
    def geoname_column():
        return 'nuts1_name'


class NUTS1UK(TableTask):
    def requires(self):
        return {
            'geoms': ImportNUTS1Geoms(),
            'geom_columns': NUTS1Columns()
        }

    def version(self):
        return 1

    def table_timespan(self):
        return get_timespan('2015')

    def targets(self):
        return {
            OBSTable(id='.'.join([self.schema(), self.name()])): GEOM_REF,
        }

    def columns(self):
        return OrderedDict(self.input()['geom_columns'])

    def populate(self):
        session = current_session()
        query = 'INSERT INTO {output} ' \
                'SELECT ST_MakeValid(wkb_geometry) as the_geom, ' \
                'nuts118cd as {nuts_id},' \
                'nuts118nm as {nuts_name} ' \
                'FROM {input}'.format(output=self.output().table,
                                      input=self.input()[
                                          'geoms'].table,
                                      nuts_id=NUTS1Columns.geoid_column(),
                                      nuts_name=NUTS1Columns.geoname_column())
        session.execute(query)

    @staticmethod
    def geoid_column():
        return NUTS1Columns.geoid_column()

    @staticmethod
    def geoname_column():
        return NUTS1Columns.geoname_column()
