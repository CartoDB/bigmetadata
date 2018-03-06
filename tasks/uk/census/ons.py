# http://webarchive.nationalarchives.gov.uk/20160105160709/http://ons.gov.uk/ons/guide-method/census/2011/census-data/bulk-data/bulk-data-downloads/index.html

from collections import OrderedDict
import csv
import os
import requests
import urllib
import shutil
from zipfile import ZipFile

from luigi import Task, Parameter
from shutil import copyfile

from lib.copy import copy_from_csv
from lib.targets import DirectoryTarget
from tasks.meta import current_session
from tasks.base_tasks import DownloadUnzipTask, TempTableTask, RepoFile

from .metadata import sanitize_identifier


class DownloadUK(Task):
    API_URL = 'https://www.nomisweb.co.uk/api/v01/dataset/def.sdmx.json?search={}*'
    DOWNLOAD_URL = 'https://www.nomisweb.co.uk/api/v01/dataset/{id}.bulk.csv?time=2011&measures=20100&geography={geo}'
    GEO_TYPES = [
        'TYPE258',  # Small Areas (Northern Ireland)
        'TYPE299'   # Output Areas (England, Wales, Scotland)
    ]

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

    def requires(self):
        return DownloadUK(self.table)

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


class DownloadEnglandWalesLocal(DownloadUnzipTask):
    URL = 'https://www.nomisweb.co.uk/output/census/2011/release_4-1_bulk_all_tables.zip'

    def version(self):
        return 1

    def requires(self):
        return RepoFile(resource_id=self.task_id,
                        version=self.version(),
                        url=self.URL)

    def download(self):
        self.output().makedirs()
        copyfile(self.input().path, '{output}.zip'.format(output=self.output().path))

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
