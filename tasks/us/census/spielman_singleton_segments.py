'''
Define special segments for the census
'''

import os
import subprocess

from collections import OrderedDict
from tasks.meta import OBSColumn, OBSColumnToColumn, OBSColumnTag
from tasks.util import (shell,  pg_cursor, classpath,
                        ColumnsTask, TableTask)

from luigi import Task, Parameter, LocalTarget, BooleanParameter


class DownloadSpielmanSingletonFile(Task):

    def filename(self):
        return 'understanding-americas-neighborhoods-using-uncertain-data-from-the-american-community-survey-output-data.zip'

    def url(self):
        return 'https://www.openicpsr.org/repoEntity/download'

    def curl_request(self):
        docid = 41528
        email = 'slynn@cartodb.com'
        return 'curl -L {url} --data "downloaderEmail={email}&agree=I Agree&id={docid}"'.format(
            docid = docid,
            email=email,
            url= self.url());

    def run(self):
        self.output().makedirs()
        try:
            self.download();
        except subprocess.CalledProcessError:
            shell('rm -f {target}'.format(target=self.output().path))
            raise

    def download(self):
        print('running ')
        print('curl -L {url} -o {target}'.format(url=self.url(), target=self.output().path))
        shell('{curl} -o {target}'.format(
            curl=self.curl_request(),
            target=self.output().path))

    def output(self):
        return LocalTarget(path=os.path.join(classpath(self), self.filename()))

class ProcessSpielmanSingletonFile(Task):

    def requires(self):
        return {'downloaded_file' :DownloadSpielmanSingletonFile()}

    def run(self):
        try:
            print 'decompressing'
            self.decompress()
            print 'renaming'
            self.rename_files()
            print 'converting to csv'
            self.convert_to_csv()
            print 'parsing'
            self.grab_relevant_columns()
            print 'cleanup'
            self.cleanup()

        except subprocess.CalledProcessError:
            print 'problem encountered exiting  '
            self.cleanup()
            raise

    def cleanup(self):
        shell('rm -rf {decompressed_folder}'.format(decompressed_folder=self.decompressed_folder()))
        shell('rm -rf {decompressed_folder}'.format(decompressed_folder=self.decompressed_folder()))
        shell('rm -f  {target}'.format(target=self.output().path.replace('.csv','_tmp.csv')))

    def filename(self):
        return self.input()['downloaded_file'].path.split("/")[-1].replace('.zip','.csv')

    def grab_relevant_columns(self):
        shell("awk -F',' 'BEGIN{{OFS=\",\"}}{{print $5, $159, $160, $161, $162}}' {source}  |  sed -e '1s/^.*$/GEOID10,X10,X31,X55,X2/' > {target}".format(
            source =self.output().path.replace('.csv','_tmp.csv') , target=self.output().path))

    def decompressed_folder(self):
        return self.input()['downloaded_file'].path.replace('.zip', '')

    def decompress(self):
        shell('unzip {target} -d {dest}'.format(target=self.input()['downloaded_file'].path,dest=self.decompressed_folder()))

    def convert_to_csv(self):
        shell('ogr2ogr -f "CSV" {target} {decompressed_folder}/US_tract_clusters_new.shp'.format(
            target=self.output().path.replace('.csv','_tmp.csv'),
            decompressed_folder=self.decompressed_folder()))

    def rename_files(self):
        rename = {
            'ustractclustersnew-41530.bin' : 'US_tract_clusters_new.dbf',
            'ustractclustersnew-41538.txt' : 'US_tract_clusters_new.prj',
            'ustractclustersnew-41563.bin' : 'US_tract_clusters_new.shp',
            'ustractclustersnew-41555.bin' : 'US_tract_clusters_new.shx'
        }
        for old_name, new_name in rename.iteritems():
            shell('mv {folder}/{old_name} {folder}/{new_name}'.format(
            old_name=old_name,
            new_name=new_name,
            folder= self.decompressed_folder()))

    def output(self):
        return LocalTarget(path=os.path.join(classpath(self), self.filename()))

class CreateSpielmanSingletonColumns(ColumnsTask):
    def requires(self):
        return {
            'data_file': ProcessSpielmanSingletonFile()
        }

    def columns(self):
        X10 = OBSColumn(
            id='X10',
            type='Text',
            name="SS_segment_10_clusters",
            description='Sociodemographic classes from Spielman and Singleton 2015, 10 clusters',
            aggregate='sum',
            weight=10,
            tags=[censustags['demographics'], tags['population']]
        )
        X2 = OBSColumn(
            id='X2',
            type='Text',
            name="SS_segment_2_clusters",
            description="Sociodemographic classes from Spielman and Singleton 2015, 10 clusters",
            aggregate='sum',
            weight=8,
            targets={total_pop: 'denominator'},
            tags=[censustags['demographics'], tags['population']]
        )
        X31 = OBSColumn(
            id='X31',
            type='Text',
            name="SS_segment_31_clusters",
            description='Sociodemographic classes from Spielman and Singleton 2015, 10 clusters',
            aggregate='sum',
            weight=8,
            targets={total_pop: 'denominator'},
            tags=[censustags['demographics'], tags['population']]
        )
        X55 = OBSColumn(
            id='X55',
            type='Text',
            name="SS_segment_55_clusters",
            description="Sociodemographic classes from Spielman and Singleton 2015, 10 clusters",
            aggregate=nil,
            weight=2,
            tags=[censustags['demographics'], tags['population']]
        )

        return OrderedDict([X10,X2,X31,X55,X2])

#
# class LoadSpielmanSingletonToDB(self):
#     def requires(self):
#         reutrn
#
#     def run(self):
#         session = current_session()
