from luigi import LocalTarget, Task

from tasks.util import (TagsTask, DownloadUnzipTask, Shp2TempTableTask, shell,
                        classpath)

import os


class SourceLicenseTags(TagsTask):

    def tags(self):
        return [
            TagsTask(
                id='eurographics-license',
                name='EuroGeographics Open Data Licence',
                type='license',
                description='This product includes Intellectual Property from European National Mapping and Cadastral Authorities and is licensed on behalf of these by EuroGeographics. Original product is available for free at `www.eurogeographics.org <www.eurogeographics.org>`_ Terms of the licence available at `http://www.eurogeographics.org/form/topographic-data-eurogeographics <http://www.eurogeographics.org/form/topographic-data-eurogeographics>`_'),
            TagsTask(
                id='eurographics-source',
                name='EuroGraphics EuroGlobalMap',
                type='source',
                description='EuroGraphics `EuroGlobalMap <http://www.eurogeographics.org/content/euroglobalmap-opendata?sid=10868>`_')
        ]


class DownloadGeographies(Task):

    URL = 'http://wxs-telechargement.ign.fr/aoar2g7319l0hi4l42nkzlc5/telechargement/prepackage/EGM_EUROPE_PACK_20151028$EGM_8-0SHP_20151028/file/EGM_8-0SHP_20151028.7z'

    def download(self):
        self.output().makedirs()
        referer = 'http://www.eurogeographics.org/content/euroglobalmap-opendata?sid=10868'
        shell("wget -O {output}.7z --referer='{referer}' '{url}'".format(
            output=self.output().path,
            referer=referer,
            url=self.URL,
        ))

    def run(self):
        os.makedirs(self.output().path)
        self.download()
        shell('7z x "{file}" -o{output}'.format(
            file=self.output().path + '.7z',
            output=self.output().path))

        #shell('unzip -d {output} {output}.zip'.format(output=self.output().path))

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))


class ImportGeographies(Shp2TempTableTask):

    def requires(self):
        return DownloadGeographies()
