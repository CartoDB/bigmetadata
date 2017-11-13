# -*- coding: utf-8 -*-

from luigi import LocalTarget, Task, IntParameter, WrapperTask

from tasks.base_tasks import ColumnsTask, TagsTask, Shp2TempTableTask, CSV2TempTableTask, TempTableTask, TableTask
from tasks.meta import current_session, OBSColumn, GEOM_REF, OBSTag
from tasks.util import shell, classpath
from tasks.tags import SectionTags, SubsectionTags, BoundaryTags
from collections import OrderedDict

import os


class SourceLicenseTags(TagsTask):

    def tags(self):
        return [
            OBSTag(
                id='eurographics-license',
                name='EuroGeographics Open Data Licence',
                type='license',
                description='This product includes Intellectual Property from European National Mapping and Cadastral Authorities and is licensed on behalf of these by EuroGeographics. Original product is available for free at `www.eurogeographics.org <www.eurogeographics.org>`_ Terms of the licence available at `http://www.eurogeographics.org/form/topographic-data-eurogeographics <http://www.eurogeographics.org/form/topographic-data-eurogeographics>`_'),
            OBSTag(
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


class ImportSHNGeoms(Shp2TempTableTask):

    def requires(self):
        return DownloadGeographies()

    def input_shp(self):
        #~/bigmetadata/tmp/eurostat.geo/DownloadGeographies__99914b932b/EGM_8-0SHP_20151028/DATA/FullEurope | grep PolbndA
        return os.path.join(self.input().path, 'EGM_8-0SHP_20151028', 'DATA', 'FullEurope', 'PolbndA.shp')


class ImportSHNNames(Shp2TempTableTask):

    encoding = 'utf8'

    def requires(self):
        return DownloadGeographies()

    def input_shp(self):
        return os.path.join(self.input().path, 'EGM_8-0SHP_20151028', 'DATA', 'FullEurope', 'EBM_NAM.dbf')


class DownloadNUTSNames(Task):

    URL = 'http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=dic%2Fen%2Fgeo.dic'

    def run(self):
        shell('wget -O {output} "{url}"'.format(
            output=self.output().path,
            url=self.URL
        ))

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))


class ImportNUTSNames(CSV2TempTableTask):

    delimiter = '\t'
    has_header = False

    def requires(self):
        return DownloadNUTSNames()

    def coldef(self):
        return [('code', 'Text'),
                ('name', 'Text')]

    def input_csv(self):
        return self.input().path


class NUTSSHNCrosswalk(TempTableTask):

    def requires(self):
        return {
            'nuts_names': ImportNUTSNames(),
            'shn_names': ImportSHNNames()
        }

    def run(self):
        session = current_session()
        session.execute('''
            CREATE TABLE {output} AS
            with nuts_unprocessed as (
              SELECT LOWER(CASE
                WHEN code ILIKE 'BE%' THEN
                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(name,
                  ' - Deutschsprachige Gemeinschaft', ''),
                  ' - communes francophones', ''),
                  'Bezirk ', ''),
                  'Hoofdstad', 'Hoofstad'),
                  'Arr. de Bruxelles-Capitale / Arr. van ', ''),
                  'Arr. ', '')
                WHEN code ILIKE 'DE%' THEN
                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(name,
                  ', Kreisfreie Stadt', ''),
                  ', Landkreis', ''),
                  ', Stadtkreis', ''),
                  ' (DE)', ''),
                  'Landkreis Rostock', 'Rostock')
                WHEN code ILIKE 'ES%' THEN
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(name,
                    'Ávila', 'Avila'), -- wrong spelling to match SHN
                    ' / Alacant', ''),
                    'La Palma', 'Las Palmas'),
                    'A Coruña', 'Coruña, A'),
                    'Tenerife', 'Santa Cruz de Tenerife'),
                    ' / ', '/')
                WHEN code ILIKE 'FI%' THEN
                  REPLACE(REPLACE(name,
                    'Åland', 'Landskapet Åland'),
                    'Helsinki-Uusimaa', 'Uusimaa')
                WHEN code ILIKE 'FR%' THEN
                  REPLACE(name, 'Nord (FR)', 'Nord')
                ELSE name
                END) as nuts_name, code, name nuts_original
              FROM {nuts_names}
              WHERE code not like '%\_%'
                and code not like '__%Z%'
                and name not like '%(NUTS%'
                and length(code) = 5
                and name not like '%Unknown%'
            ), nuts as (
              SELECT ROW_NUMBER() OVER (ORDER BY nuts_name) nuts_row, code,
              nuts_name, nuts_original
              FROM nuts_unprocessed
              ORDER BY nuts_name
            ), shn AS (
              SELECT ROW_NUMBER() OVER (ORDER BY namn, ara) shn_row,
              ara, ppl, shn,
              CASE
                WHEN icc ILIKE 'BE' THEN
                  REPLACE(namn, '#Bruxelles-Capitale', '')
                WHEN icc ILIKE 'DE' THEN
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(namn,
                      ' a.d. ', ' an der '),
                      ' i.d. ', ' in der '),
                      'OPf.', 'Oberpfalz'),
                      '(Oldb)', '(Oldenburg)'),
                      ' i. ', ' im '),
                      ' a. ', ' am ')
                WHEN icc ILIKE 'FI' THEN SPLIT_PART(namn, '#', 1)
                ELSE namn
                END shn_name,
                use - 1 AS shn_level
              FROM {shn_names}
              WHERE use = CASE
                WHEN icc IN ('LI', 'ME') THEN 1
                WHEN icc IN ('BG', 'CH', 'CZ', 'HR', 'NO', 'RO', 'SE', 'SK') THEN 2
                WHEN icc IN ('ES', 'FI', 'FR', 'HU', 'IT', 'PT') THEN 3
                WHEN icc IN ('DE', 'BE') THEN 4
              END
              ORDER BY namn, ara
            )
            SELECT b.shn, a.code as nuts3, b.shn_level
            FROM nuts a, shn b
            WHERE LOWER(nuts_name) = LOWER(shn_name)
        '''.format(output=self.output().table,
                   nuts_names=self.input()['nuts_names'].table,
                   shn_names=self.input()['shn_names'].table))


class NUTSColumns(ColumnsTask):

    level = IntParameter(default=3)

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'source_license': SourceLicenseTags(),
            'boundary': BoundaryTags()
        }

    def version(self):
        return 4

    def columns(self):
        input_ = self.input()
        section = input_['sections']['eu']
        subsection = input_['subsections']['boundary']
        source_license = input_['source_license']
        boundary_type = input_['boundary']

        nuts = OBSColumn(
            id='nuts{}'.format(self.level),
            type='Geometry',
            name='NUTS Level {}'.format(self.level),
            tags=[section, subsection, source_license['eurographics-license'],
                  source_license['eurographics-source'], boundary_type['interpolation_boundary'],
                  boundary_type['cartographic_boundary']],
            weight=self.level,
        )

        return OrderedDict([
            ('nuts{}_id'.format(self.level), OBSColumn(
                type='Text',
                targets={nuts: GEOM_REF})),
            ('the_geom', nuts)
        ])


class NUTSGeometries(TableTask):

    level = IntParameter(default=3)

    def version(self):
        return 4

    def timespan(self):
        return 2015

    def requires(self):
        return {
            'nuts_columns': NUTSColumns(level=self.level),
            'nuts_shn_crosswalk': NUTSSHNCrosswalk(),
            'shn_geoms': ImportSHNGeoms()
        }

    def columns(self):
        return self.input()['nuts_columns']

    def populate(self):
        session = current_session()
        session.execute('''
            INSERT INTO {output}
            SELECT SUBSTR(nuts3, 1, 2 + {level}) nuts_code,
                   ST_Union(wkb_geometry) the_geom
            FROM {crosswalk} xwalk, {geoms} geoms
            WHERE CASE
              WHEN xwalk.shn_level = 0 THEN geoms.shn0
              WHEN xwalk.shn_level = 1 THEN geoms.shn1
              WHEN xwalk.shn_level = 2 THEN geoms.shn2
              WHEN xwalk.shn_level = 3 THEN geoms.shn3
            END = xwalk.shn
            GROUP BY SUBSTR(nuts3, 1, 2 + {level})
                        '''.format(
                            level=self.level,
                            output=self.output().table,
                            crosswalk=self.input()['nuts_shn_crosswalk'].table,
                            geoms=self.input()['shn_geoms'].table
                        ))


class AllNUTSGeometries(WrapperTask):
    def requires(self):
        return [NUTSGeometries(level=level) for level in (0, 2, 3)]
