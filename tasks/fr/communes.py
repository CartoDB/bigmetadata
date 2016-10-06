# -*- coding: utf-8 -*-

from tasks.util import (Shp2TempTableTask, TempTableTask, TableTask, TagsTask, ColumnsTask,
                        DownloadUnzipTask, CSV2TempTableTask,
                        underscore_slugify, shell, classpath)
from tasks.meta import current_session, DENOMINATOR, GEOM_REF, UNIVERSE
from collections import OrderedDict
from luigi import IntParameter, Parameter, WrapperTask, Task, LocalTarget
import os
from tasks.meta import OBSTable, OBSColumn, OBSTag
from tasks.tags import SectionTags, SubsectionTags, UnitTags
import csv
import pandas as pd

class DownloadCommuneAreas(Task):
    # Note that this set of Commune contours corresponds boundaries in 2013

    URL = 'https://wxs-telechargement.ign.fr/oikr5jryiph0iwhw36053ptm/telechargement/inspire/GEOFLA_THEME-COMMUNE_2013_GEOFLA_1-1_SHP_LAMB93_FR-ED131/file/GEOFLA_1-1_SHP_LAMB93_FR-ED131.7z'

    def download(self):
        shell('wget -O {output}.7z {url}'.format(
        output=self.output().path,
        url=self.URL))

    def run(self):
        self.output().makedirs()
        self.download()
        shell('7z x "{file}" -o{output}'.format(file=os.path.join(self.output().path+'.7z'),output=self.output().path))

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))


class ImportCommuneAreas(Shp2TempTableTask):

    def requires(self):
        return DownloadCommuneAreas()

    def input_shp(self):
        return os.path.join(self.input().path, 'GEOFLA_1-1_SHP_LAMB93_FR-ED131/GEOFLA/1_DONNEES_LIVRAISON_2013-11-00161/GEOFLA_1-1_SHP_LAMB93_FR-ED131/COMMUNES/COMMUNE.SHP')
class OutputCommuneColumns(ColumnsTask):

    def version(self):
        return 1

    def requires(self):
        return {
            'subsections': SubsectionTags(),
            'sections': SectionTags(),
        }

    def columns(self):
        input_ = self.input()
        geom = OBSColumn(
            type='Geometry',
            name='Commune areas',
            description=u"La commune est la plus petite subdivision administrative française mais c'est aussi la plus ancienne, puisqu'elle a succédé aux villes et paroisses du Moyen Âge. Elle a été instituée en 1789 avant de connaître un début d'autonomie avec la loi du 5 avril 1884, véritable charte communale. Le maire est l'exécutif de la commune qu'il représente et dont il gère le budget. Il est l'employeur du personnel communal et exerce les compétences de proximité (écoles, urbanisme, action sociale, voirie, transports scolaires, ramassage des ordures ménagères, assainissement...). Il est également agent de l'État pour les fonctions d'état civil, d'ordre public, d'organisation des élections et de délivrance de titres réglementaires. Au 1er janvier 2010 on comptait 36 682 communes, dont 36 570 en métropole. ",
            weight=5,
            tags=[input_['subsections']['boundary'], input_['sections']['fr']]
        )
        geomref = OBSColumn(
            type='Text',
            name='INSEE_COM',
            description='Commune Code',
            weight=0,
            targets={geom: GEOM_REF}
        )

        return OrderedDict([
            ('the_geom', geom),
            ('insee_com', geomref)
        ])


class OutputCommuneAreas(TableTask):

    def requires(self):
        return {
            'geom_columns': OutputCommuneColumns(),
            'data': ImportCommuneAreas(),
        }

    def version(self):
        return 1

    def timespan(self):
        return 2013

    def columns(self):
        input_ = self.input()
        cols = OrderedDict()
        cols.update(input_['geom_columns'])
        return cols

    def populate(self):
        session = current_session()
        session.execute('INSERT INTO {output} '
                        'SELECT ST_MakeValid(wkb_geometry), INSEE_COM '
                        'FROM {input}'.format(
                            output=self.output().table,
                            input=self.input()['data'].table,
                        ))
