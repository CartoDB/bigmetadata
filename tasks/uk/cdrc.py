# https://data.cdrc.ac.uk/dataset/cdrc-2011-oac-geodata-pack-uk

from luigi import Task, Parameter, LocalTarget

from tasks.util import (TableTask, ColumnsTask, classpath, shell,
                        DownloadUnzipTask, Shp2TempTableTask)
from tasks.meta import GEOM_REF, OBSColumn, current_session
from tasks.tags import LicenseTags, SectionTags, SubsectionTags, UnitTags

from collections import OrderedDict
import os


class DownloadOutputAreas(DownloadUnzipTask):

    URL = 'https://data.cdrc.ac.uk/dataset/68771b14-72aa-4ad7-99f3-0b8d1124cb1b/resource/8fff55da-6235-459c-b66d-017577b060d3/download/output-area-classification.zip'

    def download(self):
        shell('wget --header=\'Cookie: auth_tkt="96a4778a0e3366127d4a47cf19a9c7d65751e5a9talos!userid_type:unicode"; auth_tkt="96a4778a0e3366127d4a47cf19a9c7d65751e5a9talos!userid_type:unicode";\' -O {output}.zip {url}'.format(
            output=self.output().path,
            url=self.URL))


class ImportOutputAreas(Shp2TempTableTask):

    def requires(self):
        return DownloadOutputAreas()

    def input_shp(self):
        return os.path.join(self.input().path, 'Output Area Classification',
                            'Shapefiles', '2011_OAC.shp')


class OutputAreaColumns(ColumnsTask):

    def version(self):
        return 2

    def requires(self):
        return {
            'subsections': SubsectionTags(),
            'sections': SectionTags(),
        }

    def columns(self):
        input_ = self.input()
        geom = OBSColumn(
            type='Geometry',
            name='Census Output Areas',
            description='The smallest unit for which census data are published '
                        'in the UK.  They contain at least 40 households and '
                        '100 persons, the target size being 125 households. '
                        'They were built up from postcode blocks after the '
                        'census data were available, with the intention of '
                        'standardising population sizes, geographical shape '
                        'and social homogeneity (in terms of dwelling types '
                        'and housing tenure). The OAs generated in 2001 were '
                        'retained as far as possible for the publication of '
                        'outputs from the 2011 Census (less than 3% were '
                        'changed). -`Wikipedia <https://en.wikipedia.org/'
                        'wiki/ONS_coding_system#Geography_of_the_UK_Census>`_',
            weight=8,
            tags=[input_['subsections']['boundary'], input_['sections']['uk']]
        )
        geomref = OBSColumn(
            type='Text',
            name='Census Output Area ID',
            weight=0,
            targets={geom: GEOM_REF}
        )

        return OrderedDict([
            ('the_geom', geom),
            ('oa_sa', geomref)
        ])


class CDRCSegmentColumns(ColumnsTask):

    # TODO
    def columns(self):
        return OrderedDict([
            ('sprgrp', OBSColumn(
                type='Text',
                name='',
            )),
            ('grp', OBSColumn(
                type='Text',
                name='',
            )),
            ('subgrp', OBSColumn(
                type='Text',
                name='',
            )),
        ])


class OutputAreas(TableTask):

    def requires(self):
        return {
            'segment_columns': CDRCSegmentColumns(),
            'geom_columns': OutputAreaColumns(),
            'data': ImportOutputAreas(),
        }

    def version(self):
        return 6

    def timespan(self):
        return 2011

    def columns(self):
        input_ = self.input()
        cols = OrderedDict()
        cols.update(input_['geom_columns'])
        cols.update(input_['segment_columns'])
        return cols

    def populate(self):
        session = current_session()
        session.execute('INSERT INTO {output} '
                        'SELECT ST_MakeValid(wkb_geometry), oa_sa, sprgrp, grp, subgrp '
                        'FROM {input}'.format(
                            output=self.output().table,
                            input=self.input()['data'].table,
                        ))
