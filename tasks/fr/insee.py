# -*- coding: utf-8 -*-

from tasks.util import (Shp2TempTableTask, TempTableTask, TableTask, TagsTask, ColumnsTask,
                        DownloadUnzipTask, CSV2TempTableTask,
                        underscore_slugify, shell, classpath, MetaWrapper)
from tasks.meta import current_session, DENOMINATOR, GEOM_REF, UNIVERSE
from collections import OrderedDict
from luigi import IntParameter, Parameter, WrapperTask, Task, LocalTarget
import os
from tasks.meta import OBSTable, OBSColumn, OBSTag
from tasks.tags import SectionTags, SubsectionTags, UnitTags, BoundaryTags
import csv
import pandas as pd

class DownloadUnzipFR(DownloadUnzipTask):

    table_theme = Parameter()

    URL_base = 'http://www.insee.fr/fr/ppp/bases-de-donnees/donnees-detaillees/rp2012/infracommunal/'

    def download(self):

        themes = {
            'population':'infra-population-12/',
            'housing':'infra-logement-12/',
            'education':'infra-formation-12/',
            'household':'infra-famille-12/',
            'employment':'infra-activite-resident-12/'
                }

        iris = {
            'population':'infra-population-2012.zip',
            'housing':'infra-logement-2012.zip',
            'education':'infra-formation-2012.zip',
            'household':'infra-famille-2012.zip',
            'employment':'infra-activite-resident-2012.zip'
        }

        URL = self.URL_base + themes.get(self.table_theme) + iris.get(self.table_theme)

        shell('wget -O {output}.zip {url}'.format(
           output=self.output().path,
           url=URL
        ))

class DownloadFR(Task):

    table_theme = Parameter()

    URL_base = 'http://www.insee.fr/fr/ppp/bases-de-donnees/donnees-detaillees/rp2012/infracommunal/'

    def download(self):

        themes = {
            'population':'infra-population-12/',
            'housing':'infra-logement-12/',
            'education':'infra-formation-12/',
            'household':'infra-famille-12/',
            'employment':'infra-activite-resident-12/'
                }

        iris_overseas = {
            'population':'base-ic-evol-struct-pop-2012-com.xls',
            'housing':'base-ic-logement-2012-com.xls',
            'education':'base-ic-diplomes-formation-2012-com.xls',
            'household':'base-ic-couples-familles-menages-2012-com.xls',
            'employment':'base-ic-activite-residents-2012-com.xls'
                }

        URL = self.URL_base + themes.get(self.table_theme) + iris_overseas.get(self.table_theme)

        shell('wget -P {output} {url}'.format(
           output=self.output().path,
           url=URL
        ))

    def run(self):
        self.output().makedirs()
        self.download()

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))


class RawFRData(CSV2TempTableTask):

    resolution = Parameter()
    table_theme = Parameter()

    def requires(self):
        if self.resolution == 'iris':
            return DownloadUnzipFR(table_theme=self.table_theme)
        elif self.resolution == 'iris_overseas':
            return DownloadFR(table_theme=self.table_theme)
        else:
            raise Exception('resolution {} is not permitted'.format(self.resolution))

    def input_csv(self):
        #Read in excel file, searching for it in the input path
        xls = pd.ExcelFile(os.path.join(
            self.input().path,
            [p for p in os.listdir(self.input().path) if p.endswith('.xls')][0]
        ))

        #Remove header
        df = xls.parse(skiprows=5,header=0)
        df.to_csv(os.path.join(self.input().path,'{resolution}_{table_theme}.csv'.format(resolution=self.resolution,table_theme=self.table_theme)),index=False,encoding='utf8')
        return os.path.join(self.input().path,'{resolution}_{table_theme}.csv'.format(resolution=self.resolution,table_theme=self.table_theme))


class SourceTags(TagsTask):
    def version(self):
        return 2

    def tags(self):
        return [
            OBSTag(id='insee',
                   name='National Institute of Statistics and Economic Studies (INSEE)',
                   type='source',
                   description=u'The `Institut national de la statistique et '
                   u'des études économiques <http://www.insee.fr/fr/bases-de-donnees/default.asp?page=recensement/resultats/2012/donnees-detaillees-recensement-2012.htm>`_.')
        ]


class LicenseTags(TagsTask):
    def version(self):
        return 4

    def tags(self):
        return [
            OBSTag(id='insee-license',
                   name='INSEE Copyright',
                   type='license',
                   description=u'Commercial reuse permissible, more details `here <http://www.insee.fr/en/service/default.asp?page=rediffusion/copyright.htm>`_.')
        ]


class FrenchColumns(ColumnsTask):

    table_theme = Parameter()

    def requires(self):
        requirements = {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'unittags': UnitTags(),
            'source': SourceTags(),
            'license': LicenseTags(),
        }
        if self.table_theme != 'population':
            requirements['population_vars'] = FrenchColumns(table_theme='population')

        if self.table_theme in ('employment', 'education'):
            requirements['households_vars'] = FrenchColumns(table_theme='household')
        return requirements

    def version(self):
        return 10

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()

        subsectiontags = input_['subsections']
        unittags = input_['unittags']
        france = input_['sections']['fr']
        column_reqs = {}
        column_reqs.update(input_.get('population_vars', {}))
        column_reqs.update(input_.get('households_vars', {}))

        filepath = "frenchmetadata/French Variables - {}.tsv".format(self.table_theme.title())
        session = current_session()
        with open(os.path.join(os.path.dirname(__file__),filepath)) as tsvfile:
            tsvreader = csv.reader(tsvfile, delimiter="\t")
            # Skip first row (header)
            next(tsvreader, None)
            for line in tsvreader:
                # Ignoring "Universe" and "Description" columns for now...
                var_code, short_name, long_name, var_unit, denominators, \
                  subsections, universe  = line

                denominators = denominators.split(',')
                universes = universe.split(',')

                delete = ['en 2012', '(princ)','(compl)']

                for i in delete:
                    if i in short_name:
                        short_name = short_name.replace(i,'').strip()
                # slugified_lib = underscore_slugify('{}'.format(short_name))
                targets_dict = {}
                for x in denominators:
                    x = x.strip()
                    targets_dict[cols.get(x, column_reqs[x].get(session) if x in column_reqs else None)] = 'denominator'
                for x in universes:
                    x = x.strip()
                    targets_dict[cols.get(x, column_reqs[x].get(session) if x in column_reqs else None)] = 'universe'
                targets_dict.pop(None, None)
                cols[var_code] = OBSColumn(
                    id=var_code,
                    type='Numeric',
                    name=short_name,
                    description =long_name,
                    # Ranking of importance, sometimes used to favor certain measures in auto-selection
                    # Weight of 0 will hide this column from the user.  We generally use between 0 and 10
                    weight=5,
                    aggregate='sum',
                    # Tags are our way of noting aspects of this measure like its unit, the country
                    # it's relevant to, and which section(s) of the catalog it should appear in
                    tags=[france, unittags[var_unit]],
                    targets= targets_dict
                )
                subsections = subsections.split(',')
                for s in subsections:
                    s = s.strip()
                    subsection_tag = subsectiontags[s]
                    cols[var_code].tags.append(subsection_tag)

        source = input_['source']['insee']
        license = input_['license']['insee-license']

        for _, col in cols.iteritems():
            col.tags.append(source)
            col.tags.append(license)

        return cols


class DownloadOutputAreas(DownloadUnzipTask):
    # Note that this set of IRIS contours is from 2013, may need to find 2014 contours to match the data

    URL = 'https://www.data.gouv.fr/s/resources/contour-des-iris-insee-tout-en-un/20150428-161348/iris-2013-01-01.zip'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(
        output=self.output().path,
        url=self.URL))


class ImportOutputAreas(Shp2TempTableTask):

    def requires(self):
        return DownloadOutputAreas()

    def input_shp(self):
        # may need to point to directory iris-2013-01-01?
        return os.path.join(self.input().path, 'iris-2013-01-01.shp')

class OutputAreaColumns(ColumnsTask):

    def version(self):
        return 3

    def requires(self):
        return {
            'subsections': SubsectionTags(),
            'sections': SectionTags(),
            'boundary_type': BoundaryTags()
        }

    def columns(self):
        input_ = self.input()

        geom = OBSColumn(
            type='Geometry',
            name='IRIS and Commune areas',
            description='IRIS regions are defined by INSEE census for purposes of all municipalities '
                        'of over 10000 inhabitants and most towns from 5000 to 10000. For areas in which '
                        'IRIS is not defined, the commune area is given instead. ',
            weight=5,
            tags=[input_['subsections']['boundary'], input_['sections']['fr'],
                  input_['boundary_type']['interpolation_boundary'],
                  input_['boundary_type']['cartographic_boundary'],
                  ]
        )
        geomref = OBSColumn(
            type='Text',
            name='DCOMIRIS',
            description='Full Code IRIS. Result of the concatenation of DEPCOM and IRIS attributes. ',
            weight=0,
            targets={geom: GEOM_REF}
        )

        return OrderedDict([
            ('the_geom', geom),
            ('dcomiris', geomref)
        ])


class OutputAreas(TableTask):

    def requires(self):
        return {
            'geom_columns': OutputAreaColumns(),
            'data': ImportOutputAreas(),
        }

    def version(self):
        return 2

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
                        'SELECT DISTINCT ST_MakeValid(wkb_geometry), DCOMIRIS '
                        'FROM {input}'.format(
                            output=self.output().table,
                            input=self.input()['data'].table,
                        ))


class FranceCensus(TableTask):

    table_theme = Parameter()

    def version(self):
        return 8

    def timespan(self):
        return '2012'

    def requires(self):
        requirements = {
            'iris_data': RawFRData(table_theme=self.table_theme, resolution='iris'),
            'overseas_data': RawFRData(table_theme=self.table_theme, resolution='iris_overseas'),
            'meta': FrenchColumns(table_theme=self.table_theme),
            'geometa': OutputAreaColumns(),
        }
        return requirements

    def columns(self):
        cols = OrderedDict()
        cols['IRIS'] = self.input()['geometa']['dcomiris']
        cols.update(self.input()['meta'])
        return cols

    def populate(self):
        session = current_session()

        column_targets = self.columns()
        colnames = ', '.join(column_targets.keys())
        colnames_typed = ','.join(['{}::{}'.format(colname, ct.get(session).type)
                              for colname, ct in column_targets.iteritems()])
        session.execute('INSERT INTO {output} ({ids}) '
                        'SELECT {ids_typed} '
                        'FROM {input} '.format(
                            ids=colnames,
                            ids_typed=colnames_typed,
                            output=self.output().table,
                            input=self.input()['iris_data'].table
                        ))
        session.execute('INSERT INTO {output} ({ids}) '
                        'SELECT {ids_typed} '
                        'FROM {input} '.format(
                            ids=colnames,
                            ids_typed=colnames_typed,
                            output=self.output().table,
                            input=self.input()['overseas_data'].table
                        ))


class AllGeomsThemesTables(WrapperTask):
    def requires(self):
        topics = ['population', 'housing', 'education', 'household', 'employment']
        for table_theme in topics:
            yield FranceCensus(table_theme=table_theme)


class InseeMetaWrapper(MetaWrapper):

    topic = Parameter()

    params = {
        'topic': ['population', 'housing', 'education', 'household', 'employment']
    }

    def tables(self):
        yield OutputAreas()
        yield FranceCensus(table_theme=self.topic)
