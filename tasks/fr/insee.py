# -*- coding: utf-8 -*-

from tasks.base_tasks import ColumnsTask, TableTask, TagsTask, DownloadUnzipTask, CSV2TempTableTask, MetaWrapper
from tasks.util import shell, classpath
from tasks.meta import current_session, GEOM_REF
from collections import OrderedDict
from luigi import Parameter, WrapperTask, Task, LocalTarget
import os
from tasks.meta import OBSColumn, OBSTag
from tasks.tags import SectionTags, SubsectionTags, UnitTags
from tasks.fr.geo import OutputAreaColumns, OutputAreas
import csv
import pandas as pd

TOPICS = ['population', 'housing', 'education', 'household', 'employment']


class DownloadUnzipFR(DownloadUnzipTask):

    table_theme = Parameter()

    URL_base = 'https://www.insee.fr/fr/statistiques/fichier/'

    def download(self):

        themes = {
            'population': '2028582/',
            'housing': '2028267/',
            'education': '2028265/',
            'household': '2028569/',
            'employment': '2028654/'
        }

        iris = {
            'population': 'infra-population-2012.zip',
            'housing': 'infra-logement-2012.zip',
            'education': 'infra-formation-2012.zip',
            'household': 'infra-famille-2012.zip',
            'employment': 'infra-activite-resident-2012.zip'
        }

        URL = self.URL_base + themes.get(self.table_theme) + iris.get(self.table_theme)

        shell('wget -O {output}.zip {url}'.format(
           output=self.output().path,
           url=URL
        ))


class DownloadFR(Task):

    table_theme = Parameter()

    URL_base = 'https://www.insee.fr/fr/statistiques/fichier/'

    def download(self):

        themes = {
            'population': '2028582/',
            'housing': '2028267/',
            'education': '2028265/',
            'household': '2028569/',
            'employment': '2028654/'
        }

        iris_overseas = {
            'population': 'base-ic-evol-struct-pop-2012-com.xls',
            'housing': 'base-ic-logement-2012-com.xls',
            'education': 'base-ic-diplomes-formation-2012-com.xls',
            'household': 'base-ic-couples-familles-menages-2012-com.xls',
            'employment': 'base-ic-activite-residents-2012-com.xls'
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
                   description='The `Institut national de la statistique et '
                   'des études économiques <http://www.insee.fr/fr/bases-de-donnees/default.asp?page=recensement/resultats/2012/donnees-detaillees-recensement-2012.htm>`_.')
        ]


class LicenseTags(TagsTask):
    def version(self):
        return 4

    def tags(self):
        return [
            OBSTag(id='insee-license',
                   name='INSEE Copyright',
                   type='license',
                   description='Commercial reuse permissible, more details `here <http://www.insee.fr/en/service/default.asp?page=rediffusion/copyright.htm>`_.')
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
        return 12

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
        with open(os.path.join(os.path.dirname(__file__), filepath)) as tsvfile:
            tsvreader = csv.reader(tsvfile, delimiter="\t")
            # Skip first row (header)
            next(tsvreader, None)
            for line in tsvreader:
                # Ignoring "Description" column for now...
                var_code, short_name, long_name, var_unit, denominators, \
                    subsections, universe = line

                denominators = denominators.split(',')
                universes = universe.split(',')

                delete = ['en 2012', '(princ)', '(compl)']

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
                    description=long_name,
                    # Ranking of importance, sometimes used to favor certain measures in auto-selection
                    # Weight of 0 will hide this column from the user.  We generally use between 0 and 10
                    weight=5,
                    aggregate='sum',
                    # Tags are our way of noting aspects of this measure like its unit, the country
                    # it's relevant to, and which section(s) of the catalog it should appear in
                    tags=[france, unittags[var_unit]],
                    targets=targets_dict
                )
                subsections = subsections.split(',')
                for s in subsections:
                    s = s.strip()
                    subsection_tag = subsectiontags[s]
                    cols[var_code].tags.append(subsection_tag)

        source = input_['source']['insee']
        license = input_['license']['insee-license']

        for _, col in cols.items():
            col.tags.append(source)
            col.tags.append(license)

        return cols


class FranceCensus(TableTask):

    table_theme = Parameter()

    def version(self):
        return 10

    def targets(self):
        return {
            self.input()['geo'].obs_table: GEOM_REF,
        }

    def timespan(self):
        return '2012'

    def requires(self):
        requirements = {
            'iris_data': RawFRData(table_theme=self.table_theme, resolution='iris'),
            'overseas_data': RawFRData(table_theme=self.table_theme, resolution='iris_overseas'),
            'meta': FrenchColumns(table_theme=self.table_theme),
            'geometa': OutputAreaColumns(),
            'geo': OutputAreas(),
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
        colnames = ', '.join(list(column_targets.keys()))
        colnames_typed = ','.join(['"{}"::{}'.format(colname, ct.get(session).type)
                                   for colname, ct in column_targets.items()])
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


class InseeMetaWrapper(MetaWrapper):

    topic = Parameter()

    params = {
        'topic': TOPICS,
    }

    def tables(self):
        yield OutputAreas()
        yield FranceCensus(table_theme=self.topic)


class InseeAll(WrapperTask):

    def requires(self):
        for topic in TOPICS:
            yield InseeMetaWrapper(topic=topic)
