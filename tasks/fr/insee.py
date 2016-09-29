from tasks.util import (Shp2TempTableTask, TempTableTask, TableTask, TagsTask, ColumnsTask,
                        DownloadUnzipTask, CSV2TempTableTask,
                        underscore_slugify, shell, classpath)
from tasks.meta import current_session, DENOMINATOR, GEOM_REF
from collections import OrderedDict
from luigi import IntParameter, Parameter, WrapperTask
import os
from tasks.meta import OBSTable, OBSColumn, OBSTag
from tasks.tags import SectionTags, SubsectionTags, UnitTags
import csv
import pandas as pd

class DownloadFranceCensus(DownloadUnzipTask):

    resolution = Parameter()
    table_theme = Parameter()

    URL = 'http://www.insee.fr/fr/ppp/bases-de-donnees/donnees-detaillees/rp2012/infracommunal/infra-population-12/infra-population-2012.zip'

    def download(self):

        resolutions = {'iris':'http://www.insee.fr/fr/ppp/bases-de-donnees/donnees-detaillees/rp2012/infracommunal/'}
        table_themes = {'population':'infra-population-12/infra-population-2012.zip'}

        shell('wget -O {output}.zip {url}'.format(
           output=self.output().path,
           url=self.URL
        ))

class RawFRData(CSV2TempTableTask):

    resolution = Parameter()
    table_theme = Parameter()

    def requires(self):
        return DownloadFranceCensus(table_theme=self.table_theme, resolution=self.resolution)

    def input_csv(self):
        #Read in excel file
        xls = pd.ExcelFile(os.path.join(self.input().path,os.listdir(self.input().path)[0]))

        #Remove header
        df = xls.parse(skiprows=5,header=0)
        df.to_csv(os.path.join(self.input().path,'{resolution}_{table_theme}.csv'.format(resolution=self.resolution,table_theme=self.table_theme)),index=False,encoding='utf8')
        return os.path.join(self.input().path,'{resolution}_{table_theme}.csv'.format(resolution=self.resolution,table_theme=self.table_theme))


class SourceTags(TagsTask):
    def version(self):
        return 1

    def tags(self):
        return [
            OBSTag(id='insee',
                   name='INSEE',
                   type='source',
                   description='http://www.insee.fr/fr/bases-de-donnees/default.asp?page=recensement/resultats/2012/donnees-detaillees-recensement-2012.htm')
        ]


class FrenchColumns(ColumnsTask):

    table_theme = Parameter()

    def requires(self):
        requirements = {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'unittags': UnitTags(),
            'sourcetag': SourceTags()
        }
        if self.table_theme != 'population':
            requirements['population_vars'] = FrenchColumns(table_theme='population')
        return requirements

    def version(self):
        return 6

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()

        subsections = input_['subsections']
        unittags = input_['unittags']
        france = input_['sections']['fr']
        pop_columns = input_.get('population_vars', {})

        filepath = "frenchmetadata/{} VARIABLES - IRIS {}.tsv".format(self.table_theme.upper(),self.table_theme.upper())
        session = current_session()
        with open(os.path.join(os.path.dirname(__file__),filepath)) as tsvfile:
            tsvreader = csv.reader(tsvfile, delimiter="\t")
            # Skip first row (header)
            next(tsvreader, None)
            for line in tsvreader:
                # Ignoring "Universe" and "Description" columns for now...
                var_code,short_name,long_name,var_unit,denominators = line[0], line[1], line[2], line[3], line[4]
                denominators = denominators.split(',')
                # slugified_lib = underscore_slugify('{}'.format(short_name))
                targets_dict = {}
                for x in denominators:
                    x = x.strip()
                    targets_dict[cols.get(x, pop_columns[x].get(session) if x in pop_columns else None)] = 'denominator'
                targets_dict.pop(None, None)
                cols[var_code] = OBSColumn(
                    id=var_code,
                    type='Numeric',
                    name=long_name,
                    # No description yet, can be added later.
                    # Ranking of importance, sometimes used to favor certain measures in auto-selection
                    # Weight of 0 will hide this column from the user.  We generally use between 0 and 10
                    weight=5,
                    aggregate='sum',
                    # Tags are our way of noting aspects of this measure like its unit, the country
                    # it's relevant to, and which section(s) of the catalog it should appear in
                    # Need to fix Subsection and UnitTags! Problem with unittag "families"
                    tags=[france],
                    targets= targets_dict
                )


        insee_source = input_['sourcetag']['insee']

        for _,col in cols.iteritems():
            col.tags.append(insee_source)
        return cols


class DownloadOutputAreas(DownloadUnzipTask):
    # Note that this set of IRIS contours is from 2013

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
            name='IRIS areas',
            description='IRIS regions are defined by INSEE census for purposes of all municipalities '
                        'of over 10000 inhabitants and most towns from 5000 to 10000.',
            weight=5,
            tags=[input_['subsections']['boundary'], input_['sections']['fr']]
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
                        'SELECT ST_MakeValid(wkb_geometry), DCOMIRIS '
                        'FROM {input}'.format(
                            output=self.output().table,
                            input=self.input()['data'].table,
                        ))


class FranceCensus(TableTask):

    table_theme = Parameter()
    resolution = Parameter()

    def version(self):
        return 3

    def timespan(self):
        return '2013'

    def requires(self):
        requirements = {
            'data': RawFRData(table_theme=self.table_theme, resolution=self.resolution),
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
                            input=self.input()['data'].table
                        ))


class AllGeomsThemesTables(WrapperTask):
    def requires(self):
        topics = ['population', 'housing', 'education', 'household', 'employment']
        for resolution in ('iris'):
            for table_theme in topics:
                yield FranceCensus(table_theme=table_theme, resolution=resolution)
