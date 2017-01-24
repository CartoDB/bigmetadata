import os
import urllib
import csv

from luigi import Task, Parameter, WrapperTask, LocalTarget
from collections import OrderedDict
from tasks.util import (DownloadUnzipTask, shell, TableTask, TempTableTask,
                        classpath, CSV2TempTableTask, ColumnsTask, TagsTask)
from tasks.meta import current_session, OBSColumn, OBSTag
from tasks.au.geo import (
    GEO_STE,
    GEOGRAPHIES, GeographyColumns, Geography)
from tasks.tags import SectionTags, SubsectionTags, UnitTags


PROFILES = (
    'BCP',
)

STATES = (
    # 'AUST',
    'NSW',
    'Vic',
    'Qld',
    'SA',
    'WA',
    'Tas',
    'NT',
    'ACT',
    'OT',
)

TABLES = ['B01','B02','B03','B04A','B04B','B05','B06','B07','B08A','B08B','B09','B10A','B10B','B10C','B11A','B11B','B12A','B12B','B13','B14','B15','B16A','B16B','B17A','B17B','B18','B19','B20A','B20B','B21','B22A','B22B','B23A','B23B','B24','B25','B26','B27','B28','B29','B30','B31','B32','B33','B34','B35','B36','B37','B38','B39','B40A','B40B','B41A','B41B','B41C','B42A','B42B','B43A','B43B','B43C','B43D','B44A','B44B','B45A','B45B','B46',]


URL = 'http://www.censusdata.abs.gov.au/CensusOutput/copsubdatapacks.nsf/All%20docs%20by%20catNo/{year}_{profile}_{resolution}_for_{state}/$File/{year}_{profile}_{resolution}_for_{state}_{header}-header.zip'


class SourceTags(TagsTask):

    def tags(self):
        return [
            OBSTag(id='au-census',
                   name='Australian Bureau of Statistics (ABS)',
                   type='source',
                   description=u'The `Australian Bureau of Statistics <http://abs.gov.au/websitedbs/censushome.nsf/home/datapacks>`')
        ]


class LicenseTags(TagsTask):

    def tags(self):
        return [
            OBSTag(id='au-datapacks-license',
                   name='Creative Commons Attribution 2.5 Australia licence',
                   type='license',
                   description=u'DataPacks is licenced under a `Creative Commons Attribution 2.5 Australia licence <https://creativecommons.org/licenses/by/2.5/au/>`_')
        ]


class DownloadData(DownloadUnzipTask):

    year = Parameter()
    resolution = Parameter()
    profile = Parameter()
    state = Parameter()
    header = Parameter()

    def download(self):
        urllib.urlretrieve(url=URL.format(
                               year=self.year,
                               profile=self.profile,
                               resolution=self.resolution,
                               state=self.state,
                               header=self.header
                           ),
                           filename=self.output().path + '.zip')



class ImportData(CSV2TempTableTask):

    tablename = Parameter()

    year = Parameter()
    resolution = Parameter()
    state = Parameter()
    profile = Parameter(default='BCP')
    header = Parameter(default='short')

    def requires(self):
        return DownloadData(resolution=self.resolution, profile=self.profile,
                            state=self.state, year=self.year, header=self.header)

    def input_csv(self):
        cmd = 'find {path} -name \'{year}Census_{tablename}_{state}_{resolution}_{header}.csv\''.format(
            path=self.input().path,
            year=self.year,
            tablename=self.tablename,
            state=self.state.upper(),
            resolution=self.resolution,
            header=self.header,
            )
        path = shell(cmd)
        path = path.strip()

        return path


class ImportAllTables(WrapperTask):

    year = Parameter()
    resolution = Parameter()
    state = Parameter()

    def requires(self):
        for table in TABLES:
            yield ImportData(resolution=self.resolution, state=self.state,
                             year=self.year, tablename=table)


class ImportAllStates(WrapperTask):

    year = Parameter()
    resolution = Parameter()

    def requires(self):
        for state in STATES:
            yield ImportAllTables(resolution=self.resolution, state=state,
                                  year=self.year)


class ImportAllResolutions(WrapperTask):

    year = Parameter()
    state = Parameter()

    def requires(self):
        for resolution in GEOGRAPHIES:
            yield ImportAllTables(resolution=resolution, state=self.state, year=self.year)


class ImportAll(WrapperTask):

    year = Parameter()

    def requires(self):
        for resolution in GEOGRAPHIES:
            for state in STATES:
                yield ImportAllTables(resolution=resolution, state=state, year=self.year)


class Columns(ColumnsTask):

    year = Parameter()
    profile = Parameter()
    tablename = Parameter()

    def requires(self):
        requirements = {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'units': UnitTags(),
            'source': SourceTags(),
            'license': LicenseTags()
        }
        return requirements

    def version(self):
        return 1

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()

        subsectiontags = input_['subsections']
        unittags = input_['units']
        country = input_['sections']['au']
        source = input_['source']['au-census']
        license = input_['license']['au-datapacks-license']

        # column req's from other tables
        column_reqs = {}

        filepath = "meta/Metadata_{year}_{profile}_DataPack.csv".format(year=self.year, profile=self.profile)

        session = current_session()
        with open(os.path.join(os.path.dirname(__file__),filepath)) as csv_meta_file:
            reader = csv.reader(csv_meta_file, delimiter=',', quotechar='"')

            for line in reader:
                if not line[0].startswith('B'):
                    continue

                # ignore tables we don't care about right now
                if not line[4].startswith(self.tablename):
                    continue

                col_id = line[1]            #B: short
                col_name_en = line[2]       #C: long
                denominators = line[3]      #D: denominators
                tablename = line[4]        #H: datapack file
                col_unit = line[5]          #F: unit
                col_subsections = line[6]   #G: subsection
                tablename_old = line[7]         #H: profile table
                desc = line[8]              #I: Column heading description in profile
                if tablename == 'B02':
                    col_agg = line[9]       #J: (for B02 only)
                else:
                    col_agg = None

                denominators = denominators.split('|')
                # universes = universes.split('|')

                targets_dict = {}
                for x in denominators:
                    x = x.strip()
                    targets_dict[cols.get(x, column_reqs[x].get(session) if x in column_reqs else None)] = 'denominator'
                # for x in universes:
                #     x = x.strip()
                #     targets_dict[cols.get(x, column_reqs[x].get(session) if x in column_reqs else None)] = 'universe'
                targets_dict.pop(None, None)

                col_id = tablename+'_'+col_id

                cols[col_id] = OBSColumn(
                    id=col_id,
                    type='Numeric',
                    name=col_name_en.replace('_', ' '),
                    description =desc,
                    # Ranking of importance, sometimes used to favor certain measures in auto-selection
                    # Weight of 0 will hide this column from the user.  We generally use between 0 and 10
                    weight=5,
                    aggregate= col_agg or 'sum',
                    # Tags are our way of noting aspects of this measure like its unit, the country
                    # it's relevant to, and which section(s) of the catalog it should appear in
                    tags=[source, license, country, unittags[col_unit]],
                    targets= targets_dict
                )

                # append the rest of the subsection tags
                col_subsections = col_subsections.split('|')
                for subsection in col_subsections:
                    subsection = subsection.strip()
                    subsection_tag = subsectiontags[subsection]
                    cols[col_id].tags.append(subsection_tag)

        return cols


#####################################
# COPY TO OBSERVATORY
#####################################
class BCP(TableTask):

    tablename = Parameter()
    year = Parameter()
    resolution = Parameter()

    def version(self):
        return 1

    def requires(self):
        import_data = {}
        for state in STATES:
            import_data[state] = ImportData(resolution=self.resolution,
                                            state=state, profile='BCP',
                                            tablename=self.tablename,
                                            year=self.year)
        return {
            'data': import_data,
            'geo': Geography(resolution=self.resolution, year=self.year),
            'geometa': GeographyColumns(resolution=self.resolution),
            'meta': Columns(year=self.year, profile='BCP', tablename=self.tablename),
        }

    def timespan(self):
        return unicode(self.year)

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()
        cols['region_id'] = input_['geometa']['geom_id']
        for colname, coltarget in input_['meta'].iteritems():
            # if coltarget._id.split('.')[-1].lower().startswith(self.topic.lower()):
            cols[colname] = coltarget
        return cols

    def populate(self):
        session = current_session()
        column_targets = self.columns()
        out_colnames = column_targets.keys()

        for state, input_ in self.input()['data'].iteritems():
            intable = input_.table

            in_colnames = ['"{}"::{}'.format(colname.replace(self.tablename+'_', ''), ct.get(session).type)
                           for colname, ct in column_targets.iteritems()]

            in_colnames[0] = '"region_id"'

            cmd = 'INSERT INTO {output} ({out_colnames}) ' \
                  'SELECT {in_colnames} FROM {input} '.format(
                      output=self.output().table,
                      input=intable,
                      in_colnames=', '.join(in_colnames),
                      out_colnames=', '.join(out_colnames))
            session.execute(cmd)


class BCPAllTables(WrapperTask):

    year = Parameter()
    resolution = Parameter()

    def requires(self):
        for table in TABLES:
            yield BCP(resolution=self.resolution, tablename=table, year=self.year)


class BCPAllGeographiesAllTables(WrapperTask):

    year = Parameter()

    def requires(self):
        for resolution in GEOGRAPHIES:
            yield BCPAllTables(resolution=resolution, year=self.year)
