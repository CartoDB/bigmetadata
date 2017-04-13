import os
import urllib
import csv
import re

from luigi import Task, Parameter, WrapperTask, LocalTarget
from collections import OrderedDict
from tasks.util import (DownloadUnzipTask, shell, TableTask, TempTableTask,
                        classpath, CSV2TempTableTask, ColumnsTask, TagsTask,
                        MetaWrapper)
from tasks.meta import current_session, OBSColumn, OBSTag
from tasks.au.geo import (
    GEO_STE, SourceTags, LicenseTags,
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
        # all tables except B01/B02 require B01
        if self.tablename != 'B01' and self.tablename != 'B02':
            requirements['B01'] = Columns(tablename='B01', year=self.year, profile=self.profile)
        if self.tablename == 'B04A':
            requirements['B04B'] = Columns(tablename='B04B', year=self.year, profile=self.profile)
        if self.tablename == 'B08A':
            requirements['B08B'] = Columns(tablename='B08B', year=self.year, profile=self.profile)

        if self.tablename == 'B10A':
            requirements['B10B'] = Columns(tablename='B10B', year=self.year, profile=self.profile)
            requirements['B10C'] = Columns(tablename='B10C', year=self.year, profile=self.profile)
        if self.tablename == 'B10B':
            requirements['B10C'] = Columns(tablename='B10C', year=self.year, profile=self.profile)

        if self.tablename == 'B11A':
            requirements['B11B'] = Columns(tablename='B11B', year=self.year, profile=self.profile)

        if self.tablename == 'B12A':
            requirements['B12B'] = Columns(tablename='B12B', year=self.year, profile=self.profile)

        if self.tablename == 'B16A':
            requirements['B16B'] = Columns(tablename='B16B', year=self.year, profile=self.profile)

        if self.tablename == 'B17A':
            requirements['B17B'] = Columns(tablename='B17B', year=self.year, profile=self.profile)

        if self.tablename == 'B20A':
            requirements['B20B'] = Columns(tablename='B20B', year=self.year, profile=self.profile)

        if self.tablename == 'B22A':
            requirements['B22B'] = Columns(tablename='B22B', year=self.year, profile=self.profile)

        if self.tablename == 'B23A':
            requirements['B23B'] = Columns(tablename='B23B', year=self.year, profile=self.profile)

        pattern = re.compile('B2[6-8]')
        if pattern.match(self.tablename):
            requirements['B25'] = Columns(tablename='B25', year=self.year, profile=self.profile)

        pattern = re.compile('B3[1-6]')
        if pattern.match(self.tablename):
            requirements['B29'] = Columns(tablename='B29', year=self.year, profile=self.profile)

        if self.tablename == 'B40A':
            requirements['B40B'] = Columns(tablename='B40B', year=self.year, profile=self.profile)

        if self.tablename == 'B42A':
            requirements['B42B'] = Columns(tablename='B42B', year=self.year, profile=self.profile)

        if self.tablename == 'B44A':
            requirements['B44B'] = Columns(tablename='B44B', year=self.year, profile=self.profile)

        if self.tablename == 'B45A':
            requirements['B45B'] = Columns(tablename='B45B', year=self.year, profile=self.profile)

        if self.tablename == 'B41A':
            requirements['B41B'] = Columns(tablename='B41B', year=self.year, profile=self.profile)
            requirements['B41C'] = Columns(tablename='B41C', year=self.year, profile=self.profile)
        if self.tablename == 'B41B':
            requirements['B41C'] = Columns(tablename='B41C', year=self.year, profile=self.profile)

        if self.tablename == 'B43A':
            requirements['B43B'] = Columns(tablename='B43B', year=self.year, profile=self.profile)
            requirements['B43C'] = Columns(tablename='B43C', year=self.year, profile=self.profile)
            requirements['B43D'] = Columns(tablename='B43D', year=self.year, profile=self.profile)
        if self.tablename == 'B43B':
            requirements['B43C'] = Columns(tablename='B43C', year=self.year, profile=self.profile)
            requirements['B43D'] = Columns(tablename='B43D', year=self.year, profile=self.profile)
        if self.tablename == 'B43C':
            requirements['B43D'] = Columns(tablename='B43D', year=self.year, profile=self.profile)

        return requirements

    def version(self):
        return 7

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
        column_reqs.update(input_.get('B01', {}))
        column_reqs.update(input_.get('B04B', {}))
        column_reqs.update(input_.get('B08B', {}))
        column_reqs.update(input_.get('B10B', {}))
        column_reqs.update(input_.get('B10C', {}))
        column_reqs.update(input_.get('B11B', {}))
        column_reqs.update(input_.get('B12B', {}))
        column_reqs.update(input_.get('B16B', {}))
        column_reqs.update(input_.get('B17B', {}))
        column_reqs.update(input_.get('B20B', {}))
        column_reqs.update(input_.get('B22B', {}))
        column_reqs.update(input_.get('B23B', {}))
        column_reqs.update(input_.get('B25', {}))
        column_reqs.update(input_.get('B29', {}))
        column_reqs.update(input_.get('B40B', {}))
        column_reqs.update(input_.get('B42B', {}))
        column_reqs.update(input_.get('B44B', {}))
        column_reqs.update(input_.get('B45B', {}))
        column_reqs.update(input_.get('B41B', {}))
        column_reqs.update(input_.get('B41C', {}))
        column_reqs.update(input_.get('B43B', {}))
        column_reqs.update(input_.get('B43C', {}))
        column_reqs.update(input_.get('B43D', {}))

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
                col_name = line[2]          #C: name
                denominators = line[3]      #D: denominators
                tablename = line[4]         #H: Tablename
                col_unit = line[5]          #F: unit
                col_subsections = line[6]   #G: subsection
                desc = line[7]              #H: Column heading description in profile
                if tablename == 'B02':
                    col_agg = line[8]       #I: AGG (for B02 only)
                else:
                    col_agg = None
                tabledesc = line[10]             #K: Table description

                denominators = denominators.split('|')

                targets_dict = {}
                for denom_id in denominators:
                    denom_id = denom_id.strip()
                    if not denom_id:
                        continue
                    if denom_id in column_reqs:
                        targets_dict[column_reqs[denom_id].get(session)] = 'denominator'
                    else:
                        targets_dict[cols[denom_id]] = 'denominator'
                targets_dict.pop(None, None)


                cols[col_id] = OBSColumn(
                    id=col_id,
                    type='Numeric',
                    name=col_name,
                    description =tabledesc,
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
        return 2

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
        out_colnames = [oc.lower() for oc in column_targets.keys()]

        failstates = []
        for state, input_ in self.input()['data'].iteritems():
            intable = input_.table

            in_colnames = []
            for colname, target in column_targets.iteritems():

                # weird trailing underscore for australia but no states
                if colname.endswith('Median_rent_weekly_') and \
                   ((self.resolution == 'RA' and state.lower() != 'aust') or \
                    (self.resolution == 'SA4' and state.lower() in ('vic', 'wa', 'ot')) or \
                    (self.resolution == 'SA3' and state.lower() in ('vic', 'wa')) or \
                    (self.resolution == 'SA2' and state.lower() in ('vic', 'wa', 'nsw')) or \
                    (self.resolution == 'SA1' and state.lower() in ('vic', 'wa', 'qld', 'nt', 'sa', 'nsw')) or \
                    (self.resolution == 'GCCSA' and state.lower() in ('vic', 'wa', 'ot')) or \
                    (self.resolution == 'LGA' and state.lower() in ('wa')) or \
                    (self.resolution == 'SLA' and state.lower() in ('wa')) or \
                    (self.resolution == 'SSC' and state.lower() in ('vic', 'wa', 'qld', 'nt', 'sa', 'nsw')) or \
                    (self.resolution == 'POA' and state.lower() in ('wa', 'qld', 'nsw')) or \
                    (self.resolution == 'CED' and state.lower() in ('vic', 'wa')) or \
                    (self.resolution == 'SED' and state.lower() in ('wa', 'ot'))):
                    colname = colname.replace('Median_rent_weekly_', 'Median_rent_weekly')

                in_colnames.append('"{}"::{}'.format(
                    colname.replace(self.tablename + '_', ''),
                    target.get(session).type)
                )

            in_colnames[0] = '"region_id"'

            cmd = 'INSERT INTO {output} ("{out_colnames}") ' \
                  'SELECT {in_colnames} FROM {input} '.format(
                      output=self.output().table,
                      input=intable,
                      in_colnames=', '.join(in_colnames),
                      out_colnames='", "'.join(out_colnames))
            try:
                session.execute(cmd)
            except Exception as err:
                failstates.append(state)
                session.rollback()
        if failstates:
            raise Exception('Error with columns states: {}, resolution: {}, tablename: {}'.format(
                failstates, self.resolution, self.tablename))


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


class BCPAllColumns(WrapperTask):

    year = Parameter()

    def requires(self):
        for table in TABLES:
            yield Columns(profile='BCP', tablename=table, year=self.year)

class BCPMetaWrapper(MetaWrapper):

    resolution = Parameter()
    table = Parameter()
    year = Parameter()

    params = {
        'resolution': GEOGRAPHIES,
        'table': TABLES,
        'year':['2011']
    }

    def tables(self):
        yield Geography(resolution=self.resolution, year=self.year)
        yield BCP(resolution=self.resolution, tablename=self.table, year=self.year)
