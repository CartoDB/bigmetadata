from tasks.base_tasks import (ColumnsTask, TempTableTask, TableTask, RepoFileUnzipTask, TagsTask,
                              CSV2TempTableTask)
from tasks.util import underscore_slugify, copyfile
from tasks.meta import current_session, DENOMINATOR, UNIVERSE
from tasks.us.naics import (NAICS_CODES, is_supersector, is_sector, is_public_administration,
                            get_parent_code)
from tasks.meta import OBSColumn, OBSTag, GEOM_REF
from tasks.tags import SectionTags, SubsectionTags, UnitTags, LicenseTags
from tasks.us.census.tiger import (GeoidColumns, SumLevel, ShorelineClip,
                                   GEOID_SUMLEVEL_COLUMN, GEOID_SHORELINECLIPPED_COLUMN)
from lib.timespan import get_timespan

from collections import OrderedDict
from luigi import IntParameter, Parameter, WrapperTask

import os
import glob

TIGER_YEAR = '2016'

class DownloadQCEW(RepoFileUnzipTask):

    year = IntParameter()

    URL = 'http://www.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip'

    def get_url(self):
        return self.URL.format(year=self.year)


class RawQCEW(CSV2TempTableTask):

    year = IntParameter()

    def requires(self):
        return DownloadQCEW(year=self.year)

    def input_csv(self):
        qtrs = reversed(range(1, 5))
        for qtr in qtrs:
            file = os.path.join(self.input().path, '{year}.q1-q{qtr}.singlefile.csv'.format(year=self.year, qtr=qtr))
            if glob.glob(file):
                return file


class SimpleQCEW(TempTableTask):

    year = IntParameter()
    qtr = IntParameter()

    def requires(self):
        return RawQCEW(year=self.year)

    def run(self):
        session = current_session()
        session.execute("CREATE TABLE {output} AS "
                        "SELECT * FROM {input} "
                        "WHERE agglvl_code IN ('74', '73', '71') "
                        "  AND year = '{year}' "
                        "  AND qtr = '{qtr}' "
                        "  AND own_code = '5' ".format(
                            input=self.input().table,
                            output=self.output().table,
                            year=self.year,
                            qtr=self.qtr,
                        ))


class BLSSourceTags(TagsTask):
    def version(self):
        return 1

    def tags(self):
        return [OBSTag(id='qcew',
                       name='Quartery Census of Employment and Wages (QCEW)',
                       type='source',
                       description='`Bureau of Labor Statistics QCEW <http://www.bls.gov/cew/home.htm>`_')]


class QCEWColumns(ColumnsTask):

    naics_code = Parameter()

    def version(self):
        return 3

    def requires(self):
        requirements = {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'units': UnitTags(),
            'source': BLSSourceTags(),
            'license': LicenseTags(),
        }
        parent_code = get_parent_code(self.naics_code)
        if parent_code:
            requirements['parent'] = QCEWColumns(naics_code=parent_code)

        return requirements

    def columns(self):
        cols = OrderedDict()
        code, name, description = self.naics_code, NAICS_CODES[self.naics_code], ''

        # This gives us easier access to the tags we defined as dependencies
        input_ = self.input()
        units = input_['units']
        sections = input_['sections']
        subsections = input_['subsections']
        parent = input_.get('parent')
        cols['qtrly_estabs'] = OBSColumn(
            id=underscore_slugify('qtrly_estabs_{}'.format(code)),
            type='Numeric',
            name='Establishments in {}'.format(name),
            description='Count of establishments in a given quarter in the {name} industry (NAICS {code}).'
                        '{name} is {description}.'.format(name=name, code=code, description=description),
            weight=5,
            aggregate='sum',
            tags=[units['businesses'], sections['united_states'], subsections['commerce_economy']],
            targets={parent['qtrly_estabs']: DENOMINATOR} if parent else {},
        )
        cols['avg_wkly_wage'] = OBSColumn(
            # Make sure the column ID is unique within this module
            # If left blank, will be taken from this column's key in the output OrderedDict
            id=underscore_slugify('avg_wkly_wage_{}'.format(code)),
            # The PostgreSQL type of this column.  Generally Numeric for numbers and Text
            # for categories.
            type='Numeric',
            # Human-readable name.  Will be used as header in the catalog
            name='Average weekly wage for {} establishments'.format(name),
            # Human-readable description.  Will be used as content in the catalog.
            description='Average weekly wage for a given quarter in the {name} industry (NAICS {code}).'
                        '{name} is {description}.'.format(name=name, code=code, description=description),
            # Ranking of importance, sometimes used to favor certain measures in auto-selection
            # Weight of 0 will hide this column from the user.  We generally use between 0 and 10
            weight=5,
            # How this measure was derived, for example "sum", "median", "average", etc.
            # In cases of "sum", this means functions downstream can construct estimates
            # for arbitrary geographies
            aggregate='average',
            # Tags are our way of noting aspects of this measure like its unit, the country
            # it's relevant to, and which section(s) of the catalog it should appear in.
            tags=[units['money'], sections['united_states'], subsections['income']],
            targets={cols['qtrly_estabs']: UNIVERSE},
        )
        cols['month3_emplvl'] = OBSColumn(
            id=underscore_slugify('month3_emplvl_{}'.format(code)),
            type='Numeric',
            name='Employees in {} establishments'.format(name),
            description='Number of employees in the third month of a given quarter with the {name} '
                        'industry (NAICS {code}). {name} is {description}.'.format(
                            name=name, code=code, description=description),
            weight=5,
            aggregate='sum',
            tags=[units['people'], sections['united_states'], subsections['employment']],
            targets={parent['month3_emplvl']: DENOMINATOR} if parent else {},
        )
        cols['lq_avg_wkly_wage'] = OBSColumn(
            id=underscore_slugify('lq_avg_wkly_wage_{}'.format(code)),
            type='Numeric',
            name='Average weekly wage location quotient for {} establishments'.format(name),
            description='Location quotient of the average weekly wage for a given quarter relative to '
                        'the U.S. (Rounded to the hundredths place) within the {name} industry (NAICS {code}).'
                        '{name} is {description}.'.format(name=name, code=code, description=description),
            weight=3,
            aggregate=None,
            tags=[units['ratio'], sections['united_states'], subsections['income']],
        )
        cols['lq_qtrly_estabs'] = OBSColumn(
            id=underscore_slugify('lq_qtrly_estabs_{}'.format(code)),
            type='Numeric',
            name='Location quotient of establishments in {}'.format(name),
            description='Location quotient of the quarterly establishment count relative to '
                        'the U.S. (Rounded to the hundredths place) within the {name} industry (NAICS {code}).'
                        '{name} is {description}.'.format(name=name, code=code, description=description),
            weight=3,
            aggregate=None,
            tags=[units['ratio'], sections['united_states'], subsections['commerce_economy']],
        )
        cols['lq_month3_emplvl'] = OBSColumn(
            id=underscore_slugify('lq_month3_emplvl_{}'.format(code)),
            type='Numeric',
            name='Employment level location quotient in {} establishments'.format(name),
            description='Location quotient of the employment level for the third month of a given quarter '
                        'relative to the U.S. (Rounded to the hundredths place) within the {name} '
                        'industry (NAICS {code}). {name} is {description}.'.format(
                            name=name, code=code, description=description),
            weight=3,
            aggregate=None,
            tags=[units['ratio'], sections['united_states'], subsections['employment']],
        )

        source = input_['source']['qcew']
        license = input_['license']['no-restrictions']
        for colname, col in cols.items():
            col.tags.append(source)
            col.tags.append(license)
        return cols


class QCEW(TableTask):

    year = IntParameter()
    qtr = IntParameter()

    def version(self):
        return 4

    def requires(self):
        requirements = {
            'data': SimpleQCEW(year=self.year, qtr=self.qtr),
            'geoid_cols': GeoidColumns(year=TIGER_YEAR),
            'naics': OrderedDict(),
            'sumlevel': SumLevel(year=TIGER_YEAR, geography='county'),
            'shorelineclip': ShorelineClip(year=TIGER_YEAR, geography='county'),
        }
        for naics_code in NAICS_CODES.keys():
            if not is_public_administration(naics_code):
                # Only include the more general NAICS codes
                if is_supersector(naics_code) or is_sector(naics_code) or naics_code == '10':
                    requirements['naics'][naics_code] = QCEWColumns(
                        naics_code=naics_code
                    )
        return requirements

    def targets(self):
        return {
            self.input()['shorelineclip'].obs_table: GEOM_REF,
            self.input()['sumlevel'].obs_table: GEOM_REF,
        }

    def table_timespan(self):
        return get_timespan('{year}Q{qtr}'.format(year=self.year, qtr=self.qtr))

    def columns(self):
        # Here we assemble an OrderedDict using our requirements to specify the
        # columns that go into this table.
        # The column name
        input_ = self.input()
        cols = OrderedDict([
            ('area_fipssl', input_['geoid_cols']['county_{}{}'.format(TIGER_YEAR, GEOID_SUMLEVEL_COLUMN)]),
            ('area_fipssc', input_['geoid_cols']['county_{}{}'.format(TIGER_YEAR, GEOID_SHORELINECLIPPED_COLUMN)])
        ])
        for naics_code, naics_cols in input_['naics'].items():
            for key, coltarget in naics_cols.items():
                naics_name = NAICS_CODES[naics_code]
                colname = underscore_slugify('{}_{}_{}'.format(
                        key, naics_code, naics_name))
                cols[colname] = coltarget
        return cols

    def populate(self):
        # This select statement transforms the input table, taking advantage of our
        # new column names.
        # The session is automatically committed if there are no errors.
        session = current_session()
        columns = self.columns()
        colnames = list(columns.keys())
        select_colnames = []
        input_ = self.input()
        for naics_code, naics_columns in input_['naics'].items():
            for colname in list(naics_columns.keys()):
                select_colnames.append('''MAX(CASE
                    WHEN industry_code = '{naics_code}' THEN {colname} ELSE NULL
                END)::Numeric'''.format(naics_code=naics_code,
                                        colname=colname))
        insert = '''INSERT INTO {output} ({colnames})
                    SELECT area_fips AS area_fipssl, area_fips AS area_fipssc, {select_colnames}
                    FROM {input}
                    GROUP BY area_fips '''.format(
                        output=self.output().table,
                        input=input_['data'].table,
                        colnames=', '.join(colnames),
                        select_colnames=', '.join(select_colnames),
                    )
        session.execute(insert)


class AllQCEW(WrapperTask):

    maxtimespan = Parameter()

    def requires(self):
        maxyear, maxqtr = [int(n) for n in self.maxtimespan.split('Q')]
        for year in range(2012, maxyear + 1):
            for qtr in range(1, 5):
                if year < maxyear or qtr <= maxqtr:
                    yield QCEW(year=year, qtr=qtr)
