from tasks.util import (TempTableTask, TableTask, ColumnsTask,
                        DownloadUnzipTask, TagsTask, CSV2TempTableTask,
                        underscore_slugify, shell, classpath, MetaWrapper)
from tasks.meta import current_session, DENOMINATOR
from tasks.us.naics import (NAICS_CODES, is_supersector, is_sector,
                            get_parent_code)
from tasks.meta import OBSTable, OBSColumn, OBSTag
from tasks.tags import SectionTags, SubsectionTags, UnitTags, LicenseTags
from tasks.us.census.tiger import GeoidColumns, SumLevel

from collections import OrderedDict
from luigi import IntParameter, Parameter, WrapperTask

import os

class DownloadQCEW(DownloadUnzipTask):

    year = IntParameter()

    URL = 'http://www.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(
            output=self.output().path,
            url=self.URL.format(year=self.year)
        ))


class RawQCEW(CSV2TempTableTask):

    year = IntParameter()

    def requires(self):
        return DownloadQCEW(year=self.year)

    def input_csv(self):
        return os.path.join(self.input().path, '{}.q1-q4.singlefile.csv'.format(self.year))


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
        cols['avg_wkly_wage'] = OBSColumn(
            # Make sure the column ID is unique within this module
            # If left blank, will be taken from this column's key in the output OrderedDict
            id=underscore_slugify(u'avg_wkly_wage_{}'.format(code)),
            # The PostgreSQL type of this column.  Generally Numeric for numbers and Text
            # for categories.
            type='Numeric',
            # Human-readable name.  Will be used as header in the catalog
            name=u'Average weekly wage for {} establishments'.format(name),
            # Human-readable description.  Will be used as content in the catalog.
            description=u'Average weekly wage for a given quarter in the {name} industry (NAICS {code}).'
                        u'{name} is {description}.'.format(name=name, code=code, description=description),
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
        )
        cols['qtrly_estabs'] = OBSColumn(
            id=underscore_slugify(u'qtrly_estabs_{}'.format(code)),
            type='Numeric',
            name=u'Establishments in {}'.format(name),
            description=u'Count of establishments in a given quarter in the {name} industry (NAICS {code}).'
                        u'{name} is {description}.'.format(name=name, code=code, description=description),
            weight=5,
            aggregate='sum',
            tags=[units['businesses'], sections['united_states'], subsections['commerce_economy']],
            targets={parent['qtrly_estabs']: DENOMINATOR} if parent else {},
        )
        cols['month3_emplvl'] = OBSColumn(
            id=underscore_slugify(u'month3_emplvl_{}'.format(code)),
            type='Numeric',
            name=u'Employees in {} establishments'.format(name),
            description=u'Number of employees in the third month of a given quarter with the {name} '
                        u'industry (NAICS {code}). {name} is {description}.'.format(
                            name=name, code=code, description=description),
            weight=5,
            aggregate='sum',
            tags=[units['people'], sections['united_states'], subsections['employment']],
            targets={parent['month3_emplvl']: DENOMINATOR} if parent else {},
        )
        cols['lq_avg_wkly_wage'] = OBSColumn(
            id=underscore_slugify(u'lq_avg_wkly_wage_{}'.format(code)),
            type='Numeric',
            name=u'Average weekly wage location quotient for {} establishments'.format(name),
            description=u'Location quotient of the average weekly wage for a given quarter relative to '
                        u'the U.S. (Rounded to the hundredths place) within the {name} industry (NAICS {code}).'
                        u'{name} is {description}.'.format(name=name, code=code, description=description),
            weight=3,
            aggregate=None,
            tags=[units['ratio'], sections['united_states'], subsections['income']],
        )
        cols['lq_qtrly_estabs'] = OBSColumn(
            id=underscore_slugify(u'lq_qtrly_estabs_{}'.format(code)),
            type='Numeric',
            name=u'Location quotient of establishments in {}'.format(name),
            description=u'Location quotient of the quarterly establishment count relative to '
                        u'the U.S. (Rounded to the hundredths place) within the {name} industry (NAICS {code}).'
                        u'{name} is {description}.'.format(name=name, code=code, description=description),
            weight=3,
            aggregate=None,
            tags=[units['ratio'], sections['united_states'], subsections['commerce_economy']],
        )
        cols['lq_month3_emplvl'] = OBSColumn(
            id=underscore_slugify(u'lq_month3_emplvl_{}'.format(code)),
            type='Numeric',
            name=u'Employment level location quotient in {} establishments'.format(name),
            description=u'Location quotient of the employment level for the third month of a given quarter '
                        u'relative to the U.S. (Rounded to the hundredths place) within the {name} '
                        u'industry (NAICS {code}). {name} is {description}.'.format(
                            name=name, code=code, description=description),
            weight=3,
            aggregate=None,
            tags=[units['ratio'], sections['united_states'], subsections['employment']],
        )

        source = input_['source']['qcew']
        license = input_['license']['no-restrictions']
        for colname, col in cols.iteritems():
            col.tags.append(source)
            col.tags.append(license)
        return cols

class QCEW(TableTask):

    year = IntParameter()
    qtr = IntParameter()

    def version(self):
        return 2

    def requires(self):
        requirements = {
            'data': SimpleQCEW(year=self.year, qtr=self.qtr),
            'geoid_cols': GeoidColumns(),
            'naics': OrderedDict()
        }
        for naics_code in NAICS_CODES.keys():
            # Only include the more general NAICS codes
            if is_supersector(naics_code) or is_sector(naics_code) or naics_code == '10':
                requirements['naics'][naics_code] = QCEWColumns(
                    naics_code=naics_code
                )
        return requirements

    def timespan(self):
        return '{year}Q{qtr}'.format(year=self.year, qtr=self.qtr)

    def columns(self):
        # Here we assemble an OrderedDict using our requirements to specify the
        # columns that go into this table.
        # The column name
        input_ = self.input()
        cols = OrderedDict([
            ('area_fips', input_['geoid_cols']['county_geoid'])
        ])
        for naics_code, naics_cols in input_['naics'].iteritems():
            for key, coltarget in naics_cols.iteritems():
                naics_name = NAICS_CODES[naics_code]
                colname = underscore_slugify(u'{}_{}_{}'.format(
                        key, naics_code, naics_name))
                cols[colname] = coltarget
        return cols

    def populate(self):
        # This select statement transforms the input table, taking advantage of our
        # new column names.
        # The session is automatically committed if there are no errors.
        session = current_session()
        columns = self.columns()
        colnames = columns.keys()
        select_colnames = []
        input_ = self.input()
        for naics_code, naics_columns in input_['naics'].iteritems():
            for colname in naics_columns.keys():
                select_colnames.append('''MAX(CASE
                    WHEN industry_code = '{naics_code}' THEN {colname} ELSE NULL
                END)::Numeric'''.format(naics_code=naics_code,
                                        colname=colname
                                       ))
        insert = '''INSERT INTO {output} ({colnames})
                    SELECT area_fips, {select_colnames}
                    FROM {input}
                    GROUP BY area_fips '''.format(
                        output=self.output().table,
                        input=input_['data'].table,
                        colnames=', '.join(colnames),
                        select_colnames=', '.join(select_colnames),
                    )
        session.execute(insert)


class AllQCEW(WrapperTask):

    def requires(self):
        for year in xrange(2012, 2016):
            for qtr in xrange(1, 5):
                yield QCEW(year=year, qtr=qtr)

class QCEWMetaWrapper(MetaWrapper):

    year = IntParameter()
    qtr = IntParameter()
    geography = Parameter()

    params = {
        'year': range(2012,2016),
        'qtr': range(1,5),
        'geography': ['county']
    }

    def tables(self):
        yield QCEW(year=self.year, qtr=self.qtr)
        yield SumLevel(year='2015', geography=self.geography)
