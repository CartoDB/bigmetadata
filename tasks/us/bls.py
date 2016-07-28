from tasks.util import (TempTableTask, TableTask, ColumnsTask,
                        DownloadUnzipTask, CSV2TempTableTask,
                        underscore_slugify, shell, classpath)
from tasks.meta import current_session

# We like OrderedDict because it makes it easy to pass dicts
# like {column name : column definition, ..} where order still
# can matter in SQL
from collections import OrderedDict
from luigi import IntParameter, Parameter
import os


# In[3]:

# These imports are useful for checking the database

from tasks.meta import OBSTable, OBSColumn, OBSTag


# In[4]:

# We'll also want these tags for metadata

from tasks.tags import SectionTags, SubsectionTags, UnitTags


# In[5]:

# The first step of most ETLs is going to be downloading the source
# and saving it to a temporary folder.

# `DownloadUnzipTask` is a utility class that handles the file naming
# and unzipping of the temporary output for you.  You just have to
# write the code which will do the download to the output file name.

class DownloadQCEW(DownloadUnzipTask):
    
    year = IntParameter()
    
    URL = 'http://www.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip'
    
    def download(self):
        shell('wget -O {output}.zip {url}'.format(
           output=self.output().path,
           url=self.URL.format(year=self.year)
        ))


# In[9]:

# A lot of processing can be done in PostgreSQL quite easily.
# We have utility classes to more easily bring both Shapefiles
# and CSVs into PostgreSQL.

class RawQCEW(CSV2TempTableTask):
    
    year = IntParameter()
    
    def requires(self):
        return DownloadQCEW(year=self.year)
        
    def input_csv(self):
        return self.input().path



# In[13]:

# QCEW data has a lot of rows we don't actually need --
# these can be filtered out in SQL easily

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


# In[16]:

# We have to create metadata for the measures we're interested
# in from QCEW.  Often metadata tasks aren't parameterized, but
# this one is, since we have to reorganize the table from one row
# per NAICS code to one column per NAICS code, which is easiest
# done programmatically.

class QCEWColumns(ColumnsTask):
    
    naics_code = Parameter()
    naics_name = Parameter()
    naics_description = Parameter()
    
    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'units': UnitTags(),
        }
    
    def columns(self):
        cols = OrderedDict()
        code, name, description = self.naics_code, self.naics_name, self.naics_description
        
        # This gives us easier access to the tags we defined as dependencies
        units = self.input()['units']
        sections = self.input()['sections']
        subsections = self.input()['subsections']
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
        return cols




from tasks.us.naics import NAICS_CODES, is_supersector, is_sector


# In[23]:

# Now that we have our data in a format similar to what we'll need,
# and our metadata lined up, we can tie it together with a `TableTask`.
# Under the hood, `TableTask` handles the relational lifting between
# columns and actual data, and assigns a hash number to the dataset.
#
# Several methods must be overriden for `TableTask` to work:
#
#     `version()`: a version control number, which is useful for
#                  forcing a re-run/overwrite without having to track
#                  down and delete output artifacts.
#
#     `timespan()`: the timespan (for example, '2014', or '2012Q4')
#                   that identifies the date range or point-in-time
#                   for this table.
#
#     `columns()`: an OrderedDict of (colname, ColumnTarget) pairs.
#                  This should be constructed by pulling the
#                  desired columns from required `ColumnsTask` classes.
#
#     `populate()`: a method that should populate (most often via)
#                   INSERT the output table.
#

# Since we have a column ('area_fips') that is a shared reference to
# geometries ('geom_ref') we have to import that column.
from tasks.us.census.tiger import GeoidColumns

class QCEW(TableTask):
    
    year = IntParameter()
    qtr = IntParameter()
    
    def version(self):
        return 1
    
    def requires(self):
        requirements = {
            'data': SimpleQCEW(year=self.year, qtr=self.qtr),
            'geoid_cols': GeoidColumns(),
            'naics': OrderedDict()
        }
        for naics_code, naics_name in NAICS_CODES.iteritems():
            # Only include the more general NAICS codes
            if is_supersector(naics_code) or is_sector(naics_code) or naics_code == '10':
                requirements['naics'][naics_code] = QCEWColumns(
                    naics_code=naics_code,
                    naics_name=naics_name,
                    naics_description='' # TODO these seem to be locked in a PDF
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
        for naics_code, naics_columns in self.input()['naics'].iteritems():
            for colname, coltarget in naics_columns.iteritems():
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
                        input=self.input()['data'].table,
                        colnames=', '.join(colnames),
                        select_colnames=', '.join(select_colnames),
                    )
        session.execute(insert)

