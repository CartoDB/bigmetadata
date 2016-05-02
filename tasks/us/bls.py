#!/usr/bin/env python

'''
Bigmetadata tasks

tasks to download and create metadata
'''

import os
import requests

from collections import OrderedDict
from luigi import Task, IntParameter, LocalTarget, BooleanParameter
from tasks.util import (TableTarget, shell, classpath, underscore_slugify,
                        CartoDBTarget, sql_to_cartodb_table,
                        TableTask, ColumnsTask)
from tasks.meta import OBSColumn, current_session
from tasks.us.census.tiger import GeoidColumns
from psycopg2 import ProgrammingError


class NAICSColumns(ColumnsTask):

    def columns(self):
        return {
            'industry_code': OBSColumn(type='Text',
                                       name='Six-digit NAICS Industry Code',
                                       description="6-character Industry Code (NAICS SuperSector)",
                                       weight=0),
            'industry_title': OBSColumn(type='Text',
                                        name='NAICS Industry Title',
                                        description='Title of NAICS industry',
                                        weight=0)
        }


class NAICS(TableTask):

    URL = 'http://www.bls.gov/cew/doc/titles/industry/industry_titles.csv'

    def requires(self):
        return NAICSColumns()

    def timespan(self):
        return None

    def bounds(self):
        return 'BOX(0 0,0 0)'

    def columns(self):
        return self.input()

    def populate(self):
        tablename = self.output().table
        shell("curl '{url}' | psql -c 'COPY {output} FROM STDIN WITH CSV HEADER'".format(
            output=tablename,
            url=self.URL
        ))


class RawQCEWColumns(ColumnsTask):

    def columns(self):
        return OrderedDict([
            ("own_code", OBSColumn(
                type='Text',
                name='Ownership Code',
                description="1-character ownership code: "
                            "http://www.bls.gov/cew/doc/titles/ownership/"
                            "ownership_titles.htm", # 5 for private
                weight=0
            )),
            ("agglvl_code", OBSColumn(
                type='Text',
                name='Aggregation Level Code',
                description="2-character aggregation level code: "
                            "http://www.bls.gov/cew/doc/titles/agglevel/agglevel_titles.htm",
                weight=0
            )),
            ("size_code", OBSColumn(
                type='Text',
                description="1-character size code: "
                            "http://www.bls.gov/cew/doc/titles/size/size_titles.htm",
                name='Size code',
                weight=0
            )),
            ("year", OBSColumn(
                type='Text',
                description="4-character year",
                name='Year',
                weight=0
            )),
            ("qtr", OBSColumn(
                type='Text',
                description="1-character quarter (always A for annual)",
                name='Quarter',
                weight=0
            )),
            ("disclosure_code", OBSColumn(
                type='Text',
                description="1-character disclosure code (either ' '(blank)), or 'N' not disclosed)",
                name='Disclosure code',
                weight=0
            )),
            ("qtrly_estabs", OBSColumn(
                type='Numeric',
                description="Count of establishments for a given quarter",
                name='Establishment count',
                aggregate='sum',
                weight=0
            )),
            ("month1_emplvl", OBSColumn(
                type='Numeric',
                description="Employment level for the first month of a given quarter",
                name='First month employment',
                aggregate='sum',
                weight=0
            )),
            ("month2_emplvl", OBSColumn(
                type='Numeric',
                description="Employment level for the second month of a given quarter",
                name='Second month employment',
                aggregate='sum',
                weight=0
            )),
            ("month3_emplvl", OBSColumn(
                type='Numeric',
                description="Employment level for the third month of a  given quarter",
                name='Third month employment',
                aggregate='sum',
                weight=0
            )),
            ("total_qtrly_wages", OBSColumn(
                type='Numeric',
                description="Total wages for a given quarter",
                name='Total wages',
                aggregate='sum',
                weight=0
            )),
            ("taxable_qtrly_wages", OBSColumn(
                type='Numeric',
                description="Taxable wages for a given quarter",
                name='Taxable wages',
                aggregate='sum',
                weight=0
            )),
            ("qtrly_contributions", OBSColumn(
                type='Numeric',
                description="Quarterly contributions for a given quarter",
                name='Total contributions',
                aggregate='sum',
                weight=0
            )),
            ("avg_wkly_wage", OBSColumn(
                type='Numeric',
                description="Average weekly wage for a given quarter",
                name='Average weekly wage',
                aggregate='sum',
                weight=0
            )),
            ("lq_disclosure_code", OBSColumn(
                type='Text',
                description="1-character location-quotient disclosure code "
                            "(either ' '(blank)), or 'N' not disclosed",
                name='Location quotient disclosure code',
                aggregate='sum',
                weight=0
            )),
            ("lq_qtrly_estabs", OBSColumn(
                type='Numeric',
                description="Location quotient of the quarterly establishment "
                            "count relative to the U.S. (Rounded to hundredths place)",
                name='Location quotient',
                aggregate='sum',
                weight=0
            )),
            ("lq_month1_emplvl", OBSColumn(
                type='Numeric',
                description="Location quotient of the employment level for the "
                            "first month of a given quarter relative to the "
                            "U.S. (Rounded to hundredths place)),",
                name="Location quotient first month",
                aggregate='sum',
                weight=0
            )),
            ("lq_month2_emplvl", OBSColumn(
                type='Numeric',
                description="Location quotient of the employment level for the "
                            "second month of a given quarter relative to the "
                            "U.S. (Rounded to hundredths place)),",
                name="Location quotient second month",
                aggregate='sum',
                weight=0
            )),
            ("lq_month3_emplvl", OBSColumn(
                type='Numeric',
                description="Location quotient of the employment level for the "
                            "third month of a given quarter relative to the "
                            "U.S. (Rounded to hundredths place)),",
                name="Location quotient third month",
                aggregate='sum',
                weight=0
            )),
            ("lq_total_qtrly_wages", OBSColumn(
                type='Numeric',
                description="Location quotient of the total wages for a given "
                            "quarter relative to the U.S. (Rounded to hundredths place)",
                name="Location quotient quarterly",
                aggregate='sum',
                weight=0
            )),
            ("lq_taxable_qtrly_wages", OBSColumn(
                type='Numeric',
                description="Location quotient of the total taxable wages for "
                            "a given quarter relative to the U.S. (Rounded to hundredths "
                            "place)",
                weight=0,
                aggregate='sum',
                name="Quarterly location quotient taxable wages"
            )),
            ("lq_qtrly_contributions", OBSColumn(
                type='Numeric',
                description="Location quotient of the total contributions for "
                            "a given quarter relative to the U.S. (Rounded to "
                            "hundredths place)",
                weight=0,
                aggregate='sum',
                name="Quarterly location quotient contributions"
            )),
            ("lq_avg_wkly_wage", OBSColumn(
                type='Numeric',
                description="Location quotient of the average weekly wage for "
                            "a given quarter relative to the U.S. (Rounded to "
                            "hundredths place)",
                weight=0,
                aggregate='sum',
                name="Quarterly location quotient weekly wage"
            )),
            ("oty_disclosure_code", OBSColumn(
                type='Text',
                description="1-character over-the-year disclosure code (either "
                            "' '(blank)), or 'N' not disclosed)",
                weight=0,
                aggregate='sum',
                name="Over-the-year Disclosure code"
            )),
            ("oty_qtrly_estabs_chg", OBSColumn(
                type='Numeric',
                description="Over-the-year change in the count of "
                            "establishments for a given quarter",
                weight=0,
                aggregate='sum',
                name="Over-the-year change in establishment count"
            )),
            ("oty_qtrly_estabs_pct_chg", OBSColumn(
                type='Numeric',
                description="Over-the-year percent change in the count of "
                            "establishments for a given quarter (Rounded to "
                            "the tenths place)",
                weight=0,
                name="Over-the-year percent change in establishment count"
            )),
            ("oty_month1_emplvl_chg", OBSColumn(
                type='Numeric',
                description="Over-the-year change in the first month's "
                            "employment level of a given quarter",
                weight=0,
                aggregate='sum',
                name="Over-the-year change in first month employment level"
            )),
            ("oty_month1_emplvl_pct_chg", OBSColumn(
                type='Numeric',
                description="Over-the-year percent change in the first month's "
                            "employment level of a given quarter (Rounded to "
                            "the tenths place)),",
                weight=0,
                name="Over-the-year percent change in first month employment level"
            )),
            ("oty_month2_emplvl_chg", OBSColumn(
                type='Numeric',
                description="Over-the-year change in the second month's "
                            "employment level of a given quarter",
                weight=0,
                aggregate='sum',
                name="Over-the-year change in second month employment level"
            )),
            ("oty_month2_emplvl_pct_chg", OBSColumn(
                type='Numeric',
                description="Over-the-year percent change in the second "
                            "month's employment level of a given quarter "
                            "(Rounded to the tenths place)",
                weight=0,
                aggregate='sum',
                name="Over-the-year percent change in second month employment level"
            )),
            ("oty_month3_emplvl_chg", OBSColumn(
                type='Numeric',
                description="Over-the-year change in the third month's "
                            "employment level of a given quarter",
                weight=0,
                aggregate='sum',
                name="Over-the-year change in third month employment level"
            )),
            ("oty_month3_emplvl_pct_chg", OBSColumn(
                type='Numeric',
                description="Over-the-year percent change in the third month's "
                            "employment level of a given quarter (Rounded to "
                            "the tenths place)",
                weight=0,
                aggregate='sum',
                name="Over-the-year percent change in third month employment level"
            )),
            ("oty_total_qtrly_wages_chg", OBSColumn(
                type='Numeric',
                description="Over-the-year change in total quarterly wages for a given quarter",
                weight=0,
                aggregate='sum',
                name="Over-the-year change in total quarterly wages"
            )),
            ("oty_total_qtrly_wages_pct_chg", OBSColumn(
                type='Numeric',
                description="Over-the-year percent change in total quarterly "
                            "wages for a given quarter (Rounded to the tenths place)",
                weight=0,
                aggregate='sum',
                name="Over-the-year percent change in total quarterly wages"
            )),
            ("oty_taxable_qtrly_wages_chg", OBSColumn(
                type='Numeric',
                description="Over-the-year change in taxable quarterly wages "
                            "for a given quarter",
                weight=0,
                aggregate='sum',
                name="Over-the-year change in taxable quarterly wages"
            )),
            ("oty_taxable_qtrly_wages_pct_chg", OBSColumn(
                type='Numeric',
                description="Over-the-year percent change in taxable quarterly "
                            "wages for a given quarter (Rounded to the tenths "
                            "place)",
                weight=0,
                aggregate='sum',
                name="Over-the-year percent change in taxable quarterly wages"
            )),
            ("oty_qtrly_contributions_chg", OBSColumn(
                type='Numeric',
                description="Over-the-year change in quarterly contributions "
                            "for a given quarter",
                weight=0,
                aggregate='sum',
                name="Over-the-year change in quarterly contributions"
            )),
            ("oty_qtrly_contributions_pct_chg", OBSColumn(
                type='Numeric',
                description="Over-the-year percent change in quarterly "
                            "contributions for a given quarter (Rounded to the "
                            "tenths place)",
                weight=0,
                aggregate='sum',
                name="Over-the-year percent change in quarterly contributions"
            )),
            ("oty_avg_wkly_wage_chg", OBSColumn(
                type='Numeric',
                description="Over-the-year change in average weekly wage for a "
                            "given quarter",
                weight=0,
                aggregate='sum',
                name="Over-the-year change in average weekly wage"
            )),
            ("oty_avg_wkly_wage_pct_chg", OBSColumn(
                type='Numeric',
                description="Over-the-year percent change in average weekly "
                            "wage for a given quarter (Rounded to the tenths "
                            "place)",
                weight=0,
                aggregate='sum',
                name="Over-the-year percent change in average weekly wage"
            ))
        ])


class DownloadQCEW(Task):

    year = IntParameter()
    URL = 'http://www.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip'

    def run(self):
        self.output().makedirs()
        shell('wget \'{url}\' -O {output}.zip'.format(
            url=self.URL.format(year=self.year), output=self.output().path))
        shell('unzip -o {output}.zip -d $(dirname {output})'.format(output=self.output().path))
        shell('mv $(dirname {output})/*.csv {output}'.format(output=self.output().path))

    def output(self):
        return LocalTarget(path=os.path.join(
            classpath(self), self.task_id))


class RawQCEW(TableTask):

    year = IntParameter()

    def timespan(self):
        return self.year

    def bounds(self):
        return 'BOX(0 0,0 0)'

    def requires(self):
        return {
            'data': DownloadQCEW(year=self.year),
            'metadata': RawQCEWColumns(),
            'geoids': GeoidColumns(),
            'naics': NAICSColumns()
        }

    def columns(self):
        columns = OrderedDict()
        columns['county_fips'] = self.input()['geoids']['county_geoid']
        qcew_columns = self.input()['metadata']
        columns['own_code'] = qcew_columns.pop('own_code')
        columns['naics_industry_code'] = self.input()['naics']['industry_code']
        columns.update(qcew_columns)
        return columns

    def populate(self):
        session = current_session()
        shell("psql -c '\\copy {table} FROM {input} WITH CSV HEADER'".format(
            table=self.output().table, input=self.input()['data'].path))


class SimpleQCEWColumns(ColumnsTask):

    year = IntParameter()

    def requires(self):
        return {
            'naics': NAICS(),
            'qcew_meta': RawQCEWColumns(),
            'qcew_data': RawQCEW(year=self.year)
        }

    def columns(self):
        session = current_session()
        columns = OrderedDict()

        inputcols = OrderedDict([(k, self.input()['qcew_meta'][k].get(session))
                                 for k in ('avg_wkly_wage', 'qtrly_estabs',
                                           'month3_emplvl', 'lq_avg_wkly_wage',
                                           'lq_qtrly_estabs', 'lq_month3_emplvl')])

        code_to_name = dict([(code, category) for code, category in session.execute(
            'select industry_code, industry_title from {naics}'.format(
                naics=self.input()['naics'].table))])

        # TODO implement shared column on industry_code
        codes = session.execute('SELECT DISTINCT naics_industry_code '
                                'FROM {qcew} '
                                'WHERE (LENGTH(naics_industry_code) = 4 '
                                '  AND naics_industry_code LIKE \'10%\') '
                                '  OR LENGTH(naics_industry_code) = 2 '
                                '  OR naics_industry_code LIKE \'%-%\' ' # 2-digit codes sometimes hyphenated
                                'ORDER BY naics_industry_code'.format(
                                    qcew=self.input()['qcew_data'].table))
        for code, in codes:
            name = code_to_name[code]
            for inputkey, inputcol in inputcols.iteritems():
                key = '{key}_naics{code}'.format(code=underscore_slugify(code),
                                            key=inputkey)
                columns[key] = OBSColumn(
                    type=inputcol.type,
                    description='{inputdescription} for {name}'.format(
                        inputdescription=inputcol.description,
                        name=name
                    ),
                    weight=inputcol.weight,
                    aggregate=inputcol.aggregate,
                    name='{inputname} for {name}'.format(
                        inputname=inputcol.name,
                        name=name
                    )
                )
        return columns


class SimpleQCEW(TableTask):
    '''
    Isolate the rows of QCEW we actually care about without significantly
    modifying the schema.  Brings us down to one quarter.

    We pull out private employment at the county level, divided by three-digit
    NAICS code and supercategory (four-digits, but simpler).

    agglvl 74: 2-digit by ownership
    agglvl 73: superlevel by ownership
    agglvl 71: total
    '''
    year = IntParameter()
    qtr = IntParameter()

    def version(self):
        return 1

    def requires(self):
        return RawQCEW(year=self.year)

    def timespan(self):
        return '{year}Q{quarter}'.format(year=self.year,
                                         quarter=self.qtr)

    def bounds(self):
        return 'BOX(0 0,0 0)'

    def columns(self):
        return RawQCEW(year=self.year).columns()

    def populate(self):
        session = current_session()
        session.execute('INSERT INTO {output} '
                        'SELECT * FROM {qcew} '
                        "WHERE agglvl_code IN ('74', '73', '71')"
                        "      AND year = '{year}'"
                        "      AND qtr = '{qtr}'"
                        "      AND own_code = '5'".format(
                            qtr=self.qtr,
                            year=self.year,
                            output=self.output().table,
                            qcew=self.input().table))


class QCEW(TableTask):
    '''
    Turn QCEW data into a columnar format that works better for upload
    '''

    year = IntParameter()
    qtr = IntParameter()

    def version(self):
        return 1

    def requires(self):
        return {
            'tiger_meta': GeoidColumns(),
            'qcew_meta': SimpleQCEWColumns(self.year),
            'qcew_data': SimpleQCEW(self.year, qtr=self.qtr),
        }

    def columns(self):
        columns = OrderedDict()
        columns['county_fips'] = self.input()['tiger_meta']['county_geoid']
        columns.update(self.input()['qcew_meta'])
        return columns

    def bounds(self):
        return 'BOX(0 0,0 0)'

    def timespan(self):
        return '{year}Q{qtr}'.format(year=self.year, qtr=self.qtr)


    def populate(self):
        session = current_session()
        input_table = self.input()['qcew_data'].table
        output_table = self.output().table
        session.execute('INSERT INTO {output} (county_fips) '
                        'SELECT distinct county_fips FROM {qcew} '.format(
                            output=output_table,
                            qcew=input_table,
                        ))

        #for col in self.input()['qcew'].table.columns:

        # prepare updates by naics code
        cols = SimpleQCEWColumns(self.year).columns()
        updates = {}
        for colname, col in cols.iteritems():
            if '_naics' not in colname:
                pass
            old_colname, naics_code = colname.split('_naics')
            if naics_code not in updates:
                updates[naics_code] = []
            updates[naics_code].append((colname, old_colname, ))

        for naics_code, columns in updates.iteritems():
            setstatement = ','.join([
                '{colname} = input.{old_colname}'.format(colname=colname,
                                                         old_colname=old_colname)
                for colname, old_colname in columns])
            query = ('UPDATE {output} SET {setstatement} '
                     'FROM {qcew} as input '
                     'WHERE naics_industry_code = \'{code}\' AND '
                     'input.county_fips = {output}.county_fips'.format(
                         output=output_table,
                         setstatement=setstatement,
                         qcew=input_table,
                         code=naics_code
                     ), )[0]
            print query
            session.execute(query)
