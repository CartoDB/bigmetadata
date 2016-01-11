#!/usr/bin/env python

'''
Bigmetadata tasks

tasks to download and create metadata
'''

from luigi import Task, Parameter, LocalTarget
from tasks.util import (TableTarget, ColumnTarget, shell, classpath,
                        DefaultPostgresTarget, pg_cursor)

import os
import requests


#class NAICS(Task):
#
#    URL = 'http://www.bls.gov/cew/doc/titles/industry/industry_titles.csv'
#
#    def run(self):
#        pass
#
#    def output(self):
#        pass
#
#
#class QCEWLayoutTable(Task):
#
#    URL = 'http://www.bls.gov/cew/doc/layouts/csv_quarterly_layout.csv'
#
#    def run(self):
#        shell('curl {url}'.format(url=self.URL))
#
#    def output(self):
#        yield TableTarget(filename='qcew')
#
#
#class QCEWLayoutColumns(Task):
#
#    def requires(self):
#        yield QCEWLayoutTable()
#
#    def run(self):
#        pass
#
#    def output(self):
#        for col in self.input():
#            yield ColumnTarget(filename=col)


class DownloadQCEW(Task):

    year = Parameter()
    URL = 'http://www.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip'

    #def requires(self):
    #    yield QCEWLayoutColumns()

    def run(self):
        self.output().makedirs()
        shell('wget {url} -O {output}.zip'.format(
            url=self.URL.format(year=self.year), output=self.output().path))
        shell('unzip -o {output}.zip -d $(dirname {output})'.format(output=self.output().path))
        shell('mv $(dirname {output})/*.csv {output}'.format(output=self.output().path))

    def output(self):
        return LocalTarget(path=os.path.join(
            classpath(self), self.year, 'qcew.csv'))


class QCEW(Task):

    year = Parameter()

    def requires(self):
        yield DownloadQCEW(year=self.year)

    def tablename(self):
        return '"{schema}".qcew'.format(schema=classpath(self)) + self.year

    def columns(self):
        return '''
"area_fips" Text , --"5-character FIPS code"
"own_code" Text , --"1-character ownership code"
"industry_code" Text , --"6-character Industry Code (NAICS  SuperSector)"
"agglvl_code" Text , --"2-character aggregation level code"
"size_code" Text , --"1-character size code"
"year" Text , --"4-character year"
"qtr" Text , --"1-character quarter (always A for annual)"
"disclosure_code" Text , --"1-character disclosure code (either ' '(blank) or 'N' not disclosed)"
-- "area_title" Text , --"Multi-character area title associated with the area's FIPS (Excluded from singlefile)"
-- "own_title" Text , --"Multi-character ownership title associated with the ownership code (Excluded from singlefile)"
-- "industry_title" Text , --"Multi-character industry title associated with the industry code (Excluded from singlefile)"
-- "agglvl_title" Text , --"Multi-character aggregation title associated with the agglvl code (Excluded from singlefile)"
-- "size_title" Text , --"Multi-character size title associated with the size code (Excluded from singlefile)"
"qtrly_estabs" Numeric , --"Count of establishments for a given quarter"
"month1_emplvl" Numeric , --"Employment level for the first month of a given quarter"
"month2_emplvl" Numeric , --"Employment level for the second month of a given quarter"
"month3_emplvl" Numeric , --"Employment level for the third month of a  given quarter"
"total_qtrly_wages" Numeric , --"Total wages for a given quarter"
"taxable_qtrly_wages" Numeric , --"Taxable wages for a given quarter"
"qtrly_contributions" Numeric , --"Quarterly contributions for a given quarter"
"avg_wkly_wage" Numeric , --"Average weekly wage for a given quarter"
"lq_disclosure_code" Text                                    , --"1-character location-quotient disclosure code (either ' '(blank) or 'N' not disclosed"
"lq_qtrly_estabs" Numeric , --"Location quotient of the quarterly establishment count relative to the U.S. (Rounded to hundredths place)"
"lq_month1_emplvl" Numeric , --"Location quotient of the emloyment level for the first month of a given quarter relative to the U.S. (Rounded to hundredths place)"
"lq_month2_emplvl" Numeric , --"Location quotient of the emloyment level for the second month of a given quarter relative to the U.S. (Rounded to hundredths place)"
"lq_month3_emplvl" Numeric , --"Location quotient of the emloyment level for the third month of a given quarter relative to the U.S. (Rounded to hundredths place)"
"lq_total_qtrly_wages" Numeric , --"Location quotient of the total wages for a given quarter relative to the U.S. (Rounded to hundredths place)"
"lq_taxable_qtrly_wages" Numeric , --"Location quotient of the total taxable wages for a given quarter relative to the U.S. (Rounded to hundredths place)"
"lq_qtrly_contributions" Numeric , --"Location quotient of the total contributions for a given quarter relative to the U.S. (Rounded to hundredths place)"
"lq_avg_wkly_wage" Numeric , --"Location quotient of the average weekly wage for a given quarter relative to the U.S. (Rounded to hundredths place)"
"oty_disclosure_code" Text                                    , --"1-character over-the-year disclosure code (either ' '(blank) or 'N' not disclosed)"
"oty_qtrly_estabs_chg" Numeric , --"Over-the-year change in the count of establishments for a given quarter"
"oty_qtrly_estabs_pct_chg" Numeric , --"Over-the-year percent change in the count of establishments for a given quarter (Rounded to the tenths place)"
"oty_month1_emplvl_chg" Numeric , --"Over-the-year change in the first month's employment level of a given quarter"
"oty_month1_emplvl_pct_chg" Numeric , --"Over-the-year percent change in the first month's employment level of a given quarter (Rounded to the tenths place)"
"oty_month2_emplvl_chg" Numeric , --"Over-the-year change in the second month's employment level of a given quarter"
"oty_month2_emplvl_pct_chg" Numeric , --"Over-the-year percent change in the second month's employment level of a given quarter (Rounded to the tenths place)"
"oty_month3_emplvl_chg" Numeric , --"Over-the-year change in the third month's employment level of a given quarter"
"oty_month3_emplvl_pct_chg" Numeric , --"Over-the-year percent change in the third month's employment level of a given quarter (Rounded to the tenths place)"
"oty_total_qtrly_wages_chg" Numeric , --"Over-the-year change in total quarterly wages for a given quarter"
"oty_total_qtrly_wages_pct_chg" Numeric , --"Over-the-year percent change in total quarterly wages for a given quarter (Rounded to the tenths place)"
"oty_taxable_qtrly_wages_chg" Numeric , --"Over-the-year change in taxable quarterly wages for a given quarter"
"oty_taxable_qtrly_wages_pct_chg" Numeric , --"Over-the-year percent change in taxable quarterly wages for a given quarter (Rounded to the tenths place)"
"oty_qtrly_contributions_chg" Numeric , --"Over-the-year change in quarterly contributions for a given quarter"
"oty_qtrly_contributions_pct_chg" Numeric , --"Over-the-year percent change in quarterly contributions for a given quarter (Rounded to the tenths place)"
"oty_avg_wkly_wage_chg" Numeric , --"Over-the-year change in average weekly wage for a given quarter"
"oty_avg_wkly_wage_pct_chg" Numeric  --"Over-the-year percent change in average weekly wage for a given quarter (Rounded to the tenths place)"
    '''

    def run(self):
        cursor = pg_cursor()
        cursor.execute('CREATE SCHEMA IF NOT EXISTS "{schema}"'.format(
            schema=classpath(self)))
        cursor.execute('''
DROP TABLE IF EXISTS {tablename};
CREATE TABLE {tablename} (
    {columns}
)
                       '''.format(tablename=self.tablename(),
                                  columns=self.columns()))
        cursor.connection.commit()
        shell(r"psql -c '\copy {tablename} FROM {input} WITH CSV HEADER'".format(
            tablename=self.tablename(), input=self.input()[0].path))
        self.output().touch()

    def output(self):
        return DefaultPostgresTarget(self.tablename())
