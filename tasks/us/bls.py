#!/usr/bin/env python

'''
Bigmetadata tasks

tasks to download and create metadata
'''

import os
import requests

from luigi import Task, Parameter, LocalTarget, BooleanParameter
from tasks.util import (TableTarget, shell, classpath, pg_cursor)
from psycopg2 import ProgrammingError

#from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Text, Numeric, Column, Table, Integer


class NAICS(Task):

    URL = 'http://www.bls.gov/cew/doc/titles/industry/industry_titles.csv'
    force = BooleanParameter(default=False)

    def columns(self):
        return [
            Column('industry_code', Text, info={'description': 'NAICS Industry Code'}),
            Column('industry_title', Text, info={'description': 'Title of NAICS industry'}),
        ]

    def run(self):
        self.output().create()
        try:
            shell("curl '{url}' | psql -c 'COPY {output} FROM STDIN WITH CSV HEADER'".format(
                output=self.output(),
                url=self.URL
            ))
        except:
            self.output().remove()
            raise

    def output(self):
        target = TableTarget(self, self.columns())
        if self.force:
            target.drop(checkfirst=True)
            self.force = False
        return target


class DownloadQCEW(Task):

    year = Parameter()
    URL = 'http://www.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip'

    def run(self):
        self.output().makedirs()
        shell('wget \'{url}\' -O {output}.zip'.format(
            url=self.URL.format(year=self.year), output=self.output().path))
        shell('unzip -o {output}.zip -d $(dirname {output})'.format(output=self.output().path))
        shell('mv $(dirname {output})/*.csv {output}'.format(output=self.output().path))

    def output(self):
        return LocalTarget(path=os.path.join(
            classpath(self), self.year, 'qcew.csv'))


class RawQCEW(Task):

    year = Parameter()
    force = BooleanParameter(default=False)

    def columns(self):
        return [
            Column("area_fips", Text, info={
                'description': "5-character FIPS code (State-county)",
                'is_geoid': True
            }),
            Column("own_code", Text, info={
                'description': "1-character ownership code"
            }),
            Column("industry_code", Text, info={
                'description': "6-character Industry Code (NAICS  SuperSector),"
            }),
            Column("agglvl_code", Text, info={
                'description': "2-character aggregation level code"
            }),
            Column("size_code", Text, info={
                'description': "1-character size code"
            }),
            Column("year", Text, info={
                'description': "4-character year"
            }),
            Column("qtr", Text, info={
                'description': "1-character quarter (always A for annual),"
            }),
            Column("disclosure_code", Text, info={
                'description': "1-character disclosure code (either ' '(blank), or 'N' not disclosed),"
            }),
            # area_title = Column(Text, info={ 'description': "Multi-character area title associated with the area's FIPS (Excluded from singlefile)," }),
            # own_title = Column(Text, info={ 'description': "Multi-character ownership title associated with the ownership code (Excluded from singlefile)," }),
            # industry_title = Column(Text, info={ 'description': "Multi-character industry title associated with the industry code (Excluded from singlefile)," }),
            # agglvl_title = Column(Text, info={ 'description': "Multi-character aggregation title associated with the agglvl code (Excluded from singlefile)," }),
            # size_title = Column(Text, info={ 'description': "Multi-character size title associated with the size code (Excluded from singlefile)," }),
            Column("qtrly_estabs", Numeric, info={
                'description': "Count of establishments for a given quarter"
            }),
            Column("month1_emplvl", Numeric, info={
                'description': "Employment level for the first month of a given quarter"
            }),
            Column("month2_emplvl", Numeric, info={
                'description': "Employment level for the second month of a given quarter"
            }),
            Column("month3_emplvl", Numeric, info={
                'description': "Employment level for the third month of a  given quarter"
            }),
            Column("total_qtrly_wages", Numeric, info={
                'description': "Total wages for a given quarter"
            }),
            Column("taxable_qtrly_wages", Numeric, info={
                'description': "Taxable wages for a given quarter"
            }),
            Column("qtrly_contributions", Numeric, info={
                'description': "Quarterly contributions for a given quarter"
            }),
            Column("avg_wkly_wage", Numeric, info={
                'description': "Average weekly wage for a given quarter"
            }),
            Column("lq_disclosure_code", Text, info={
                'description': "1-character location-quotient disclosure code (either ' '(blank), or 'N' not disclosed"
            }),
            Column("lq_qtrly_estabs", Numeric, info={
                'description': "Location quotient of the quarterly establishment count relative to the U.S. (Rounded to hundredths place),"
            }),
            Column("lq_month1_emplvl", Numeric, info={
                'description': "Location quotient of the emloyment level for the first month of a given quarter relative to the U.S. (Rounded to hundredths place),"
            }),
            Column("lq_month2_emplvl", Numeric, info={
                'description': "Location quotient of the emloyment level for the second month of a given quarter relative to the U.S. (Rounded to hundredths place),"
            }),
            Column("lq_month3_emplvl", Numeric, info={
                'description': "Location quotient of the emloyment level for the third month of a given quarter relative to the U.S. (Rounded to hundredths place),"
            }),
            Column("lq_total_qtrly_wages", Numeric, info={
                'description': "Location quotient of the total wages for a given quarter relative to the U.S. (Rounded to hundredths place),"
            }),
            Column("lq_taxable_qtrly_wages", Numeric, info={
                'description': "Location quotient of the total taxable wages for a given quarter relative to the U.S. (Rounded to hundredths place),"
            }),
            Column("lq_qtrly_contributions", Numeric, info={
                'description': "Location quotient of the total contributions for a given quarter relative to the U.S. (Rounded to hundredths place),"
            }),
            Column("lq_avg_wkly_wage", Numeric, info={
                'description': "Location quotient of the average weekly wage for a given quarter relative to the U.S. (Rounded to hundredths place),"
            }),
            Column("oty_disclosure_code", Text, info={
                'description': "1-character over-the-year disclosure code (either ' '(blank), or 'N' not disclosed),"
            }),
            Column("oty_qtrly_estabs_chg", Numeric, info={
                'description': "Over-the-year change in the count of establishments for a given quarter"
            }),
            Column("oty_qtrly_estabs_pct_chg", Numeric, info={
                'description': "Over-the-year percent change in the count of establishments for a given quarter (Rounded to the tenths place),"
            }),
            Column("oty_month1_emplvl_chg", Numeric, info={
                'description': "Over-the-year change in the first month's employment level of a given quarter"
            }),
            Column("oty_month1_emplvl_pct_chg", Numeric, info={
                'description': "Over-the-year percent change in the first month's employment level of a given quarter (Rounded to the tenths place),"
            }),
            Column("oty_month2_emplvl_chg", Numeric, info={
                'description': "Over-the-year change in the second month's employment level of a given quarter"
            }),
            Column("oty_month2_emplvl_pct_chg", Numeric, info={
                'description': "Over-the-year percent change in the second month's employment level of a given quarter (Rounded to the tenths place),"
            }),
            Column("oty_month3_emplvl_chg", Numeric, info={
                'description': "Over-the-year change in the third month's employment level of a given quarter"
            }),
            Column("oty_month3_emplvl_pct_chg", Numeric, info={
                'description': "Over-the-year percent change in the third month's employment level of a given quarter (Rounded to the tenths place),"
            }),
            Column("oty_total_qtrly_wages_chg", Numeric, info={
                'description': "Over-the-year change in total quarterly wages for a given quarter"
            }),
            Column("oty_total_qtrly_wages_pct_chg", Numeric, info={
                'description': "Over-the-year percent change in total quarterly wages for a given quarter (Rounded to the tenths place),"
            }),
            Column("oty_taxable_qtrly_wages_chg", Numeric, info={
                'description': "Over-the-year change in taxable quarterly wages for a given quarter"
            }),
            Column("oty_taxable_qtrly_wages_pct_chg", Numeric, info={
                'description': "Over-the-year percent change in taxable quarterly wages for a given quarter (Rounded to the tenths place),"
            }),
            Column("oty_qtrly_contributions_chg", Numeric, info={
                'description': "Over-the-year change in quarterly contributions for a given quarter"
            }),
            Column("oty_qtrly_contributions_pct_chg", Numeric, info={
                'description': "Over-the-year percent change in quarterly contributions for a given quarter (Rounded to the tenths place),"
            }),
            Column("oty_avg_wkly_wage_chg", Numeric, info={
                'description': "Over-the-year change in average weekly wage for a given quarter"
            }),
            Column("oty_avg_wkly_wage_pct_chg", Numeric, info={
                'description': "Over-the-year percent change in average weekly wage for a given quarter (Rounded to the tenths place)"
            })
        ]

    def requires(self):
        yield DownloadQCEW(year=self.year)

    def run(self):
        self.output().create()
        try:
            shell("psql -c '\\copy {table} FROM {input} WITH CSV HEADER'".format(
                table=self.output(), input=self.input()[0].path))
        except:
            self.output().drop()
            raise

    def output(self):
        target = TableTarget(self, self.columns())
        if self.force:
            target.drop(checkfirst=True)
            self.force = False
        return target


class QCEW(Task):
    '''
    Turn QCEW data into a columnar format that works better for upload
    '''

    year = Parameter()

    def requires(self):
        return {
            'qcew': RawQCEW(year=self.year),
            'naics': NAICS()
        }

    def columns(self):
        cursor = pg_cursor()
        #for 

    def run(self):
        pass

    def output(self):
        return TableTarget(self, self.columns())
