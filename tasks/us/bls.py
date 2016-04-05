#!/usr/bin/env python

'''
Bigmetadata tasks

tasks to download and create metadata
'''

import os
import requests

from luigi import Task, Parameter, LocalTarget, BooleanParameter
from tasks.util import (TableTarget, shell, classpath, pg_cursor, slug_column,
                        CartoDBTarget, sql_to_cartodb_table, session_scope,
                        SessionTask)
from tasks.us.census.tiger import ProcessTiger
from psycopg2 import ProgrammingError

#from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Text, Numeric, Column, Table, Integer

naics_industry_code = lambda: Column("industry_code", Text, info={
    'description': "6-character Industry Code (NAICS SuperSector),"
})

class NAICS(SessionTask):

    URL = 'http://www.bls.gov/cew/doc/titles/industry/industry_titles.csv'

    def columns(self):
        return [
            naics_industry_code(),
            Column('industry_title', Text, info={'description': 'Title of NAICS industry'}),
        ]

    def runsession(self, session):
        shell("curl '{url}' | psql -c 'COPY {output} FROM STDIN WITH CSV HEADER'".format(
            output=self.output(),
            url=self.URL
        ))


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
                'target': True
            }),
            Column("own_code", Text, info={
                'description': "1-character ownership code: http://www.bls.gov/cew/doc/titles/ownership/ownership_titles.htm" # 5 for private
            }),
            naics_industry_code(),
            Column("agglvl_code", Text, info={
                'description': "2-character aggregation level code: http://www.bls.gov/cew/doc/titles/agglevel/agglevel_titles.htm"
            }),
            Column("size_code", Text, info={
                'description': "1-character size code: http://www.bls.gov/cew/doc/titles/size/size_titles.htm"
            }),
            Column("year", Text, info={
                'description': "4-character year"
            }),
            Column("qtr", Text, info={
                'description': "1-character quarter (always A for annual)"
            }),
            Column("disclosure_code", Text, info={
                'description': "1-character disclosure code (either ' '(blank), or 'N' not disclosed)"
            }),
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


class SimpleQCEW(Task):
    '''
    Isolate the rows of QCEW we actually care about without significantly
    modifying the schema.  Brings us down to one quarter.

    We pull out private employment at the county level, divided by three-digit
    NAICS code and supercategory (four-digits, but simpler).

    agglvl 75: 3-digit by ownership
    agglvl 73: superlevel by ownership
    '''
    year = Parameter()
    qtr = Parameter()

    def columns(self):
        return RawQCEW(self.year).columns()

    def requires(self):
        return RawQCEW(self.year)

    def run(self):
        cursor = pg_cursor()
        cursor.execute('CREATE TABLE {output} AS '
                       'SELECT * FROM {qcew} '
                       "WHERE agglvl_code IN ('75', '73')"
                       "      AND year = '{year}'"
                       "      AND qtr = '{qtr}'"
                       "      AND own_code = '5'".format(
                           qtr=self.qtr,
                           year=self.year,
                           output=self.output(),
                           qcew=self.input()))
        cursor.connection.commit()

    def output(self):
        return TableTarget(self, self.columns())


class QCEW(SessionTask):
    '''
    Turn QCEW data into a columnar format that works better for upload
    '''

    year = Parameter()
    qtr = Parameter()

    def requires(self):
        return {
            'qcew': SimpleQCEW(year=self.year, qtr=self.qtr),
            'naics': NAICS()
        }

    def columns(self):
        '''
        Define a limited set of columns for the export
        '''
        yield Column('area_fips', Text)
        dimensions = ('avg_wkly_wage', 'qtrly_estabs', 'month3_emplvl',
                      'lq_avg_wkly_wage', 'lq_qtrly_estabs', 'lq_month3_emplvl')
        naics = self.input()['naics']
        qcew = self.input()['qcew']
        code_to_name = dict([(code, category) for code, category in naics.select().execute()])
        cursor = pg_cursor()
        # TODO implement shared column on industry_code
        cursor.execute('SELECT DISTINCT {code} FROM {qcew} ORDER BY {code} ASC'.format(
            code=naics_industry_code().name, qcew=qcew))
        for code, in cursor:
            name = code_to_name[code]
            for dim in dimensions:
                column = Column(dim + '_' + slug_column(name), Integer, info={
                    'code': code,
                    'dimension': dim,
                    'description': '{dim} for {name}'.format(
                        dim=qcew.table.columns[dim].info['description'],
                        name=name
                    )
                })
                yield column

    def runsession(self, session):
        session.execute('INSERT INTO {output} (area_fips) '
                        'SELECT distinct area_fips FROM {qcew} '.format(
                            output=self.output(),
                            qcew=self.input()['qcew']
                        ))
        for col in self.output().table.columns:
            if 'code' not in col.info:
                continue
            query = ('UPDATE {output} SET {column} = {dim} '
                     'FROM {qcew} '
                     'WHERE {industry_code} = \'{code}\' AND '
                     '{qcew}.area_fips = {output}.area_fips'.format(
                         code=col.info['code'],
                         dim=col.info['dimension'],
                         output=self.output(),
                         column=col.name,
                         qcew=self.input()['qcew'],
                         industry_code=naics_industry_code().name
                     ), )[0]
            session.execute(query)
