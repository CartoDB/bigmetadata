#!/usr/bin/env python

'''
Bigmetadata tasks

tasks to download and create metadata
'''

#import requests
#import datetime
#import json
import csv
import json
import os
from sqlalchemy import Column, Numeric, Text
from luigi import Parameter, BooleanParameter, Task, WrapperTask, LocalTarget
from tasks.util import (LoadPostgresFromURL, classpath, pg_cursor, shell,
                        CartoDBTarget, get_logger, slug_column)
from tasks.us.census.tiger import load_sumlevels
from psycopg2 import ProgrammingError
from tasks.us.census.tiger import (SUMLEVELS, load_sumlevels, columns,
                                   ShorelineClipTiger)

from tasks.meta import (BMD, BMDColumn, BMDTag, BMDColumnToColumn, SessionTask,
                        BMDColumnTag, session_scope, BMDColumnTable)
from tasks.tags import tags

LOGGER = get_logger(__name__)


class Columns(BMD):
    total_pop = BMDColumn(
        id='B01001001',
        type='Numeric',
        name="Total Population",
        description='The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.',
        aggregate='sum',
        tags=[BMDColumnTag(tag=tags.denominator),
              BMDColumnTag(tag=tags.population)]
    )
    male_pop = BMDColumn(
        id='B01001002',
        type='Numeric',
        name="Male Population",
        description="The number of people within each geography who are male.",
        aggregate='sum',
        target_columns=[BMDColumnToColumn(target=total_pop, reltype='denominator')],
        tags=[BMDColumnTag(tag=tags.population)]
    )
    female_pop = BMDColumn(
        id='B01001026',
        type='Numeric',
        name="Female Population",
        description="The number of people within each geography who are female.",
        aggregate='sum',
        target_columns=[BMDColumnToColumn(target=total_pop, reltype='denominator')],
        tags=[BMDColumnTag(tag=tags.population)]
    )
    white_pop = BMDColumn(
        id='B03002003',
        type='Numeric',
        name="White Population",
        description="The number of people identifying as white, non-Hispanic in each geography.",
        aggregate='sum',
        target_columns=[BMDColumnToColumn(target=total_pop, reltype='denominator')],
        tags=[BMDColumnTag(tag=tags.population)]
    )
    black_pop = BMDColumn(
        id='B03002004',
        type='Numeric',
        name='Black or African American Population',
        description="The number of people identifying as black or African American, non-Hispanic in each geography.",
        aggregate='sum',
        target_columns=[BMDColumnToColumn(target=total_pop, reltype='denominator')],
        tags=[BMDColumnTag(tag=tags.population)]
    )
    asian_pop = BMDColumn(
        id='B03002006',
        type='Numeric',
        name='Asian Population',
        description="The number of people identifying as Asian, non-Hispanic in each geography.",
        aggregate='sum',
        target_columns=[BMDColumnToColumn(target=total_pop, reltype='denominator')],
        tags=[BMDColumnTag(tag=tags.population)]
    )
    hispanic_pop = BMDColumn(
        id='B03002012',
        type='Numeric',
        name='Asian Population',
        description="The number of people identifying as Hispanic or Latino in each geography.",
        aggregate='sum',
        target_columns=[BMDColumnToColumn(target=total_pop, reltype='denominator')],
        tags=[BMDColumnTag(tag=tags.population)]
    )


ACS_COLUMNS = Columns()

#
#HIGH_WEIGHT_TABLES = set([
#    'B01001',
#    'B01002',
#    'B03002',
#    'B05001',
#    #'B05011',
#    #'B07101',
#    'B08006',
#    'B08013',
#    #'B08101',
#    'B09001',
#    'B11001',
#    #'B11002',
#    #'B11012',
#    'B14001',
#    'B15003',
#    'B16001',
#    'B17001',
#    'B19013',
#    'B19083',
#    'B19301',
#    'B25001',
#    'B25002',
#    'B25003',
#    #'B25056',
#    'B25058',
#    'B25071',
#    'B25075',
#    'B25081',
#    #'B25114',
#])
#
#MEDIUM_WEIGHT_TABLES = set([
#    "B02001",
#    "B04001",
#    "B05002",
#    "B05012",
#    "B06011",
#    "B06012",
#    "B07001",
#    "B07204",
#    "B08011",
#    "B08012",
#    "B08103",
#    "B08134",
#    "B08136",
#    "B08301",
#    "B08303",
#    "B09002",
#    "B09005",
#    "B09008",
#    "B09010",
#    "B09018",
#    "B09019",
#    "B11005",
#    "B11006",
#    "B11007",
#    "B11011",
#    "B11014",
#    "B11016",
#    "B11017",
#    "B12001",
#    "B12002",
#    "B12007",
#    "B12501",
#    "B12503",
#    "B12504",
#    "B12505",
#    "B13002",
#    "B13016",
#    "B14002",
#    "B14003",
#    "B15001",
#    "B15002",
#    "B16002",
#    "B16006",
#    "B17015",
#    "B19001",
#    "B19013A",
#    "B19013B",
#    "B19013C",
#    "B19013D",
#    "B19013E",
#    "B19013F",
#    "B19013G",
#    "B19013H",
#    "B19013I",
#    "B19019",
#    "B19051",
#    "B19052",
#    "B19053",
#    "B19054",
#    "B19055",
#    "B19056",
#    "B19057",
#    "B19058",
#    "B19059",
#    "B19060",
#    "B19080",
#    "B19081",
#    "B19082",
#    "B19101",
#    "B19113",
#    "B19301A",
#    "B19301B",
#    "B19301C",
#    "B19301D",
#    "B19301E",
#    "B19301F",
#    "B19301G",
#    "B19301H",
#    "B19301I",
#    "B23001",
#    "B23006",
#    "B23020",
#    "B23025",
#    "B25003A",
#    "B25003B",
#    "B25003C",
#    "B25003D",
#    "B25003E",
#    "B25003F",
#    "B25003G",
#    "B25003H",
#    "B25003I",
#    "B25004",
#    "B25017",
#    "B25018",
#    "B25019",
#    "B25024",
#    "B25026",
#    "B25027",
#    "B25034",
#    "B25035",
#    "B25036",
#    "B25037",
#    "B25040",
#    "B25041",
#    "B25057",
#    "B25059",
#    "B25060",
#    "B25061",
#    "B25062",
#    "B25063",
#    "B25064",
#    "B25065",
#    "B25070",
#    "B25076",
#    "B25077",
#    "B25078",
#    "B25085",
#    "B25104",
#    "B25105",
#    "B27001",
#    "B27002",
#    "B27003",
#    "B27010",
#    "B27011",
#    "B27015",
#    "B27019",
#    "B27020",
#    "B27022",
#    "C02003",
#    "C15002A",
#    "C15010",
#    "C17002",
#    "C24010",
#    "C24020",
#    "C24030",
#    "C24040"
#])

# STEPS:
#
# 1. load ACS SQL into postgres
# 2. extract usable metadata from the imported tables, persist as json
#

class DownloadACS(LoadPostgresFromURL):

    # http://censusreporter.tumblr.com/post/73727555158/easier-access-to-acs-data
    url_template = 'https://s3.amazonaws.com/census-backup/acs/{year}/' \
            'acs{year}_{sample}/acs{year}_{sample}_backup.sql.gz'

    year = Parameter()
    sample = Parameter()

    @property
    def schema(self):
        return 'acs{year}_{sample}'.format(year=self.year, sample=self.sample)

    def identifier(self):
        return self.schema

    def run(self):
        cursor = pg_cursor()
        try:
            cursor.execute('CREATE ROLE census')
            cursor.connection.commit()
        except ProgrammingError:
            cursor.connection.rollback()
        try:
            cursor.execute('DROP SCHEMA {schema} CASCADE'.format(schema=self.schema))
            cursor.connection.commit()
        except ProgrammingError:
            cursor.connection.rollback()
        url = self.url_template.format(year=self.year, sample=self.sample)
        self.load_from_url(url)


class DumpACS(WrapperTask):
    #TODO
    '''
    Dump a table in postgres compressed format
    '''
    year = Parameter()
    sample = Parameter()

    def requires(self):
        pass



class AllACS(WrapperTask):

    force = BooleanParameter(default=False)

    def requires(self):
        for year in xrange(2010, 2014):
            for sample in ('1yr', '3yr', '5yr'):
                yield ProcessACS(year=year, sample=sample, force=self.force)


class Extract(SessionTask):
    '''
    Generate an extract of important ACS columns for CartoDB
    '''

    year = Parameter()
    sample = Parameter()
    geography = Parameter()
    force = BooleanParameter(default=False)
    clipped = BooleanParameter()

    def requires(self):
        if self.clipped:
            geography = load_sumlevels()[self.sumlevel]['table']
            return ShorelineClipTiger(year=self.year, geography=geography)

    def generate_columns(self):
        return [
            BMDColumnTable(colname='geoid',
                           column=getattr(TIGER_COLUMNS, self.resolution)),
            BMDColumnTable(colname='total_pop', column=ACS_COLUMNS.total_pop),
            BMDColumnTable(colname='female_pop', column=ACS_COLUMNS.female_pop),
            BMDColumnTable(colname='male_pop', column=ACS_COLUMNS.male_pop),
            BMDColumnTable(colname='white_pop', column=ACS_COLUMNS.white_pop),
            BMDColumnTable(colname='black_pop', column=ACS_COLUMNS.black_pop),
            BMDColumnTable(colname='asian_pop', column=ACS_COLUMNS.asian_pop),
            BMDColumnTable(colname='hispanic_pop', column=ACS_COLUMNS.hispanic_pop)
        ]

    def requires(self):
        return DownloadACS(year=self.year, sample=self.sample)

    def runsession(self, session):
        '''
        load relevant columns from underlying census tables
        '''
        sumlevel = '040' # TODO
        colids = []
        colnames = []
        tableids = set()
        inputschema = self.input().table
        for coltable in self.columns():
            session.add(coltable)
            session.add(coltable.column)
            colid = coltable.column.id.split('.')[-1]
            colids.append(colid)
            colnames.append(coltable.colname)
            tableids.add(colid[0:6])
        tableclause = '"{inputschema}".{inputtable} '.format(
            inputschema=inputschema, inputtable=tableids.pop())
        for tableid in tableids:
            tableclause += 'JOIN "{inputschema}".{inputtable} ' \
                           'USING (geoid)'.format(inputschema=inputschema,
                                                  inputtable=tableid)
        session.execute('INSERT INTO {output} ({colnames}) '
                        '  SELECT {colids} '
                        '  FROM {tableclause} '
                        '  WHERE geoid LIKE :sumlevelprefix '
                        ''.format(
                            output=self.output().table_id,
                            colnames=', '.join(colnames),
                            colids=', '.join(colids),
                            tableclause=tableclause
                        ), {
                            'sumlevelprefix': sumlevel + '00US%'
                        })


class ExtractAllACS(Task):
    force = BooleanParameter(default=False)
    year = Parameter()
    sample = Parameter()
    clipped = BooleanParameter()

    def requires(self):
        #for sumlevel in ('040', '050', '140', '150', '795', '860',):
        for sumlevel in ('state', 'county', 'census-tract', 'census-block', 'puma', 'zcta5',):
            yield ExtractACS(sumlevel=sumlevel, year=self.year,
                             clipped=self.clipped,
                             sample=self.sample, force=self.force)
