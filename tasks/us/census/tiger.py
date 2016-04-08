#!/usr/bin/env python

'''
Tiger
'''

import json
import os
import subprocess
from collections import OrderedDict
from tasks.util import (LoadPostgresFromURL, classpath, pg_cursor,
                        DefaultPostgresTarget, CartoDBTarget,
                        sql_to_cartodb_table, grouper, shell,
                        underscore_slugify, TableTask, ColumnTarget,
                        ColumnsTask
                       )
from tasks.meta import (OBSColumnTable, OBSColumn, current_session,
                        OBSColumnTag, OBSColumnToColumn)
from tasks.tags import CategoryTags

from luigi import Task, WrapperTask, Parameter, LocalTarget, BooleanParameter
from psycopg2 import ProgrammingError


class GeomColumns(ColumnsTask):

    def version(self):
        return '1'

    def requires(self):
        return {
            'tags': CategoryTags(),
        }

    def columns(self):
        tags = self.input()['tags']
        return {
            'block_group': OBSColumn(
                type='Geometry',
                name='US Census Block Groups',
                description="Block groups (BGs) are statistical divisions of census tracts, are generally defined to contain between 600 and 3,000 people, and are used to present data and control block numbering. A block group consists of clusters of blocks within the same census tract that have the same first digit of their four-digit census block number. For example, blocks 3001, 3002, 3003, ..., 3999 in census tract 1210.02 belong to BG 3 in that census tract. Most BGs were delineated by local participants in the Census Bureau\u2019s Participant Statistical Areas Program. The Census Bureau delineated BGs only where a local or tribal government declined to participate, and a regional organization or State Data Center was not available to participate.\r\n\r\nA BG usually covers a contiguous area. Each census tract contains at least one BG, and BGs are uniquely numbered within the census tract. Within the standard census geographic hierarchy, BGs never cross state, county, or census tract boundaries but may cross the boundaries of any other geographic entity. Tribal census tracts and tribal BGs are separate and unique geographic areas defined within federally recognized American Indian reservations and can cross state and county boundaries (see \u201cTribal Census Tract\u201d and \u201cTribal Block Group\u201d). The tribal census tracts and tribal block groups may be completely different from the census tracts and block groups defined by state and county.",
                weight=10,
                tags=[tags['boundary']]
            ),
            'block': OBSColumn(
                type='Geometry',
                name='US Census Blocks',
                description="Census blocks are numbered uniquely with a four-digit census block number from 0000 to 9999 within census tract, which nest within state and county. The first digit of the census block number identifies the block group. Block numbers beginning with a zero (in Block Group 0) are only associated with water-only areas.",
                weight=3,
                tags=[tags['boundary']]
            ),
            'census_tract': OBSColumn(
                type='Geometry',
                name='US Census Tracts',
                description="Census tracts are identified by an up to four-digit integer number and may have an optional two-digit suffix; for example 1457.02 or 23. The census tract codes consist of six digits with an implied decimal between the fourth and fifth digit corresponding to the basic census tract number but with leading zeroes and trailing zeroes for census tracts without a suffix. The tract number examples above would have codes of 145702 and 002300, respectively.\r\n\r\nSome ranges of census tract numbers in the 2010 Census are used to identify distinctive types of census tracts. The code range in the 9400s is used for those census tracts with a majority of population, housing, or land area associated with an American Indian area and matches the numbering used in Census 2000. The code range in the 9800s is new for 2010 and is used to specifically identify special land-use census tracts; that is, census tracts defined to encompass a large area with little or no residential population with special characteristics, such as large parks or employment areas. The range of census tracts in the 9900s represents census tracts delineated specifically to cover large bodies of water. This is different from Census 2000 when water-only census tracts were assigned codes of all zeroes (000000); 000000 is no longer used as a census tract code for the 2010 Census.\r\n\r\nThe Census Bureau uses suffixes to help identify census tract changes for comparison purposes. Census tract suffixes may range from .01 to .98. As part of local review of existing census tracts before each census, some census tracts may have grown enough in population size to qualify as more than one census tract. When a census tract is split, the split parts usually retain the basic number but receive different suffixes. For example, if census tract 14 is split, the new tract numbers would be 14.01 and 14.02. In a few counties, local participants request major changes to, and renumbering of, the census tracts; however, this is generally discouraged. Changes to individual census tract boundaries usually do not result in census tract numbering changes.\r\n\r\nThe Census Bureau introduced the concept of tribal census tracts for the first time for Census 2000. Tribal census tracts for that census consisted of the standard county-based census tracts tabulated within American Indian areas, thus allowing for the tracts to ignore state and county boundaries for tabulation. The Census Bureau assigned the 9400 range of numbers to identify specific tribal census tracts; however, not all tribal census tracts used this numbering scheme. For the 2010 Census, tribal census tracts no longer are tied to or numbered in the same way as the county-based census tracts (see \u201cTribal Census Tract\u201d).",
                weight=9,
                tags=[tags['boundary']]
            ),
            'congressional_district': OBSColumn(
                type='Geometry',
                name='US Congressional Districts',
                description="Congressional districts are identified by a two-character numeric Federal Information Processing Series (FIPS) code numbered uniquely within the state. The District of Columbia, Puerto Rico, and the Island Areas have code 98 assigned identifying their nonvoting delegate status with respect to representation in Congress:\r\n\r\n01 to 53: Congressional district codes\r\n00: At large (single district for state)\r\n98: Nonvoting delegate",
                weight=5,
                tags=[tags['boundary']]
            ),
            'county': OBSColumn(
                type='Geometry',
                name='US County',
                description="The primary legal divisions of most states are termed counties. In Louisiana, these divisions are known as parishes. In Alaska, which has no counties, the equivalent entities are the organized boroughs, city and boroughs, municipalities, and census areas; the latter of which are delineated cooperatively for statistical purposes by the state of Alaska and the Census Bureau. In four states (Maryland, Missouri, Nevada, and Virginia), there are one or more incorporated places that are independent of any county organization and thus constitute primary divisions of their states. These incorporated places are known as independent cities and are treated as equivalent entities for purposes of data presentation. The District of Columbia and Guam have no primary divisions, and each area is considered an equivalent entity for purposes of data presentation. All of the counties in Connecticut and Rhode Island and nine counties in Massachusetts were dissolved as functioning governmental entities; however, the Census Bureau continues to present data for these historical entities in order to provide comparable geographic units at the county level of the geographic hierarchy for these states and represents them as nonfunctioning legal entities in data products. The Census Bureau treats the following entities as equivalents of counties for purposes of data presentation: municipios in Puerto Rico, districts and islands in American Samoa, municipalities in the Commonwealth of the Northern Mariana Islands, and islands in the U.S. Virgin Islands. Each county or statistically equivalent entity is assigned a three-character numeric Federal Information Processing Series (FIPS) code based on alphabetical sequence that is unique within state and an eight-digit National Standard feature identifier.",
                weight=7,
                tags=[tags['boundary']]
            ),
            'puma': OBSColumn(
                type='Geometry',
                name='US Census Public Use Microdata Areas',
                description="PUMAs are geographic areas for which the Census Bureau provides selected extracts of raw data from a small sample of census records that are screened to protect confidentiality. These extracts are referred to as public use microdata sample (PUMS) files.\r\n\r\nFor the 2010 Census, each state, the District of Columbia, Puerto Rico, and some Island Area participants delineated PUMAs for use in presenting PUMS data based on a 5 percent sample of decennial census or American Community Survey data. These areas are required to contain at least 100,000 people. This is different from Census 2000 when two types of PUMAs were defined: a 5 percent PUMA as for 2010 and an additional super-PUMA designed to provide a 1 percent sample. The PUMAs are identified by a five-digit census code unique within state.",
                weight=6,
                tags=[tags['boundary']]
            ),
            'state': OBSColumn(
                type='Geometry',
                name='US States',
                description="States and Equivalent Entities are the primary governmental divisions of the United States. In addition to the 50 states, the Census Bureau treats the District of Columbia, Puerto Rico, American Samoa, the Commonwealth of the Northern Mariana Islands, Guam, and the U.S. Virgin Islands as the statistical equivalents of states for the purpose of data presentation.",
                weight=8,
                tags=[tags['boundary']]
            ),
            'zcta5': OBSColumn(
                type='Geometry',
                name='US Census Zip Code Tabulation Areas',
                description="ZCTAs are approximate area representations of U.S. Postal Service (USPS) five-digit ZIP Code service areas that the Census Bureau creates using whole blocks to present statistical data from censuses and surveys. The Census Bureau defines ZCTAs by allocating each block that contains addresses to a single ZCTA, usually to the ZCTA that reflects the most frequently occurring ZIP Code for the addresses within that tabulation block. Blocks that do not contain addresses but are completely surrounded by a single ZCTA (enclaves) are assigned to the surrounding ZCTA; those surrounded by multiple ZCTAs will be added to a single ZCTA based on limited buffering performed between multiple ZCTAs. The Census Bureau identifies five-digit ZCTAs using a five-character numeric code that represents the most frequently occurring USPS ZIP Code within that ZCTA, and this code may contain leading zeros.\r\n\r\nThere are significant changes to the 2010 ZCTA delineation from that used in 2000. Coverage was extended to include the Island Areas for 2010 so that the United States, Puerto Rico, and the Island Areas have ZCTAs. Unlike 2000, when areas that could not be assigned to a ZCTA were given a generic code ending in \u201cXX\u201d (land area) or \u201cHH\u201d (water area), for 2010 there is no universal coverage by ZCTAs, and only legitimate five-digit areas are defined. The 2010 ZCTAs will better represent the actual Zip Code service areas because the Census Bureau initiated a process before creation of 2010 blocks to add block boundaries that split polygons with large numbers of addresses using different Zip Codes.\r\n\r\nData users should not use ZCTAs to identify the official USPS ZIP Code for mail delivery. The USPS makes periodic changes to ZIP Codes to support more efficient mail delivery. The ZCTAs process used primarily residential addresses and was biased towards Zip Codes used for city-style mail delivery, thus there may be Zip Codes that are primarily nonresidential or boxes only that may not have a corresponding ZCTA.",
                weight=6,
                tags=[tags['boundary']]
            )
        }


class GeoidColumns(ColumnsTask):

    def version(self):
        return '1'

    def requires(self):
        return GeomColumns()

    def columns(self):
        geoms = self.input()
        return {
            'block_group_geoid': OBSColumn(
                type='Text',
                name='US Census Block Group Geoids',
                weight=0,
                targets={
                    geoms['block_group']: 'geom_ref'
                }
            ),
            'block_geoid': OBSColumn(
                type='Text',
                name='US Census Block Geoids',
                weight=0,
                targets={
                    geoms['block']: 'geom_ref'
                }
            ),
            'census_tract_geoid': OBSColumn(
                type='Text',
                name='US Census Tract Geoids',
                description="",
                weight=0,
                targets={
                    geoms['census_tract']: 'geom_ref'
                }
            ),
            'congressional_district_geoid': OBSColumn(
                type='Text',
                name='US Congressional District Geoids',
                description="",
                weight=0,
                targets={
                    geoms['congressional_district']: 'geom_ref'
                }
            ),
            'county_geoid': OBSColumn(
                type='Text',
                name='US County Geoids',
                description="",
                weight=0,
                targets={
                    geoms['county']: 'geom_ref'
                }
            ),
            'puma_geoid': OBSColumn(
                type='Text',
                name='US Census Public Use Microdata Area Geoids',
                description="",
                weight=0,
                targets={
                    geoms['puma']: 'geom_ref'
                }
            ),
            'state_geoid': OBSColumn(
                type='Text',
                name='US State Geoids',
                description="",
                weight=0,
                targets={
                    geoms['state']: 'geom_ref'
                }
            ),
            'zcta5_geoid': OBSColumn(
                type='Text',
                name='US Census Zip Code Tabulation Area Geoids',
                description="",
                weight=0,
                targets={
                    geoms['zcta5']: 'geom_ref'
                }
            )
        }


class DownloadTigerGeography(Task):

    year = Parameter()
    geography = Parameter()
    force = BooleanParameter() # TODO

    url_format = 'ftp://ftp2.census.gov/geo/tiger/TIGER{year}/{geography}/'

    @property
    def url(self):
        return self.url_format.format(year=self.year, geography=self.geography)

    @property
    def directory(self):
        return os.path.join('tmp', classpath(self), self.year)

    def run(self):
        subprocess.check_call('wget --recursive --continue --accept=*.zip '
                              '--no-parent --cut-dirs=3 --no-host-directories '
                              '--directory-prefix={directory} '
                              '{url}'.format(directory=self.directory, url=self.url), shell=True)

    def output(self):
        filenames = subprocess.check_output('wget --recursive --accept=*.zip --reject *.zip '
                                            '--no-parent --cut-dirs=3 --no-host-directories '
                                            '{url} 2>&1 | grep Rejecting'.format(url=self.url), shell=True)
        for fname in filenames.split('\n'):
            if not fname:
                continue
            path = os.path.join(self.directory, self.geography,
                                fname.replace("Rejecting '", '').replace("'.", ''))
            yield LocalTarget(path)


class UnzipTigerGeography(Task):
    '''
    Unzip tiger geography
    '''

    year = Parameter()
    geography = Parameter()

    def requires(self):
        return DownloadTigerGeography(year=self.year, geography=self.geography)

    def run(self):
        for infile in self.input():
            subprocess.check_call("unzip -n -q -d $(dirname {zippath}) '{zippath}'".format(
                zippath=infile.path), shell=True)

    def output(self):
        for infile in self.input():
            yield LocalTarget(infile.path.replace('.zip', '.shp'))


class TigerGeographyShapefileToSQL(Task):
    '''
    Take downloaded shapefiles and turn them into a SQL dump.
    '''

    year = Parameter()
    geography = Parameter()
    force = BooleanParameter()

    @property
    def table(self):
        return str(self.geography).lower()

    @property
    def schema(self):
        return classpath(self)

    @property
    def qualified_table(self):
        return '"{schema}"."{table}{year}"'.format(schema=self.schema,
                                                   table=self.table, year=self.year)

    def complete(self):
        if self.force is True:
            return False
        return super(TigerGeographyShapefileToSQL, self).complete()

    def requires(self):
        return UnzipTigerGeography(year=self.year, geography=self.geography)

    def run(self):
        cursor = pg_cursor()
        cursor.execute('CREATE SCHEMA IF NOT EXISTS "{schema}"'.format(schema=self.schema))
        cursor.execute('DROP TABLE IF EXISTS {qualified_table}'.format(
            qualified_table=self.qualified_table))
        cursor.connection.commit()

        shapefiles = self.input()
        cmd = 'PG_USE_COPY=yes PGCLIENTENCODING=latin1 ' \
                'ogr2ogr -f PostgreSQL PG:dbname=$PGDATABASE ' \
                '-t_srs "EPSG:4326" -nlt MultiPolygon -nln {qualified_table} ' \
                '{shpfile_path} '.format(
                    qualified_table=self.qualified_table,
                    shpfile_path=shapefiles.next().path)
        shell(cmd)

        # chunk into 500 shapefiles at a time.
        for shape_group in grouper(shapefiles, 500):
            subprocess.check_call(
                'export PG_USE_COPY=yes PGCLIENTENCODING=latin1; '
                'echo \'{shapefiles}\' | xargs -P 16 -I shpfile_path '
                'ogr2ogr -f PostgreSQL PG:dbname=$PGDATABASE -append '
                '-t_srs "EPSG:4326" -nlt MultiPolygon -nln {qualified_table} '
                'shpfile_path '.format(
                    shapefiles='\n'.join([shp.path for shp in shape_group if shp]),
                    qualified_table=self.qualified_table),
                shell=True)

        # Spatial index
        cursor.execute('ALTER TABLE {qualified_table} RENAME COLUMN '
                       'wkb_geometry TO geom'.format(qualified_table=self.qualified_table))
        cursor.execute('CREATE INDEX ON {qualified_table} USING GIST (geom)'.format(
            qualified_table=self.qualified_table))
        cursor.connection.commit()
        self.output().touch()
        self.force = False

    def output(self):
        return DefaultPostgresTarget(table=self.qualified_table,
                                     update_id=self.qualified_table)


class DownloadTiger(LoadPostgresFromURL):

    url_template = 'https://s3.amazonaws.com/census-backup/tiger/{year}/tiger{year}_backup.sql.gz'
    year = Parameter()

    @property
    def schema(self):
        return 'tiger{year}'.format(year=self.year)

    def identifier(self):
        return self.schema

    def run(self):
        cursor = pg_cursor()
        try:
            cursor.execute('DROP SCHEMA {schema} CASCADE'.format(schema=self.schema))
            cursor.connection.commit()
        except ProgrammingError:
            cursor.connection.rollback()
        url = self.url_template.format(year=self.year)
        self.load_from_url(url)
        self.output().touch()


class SimpleShoreline(Task):

    force = BooleanParameter(default=False)
    year = Parameter()

    def requires(self):
        return TigerGeographyShapefileToSQL(geography='AREAWATER', year=self.year)

    def run(self):
        cursor = pg_cursor()
        cursor.execute('DROP TABLE IF EXISTS {output}'.format(
            output=self.output().table))
        cursor.execute('CREATE TABLE {output} AS '
                       'SELECT ST_Subdivide(geom) geom FROM {input} '
                       "WHERE mtfcc != 'H2030' OR awater > 3000000".format(
                           input=self.input().table,
                           output=self.output().table
                       ))
        cursor.execute('CREATE INDEX ON {output} USING GIST (geom)'.format(
            output=self.output().table
        ))
        cursor.connection.commit()
        self.output().touch()

    def output(self):
        output = DefaultPostgresTarget(
            table='"' + classpath(self) +'".simple_shoreline' + str(self.year))
        if self.force:
            output.untouch()
            self.force = False
        return output


class ShorelineClipTiger(Task):
    '''
    Clip the provided geography to shoreline.
    '''

    # MTFCC meanings:
    # http://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp2009/TGRSHP09AF.pdf

    year = Parameter()
    geography = Parameter()
    force = BooleanParameter()

    def requires(self):
        return {
            'tiger': SumLevel(year=self.year, geography=self.geography),
            'water': SimpleShoreline(year=self.year)
        }

    def run(self):
        cursor = pg_cursor()
        #tiger = [t for t in self.input()['tiger'] if t.data['slug'] == self.geography.lower()][0]
        pos = 'tiger{}.{}'.format(self.year, self.geography)
        neg = self.input()['water'].table
        if self.geography in ('puma', 'zcta5'):
            geoid = 'geoid10'
        else:
            geoid = 'geoid'
        pos_split = pos.split('.')[-1] + '_split'
        pos_neg_joined = pos_split + '_' + neg.split('.')[-1] + '_joined'
        pos_neg_joined_diffed = pos_neg_joined + '_diffed'
        pos_neg_joined_diffed_merged = pos_neg_joined_diffed + '_merged'
        output = self.output().table

        cursor.execute('CREATE SCHEMA IF NOT EXISTS "{schema}"'.format(
            schema=classpath(self)))

        # Split the positive table into geoms with a reasonable number of
        # vertices.
        cursor.execute('DROP TABLE IF EXISTS {pos_split}'.format(
            pos_split=pos_split))
        cursor.execute('CREATE TEMPORARY TABLE {pos_split} '
                       '(id serial primary key, {geoid} text, geom geometry)'.format(
                           geoid=geoid, pos_split=pos_split))
        cursor.execute('INSERT INTO {pos_split} ({geoid}, geom) '
                       'SELECT {geoid}, ST_Subdivide(geom) geom '
                       'FROM {pos}'.format(pos=pos, pos_split=pos_split, geoid=geoid))

        cursor.execute('CREATE INDEX ON {pos_split} USING GIST (geom)'.format(
            pos_split=pos_split))

        # Join the split up pos to the split up neg, then union the geoms based
        # off the split pos id (technically the union on pos geom is extraneous)
        cursor.execute('DROP TABLE IF EXISTS {pos_neg_joined}'.format(
            pos_neg_joined=pos_neg_joined))
        cursor.execute('CREATE TEMPORARY TABLE {pos_neg_joined} AS '
                       'SELECT id, {geoid}, ST_Union(neg.geom) neg_geom, '
                       '       ST_Union(pos.geom) pos_geom '
                       'FROM {pos_split} pos, {neg} neg '
                       'WHERE ST_Intersects(pos.geom, neg.geom) '
                       'GROUP BY id'.format(geoid=geoid, neg=neg,
                                            pos_split=pos_split,
                                            pos_neg_joined=pos_neg_joined))

        # Calculate the difference between the pos and neg geoms
        cursor.execute('DROP TABLE IF EXISTS {pos_neg_joined_diffed}'.format(
            pos_neg_joined_diffed=pos_neg_joined_diffed))
        cursor.execute('CREATE TEMPORARY TABLE {pos_neg_joined_diffed} '
                       'AS SELECT {geoid}, id, ST_Difference( '
                       'ST_MakeValid(pos_geom), ST_MakeValid(neg_geom)) geom '
                       'FROM {pos_neg_joined}'.format(
                           geoid=geoid,
                           pos_neg_joined=pos_neg_joined,
                           pos_neg_joined_diffed=pos_neg_joined_diffed))

        # Create new table with both diffed and non-diffed (didn't intersect with
        # water) geoms
        cursor.execute('DROP TABLE IF EXISTS {pos_neg_joined_diffed_merged}'.format(
            pos_neg_joined_diffed_merged=pos_neg_joined_diffed_merged))
        cursor.execute('CREATE TEMPORARY TABLE {pos_neg_joined_diffed_merged} '
                       'AS SELECT * FROM {pos_neg_joined_diffed}'.format(
                           pos_neg_joined_diffed=pos_neg_joined_diffed,
                           pos_neg_joined_diffed_merged=pos_neg_joined_diffed_merged))
        cursor.execute('INSERT INTO {pos_neg_joined_diffed_merged} '
                       'SELECT {geoid}, id, geom FROM {pos_split} '
                       'WHERE id NOT IN (SELECT id from {pos_neg_joined_diffed})'.format(
                           geoid=geoid,
                           pos_split=pos_split,
                           pos_neg_joined_diffed=pos_neg_joined_diffed,
                           pos_neg_joined_diffed_merged=pos_neg_joined_diffed_merged))
        cursor.execute('CREATE INDEX ON {pos_neg_joined_diffed_merged} '
                       'USING GIST (geom)'.format(
                           pos_neg_joined_diffed_merged=pos_neg_joined_diffed_merged))

        # Re-union the pos table based off its geoid
        cursor.execute('DROP TABLE IF EXISTS {output}'.format(output=output))
        cursor.execute('CREATE TABLE {output} AS '
                       'SELECT {geoid} AS geoid, ST_UNION(geom) AS geom '
                       'FROM {pos_neg_joined_diffed_merged} '
                       'GROUP BY {geoid}'.format(
                           geoid=geoid,
                           output=output,
                           pos_neg_joined_diffed_merged=pos_neg_joined_diffed_merged))

        cursor.connection.commit()
        self.output().touch()

    def output(self):
        tablename = '"{schema}".{geography}_{year}_shoreline_clipped'.format(
            schema=classpath(self),
            geography=str(self.geography).lower(),
            year=self.year)

        target = DefaultPostgresTarget(table=tablename)
        if self.force:
            target.untouch()
            self.force = False
        return target



class ExtractClippedTiger(Task):
    # TODO this should be merged with ExtractTiger

    force = BooleanParameter(default=False)
    year = Parameter()
    sumlevel = Parameter()

    def requires(self):
        pass

    def run(self):
        sql_to_cartodb_table(self.tablename(), self.input().table)

    def tablename(self):
        return self.input().table.replace('-', '_').replace('/', '_').replace('"', '').replace('.', '_')

    def output(self):
        target = CartoDBTarget(self.tablename())
        if self.force and target.exists():
            target.remove()
        return target


class SumLevel(TableTask):

    clipped = BooleanParameter(default=False)
    geography = Parameter()
    year = Parameter()

    @property
    def geoid(self):
        return 'geoid10' if self.geography in ('zcta5', 'puma', ) else 'geoid'

    @property
    def input_tablename(self):
        return SUMLEVELS_BY_SLUG[self.geography]['table']

    def version(self):
        return '4'

    def requires(self):
        if self.clipped:
            tiger = ShorelineClipTiger(
                year=self.year, geography=self.input_tablename)
        else:
            tiger = DownloadTiger(year=self.year)
        return {
            'data': tiger,
            'geoids': GeoidColumns(),
            'geoms': GeomColumns()
        }

    def columns(self):
        return OrderedDict([
            ('geoid', self.input()['geoids'][self.geography + '_geoid']),
            ('the_geom', self.input()['geoms'][self.geography])
        ])

    def timespan(self):
        return self.year

    def bounds(self):
        if not self.input()['data'].exists():
            return
        if self.clipped:
            from_clause = self.input()['data'].table
        else:
            from_clause = '{inputschema}.{input_tablename}'.format(
                inputschema=self.input()['data'].table,
                input_tablename=self.input_tablename,
            )
        session = current_session()
        return session.execute('SELECT ST_EXTENT(geom) FROM '
                               '{from_clause}'.format(
                                   from_clause=from_clause
                               )).first()[0]

    def populate(self):
        session = current_session()
        if self.clipped:
            from_clause = self.input()['data'].table
        else:
            from_clause = '{inputschema}.{input_tablename}'.format(
                inputschema=self.input()['data'].table,
                input_tablename=self.input_tablename,
            )
        session.execute('INSERT INTO {output} (geoid, the_geom) '
                        'SELECT {geoid}, geom the_geom  '
                        'FROM {from_clause}'.format(
                            geoid=self.geoid,
                            output=self.output().get(session).id,
                            from_clause=from_clause
                        ))


class AllSumLevels(WrapperTask):
    '''
    Compute all sumlevels
    '''

    year = Parameter(default=2013)

    def requires(self):
        for clipped in (True, False):
            for geo in ('state', 'county', 'census_tract', 'block_group', 'puma', 'zcta5',):
                yield SumLevel(year=self.year, geography=geo, clipped=clipped)


def load_sumlevels():
    '''
    Load summary levels from JSON. Returns a dict by sumlevel number.
    '''
    with open(os.path.join(os.path.dirname(__file__), 'summary_levels.json')) as fhandle:
        sumlevels_list = json.load(fhandle)
    sumlevels = {}
    for slevel in sumlevels_list:
        # Replace pkey ancestors with paths to columns
        # We subtract 1 from the pkey because it's 1-indexed, unlike python
        fields = slevel['fields']
        for i, ancestor in enumerate(fields['ancestors']):
            colpath = os.path.join('columns', classpath(load_sumlevels),
                                   sumlevels_list[ancestor - 1]['fields']['slug'])
            fields['ancestors'][i] = colpath
        if fields['parent']:
            fields['parent'] = os.path.join(
                'columns', classpath(load_sumlevels),
                sumlevels_list[fields['parent'] - 1]['fields']['slug'])

        sumlevels[fields['summary_level']] = fields
    return sumlevels


SUMLEVELS = load_sumlevels()
SUMLEVELS_BY_SLUG = dict([(v['slug'], v) for k, v in SUMLEVELS.iteritems()])
