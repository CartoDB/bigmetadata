'''
Tasks to sync data locally to CartoDB
'''

from tasks.meta import current_session, OBSTable, Base, OBSColumn
from tasks.util import (TableToCarto, underscore_slugify, query_cartodb,
                        classpath, shell, PostgresTarget, TempTableTask)

from luigi import (WrapperTask, BooleanParameter, Parameter, Task, LocalTarget,
                   DateParameter, IntParameter, FloatParameter)
from luigi.task_register import Register
from nose.tools import assert_equal
from urllib import quote_plus
from datetime import date
from decimal import Decimal
from cStringIO import StringIO
from PIL import Image, ImageOps

import requests

import os
import json
import requests

def extract_dict_a_from_b(a, b):
    return dict([(k, b[k]) for k in a.keys() if k in b.keys()])


def metatables():
    for tablename, table in Base.metadata.tables.iteritems():
        if tablename.startswith('observatory.obs_'):
            yield tablename, table


class Import(TempTableTask):
    '''
    Import a table from a CartoDB account
    '''

    username = Parameter(default='')
    subdomain = Parameter(default='observatory')
    table = Parameter()

    TYPE_MAP = {
        'string': 'TEXT',
        'number': 'NUMERIC',
        'geometry': 'GEOMETRY',
    }

    @property
    def _url(self):
        return 'https://{subdomain}.cartodb.com/{username}api/v2/sql'.format(
            username=self.username + '/' if self.username else '',
            subdomain=self.subdomain
        )

    def _query(self, **params):
        return requests.get(self._url, params=params)

    def _create_table(self):
        resp = self._query(
            q='SELECT * FROM {table} LIMIT 0'.format(table=self.table)
        )
        coltypes = dict([
            (k, self.TYPE_MAP[v['type']]) for k, v in resp.json()['fields'].iteritems()
        ])
        resp = self._query(
            q='SELECT * FROM {table} LIMIT 0'.format(table=self.table),
            format='csv'
        )
        colnames = resp.text.strip().split(',')
        columns = ', '.join(['{colname} {type}'.format(
            colname=c,
            type=coltypes[c]
        ) for c in colnames])
        stmt = 'CREATE TABLE {table} ({columns})'.format(table=self.output().table,
                                                         columns=columns)
        shell("psql -c '{stmt}'".format(stmt=stmt))

    def _load_rows(self):
        url = self._url + '?q={q}&format={format}'.format(
            q=quote_plus('SELECT * FROM {table}'.format(table=self.table)),
            format='csv'
        )
        shell(r"curl '{url}' | "
              r"psql -c '\copy {table} FROM STDIN WITH CSV HEADER'".format(
                  table=self.output().table,
                  url=url))

    def run(self):
        self._create_table()
        self._load_rows()
        shell("psql -c 'CREATE INDEX ON {table} USING gist (the_geom)'".format(
            table=self.output().table,
        ))


class SyncMetadata(WrapperTask):

    force = BooleanParameter(default=True)

    def requires(self):
        for tablename, _ in metatables():
            schema, tablename = tablename.split('.')
            yield TableToCarto(table=tablename, outname=tablename, force=self.force,
                               schema=schema)


def should_upload(table):
    '''
    Determine whether a table has any important columns.  If so, it should be
    uploaded, otherwise it should be ignored.
    '''
    # TODO this table doesn't want to upload
    if table.tablename == 'obs_ffebc3eb689edab4faa757f75ca02c65d7db7327':
        return False
    for coltable in table.columns:
        if coltable.column.weight > 0:
            return True
    return False


class SyncColumn(WrapperTask):
    '''
    Upload tables relevant to updating a particular column by keyword.
    '''
    keywords = Parameter()

    def requires(self):
        session = current_session()
        cols = session.query(OBSColumn).filter(OBSColumn.id.ilike(
            '%' + self.keywords + '%'
        ))
        if cols.count():
            for col in cols:
                for coltable in col.tables:
                    yield SyncData(exact_id=coltable.table.id)
        else:
            tables = session.query(OBSTable).filter(OBSTable.id.ilike(
                '%' + self.keywords + '%'
            ))
            if tables.count():
                for table in tables:
                    yield SyncData(exact_id=table.id)
            else:
                raise Exception('Unable to find any tables or columns with ID '
                                'that matched "{keywords}" via ILIKE'.format(
                                    keywords=self.keywords
                                ))


class SyncData(WrapperTask):
    '''
    Upload a single OBS table to cartodb by fuzzy ID
    '''
    force = BooleanParameter(default=True)
    id = Parameter(default=None)
    exact_id = Parameter(default=None)
    tablename = Parameter(default=None)

    def requires(self):
        session = current_session()
        if self.exact_id:
            table = session.query(OBSTable).get(self.exact_id)
        elif self.tablename:
            table = session.query(OBSTable).filter(OBSTable.tablename == self.tablename).one()
        elif self.id:
            table = session.query(OBSTable).filter(OBSTable.id.ilike('%' + self.id + '%')).one()
        else:
            raise Exception('Need id or exact_id for SyncData')
        return TableToCarto(table=table.tablename, force=self.force)


class SyncAllData(WrapperTask):

    force = BooleanParameter(default=False)

    def requires(self):
        tables = {}
        session = current_session()
        for table in session.query(OBSTable):
            if should_upload(table):
                tables[table.id] = table.tablename

        for table_id, tablename in tables.iteritems():
            yield TableToCarto(table=tablename, outname=tablename, force=self.force)


class ImagesForMeasure(Task):
    '''
    Generate a set of static images for a measure
    '''

    MAP_URL = '{cartodb_url}/api/v1/map'.format(
        cartodb_url=os.environ['CARTODB_URL'])

    BASEMAP = {
        "type": "http",
        "options": {
            "urlTemplate": "http://{s}.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}.png",
            "subdomains": "abcd",
        }
    }

    LABELS = {
        "type": "http",
        "options": {
            "urlTemplate": "http://{s}.basemaps.cartocdn.com/light_only_labels/{z}/{x}/{y}.png",
            "subdomains": "abcd",
        }
    }

    SPAIN_CENTERS = [
        [40.4139017, -3.7350414],
        [40.4139017, -3.7350414],
        [40.4139017, -3.7350414],
        [40.4139017, -3.7050414],
    ]
    SPAIN_ZOOMS = [
        #6, 9, 12, 15
        #6, 8, 10, 13
        6, 8, 11, 13
    ]
    US_CENTERS = [
        [37.996162679728116, -97.6904296875],
        [38.16911413556086, -114.884033203125],
        #[37.67512527892127, -121.06109619140625],
        [37.75225820732333, -122.11584777832031],
        [37.75225820732333, -122.44584777832031],
    ]
    US_ZOOMS = [
        3, 5, 9, 12
    ]
    US_BOUNDARIES = [
        'us.census.tiger.state_clipped',
        'us.census.tiger.county_clipped',
        'us.census.tiger.census_tract_clipped',
        'us.census.tiger.block_group_clipped',
    ]

    PALETTES = {
        'tags.people': '''
@5:#6c2167;
@4:#a24186;
@3:#ca699d;
@2:#e498b4;
@1:#f3cbd3;''',
        'tags.money': '''
@5:#1d4f60;
@4:#2d7974;
@3:#4da284;
@2:#80c799;
@1:#c4e6c3;''',
        'tags.households': '''
@5:#63589f;
@4:#9178c4;
@3:#b998dd;
@2:#dbbaed;
@1:#f3e0f7;''',
        'tags.housing': '''
@5:#2a5674;
@4:#45829b;
@3:#68abb8;
@2:#96d0d1;
@1:#d1eeea;''',
        'tags.ratio': '''
@5:#eb4a40;
@4:#f17854;
@3:#f59e72;
@2:#f9c098;
@1:#fde0c5;''',
    }

    measure = Parameter()
    force = BooleanParameter(default=False)

    def __init__(self, *args, **kwargs):
        if kwargs.get('force'):
            target_path = self.output(measure=kwargs['measure']).path
            try:
                os.unlink(target_path)
            except OSError:
                pass
        super(ImagesForMeasure, self).__init__(*args, **kwargs)

    def _generate_config(self, zoom, lon, lat, boundary=None):
        layers = []
        layers.append(self.BASEMAP)
        session = current_session()
        measure = session.query(OBSColumn).get(self.measure)
        mainquery = '''
SELECT data_t.timespan, data_c.aggregate, target_c.id boundary_id,
       (data_ct.extra->'stats'->>'stddev')::NUMERIC "stddev",
       (data_ct.extra->'stats'->>'avg')::NUMERIC "avg",
       (data_ct.extra->'stats'->>'min')::NUMERIC "min",
       (data_ct.extra->'stats'->>'max')::NUMERIC "max",
       data_ct.colname as data_colname, data_geoid_ct.colname data_geoid_colname,
       data_t.tablename as data_tablename,
       geom_geoid_ct.colname geom_geoid_colname,
       geom_ct.colname geom_geom_colname, geom_t.tablename as geom_tablename,
       denom_ct.colname denominator_colname
       --target_c.weight, target_c.id
FROM observatory.obs_column_to_column geom_ref_c2c
 JOIN observatory.obs_column target_c
   ON geom_ref_c2c.target_id = target_c.id
 JOIN observatory.obs_column source_c
   ON geom_ref_c2c.source_id = source_c.id
 JOIN observatory.obs_column_table data_geoid_ct
   ON source_c.id = data_geoid_ct.column_id
 JOIN observatory.obs_column_table geom_geoid_ct
   ON source_c.id = geom_geoid_ct.column_id
 JOIN observatory.obs_column_table geom_ct
   ON geom_ct.column_id = target_c.id
 JOIN observatory.obs_table geom_t
   ON geom_ct.table_id = geom_t.id
   AND geom_geoid_ct.table_id = geom_t.id
 JOIN observatory.obs_table data_t
   ON data_geoid_ct.table_id = data_t.id
 JOIN observatory.obs_column_table data_ct
   ON data_ct.table_id = data_t.id
 JOIN observatory.obs_column data_c
   ON data_ct.column_id = data_c.id
 LEFT JOIN
   (observatory.obs_column_to_column denom_c2c
       JOIN observatory.obs_column_table denom_ct
         ON denom_c2c.target_id = denom_ct.column_id) -- force denom in same table
   ON denom_c2c.source_id = data_ct.column_id
WHERE
  data_ct.column_id = '{measure}'
  {boundary_clause}
  AND geom_ref_c2c.reltype = 'geom_ref'
  AND target_c.type ILIKE 'geometry'
  AND (data_ct.extra->'stats'->>'avg')::NUMERIC IS NOT NULL
  AND (data_ct.extra->'stats'->>'stddev')::NUMERIC IS NOT NULL
  AND (data_ct.extra->'stats'->>'min')::NUMERIC IS NOT NULL
  AND (data_ct.extra->'stats'->>'max')::NUMERIC IS NOT NULL
  AND (denom_c2c.reltype IS NULL OR denom_c2c.reltype = 'denominator')
  AND (denom_ct.table_id IS NULL OR denom_ct.table_id = data_t.id)
ORDER BY target_c.weight DESC, data_t.timespan DESC, geom_ct.column_id DESC;
'''
        query = mainquery.format(
            measure=self.measure,
            boundary_clause="AND target_c.id = '{}'".format(boundary) if boundary else '')
        resp = session.execute(query)
        results = resp.fetchone()
        if not results:
            # give up boundary clause if no results
            query = mainquery.format(measure=self.measure, boundary_clause='')
            resp = session.execute(query)
            results = resp.fetchone()
        try:
            timespan, aggregate, boundary_id, stddev, avg, min, max, data_data_colname, \
                    data_geoid_colname, data_tablename, geom_geoid_colname, \
                    geom_geom_colname, geom_tablename, denom_colname = results
        except TypeError:
            import pdb
            pdb.set_trace()
        calcmax = avg + (stddev * 3)
        max = max if max < calcmax else calcmax
        calcmin = avg - (stddev * 3)
        min = min if min > calcmin else calcmin

        if denom_colname:
            cartosql = "SELECT geom.cartodb_id, geom.{geom_geom_colname} as the_geom, " \
                    "geom.the_geom_webmercator, " \
                    "data.{data_data_colname} / NULLIF(data.{denom_colname}, 0) measure " \
                    "FROM {geom_tablename} as geom, {data_tablename} as data " \
                    "WHERE geom.{geom_geoid_colname} = data.{data_geoid_colname} "
            statssql = "SELECT ST_Xmin(ST_Extent(geom.{geom_geom_colname})) x_min, " \
                    "ST_Ymin(ST_Extent(geom.{geom_geom_colname})) y_min, " \
                    "ST_Xmax(ST_Extent(geom.{geom_geom_colname})) x_max, " \
                    "ST_Ymax(ST_Extent(geom.{geom_geom_colname})) y_max, " \
                    'MIN(data.{data_data_colname} / NULLIF(data.{denom_colname}, 0)) "min", ' \
                    'MAX(data.{data_data_colname} / NULLIF(data.{denom_colname}, 0)) "max", ' \
                    'AVG(data.{data_data_colname} / NULLIF(data.{denom_colname}, 0)) "avg", ' \
                    'MODE() WITHIN GROUP (ORDER BY data.{data_data_colname} / ' \
                    '  NULLIF(data.{denom_colname}, 0)) "mode", ' \
                    'STDDEV_POP(data.{data_data_colname} / ' \
                    '  NULLIF(data.{denom_colname}, 0)) "stddev_pop", ' \
                    'PERCENTILE_CONT(0.5) WITHIN ' \
                    '  GROUP (ORDER BY data.{data_data_colname} / ' \
                    '  NULLIF(data.{denom_colname}, 0)) "median" ' \
                    "FROM observatory.{geom_tablename} as geom, " \
                    "     observatory.{data_tablename} as data " \
                    "WHERE geom.{geom_geoid_colname} = data.{data_geoid_colname} "
        elif aggregate == 'sum':
            cartosql = "SELECT geom.cartodb_id, geom.{geom_geom_colname} as the_geom, " \
                    "geom.the_geom_webmercator, " \
                    "data.{data_data_colname} / " \
                    "  ST_Area(ST_Transform(geom.{geom_geom_colname}, 3857)) * 1000000.0 measure " \
                    "FROM {geom_tablename} as geom, {data_tablename} as data " \
                    "WHERE geom.{geom_geoid_colname} = data.{data_geoid_colname} "
            statssql = "SELECT ST_Xmin(ST_Extent(geom.{geom_geom_colname})) x_min, " \
                    "ST_Ymin(ST_Extent(geom.{geom_geom_colname})) y_min, " \
                    "ST_Xmax(ST_Extent(geom.{geom_geom_colname})) x_max, " \
                    "ST_Ymax(ST_Extent(geom.{geom_geom_colname})) y_max, " \
                    'MIN(data.{data_data_colname} / {landarea}) "min", ' \
                    'MAX(data.{data_data_colname} / {landarea}) "max", ' \
                    'AVG(data.{data_data_colname} / {landarea}) "avg", ' \
                    'MODE() WITHIN GROUP (ORDER BY data.{data_data_colname} / ' \
                    '  {landarea}) "mode", ' \
                    'STDDEV_POP(data.{data_data_colname} / ' \
                    '  {landarea}) "stddev_pop", ' \
                    'PERCENTILE_CONT(0.5) WITHIN ' \
                    '  GROUP (ORDER BY data.{data_data_colname} / ' \
                    '  {landarea}) "median" ' \
                    "FROM observatory.{geom_tablename} as geom, " \
                    "     observatory.{data_tablename} as data " \
                    "WHERE geom.{geom_geoid_colname} = data.{data_geoid_colname} "
        else:
            cartosql = "SELECT geom.cartodb_id, geom.{geom_geom_colname} as the_geom, " \
                    "geom.the_geom_webmercator, " \
                    "data.{data_data_colname} measure " \
                    "FROM {geom_tablename} as geom, {data_tablename} as data " \
                    "WHERE geom.{geom_geoid_colname} = data.{data_geoid_colname} "
            statssql = "SELECT ST_Xmin(ST_Extent(geom.{geom_geom_colname})) x_min, " \
                    "ST_Ymin(ST_Extent(geom.{geom_geom_colname})) y_min, " \
                    "ST_Xmax(ST_Extent(geom.{geom_geom_colname})) x_max, " \
                    "ST_Ymax(ST_Extent(geom.{geom_geom_colname})) y_max, " \
                    'MIN(data.{data_data_colname}) "min", ' \
                    'MAX(data.{data_data_colname}) "max", ' \
                    'AVG(data.{data_data_colname}) "avg", ' \
                    'MODE() WITHIN GROUP (ORDER BY data.{data_data_colname}) "mode", ' \
                    'STDDEV_POP(data.{data_data_colname}) "stddev_pop", ' \
                    'PERCENTILE_CONT(0.5) WITHIN ' \
                    '  GROUP (ORDER BY data.{data_data_colname}) "median" ' \
                    "FROM observatory.{geom_tablename} as geom, " \
                    "     observatory.{data_tablename} as data " \
                    "WHERE geom.{geom_geoid_colname} = data.{data_geoid_colname} "

        if boundary_id.lower().startswith('us.census.tiger'):
            landarea = 'aland * 1000000.0'
        else:
            landarea = 'ST_Area(ST_Transform(geom.{geom_geom_colname}, 3857))' \
                    ' * 1000000.0'.format(geom_geom_colname=geom_geom_colname)

        cartosql = cartosql.format(geom_geom_colname=geom_geom_colname,
                                   data_data_colname=data_data_colname,
                                   geom_tablename=geom_tablename,
                                   data_tablename=data_tablename,
                                   geom_geoid_colname=geom_geoid_colname,
                                   data_geoid_colname=data_geoid_colname,
                                   denom_colname=denom_colname,
                                   landarea=landarea)
        statssql = statssql.format(geom_geom_colname=geom_geom_colname,
                                   data_data_colname=data_data_colname,
                                   geom_tablename=geom_tablename,
                                   data_tablename=data_tablename,
                                   geom_geoid_colname=geom_geoid_colname,
                                   data_geoid_colname=data_geoid_colname,
                                   denom_colname=denom_colname,
                                   landarea=landarea)

        xmin, ymin, xmax, ymax, min, max, avg, mode, stddev, median = \
                session.execute(statssql).fetchone()

        if measure.unit():
            ramp = self.PALETTES.get(measure.unit().id, self.PALETTES['tags.ratio'])
        else:
            ramp = self.PALETTES['tags.ratio']

        layers.append({
            'type': 'mapnik',
            'options': {
                'layer_name': data_tablename,
                'cartocss': '''/** choropleth visualization */

{ramp}

#data {{
  polygon-opacity: 0.9;
  polygon-gamma: 0.5;
  line-color: #000000;
  line-width: 0.25;
  line-opacity: 0.2;
  line-comp-op: hard-light;

  [measure=null]{{
     polygon-fill: #cacdce;
  }}
  [measure <= {range5}] {{
     polygon-fill: @5;
  }}
  [measure <= {range4}] {{
     polygon-fill: @4;
  }}
  [measure <= {range3}] {{
     polygon-fill: @3;
  }}
  [measure <= {range2}] {{
     polygon-fill: @2;
  }}
  [measure <= {range1}] {{
     polygon-fill: @1;
  }}
}}'''.format(
    ramp=ramp,
    range1=min,
    range2=float(min) + (float(avg - min) / 2.0),
    range3=avg,
    range4=float(avg) + (float(max - avg) / 2.0),
    range5=max),
                'cartocss_version': "2.1.1",
                'sql': cartosql,
                "table_name": "\"\"."
            }
        })
        #layers.append(self.LABELS)
        return {
            'layers': layers,
            'center': [lon, lat],
            #'bounds': self.bounds,
            'zoom': zoom
        }

    def get_named_map(self, map_config):

        config = {
            "version": "1.3.0",
            "layers": map_config
        }
        resp = requests.get(self.MAP_URL,
                            headers={'content-type':'application/json'},
                            params={'config': json.dumps(config)})
        return resp.json()

    def run(self):
        self.output().makedirs()

        if self.measure.lower().startswith('es.ine'):
            zooms = self.SPAIN_ZOOMS
            centers = self.SPAIN_CENTERS
        else:
            zooms = self.US_ZOOMS
            centers = self.US_CENTERS
        image_urls = []
        for center, zoom, boundary in zip(centers, zooms, self.US_BOUNDARIES):
            lon, lat = center
            if self.measure.lower().startswith('us.census.acs'):
                config = self._generate_config(zoom, lon, lat, boundary)
            else:
                config = self._generate_config(zoom, lon, lat)
            named_map = self.get_named_map(config['layers'])
            #if 'layergroupid' not in named_map:
            #    import pdb
            #    pdb.set_trace()
            image_urls.append('{cartodb_url}/api/v1/map/static/center/' \
                              '{layergroupid}/{zoom}/{center_lon}/{center_lat}/500/500.png'.format(
                                  cartodb_url=os.environ['CARTODB_URL'],
                                  layergroupid=named_map['layergroupid'],
                                  zoom=zoom,
                                  center_lon=lon,
                                  center_lat=lat
                              ))

        url1 = image_urls.pop(0)
        print url1
        file1 = StringIO(requests.get(url1, stream=True).content)
        image1 = ImageOps.expand(Image.open(file1), border=10, fill='white')

        for url2 in image_urls:
            print url2
            file2 = StringIO(requests.get(url2, stream=True).content)

            image2 = ImageOps.expand(Image.open(file2), border=10, fill='white')

            (width1, height1) = image1.size
            (width2, height2) = image2.size

            result_width = width1 + width2
            result_height = max(height1, height2)

            result = Image.new('RGB', (result_width, result_height))
            result.paste(im=image1, box=(0, 0))
            result.paste(im=image2, box=(width1, 0))

            image1 = result
        image1.save(self.output().path)

    def output(self, measure=None):
        if measure is None:
            measure = self.measure
        return LocalTarget(os.path.join('catalog/img', measure + '.png'))
        #return LocalTarget(os.path.join('catalog/build/html/_images', measure + '.png'))


class GenerateStaticImage(Task):

    BASEMAP = {
        "type": "http",
        "options": {
            #"urlTemplate": "https://{s}.maps.nlp.nokia.com/maptile/2.1/maptile/newest/satellite.day/{z}/{x}/{y}/256/jpg?lg=eng&token=A7tBPacePg9Mj_zghvKt9Q&app_id=KuYppsdXZznpffJsKT24",
            #"subdomains": "1234",
            # Dark Matter
            "urlTemplate": "http://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}.png",
            "subdomains": "abcd",
            #"urlTemplate": "http://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}.png",
            #"subdomains": ["a", "b", "c"]
        }
    }

    LABELS = {
        "type": "http",
        "options": {
            "urlTemplate": "http://{s}.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}.png",
            "subdomains": "abcd",
        }
    }

    #57d9408e-0351-11e6-9c12-0e787de82d45

    viz = Parameter()
    VIZ_URL = '{cartodb_url}/api/v2/viz/{{viz}}/viz.json'.format(
        cartodb_url=os.environ['CARTODB_URL'])
    MAP_URL = '{cartodb_url}/api/v1/map'.format(
        cartodb_url=os.environ['CARTODB_URL'])

    def viz_to_config(self):
        resp = requests.get(self.VIZ_URL.format(viz=self.viz))

        assert resp.status_code == 200
        data = resp.json()
        layers = []
        layers.append(self.BASEMAP)
        for data_layer in data['layers']:
            if data_layer['type'] == 'layergroup':
                for layer in data_layer['options']['layer_definition']['layers']:
                    if layer['visible'] is True:
                        layers.append({'type': 'mapnik', 'options': layer['options']})
        layers.append(self.LABELS)
        return {
            'layers': layers,
            'center': json.loads(data['center']),
            'bounds': data['bounds'],
            'zoom': data['zoom']
        }

    def get_named_map(self, map_config):

        config = {
            "version": "1.3.0",
            "layers": map_config
        }
        resp = requests.get(self.MAP_URL,
                            headers={'content-type':'application/json'},
                            params={'config': json.dumps(config)})
        return resp.json()

    def run(self):
        self.output().makedirs()
        config = self.viz_to_config()
        named_map = self.get_named_map(config['layers'])
        img_url = '{cartodb_url}/api/v1/map/static/center/' \
                '{layergroupid}/{zoom}/{center_lon}/{center_lat}/800/500.png'.format(
                    cartodb_url=os.environ['CARTODB_URL'],
                    layergroupid=named_map['layergroupid'],
                    zoom=config['zoom'],
                    center_lon=config['center'][0],
                    center_lat=config['center'][1]
                )
        print img_url
        shell('curl "{img_url}" > {output}'.format(img_url=img_url,
                                                   output=self.output().path))

    def output(self):
        return LocalTarget(os.path.join('catalog/source/img', self.task_id + '.png'))


class PurgeFunctions(Task):
    '''
    Purge remote functions
    '''
    pass


class PurgeMetadataTags(Task):
    '''
    Purge local metadata tables that no longer have tasks linking to them
    '''
    pass


class PurgeMetadataColumns(Task):
    '''
    Purge local metadata tables that no longer have tasks linking to them
    '''
    pass


class PurgeUndocumentedTables(Task):
    '''
    Purge tables that should be in metadata but are not.
    '''

    def run(self):
        session = current_session()
        resp = session.execute('SELECT table_schema, table_name '
                               'FROM information_schema.tables '
                               "WHERE table_schema ILIKE 'observatory' ")
        for _, tablename in resp:
            if tablename in ('obs_table', 'obs_column_table', 'obs_column',
                             'obs_tag', 'obs_column_to_column', 'obs_column_tag'):
                continue
            if session.query(OBSTable).filter_by(tablename=tablename).count() == 0:
                cnt = session.execute('SELECT COUNT(*) FROM observatory.{tablename}'.format(
                    tablename=tablename)).fetchone()[0]
                if cnt == 0:
                    stmt = 'DROP TABLE observatory.{tablename}'.format(
                        tablename=tablename)
                    print(stmt)
                    session.execute(stmt)
                    session.commit()
                else:
                    raise Exception("Will not automatically drop table {tablename} "
                                    "with data in it".format(tablename=tablename))


class PurgeMetadataTables(Task):
    '''
    Purge local metadata tables that no longer have tasks linking to them,
    as well as entries in obs_table that do not link to any table.
    '''

    def run(self):
        session = current_session()
        for _output in self.output():
            if not _output.exists():
                resp = session.execute("SELECT id from observatory.obs_table "
                                       "WHERE tablename = '{tablename}'".format(
                                           tablename=_output.tablename))
                _id = resp.fetchall()[0][0]
                stmt = "DELETE FROM observatory.obs_table " \
                        "WHERE id = '{id}'".format(id=_id)
                print(stmt)
                session.execute(stmt)
                session.commit()

    def output(self):
        session = current_session()
        for table in session.query(OBSTable):
            split = table.id.split('.')
            schema, task_id = split[0:-1], split[-1]
            modname = 'tasks.' + '.'.join(schema)
            module = __import__(modname, fromlist=['*'])
            exists = False
            for name in dir(module):
                kls = getattr(module, name)
                if not isinstance(kls, Register):
                    continue
                # this doesn't work because of underscore_slugify
                #possible_kls = '_'.join(task_id.split('_')[0:-len(kls.get_params())-1])
                if task_id.startswith(underscore_slugify(name)):
                    exists = True
            if exists is True:
                print('{table} exists'.format(table=table))
            else:
                # TODO drop table
                import pdb
                pdb.set_trace()
                print table
            #task_classes = dict([(underscore_slugify(kls), getattr(module, kls))
            #                     for kls in dir(module)
            #                     if isinstance(getattr(module, kls), Register)])
            #try:
            #    import pdb
            #    pdb.set_trace()
            #    module = __import__(modname)
            #except ImportError:
            #    # drop table
            #    pass
            yield PostgresTarget(schema='observatory', tablename=table.tablename)


class ConfirmTablesDescribedExist(Task):
    '''
    Confirm that all tables described in obs_table actually exist.
    '''

    def complete(self):
        return getattr(self, '_complete', False)

    def run(self):
        session = current_session()
        for table in session.query(OBSTable):
            target = PostgresTarget('observatory', table.tablename)
            assert target.exists()
            assert session.execute(
                'SELECT COUNT(*) FROM observatory.{tablename}'.format(
                    tablename=table.tablename)).fetchone()[0] > 0
        self._complete = True


class PurgeMetadata(WrapperTask):
    '''
    Purge local metadata that no longer has tasks linking to it
    '''

    def requires(self):
        yield PurgeMetadataTags()
        yield PurgeMetadataColumns()
        yield PurgeMetadataTables()


class PurgeData(Task):
    '''
    Purge local data that no longer has tasks linking to it.
    '''
    pass


class PurgeRemoteData(Task):
    '''
    Purge remote data that is no longer available locally
    '''
    pass


class TestData(Task):
    '''
    See if a dataset has been uploaded & is in sync (at the least, has
    the same number of rows & columns as local).
    '''
    pass


class TestAllData(Task):
    '''
    See if all datasets have been uploaded & are in sync
    '''

    pass


class TestMetadata(Task):
    '''
    Make sure all metadata is uploaded & in sync
    '''

    def run(self):
        session = current_session()
        for tablename, table in metatables():
            pkey = [c.name for c in table.primary_key]

            resp = query_cartodb('select * from {tablename}'.format(
                tablename=tablename))
            for remote_row in resp.json()['rows']:
                uid = dict([
                    (k, remote_row[k]) for k in pkey
                ])
                local_vals = [unicode(v) for v in session.query(table).filter_by(**uid).one()]
                local_row = dict(zip([col.name for col in table.columns], local_vals))
                remote_row = dict([(k, unicode(v)) for k, v in remote_row.iteritems()])
                try:
                    assert_equal(local_row, extract_dict_a_from_b(local_row, remote_row))
                except Exception as err:
                    import pdb
                    pdb.set_trace()
                    print err

        self._complete = True

    def complete(self):
        return hasattr(self, '_complete') and self._complete is True


class Dump(Task):
    '''
    Dump of the entire observatory schema
    '''

    timestamp = DateParameter(default=date.today())

    def requires(self):
        return ConfirmTablesDescribedExist()

    def run(self):
        self.output().makedirs()
        shell('pg_dump -Fc -Z0 -x -n observatory -f {output}'.format(
            output=self.output().path))

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id + '.dump'))
