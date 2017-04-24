'''
Tasks to sync data locally to CartoDB
'''

from tasks.meta import (current_session, OBSTable, Base, OBSColumn,)
from tasks.util import (TableToCarto, underscore_slugify, query_cartodb,
                        classpath, shell, PostgresTarget, TempTableTask, LOGGER,
                        CartoDBTarget, import_api, TableToCartoViaImportAPI)

from luigi import (WrapperTask, BooleanParameter, Parameter, Task, LocalTarget,
                   DateParameter, IntParameter, FloatParameter)
from luigi.task_register import Register
from luigi.s3 import S3Target
from datetime import date
from decimal import Decimal
from cStringIO import StringIO
from PIL import Image, ImageOps
from pprint import pprint

import requests

import time
import os
import json
import requests


META_TABLES = ('obs_table', 'obs_column_table', 'obs_column', 'obs_column_to_column',
               'obs_column_tag', 'obs_tag', 'obs_dump_version', )



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
    force = BooleanParameter(default=True, significant=False)
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
    '''
    Sync all data to the linked CARTO account.
    '''

    force = BooleanParameter(default=False, significant=False)

    def requires(self):
        existing_table_versions = dict([
            (r['tablename'], r['version']) for r in query_cartodb(
                'SELECT * FROM obs_table'
            ).json()['rows']
        ])
        tables = dict([(k, v) for k, v in current_session().execute(
            '''
            SELECT tablename, t.version
            FROM observatory.obs_table t,
                 observatory.obs_column_table ct,
                 observatory.obs_column c
            WHERE t.id = ct.table_id
              AND c.id = ct.column_id
              AND t.tablename NOT IN ('obs_ffebc3eb689edab4faa757f75ca02c65d7db7327')
              AND c.weight > 0
            '''
        )])

        for tablename, version in tables.iteritems():
            if version > existing_table_versions.get(tablename):
                force = True
            else:
                force = self.force
            yield TableToCartoViaImportAPI(table=tablename, force=force)


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
    CENTER_ZOOM_BOUNDS = {
        'es': [
            ((40.4139017, -3.7350414), 6, None,),
            ((40.4139017, -3.7350414), 8, None, ),
            ((40.4139017, -3.7350414), 11, None, ),
            ((40.4139017, -3.7050414), 13, None, ),
        ],
        'mx': [
            ((22.979, -101.777), 4, 'mx.inegi.entidad', ),
            ((19.316, -99.152), 7, 'mx.inegi.municipio', ),
            ((19.441989391028706, -99.14474487304688), 11, 'mx.inegi.ageb', ),
            ((19.441989391028706, -99.14474487304688), 13, 'mx.inegi.manzana', ),
        ],
        'uk': [
            ((52.51622086393074, -1.197509765625), 5, None, ), # All England
            ((51.50190410761811, -0.120849609375), 9, None, ), # London
            ((52.47274306920925, -3.982543945312), 7, None, ), # Wales
            ((53.491313790532956, -2.9706787109375), 9, None, ), # Manchester
        ],
        'us': [
            ((37.996162679728116, -97.6904296875), 3,
             'us.census.tiger.state_clipped', ),
            ((38.16911413556086, -114.884033203125), 5,
             'us.census.tiger.county_clipped', ),
            ((37.75225820732333, -122.11584777832031), 9,
             'us.census.tiger.census_tract_clipped', ),
            ((37.75225820732333, -122.44584777832031), 12,
             'us.census.tiger.block_group_clipped', ),
        ],
    }

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
        'tags.segmentation': '''
@1:#7F3C8D;
@2:#11A579;
@3:#3969AC;
@4:#F2B701;
@5:#E73F74;
@6:#80BA5A;
@7:#E68310;
@8:#008695;
@9:#CF1C90;
@10:#f97b72;
@11:#A5AA99;''',
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
SELECT numer_aggregate, numer_type,
       numer_colname, numer_geomref_colname,
       numer_tablename,
       geom_geomref_colname,
       geom_colname, geom_tablename,
       denom_colname, denom_tablename, denom_geomref_colname
FROM observatory.obs_meta
WHERE numer_id = '{measure}' {boundary_clause}
ORDER BY geom_weight DESC, numer_timespan DESC, geom_colname DESC;
        '''
        query = mainquery.format(
            measure=self.measure,
            boundary_clause="AND geom_id = '{}'".format(boundary) if boundary else '')

        resp = session.execute(query)
        results = resp.fetchone()

        # how should we determine fallback resolution?
        if results is None:
            query = mainquery.format(measure=self.measure, boundary_clause="")
            resp = session.execute(query)
            results = resp.fetchone()

        numer_aggregate, numer_type, numer_colname, numer_geomref_colname, \
                numer_tablename, geom_geomref_colname, geom_colname, \
                geom_tablename, denom_colname, \
                denom_tablename, denom_geomref_colname = results

        if denom_colname:
            cartosql = "SELECT geom.cartodb_id, geom.{geom_colname} as the_geom, " \
                    "geom.the_geom_webmercator, " \
                    "numer.{numer_colname} / NULLIF(denom.{denom_colname}, 0) measure " \
                    "FROM {geom_tablename} as geom, {numer_tablename} as numer, " \
                    "     {denom_tablename} as denom " \
                    "WHERE geom.{geom_geomref_colname} = numer.{numer_geomref_colname} " \
                    "  AND numer.{numer_geomref_colname} = denom.{denom_geomref_colname} "
            statssql = "SELECT  " \
                    'CDB_HeadsTailsBins(array_agg(distinct( ' \
                    '      (numer.{numer_colname} / ' \
                    '      NULLIF(denom.{denom_colname}, 0))::NUMERIC)), 4) as "headtails" ' \
                    "FROM {geom_tablename} as geom, " \
                    "     {numer_tablename} as numer, " \
                    "     {denom_tablename} as denom " \
                    "WHERE geom.{geom_geomref_colname} = numer.{numer_geomref_colname} " \
                    "  AND numer.{numer_geomref_colname} = denom.{denom_geomref_colname} "
        elif numer_aggregate == 'sum':
            cartosql = "SELECT geom.cartodb_id, geom.{geom_colname} as the_geom, " \
                    "geom.the_geom_webmercator, " \
                    "numer.{numer_colname} / " \
                    "  ST_Area(geom.the_geom) * 1000000.0 measure " \
                    "FROM {geom_tablename} as geom, {numer_tablename} as numer " \
                    "WHERE geom.{geom_geomref_colname} = numer.{numer_geomref_colname} "
            statssql = "SELECT CDB_HeadsTailsBins(array_agg(distinct( " \
                    '  (numer.{numer_colname} / ST_Area(geom.the_geom) ' \
                    '      * 1000000.0)::NUMERIC)), 4) as "headtails" ' \
                    "FROM {geom_tablename} as geom, " \
                    "     {numer_tablename} as numer " \
                    "WHERE geom.{geom_geomref_colname} = numer.{numer_geomref_colname} "
        else:
            cartosql = "SELECT geom.cartodb_id, geom.{geom_colname} as the_geom, " \
                    "  geom.the_geom_webmercator, " \
                    "  numer.{numer_colname} measure " \
                    "FROM {geom_tablename} as geom, {numer_tablename} as numer " \
                    "  WHERE geom.{geom_geomref_colname} = numer.{numer_geomref_colname} "
            if numer_type.lower() == 'numeric':
                statssql = "SELECT " \
                        'CDB_HeadsTailsBins(array_agg( ' \
                        '  distinct(numer.{numer_colname}::NUMERIC)), 4) as "headtails" ' \
                        "FROM {geom_tablename} as geom, " \
                        "     {numer_tablename} as numer " \
                        "WHERE geom.{geom_geomref_colname} = numer.{numer_geomref_colname} "
            else:
                statssql = '''
                SELECT array_agg(category) categories FROM (
                SELECT row_number() over () catname, {numer_colname} as category, COUNT(*) cnt
                FROM {numer_tablename}
                GROUP BY {numer_colname} ORDER BY COUNT(*) DESC
                LIMIT 10
                ) foo'''

        cartosql = cartosql.format(geom_colname=geom_colname,
                                   numer_colname=numer_colname,
                                   geom_tablename=geom_tablename,
                                   numer_tablename=numer_tablename,
                                   geom_geomref_colname=geom_geomref_colname,
                                   numer_geomref_colname=numer_geomref_colname,
                                   denom_colname=denom_colname,
                                   denom_tablename=denom_tablename,
                                   denom_geomref_colname=denom_geomref_colname)
        statssql = statssql.format(geom_colname=geom_colname,
                                   numer_colname=numer_colname,
                                   geom_tablename=geom_tablename,
                                   numer_tablename=numer_tablename,
                                   geom_geomref_colname=geom_geomref_colname,
                                   numer_geomref_colname=numer_geomref_colname,
                                   denom_colname=denom_colname,
                                   denom_tablename=denom_tablename,
                                   denom_geomref_colname=denom_geomref_colname)

        resp = query_cartodb(statssql)
        if resp.status_code != 200:
            raise Exception("Unable to obtain statssql: {}".format(
                resp.text))

        if measure.unit():
            ramp = self.PALETTES.get(measure.unit().id, self.PALETTES['tags.ratio'])
        else:
            ramp = self.PALETTES['tags.ratio']

        bucket_css = u''
        if numer_type.lower() == 'numeric':
            buckets = resp.json()['rows'][0]['headtails']


            for i, bucket in enumerate(buckets):
                bucket_css = u'''
    [measure <= {bucket}] {{
       polygon-fill: @{i};
    }}
                '''.format(bucket=bucket, i=i+1) + bucket_css
        else:
            buckets = resp.json()['rows'][0]['categories']
            for i, bucket in enumerate(buckets):
                bucket_css = u'''
    [measure = "{bucket}"] {{
       polygon-fill: @{i};
    }}
                '''.format(bucket=bucket, i=i+1) + bucket_css

        layers.append({
            'type': 'mapnik',
            'options': {
                'layer_name': geom_tablename,
                'cartocss': '''/** choropleth visualization */

{ramp}

#data {{
  polygon-opacity: 0.9;
  polygon-gamma: 0.5;
  line-color: #000000;
  line-width: 0.25;
  line-opacity: 0.2;
  line-comp-op: hard-light;
  polygon-fill: @{bucketlen};

  [measure=null]{{
     polygon-fill: #cacdce;
  }}
  {bucket_css}
}}'''.format(
    ramp=ramp,
    bucketlen=len(buckets) + 1,
    bucket_css=bucket_css),
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
                            params={'config': json.dumps(config)}).json()
        if 'layergroupid' not in resp:
            raise Exception('Named map returned no layergroupid: {}'.format(
                pprint(resp)))
        return resp

    def run(self):
        self.output().makedirs()

        image_urls = []
        country = self.measure.split('.')[0]
        for center, zoom, boundary in self.CENTER_ZOOM_BOUNDS[country]:
            lon, lat = center

            if country == 'uk':
                image_size = (300, 700, )
            else:
                image_size = (500, 500, )

            config = self._generate_config(zoom, lon, lat, boundary)

            named_map = self.get_named_map(config['layers'])
            image_urls.append('{cartodb_url}/api/v1/map/static/center/' \
                              '{layergroupid}/{zoom}/{center_lon}/{center_lat}/{x}/{y}.png'.format(
                                  cartodb_url=os.environ['CARTODB_URL'],
                                  layergroupid=named_map['layergroupid'],
                                  zoom=zoom,
                                  center_lon=lon,
                                  center_lat=lat,
                                  x=image_size[0],
                                  y=image_size[1],
                              ))

        url1 = image_urls.pop(0)
        LOGGER.info(url1)
        file1 = StringIO(requests.get(url1, stream=True).content)
        image1 = ImageOps.expand(Image.open(file1), border=10, fill='white')

        for url2 in image_urls:
            LOGGER.info(url2)
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

    def complete(self):
        '''
        If we support this country,
        '''
        country = self.measure.split('.')[0]
        if country in self.CENTER_ZOOM_BOUNDS:
            return super(ImagesForMeasure, self).complete()
        else:
            LOGGER.warn('No info to create images for %s', self.measure)
            return True

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
                            params={'config': json.dumps(config)}).json()
        if 'layergroupid' not in resp:
            raise Exception('Named map returned no layergroupid: {}'.format(
                pprint(resp)))
        return resp

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
        LOGGER.info(img_url)
        shell('curl "{img_url}" > {output}'.format(img_url=img_url,
                                                   output=self.output().path))

    def output(self):
        return LocalTarget(os.path.join('catalog/source/img', self.task_id + '.png'))


class GenerateThumb(Task):

    measure = Parameter(default=None)
    viz = Parameter(default=None)
    force = Parameter(default=False, significant=False)

    def requires(self):
        if self.viz and self.measure:
            raise Exception('Specify either viz or measure')
        elif self.viz:
            return GenerateStaticImage(viz=self.viz)  #TODO no force option for generatestaticimage
        elif self.measure:
            return ImagesForMeasure(measure=self.measure, force=self.force)
        else:
            raise Exception('Must specify viz or measure')

    def run(self):
        self.output().makedirs()
        img = Image.open(self.input().path)
        img.resize((img.size[0] / 2, img.size[1] / 2))
        img.save(self.output().path, format='JPEG', quality=75, optimized=True)

    def output(self):
        return LocalTarget(self.input().path.replace('/img/', '/img_thumb/'))


class PurgeMetadataTasks(Task):
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
                    stmt = 'DROP TABLE observatory.{tablename} CASCADE'.format(
                        tablename=tablename)
                    LOGGER.info(stmt)
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
                LOGGER.info(stmt)
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
                LOGGER.info('{table} exists'.format(table=table))
            else:
                # TODO drop table
                import pdb
                pdb.set_trace()
                LOGGER.info(table)
            yield PostgresTarget(schema='observatory', tablename=table.tablename)


class ConfirmTableExists(Task):
    '''
    Confirm a table exists
    '''

    schema = Parameter(default='observatory')
    tablename = Parameter()

    def run(self):
        raise Exception('Table {} does not exist'.format(self.tablename))

    def output(self):
        return PostgresTarget(self.schema, self.tablename)


class ConfirmTablesDescribedExist(WrapperTask):
    '''
    Confirm that all tables described in obs_table actually exist.
    '''

    def requires(self):
        session = current_session()
        for table in session.query(OBSTable):
            yield ConfirmTableExists(tablename=table.tablename)


class PurgeMetadata(WrapperTask):
    '''
    Purge local metadata that no longer has tasks linking to it
    '''

    def requires(self):
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


class Dump(Task):
    '''
    Dumps the entire ``observatory`` schema to a local file using the
    `binary <https://www.postgresql.org/docs/9.4/static/app-pgdump.html>`_
    Postgres dump format.

    Automatically updates :class:`~.meta.OBSDumpVersion`.

    :param timestamp: Optional date parameter, defaults to today.
    '''

    timestamp = DateParameter(default=date.today())

    def requires(self):
        yield OBSMetaToLocal()

    def run(self):
        session = current_session()
        try:
            self.output().makedirs()
            session.execute(
                'INSERT INTO observatory.obs_dump_version (dump_id) '
                "VALUES ('{task_id}')".format(task_id=self.task_id))
            session.commit()
            shell('pg_dump -Fc -Z0 -x -n observatory -f {output}'.format(
                output=self.output().path))
        except Exception as err:
            session.rollback()
            session.execute(
                'DELETE FROM observatory.obs_dump_version '
                "WHERE dump_id =  '{task_id}'".format(task_id=self.task_id))
            session.commit()
            raise err

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id + '.dump'))


class DumpS3(Task):
    '''
    Uploads ``observatory`` schema dumped from :class:`~.carto.Dump` to
    `Amazon S3 <https://aws.amazon.com/s3/>`_, using credentials from ``.env``.

    Automatically updates :class:`~.meta.OBSDumpVersion`.

    :param timestamp: Optional date parameter, defaults to today.
    '''
    timestamp = DateParameter(default=date.today())
    force = BooleanParameter(default=False, significant=False)

    def requires(self):
        return Dump(timestamp=self.timestamp)

    def run(self):
        shell('aws s3 cp {input} {output}'.format(
            input=self.input().path,
            output=self.output().path
        ))

    def output(self):
        path = self.input().path.replace('tmp/carto/Dump_', 'do-release-')
        path = path.replace('.dump', '/obs.dump')
        path = 's3://cartodb-observatory-data/{path}'.format(
            path=path
        )
        LOGGER.info(path)
        target = S3Target(path)
        if self.force:
            shell('aws s3 rm {output}'.format(
                output=path
            ))
            self.force = False
        return target


class OBSMeta(Task):

    force = BooleanParameter(default=True)

    FIRST_AGGREGATE = '''
    CREATE OR REPLACE FUNCTION public.first_agg ( anyelement, anyelement )
    RETURNS anyelement LANGUAGE SQL IMMUTABLE STRICT AS $$
            SELECT $1;
    $$;

    DROP AGGREGATE IF EXISTS public.FIRST (anyelement);
    CREATE AGGREGATE public.FIRST (
            sfunc    = public.first_agg,
            basetype = anyelement,
            stype    = anyelement
    );
    '''

    QUERIES = ['''
      CREATE TABLE {obs_meta} AS
      WITH denoms as (
        SELECT
             numer_c.id numer_id,
             denom_c.id denom_id,
             denom_t.id denom_tid,
             geomref_c.id geomref_id,
             null::varchar denom_name,
             null::varchar denom_description,
             null::varchar denom_t_description,
             null::varchar denom_aggregate,
             null::varchar denom_type,
             null::varchar denom_reltype,
             null::varchar denom_colname,
             FIRST(denom_geomref_ct.colname) denom_geomref_colname,
             null::varchar denom_tablename,
             FIRST(denom_t.timespan) denom_timespan,
             null::int as denom_weight,
             null::jsonb as denom_tags,
             null::jsonb denom_extra,
             null::jsonb denom_ct_extra
        FROM observatory.obs_column numer_c
             , observatory.obs_column_to_column denom_c2c
             , observatory.obs_column denom_c
             , observatory.obs_column_table denom_data_ct
             , observatory.obs_table denom_t
             , observatory.obs_column_tag denom_ctag
             , observatory.obs_tag denom_tag
             , observatory.obs_column_table denom_geomref_ct
             , observatory.obs_column geomref_c
             , observatory.obs_column_to_column geomref_c2c
        WHERE denom_c.weight > 0
          AND denom_c2c.source_id = numer_c.id
          AND denom_c2c.target_id = denom_c.id
          AND denom_data_ct.column_id = denom_c.id
          AND denom_data_ct.table_id = denom_t.id
          AND denom_c.id = denom_ctag.column_id
          AND denom_ctag.tag_id = denom_tag.id
          AND denom_c2c.reltype IN ('denominator', 'universe')
          AND denom_geomref_ct.table_id = denom_t.id
          AND denom_geomref_ct.column_id = geomref_c.id
          AND geomref_c2c.reltype = 'geom_ref'
          AND geomref_c2c.source_id = geomref_c.id
        GROUP BY numer_c.id, denom_c.id, denom_t.id, geomref_c.id
      ), leftjoined_denoms AS (
        SELECT numer_c.id all_numer_id, denoms.*
        FROM observatory.obs_column numer_c
             LEFT JOIN denoms ON numer_c.id = denoms.numer_id
      ) SELECT numer_c.id numer_id,
             denom_id,
             geom_c.id geom_id,
             FIRST(numer_t.id) numer_tid,
             FIRST(denom_tid) denom_tid,
             FIRST(geom_t.id ORDER BY geom_t.timespan DESC) geom_tid,
             null::varchar numer_name,
             null::varchar denom_name,
             null::varchar geom_name,
             null::varchar numer_description,
             null::varchar denom_description,
             null::varchar geom_description,
             null::varchar numer_t_description,
             null::varchar denom_t_description,
             null::varchar geom_t_description,
             null::varchar numer_aggregate,
             null::varchar denom_aggregate,
             null::varchar geom_aggregate,
             null::varchar numer_type,
             null::varchar denom_type,
             null::varchar denom_reltype,
             null::varchar geom_type,
             null::varchar numer_colname,
             null::varchar denom_colname,
             null::varchar geom_colname,
             FIRST(numer_geomref_ct.colname) numer_geomref_colname,
             FIRST(denom_geomref_colname) denom_geomref_colname,
             FIRST(geom_geomref_ct.colname ORDER BY geom_t.timespan DESC) geom_geomref_colname,
             null::varchar numer_tablename,
             null::varchar denom_tablename,
             null::varchar geom_tablename,
             numer_t.timespan numer_timespan,
             null::varchar denom_timespan,
             null::numeric numer_weight,
             null::numeric denom_weight,
             null::numeric geom_weight,
             null::varchar geom_timespan,
             null::geometry the_geom,
             null::jsonb numer_tags,
             null::jsonb denom_tags,
             null::jsonb geom_tags,
             null::jsonb timespan_tags,
             null::varchar[] section_tags,
             null::varchar[] subsection_tags,
             null::varchar[] unit_tags,
             null::jsonb numer_extra ,
             null::jsonb numer_ct_extra ,
             null::jsonb denom_extra,
             null::jsonb denom_ct_extra,
             null::jsonb geom_extra,
             null::jsonb geom_ct_extra
      FROM observatory.obs_column_table numer_data_ct,
           observatory.obs_table numer_t,
           observatory.obs_column_table numer_geomref_ct,
           observatory.obs_column geomref_c,
           observatory.obs_column_to_column geomref_c2c,
           observatory.obs_column_table geom_geom_ct,
           observatory.obs_column_table geom_geomref_ct,
           observatory.obs_table geom_t,
           observatory.obs_column_tag numer_ctag,
           observatory.obs_tag numer_tag,
           observatory.obs_column numer_c,
           leftjoined_denoms,
           observatory.obs_column geom_c
           LEFT JOIN (
              observatory.obs_column_tag geom_ctag JOIN
              observatory.obs_tag geom_tag ON geom_tag.id = geom_ctag.tag_id
           ) ON geom_c.id = geom_ctag.column_id
      WHERE numer_c.weight > 0
        AND numer_c.id = numer_data_ct.column_id
        AND numer_data_ct.table_id = numer_t.id
        AND numer_t.id = numer_geomref_ct.table_id
        AND numer_geomref_ct.column_id = geomref_c.id
        AND geomref_c2c.reltype = 'geom_ref'
        AND geomref_c.id = geomref_c2c.source_id
        AND geom_c.id = geomref_c2c.target_id
        AND geom_geomref_ct.column_id = geomref_c.id
        AND geom_geomref_ct.table_id = geom_t.id
        AND geom_geom_ct.column_id = geom_c.id
        AND geom_geom_ct.table_id = geom_t.id
        AND geom_c.type ILIKE 'geometry%'
        AND numer_c.type NOT ILIKE 'geometry%'
        AND numer_c.id != geomref_c.id
        AND numer_ctag.column_id = numer_c.id
        AND numer_ctag.tag_id = numer_tag.id
        AND numer_c.id = leftjoined_denoms.all_numer_id
        AND (leftjoined_denoms.numer_id IS NULL OR (
          numer_t.timespan = leftjoined_denoms.denom_timespan
          AND geomref_c.id = leftjoined_denoms.geomref_id
        ))
      GROUP BY numer_c.id, denom_id, geom_c.id, numer_t.timespan;
      ''',

      '''CREATE UNIQUE INDEX ON {obs_meta} (numer_id, geom_id, numer_timespan, denom_id);''',

      '''-- update numer coltable info
      UPDATE {obs_meta} SET
        numer_name = name,
        numer_description = c.description,
        numer_t_description = t.description,
        numer_aggregate = aggregate,
        numer_type = type,
        numer_colname = colname,
        numer_tablename = tablename,
        numer_timespan = timespan,
        numer_weight = weight,
        numer_extra = c.extra,
        numer_ct_extra = ct.extra
      FROM observatory.obs_column c, observatory.obs_column_table ct, observatory.obs_table t
      WHERE c.id = numer_id
        AND t.id = numer_tid
        AND c.id = ct.column_id
        AND t.id = ct.table_id;''',

      '''-- update denom coltable info
      UPDATE {obs_meta} SET
        denom_name = name,
        denom_description = c.description,
        denom_t_description = t.description,
        denom_aggregate = aggregate,
        denom_type = type,
        denom_colname = colname,
        denom_tablename = tablename,
        denom_timespan = timespan,
        denom_weight = weight,
        denom_extra = c.extra,
        denom_ct_extra = ct.extra
      FROM observatory.obs_column c, observatory.obs_column_table ct, observatory.obs_table t
      WHERE c.id = denom_id
        AND t.id = denom_tid
        AND c.id = ct.column_id
        AND t.id = ct.table_id;''',

     '''-- update geom coltable info
     UPDATE {obs_meta} SET
       geom_name = name,
       geom_description = c.description,
       geom_t_description = t.description,
       geom_aggregate = aggregate,
       geom_type = type,
       geom_colname = colname,
       geom_tablename = tablename,
       geom_timespan = timespan,
       the_geom = t.the_geom,
       geom_weight = weight,
       geom_extra = c.extra,
       geom_ct_extra = ct.extra
     FROM observatory.obs_column c, observatory.obs_column_table ct, observatory.obs_table t
     WHERE c.id = geom_id
       AND t.id = geom_tid
       AND c.id = ct.column_id
       AND t.id = ct.table_id;''',

     '''-- update coltag info
     DROP TABLE IF EXISTS _obs_coltags;
     CREATE TEMPORARY TABLE _obs_coltags AS
     SELECT
       c.id,
       JSONB_OBJECT_AGG(
         t.type || '/' || t.id, t.name
       ) tags,
       ARRAY_AGG(DISTINCT t.id) FILTER (WHERE t.type = 'section') section_tags,
       ARRAY_AGG(DISTINCT t.id) FILTER (WHERE t.type = 'subsection') subsection_tags,
       ARRAY_AGG(DISTINCT t.id) FILTER (WHERE t.type = 'unit') unit_tags
     FROM observatory.obs_column c, observatory.obs_column_tag ct, observatory.obs_tag t
     WHERE c.id = ct.column_id
       AND t.id = ct.tag_id
     GROUP BY c.id;
     CREATE UNIQUE INDEX ON _obs_coltags (id);''',

     '''UPDATE {obs_meta} SET
       numer_tags = tags,
       section_tags = _obs_coltags.section_tags,
       subsection_tags = _obs_coltags.subsection_tags,
       unit_tags = _obs_coltags.unit_tags
     FROM _obs_coltags WHERE id = numer_id;''',

     '''UPDATE {obs_meta} SET
       geom_tags = tags
     FROM _obs_coltags WHERE id = geom_id;''',

     '''UPDATE {obs_meta} SET
       denom_tags = tags
     FROM _obs_coltags WHERE id = denom_id;''',

     '''-- update denom reltype info
     UPDATE {obs_meta} SET
       denom_reltype = c2c.reltype
     FROM observatory.obs_column source,
          observatory.obs_column_to_column c2c,
          observatory.obs_column target
     WHERE c2c.source_id = source.id
       AND c2c.target_id = target.id
       AND source.id = numer_id
       AND target.id = denom_id;
    ''',
    '''CREATE INDEX ON {obs_meta} USING gist (the_geom)''',
    '''CREATE INDEX ON {obs_meta} USING gin (numer_tags)'''
    ]

    DIMENSIONS = {
        # before took 206s
        'numer': ['''
CREATE TABLE {obs_meta} AS
SELECT numer_id ,
         NULL::TEXT numer_name, --FIRST(numer_description)::TEXT numer_name,
         NULL::TEXT numer_description, --FIRST(numer_description)::TEXT numer_description,
         NULL::JSONB numer_tags, --FIRST(numer_tags)::JSONB numer_tags,
         NULL::NUMERIC numer_weight, --FIRST(numer_weight)::NUMERIC numer_weight,
         NULL::JSONB numer_extra, --FIRST(numer_extra)::JSONB numer_extra,
         NULL::TEXT numer_type, --FIRST(numer_type)::TEXT numer_type,
         NULL::TEXT numer_aggregate, --FIRST(numer_aggregate)::TEXT numer_aggregate,
         ARRAY_AGG(DISTINCT denom_id)::TEXT[] denoms,
         ARRAY_AGG(DISTINCT geom_id)::TEXT[] geoms,
         ARRAY_AGG(DISTINCT numer_timespan)::TEXT[] timespans,
         NULL::Geometry(Geometry, 4326) the_geom -- ST_Union(DISTINCT ST_SetSRID(the_geom, 4326)) the_geom
FROM observatory.obs_meta_next
GROUP BY numer_id;
        ''',
        ''' ALTER TABLE {obs_meta} ADD PRIMARY KEY (numer_id); ''',
        '''
UPDATE {obs_meta} SET
numer_name = obs_meta.numer_name,
numer_description = obs_meta.numer_description,
numer_tags = obs_meta.numer_tags,
numer_weight = obs_meta.numer_weight,
numer_extra = obs_meta.numer_extra,
numer_type = obs_meta.numer_type,
numer_aggregate = obs_meta.numer_aggregate
FROM observatory.obs_meta_next obs_meta
WHERE obs_meta.numer_id = {obs_meta}.numer_id;
        ''',
        '''CREATE INDEX ON observatory.obs_meta_next (numer_id, geom_tid); ''',
        '''
WITH geom_tids AS (
  SELECT  ARRAY_AGG(distinct geom_tid) geom_tids, numer_id
  FROM observatory.obs_meta_next
  GROUP BY numer_id
), unique_geom_ids AS (
  SELECT ARRAY_AGG(distinct numer_id) numer_ids, geom_tids
  FROM geom_tids
  GROUP BY geom_tids
), union_geoms AS (
  SELECT numer_ids, geom_tids, ST_Union(the_geom) the_geom
  FROM unique_geom_ids, observatory.obs_table
  WHERE id = ANY(geom_tids)
  GROUP BY numer_ids, geom_tids
) UPDATE {obs_meta}
SET the_geom = union_geoms.the_geom
FROM union_geoms
WHERE {obs_meta}.numer_id = ANY(union_geoms.numer_ids);
        '''
        ],
        'denom': ['''
CREATE UNIQUE INDEX ON observatory.obs_meta_next (denom_id, numer_id, geom_id, numer_timespan, denom_timespan);
        ''',
        '''
CREATE TABLE {obs_meta} AS
SELECT denom_id::TEXT,
         NULL::TEXT denom_name, --FIRST(denom_name)::TEXT denom_name,
         NULL::TEXT denom_description, --FIRST(denom_description)::TEXT denom_description,
         NULL::JSONB denom_tags, --FIRST(denom_tags)::JSONB denom_tags,
         NULL::NUMERIC denom_weight, --FIRST(denom_weight)::NUMERIC denom_weight,
         NULL::TEXT reltype, --'denominator'::TEXT reltype,
         NULL::JSONB denom_extra, --FIRST(denom_extra)::JSONB denom_extra,
         NULL::TEXT denom_type, --FIRST(denom_type)::TEXT denom_type,
         NULL::TEXT denom_aggregate, --FIRST(denom_aggregate)::TEXT denom_aggregate,
         ARRAY_AGG(DISTINCT numer_id)::TEXT[] numers,
         ARRAY_AGG(DISTINCT geom_id)::TEXT[] geoms,
         ARRAY_AGG(DISTINCT denom_timespan)::TEXT[] timespans,
         NULL::Geometry(Geometry, 4326) the_geom -- ST_Union(DISTINCT ST_SetSRID(the_geom, 4326)) the_geom
FROM observatory.obs_meta_next
WHERE denom_id IS NOT NULL
GROUP BY denom_id;
        ''',
        '''
ALTER TABLE {obs_meta} ADD PRIMARY KEY (denom_id);
        ''',
        '''
UPDATE {obs_meta} SET
denom_name = obs_meta.denom_name,
denom_description = obs_meta.denom_description,
denom_tags = obs_meta.denom_tags,
denom_weight = obs_meta.denom_weight,
reltype = obs_meta.denom_reltype,
denom_extra = obs_meta.denom_extra,
denom_type = obs_meta.denom_type,
denom_aggregate = obs_meta.denom_aggregate
FROM observatory.obs_meta_next obs_meta
WHERE obs_meta.denom_id = {obs_meta}.denom_id;
        ''',
        '''CREATE INDEX ON observatory.obs_meta_next (denom_id, geom_tid); ''',
        '''
WITH geom_tids AS (
  SELECT  ARRAY_AGG(geom_tid) geom_tids, numer_id
  FROM observatory.obs_meta_next
  GROUP BY numer_id
) , unique_geom_ids AS (
  SELECT ARRAY_AGG(numer_id) numer_ids, geom_tids
  FROM geom_tids
  GROUP BY geom_tids
), union_geoms AS (
  SELECT numer_ids, geom_tids, ST_Union(the_geom) the_geom
  FROM unique_geom_ids, observatory.obs_table
  WHERE id = ANY(geom_tids)
  GROUP BY numer_ids, geom_tids
) UPDATE {obs_meta}
SET the_geom = union_geoms.the_geom
FROM union_geoms
WHERE {obs_meta}.denom_id = ANY(union_geoms.numer_ids);
        '''
        ],
        'geom': [
        ''' CREATE UNIQUE INDEX ON observatory.obs_meta_next
        (geom_id, numer_id, numer_timespan, geom_timespan, denom_id);
        ''',
        '''
CREATE TABLE {obs_meta} AS
SELECT geom_id::TEXT,
         NULL::TEXT geom_name, --FIRST(geom_name)::TEXT geom_name,
         NULL::TEXT geom_description, --FIRST(geom_description)::TEXT geom_description,
         NULL::JSONB geom_tags, --FIRST(geom_tags)::JSONB geom_tags,
         NULL::NUMERIC geom_weight, --FIRST(geom_weight)::NUMERIC geom_weight,
         NULL::JSONB geom_extra, --FIRST(geom_extra)::JSONB geom_extra,
         NULL::TEXT geom_type, --FIRST(geom_type)::TEXT geom_type,
         NULL::TEXT geom_aggregate, --FIRST(geom_aggregate)::TEXT geom_aggregate
         NULL::Geometry(Geometry, 4326) the_geom, --ST_SetSRID(FIRST(the_geom), 4326)::GEOMETRY(GEOMETRY, 4326) the_geom,
         ARRAY_AGG(DISTINCT numer_id)::TEXT[] numers,
         ARRAY_AGG(DISTINCT denom_id)::TEXT[] denoms,
         ARRAY_AGG(DISTINCT geom_timespan)::TEXT[] timespans
  FROM observatory.obs_meta_next
  GROUP BY geom_id;
        ''',
        ''' ALTER TABLE {obs_meta} ADD PRIMARY KEY (geom_id); ''',
        '''
UPDATE {obs_meta} SET
geom_name = obs_meta.geom_name,
geom_description = obs_meta.geom_description,
geom_tags = obs_meta.geom_tags,
geom_weight = obs_meta.geom_weight,
geom_extra = obs_meta.geom_extra,
geom_type = obs_meta.geom_type,
geom_aggregate = obs_meta.geom_aggregate
FROM observatory.obs_meta_next obs_meta
WHERE obs_meta.geom_id = {obs_meta}.geom_id;
        ''',
        '''
WITH geom_tids AS (
  SELECT  ARRAY_AGG(geom_tid) geom_tids, geom_id
  FROM observatory.obs_meta_next
  GROUP BY geom_id
) , unique_geom_ids AS (
  SELECT ARRAY_AGG(geom_id) geom_ids, geom_tids
  FROM geom_tids
  GROUP BY geom_tids
), union_geoms AS (
  SELECT geom_ids, geom_tids, ST_Union(the_geom) the_geom
  FROM unique_geom_ids, observatory.obs_table
  WHERE id = ANY(geom_tids)
  GROUP BY geom_ids, geom_tids
) UPDATE {obs_meta}
SET the_geom = union_geoms.the_geom
FROM union_geoms
WHERE {obs_meta}.geom_id = ANY(union_geoms.geom_ids);
        '''
        ],
        'timespan': ['''
CREATE TABLE {obs_meta} AS
SELECT numer_timespan::TEXT timespan_id,
       numer_timespan::TEXT timespan_name,
       NULL::TEXT timespan_description,
       NULL::JSONB timespan_tags, --FIRST(timespan_tags)::JSONB timespan_tags,
       NULL::NUMERIC timespan_weight,
       NULL::JSONB timespan_extra,
       NULL::TEXT timespan_type,
       NULL::TEXT timespan_aggregate,
       ARRAY_AGG(DISTINCT numer_id)::TEXT[] numers,
       ARRAY_AGG(DISTINCT denom_id)::TEXT[] denoms,
       ARRAY_AGG(DISTINCT geom_id)::TEXT[] geoms,
       NULL::Geometry(Geometry, 4326) the_geom --, ST_Union(DISTINCT ST_SetSRID(the_geom, 4326)) the_geom
FROM observatory.obs_meta_next
GROUP BY numer_timespan;
        ''',
        '''
ALTER TABLE {obs_meta} ADD PRIMARY KEY (timespan_id);
        ''',
        '''
UPDATE {obs_meta} SET
timespan_tags = obs_meta.timespan_tags
FROM observatory.obs_meta_next obs_meta
WHERE obs_meta.numer_timespan = {obs_meta}.timespan_id;
        ''',
        '''
WITH geom_tids AS (
  SELECT  ARRAY_AGG(geom_tid) geom_tids, numer_timespan
  FROM observatory.obs_meta_next
  GROUP BY numer_timespan
) , unique_geom_ids AS (
  SELECT ARRAY_AGG(numer_timespan) numer_timespans, geom_tids
  FROM geom_tids
  GROUP BY geom_tids
), union_geoms AS (
  SELECT numer_timespans, geom_tids, ST_Union(the_geom) the_geom
  FROM unique_geom_ids, observatory.obs_table
  WHERE id = ANY(geom_tids)
  GROUP BY numer_timespans, geom_tids
) UPDATE {obs_meta}
SET the_geom = union_geoms.the_geom
FROM union_geoms
WHERE {obs_meta}.timespan_id = ANY(union_geoms.numer_timespans);
        ''']
    }


class DropRemoteOrphanTables(Task):
    '''
    Clean up & remove tables that are not linked to in the deployed obs_table.
    '''

    start = IntParameter(default=1)
    end = IntParameter(default=10)

    def run(self):

        resp = query_cartodb('SELECT tablename FROM obs_table')
        tablenames = set([r['tablename'] for r in resp.json()['rows']])
        remote_tables = []
        for page in xrange(self.start, self.end + 1):
            remote_tables.extend(shell("curl -s '{cartodb_url}/datasets?page={page}' "
                                       "| grep -Eo 'obs_[0-f]{{40}}' | uniq".format(
                                           cartodb_url=os.environ['CARTODB_URL'],
                                           page=page
                                       )).strip().split('\n'))
        for table in remote_tables:
            LOGGER.info('keeping %s', table)
            if table not in tablenames:
                LOGGER.info('removing %s', table)
                try:
                    CartoDBTarget(table).remove()
                except Exception as err:
                    LOGGER.warn(err)


class OBSMetaToLocal(OBSMeta):

    def requires(self):
        yield ConfirmTablesDescribedExist()

    def run(self):
        session = current_session()
        try:
            session.execute('DROP TABLE IF EXISTS observatory.obs_meta_next')
            session.execute(self.FIRST_AGGREGATE)
            for i, q in enumerate(self.QUERIES):
                before = time.time()
                query = q.format(obs_meta='observatory.obs_meta_next')
                #LOGGER.info(query)
                session.execute(query)
                after = time.time()
                LOGGER.info('time taken for obs_meta:%s: %s', i, round(after - before, 2))
                if i == 1:
                    session.commit()
            session.commit()
        except:
            session.rollback()
            raise

        shell("psql -c 'VACUUM ANALYZE observatory.obs_meta_next'")

        try:
            for dimension, queries in self.DIMENSIONS.iteritems():
                before = time.time()
                session.execute('DROP TABLE IF EXISTS observatory.obs_meta_next_{dimension}'.format(
                    dimension=dimension))
                for i, q in enumerate(queries):
                    before = time.time()
                    query = q.format(obs_meta='observatory.obs_meta_next_{}'.format(dimension))
                    #LOGGER.info(query)
                    session.execute(query)
                    session.flush()
                    after = time.time()
                    LOGGER.info('time taken for %s:%s: %s', dimension, i, round(after - before, 2))
                #session.execute('CREATE TABLE observatory.obs_meta_next_{dimension} '
                #                'AS {select}'.format(
                #                    dimension=dimension,
                #                    select=query
                #                ))
                session.execute('CREATE INDEX ON observatory.obs_meta_next_{dimension} USING gist '
                                '(the_geom)'.format(dimension=dimension))
                after = time.time()
            session.commit()
        except:
            session.rollback()
            session.execute('DROP TABLE IF EXISTS observatory.obs_meta_next')
            session.commit()
            raise

        try:
            session.execute('DROP TABLE IF EXISTS observatory.obs_meta')
            session.execute('ALTER TABLE observatory.obs_meta_next RENAME TO obs_meta')
            for dimension, query in self.DIMENSIONS.iteritems():
                session.execute('DROP TABLE IF EXISTS observatory.obs_meta_{dimension}'.format(
                    dimension=dimension
                ))
                session.execute('''
                    ALTER TABLE IF EXISTS observatory.obs_meta_next_{dimension}
                    RENAME TO obs_meta_{dimension}'''.format(
                        dimension=dimension
                    ))
            session.commit()
            self._complete = True
        except:
            session.rollback()
            session.execute('DROP TABLE IF EXISTS observatory.obs_meta_next')
            session.commit()
            raise

    def complete(self):
        return getattr(self, '_complete', False)


class SyncMetadata(WrapperTask):

    no_force = BooleanParameter(default=False, significant=False)

    def requires(self):
        for table in ('obs_table', 'obs_column', 'obs_column_table',
                      'obs_tag', 'obs_column_tag', 'obs_dump_version',
                      'obs_column_to_column', 'obs_meta', 'obs_meta_numer',
                      'obs_meta_denom', 'obs_meta_geom', 'obs_meta_timespan',
                      'obs_column_table_tile',
                     ):
            if table == 'obs_meta':
                yield TableToCartoViaImportAPI(
                    columns=[
                        'numer_id', 'denom_id', 'geom_id', 'numer_name',
                        'denom_name', 'geom_name', 'numer_description',
                        'denom_description', 'geom_description',
                        'numer_aggregate', 'denom_aggregate', 'geom_aggregate',
                        'numer_type', 'denom_type', 'geom_type', 'numer_colname',
                        'denom_colname', 'geom_colname', 'numer_geomref_colname',
                        'denom_geomref_colname', 'geom_geomref_colname',
                        'numer_tablename', 'denom_tablename', 'geom_tablename',
                        'numer_timespan', 'denom_timespan', 'numer_weight',
                        'denom_weight', 'geom_weight', 'geom_timespan',
                        'numer_tags', 'denom_tags', 'geom_tags', 'timespan_tags',
                        'section_tags', 'subsection_tags', 'unit_tags',
                        'numer_extra', 'numer_ct_extra', 'denom_extra',
                        'denom_ct_extra', 'geom_extra', 'geom_ct_extra'
                    ],
                    table=table,
                    force=not self.no_force)
            else:
                yield TableToCartoViaImportAPI(table=table, force=not self.no_force)
