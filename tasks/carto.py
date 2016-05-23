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

    def requires(self):
        session = current_session()
        if self.exact_id:
            table = session.query(OBSTable).get(self.exact_id)
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

    measure = Parameter()
    lon = FloatParameter()
    lat = FloatParameter()
    #bounds = Parameter()
    zoom = IntParameter()

    def _generate_config(self):
        layers = []
        layers.append(self.BASEMAP)
        session = current_session()
        query = '''
SELECT data_t.timespan,
       (data_ct.extra->'stats'->>'stddev')::NUMERIC "stddev",
       (data_ct.extra->'stats'->>'avg')::NUMERIC "avg",
       (data_ct.extra->'stats'->>'min')::NUMERIC "min",
       (data_ct.extra->'stats'->>'max')::NUMERIC "max",
       data_ct.colname as data_colname, data_geoid_ct.colname data_geoid_colname,
       data_t.tablename as data_tablename,
       geom_geoid_ct.colname geom_geoid_colname,
       geom_ct.colname geom_geom_colname, geom_t.tablename as geom_tablename
       --target_c.weight, target_c.id
FROM observatory.obs_column source_c,
     observatory.obs_column target_c,
     observatory.obs_column_to_column c2c,
     observatory.obs_column_table data_ct,
     observatory.obs_column_table data_geoid_ct,
     observatory.obs_column_table geom_geoid_ct,
     observatory.obs_column_table geom_ct,
     observatory.obs_table geom_t,
     observatory.obs_table data_t
WHERE source_c.id = data_geoid_ct.column_id
  AND source_c.id = geom_geoid_ct.column_id
  AND data_ct.column_id = '{measure}'
  AND data_ct.table_id = data_t.id
  AND data_geoid_ct.table_id = data_t.id
  AND geom_geoid_ct.table_id = geom_t.id
  AND geom_ct.table_id = geom_t.id
  AND geom_ct.column_id = target_c.id
  AND c2c.target_id = target_c.id
  AND c2c.source_id = source_c.id
  AND c2c.reltype = 'geom_ref'
  AND target_c.type ILIKE 'geometry'
  AND (data_ct.extra->'stats'->>'avg')::NUMERIC IS NOT NULL
  AND (data_ct.extra->'stats'->>'stddev')::NUMERIC IS NOT NULL
  AND (data_ct.extra->'stats'->>'min')::NUMERIC IS NOT NULL
  AND (data_ct.extra->'stats'->>'max')::NUMERIC IS NOT NULL
ORDER BY target_c.weight DESC, data_t.timespan DESC, geom_ct.column_id DESC;
'''.format(measure=self.measure)
        resp = session.execute(query)
        timespan, stddev, avg, min, max, data_data_colname, data_geoid_colname, \
                data_tablename, geom_geoid_colname, \
                geom_geom_colname, geom_tablename = resp.fetchone()
        calcmax = avg + (stddev * 3)
        max = max if max < calcmax else calcmax
        calcmin = avg - (stddev * 3)
        min = min if min > calcmin else calcmin

        cartosql = "SELECT geom.cartodb_id, geom.{geom_geom_colname} as the_geom, " \
                "geom.the_geom_webmercator, data.{data_data_colname} measure " \
                "FROM {geom_tablename} as geom, {data_tablename} as data " \
                "WHERE geom.{geom_geoid_colname} = data.{data_geoid_colname} ".format(
                    geom_geom_colname=geom_geom_colname,
                    data_data_colname=data_data_colname,
                    geom_tablename=geom_tablename,
                    data_tablename=data_tablename,
                    geom_geoid_colname=geom_geoid_colname,
                    data_geoid_colname=data_geoid_colname)
        layers.append({
            'type': 'mapnik',
            'options': {
                'layer_name': data_tablename,
                'cartocss': '''/** choropleth visualization */

/*
@1:#045275;
@2:#00718b;
@3:#089099;
@4:#46aea0;
@5:#7ccba2;
@6:#b7e6a5;
@7:#f7feae;
*/
/*
@1:#442D6B;
@2:#993291;
@3:#C53986;
@4:#D55C60;
@5:#EDAB77;
@6:#FADF9D;
@7:#FEF9CC;
*/
/*
@1:#7A003B;
@2:#921b45;
@3:#af3051;
@4:#c64d5d;
@5:#d9726c;
@6:#eda18b;
@7:#fdcdae;
*/

@1:#324546;
@2:#4e5e52;
@3:#6e7f61;
@4:#909e74;
@5:#b4bc89;
@6:#DAD59F;
@7:#f9ebb2;

/*
@1:#443F7A;
@2:#68578D;
@3:#8A71A3;
@4:#AB8CB9;
@5:#CCA9D0;
@6:#EBC7E8;
@7:#FFE6FF;
*/

#data {{
  polygon-opacity: 0.9;
  line-color: transparent;
  line-width: 0.5;
  line-opacity: 1;

  [measure=null]{{
    polygon-fill: lightgray;
    polygon-pattern-file: url(http://com.cartodb.users-assets.production.s3.amazonaws.com/patterns/diagonal_1px_med.png);
    polygon-pattern-opacity: 0.2;
    polygon-opacity: 0;
  }}

  [measure <= {range7}] {{
     polygon-fill: @1;
     line-color: lighten(@1,5);
  }}
  [measure <= {range6}] {{
     polygon-fill: @2;
     line-color: lighten(@2,5);
  }}
  [measure <= {range5}] {{
     polygon-fill: @3;
     line-color: lighten(@3,5);
  }}
  [measure <= {range4}] {{
     polygon-fill: @4;
     line-color: lighten(@4,5);
  }}
  [measure <= {range3}] {{
     polygon-fill: @5;
     line-color: lighten(@5,5);
  }}
  [measure <= {range2}] {{
     polygon-fill: @6;
     line-color: lighten(@6,5);
  }}
  [measure <= {range1}] {{
     polygon-fill: @7;
     line-color: lighten(@7,5);
  }}
}}'''.format(
    range1=min,
    range2=min + ((avg - min) / Decimal(3.0)),
    range3=min + ((avg - min) * Decimal(2.0)/Decimal(3.0)),
    range4=avg,
    range5=avg + ((max - avg) / Decimal(3.0)),
    range6=avg + ((max - avg) * Decimal(2.0)/Decimal(3.0)),
    range7=max),
                'cartocss_version': "2.1.1",
                'sql': cartosql,
                "table_name": "\"\"."
            }
        })
        layers.append(self.LABELS)
        return {
            'layers': layers,
            'center': [self.lon, self.lat],
            #'bounds': self.bounds,
            'zoom': self.zoom
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
        config = self._generate_config()
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
        return LocalTarget(os.path.join('catalog/source/img', self.measure + '.png'))


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
