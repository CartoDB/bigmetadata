import os
from luigi import FloatParameter, Task
from lib.logger import get_logger
from tasks.targets import PostgresTarget, LocalTarget
from tasks.util import shell
from tasks.meta import current_session
LOGGER = get_logger(__name__)


def execute_ch_query(query):
    cmd = 'clickhouse-client --host {host} --query="{query}";'.format(
        host='clickhouse-server',
        query=query,
    )
    return shell(cmd)


def iterate_point(minx, miny, maxx, maxy, increment):
        currentx = minx
        currenty = miny
        while currentx < maxx:
            while currenty < maxy:
                yield currentx, currenty
                currenty = currenty + increment
            currenty = miny
            currentx = currentx + increment


class CreateGrid(Task):
    minx = FloatParameter()
    miny = FloatParameter()
    maxx = FloatParameter()
    maxy = FloatParameter()
    increment = FloatParameter()

    def run(self):
        session = current_session()

        query = '''
                CREATE SCHEMA IF NOT EXISTS "{schema}";
                '''.format(
                    schema=self.output().schema
                )
        session.execute(query)
        session.commit()

        query = '''
                CREATE TABLE "{schema}".{table} (
                    tot_pop numeric,
                    score numeric,
                    the_geom geometry(geometry,4326)
                )
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                )
        session.execute(query)
        session.commit()

        i = 0
        for point in iterate_point(self.minx, self.miny, self.maxx, self.maxy, self.increment):
            query = '''
                    INSERT INTO "{schema}".{table} (the_geom)
                    SELECT ST_SetSRID(ST_MakePoint({x}, {y}), 4326)
                    '''.format(
                        schema=self.output().schema,
                        table=self.output().tablename,
                        x=point[0],
                        y=point[1],
                    )
            session.execute(query)
            if i % 100000 == 0:
                LOGGER.error('Inserted: {}'.format(i))
                session.commit()

            i += 1
        session.commit()

        query = '''
                CREATE INDEX grid_idx ON "{schema}".{table} USING GIST (the_geom);
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                )
        session.execute(query)
        session.commit()

        query = '''
                CLUSTER "{schema}".{table} USING grid_idx;
                '''.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                )
        session.execute(query)
        session.commit()

    def output(self):
        return PostgresTarget('leapfrog', 'grid')


class UpdateGrid(Task):
    minx = FloatParameter()
    miny = FloatParameter()
    maxx = FloatParameter()
    maxy = FloatParameter()
    increment = FloatParameter()

    def requires(self):
        return CreateGrid(minx=self.minx, miny=self.miny,
                          maxx=self.maxx, maxy=self.maxy,
                          increment=self.increment)

    def _mock(self, session):
        query = '''
                UPDATE "{schema}".{table} SET tot_pop = round((random() * 5)::numeric, 2), score = round((random() * 200)::numeric, 2)
                '''.format(schema=self.output().schema,
                           table=self.output().tablename)
        session.execute(query)
        session.commit()

    def _from_DO(self, session):
        query = '''
                UPDATE "{schema}".{table} p SET (tot_pop, score) = (
                    select a.total_pop / b.num, a.score from
                    (
                        select total_pop, random() * 10 score
                        from observatory.obs_33cabe86372ca1577bc7d80917f68f8e90f17fe0 geo, --shoreline clip block
                             observatory.obs_904e770b77c9b3dc25ab96b0c47405c757617d86 data
                        where geo.geoid = data.geoidsc
                        and ST_Contains(p.the_geom, geo.the_geom)
                    ) a,
                    (
                        select count(*) num
                        from observatory.obs_33cabe86372ca1577bc7d80917f68f8e90f17fe0 geo,
                             leapfrog.grid grid
                        where ST_Contains(grid.the_geom, geo.the_geom)
                    ) b
                )
                '''.format(schema=self.output().schema,
                           table=self.output().tablename)
        session.execute(query)
        session.commit()

    def run(self):
        session = current_session()
        self._mock(session)

#    def complete(self):
#        session = current_session()
#        query = '''
#                SELECT 1
#                FROM "{schema}".{table}
#                WHERE tot_pop IS NOT NULL
#                LIMIT 1;
#                '''.format(schema=self.output().schema,
#                           table=self.output().tablename)
#        return len(session.execute(query).fetchall()[0]) > 0

    def output(self):
        return PostgresTarget('leapfrog', 'grid')


class ExportGridFromPG(Task):
    minx = FloatParameter()
    miny = FloatParameter()
    maxx = FloatParameter()
    maxy = FloatParameter()
    increment = FloatParameter()

    def requires(self):
        return UpdateGrid(minx=self.minx, miny=self.miny,
                          maxx=self.maxx, maxy=self.maxy,
                          increment=self.increment)

    def run(self):
        session = current_session()
        with session.connection().connection.cursor() as cursor:
            with open(self.output().path, 'a') as file:
                # COPY (SELECT st_x(the_geom) lon, st_y(the_geom) lat, tot_pop, score, to_char(now(), 'YYYY-MM-DD') the_date
                #              FROM "{schema}".{table}) TO STDOUT WITH CSV
                query = '''
                        COPY (SELECT st_x(the_geom) lon, st_y(the_geom) lat, to_char(now(), 'YYYY-MM-DD') the_date
                              FROM "{schema}".{table}) TO STDOUT WITH CSV
                        '''.format(
                            schema=self.input().schema,
                            table=self.input().tablename,
                        )
                cursor.copy_expert(query, file)

    def output(self):
        return LocalTarget(os.path.join('tmp', 'leapfrog_grid.csv'))


class ImportGridToCH(Task):
    minx = FloatParameter()
    miny = FloatParameter()
    maxx = FloatParameter()
    maxy = FloatParameter()
    increment = FloatParameter()

    def requires(self):
        return ExportGridFromPG(minx=self.minx, miny=self.miny,
                                maxx=self.maxx, maxy=self.maxy,
                                increment=self.increment)

    def _create_table(self):
        query = '''
                CREATE DATABASE IF NOT EXISTS "{database}";
                '''.format(
                    database='leapfrog'
                )
        execute_ch_query(query)

        query = '''
                CREATE TABLE IF NOT EXISTS {database}.{table} (
                    lon Float64,
                    lat Float64,
                    tot_pop Float64 DEFAULT 2,
                    score Float64 DEFAULT 200,
                    date Date
                ) ENGINE = MergeTree(date, (lon, lat, date), 8192)
                '''.format(
                    database='leapfrog',
                    table='grid',
                )
        execute_ch_query(query)

    def _import_data(self):
        cmd = 'cat {file} | clickhouse-client --host {host} --query="INSERT INTO {database}.{table}(lon, lat, date) FORMAT CSV"'.format(
            file=self.input().path,
            host='clickhouse-server',
            database='leapfrog',
            table='grid',
        )
        return shell(cmd)

    def run(self):
        self._create_table()
        self._import_data()

    def complete(self):
        query = '''
                EXISTS TABLE  {database}.{table}
                '''.format(
                    database='leapfrog',
                    table='grid',
                )
        exists = execute_ch_query(query)
        return exists == '1'


class FakeCSV(Task):
    minx = FloatParameter()
    miny = FloatParameter()
    maxx = FloatParameter()
    maxy = FloatParameter()
    increment = FloatParameter()

    def run(self):
        with open(self.output().path, 'a') as file:
            for point in iterate_point(self.minx, self.miny, self.maxx, self.maxy, self.increment):
                #LOGGER.error('a')
                file.write(','.join([str(point[0]), str(point[1])] + ['2018-09-29'])+'\n')

    def output(self):
        return LocalTarget(os.path.join('tmp', 'leapfrog_huge_grid.csv'))
