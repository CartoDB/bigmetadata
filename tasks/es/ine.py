#http://www.ine.es/pcaxisdl/t20/e245/p07/a2015/l0/0001.px

import csv
import os


from collections import OrderedDict
from luigi import Task, LocalTarget
from tasks.meta import OBSColumn, OBSColumnToColumn, OBSTag, current_session
from tasks.util import (LoadPostgresFromURL, classpath, pg_cursor, shell,
                        CartoDBTarget, get_logger, underscore_slugify, TableTask,
                        ColumnTarget, ColumnsTask, TagsTask,
                        classpath, DefaultPostgresTarget, tablize)


class Tags(TagsTask):

    def tags(self):
        return [
            OBSTag(id='demographics',
                   name='Demographics of Spain',
                   type='catalog',
                   description='Demographics of Spain from the INE Census')
        ]


class DownloadGeometry(Task):

    URL = 'http://www.ine.es/censos2011_datos/cartografia_censo2011_nacional.zip'

    def run(self):
        self.output().makedirs()
        shell('wget {url} -O {output}'.format(url=self.URL,
                                              output=self.output().path))

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self),
                                        'cartografia_censo2011_nacional.zip'))


class RawGeometry(Task):

    def requires(self):
        return DownloadGeometry()

    @property
    def schema(self):
        return classpath(self)

    @property
    def tablename(self):
        return 'raw_geometry'

    def run(self):
        session = current_session()
        session.execute('CREATE SCHEMA IF NOT EXISTS "{schema}"'.format(
            schema=classpath(self)))

        cmd = 'unzip -o "{input}" -d "$(dirname {input})/$(basename {input} .zip)"'.format(
            input=self.input().path)
        shell(cmd)
        cmd = 'PG_USE_COPY=yes PGCLIENTENCODING=latin1 ' \
                'ogr2ogr -f PostgreSQL PG:dbname=$PGDATABASE ' \
                '-t_srs "EPSG:4326" -nlt MultiPolygon -nln {table} ' \
                '-lco OVERWRITE=yes ' \
                '-lco SCHEMA={schema} -lco PRECISION=no ' \
                '$(dirname {input})/$(basename {input} .zip)/*.shp '.format(
                    schema=self.schema,
                    table=self.tablename,
                    input=self.input().path)
        shell(cmd)
        self.output().touch()

    def output(self):
        return DefaultPostgresTarget(table='"{schema}".{table}'.format(
            schema=self.schema,
            table=self.tablename
        ))


class GeometryColumns(ColumnsTask):

    def version(self):
        return 2

    def columns(self):
        cusec_geom = OBSColumn(
            #id='cusec_geom',
            name=u'Secci\xf3n Censal',
            type="Geometry",
            weight=10,
            description='The finest division of the Spanish Census.'
        )
        cusec_id = OBSColumn(
            #id='cusec_id',
            name=u"Secci\xf3n Censal",
            type="Text",
            targets={
                #cusec_geom: 'geom_ref'
            }
        )
        return OrderedDict([
            ("cusec_id", cusec_id),
            ("geom", cusec_geom),
        ])


class Geometry(TableTask):

    def requires(self):
        return {
            'meta': GeometryColumns(),
            'data': RawGeometry()
        }

    def columns(self):
        return self.input()['meta']

    def timespan(self):
        return '2011'

    def bounds(self):
        if not self.input()['data'].exists():
            return
        session = current_session()
        return session.execute(
            'SELECT ST_EXTENT(wkb_geometry) FROM '
            '{input}'.format(input=self.input()['data'].table)
        ).first()[0]

    def populate(self):
        session = current_session()
        session.execute('INSERT INTO {output} '
                        'SELECT cusec as cusec_id, '
                        '       wkb_geometry as cusec_geom '
                        'FROM {input} '.format(
                            output=self.output().get(session).id,
                            input=self.input()['data'].table))


class FiveYearPopulationDownload(Task):

    URL = 'http://www.ine.es/pcaxisdl/t20/e245/p07/a2015/l0/0001.px'

    def run(self):
        self.output().makedirs()
        cmd = 'wget "{url}" -O "{output}"'.format(url=self.URL,
                                                  output=self.output().path)
        shell(cmd)

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), '0001.px'))


class FiveYearPopulationParse(Task):
    '''
    convert px file to csv
    '''

    def requires(self):
        return FiveYearPopulationDownload()

    def run(self):
        output = self.output()
        dimensions = []
        self.output().makedirs()
        with self.output().open('w') as outfile:
            section = None
            with self.input().open() as infile:
                for line in infile:
                    line = line.strip()
                    if line.startswith('VALUES'):
                        section = 'values'
                        dimensions.append([line.split('"')[1], [], 0])
                        line = line.split('=')[1]

                    if section == 'values':
                        dimensions[-1][1].extend([l.strip(';" ') for l in line.split(',') if l])
                        if line.endswith(';'):
                            section = None
                        continue

                    if line.startswith('DATA='):
                        section = 'data'
                        headers = [d[0] for d in dimensions[0:-1]]
                        headers.extend([h.strip(';" ') for h in dimensions.pop()[1] if h])
                        writer = csv.DictWriter(outfile, headers)
                        writer.writeheader()
                        continue

                    if section == 'data':
                        if line.startswith(';'):
                            continue

                        values = {}
                        for dimname, dimvalues, dimcnt in dimensions:
                            values[dimname] = dimvalues[dimcnt]
                        i = len(dimensions) - 1
                        while dimensions[i][2] + 2 > len(dimensions[i][1]):
                            dimensions[i][2] = 0
                            i -= 1
                        dimensions[i][2] += 1

                        for i, d in enumerate(line.split(' ')):
                            values[headers[len(dimensions) + i]] = d

                        writer.writerow(values)


    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), '0001.csv'))


class FiveYearPopulationColumns(ColumnsTask):

    def version(self):
        return 0

    def requires(self):
        return {
            'tags': Tags()
        }

    def columns(self):
        tags = self.input()['tags']
        total_pop = OBSColumn(
            type='Numeric',
            name='Total Population',
            description='The total number of all people living in a geographic area.',
            aggregate='sum',
            weight=10,
            tags=[tags['demographics']]
        )
        columns = OrderedDict([
            ('gender', OBSColumn(
                type='Text',
                name='Gender',
                weight=0
            )),
            ('total_pop', total_pop)
        ])
        for i in xrange(0, 20):
            start = i * 5
            end = start + 4
            _id = 'pop_{start}_{end}'.format(start=start, end=end)
            columns[_id] = OBSColumn(
                type='Numeric',
                name='Population age {start} to {end}'.format(
                    start=start, end=end),
                targets={total_pop: 'denominator'},
                tags=[tags['demographics']]
            )
        columns['pop_100_more'] = OBSColumn(
            type='Numeric',
            name='Population age 100 or more'.format(
                start=start, end=end),
            targets={total_pop: 'denominator'},
            tags=[tags['demographics']]
        )

        return columns


class RawFiveYearPopulation(TableTask):
    '''
    Load csv into postgres
    '''

    def requires(self):
        return {
            'data': FiveYearPopulationParse(),
            'meta': FiveYearPopulationColumns(),
            'geometa': GeometryColumns(),
            'geotable': Geometry()
        }

    def timespan(self):
        return '2011'

    def bounds(self):
        if not self.input()['geotable'].exists():
            return
        else:
            session = current_session()
            return self.input()['geotable'].get(session).bounds

    def columns(self):
        '''
        Add the geoid (cusec_id) column into the second position as expected
        '''
        metacols = self.input()['meta']
        cols = OrderedDict()
        cols['gender'] = metacols.pop('gender')
        cols['cusec_id'] = self.input()['geometa']['cusec_id']
        for key, col in metacols.iteritems():
            cols[key] = col
        return cols

    def populate(self):
        session = current_session()
        shell("cat '{input}' | psql -c '\\copy {output} FROM STDIN WITH CSV "
              "HEADER ENCODING '\"'\"'latin1'\"'\"".format(
                  output=self.output().get(session).id,
                  input=self.input()['data'].path
              ))


class FiveYearPopulation(TableTask):
    '''
    Keep only the "ambos sexos" entries
    '''

    def requires(self):
        return {
            'data': RawFiveYearPopulation(),
        }

    def columns(self):
        '''
        Add the geoid (cusec_id) column into the second position as expected
        '''
        cols = self.requires()['data'].columns()
        cols.pop('gender')
        return cols

    def timespan(self):
        return self.requires()['data'].timespan()

    def bounds(self):
        return self.requires()['data'].bounds()

    def populate(self):
        session = current_session()
        session.execute('INSERT INTO {output} '
                        'SELECT {cols} FROM {input} '
                        "WHERE gender = 'Ambos Sexos'".format(
                            cols=', '.join(self.columns().keys()),
                            output=self.output().get(session).id,
                            input=self.input()['data'].get(session).id
                        ))
