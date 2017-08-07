import csv
import os

from collections import OrderedDict
from luigi import Task, LocalTarget
from tasks.meta import (OBSColumn, OBSColumnToColumn, OBSTag, current_session,
                        DENOMINATOR, GEOM_REF)
from tasks.util import (LoadPostgresFromURL, classpath, shell, LOGGER,
                        CartoDBTarget, get_logger, underscore_slugify, TableTask,
                        ColumnTarget, ColumnsTask, TagsTask, TempTableTask,
                        classpath, PostgresTarget, MetaWrapper)
from tasks.tags import SectionTags, SubsectionTags, UnitTags, BoundaryTags
from time import time


class DownloadGeometry(Task):

    URL = 'http://www.ine.es/censos2011_datos/cartografia_censo2011_nacional.zip'

    def run(self):
        self.output().makedirs()
        shell('wget {url} -O {output}'.format(url=self.URL,
                                              output=self.output().path))
    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self),
                                        'cartografia_censo2011_nacional.zip'))

class RawGeometry(TempTableTask):

    def requires(self):
        return DownloadGeometry()

    def run(self):
        cmd = 'unzip -o "{input}" -d "$(dirname {input})/$(basename {input} .zip)"'.format(
            input=self.input().path)
        shell(cmd)
        cmd = 'PG_USE_COPY=yes PGCLIENTENCODING=latin1 ' \
                'ogr2ogr -f PostgreSQL PG:dbname=$PGDATABASE ' \
                '-t_srs "EPSG:4326" -nlt MultiPolygon -nln {table} ' \
                '-lco OVERWRITE=yes ' \
                '-lco SCHEMA={schema} -lco PRECISION=no ' \
                '$(dirname {input})/$(basename {input} .zip)/*.shp '.format(
                    schema=self.output().schema,
                    table=self.output().tablename,
                    input=self.input().path)
        shell(cmd)


class GeometryColumns(ColumnsTask):

    def version(self):
        return 1

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'source': SourceTags(),
            'license': LicenseTags(),
            'boundary': BoundaryTags(),
        }

    def columns(self):
        input_ = self.input()
        sections = input_['sections']
        subsections = input_['subsections']
        license = input_['license']['ine-license']
        source = input_['source']['ine-source']
        boundary_type = input_['boundary']
        cusec_geom = OBSColumn(
            name=u'Secci\xf3n Censal',
            type="Geometry",
            weight=10,
            description='The smallest division of the Spanish Census.',
            tags=[sections['spain'], subsections['boundary'], source, license,
                  boundary_type['interpolation_boundary'],
                  boundary_type['cartographic_boundary']],
        )
        cusec_id = OBSColumn(
            name=u"Secci\xf3n Censal",
            type="Text",
            targets={cusec_geom: GEOM_REF}
        )
        return OrderedDict([
            ("cusec_id", cusec_id),
            ("the_geom", cusec_geom),
        ])

class SourceTags(TagsTask):
    def version(self):
        return 1

    def tags(self):
        return [
            OBSTag(
                id='ine-source',
                name='National Statistics Institute (INE) Legal Notice',
                type='source',
                description='More information `here <http://ine.es/ss/Satellite?L=1&c=Page&cid=1254735849170&p=1254735849170&pagename=Ayuda%2FINELayout#>`_')
        ]

class LicenseTags(TagsTask):
    def version(self):
        return 1

    def tags(self):
        return [OBSTag(id='ine-license',
                name='National Statistics Institute (INE)',
                type='license',
                description='`INE website <http://www.ine.es>`_')
            ]

class Geometry(TableTask):

    def version(self):
        return 1

    def requires(self):
        return {
            'meta': GeometryColumns(),
            'data': RawGeometry()
        }

    def columns(self):
        return self.input()['meta']

    def timespan(self):
        # TODO Add a param year
        return '2011'

    def populate(self):
        session = current_session()
        session.execute('INSERT INTO {output} '
                        'SELECT cusec as cusec_id, '
                        '       wkb_geometry as cusec_geom '
                        'FROM {input} '.format(
                            output=self.output().table,
                            input=self.input()['data'].table))

class SeccionColumns(ColumnsTask):

    # metadata source: http://www.ine.es/en/censos2011_datos/indicadores_seccen_rejilla_en.xls

    def requires(self):
        return {
            'tags': SubsectionTags(),
            'sections': SectionTags(),
            'units': UnitTags(),
            'license': LicenseTags(),
            'source': SourceTags(),
        }

    def version(self):
        return 1

    def columns(self):
        input_ = self.input()
        spain = input_['sections']['spain']
        tags = input_['tags']
        units = input_['units']
        source = input_['source']['ine-source']
        license = input_['license']['ine-license']

        total_pop = OBSColumn(
            id='t1_1',
            name='Total population',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['age_gender'], spain, units['people']])
        male_pop = OBSColumn(
            id='t2_1',
            name='Males',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['age_gender'], spain, units['people']],
            targets={total_pop: DENOMINATOR})

        columns = OrderedDict([
            ('total_pop', total_pop),
            ('male_pop', male_pop),
            ])

        for _, col in columns.iteritems():
            col.tags.append(source)
            col.tags.append(license)
        return columns

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
        return 1

    def requires(self):
        return {
            'seccion_columns': SeccionColumns(),
            'tags': SubsectionTags(),
            'sections': SectionTags(),
            'units': UnitTags(),
            'license': LicenseTags(),
            'source': SourceTags()
        }

    def columns(self):
        input_ = self.input()
        spain = input_['sections']['spain']
        tags = input_['tags']
        units = input_['units']
        license = input_['license']['ine-license']
        source = input_['source']['ine-source']
        session = current_session()
        total_pop = input_['seccion_columns']['total_pop'].get(session)
        columns = OrderedDict([
            ('gender', OBSColumn(
                type='Text',
                name='Gender',
                weight=0
            ))
        ])
        for i in xrange(0, 20):
            start = i * 5
            end = start + 4
            _id = 'pop_{start}_{end}'.format(start=start, end=end)
            columns[_id] = OBSColumn(
                type='Numeric',
                name='Population age {start} to {end}'.format(
                    start=start, end=end),
                targets={total_pop: DENOMINATOR},
                description='',
                aggregate='sum',
                weight=3,
                tags=[spain, tags['age_gender'], units['people'], license, source]
            )
        columns['pop_100_more'] = OBSColumn(
            type='Numeric',
            name='Population age 100 or more'.format(
                start=start, end=end),
            targets={total_pop: DENOMINATOR},
            description='',
            aggregate='sum',
            weight=3,
            tags=[tags['age_gender'], units['people'], spain, license, source]
        )

        return columns


class RawFiveYearPopulation(TempTableTask):
    '''
    Load csv into postgres
    '''

    def requires(self):
        return {
            'data': FiveYearPopulationParse(),
            'meta': FiveYearPopulationColumns(),
            'seccion_columns': SeccionColumns(),
            'geometa': GeometryColumns(),
        }

    def columns(self):
        '''
        Add the geoid (cusec_id) column into the second position as expected
        '''
        metacols = self.input()['meta']
        cols = OrderedDict()
        cols['gender'] = metacols.pop('gender')
        cols['cusec_id'] = self.input()['geometa']['cusec_id']
        cols['total_pop'] = self.input()['seccion_columns']['total_pop']
        for key, col in metacols.iteritems():
            cols[key] = col
        return cols

    def run(self):
        session = current_session()
        cols = ['{colname} {coltype}'.format(colname=colname,
                                             coltype=coltarget.get(session).type)
                for colname, coltarget in self.columns().iteritems()]
        create_table = 'CREATE TABLE {output} ({cols})'.format(
            cols=', '.join(cols),
            output=self.output().table
        )
        session.execute(create_table)
        session.commit()

        shell("cat '{input}' | psql -c '\\copy {output} FROM STDIN WITH CSV "
              "HEADER ENCODING '\"'\"'latin1'\"'\"".format(
                  output=self.output().table,
                  input=self.input()['data'].path
              ))


class FiveYearPopulation(TableTask):
    '''
    Keep only the "ambos sexos" entries
    '''

    def requires(self):
        return {
            'data': RawFiveYearPopulation(),
            'meta': FiveYearPopulationColumns(),
            'seccion_columns': SeccionColumns(),
            'geometa': GeometryColumns(),
        }

    def version(self):
        return 1

    def columns(self):
        '''
        Add the geoid (cusec_id) column into the second position as expected
        '''
        metacols = self.input()['meta']
        cols = OrderedDict()
        cols['cusec_id'] = self.input()['geometa']['cusec_id']
        cols['total_pop'] = self.input()['seccion_columns']['total_pop']
        for key, col in metacols.iteritems():
            if key == 'gender':
                continue
            cols[key] = col
        return cols

    def timespan(self):
        return '2015'

    def populate(self):
        session = current_session()
        session.execute('INSERT INTO {output} '
                        'SELECT {cols} FROM {input} '
                        "WHERE gender = 'Ambos Sexos'".format(
                            cols=', '.join(self.columns().keys()),
                            output=self.output().table,
                            input=self.input()['data'].table
                        ))


class FiveYearPopulationMeta(MetaWrapper):

    def tables(self):
        yield FiveYearPopulation()
        yield Geometry()
