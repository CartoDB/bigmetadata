#http://www.ine.es/pcaxisdl/t20/e245/p07/a2015/l0/0001.px

import csv
import os


from collections import OrderedDict
from luigi import Task, LocalTarget
from tasks.meta import OBSColumn, OBSColumnToColumn, OBSTag, current_session
from tasks.util import (LoadPostgresFromURL, classpath, shell,
                        CartoDBTarget, get_logger, underscore_slugify, TableTask,
                        ColumnTarget, ColumnsTask, TagsTask, TempTableTask,
                        classpath, PostgresTarget )
from tasks.tags import SectionTags, SubsectionTags


class Tags(TagsTask):

    def version(self):
        return 2

    def tags(self):
        return [
            OBSTag(id='demographics',
                   name='Demographics',
                   type='subsection',
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
        return 5

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
        }

    def columns(self):
        sections = self.input()['sections']
        subsections = self.input()['subsections']
        cusec_geom = OBSColumn(
            name=u'Secci\xf3n Censal',
            type="Geometry",
            weight=10,
            description='The smallest division of the Spanish Census.',
            tags=[sections['spain'], subsections['boundary']],
        )
        cusec_id = OBSColumn(
            name=u"Secci\xf3n Censal",
            type="Text",
            tags=[],
            targets={cusec_geom: 'geom_ref'}
        )
        return OrderedDict([
            ("cusec_id", cusec_id),
            ("the_geom", cusec_geom),
        ])


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
                            output=self.output().table,
                            input=self.input()['data'].table))


class SeccionDataDownload(Task):


    # metadata source: http://www.ine.es/en/censos2011_datos/indicadores_seccen_rejilla_en.xls

    URL = 'http://www.ine.es/en/censos2011_datos/indicadores_seccion_censal_csv_en.zip'
    # inside that URL, concatenate all CSVs together to get all seccions
    # available data:

    # t1_1    Total population
    # t2_1    Males
    # t2_2    Females
    # t3_1    Persons aged under 16 years
    # t3_2    Persons aged between 16 (included) and 64 (included) years
    # t3_3    Persons aged over 64 years
    # t4_1    Persons who were born in Spain
    # t4_2    Persons who were born in another EU member state
    # t4_3    Persons who were born in a European country which is not a member of the EU
    # t4_4    Persons who were born in Africa
    # t4_5    Persons who were born in Central America, South America or the Caribbean 
    # t4_6    Persons who were born in North America
    # t4_7    Persons who were born in Asia
    # t4_8    Persons who were born in Oceania
    # t5_1    Persons with Spanish nationality who were born in Spain
    # t5_2    Persons with a foreign nationality who were born in Spain
    # t5_3    Persons with Spanish nationality who were born in another EU member state
    # t5_4    Persons with a foreign nationality who were born in another EU member state
    # t5_5    Persons with Spanish nationality who were born in a European country which is not a member of the EU
    # t5_6    Persons with a foreign nationality who were born in a European country which is not a member of the EU
    # t5_7    Persons with Spanish nationality who were born in Africa
    # t5_8    Persons with a foreign nationality who were born in Africa
    # t5_9    Persons with Spanish nationality who were born in Central America, South America or the Caribbean 
    # t5_10   Persons with a foreign nationality who were born in Central America, South America or the Caribbean 
    # t5_11   Persons with Spanish nationality who were born in North America
    # t5_12   Persons with a foreign nationality who were born in North America
    # t5_13   Persons with Spanish nationality who were born in Asia
    # t5_14   Persons with a foreign nationality who were born in Asia
    # t5_15   Persons with Spanish nationality who were born in Oceania
    # t5_16   Persons with a foreign nationality who were born in Oceania
    # t6_1    Persons with Spanish nationality
    # t6_2    Persons with a foreign nationality
    # t7_1    Males under 16 years of age
    # t7_2    Males between 16 (included) and 64 (included) years of age
    # t7_3    Males over 64 years of age
    # t7_4    Females under 16 years of age
    # t7_5    Females between 16 (included) and 64 (included) years of age
    # t7_6    Females over 64 years of age
    # t8_1    Males with Spanish nationality
    # t8_2    Males with a foreign nationality
    # t8_3    Females with Spanish nationality
    # t8_4    Females with a foreign nationality
    # t9_1    Persons with Spanish nationality and under 16 years of age
    # t9_2    Persons with a foreign nationality and under 16 years of age
    # t9_3    Persons with Spanish nationality and between 16 (included) and 64 (included) years of age
    # t9_4    Persons with a foreign nationality and between 16 (included) and 64 (included) years of age
    # t9_5    Persons with Spanish nationality and over 64 years of age
    # t9_6    Persons with a foreign nationality and over 64 years of age
    # t10_1   Persons with marital status: single 
    # t10_2   Persons with marital status: married
    # t10_3   Persons with marital status: separated
    # t10_4   Persons with marital status: divorced
    # t10_5   Persons with marital status: widowed
    # t11_1   Persons with marital status: single and under 16 years of age
    # t11_2   Persons with marital status: single and between 16 (included) and 64 (included) years of age
    # t11_3   Persons with marital status: single and over 64 years of age
    # t11_4   Persons with marital status: married and under 16 years of age
    # t11_5   Persons with marital status: married and between 16 (included) and 64 (included) years of age
    # t11_6   Persons with marital status: married and over 64 years of age
    # t11_7   Persons with marital status: separated and under 16 years of age
    # t11_8   Persons with marital status: separated and between 16 (included) and 64 (included) years of age
    # t11_9   Persons with marital status: separated and over 64 years of age
    # t11_10  Persons with marital status: divorced and under 16 years of age
    # t11_11  Persons with marital status: divorced and between 16 (included) and 64 (included) years of age
    # t11_12  Persons with marital status: divorced and over 64 years of age
    # t11_13  Persons with marital status: widowed and under 16 years of age
    # t11_14  Persons with marital status: widowed and between 16 (included) and 64 (included) years of age
    # t11_15  Persons with marital status: widowed and over 64 years of age
    # t12_1   Illiterate persons
    # t12_2   Persons without studies
    # t12_3   Persons with first level studies
    # t12_4   Persons with second level studies
    # t12_5   Persons with third level studies
    # t12_6   Persons without information regarding their educational level (aged under 16 years)
    # t13_1   Illiterate males 
    # t13_2   Illiterate females
    # t13_3   Males without studies
    # t13_4   Females without studies
    # t13_5   Males with first level studies
    # t13_6   Females with first level studies
    # t13_7   Males with second level studies
    # t13_8   Females with second level studies
    # t13_9   Males with third level studies
    # t13_10  Females with third level studies
    # t13_11  Males without information regarding their educational level (under 16 years of age)
    # t13_12  Females without information regarding their educational level  (under 16 years of age)
    # t14_1   Persons with Spanish nationality and illiterate
    # t14_2   Persons with a foreign nationality and illiterate
    # t14_3   Persons with Spanish nationality and without studies
    # t14_4   Persons with a foreign nationality and without studies
    # t14_5   Persons with Spanish nationality and with first level studies
    # t14_6   Persons with a foreign nationality and with first level studies
    # t14_7   Persons with Spanish nationality and with second level studies
    # t14_8   Persons with a foreign nationality and with second level studies
    # t14_9   Persons with Spanish nationality and with third level studies
    # t14_10  Persons with a foreign nationality and with third level studies
    # t14_11  Persons with Spanish nationality without information regarding their educational level (under 16 years of age)
    # t14_12  Persons with a foreign nationality  without information regarding their educational leve (under 16 years of age)
    # t15_2   Persons aged between 16 (included) and 64 (included) years and illiterate
    # t15_3   Persons aged over 64 years and illiterate
    # t15_5   Persons aged between 16 (included) and 64 (included) years and without studies
    # t15_6   Persons aged over 64 years and without studies
    # t15_8   Persons aged between 16 (included) and 64 (included) years and with first level studies
    # t15_9   Persons aged over 64 years and with first level studies
    # t15_11  Persons aged between 16 (included) and 64 (included) years and with second level studies
    # t15_12  Persons aged over 64 years and with second level studies
    # t15_14  Persons aged between 16 (included) and 64 (included) years and with third level studies
    # t15_15  Persons aged over 64 years and with third level studies
    # t15_16  Persons aged under 16 years (information regarding the educational level is not available)
    # t16_1   Total households
    # t17_1   Main dwellings
    # t17_2   Secondary dwellings
    # t17_3   Empty dwellings
    # t18_1   Dwellings owned, by purchase, totally paid
    # t18_2   Dwellings owned, by purchase, with outstanding
    # t18_3   Dwellings owned, by inheritance or donation
    # t18_4   Dwellings rented
    # t18_5   Dwellings transferred for free or at low cost 
    # t18_6   Dwellings with another tenancy regime
    # t19_1   Dwellins with less than 30m2
    # t19_2   Dwellings between 30-45 m2
    # t19_3   Dwellings between 46-60 m2
    # t19_4   Dwellings between 61-75 m2
    # t19_5   Dwellings between 76-90 m2
    # t19_6   Dwellings between 91-105 m2
    # t19_7   Dwellings between 106-120 m2
    # t19_8   Dwellings between 121-150 m2
    # t19_9   Dwellings between 151-180 m2
    # t19_10  Dwellings with more than 180 m2
    # t20_1   Dwellings with 1 room
    # t20_2   Dwellings with 2 rooms
    # t20_3   Dwellings with 3 rooms
    # t20_4   Dwellings with 4 rooms
    # t20_5   Dwellings with 5 rooms
    # t20_6   Dwellings with 6 rooms
    # t20_7   Dwellings with 7 rooms
    # t20_8   Dwellings with 8 rooms
    # t20_9   Dwellings with 9 or more rooms
    # t21_1   Total households
    # t22_1   Households with 1 person
    # t22_2   Households with 2 persons
    # t22_3   Households with 3 persons
    # t22_4   Households with 4 persons
    # t22_5   Households with 5 persons
    # t22_6   Households with 6 or more persons
    # t22_2   Hogares de 2 personas
    # t22_3   Hogares de 3 personas
    # t22_4   Hogares de 4 personas
    # t22_5   Hogares de 5 personas
    # t22_6   Hogares de 6 o más personas

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
        return 4

    def requires(self):
        return {
            'tags': Tags(),
            'sections': SectionTags(),
        }

    def columns(self):
        spain = self.input()['sections']['spain']
        tags = self.input()['tags']
        total_pop = OBSColumn(
            type='Numeric',
            name='Total Population',
            description='The total number of all people living in a geographic area.',
            aggregate='sum',
            weight=10,
            tags=[tags['demographics'], spain]
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
                description='',
                weight=3,
                tags=[spain, tags['demographics']]
            )
        columns['pop_100_more'] = OBSColumn(
            type='Numeric',
            name='Population age 100 or more'.format(
                start=start, end=end),
            targets={total_pop: 'denominator'},
            description='',
            weight=3,
            tags=[tags['demographics'], spain]
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

    def version(self):
        return 1

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
        }

    def version(self):
        return 1

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
                            output=self.output().table,
                            input=self.input()['data'].table
                        ))
