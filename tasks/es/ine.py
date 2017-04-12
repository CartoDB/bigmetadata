#http://www.ine.es/pcaxisdl/t20/e245/p07/a2015/l0/0001.px

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
        return 8

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


class Geometry(TableTask):

    def version(self):
        return 6

    def requires(self):
        return {
            'meta': GeometryColumns(),
            'data': RawGeometry()
        }

    def columns(self):
        return self.input()['meta']

    def timespan(self):
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
        return 40

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
        female_pop = OBSColumn(
            id='t2_2',
            name='Females',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['age_gender'], spain, units['people']],
            targets={total_pop: DENOMINATOR})
        pop_under_16 = OBSColumn(
            id='t3_1',
            name='Persons aged under 16 years',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['age_gender'], spain, units['people']],
            targets={total_pop: DENOMINATOR})
        pop_16_to_64 = OBSColumn(
            id='t3_2',
            name='Persons aged between 16 (included) and 64 (included) years',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['age_gender'], spain, units['people']],
            targets={total_pop: DENOMINATOR})
        pop_over_64 = OBSColumn(
            id='t3_3',
            name='Persons aged over 64 years',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['age_gender'], spain, units['people']],
            targets={total_pop: DENOMINATOR})
        spanish_nationality = OBSColumn(
            id='t6_1',
            name='Persons with Spanish nationality',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], spain, units['people']],
            targets={total_pop: DENOMINATOR})
        foreign_nationality = OBSColumn(
            id='t6_2',
            name='Persons with a foreign nationality',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={total_pop: DENOMINATOR})
        spain_born = OBSColumn(
            id='t4_1',
            name='Persons who were born in Spain',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={total_pop: DENOMINATOR})
        eu_born = OBSColumn(
            id='t4_2',
            name='Persons who were born in another EU member state',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={total_pop: DENOMINATOR})
        europe_non_eu_born = OBSColumn(
            id='t4_3',
            name='Persons who were born in a European country which is not a member of the EU',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={total_pop: DENOMINATOR})
        african_born = OBSColumn(
            id='t4_4',
            name='Persons who were born in Africa',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={total_pop: DENOMINATOR})
        central_south_american_caribbean_born = OBSColumn(
            id='t4_5',
            name='Persons who were born in Central America, South America or the Caribbean ',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={total_pop: DENOMINATOR})
        north_american_born = OBSColumn(
            id='t4_6',
            name='Persons who were born in North America',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={total_pop: DENOMINATOR})
        asian_born = OBSColumn(
            id='t4_7',
            name='Persons who were born in Asia',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={total_pop: DENOMINATOR})
        oceania_born = OBSColumn(
            id='t4_8',
            name='Persons who were born in Oceania',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={total_pop: DENOMINATOR})
        spain_born_spanish_nationality = OBSColumn(
            id='t5_1',
            name='Persons with Spanish nationality who were born in Spain',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={spain_born: DENOMINATOR,
                     spanish_nationality: DENOMINATOR})
        spain_born_foreign_nationality = OBSColumn(
            id='t5_2',
            name='Persons with a foreign nationality who were born in Spain',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={spain_born: DENOMINATOR,
                     foreign_nationality: DENOMINATOR})
        eu_born_spanish_nationality = OBSColumn(
            id='t5_3',
            name='Persons with Spanish nationality who were born in another EU member state',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={eu_born: DENOMINATOR,
                     spanish_nationality: DENOMINATOR})
        eu_born_foreign_nationality = OBSColumn(
            id='t5_4',
            name='Persons with a foreign nationality who were born in another EU member state',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={eu_born: DENOMINATOR,
                     foreign_nationality: DENOMINATOR})
        europe_non_eu_born_spanish_nationality = OBSColumn(
            id='t5_5',
            name='Persons with Spanish nationality who were born in a European country which is not a member of the EU',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={europe_non_eu_born: DENOMINATOR,
                     spanish_nationality: DENOMINATOR})
        europe_non_eu_born_foreign_nationality = OBSColumn(
            id='t5_6',
            name='Persons with a foreign nationality who were born in a European country which is not a member of the EU',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={europe_non_eu_born: DENOMINATOR,
                     foreign_nationality: DENOMINATOR})
        african_born_spanish_nationality = OBSColumn(
            id='t5_7',
            name='Persons with Spanish nationality who were born in Africa',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={african_born: DENOMINATOR,
                     spanish_nationality: DENOMINATOR})
        african_born_foreign_nationality = OBSColumn(
            id='t5_8',
            name='Persons with a foreign nationality who were born in Africa',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={african_born: DENOMINATOR,
                     foreign_nationality: DENOMINATOR})
        central_south_american_caribbean_born_spanish_nationality = OBSColumn(
            id='t5_9',
            name='Persons with Spanish nationality who were born in Central America, South America or the Caribbean ',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={central_south_american_caribbean_born: DENOMINATOR,
                     spanish_nationality: DENOMINATOR})
        central_south_american_caribbean_born_foreign_nationality = OBSColumn(
            id='t5_10',
            name='Persons with a foreign nationality who were born in Central America, South America or the Caribbean ',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={central_south_american_caribbean_born: DENOMINATOR,
                     foreign_nationality: DENOMINATOR})
        north_american_born_spanish_nationality = OBSColumn(
            id='t5_11',
            name='Persons with Spanish nationality who were born in North America',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={north_american_born: DENOMINATOR,
                     spanish_nationality: DENOMINATOR})
        north_american_born_foreign_nationality = OBSColumn(
            id='t5_12',
            name='Persons with a foreign nationality who were born in North America',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={north_american_born: DENOMINATOR,
                     foreign_nationality: DENOMINATOR})
        asian_born_spanish_nationality = OBSColumn(
            id='t5_13',
            name='Persons with Spanish nationality who were born in Asia',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={asian_born: DENOMINATOR,
                     spanish_nationality: DENOMINATOR})
        asian_born_foreign_nationality = OBSColumn(
            id='t5_14',
            name='Persons with a foreign nationality who were born in Asia',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={asian_born: DENOMINATOR,
                     foreign_nationality: DENOMINATOR})
        oceania_born_spanish_nationality = OBSColumn(
            id='t5_15',
            name='Persons with Spanish nationality who were born in Oceania',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={oceania_born: DENOMINATOR,
                     spanish_nationality: DENOMINATOR})
        oceania_born_foreign_nationality = OBSColumn(
            id='t5_16',
            name='Persons with a foreign nationality who were born in Oceania',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], units['people'], spain],
            targets={oceania_born: DENOMINATOR,
                     foreign_nationality: DENOMINATOR})
        males_under_16 = OBSColumn(
            id='t7_1',
            name='Males under 16 years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['age_gender'], units['people'], spain],
            targets={male_pop: DENOMINATOR,
                     pop_under_16: DENOMINATOR})
        males_16_to_64 = OBSColumn(
            id='t7_2',
            name='Males between 16 (included) and 64 (included) years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['age_gender'], units['people'], spain],
            targets={male_pop: DENOMINATOR,
                     pop_16_to_64: DENOMINATOR})
        males_over_64 = OBSColumn(
            id='t7_3',
            name='Males over 64 years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['age_gender'], units['people'], spain],
            targets={male_pop: DENOMINATOR,
                     pop_over_64: DENOMINATOR})
        females_under_16 = OBSColumn(
            id='t7_4',
            name='Females under 16 years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['age_gender'], units['people'], spain],
            targets={female_pop: DENOMINATOR,
                     pop_under_16: DENOMINATOR})
        females_16_to_64 = OBSColumn(
            id='t7_5',
            name='Females between 16 (included) and 64 (included) years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['age_gender'], units['people'], spain],
            targets={female_pop: DENOMINATOR,
                     pop_16_to_64: DENOMINATOR})
        females_over_64 = OBSColumn(
            id='t7_6',
            name='Females over 64 years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['age_gender'], units['people'], spain],
            targets={female_pop: DENOMINATOR,
                     pop_over_64: DENOMINATOR})
        males_spanish_nationality = OBSColumn(
            id='t8_1',
            name='Males with Spanish nationality',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], tags['age_gender'], units['people'], spain],
            targets={male_pop: DENOMINATOR,
                     spanish_nationality: DENOMINATOR})
        males_foreign_nationality = OBSColumn(
            id='t8_2',
            name='Males with a foreign nationality',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], tags['age_gender'], units['people'], spain],
            targets={male_pop: DENOMINATOR,
                     foreign_nationality: DENOMINATOR})
        females_spanish_nationality = OBSColumn(
            id='t8_3',
            name='Females with Spanish nationality',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], tags['age_gender'], units['people'], spain],
            targets={female_pop: DENOMINATOR,
                     spanish_nationality: DENOMINATOR})
        females_foreign_nationality = OBSColumn(
            id='t8_4',
            name='Females with a foreign nationality',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], tags['age_gender'], units['people'], spain],
            targets={female_pop: DENOMINATOR,
                     foreign_nationality: DENOMINATOR})
        spanish_nationality_under_16 = OBSColumn(
            id='t9_1',
            name='Persons with Spanish nationality and under 16 years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], tags['age_gender'], units['people'], spain],
            targets={spanish_nationality: DENOMINATOR,
                     pop_under_16: DENOMINATOR})
        foreign_nationality_under_16 = OBSColumn(
            id='t9_2',
            name='Persons with a foreign nationality and under 16 years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], tags['age_gender'], units['people'], spain],
            targets={foreign_nationality: DENOMINATOR,
                     pop_under_16: DENOMINATOR})
        spanish_nationality_16_to_64 = OBSColumn(
            id='t9_3',
            name='Persons with Spanish nationality and between 16 (included) and 64 (included) years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], tags['age_gender'], units['people'], spain],
            targets={spanish_nationality: DENOMINATOR,
                     pop_16_to_64: DENOMINATOR})
        foreign_nationality_16_to_64 = OBSColumn(
            id='t9_4',
            name='Persons with a foreign nationality and between 16 (included) and 64 (included) years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], tags['age_gender'], units['people'], spain],
            targets={foreign_nationality: DENOMINATOR,
                     pop_16_to_64: DENOMINATOR})
        spanish_nationality_over_64 = OBSColumn(
            id='t9_5',
            name='Persons with Spanish nationality and over 64 years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], tags['age_gender'], units['people'], spain],
            targets={spanish_nationality: DENOMINATOR,
                     pop_over_64: DENOMINATOR})
        foreign_nationality_over_64 = OBSColumn(
            id='t9_6',
            name='Persons with a foreign nationality and over 64 years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['nationality'], tags['age_gender'], units['people'], spain],
            targets={foreign_nationality: DENOMINATOR,
                     pop_over_64: DENOMINATOR})
        unmarried = OBSColumn(
            id='t10_1',
            name='Persons with marital status: single ',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], units['people'], spain],
            targets={total_pop: DENOMINATOR})
        married = OBSColumn(
            id='t10_2',
            name='Persons with marital status: married',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], units['people'], spain],
            targets={total_pop: DENOMINATOR})
        separated = OBSColumn(
            id='t10_3',
            name='Persons with marital status: separated',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], units['people'], spain],
            targets={total_pop: DENOMINATOR})
        divorced = OBSColumn(
            id='t10_4',
            name='Persons with marital status: divorced',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], units['people'], spain],
            targets={total_pop: DENOMINATOR})
        widowed = OBSColumn(
            id='t10_5',
            name='Persons with marital status: widowed',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], units['people'], spain],
            targets={total_pop: DENOMINATOR})
        unmarried_under_16 = OBSColumn(
            id='t11_1',
            name='Persons with marital status: single and under 16 years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], tags['age_gender'], units['people'], spain],
            targets={pop_under_16: DENOMINATOR,
                     unmarried: DENOMINATOR})
        unmarried_16_to_64 = OBSColumn(
            id='t11_2',
            name='Persons with marital status: single and between 16 (included) and 64 (included) years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], tags['age_gender'], units['people'], spain],
            targets={pop_16_to_64: DENOMINATOR,
                     unmarried: DENOMINATOR})
        unmarried_over_64 = OBSColumn(
            id='t11_3',
            name='Persons with marital status: single and over 64 years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], tags['age_gender'], units['people'], spain],
            targets={pop_over_64: DENOMINATOR,
                     unmarried: DENOMINATOR})
        married_under_16 = OBSColumn(
            id='t11_4',
            name='Persons with marital status: married and under 16 years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], tags['age_gender'], units['people'], spain],
            targets={pop_under_16: DENOMINATOR,
                     married: DENOMINATOR})
        married_16_to_64 = OBSColumn(
            id='t11_5',
            name='Persons with marital status: married and between 16 (included) and 64 (included) years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], tags['age_gender'], units['people'], spain],
            targets={pop_16_to_64: DENOMINATOR,
                     married: DENOMINATOR})
        married_over_64 = OBSColumn(
            id='t11_6',
            name='Persons with marital status: married and over 64 years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], tags['age_gender'], units['people'], spain],
            targets={pop_over_64: DENOMINATOR,
                     married: DENOMINATOR})
        separated_under_16 = OBSColumn(
            id='t11_7',
            name='Persons with marital status: separated and under 16 years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], tags['age_gender'], units['people'], spain],
            targets={pop_under_16: DENOMINATOR,
                     separated: DENOMINATOR})
        separated_16_to_64 = OBSColumn(
            id='t11_8',
            name='Persons with marital status: separated and between 16 (included) and 64 (included) years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], tags['age_gender'], units['people'], spain],
            targets={pop_16_to_64: DENOMINATOR,
                     separated: DENOMINATOR})
        separated_over_64 = OBSColumn(
            id='t11_9',
            name='Persons with marital status: separated and over 64 years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], tags['age_gender'], units['people'], spain],
            targets={pop_over_64: DENOMINATOR,
                     separated: DENOMINATOR})
        divorced_under_16 = OBSColumn(
            id='t11_10',
            name='Persons with marital status: divorced and under 16 years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], tags['age_gender'], units['people'], spain],
            targets={pop_under_16: DENOMINATOR,
                     divorced: DENOMINATOR})
        divorced_16_to_64 = OBSColumn(
            id='t11_11',
            name='Persons with marital status: divorced and between 16 (included) and 64 (included) years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], tags['age_gender'], units['people'], spain],
            targets={pop_16_to_64: DENOMINATOR,
                     divorced: DENOMINATOR})
        divorced_over_64 = OBSColumn(
            id='t11_12',
            name='Persons with marital status: divorced and over 64 years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], tags['age_gender'], units['people'], spain],
            targets={pop_over_64: DENOMINATOR,
                     divorced: DENOMINATOR})
        widowed_under_16 = OBSColumn(
            id='t11_13',
            name='Persons with marital status: widowed and under 16 years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], tags['age_gender'], units['people'], spain],
            targets={pop_under_16: DENOMINATOR,
                     widowed: DENOMINATOR})
        widowed_16_to_64 = OBSColumn(
            id='t11_14',
            name='Persons with marital status: widowed and between 16 (included) and 64 (included) years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], tags['age_gender'], units['people'], spain],
            targets={pop_16_to_64: DENOMINATOR,
                     widowed: DENOMINATOR})
        widowed_over_64 = OBSColumn(
            id='t11_15',
            name='Persons with marital status: widowed and over 64 years of age',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], tags['age_gender'], units['people'], spain],
            targets={pop_over_64: DENOMINATOR,
                     widowed: DENOMINATOR})
        illiterate = OBSColumn(
            id='t12_1',
            name='Illiterate persons',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], units['people'], spain],
            targets={total_pop: DENOMINATOR})
        without_studies = OBSColumn(
            id='t12_2',
            name='Persons without studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], units['people'], spain],
            targets={total_pop: DENOMINATOR})
        first_level_studies = OBSColumn(
            id='t12_3',
            name='Persons with first level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], units['people'], spain],
            targets={total_pop: DENOMINATOR})
        second_level_studies = OBSColumn(
            id='t12_4',
            name='Persons with second level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], units['people'], spain],
            targets={total_pop: DENOMINATOR})
        third_level_studies = OBSColumn(
            id='t12_5',
            name='Persons with third level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], units['people'], spain],
            targets={total_pop: DENOMINATOR})
        no_education_info_under_16 = OBSColumn(
            id='t12_6',
            name='Persons without information regarding their educational level (aged under 16 years)',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={pop_under_16: DENOMINATOR})
        illiterate_male_pop = OBSColumn(
            id='t13_1',
            name='Illiterate males',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={male_pop: DENOMINATOR,
                     illiterate: DENOMINATOR})
        illiterate_female_pop = OBSColumn(
            id='t13_2',
            name='Illiterate females',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={female_pop: DENOMINATOR,
                     illiterate: DENOMINATOR})
        male_without_studies = OBSColumn(
            id='t13_3',
            name='Males without studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={male_pop: DENOMINATOR,
                     without_studies: DENOMINATOR})
        female_without_studies = OBSColumn(
            id='t13_4',
            name='Females without studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={female_pop: DENOMINATOR,
                     without_studies: DENOMINATOR})
        male_first_level_studies = OBSColumn(
            id='t13_5',
            name='Males with first level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={male_pop: DENOMINATOR,
                     first_level_studies: DENOMINATOR})
        female_first_level_studies = OBSColumn(
            id='t13_6',
            name='Females with first level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={female_pop: DENOMINATOR,
                     first_level_studies: DENOMINATOR})
        male_second_level_studies = OBSColumn(
            id='t13_7',
            name='Males with second level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={male_pop: DENOMINATOR,
                     second_level_studies: DENOMINATOR})
        female_second_level_studies = OBSColumn(
            id='t13_8',
            name='Females with second level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={female_pop: DENOMINATOR,
                     second_level_studies: DENOMINATOR})
        male_third_level_studies = OBSColumn(
            id='t13_9',
            name='Males with third level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={male_pop: DENOMINATOR,
                     third_level_studies: DENOMINATOR})
        female_third_level_studies = OBSColumn(
            id='t13_10',
            name='Females with third level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={female_pop: DENOMINATOR,
                     third_level_studies: DENOMINATOR})
        male_no_education_info_under_16 = OBSColumn(
            id='t13_11',
            name='Males without information regarding their educational level (under 16 years of age)',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={male_pop: DENOMINATOR,
                     no_education_info_under_16: DENOMINATOR})
        female_no_education_info_under_16 = OBSColumn(
            id='t13_12',
            name='Females without information regarding their educational level  (under 16 years of age)',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={female_pop: DENOMINATOR,
                     no_education_info_under_16: DENOMINATOR})
        illiterate_spanish_nationality = OBSColumn(
            id='t14_1',
            name='Persons with Spanish nationality and illiterate',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['nationality'], units['people'], spain],
            targets={illiterate: DENOMINATOR,
                     spanish_nationality: DENOMINATOR})
        illiterate_foreign_nationality = OBSColumn(
            id='t14_2',
            name='Persons with a foreign nationality and illiterate',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['nationality'], units['people'], spain],
            targets={illiterate: DENOMINATOR,
                     foreign_nationality: DENOMINATOR})
        without_studies_spanish_nationality = OBSColumn(
            id='t14_3',
            name='Persons with Spanish nationality and without studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['nationality'], units['people'], spain],
            targets={without_studies: DENOMINATOR,
                     spanish_nationality: DENOMINATOR})
        without_studies_foreign_nationality = OBSColumn(
            id='t14_4',
            name='Persons with a foreign nationality and without studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['nationality'], units['people'], spain],
            targets={without_studies: DENOMINATOR,
                     foreign_nationality: DENOMINATOR})
        first_level_studies_spanish_nationality = OBSColumn(
            id='t14_5',
            name='Persons with Spanish nationality and with first level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['nationality'], units['people'], spain],
            targets={first_level_studies: DENOMINATOR,
                     spanish_nationality: DENOMINATOR})
        first_level_studies_foreign_nationality = OBSColumn(
            id='t14_6',
            name='Persons with a foreign nationality and with first level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['nationality'], units['people'], spain],
            targets={first_level_studies: DENOMINATOR,
                     foreign_nationality: DENOMINATOR})
        second_level_studies_spanish_nationality = OBSColumn(
            id='t14_7',
            name='Persons with Spanish nationality and with second level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['nationality'], units['people'], spain],
            targets={second_level_studies: DENOMINATOR,
                     spanish_nationality: DENOMINATOR})
        second_level_studies_foreign_nationality = OBSColumn(
            id='t14_8',
            name='Persons with a foreign nationality and with second level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['nationality'], units['people'], spain],
            targets={second_level_studies: DENOMINATOR,
                     foreign_nationality: DENOMINATOR})
        third_level_studies_spanish_nationality = OBSColumn(
            id='t14_9',
            name='Persons with Spanish nationality and with third level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['nationality'], units['people'], spain],
            targets={third_level_studies: DENOMINATOR,
                     spanish_nationality: DENOMINATOR})
        third_level_studies_foreign_nationality = OBSColumn(
            id='t14_10',
            name='Persons with a foreign nationality and with third level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['nationality'], units['people'], spain],
            targets={third_level_studies: DENOMINATOR,
                     foreign_nationality: DENOMINATOR})
        no_education_info_spanish_nationality_under_16 = OBSColumn(
            id='t14_11',
            name='Persons with Spanish nationality without information regarding '
            'their educational level (under 16 years of age)',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=1,
            tags=[tags['education'], tags['nationality'], tags['age_gender'], units['people'], spain],
            targets={no_education_info_under_16: DENOMINATOR,
                     spanish_nationality: DENOMINATOR})
        no_education_info_foreign_nationality_under_16 = OBSColumn(
            id='t14_12',
            name='Persons with a foreign nationality without information '
            'regarding their educational level (under 16 years of age)',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=1,
            tags=[tags['education'], tags['nationality'], tags['age_gender'], units['people'], spain],
            targets={no_education_info_under_16: DENOMINATOR,
                     spanish_nationality: DENOMINATOR})
        illiterate_16_to_64 = OBSColumn(
            id='t15_2',
            name='Persons aged between 16 (included) and 64 (included) years and illiterate',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={pop_16_to_64: DENOMINATOR,
                     illiterate: DENOMINATOR})
        illiterate_over_64 = OBSColumn(
            id='t15_3',
            name='Persons aged over 64 years and illiterate',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={pop_over_64: DENOMINATOR,
                     illiterate: DENOMINATOR})
        without_studies_16_to_64 = OBSColumn(
            id='t15_5',
            name='Persons aged between 16 (included) and 64 (included) years and without studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={pop_16_to_64: DENOMINATOR,
                     without_studies: DENOMINATOR})
        without_studies_over_64 = OBSColumn(
            id='t15_6',
            name='Persons aged over 64 years and without studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={pop_over_64: DENOMINATOR,
                     without_studies: DENOMINATOR})
        first_level_studies_16_to_64 = OBSColumn(
            id='t15_8',
            name='Persons aged between 16 (included) and 64 (included) years and with first level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={pop_16_to_64: DENOMINATOR,
                     first_level_studies: DENOMINATOR})
        first_level_studies_over_64 = OBSColumn(
            id='t15_9',
            name='Persons aged over 64 years and with first level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={pop_over_64: DENOMINATOR,
                     first_level_studies: DENOMINATOR})
        second_level_studies_16_to_64 = OBSColumn(
            id='t15_11',
            name='Persons aged between 16 (included) and 64 (included) years and with second level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={pop_16_to_64: DENOMINATOR,
                     first_level_studies: DENOMINATOR})
        second_level_studies_over_64 = OBSColumn(
            id='t15_12',
            name='Persons aged over 64 years and with second level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={pop_over_64: DENOMINATOR,
                     first_level_studies: DENOMINATOR})
        third_level_studies_16_to_64 = OBSColumn(
            id='t15_14',
            name='Persons aged between 16 (included) and 64 (included) years and with third level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={pop_16_to_64: DENOMINATOR,
                     first_level_studies: DENOMINATOR})
        third_level_studies_over_64 = OBSColumn(
            id='t15_15',
            name='Persons aged over 64 years and with third level studies',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={pop_over_64: DENOMINATOR,
                     first_level_studies: DENOMINATOR})
        no_education_info_under_16_2 = OBSColumn(
            id='t15_16',
            name='Persons aged under 16 years (information regarding the educational level is not available)',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=0,
            tags=[tags['education'], tags['age_gender'], units['people'], spain],
            targets={pop_under_16: DENOMINATOR})
        dwellings = OBSColumn(
            id='t16_1',
            name='Total dwellings',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain])
        main_dwellings = OBSColumn(
            id='t17_1',
            name='Main dwellings',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        secondary_dwellings = OBSColumn(
            id='t17_2',
            name='Secondary dwellings',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        empty_dwellings = OBSColumn(
            id='t17_3',
            name='Empty dwellings',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_owned_purchased_paid = OBSColumn(
            id='t18_1',
            name='Dwellings owned, by purchase, totally paid',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_owned_purchased_outstanding = OBSColumn(
            id='t18_2',
            name='Dwellings owned, by purchase, with outstanding',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_owned_inherited_donated = OBSColumn(
            id='t18_3',
            name='Dwellings owned, by inheritance or donation',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_rented = OBSColumn(
            id='t18_4',
            name='Dwellings rented',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_transferred_free_low_cost = OBSColumn(
            id='t18_5',
            name='Dwellings transferred for free or at low cost',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_other_tenancy = OBSColumn(
            id='t18_6',
            name='Dwellings with another tenancy regime',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_less_30m2 = OBSColumn(
            id='t19_1',
            name='Dwellings with less than 30m2',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_30_to_45m2 = OBSColumn(
            id='t19_2',
            name='Dwellings between 30-45 m2',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_46_to_60m2 = OBSColumn(
            id='t19_3',
            name='Dwellings between 46-60 m2',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_61_to_75m2 = OBSColumn(
            id='t19_4',
            name='Dwellings between 61-75 m2',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_76_to_90m2 = OBSColumn(
            id='t19_5',
            name='Dwellings between 76-90 m2',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_91_to_105m2 = OBSColumn(
            id='t19_6',
            name='Dwellings between 91-105 m2',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_106_to_120m2 = OBSColumn(
            id='t19_7',
            name='Dwellings between 106-120 m2',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_121_to_150m2 = OBSColumn(
            id='t19_8',
            name='Dwellings between 121-150 m2',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_151_to_180m2 = OBSColumn(
            id='t19_9',
            name='Dwellings between 151-180 m2',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_180m2_more = OBSColumn(
            id='t19_10',
            name='Dwellings with more than 180 m2',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_1_room = OBSColumn(
            id='t20_1',
            name='Dwellings with 1 room',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_2_rooms = OBSColumn(
            id='t20_2',
            name='Dwellings with 2 rooms',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_3_rooms = OBSColumn(
            id='t20_3',
            name='Dwellings with 3 rooms',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_4_rooms = OBSColumn(
            id='t20_4',
            name='Dwellings with 4 rooms',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_5_rooms = OBSColumn(
            id='t20_5',
            name='Dwellings with 5 rooms',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_6_rooms = OBSColumn(
            id='t20_6',
            name='Dwellings with 6 rooms',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_7_rooms = OBSColumn(
            id='t20_7',
            name='Dwellings with 7 rooms',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_8_rooms = OBSColumn(
            id='t20_8',
            name='Dwellings with 8 rooms',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        dwellings_9_rooms_more = OBSColumn(
            id='t20_9',
            name='Dwellings with 9 or more rooms',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['housing'], units['housing_units'], spain],
            targets={dwellings: DENOMINATOR})
        households = OBSColumn(
            id='t21_1',
            name='Total households',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], units['households'], spain])
        households_1_person = OBSColumn(
            id='t22_1',
            name='Households with 1 person',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], units['households'], spain],
            targets={households: DENOMINATOR})
        households_2_people = OBSColumn(
            id='t22_2',
            name='Households with 2 persons',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], units['households'], spain],
            targets={households: DENOMINATOR})
        households_3_people = OBSColumn(
            id='t22_3',
            name='Households with 3 persons',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], units['households'], spain],
            targets={households: DENOMINATOR})
        households_4_people = OBSColumn(
            id='t22_4',
            name='Households with 4 persons',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], units['households'], spain],
            targets={households: DENOMINATOR})
        households_5_people = OBSColumn(
            id='t22_5',
            name='Households with 5 persons',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], units['households'], spain],
            targets={households: DENOMINATOR})
        households_6_more_people = OBSColumn(
            id='t22_6',
            name='Households with 6 or more persons',
            description='',
            aggregate='sum',
            type='Numeric',
            weight=5,
            tags=[tags['families'], units['households'], spain],
            targets={households: DENOMINATOR})
        columns = OrderedDict([
            ('total_pop', total_pop),
            ('male_pop', male_pop),
            ('female_pop', female_pop),
            ('pop_under_16', pop_under_16),
            ('pop_16_to_64', pop_16_to_64),
            ('pop_over_64', pop_over_64),
            ('spanish_nationality', spanish_nationality),
            ('foreign_nationality', foreign_nationality),
            ('spain_born', spain_born),
            ('eu_born', eu_born),
            ('europe_non_eu_born', europe_non_eu_born),
            ('african_born', african_born),
            ('central_south_american_caribbean_born', central_south_american_caribbean_born),
            ('north_american_born', north_american_born),
            ('asian_born', asian_born),
            ('oceania_born', oceania_born),
            ('spain_born_spanish_nationality', spain_born_spanish_nationality),
            ('spain_born_foreign_nationality', spain_born_foreign_nationality),
            ('eu_born_spanish_nationality', eu_born_spanish_nationality),
            ('eu_born_foreign_nationality', eu_born_foreign_nationality),
            ('europe_non_eu_born_spanish_nationality', europe_non_eu_born_spanish_nationality),
            ('europe_non_eu_born_foreign_nationality', europe_non_eu_born_foreign_nationality),
            ('african_born_spanish_nationality', african_born_spanish_nationality),
            ('african_born_foreign_nationality', african_born_foreign_nationality),
            ('central_south_american_caribbean_born_spanish_nationality',
             central_south_american_caribbean_born_spanish_nationality),
            ('central_south_american_caribbean_born_foreign_nationality',
             central_south_american_caribbean_born_foreign_nationality),
            ('north_american_born_spanish_nationality', north_american_born_spanish_nationality),
            ('north_american_born_foreign_nationality', north_american_born_foreign_nationality),
            ('asian_born_spanish_nationality', asian_born_spanish_nationality),
            ('asian_born_foreign_nationality', asian_born_foreign_nationality),
            ('oceania_born_spanish_nationality', oceania_born_spanish_nationality),
            ('oceania_born_foreign_nationality', oceania_born_foreign_nationality),
            ('males_under_16', males_under_16),
            ('males_16_to_64', males_16_to_64),
            ('males_over_64', males_over_64),
            ('females_under_16', females_under_16),
            ('females_16_to_64', females_16_to_64),
            ('females_over_64', females_over_64),
            ('males_spanish_nationality', males_spanish_nationality),
            ('males_foreign_nationality', males_foreign_nationality),
            ('females_spanish_nationality', females_spanish_nationality),
            ('females_foreign_nationality', females_foreign_nationality),
            ('spanish_nationality_under_16', spanish_nationality_under_16),
            ('foreign_nationality_under_16', foreign_nationality_under_16),
            ('spanish_nationality_16_to_64', spanish_nationality_16_to_64),
            ('foreign_nationality_16_to_64', foreign_nationality_16_to_64),
            ('spanish_nationality_over_64', spanish_nationality_over_64),
            ('foreign_nationality_over_64', foreign_nationality_over_64),
            ('unmarried', unmarried),
            ('married', married),
            ('separated', separated),
            ('divorced', divorced),
            ('widowed', widowed),
            ('unmarried_under_16', unmarried_under_16),
            ('unmarried_16_to_64', unmarried_16_to_64),
            ('unmarried_over_64', unmarried_over_64),
            ('married_under_16', married_under_16),
            ('married_16_to_64', married_16_to_64),
            ('married_over_64', married_over_64),
            ('separated_under_16', separated_under_16),
            ('separated_16_to_64', separated_16_to_64),
            ('separated_over_64', separated_over_64),
            ('divorced_under_16', divorced_under_16),
            ('divorced_16_to_64', divorced_16_to_64),
            ('divorced_over_64', divorced_over_64),
            ('widowed_under_16', widowed_under_16),
            ('widowed_16_to_64', widowed_16_to_64),
            ('widowed_over_64', widowed_over_64),
            ('illiterate', illiterate),
            ('without_studies', without_studies),
            ('first_level_studies', first_level_studies),
            ('second_level_studies', second_level_studies),
            ('third_level_studies', third_level_studies),
            ('no_education_info_under_16', no_education_info_under_16),
            ('illiterate_male_pop', illiterate_male_pop),
            ('illiterate_female_pop', illiterate_female_pop),
            ('male_without_studies', male_without_studies),
            ('female_without_studies', female_without_studies),
            ('male_first_level_studies', male_first_level_studies),
            ('female_first_level_studies', female_first_level_studies),
            ('male_second_level_studies', male_second_level_studies),
            ('female_second_level_studies', female_second_level_studies),
            ('male_third_level_studies', male_third_level_studies),
            ('female_third_level_studies', female_third_level_studies),
            ('male_no_education_info_under_16', male_no_education_info_under_16),
            ('female_no_education_info_under_16', female_no_education_info_under_16),
            ('illiterate_spanish_nationality', illiterate_spanish_nationality),
            ('illiterate_foreign_nationality', illiterate_foreign_nationality),
            ('without_studies_spanish_nationality', without_studies_spanish_nationality),
            ('without_studies_foreign_nationality', without_studies_foreign_nationality),
            ('first_level_studies_spanish_nationality', first_level_studies_spanish_nationality),
            ('first_level_studies_foreign_nationality', first_level_studies_foreign_nationality),
            ('second_level_studies_spanish_nationality', second_level_studies_spanish_nationality),
            ('second_level_studies_foreign_nationality', second_level_studies_foreign_nationality),
            ('third_level_studies_spanish_nationality', third_level_studies_spanish_nationality),
            ('third_level_studies_foreign_nationality', third_level_studies_foreign_nationality),
            ('no_education_info_spanish_nationality_under_16',
             no_education_info_spanish_nationality_under_16),
            ('no_education_info_foreign_nationality_under_16',
             no_education_info_foreign_nationality_under_16),
            ('illiterate_16_to_64', illiterate_16_to_64),
            ('illiterate_over_64', illiterate_over_64),
            ('without_studies_16_to_64', without_studies_16_to_64),
            ('without_studies_over_64', without_studies_over_64),
            ('first_level_studies_16_to_64', first_level_studies_16_to_64),
            ('first_level_studies_over_64', first_level_studies_over_64),
            ('second_level_studies_16_to_64', second_level_studies_16_to_64),
            ('second_level_studies_over_64', second_level_studies_over_64),
            ('third_level_studies_16_to_64', third_level_studies_16_to_64),
            ('third_level_studies_over_64', third_level_studies_over_64),
            ('no_education_info_under_16_2', no_education_info_under_16_2),
            ('dwellings', dwellings),
            ('main_dwellings', main_dwellings),
            ('secondary_dwellings', secondary_dwellings),
            ('empty_dwellings', empty_dwellings),
            ('dwellings_owned_purchased_paid', dwellings_owned_purchased_paid),
            ('dwellings_owned_purchased_outstanding', dwellings_owned_purchased_outstanding),
            ('dwellings_owned_inherited_donated', dwellings_owned_inherited_donated),
            ('dwellings_rented', dwellings_rented),
            ('dwellings_transferred_free_low_cost', dwellings_transferred_free_low_cost),
            ('dwellings_other_tenancy', dwellings_other_tenancy),
            ('dwellings_less_30m2', dwellings_less_30m2),
            ('dwellings_30_to_45m2', dwellings_30_to_45m2),
            ('dwellings_46_to_60m2', dwellings_46_to_60m2),
            ('dwellings_61_to_75m2', dwellings_61_to_75m2),
            ('dwellings_76_to_90m2', dwellings_76_to_90m2),
            ('dwellings_91_to_105m2', dwellings_91_to_105m2),
            ('dwellings_106_to_120m2', dwellings_106_to_120m2),
            ('dwellings_121_to_150m2', dwellings_121_to_150m2),
            ('dwellings_151_to_180m2', dwellings_151_to_180m2),
            ('dwellings_180m2_more', dwellings_180m2_more),
            ('dwellings_1_room', dwellings_1_room),
            ('dwellings_2_rooms', dwellings_2_rooms),
            ('dwellings_3_rooms', dwellings_3_rooms),
            ('dwellings_4_rooms', dwellings_4_rooms),
            ('dwellings_5_rooms', dwellings_5_rooms),
            ('dwellings_6_rooms', dwellings_6_rooms),
            ('dwellings_7_rooms', dwellings_7_rooms),
            ('dwellings_8_rooms', dwellings_8_rooms),
            ('dwellings_9_rooms_more', dwellings_9_rooms_more),
            ('households', households),
            ('households_1_person', households_1_person),
            ('households_2_people', households_2_people),
            ('households_3_people', households_3_people),
            ('households_4_people', households_4_people),
            ('households_5_people', households_5_people),
            ('households_6_more_people', households_6_more_people),
            ])

        for _, col in columns.iteritems():
            col.tags.append(source)
            col.tags.append(license)
        return columns


class LicenseTags(TagsTask):
    def version(self):
        return 1

    def tags(self):
        return [OBSTag(id='ine-license',
                name='National Statistics Institute (INE)',
                type='license',
                description='`INE website <http://www.ine.es>`_')
            ]


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


class SeccionDataDownload(Task):

    URL = 'http://www.ine.es/en/censos2011_datos/indicadores_seccion_censal_csv_en.zip'
    # inside that URL, concatenate all CSVs together to get all seccions

    def run(self):
        self.output().makedirs()
        cmd = 'wget "{url}" -O {output_dir}.zip && ' \
                'mkdir -p {output_dir} && ' \
                'unzip -o {output_dir}.zip -d {output_dir} && ' \
                'tail -n +2 -q {output_dir}/*.csv > {output_csv}'.format(
                    url=self.URL,
                    output_dir=self.output().path.replace('.csv', ''),
                    output_csv=self.output().path)
        shell(cmd)

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id) + '.csv')


class PopulationHouseholdsHousing(TableTask):

    def requires(self):
        return {
            'meta': SeccionColumns(),
            'geometa': GeometryColumns(),
            'data': SeccionDataDownload(),
        }

    def version(self):
        return 1

    def timespan(self):
        return '2011'

    def columns(self):
        '''
        Add the geoid (cusec_id) column into first position as expected
        '''
        metacols = self.input()['meta']
        cols = OrderedDict()
        cols['cusec_id'] = self.input()['geometa']['cusec_id']
        for key, col in metacols.iteritems():
            cols[key] = col
        return cols

    def populate(self):
        session = current_session()
        with self.input()['data'].open() as infile:
            for fields in csv.reader(infile):
                cusec = "'" + ''.join(fields[0:5]) + "'"
                fields = [f.replace('"', '') for f in fields[5:]]
                fields = [f or 'NULL' for f in fields]
                fields.insert(0, cusec)
                stmt = 'INSERT INTO {output} VALUES ({values})'.format(
                    output=self.output().table,
                    values=', '.join(fields))
                session.execute(stmt)


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
        return 10

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
        return 3

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
