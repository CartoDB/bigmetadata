# http://webarchive.nationalarchives.gov.uk/20160105160709/http://ons.gov.uk/ons/guide-method/census/2011/census-data/bulk-data/bulk-data-downloads/index.html

from luigi import Task, Parameter, LocalTarget, WrapperTask

from tasks.meta import OBSColumn, DENOMINATOR, current_session, OBSTag
from tasks.util import (TableTask, ColumnsTask, classpath, shell,
                        DownloadUnzipTask, TagsTask, TempTableTask, MetaWrapper)
from tasks.uk.cdrc import OutputAreaColumns, OutputAreas
from tasks.tags import UnitTags, SectionTags, SubsectionTags, LicenseTags

from collections import OrderedDict
import os

class SourceTags(TagsTask):

    def version(self):
        return 1

    def tags(self):
        return [OBSTag(id='ons',
                    name='Office for National Statistics (ONS)',
                    type='source',
                    description="The UK's largest independent producer of official statistics and the recognised national statistical institute of the UK (`ONS <https://www.ons.gov.uk/>`_)")]


class DownloadEnglandWalesLocal(DownloadUnzipTask):

    URL = 'http://data.statistics.gov.uk/Census/BulkLocalCharacteristicsoaandinfo310713.zip'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(output=self.output().path,
                                                  url=self.URL))


class ImportEnglandWalesLocal(TempTableTask):

    table = Parameter()

    def requires(self):
        return DownloadEnglandWalesLocal()

    def run(self):
        session = current_session()
        infile = os.path.join(self.input().path, self.table + 'DATA.CSV')
        headers = shell('head -n 1 {csv}'.format(csv=infile))
        cols = ['{} NUMERIC'.format(h) for h in headers.split(',')[1:]]
        session.execute('CREATE TABLE {output} (GeographyCode TEXT, {cols})'.format(
            output=self.output().table,
            cols=', '.join(cols)
        ))
        session.commit()
        shell("cat '{infile}' | psql -c 'COPY {output} FROM STDIN WITH CSV HEADER'".format(
            output=self.output().table,
            infile=infile,
        ))
        session.execute('ALTER TABLE {output} ADD PRIMARY KEY (geographycode)'.format(
            output=self.output().table
        ))


class CensusColumns(ColumnsTask):

    def requires(self):
        return {
            'units': UnitTags(),
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'source': SourceTags(),
            'license': LicenseTags(),
        }

    def version(self):
        return 4

    def columns(self):
        input_ = self.input()
        source = input_['source']['ons']
        license = input_['license']['uk_ogl']
        uk = input_['sections']['uk']
        subsections = input_['subsections']
        units = input_['units']
        total_pop = OBSColumn(
            id='LC2102EW0001',
            name='Total Population',
            description='',
            type='Numeric',
            weight=10,
            aggregate='sum',
            tags=[uk, units['people'], subsections['age_gender']],
        )
        pop_0_to_24 = OBSColumn(
            id='LC2102EW0016',
            name='Population age 0 to 24',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['age_gender']],
        )
        pop_25_to_49 = OBSColumn(
            id='LC2102EW0031',
            name='Population age 25 to 49',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['age_gender'], ],
        )
        pop_50_to_64 = OBSColumn(
            id='LC2102EW0046',
            name='Population age 50 to 64',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['age_gender'], ],
        )
        pop_65_to_74 = OBSColumn(
            id='LC2102EW0061',
            name='Population age 65 to 74',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['age_gender'], ],
        )
        pop_75_and_over = OBSColumn(
            id='LC2102EW0076',
            name='Population age 75 and over',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['age_gender'], ],
        )
        pop_3_and_over = OBSColumn(
            id='LC2104EW0001',
            name='Population age 3 and over',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[uk, units['people'], subsections['age_gender'], ],
        )
        pop_3_to_15 = OBSColumn(
            id='LC2104EW0024',
            name='Population age 3 to 15',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_3_and_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['age_gender'], ],
        )
        pop_16_to_49 = OBSColumn(
            id='LC2104EW0047',
            name='Population age 16 to 49',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_3_and_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['age_gender'], ],
        )
        language_english = OBSColumn(
            id='LC2104EW0002',
            name='Population whose main language is English (English or Welsh '
                 'if in Wales)',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_3_and_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['language'], ],
        )
        language_french = OBSColumn(
            id='LC2104EW0003',
            name='Population whose main language is French',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_3_and_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['language'], ],
        )
        language_portuguese = OBSColumn(
            id='LC2104EW0004',
            name='Population whose main language is Portuguese',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_3_and_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['language'], ],
        )
        language_spanish = OBSColumn(
            id='LC2104EW0005',
            name='Population whose main language is Spanish',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_3_and_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['language'], ],
        )
        language_polish = OBSColumn(
            id='LC2104EW0007',
            name='Population whose main language is Polish',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_3_and_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['language'], ],
        )
        language_arabic = OBSColumn(
            id='LC2104EW0010',
            name='Population whose main language is Arabic',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_3_and_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['language'], ],
        )
        language_panjabi = OBSColumn(
            id='LC2104EW0013',
            name='Population whose main language is Panjabi',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_3_and_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['language'], ],
        )
        language_urdu = OBSColumn(
            id='LC2104EW0014',
            name='Population whose main language is Urdu',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_3_and_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['language'], ],
        )
        language_bengali = OBSColumn(
            id='LC2104EW0015',
            name='Population whose main language is Bengali (with Sylheti and '
                 'Chatgaya)',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_3_and_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['language'], ],
        )
        language_gujarati = OBSColumn(
            id='LC2104EW0016',
            name='Population whose main language is Gujarati',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_3_and_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['language'], ],
        )
        language_tamil = OBSColumn(
            id='LC2104EW0017',
            name='Population whose main language is Tamil',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_3_and_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['language'], ],
        )
        language_chinese = OBSColumn(
            id='LC2104EW0020',
            name='Population whose main language is Chinese',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_3_and_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['language'], ],
        )
        religion_christian = OBSColumn(
            id='LC2107EW0002',
            name='Christian Population',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['religion'], ],
        )
        religion_buddhist = OBSColumn(
            id='LC2107EW0003',
            name='Buddhist Population',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['religion'], ],
        )
        religion_hindu = OBSColumn(
            id='LC2107EW0004',
            name='Hindu Population',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['religion'], ],
        )
        religion_jewish = OBSColumn(
            id='LC2107EW0005',
            name='Jewish Population',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['religion'], ],
        )
        religion_muslim = OBSColumn(
            id='LC2107EW0006',
            name='Muslim Population',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['religion'], ],
        )
        religion_sikh = OBSColumn(
            id='LC2107EW0007',
            name='Sikh Population',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['religion'], ],
        )
        religion_none = OBSColumn(
            id='LC2107EW0009',
            name='Population who are not religious',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['religion'], ],
        )
        white = OBSColumn(
            id='LC2201EW0011',
            name='White population',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['race_ethnicity'], ],
        )
        white_english_welsh_scottish_n_irish_british = OBSColumn(
            id='LC2201EW0021',
            name='White English/Welsh/Scottish/Northern Irish/British population',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={white: DENOMINATOR},
            tags=[uk, units['people'], subsections['race_ethnicity'], ],
        )
        white_irish = OBSColumn(
            id='LC2201EW0031',
            name='White Irish population',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={white: DENOMINATOR},
            tags=[uk, units['people'], subsections['race_ethnicity'], ],
        )
        mixed_multiple_ethnic_group = OBSColumn(
            id='LC2201EW0051',
            name='Mixed/multiple ethnic group population',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['race_ethnicity'], ],
        )
        asian_pop = OBSColumn(
            id='LC2201EW0061',
            name='Asian/Asian British population',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['race_ethnicity'], ],
        )
        black_pop = OBSColumn(
            id='LC2201EW0071',
            name='Black/African/Caribbean/Black British population',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['race_ethnicity'], ],
        )
        other_race_pop = OBSColumn(
            id='LC2201EW0081',
            name='Other ethnic group population',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['race_ethnicity'], ],
        )
        born_europe = OBSColumn(
            id='LC2205EW0010',
            name='Born in Europe',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_uk = OBSColumn(
            id='LC2205EW0019',
            name='Born in the UK',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={born_europe: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_england = OBSColumn(
            id='LC2205EW0028',
            name='Born in England',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={born_uk: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_northern_ireland = OBSColumn(
            id='LC2205EW0037',
            name='Born in Northern Ireland',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={born_uk: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_scotland = OBSColumn(
            id='LC2205EW0046',
            name='Born in Scotland',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={born_uk: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_wales = OBSColumn(
            id='LC2205EW0055',
            name='Born in Wales',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={born_uk: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_ireland = OBSColumn(
            id='LC2205EW0082',
            name='Born in Ireland',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_europe_outside_uk_ireland = OBSColumn(
            id='LC2205EW0091',
            name='Born in Europe outside the UK and Ireland',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_africa = OBSColumn(
            id='LC2205EW0136',
            name='Born in Africa',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_north_africa = OBSColumn(
            id='LC2205EW0145',
            name='Born in North Africa',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={born_africa: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_central_western_africa = OBSColumn(
            id='LC2205EW0154',
            name='Born in Central and Western Africa',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={born_africa: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_south_eastern_africa = OBSColumn(
            id='LC2205EW0163',
            name='Born in South and Eastern Africa',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={born_africa: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_asia_middle_east = OBSColumn(
            id='LC2205EW0181',
            name='Born in the Middle East and Asia',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_middle_east = OBSColumn(
            id='LC2205EW0190',
            name='Born in the Middle East',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={born_asia_middle_east: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_eastern_asia = OBSColumn(
            id='LC2205EW0199',
            name='Born in Eastern Asia',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={born_asia_middle_east: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_southern_asia = OBSColumn(
            id='LC2205EW0208',
            name='Born in Southern Asia',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={born_asia_middle_east: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_southeast_asia = OBSColumn(
            id='LC2205EW0217',
            name='Born in Southeast Asia',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={born_asia_middle_east: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_central_asia = OBSColumn(
            id='LC2205EW0226',
            name='Born in Central Asia',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={born_asia_middle_east: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_americas = OBSColumn(
            id='LC2205EW0235',
            name='Born in the Americas and Caribbean',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_north_america = OBSColumn(
            id='LC2205EW0244',
            name='Born in North America',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={born_americas: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_central_south_america = OBSColumn(
            id='LC2205EW0253',
            name='Born in Central and South America',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={born_americas: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        born_australia = OBSColumn(
            id='LC2205EW0262',
            name='Born in Australia, Australaisa, Oceania, and Antarctica',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={total_pop: DENOMINATOR},
            tags=[uk, units['people'], subsections['nationality'], ],
        )
        good_health = OBSColumn(
            id='LC3202WA0004',
            name='Very good or good health',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_3_and_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['health'], ],
        )
        fair_health = OBSColumn(
            id='LC3202WA0007',
            name='Fair health',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_3_and_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['health'], ],
        )
        bad_health = OBSColumn(
            id='LC3202WA0010',
            name='Bad or very bad health',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_3_and_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['health'], ],
        )
        very_limited_activities = OBSColumn(
            id='LC3204WA0004',
            name='Day-to-day activities very limited',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_3_and_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['health'], ],
        )
        limited_activities = OBSColumn(
            id='LC3204WA0007',
            name='Day-to-day activities somewhat limited',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_3_and_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['health'], ],
        )
        activities_not_limited = OBSColumn(
            id='LC3204WA0010',
            name='Day-to-day activities not limited',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_3_and_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['health'], ],
        )
        pop_16_over = OBSColumn(
            id='LC6201EW0001',
            name='Population age 16 and over',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[uk, units['people'], subsections['employment'], ],
        )
        economically_active = OBSColumn(
            id='LC6201EW0010',
            name='Economically active',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_16_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        employed = OBSColumn(
            id='LC6201EW0019',
            name='Employed population',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={economically_active: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        employed_as_employee = OBSColumn(
            id='LC6201EW0028',
            name='Employed as an employee',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={employed: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        part_time_employee = OBSColumn(
            id='LC6201EW0037',
            name='Part-time employees',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={employed_as_employee: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        full_time_employee = OBSColumn(
            id='LC6201EW0046',
            name='Full-time employees',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={employed_as_employee: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        self_employed = OBSColumn(
            id='LC6201EW0055',
            name='Self-employed',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={employed: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        self_employed_part_time = OBSColumn(
            id='LC6201EW0064',
            name='Self-employed part-time',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={self_employed: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        self_employed_full_time = OBSColumn(
            id='LC6201EW0073',
            name='Self-employed full-time',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={self_employed: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        full_time_students = OBSColumn(
            id='LC6201EW0082',
            name='Full-time students',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={employed: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        unemployed = OBSColumn(
            id='LC6201EW0091',
            name='Unemployed',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={economically_active: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        unemployed_not_student = OBSColumn(
            id='LC6201EW0100',
            name='Unemployed (excluding full-time students)',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={unemployed: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        unemployed_student = OBSColumn(
            id='LC6201EW0109',
            name='Full-time students, unemployed',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={unemployed: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        economically_inactive = OBSColumn(
            id='LC6201EW0118',
            name='Economically inactive',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_16_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        retired = OBSColumn(
            id='LC6201EW0127',
            name='Retired population',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={economically_inactive: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        student_economically_inactive = OBSColumn(
            id='LC6201EW0136',
            name='Economically inactive students',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={economically_inactive: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        looking_after_home = OBSColumn(
            id='LC6201EW0145',
            name='Looking after home or family',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={economically_inactive: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        long_term_sick_disabled = OBSColumn(
            id='LC6201EW0154',
            name='Long-term sick or disabled',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={economically_inactive: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        managerial_admin_occupations = OBSColumn(
            id='LC6206EW0010',
            name='Higher managerial, administrative and professional occupations',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_16_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        large_employers = OBSColumn(
            id='LC6206EW0019',
            name='Large employers and higher managerial and administrative '
                 'occupations',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={managerial_admin_occupations: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        higher_professional_occupations = OBSColumn(
            id='LC6206EW0028',
            name='Higher professional occupations',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={managerial_admin_occupations: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        lower_managerial_occupations = OBSColumn(
            id='LC6206EW0037',
            name='Lower managerial, administrative and professional occupations',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_16_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        intermediate_occupations = OBSColumn(
            id='LC6206EW0046',
            name='Intermediate occupations',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_16_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        small_employers_account_workers = OBSColumn(
            id='LC6206EW0055',
            name='Small employers and account workers',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_16_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        lower_supervisory = OBSColumn(
            id='LC6206EW0064',
            name='Lower supervisory and technical occupations',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_16_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        semi_routine_occupations = OBSColumn(
            id='LC6206EW0073',
            name='Semi-routine occupations',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_16_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        routine_occupations = OBSColumn(
            id='LC6206EW0082',
            name='Routine occupations',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_16_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        never_worked_long_term_unemployed = OBSColumn(
            id='LC6206EW0091',
            name='Never worked and long-term unemployed',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={pop_16_over: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        never_worked = OBSColumn(
            id='LC6206EW0100',
            name='Never worked',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={never_worked_long_term_unemployed: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )
        long_term_unemployed = OBSColumn(
            id='LC6206EW0109',
            name='Long-term unemployed',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            targets={never_worked_long_term_unemployed: DENOMINATOR},
            tags=[uk, units['people'], subsections['employment'], ],
        )

        columns = OrderedDict([
            ('total_pop', total_pop),
            ('pop_0_to_24', pop_0_to_24),
            ('pop_25_to_49', pop_25_to_49),
            ('pop_50_to_64', pop_50_to_64),
            ('pop_65_to_74', pop_65_to_74),
            ('pop_75_and_over', pop_75_and_over),
            ('pop_3_and_over', pop_3_and_over),
            ('pop_3_to_15', pop_3_to_15),
            ('pop_16_to_49', pop_16_to_49),
            ('language_english', language_english),
            ('language_french', language_french),
            ('language_portuguese', language_portuguese),
            ('language_spanish', language_spanish),
            ('language_polish', language_polish),
            ('language_arabic', language_arabic),
            ('language_panjabi', language_panjabi),
            ('language_urdu', language_urdu),
            ('language_bengali', language_bengali),
            ('language_gujarati', language_gujarati),
            ('language_tamil', language_tamil),
            ('language_chinese', language_chinese),
            ('religion_christian', religion_christian),
            ('religion_buddhist', religion_buddhist),
            ('religion_hindu', religion_hindu),
            ('religion_jewish', religion_jewish),
            ('religion_muslim', religion_muslim),
            ('religion_sikh', religion_sikh),
            ('religion_none', religion_none),
            ('white', white),
            ('white_english_welsh_scottish_n_irish_british', white_english_welsh_scottish_n_irish_british),
            ('white_irish', white_irish),
            ('mixed_multiple_ethnic_group', mixed_multiple_ethnic_group),
            ('asian_pop', asian_pop),
            ('black_pop', black_pop),
            ('other_race_pop', other_race_pop),
            ('born_europe', born_europe),
            ('born_uk', born_uk),
            ('born_england', born_england),
            ('born_northern_ireland', born_northern_ireland),
            ('born_scotland', born_scotland),
            ('born_wales', born_wales),
            ('born_ireland', born_ireland),
            ('born_europe_outside_uk_ireland', born_europe_outside_uk_ireland),
            ('born_africa', born_africa),
            ('born_north_africa', born_north_africa),
            ('born_central_western_africa', born_central_western_africa),
            ('born_south_eastern_africa', born_south_eastern_africa),
            ('born_asia_middle_east', born_asia_middle_east),
            ('born_middle_east', born_middle_east),
            ('born_eastern_asia', born_eastern_asia),
            ('born_southern_asia', born_southern_asia),
            ('born_southeast_asia', born_southeast_asia),
            ('born_central_asia', born_central_asia),
            ('born_americas', born_americas),
            ('born_north_america', born_north_america),
            ('born_central_south_america', born_central_south_america),
            ('born_australia', born_australia),
            ('good_health', good_health),
            ('fair_health', fair_health),
            ('bad_health', bad_health),
            ('very_limited_activities', very_limited_activities),
            ('limited_activities', limited_activities),
            ('activities_not_limited', activities_not_limited),
            ('pop_16_over', pop_16_over),
            ('economically_active', economically_active),
            ('employed', employed),
            ('employed_as_employee', employed_as_employee),
            ('part_time_employee', part_time_employee),
            ('full_time_employee', full_time_employee),
            ('self_employed', self_employed),
            ('self_employed_part_time', self_employed_part_time),
            ('self_employed_full_time', self_employed_full_time),
            ('full_time_students', full_time_students),
            ('unemployed', unemployed),
            ('unemployed_not_student', unemployed_not_student),
            ('unemployed_student', unemployed_student),
            ('economically_inactive', economically_inactive),
            ('retired', retired),
            ('student_economically_inactive', student_economically_inactive),
            ('looking_after_home', looking_after_home),
            ('long_term_sick_disabled', long_term_sick_disabled),
            ('managerial_admin_occupations', managerial_admin_occupations),
            ('large_employers', large_employers),
            ('higher_professional_occupations', higher_professional_occupations),
            ('lower_managerial_occupations', lower_managerial_occupations),
            ('intermediate_occupations', intermediate_occupations),
            ('small_employers_account_workers', small_employers_account_workers),
            ('lower_supervisory', lower_supervisory),
            ('semi_routine_occupations', semi_routine_occupations),
            ('routine_occupations', routine_occupations),
            ('never_worked_long_term_unemployed', never_worked_long_term_unemployed),
            ('never_worked', never_worked),
            ('long_term_unemployed', long_term_unemployed),
        ])
        for _, col in columns.iteritems():
            col.tags.append(source)
            col.tags.append(license)
        return columns

class ImportAllEnglandWalesLocal(Task):

    def requires(self):
        return DownloadEnglandWalesLocal()

    def run(self):
        infiles = shell('ls {input}/LC*DATA.CSV'.format(
            input=self.input().path))
        fhandle = self.output().open('w')
        for infile in infiles.strip().split('\n'):
            table = os.path.split(infile)[-1].split('DATA.CSV')[0]
            data = yield ImportEnglandWalesLocal(table=table)
            fhandle.write('{table}\n'.format(table=data.table))
        fhandle.close()

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))


class EnglandWalesLocal(TableTask):

    def requires(self):
        return {
            'data': ImportAllEnglandWalesLocal(),
            'geom_columns': OutputAreaColumns(),
            'data_columns': CensusColumns(),
        }

    def timespan(self):
        return 2011

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()
        cols['GeographyCode'] = input_['geom_columns']['oa_sa']
        cols.update(input_['data_columns'])
        return cols

    def populate(self):
        session = current_session()
        cols = self.columns()
        inserted = False
        for line in self.input()['data'].open():
            intable = line.strip()
            table = intable.split('_')[1]
            cols_for_table = OrderedDict([
                (n, t,) for n, t in cols.iteritems() if table in t._id
            ])
            out_colnames = cols_for_table.keys()
            in_colnames = [t._id.split('.')[-1] for t in cols_for_table.values()]
            assert len(out_colnames) == len(in_colnames)
            if not out_colnames:
                continue
            if not inserted:
                stmt = 'INSERT INTO {output} (geographycode, {out_colnames}) ' \
                        'SELECT geographycode, {in_colnames} ' \
                        'FROM {input} ' \
                        'WHERE geographycode LIKE \'E00%\' OR ' \
                        '      geographycode LIKE \'W00%\''.format(
                            output=self.output().table,
                            out_colnames=', '.join(out_colnames),
                            in_colnames=', '.join(in_colnames),
                            input=intable)
                session.execute(stmt)
                inserted = True
            else:
                stmt = 'UPDATE {output} out ' \
                        'SET {set} ' \
                        'FROM {input} intable ' \
                        'WHERE intable.geographycode = out.geographycode ' \
                        .format(
                            set=', '.join([
                                '{} = {}'.format(a, b) for a, b in zip(
                                    out_colnames, in_colnames)
                            ]),
                            output=self.output().table,
                            input=intable)
                session.execute(stmt)

class EnglandWalesMetaWrapper(MetaWrapper):
    def tables(self):
        yield EnglandWalesLocal()
        yield OutputAreas()
