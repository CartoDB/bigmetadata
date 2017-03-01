from tasks.eu.geo import NUTSColumns
from tasks.meta import (OBSTable, OBSColumn, OBSTag, current_session,
                        DENOMINATOR, GEOM_REF, UNIVERSE)
from tasks.tags import SectionTags, SubsectionTags, UnitTags
from tasks.util import (Shp2TempTableTask, TempTableTask, TableTask, TagsTask, ColumnsTask,
                        DownloadUnzipTask, CSV2TempTableTask, MetaWrapper,
                        underscore_slugify, shell, classpath, LOGGER)

from luigi import IntParameter, Parameter, WrapperTask, Task, LocalTarget, ListParameter
from collections import OrderedDict
from time import time

import csv
import os
import re
import itertools

#
# dl_code_list = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&downfile=dic%2Fen%2F{code}.dic".format(code=code)
# flag_explanation = "http://ec.europa.eu/eurostat/data/database/information"
# database = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?dir=data&sort=1&sort=2&start={}".format(first_letter)
# dl_data = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&downfile=data%2F{}.tsv.gz".format(table_code)
# dl_data = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=data%2Fdemo_r_pjangrp3.tsv.gz

class DownloadEurostat(Task):
    table_code = Parameter()
    URL = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=data%2F{code}.tsv.gz"

    def download(self):
        url = self.URL.format(code=self.table_code.lower())
        shell('wget -O {output}.gz "{url}"'.format(
            output=self.output().path,
            url=url))

    def run(self):
        self.output().makedirs()
        self.download()
        shell('gunzip {output}.gz'.format(output=self.output().path))

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))


class ProcessCSV(Task):
    table_code = Parameter()

    def requires(self):
        return DownloadEurostat(table_code=self.table_code)

    def run(self):
        shell("cat {infile} | tr '\' ',' | tr '\t' ',' > {outfile}".format(
            outfile=self.output().path,
            infile=self.input().path))

    def output(self):
        return LocalTarget(self.input().path + '.csv')


class EUTempTable(CSV2TempTableTask):

    delimiter = Parameter(default=',', significant=False)
    table_name = Parameter() # Ex. "DEMO_R_PJANAGGR3"

    def version(self):
        return 5

    def requires(self):
        return ProcessCSV(table_code=self.table_name)

    def coldef(self):
        coldefs = super(EUTempTable, self).coldef()
        for i, cd in enumerate(coldefs):
            cdtemp = list(cd)
            cdtemp[0] = cdtemp[0].strip()
            newcd = tuple(cdtemp)
            coldefs[i] = newcd
        return coldefs

    def input_csv(self):
        return self.input().path


class DICTablesCache(object):

    def __init__(self):
        self._cache = {}

    def get(self, fname):
        if fname not in self._cache:
            LOGGER.info('Caching %s', fname)
            with open(os.path.join(os.path.dirname(__file__), fname), 'r') as fhandle:
                reader = csv.reader(fhandle, delimiter='\t')
                self._cache[fname] = {}
                for key, val in reader:
                    self._cache[fname][key] = val

            LOGGER.info('Cached %s, with %s lines', fname, len(self._cache[fname]))
        else:
            LOGGER.debug('Cache hit for %s', fname)

        return self._cache[fname]

class DownloadMetabase(Task):
    URL = 'http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&downfile=metabase.txt.gz'

    def download(self):
        shell('wget -O {output}.gz "{url}"'.format(
            output=self.output().path,
            url=self.URL))

    def run(self):
        self.output().makedirs()
        self.download()
        shell('gunzip {output}.gz'.format(output=self.output().path))

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))

class MetabaseTable(CSV2TempTableTask):

    has_header = False
    delimiter = '\t'

    def coldef(self):
        return [
            ('table_code', 'TEXT',),
            ('dimension', 'TEXT',),
            ('value', 'TEXT',),
        ]

    def requires(self):
        return DownloadMetabase()

    def input_csv(self):
        return DownloadMetabase().output().path

    def after_copy(self):
        session = current_session()
        session.execute('CREATE UNIQUE INDEX ON {table} (table_code, dimension, value)'.format(
            table=self.output().table
        ))


class SourceTags(TagsTask):

    def tags(self):
        return [OBSTag(id='eurostat-source',
                       name='Eurostat',
                       type='source',
                       description='Eurostat data can be found `here <http://ec.europa.eu>`_.')]


class LicenseTags(TagsTask):

    def tags(self):
        return [OBSTag(id='eurostat-license',
                       name='Copyright European Union',
                       type='license',
                       description='Reuse is authorised, provided the source is acknowledged.  Full information `here <https://ec.europa.eu/info/legal-notice_en#copyright-notice>`_')]


CACHE = DICTablesCache()


def simplify_description(description):
    description = description.replace(
     'Employer business demography - ',
     '')
    description = description.replace(
     'Business demography - ',
     '')
    description = description.replace(
     'number of persons employed in the reference period (t) among enterprises newly born in t divided by the number of enterprises newly born in t,',
     '')
    description = description.replace(
     'Industry, construction and services except insurance activities of holding companies,',
     '')
    description = description.replace(
     'Number of births of enterprises in t',
     'Number of enterprise births')

    description = description.replace(
     'number of persons employed in the reference period (t) among enterprises newly born in t divided by the number of enterprises newly born in t,',
     '')
    description = description.replace(
     'number of persons employed in the reference period (t) among enterprises newly born in t-3 having survived to t divided by the number of enterprises in t newly born in t-3 having survived to t,',
     '')
    description = description.replace(
     'number of enterprise births in the reference period (t) divided by the number of enterprises active in t,',
     '')
    description = description.replace('Birth rate: number of enterprise births in the reference period (t) divided by the number of enterprises active in t,','Enterprise birth rate:')

    description = description.replace('Death rate: number of enterprise deaths in the reference period (t) divided by the number of enterprises active in t,',
    'Enterprise death rate:')

    description = description.replace('Number of persons employed in enterprises newly born in t-3 having survived to t, divided by the number of persons employed in the population of active enterprises in t,'
    ,'')

    description = description.replace('Employment share of enterprise births: number of persons employed in the reference period (t) among enterprises newly born in t divided by the number of persons employed in t among the stock of enterprises active in t,'
    ,'Employment share of new enterprises: ')

    description = description.replace('number of persons employed in the reference period (t) among enterprise deaths divided by the number of persons employed in t among the stock of active enterprises in t,'
    ,'')

    description = description.replace('number of employees in the reference period (t) among enterprises newly born in t divided by the number of persons employed in t among enterprises newly born in t,'
    ,'')

    description = description.replace('Number of births of enterprises in t,'
    ,'Enterprise births:')

    description = description.replace('Number of deaths of enterprises in t,'
    ,'Enterprise deaths:')

    description = description.replace('Number of employees in the population of active enterprises in t,'
    ,'Employees of active enterprises:')

    description = description.replace('Number of employees in the population of births in t,'
    ,'Employees of new enterprises:')

    description = description.replace('Number of employees in the population of deaths in t,'
    ,'Employees in the population of enterprise deaths:')

    description = description.replace('Number of persons employed in the population of enterprises newly born in t-3 having survived to t,'
    ,'People employed in surviving three-year old enterprises:')

    description = description.replace('Number of persons employed in the year of birth in the population of enterprises newly born in t-3 having survived to t,'
    ,'People continuously employed in surviving three-year old enterprises:')

    description = description.replace('Population of active enterprises in t,'
    ,'Active enterprises:')

    description = description.replace('number of persons employed in the reference period (t) among enterprises newly born in t-3 having survived to t divided by the number of persons employed in t-3 by the same enterprises, expressed as a percentage growth rate,'
    ,'')

    description = description.replace(
    'Number of enterprises newly born in t-3 having survived to t',
    'Number of surviving three-year old enterprises'
    )

    description = description.replace(
    'Number of persons employed in the population of active enterprises in t',
    'People employed in active enterprises'
    )

    description = description.replace(
    'Number of persons employed in the population of births in t',
    'People employed in new enterprises'
    )

    description = description.replace(
    'Number of persons employed in the population of deaths in t',
    'People employed in the population of enterprise deaths'
    )

    description = description.replace(
    'Proportion of enterprise births in the reference period (t) by size class,',
    'Proportion of enterprise births by size class:'
    )

    description = description.replace(
    'Hotels; holiday and other short-stay accommodation; camping grounds, recreational vehicle parks and trailer parks',
    'Hotels, holiday, campgrounds, and other short-stay accomodations'
    )
    description = description.replace(
    'Arts, entertainment and recreation; other service activities; activities of household and extra-territorial organizations and bodies',
    'Arts, entertainment, recreation, and other service activities')

    description = description.replace(
    'Proportion of enterprise deaths in the reference period (t) by size class,',
    'Proportion of enterprise deaths by size class:'
    )

    description = description.replace(
    'number of persons employed in the reference period (t) among enterprise deaths  in t divided by the number of enterprise deaths in t',
    ''
    )

    description = description.replace(
     'Industry, construction and services except insurance activities of holding companies,',
     '')
    description = description.replace(
    'Professional, scientific and technical activities; administrative and support service activities'
    ,'Professional, scientific, technical, administrative and support service activities')

    description = description.replace(
    'Financial and insurance activities; real estate activities except activities of holding companies',
    'Financial, insurance, and real estate activities'
    )

    description = re.sub(r' zero$', ' zero employees', description)
    return description

class FlexEurostatColumns(ColumnsTask):

    subsection = Parameter() # Ex. 'age_gender'
    units = Parameter() # Ex. 'people'
    table_name = Parameter()  # Ex. "DEMO_R_PJANAGGR3"

    # From tablename, determine basis of name for columns from table_dic.dic
    # Then, look at metabase.txt to find relevant dimensions (exclude "geo" and "time", utilize "unit")
    # Finally, look up definitions for dimensions from their .dic files, and use that to complete the metadata definition

    def requires(self):
        return {
            'units': UnitTags(),
            'subsection': SubsectionTags(),
            'section': SectionTags(),
            'source': SourceTags(),
            'license': LicenseTags(),
            'metabase': MetabaseTable(),
        }

    def version(self):
        return 16

    def columns(self):
        columns = OrderedDict()

        input_ = self.input()

        subsectiontags = input_['subsection']
        unittags = input_['units']
        eu = input_['section']['eu']
        license = input_['license']['eurostat-license']
        source = input_['source']['eurostat-source']

        cache = CACHE

        session = current_session()
        resp = session.execute('''
            SELECT ARRAY_AGG(DISTINCT dimension) FROM {table}
            WHERE dimension NOT IN ('geo', 'time') AND table_code = '{table_code}';
        '''.format(table=input_['metabase'].table, table_code=self.table_name.lower()))
        dimensions = resp.fetchone()[0]

        resp = session.execute('''
            WITH dimensions AS (SELECT value, dimension
            FROM {table}
            WHERE table_code = '{table_code}'
              AND dimension NOT IN ('time', 'geo'))
            SELECT ARRAY_AGG(JSON_BUILD_OBJECT({select}))
            FROM {from_}
            WHERE {where}
        '''.format(
            table=input_['metabase'].table,
            table_code=self.table_name.lower(),
            select=', '.join(["'{}', {}.value".format(dim, dim) for dim in dimensions]),
            from_=', '.join(['dimensions {}'.format(dim) for dim in dimensions]),
            where=' AND '.join(["{}.dimension = '{}'".format(dim, dim) for dim in dimensions])
        ))
        cross_prod = resp.fetchone()[0]

        tables = cache.get('table_dic.dic')

        table_desc = tables[self.table_name]
        variable_name = table_desc.split('by')[0].strip()
        # for possible_tablenames, table_description in cache.get('table_dic.dic')
        #     if self.table_name.lower() == possible_tablenames.lower():
                # table_desc = table_description
                # variable_name = table_description.split('by')[0].strip()
                # break

        for i in cross_prod:
            dimdefs = []
            if len(cross_prod) > 1: # Multiple variables
                var_code = underscore_slugify(self.table_name+"_".join(i.values()))
                if len(i) == 1: # Only one dimension, usually "unit"
                    for unit_dic, unit_value in i.iteritems():
                        units = cache.get('dic_lists/{dimension}.dic'.format(dimension=unit_dic))
                        dimdefs.append(units[unit_value])
                    description = "{} ".format(variable_name) + "- " + ", ".join([str(x) for x in dimdefs])
                else: # multiple dimensions, ignore "unit" when building name
                    for dimname, dimvalue in i.iteritems():
                        if dimname != 'unit':
                            dim_dic = cache.get('dic_lists/{dimension}.dic'.format(dimension=dimname))
                            dimdefs.append(dim_dic[dimvalue])
                        description = "{} ".format(variable_name) + "- " + ", ".join([str(x) for x in dimdefs])
            else: # Only one variable
                var_code = underscore_slugify(self.table_name)
                for unit_dic, unit_value in i.iteritems():
                    units = cache.get('dic_lists/{dimension}.dic'.format(dimension=unit_dic))
                    dimdefs.append(units[unit_value])
                description = "{} ".format(variable_name) + "- " + ", ".join([str(x) for x in dimdefs])

            try:
                units = cache.get('dic_lists/unit.dic')
                unitdef = units[i['unit']]
                if "percentage" in unitdef.lower() or "per" in unitdef.lower() or "rate" in unitdef.lower():
                    final_unit_tag = "ratio"
                    aggregate = None
                elif 'nama_aux_cra' in var_code:
                    aggregate = None
                else:
                    final_unit_tag = self.units
                    aggregate = 'sum'
            except:
                final_unit_tag = self.units
                aggregate = 'sum'
            tags = [eu, subsectiontags[self.subsection], unittags[final_unit_tag]]

            if ('ths' in var_code or 'th_t' in var_code) and '(thousand persons)' not in description:
                description = description + ' (thousands)'

            columns[var_code] = OBSColumn(
                id=var_code,
                name=simplify_description(description),
                type='Numeric',
                description=description,
                weight=1,
                aggregate=aggregate, #???
                targets={}, #???
                tags=tags,
                extra=i,
                )
            columns[var_code + '_flag'] = OBSColumn(
                id=var_code + '_flag',
                name='',
                type='Text',
                weight=0,
                aggregate=None, #???
                targets={}, #???
                tags={},
                extra=i,
            )

        for colname, col in columns.iteritems():
            col.tags.append(source)
            col.tags.append(license)


        targets_dict = {}
        for colname, col in columns.iteritems():
            if 'flag' not in col.id:
                for i,v in col.extra.iteritems():
                    if v == 'TOTAL' or v == 'T':
                        temp = dict((key,value) for key, value in col.extra.iteritems() if key != i)
                        targets_dict[tuple(temp.items())] = colname
        for colname, col in columns.iteritems():
            denoms = {}
            for nontotals,code in targets_dict.iteritems():
                if all(item in col.extra.items() for item in nontotals) and code != colname:
                    denoms[columns.get(code)] = 'denominator'
            col.targets = denoms

        nonsum = ['proportion','average','percentage','rate',r'%','share']
        for colname, col in columns.iteritems():
            if 'flag' not in col.id:
                if any(word in col.name.lower() for word in nonsum):
                    col.aggregate=None
        return columns


class TableEU(TableTask):

    table_name = Parameter()
    subsection = Parameter()
    nuts_level = IntParameter()
    unit = Parameter()
    year = Parameter()

    def version(self):
        return 8

    def timespan(self):
        return str(self.year).replace('_',' - ')

    def requires(self):
        requirements = {
            'data': EUTempTable(table_name=self.table_name),
            'csv': ProcessCSV(table_code=self.table_name),
            'meta': FlexEurostatColumns(table_name=self.table_name,
                                        subsection=self.subsection,
                                        units=self.unit),
            'geometa': NUTSColumns(level=self.nuts_level),
        }
        return requirements

    def columns(self):
        input_ = self.input()
        cols = OrderedDict()
        cols['nuts{}_id'.format(self.nuts_level)] = input_['geometa']['nuts{}_id'.format(self.nuts_level)]
        cols.update(input_['meta'])
        return cols

    def populate(self):
        input_ = self.input()
        path_to_csv = input_['csv'].path
        with open(path_to_csv) as csvfile:
            header = csvfile.next()
            header = re.split(',',header)
        unit = None
        for i,val in enumerate(header):
            header[i] = val.strip()
            if "geo" in val:
                geo = val
            if r"unit" in val:
                unit = val
        # print header
        session = current_session()
        session.execute('ALTER TABLE {output} ADD PRIMARY KEY (nuts{level}_id)'.format(
            output=self.output().table,
            level=self.nuts_level))
        session.flush()
        column_targets = self._columns
        for colname, coltarget in column_targets.items():
            # print colname
            if colname != 'nuts{}_id'.format(self.nuts_level) and not colname.endswith('_flag'):
                col = coltarget.get(session)
                extra = col.extra
                multiple = ''
                # if 'unit' in extra.keys():
                #     multiplier = extra['unit']
                #     if "THS" in multiplier or "1000" in multiplier or multiplier == 'KTOE':
                #         multiple = '1000*'
                #     if "MIO" in multiplier:
                #         multiple = '1000000*'
                #     else:
                #         multiple = ''
                # else:
                #     multiple = ''
                keys = extra.keys()
                vals = [extra[k_] for k_ in keys]
                # metabase unit does not correspond to headers due to lack of
                # \time"
                if unit:
                    if 'unit' in keys:
                        keys[keys.index('unit')] = unit
                stmt = '''
                    INSERT INTO {output} (nuts{level}_id, {colname}, {colname}_flag)
                    SELECT "{geo}",
                      {multiply}NullIf(SPLIT_PART("{year}", ' ', 1), ':')::Numeric,
                      NullIf(SPLIT_PART("{year}", ' ', 2), '')::Text
                    FROM {input}
                    WHERE ("{input_dims}") = ('{output_dims}')
                    ON CONFLICT (nuts{level}_id)
                       DO UPDATE SET {colname} = EXCLUDED.{colname}'''.format(
                           geo=geo,
                           level=self.nuts_level,
                           colname=colname,
                           multiply=multiple,
                           year=self.year,
                           input_dims='", "'.join(keys),
                           output_dims="', '".join(vals),
                           output=self.output().table,
                           input=input_['data'].table
                       )
                LOGGER.info(stmt)
                # import pdb
                # pdb.set_trace()
                session.execute(stmt)


class AllEUTableYears(Task):

    table_name = Parameter()
    subsection = Parameter()
    nuts_level = IntParameter()
    unit = Parameter()

    def requires(self):
        return ProcessCSV(table_code=self.table_name)

    def run(self):
        csv_path = self.input().path
        with open(csv_path, 'r') as csvfile:
            headers = csvfile.next().split(',')

        years = [h.strip() for h in headers if h.strip()[-4:].isdigit()]
        for year in years:
            yield TableEU(table_name=self.table_name,
                          subsection=self.subsection,
                          nuts_level=self.nuts_level,
                          unit=self.unit,
                          year=year
            )
        self._complete = True

    def complete(self):
        return getattr(self, '_complete', False)

class EUColumns(WrapperTask):
    def requires(self):
        with open(os.path.join(os.path.dirname(__file__), 'wrappertables.csv')) as wrappertables:
            reader = csv.reader(wrappertables)
            for subsection, table_code, nuts, units in reader:
                yield FlexEurostatColumns(subsection=subsection,
                                          table_name=table_code,
                                          units=units)

class EURegionalTables(WrapperTask):

    def requires(self):
        with open(os.path.join(os.path.dirname(__file__), 'wrappertables.csv')) as wrappertables:
            reader = csv.reader(wrappertables)
            for subsection, table_code, nuts, units in reader:
                nuts = int(nuts)
                yield AllEUTableYears(table_name=table_code,
                                      subsection=subsection,
                                      nuts_level=nuts,
                                      unit=units)

# class EUMetaWrapper(MetaWrapper):
#
#     table_name = Parameter()
#     subsection = Parameter()
#     nuts_level = Parameter()
#     unit = Parameter()
#     year = Parameter()
#
#     params = {
#         'table_name': ,
#         'subsection': ,
#         'nuts_level': ,
#         'unit': ,
#         'year': ,
#     }
#
#     def tables(self):
#         yield TableEU(table_name=self.table_name,
#                       subsection=self.subsection,
#                       nuts_level=self.nuts_level,
#                       unit=self.unit,
#                       year=self.year)
