from tasks.eu.geo import NUTSColumns
from tasks.meta import (OBSTable, OBSColumn, OBSTag, current_session,
                        DENOMINATOR, GEOM_REF, UNIVERSE)
from tasks.tags import SectionTags, SubsectionTags, UnitTags
from tasks.util import (Shp2TempTableTask, TempTableTask, TableTask, TagsTask, ColumnsTask,
                        DownloadUnzipTask, CSV2TempTableTask,
                        underscore_slugify, shell, classpath)

from luigi import IntParameter, Parameter, WrapperTask, Task, LocalTarget, ListParameter
from collections import OrderedDict

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


class EUTempTable(CSV2TempTableTask):

    delimiter = Parameter(default=',', significant=False)
    table_name = Parameter() # Ex. "DEMO_R_PJANAGGR3"

    def version(self):
        return 3

    def requires(self):
        return DownloadEurostat(table_code=self.table_name)

    def coldef(self):
        coldefs = super(CSV2TempTableTask, self).coldef()
        for i, cd in enumerate(coldefs):
            coldefs[i][0] = cd[0].strip()

    def input_csv(self):
        shell("cat {path} | tr '\' ',' | tr '\t' ',' > {path}.csv".format(
            path=self.input().path))
        return self.input().path + '.csv'


class EUFormatTable(TempTableTask):

    def requires(self):
        return EUTempTable()


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
            'section': SectionTags()
        }

    def version(self):
        return 4

    def columns(self):
        columns = OrderedDict()

        input_ = self.input()

        subsectiontags = input_['subsection']
        unittags = input_['units']
        eu = input_['section']['eu']

        current_code = None
        codes = {}
        current_list = []
        with open(os.path.join(os.path.dirname(__file__), 'metabase.txt')) as metabase:
            reader = csv.reader(metabase, delimiter='\t')
            for possible_tablenames, code, code_value in reader:
                if self.table_name.lower() == possible_tablenames.lower():
                    if code != "geo" and code != "time":
                        if current_code == code:
                            current_list.append(code_value)
                        elif current_code:
                            codes[current_code] = current_list
                            current_list = []
                            current_list.append(code_value)
                        else:
                            current_list.append(code_value)
                        current_code = code
                        codes[current_code] = current_list

        product = [x for x in apply(itertools.product, codes.values())]
        cross_prod = [dict(zip(codes.keys(), p)) for p in product]

        with open(os.path.join(os.path.dirname(__file__), 'table_dic.dic')) as tabledicfile:
            tablereader = csv.reader(tabledicfile, delimiter='\t')
            for possible_tablenames, table_description in tablereader:
                if self.table_name.lower() == possible_tablenames.lower():
                    table_desc = table_description
                    variable_name = table_description.split('by')[0].strip()
                    break
        for i in cross_prod:
            if len(cross_prod) > 1: # Multiple variables
                var_code = underscore_slugify(self.table_name+"_".join(i.values()))
                if len(i) == 1: # Only units are unique
                    dimdefs = []
                    for unit_dic, unit_value in i.iteritems():
                        with open(os.path.join(os.path.dirname(__file__),'dic_lists/{dimension}.dic'.format(dimension=unit_dic))) as dimfile:
                            reader = csv.reader(dimfile, delimiter='\t')
                            for possible_dimvalue, dimdef in reader:
                                if unit_value == possible_dimvalue:
                                    dimdefs.append(dimdef)
                    description = "{} ".format(variable_name) + "- " + ", ".join([str(x) for x in dimdefs])
                else:
                    dimdefs = []
                    for dimname, dimvalue in i.iteritems():
                        with open(os.path.join(os.path.dirname(__file__),'dic_lists/{dimension}.dic'.format(dimension=dimname))) as dimfile:
                            reader = csv.reader(dimfile, delimiter='\t')
                            for possible_dimvalue, dimdef in reader:
                                if dimvalue == possible_dimvalue:
                                    if dimname != "unit": # Only take non-unit definitions for name. This may be problematic
                                        dimdefs.append(dimdef)
                    description = "{} ".format(variable_name) + "- " + ", ".join([str(x) for x in dimdefs])
            else: # Only one variable
                var_code = underscore_slugify(self.table_name)
                description = variable_name
            with open(os.path.join(os.path.dirname(__file__),'dic_lists/unit.dic')) as unitfile:
                reader = csv.reader(unitfile, delimiter='\t')
                for possible_unit, unitdef in reader:
                    if i['unit'] == possible_unit:
                        if "percentage" in unitdef:
                            final_unit_tag = "ratio"
                        else:
                            final_unit_tag = self.units
            tags = [eu, subsectiontags[self.subsection], unittags[final_unit_tag]]
            columns[var_code] = OBSColumn(
                id=var_code,
                name=description,
                type='Numeric',
                description=table_desc,
                weight=1,
                aggregate=None, #???
                targets={}, #???
                tags=tags,
                extra=i
                )
            columns[var_code + '_flag'] = OBSColumn(
                id=var_code + '_flag',
                name=description + ' flag',
                type='Text',
                description=table_desc,
                weight=0,
                aggregate=None, #???
                targets={}, #???
                tags=tags,
                extra=i
            )
        return columns


class TableEU(TableTask):

    table_name = Parameter()
    subsection = Parameter()
    nuts_level = IntParameter()
    unit = Parameter()
    year = IntParameter()

    def version(self):
        return 6

    def timespan(self):
        return str(self.year)

    def requires(self):
        requirements = {
            'data': EUTempTable(table_name=self.table_name),
            'csv': DownloadEurostat(table_code=self.table_name),
            'meta': FlexEurostatColumns(table_name=self.table_name,
                                        subsection=self.subsection,
                                        units=self.unit),
            'geometa': NUTSColumns(level=self.nuts_level),
        }
        return requirements

    def columns(self):
        cols = OrderedDict()
        cols['nuts{}_id'.format(self.nuts_level)] = self.input()['geometa']['nuts{}_id'.format(self.nuts_level)]
        cols.update(self.input()['meta'])
        return cols

    def populate(self):
        path_to_csv = self.input()['csv'].path + '.csv'
        with open(path_to_csv) as csvfile:
            header = csvfile.next()
            header = re.split(',',header)
        for i,val in enumerate(header):
            header[i] = val.strip()
            if "geo" in val:
                geo = val
        # print header
        session = current_session()
        session.execute('ALTER TABLE {output} ADD PRIMARY KEY (nuts{level}_id)'.format(
            output=self.output().table,
            level=self.nuts_level))
        session.flush()
        column_targets = self.columns()
        for colname, coltarget in column_targets.iteritems():
            # print colname
            if colname != 'nuts{}_id'.format(self.nuts_level) and not colname.endswith('_flag'):
                col = coltarget.get(session)
                extra = col.extra
                thousands = extra['unit']
                # print col.extra[1]
                if "THS" in thousands:
                    multiple = '1000*'
                else:
                    multiple = ''
                keys = extra.keys()
                vals = [extra[k_] for k_ in keys]
                session.execute('''
                    INSERT INTO {output} (nuts{level}_id, {colname}, {colname}_flag)
                    SELECT "{geo}",
                      {multiply}NullIf(SPLIT_PART("{year}", ' ', 1), ':')::Numeric,
                      NullIf(SPLIT_PART("{year}", ' ', 2), '')::Text
                    FROM {input}
                    WHERE ({input_dims}) = ('{output_dims}')
                    ON CONFLICT (nuts{level}_id)
                       DO UPDATE SET {colname} = EXCLUDED.{colname}'''.format(
                           geo=geo,
                           level=self.nuts_level,
                           colname=colname,
                           multiply=multiple,
                           year=self.timespan(),
                           input_dims=', '.join(keys),
                           output_dims="', '".join(vals),
                           output=self.output().table,
                           input=self.input()['data'].table
                       ))

class EURegionalTables(WrapperTask):
    def requires(self):
        with open(os.path.join(os.path.dirname(__file__), 'wrappertables.csv')) as wrappertables:
            reader = csv.reader(wrappertables)
            for subsection, table_code, nuts, units in reader:
                nuts = int(nuts)
                for year in range(1990,2016):
                    try:
                        yield TableEU(table_name=table_code, subsection=subsection, nuts_level=nuts, unit=units, year=year)
                    except:
                        pass
