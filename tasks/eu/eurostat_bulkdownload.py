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

    delimiter = Parameter(default='\t', significant=False)
    table_name = Parameter() # Ex. "DEMO_R_PJANAGGR3"

    def version(self):
        return 3

    def requires(self):
        return DownloadEurostat(table_code=self.table_name)

    def input_csv(self):
        shell("cat {path} | tr ',' '\t' > {path}.csv".format(
            path=self.input().path))
        return self.input().path + '.csv'


class EUFormatTable(TempTableTask):

    def requires(self):
        return EUTempTable()


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


class FlexEurostatColumns(ColumnsTask):

    subsection = Parameter()
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
        }

    def version(self):
        return 5

    def columns(self):
        columns = OrderedDict()

        input_ = self.input()

        subsectiontags = input_['subsection']
        unittags = input_['units'] #???
        eu = input_['section']['eu']
        license = input_['license']['eurostat-license']
        source = input_['source']['eurostat-source']

        tags = [eu, subsectiontags[self.subsection]]

        current_code = None
        codes = {}
        current_list = []
        with open(os.path.join(os.path.dirname(__file__), 'metabase.txt')) as metabase:
            reader = csv.reader(metabase, delimiter='\t')
            for possible_tablenames, code, code_value in reader:
                if self.table_name.lower() == possible_tablenames.lower():
                    if code != "geo" and code != "unit" and code != "time":
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

        if cross_prod:
            for i in cross_prod:
                var_code = underscore_slugify(variable_name+"_".join(i.values()))
                dimdefs = []
                for dimname, value in i.iteritems():
                    with open(os.path.join(os.path.dirname(__file__),
                                           'dic_lists', '{dimension}.dic'.format(
                                               dimension=dimname))) as dimfile:
                        reader = csv.reader(dimfile, delimiter='\t')
                        for possible_dimvalue, dimdef in reader:
                            if value == possible_dimvalue:
                                dimdefs.append(dimdef)
                description = "{} ".format(variable_name)+", ".join([str(x) for x in dimdefs])
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
                    name=description,
                    type='Text',
                    description=table_desc,
                    weight=1,
                    aggregate=None, #???
                    targets={}, #???
                    tags=tags,
                    extra=i
                )
        else:
            var_code = underscore_slugify(variable_name)
            description = variable_name
            columns[var_code] = OBSColumn(
                id=var_code,
                name=variable_name,
                type='Numeric',
                description=table_desc,
                weight=1,
                aggregate=None, #???
                targets={}, #???
                tags=tags,
                extra=None
            )
            columns[var_code + '_flag'] = OBSColumn(
                id=var_code + '_flag',
                name=variable_name,
                type='Text',
                description=table_desc,
                weight=1,
                aggregate=None, #???
                targets={}, #???
                tags=tags,
                extra=None
            )

        for colname, col in columns.iteritems():
            col.tags.append(source)
            col.tags.append(license)

        return columns


class TableEU(TableTask):

    table_name = Parameter()
    subsection = Parameter()
    nuts_level = IntParameter()

    def version(self):
        return 3

    def timespan(self):
        return '2015'

    def requires(self):
        requirements = {
            'data': EUTempTable(table_name=self.table_name),
            'meta': FlexEurostatColumns(table_name=self.table_name,
                                        subsection=self.subsection),
            'geometa': NUTSColumns(level=self.nuts_level),
        }
        return requirements

    def columns(self):
        cols = OrderedDict()
        cols['nuts3_id'] = self.input()['geometa']['nuts3_id']
        cols.update(self.input()['meta'])
        return cols

    def populate(self):
        session = current_session()
        session.execute('ALTER TABLE {output} ADD PRIMARY KEY (nuts3_id)'.format(
            output=self.output().table))
        session.flush()
        column_targets = self.columns()
        for colname, coltarget in column_targets.iteritems():
            if colname != 'nuts3_id' and not colname.endswith('_flag'):
                col = coltarget.get(session)
                extra = col.extra
                keys = extra.keys()
                vals = [extra[k_] for k_ in keys]
                session.execute('''
                    INSERT INTO {output} (nuts3_id, {colname}, {colname}_flag)
                    SELECT "geo\\time",
                      NullIf(SPLIT_PART("{year} ", ' ', 1), ':')::Numeric,
                      NullIf(SPLIT_PART("{year} ", ' ', 2), '')::Text
                    FROM {input}
                    WHERE ({input_dims}) = ('{output_dims}')
                    ON CONFLICT (nuts3_id)
                       DO UPDATE SET {colname} = EXCLUDED.{colname}'''.format(
                           colname=colname,
                           year=self.timespan(),
                           input_dims=', '.join(keys),
                           output_dims="', '".join(vals),
                           output=self.output().table,
                           input=self.input()['data'].table
                       ))
            print colname

class EURegionalTables(WrapperTask):
    def requires(self):
        with open(os.path.join(os.path.dirname(__file__), 'wrappertables.csv')) as wrappertables:
            reader = csv.reader(wrappertables)
            for subsection, table_code, nuts in reader:
                nuts = int(nuts)
                if nuts == 3: # Remove this line when NUTS2 and NUTS1 are available
                    yield TableEU(table_name=table_code, subsection=subsection, nuts_level=nuts)
