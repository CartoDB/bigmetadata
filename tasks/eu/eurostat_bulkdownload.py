from tasks.util import (Shp2TempTableTask, TempTableTask, TableTask, TagsTask, ColumnsTask,
                        DownloadUnzipTask, CSV2TempTableTask,
                        underscore_slugify, shell, classpath)
from tasks.meta import current_session, DENOMINATOR, GEOM_REF, UNIVERSE
from collections import OrderedDict
from luigi import IntParameter, Parameter, WrapperTask, Task, LocalTarget, ListParameter
import os
from tasks.meta import OBSTable, OBSColumn, OBSTag
from tasks.tags import SectionTags, SubsectionTags, UnitTags
from tasks.fr.communes import OutputCommuneColumns
import csv
import pandas as pd
import itertools
#
# dl_code_list = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&downfile=dic%2Fen%2F{code}.dic".format(code=code)
# flag_explanation = "http://ec.europa.eu/eurostat/data/database/information"
# database = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?dir=data&sort=1&sort=2&start={}".format(first_letter)
# dl_data = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&downfile=data%2F{}.tsv.gz".format(table_code)
# dl_data = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=data%2Fdemo_r_pjangrp3.tsv.gz

class DownloadEurostat(Task):

    table_code =  Parameter()
    URL="http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=data%2F{code}.tsv.gz"

    def download(self):
        URL = self.URL.format(code=self.table_code.lower())
        shell('wget -O {output}.gz "{url}"'.format(
        output=self.output().path,
        url=URL))

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
        shell("cat {} | tr ',' '\t' > {}.csv".format(self.input().path,self.input().path))
        return self.input().path + '.csv'


class EUFormatTable(TempTableTask):

    def requires(self):
        return EUTempTable()


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
            'section': SectionTags()
        }

    def version(self):
        return 3

    def columns(self):
        columns = OrderedDict()

        input_ = self.input()

        subsectiontags = input_['subsection']
        unittags = input_['units'] #???
        eu = input_['section']['eu']

        tags=[eu,subsectiontags[self.subsection]]

        current_code = None
        codes = {}
        current_list = []
        with open(os.path.join(os.path.dirname(__file__),'metabase.txt')) as metabase:
            reader = csv.reader(metabase,delimiter='\t')
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

        with open(os.path.join(os.path.dirname(__file__),'table_dic.dic')) as tabledicfile:
            tablereader = csv.reader(tabledicfile,delimiter='\t')
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
                    with open(os.path.join(os.path.dirname(__file__),'dic_lists/{dimension}.dic'.format(dimension=dimname))) as dimfile:
                        reader = csv.reader(dimfile, delimiter='\t')
                        for possible_dimvalue, dimdef in reader:
                            if value == possible_dimvalue:
                                dimdefs.append(dimdef)
                description = "{} ".format(variable_name)+", ".join([str(x) for x in dimdefs])
                columns[var_code] = OBSColumn(
                    id=var_code,
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
                type='Text',
                description= table_desc,
                weight=1,
                aggregate=None, #???
                targets={}, #???
                tags=tags,
                extra=None
            )

        return columns


class NUTSMeta(ColumnsTask):
    def columns(self):
        columns = OrderedDict()
        columns['geo'] = OBSColumn(
            id='geo',
            type='text',
            weight=0,
        )
        return columns


class TableEU(TableTask):

    table_name = Parameter()
    subsection = Parameter()

    def version(self):
        return

    def timespan(self):
        return '2015'

    def requires(self):
        requirements = {
            'data': EUTempTable(table_name=self.table_name),
            'meta': FlexEurostatColumns(table_name=self.table_name, subsection = self.subsection),
            'geometa': NUTSMeta(), #still needed
        }
        return requirements

    def columns(self):
        cols = OrderedDict()
        cols['geo'] = self.input()['geometa']['geo'] #still needed
        cols.update(self.input()['meta'])
        return cols

    def populate(self):
        session = current_session()
        session.execute('ALTER TABLE {output} ADD PRIMARY KEY (geo)'.format(
            output=self.output().table))
        session.flush()
        column_targets = self.columns()
        for k,v in column_targets.iteritems():
            if k != 'geo':
                col = v.get(session)
                extra = col.extra
                keys = extra.keys()
                vals = [extra[k_] for k_ in keys]
                session.execute('INSERT INTO {output} (geo, {id}) '
                                'SELECT "{geo_id}", Nullif("{year} ", \': \')::Text '
                                'FROM {input} '
                                'WHERE ({input_dims}) = (\'{output_dims}\')'
                                'ON CONFLICT (geo)'
                                '   DO UPDATE SET {id} = EXCLUDED.{id}'.format(
                                    geo_id=r"geo\time",
                                    id=k,
                                    year=self.timespan(),
                                    input_dims = ', '.join(keys),
                                    output_dims = "', '".join(vals),
                                    output=self.output().table,
                                    input=self.input()['data'].table
                                ))
