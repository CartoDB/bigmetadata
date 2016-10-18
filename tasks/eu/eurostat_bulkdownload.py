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
    URL = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&downfile=data%2F{}.tsv.gz".format(table_code)

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


class EUTempTable(CSV2TempTableTask):
    delimiter = Parameter(default='\t', significant=False)
    def version(self):
        return 2

    def requires(self):
        return DownloadEurostat()

    def input_csv(self):
        shell("cat {} | tr ',' '\t' > {}.csv".format(self.input().path,self.input().path))
        return self.input().path + '.csv'


class EUFormatTable(TempTableTask):

    def requires(self):
        return EUTempTable()

    def run(self):
        pass


class FlexEurostatColumns(ColumnsTask):

    subsection = Parameter()
    tablename = Parameter()  # Ex. "DEMO_R_PJANAGGR3"

    # From tablename, determine basis of name for columns from table_dic.dic
    # Then, look at metabase.txt to find relevant dimensions (exclude "geo" and "time", utilize "unit")
    # Finally, look up definitions for dimensions from their .dic files, and use that to complete the metadata definition

    def requires(self):
        return {
            'units': UnitTags(),
            'subsection': SubsectionTags(),
            'section': SectionTags()
        }

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
                if self.tablename.lower() == possible_tablenames.lower():
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
                if self.tablename.lower() == possible_tablenames.lower():
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
                    type='Numeric',
                    description=table_desc,
                    weight=1,
                    aggregate=None, #???
                    targets={}, #???
                    tags=tags,
                )
        else:
            var_code = underscore_slugify(variable_name)
            description = variable_name
            columns[var_code] = OBSColumn(
                id=var_code,
                name=variable_name,
                type='Numeric',
                description= table_desc,
                weight=1,
                aggregate=None, #???
                targets={}, #???
                tags=tags,
            )

        return columns

class FranceCensus(TableTask):

    table_name = Parameter()
    subsection = Parameter()

    def version(self):
        return 8

    def timespan(self):
        return '2012'

    def requires(self):
        requirements = {
            'data': RawFRData(table_name=self.table_name),
=            'meta': FlexEurostatColumns(table_name=self.table_theme, subsection = self.subsection),
            'geometa': OutputAreaColumns(),
        }
        return requirements

    def columns(self):
        cols = OrderedDict()
        cols['IRIS'] = self.input()['geometa']['dcomiris']
        cols.update(self.input()['meta'])
        return cols

    def populate(self):
        session = current_session()

        column_targets = self.columns()
        colnames = ', '.join(column_targets.keys())
        colnames_typed = ','.join(['{}::{}'.format(colname, ct.get(session).type)
                              for colname, ct in column_targets.iteritems()])
        session.execute('INSERT INTO {output} ({ids}) '
                        'SELECT {ids_typed} '
                        'FROM {input} '.format(
                            ids=colnames,
                            ids_typed=colnames_typed,
                            output=self.output().table,
                            input=self.input()['iris_data'].table
                        ))
        session.execute('INSERT INTO {output} ({ids}) '
                        'SELECT {ids_typed} '
                        'FROM {input} '.format(
                            ids=colnames,
                            ids_typed=colnames_typed,
                            output=self.output().table,
                            input=self.input()['overseas_data'].table
                        ))
