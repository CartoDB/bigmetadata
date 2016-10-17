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
#
# dl_code_list = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&downfile=dic%2Fen%2F{code}.dic".format(code=code)
# flag_explanation = "http://ec.europa.eu/eurostat/data/database/information"
# database = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?dir=data&sort=1&sort=2&start={}".format(first_letter)
# dl_data = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&downfile=data%2F{}.tsv.gz".format(table_code)
# dl_data = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=data%2Fdemo_r_pjangrp3.tsv.gz

class DownloadEurostat(Task):

    table_code = "demo_r_pjangrp3"
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
        session = current_session()
        resp = session.execute('''
            SELECT DISTINCT {dimnames}, UNIT FROM {input}
        '''.format(
            input=self.input()['data'].table,
            dimnames=', '.join(self.dimnames)
        ))
        for row in resp:
            dimvalues = row[0:-1]
            unit = row[-1]

            input_ = self.input()

            subsectiontags = input_['subsection']
            unittags = input_['units']
            eu = input_['section']['eu']

            tags = [eu] # ?? figure out appropriate tags here from our unit info
            aggregate = 'sum' # ?? does not work for Fertility Rate, Business Demographics, Transport...
            if unit == 'NR': # ?? does not work for Euros, Fertility Rate, Business Demographics, Transport...
                tags.append(unittags['people'])
            tags.append(subsectiontags[self.subsection])

            columns['_'.join(dimvalues)] = generate_eurostat_age_gender_column('DEMO_R_PJANGRP3',
                tags, aggregate,
                OrderedDict(zip(self.dimnames, dimvalues)))
        return columns
