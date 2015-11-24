#!/usr/bin/env python

'''
Bigmetadata tasks

tasks to download and create metadata
'''

import requests
import datetime
import json
import csv
import os
from luigi import Task, Parameter, LocalTarget, DateParameter, run


class DownloadTask(Task):
    url = Parameter()
    name = Parameter()
    date_checked = DateParameter(default=datetime.date.today())

    def run(self):
        out = self.output().open('w')
        out.write(requests.get(self.url).content)
        out.close()

    def output(self):
        return LocalTarget('tmp/{}.csv'.format(self.name))


class ProcessACS(Task):
    year = Parameter()
    sample = Parameter()

    @property
    def name(self):
        return 'acs{year}_{sample}'.format(year=self.year, sample=self.sample)

    def requires(self):
        return DownloadTask(name=self.name,
            url='http://www2.census.gov/{name}/summaryfile/Sequence_Number_and_Table_Number_Lookup.txt'.format(name=self.name))

    def run(self):

        try:
            os.makedirs('data/acs')
        except OSError:
            pass

        for i, line in enumerate(csv.reader(self.input().open())):

            # Skip header
            if i == 0:
                continue

            _, table_id, seqnum, line_no, start_pos, _, _, table_title, \
                    subject_area = [_.strip() for _ in line]

            # Handle metadata rows
            if not line_no or line_no == '.':
                parent = None
                parent_table_title = None
                if start_pos and start_pos != '.':
                    table_description = table_title
                else:
                    universe = table_title
                continue

            try:
                column_id = '{}{:03d}'.format(table_id, int(line_no)).lower()
            except ValueError:
                column_id = '{}{:03.1f}'.format(table_id, float(line_no)).lower()
            source_table = '{}.seq{:04d}'.format(self.name, int(seqnum))

            json_file_path = 'data/acs/{}.json'.format(column_id)

            try:
                with open(json_file_path, 'r') as fhandle:
                    obj = json.load(fhandle)
            except IOError:
                obj = {}

            if 'tables' not in obj:
                obj['tables'] = {}

            column_name = table_title
            if parent:
                obj['relationships'] = [{
                    "type": "parent",
                    "link": parent
                }]
                if parent and not parent.lower().startswith('total'):
                    column_name = '{} {}'.format(parent_table_title, table_title)

            obj['tables'][source_table] = {
                "column": column_id,
                "description": '{table} {column} in {universe}'.format(
                    table=table_description,
                    column=column_name,
                    universe=universe)
            }

            with open(json_file_path, 'w') as fhandle:
                json.dump(obj, fhandle, indent=4)

            # set parent
            if table_title.endswith(':'):
                parent = column_id
                parent_table_title = table_title


class AllACS(Task):

    date_checked = DateParameter(default=datetime.date.today())
    def requires(self):
        for year in xrange(2010, 2014):
            for sample in ('1yr', '3yr', '5yr'):
                #return ProcessACS(year=year, sample=sample)
                yield ProcessACS(year=year, sample=sample)


if __name__ == '__main__':
    run()
