from __future__ import print_function

import os

from luigi import Task, Parameter, WrapperTask, LocalTarget
from tasks.util import shell
import csv




class CreateColumns(Task):
    tablename = Parameter(default='Pessoa13')

    # def requires(self):
    #     return LocalTarget(os.path.join('tmp', classpath(self), self.task_id))

    def run(self):
        infiles = shell('ls tasks/br/meta/{tablename}.csv'.format(
            tablename=self.tablename
        ))
        # print infiles
        in_csv_files = infiles.strip().split('\n')
        os.makedirs(self.output().path)
        MetaParser().csv_to_cols(in_csv_files, self.output().path, self.tablename)

    def output(self):
        return LocalTarget(os.path.join('tasks', 'br', 'cols'))


# t001c001_t = OBSColumn(
#     id='t001c001_t',
#     name='Total population (total)',
#     type='Numeric',
#     weight=3,
#     aggregate='sum',
#     tags=[ca, unit_people, subsections['age_gender']],
#     targets={},)

class MetaParser(object):

    def csv_to_cols(self, csv_paths, output_dir, tablename):
        self._file_handlers = {}
        self._output_dir = output_dir

        try:
            if isinstance(csv_paths, (str, unicode,)):
                csv_paths = [csv_paths]

            for csv_path in csv_paths:
                _ids = []
                self._header = None
                with file(csv_path, 'rb') as csvfile:

                    filename = '{tablename}.py'.format(tablename=tablename)

                    # Need to handle csv files where header row isn't row #1
                    # header_row_index, self._header = self.get_header_row(csvfile)
                    # if header_row_index == -1:
                    #     raise ValueError('Invalid file')

                    reader = csv.reader(csvfile, delimiter=',', quotechar='"')

                    print('file: {}'.format(csv_path))

                    # uid = None
                    record = []

                    str_header = '''
from tasks.meta import OBSColumn, DENOMINATOR, UNIVERSE
from tasks.util import ColumnsTask
from tasks.tags import SectionTags, SubsectionTags, UnitTags

from collections import OrderedDict

class {tablename}Columns(ColumnsTask):
                    '''.format(tablename=tablename)

                    str_header += '''
    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'units': UnitTags(),
        }

    def version(self):
        return 3

    def columns(self):
'''
                    self._write_to_file(filename, str_header)

                    for row in reader:
                        if not row[0].startswith('V'):
                            continue

                        # save a reference to append to the end of the file
                        _ids.append(row[0])

                        col_id = row[0]
                        col_name = row[1]
                        col_unit = row[3]
                        col_target = row[4]
                        col_subsection = row[5]
                        country_iso2 = 'br'

                        str_cols = '''
        {key} = OBSColumn(
            id=\'{id}\',
            name=\'{name}\',
            type=\'Numeric\',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['{country_iso2}'], self.input()['units']['{unit}'], self.input()['subsections']['{subsection}']],
                            '''.format(
                                    key=col_id,
                                    id=col_id,
                                    name=col_name,
                                    country_iso2=country_iso2,
                                    unit=col_unit,
                                    subsection=col_subsection,
                                )
                        if col_target is None or len(col_target) == 0:
                            str_cols += '''targets={},'''
                        else:
                            str_cols += '''targets={{ {target}: DENOMINATOR }},'''.format(target=col_target)
                        str_cols += ')'

                        self._write_to_file(filename, str_cols)


                    # write out the footer
                    str_footer = '''        return OrderedDict([
'''
                    # self._write_to_file(filename, 'return OrderedDict([')
                    for _id in _ids:
                        str_footer += '''           (\'{}\', {}),\n'''.format(_id, _id)
                        # self._write_to_file(filename, str_footer)
                    str_footer += '])'
                    self._write_to_file(filename, str_footer)


        finally:
            for file_handler in self._file_handlers.values():
                file_handler.close()

            self._output_dir = None
            self._file_handlers = None


    def _write_to_file(self, filename, string):
        if filename not in self._file_handlers:
            file_path = os.path.join(self._output_dir, filename)
            file_handle = file(file_path, 'w')
            self._file_handlers[filename] = file_handle

        print(string, file=self._file_handlers[filename])
