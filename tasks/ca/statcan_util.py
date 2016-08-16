from __future__ import print_function

import csv
import itertools
import os
from collections import OrderedDict

from util import underscore_slugify


class StatCanParser(object):
    # Geography id column
    GEO_COLUMN = 'Geo_Code'

    # Column used to split for defining
    # output file.
    PARSE_COLUMN = 'Topic'

    # TRANSPOSE_COLUMN_PREFIX column value is added as
    # a prefix to each TRANSPOSE_COLUMNS
    # example:
    #   TRANSPOSE_COLUMN_PREFIX value == 'Household total income in 2010'
    #   Output columns:
    #   - household_total_income_in_2010_note
    #   - household_total_income_in_2010_total
    #   - household_total_income_in_2010_male
    #   - household_total_income_in_2010_female
    TRANSPOSE_COLUMN_PREFIX = 'Characteristic'
    TRANSPOSE_COLUMNS = ('Note', 'Total', 'Male', 'Female',)

    # Only use these columns when parsing file
    COLUMNS = (
        GEO_COLUMN,
        PARSE_COLUMN,
        TRANSPOSE_COLUMN_PREFIX,
    ) + TRANSPOSE_COLUMNS

    def _transpose_row(self, row):
        '''
        Only transpose columns that are defined in TRANSPOSE_COLUMNS
        '''
        char_val = row[self.TRANSPOSE_COLUMN_PREFIX]
        # Level tracks how indented the TRANSPOSE_COLUMN_PREFIX
        # 2 spaces == 1 level
        level = (len(char_val) - len(char_val.lstrip())) / 2
        char_val = underscore_slugify(char_val.strip())
        vals = [('{}_level'.format(char_val),  level)]
        for col in self.TRANSPOSE_COLUMNS:
            vals.append((underscore_slugify('{}_{}'.format(char_val, col)), row[col]),)

        return tuple(vals)

    def _group_record(self, record):
        # Columns not transposed
        geo_col = ((self.GEO_COLUMN, record[0][self.GEO_COLUMN],),)
        # For each row in record transpose columns defined
        # in TRANSPOSE_COLUMNS.
        transposed_cols = tuple(itertools.chain.from_iterable(map(self._transpose_row, record)))
        return OrderedDict(geo_col + transposed_cols)

    def _write_record_to_csv(self, parse_col, record):
        if len(record) == 0:
            return

        formatted_record = self._group_record(record)
        if parse_col not in self._file_handlers:
            file_path = os.path.join(self._output_dir,
                                     '{}.csv'.format(underscore_slugify(parse_col)))
            file_handle = file(file_path, 'w')
            self._file_handlers[parse_col] = file(file_path, 'w')
            print('"{}"'.format('","'.join(formatted_record.keys())),
                  file=self._file_handlers[parse_col])

        print(','.join(map(str, formatted_record.values())),
              file=self._file_handlers[parse_col])

    def _map_row(self, row):
        '''
        Convert list row to dict row
        '''
        try:
            row_dict = {}
            for col_name in self.COLUMNS:
                idx = self._header.get(col_name)
                if idx is None:
                    continue
                row_dict[col_name] = row[idx]

            return row_dict
        except IndexError:
            return None

    def get_header_row(self, csvfile):
        '''
        Get row index (0-based) and list of column names.
        '''
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for i, row in enumerate(reader):
            if len([c for c in row if c in self.COLUMNS]) == len(self.COLUMNS):
                return i, {col_name: i for i, col_name in enumerate(row)}

        return -1, None

    def parse_csv_to_files(self, csv_paths, output_dir):
        self._file_handlers = {}
        self._output_dir = output_dir

        try:
            if isinstance(csv_paths, (str, unicode,)):
                csv_paths = [csv_paths]

            for csv_path in csv_paths:
                self._header = None
                with file(csv_path, 'rb') as csvfile:
                    # Need to handle csv files where header row isn't row #1
                    header_row_index, self._header = self.get_header_row(csvfile)
                    if header_row_index == -1:
                        raise ValueError('Invalid file')

                    reader = csv.reader(csvfile, delimiter=',', quotechar='"')

                    uid = None
                    parse_col_val = None
                    record = []
                    for row in reader:
                        row = self._map_row(row)
                        if row is None:
                            continue

                        if uid != row[self.GEO_COLUMN] or parse_col_val != row[self.PARSE_COLUMN]:
                            self._write_record_to_csv(parse_col_val, record)
                            record = []
                            uid = row[self.GEO_COLUMN]
                            parse_col_val = row[self.PARSE_COLUMN]

                        record.append(row)

                    self._write_record_to_csv(parse_col_val, record)

        finally:
            for file_handler in self._file_handlers.values():
                file_handler.close()

            self._output_dir = None
            self._file_handlers = None
