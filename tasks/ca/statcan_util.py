from __future__ import print_function

import csv
import itertools
import os
from collections import OrderedDict

from util import underscore_slugify


class StatCanParser(object):
    GEO_COLUMN = 'Geo_Code'
    PARSE_COLUMN = 'Topic'
    TRANSFORM_COLUMN = 'Characteristic'

    def _transpose_row(self, row):
        char_val = row[self.TRANSFORM_COLUMN]
        level = (len(char_val) - len(char_val.lstrip())) / 2
        char_val = underscore_slugify(char_val.strip())
        return (
            ('{}_level'.format(char_val),  level),
            ('{}_note'.format(char_val),  row['Note']),
            ('{}_total'.format(char_val),  row['Total']),
            ('{}_male'.format(char_val),  row['Male']),
            ('{}_female'.format(char_val),  row['Female']),
        )

    def _group_record(self, record):
        common_cols = tuple([(h, record[0][h]) for h in self._common_col_names])
        transposed_cols = tuple(itertools.chain.from_iterable(map(self._transpose_row, record)))
        return OrderedDict(common_cols + transposed_cols)

    def _write_record_to_csv(self, parse_col, record):
        if len(record) == 0:
            return

        formatted_record = self._group_record(record)
        if parse_col not in self._file_handlers:
            file_path = os.path.join(self._output_dir,
                                     '{}.csv'.format(underscore_slugify(parse_col)))
            file_handle = file(file_path, 'w')
            self._file_handlers[parse_col] = file(file_path, 'w')
            print(','.join(formatted_record.keys()), file=self._file_handlers[parse_col])

        print(','.join([str(v) for v in formatted_record.values()]),
              file=self._file_handlers[parse_col])

    def get_header(self, csv_path):
        with file(csv_path, 'rb') as csvfile:
            reader = csv.reader(csvfile)
            return reader.next()

    def parse_csv_to_files(self, csv_paths, output_dir):
        self._file_handlers = {}
        self._output_dir = output_dir
        self._header = None
        self._common_col_names = None

        try:
            if isinstance(csv_paths, (str, unicode,)):
                csv_paths = [csv_paths]

            for csv_path in csv_paths:
                with file(csv_path, 'rb') as csvfile:
                    reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
                    self._header = self.get_header(csv_path)
                    end_common_cols = self._header.index(self.TRANSFORM_COLUMN) + 1
                    self._common_col_names = self._header[0:end_common_cols]
                    uid = None
                    parse_col_val = None
                    record = []
                    for row in reader:
                        if uid != row[self.GEO_COLUMN] or parse_col_val != row[self.PARSE_COLUMN]:
                            self._write_record_to_csv(parse_col_val, record)
                            record = []
                            uid = row[self.GEO_COLUMN]
                            parse_col_val = row[self.PARSE_COLUMN]

                        record.append(row)
                    else:
                        self._write_record_to_csv(parse_col_val, record)

                print(csv_path)

        finally:
            for file_handler in self._file_handlers.values():
                file_handler.close()

            self._output_dir = None
            self._header = None
            self._file_handlers = None
            self._common_col_names = None
