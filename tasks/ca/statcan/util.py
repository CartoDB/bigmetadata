

import csv
import itertools
import os
import re
from collections import OrderedDict
from operator import itemgetter

from tasks.util import underscore_slugify


class StatCanParser(object):
    # Geography id column
    GEO_COLUMN = 'geo_code'

    # Column used to split for defining
    # output file.
    PARSE_COLUMN = 'topic'

    # TRANSPOSE_COLUMN_PREFIX column value is added as
    # a prefix to each TRANSPOSE_COLUMNS
    # example:
    #   TRANSPOSE_COLUMN_PREFIX value == 'Household total income in 2010'
    #   Output columns:
    #   - household_total_income_in_2010_note
    #   - household_total_income_in_2010_total
    #   - household_total_income_in_2010_male
    #   - household_total_income_in_2010_female
    TRANSPOSE_COLUMN_PREFIX = 'characteristic'

    # !!!be careful when changing the order of these!!!
    TRANSPOSE_COLUMNS = (
        # 'Note',
        'total',
        'male',
        'female',
    )

    # Only use these columns when parsing file
    COLUMNS = (
        GEO_COLUMN,
        PARSE_COLUMN,
        TRANSPOSE_COLUMN_PREFIX,
    ) + TRANSPOSE_COLUMNS

    division_column = None
    division_pattern = None

    def __init__(self, division_splitted):
        if division_splitted is not None:
            self.division_column = division_splitted[0]
            self.division_pattern = division_splitted[1]
            if self.division_column is not None:
                self.COLUMNS += (self.division_column,)

        self.COLUMN_MATCH_PATTERNS = {c: re.compile(c, re.IGNORECASE) for c in self.COLUMNS}

    def shorten_col(self, text):
        text = text.lower()
        words = text.split()

        short_list = []
        for word in words:
            short_list.append(word[:2])

        return ''.join(short_list)

    def _transpose_row(self, row):
        '''
        Only transpose columns that are defined in TRANSPOSE_COLUMNS
        '''
        # create the id
        char_id = 't{:03d}c{:03d}'.format(self._topic_idx, self._char_idx)

        # if the first Characteristic, then reset the lineage and record the parent
        if self._char_idx == 1:
            self._prev_char_id = None
            self._topic_lineage = []
        else:
            self._prev_char_id = 't{:03d}c{:03d}'.format(self._topic_idx, self._char_idx - 1)

        # Level tracks how indented the TRANSPOSE_COLUMN_PREFIX
        # 3 spaces == 1 cur_generation
        char_val = row[self.TRANSPOSE_COLUMN_PREFIX]
        char_val = re.sub('[^\s!-~]', ' ', char_val)   # remove gremlins
        num_spaces = (len(char_val) - len(char_val.lstrip()))
        if num_spaces == 2 or num_spaces == 3:
            self._space_divisor = num_spaces

        if num_spaces == 1:
            raise Exception('num_spaces == 1')
        cur_generation = num_spaces / self._space_divisor

        if cur_generation > len(self._topic_lineage):
            self._topic_lineage.append(self._prev_char_id)     # add prev id to the lineage

        # remove all ancestors until we're at cur_generation
        while cur_generation < len(self._topic_lineage):
            self._topic_lineage.pop()

        if len(self._topic_lineage) == 0:
            parent_id = None
        else:
            parent_id = self._topic_lineage[-1]    # parent is the last id in the lineage

        vals = []
        for col in self.TRANSPOSE_COLUMNS:
            new_char_id = '{}_{}'.format(char_id, col[:1])
            vals.append((new_char_id, row[col]),)

            #####################################
            # testing - TODO remove me
            #####################################
            if parent_id is not None:
                new_parent_id = '{}_{}'.format(parent_id, col[:1])

            if self._topic_idx <= 10:
                str1 = '''
                    {char_key} = OBSColumn(
                        id=\'{char_id}\',
                        name=\'{char_val} ({gender})\',
                        type=\'Numeric\',
                        weight=3,
                        tags=[ca, unit_people, subsections['age_gender']],
                    '''.format(
                            char_key=new_char_id,
                            char_id=new_char_id,
                            char_val=char_val.strip().replace('\'', '\\\''),
                            gender=col,
                        )
                if parent_id is None:
                    str1 += 'targets={},'
                else:
                    str1 += 'targets={{ {}: DENOMINATOR }},'.format(new_parent_id)

                str1 += ')'

                filename = 'cols.py'
                self._write_to_file(filename, str1)

                # write the 2nd part of the columns
                str2 = '(\'{}\', {}),'.format(new_char_id, new_char_id)

                filename = 'dict.py'
                self._write_to_file(filename, str2)
            #####################################

        self._char_idx += 1

        return tuple(vals)

    def _write_to_file(self, filename, string):
        if filename not in self._file_handlers:
            file_path = os.path.join(self._output_dir, filename)
            file_handle = file(file_path, 'w')
            self._file_handlers[filename] = file_handle

        print(string, file=self._file_handlers[filename])

    def _group_record(self, record):
        # Columns not transposed
        geo_col = ((self.GEO_COLUMN, record[0][self.GEO_COLUMN],),)
        # For each row in record transpose columns defined
        # in TRANSPOSE_COLUMNS.
        transposed_row = list(map(self._transpose_row, record))
        transposed_cols = tuple(itertools.chain.from_iterable(transposed_row))
        return OrderedDict(geo_col + transposed_cols)

    def _write_group_to_csv(self, parse_col, group):
        if len(group) == 0:
            return

        formatted_record = self._group_record(group)
        if parse_col not in self._file_handlers:
            file_path = os.path.join(self._output_dir,
                                     't{:03d}.csv'.format(
                                        self._topic_idx))
            file_handle = file(file_path, 'w')
            self._file_handlers[parse_col] = file_handle
            self._topic_idx += 1
            self._char_idx = 1
            print('"{}"'.format('","'.join(list(formatted_record.keys()))),
                  file=self._file_handlers[parse_col])

        print(','.join(map(str, list(formatted_record.values()))),
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

    def _match_col(self, col):
        '''
        Check if a column ~matches one of the predefined columns.
        -> case insensitive
        -> match has to be at the beginning of the string
        '''

        for col_key, pat in list(self.COLUMN_MATCH_PATTERNS.items()):
            r = pat.match(col)
            if r is not None and r.start() == 0:
                return col_key
        return

    def get_header_row(self, csvfile):
        '''
        Get row index (0-based) in csv and column index
        for each predefined column.
        '''
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')

        col_count = len(self.COLUMNS)
        for i, row in enumerate(reader):
            # index column locations in csv
            col_indices = {self._match_col(col): n for n, col in enumerate(row)}

            # Remove unmatched result
            col_indices.pop(None)

            if len(col_indices) == col_count:
                return i, col_indices

        return -1, None

    def _write_record(self, record):
        if len(record) == 0:
            return

        record = sorted(record, key=itemgetter(self.PARSE_COLUMN))

        parse_col_val = record[0][self.PARSE_COLUMN]
        group = []
        for row in record:
            if parse_col_val != row[self.PARSE_COLUMN]:
                self._write_group_to_csv(parse_col_val, group)
                parse_col_val = row[self.PARSE_COLUMN]
                group = []

            group.append(row)

        self._write_group_to_csv(parse_col_val, group)

    # If the administrative division is splitted  we need to ignore the row
    # (the non-part row holds the aggregated data)
    def _is_administrative_division_splitted(self, row):
        if self.division_column is not None and self.division_pattern is not None:
            for pattern in self.division_pattern:
                if self.division_column in row and re.search(pattern, row[self.division_column]):
                    return True
        return False

    def parse_csv_to_files(self, csv_paths, output_dir):
        self._file_handlers = {}
        self._output_dir = output_dir
        self._char_idx = 1
        self._topic_idx = 1
        self._topic_lineage = []
        self._space_divisor = 2

        try:
            if isinstance(csv_paths, str):
                csv_paths = [csv_paths]

            for csv_path in csv_paths:
                self._header = None
                with file(csv_path, 'rb') as csvfile:
                    # Need to handle csv files where header row isn't row #1
                    header_row_index, self._header = self.get_header_row(csvfile)
                    if header_row_index == -1:
                        raise ValueError('Invalid file', csv_path)

                    reader = csv.reader(csvfile, delimiter=',', quotechar='"')
                    uid = None
                    record = []
                    for row in reader:
                        row = self._map_row(row)
                        if row is None:
                            continue

                        if self._is_administrative_division_splitted(row):
                            continue

                        # got a new Geo_Code
                        if uid != row[self.GEO_COLUMN]:
                            self._write_record(record)
                            record = []
                            uid = row[self.GEO_COLUMN]

                        record.append(row)

                    # parse the last record
                    self._write_record(record)

        finally:
            for file_handler in list(self._file_handlers.values()):
                file_handler.close()

            self._output_dir = None
            self._file_handlers = None
