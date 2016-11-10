import os

from luigi import Task, Parameter, WrapperTask, LocalTarget
import csv




class CreateColumns(Task):
    tablename = Parameter(default='Basico')

    # def requires(self):
    #     return DownloadData(resolution=self.resolution, survey=self.survey)

    def run(self):
        infile = shell('ls meta/{tablename}.csv'.format(
            tablename=self.tablename
        ))
        # in_csv_files = infile.strip().split('\n')
        # os.makedirs(self.output().path)
        MetaParser().csv_to_cols(infile, self.output().path)

    def output(self):
        return LocalTarget('cols/{tablename}.py')


class StatCanParser(object):

    def csv_to_cols(self, csv_path, output):
        self._file_handlers = {}
        self._output = output

        try:
            self._header = None
            with file(csv_path, 'rb') as csvfile:
                # Need to handle csv files where header row isn't row #1
                header_row_index, self._header = self.get_header_row(csvfile)
                if header_row_index == -1:
                    raise ValueError('Invalid file')

                reader = csv.reader(csvfile, delimiter=',', quotechar='"')

                uid = None
                record = []
                for row in reader:
                    row = self._map_row(row)
                    if row is None:
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
            for file_handler in self._file_handlers.values():
                file_handler.close()

            self._output = None
            self._file_handlers = None
