import io
import csv


class CSVNormalizerStream(io.IOBase):
    '''
    Filter for applying a function to each line of a CSV file, compatible with iostreams
    '''
    def __init__(self, infile, func):
        '''
        :param infile: A stream that reads a CSV file. e.g: a file
        :param func: A function to apply to each CSV row. It takes an array of fields and returns an array of fields.
        '''
        self._csvreader = csv.reader(infile)
        self._buffer = ''
        self._func = func

    def read(self, nbytes):
        try:
            while len(self._buffer) < nbytes:
                self.getline()
            out, self._buffer = self._buffer[:nbytes], self._buffer[nbytes:]
            return out
        except StopIteration:
            out = self._buffer
            self._buffer = ''
            return out

    def getline(self):
        line = next(self._csvreader)
        clean_line = self._func(line)
        self._buffer += (','.join(clean_line) + ' \n')
