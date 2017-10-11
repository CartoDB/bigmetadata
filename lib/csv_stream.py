import io
import csv


class CSVNormalizerStream(io.IOBase):
    def __init__(self, infile, func):
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
        line = self._csvreader.next()
        clean_line = self._func(line)
        self._buffer += (','.join(clean_line) + ' \n')
