#http://www.ine.es/pcaxisdl/t20/e245/p07/a2015/l0/0001.px

import csv
import os

from luigi import Task, LocalTarget
from tasks.util import (LoadPostgresFromURL, classpath, pg_cursor, shell,
                        CartoDBTarget, get_logger, underscore_slugify, TableTask,
                        session_scope, ColumnTarget, ColumnsTask, TagsTask,
                        classpath)


class Tags(TagsTask):

    def tags(self):
        pass


class Columns(ColumnsTask):

    def columns(self):
        pass


class Download(Task):

    URL = 'http://www.ine.es/pcaxisdl/t20/e245/p07/a2015/l0/0001.px'

    def run(self):
        self.output().makedirs()
        cmd = 'wget "{url}" -O "{output}"'.format(url=self.URL,
                                              output=self.output().path)
        shell(cmd)

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), '0001.px'))


class Parse(Task):
    '''
    convert px file to csv
    '''

    def requires(self):
        return Download()

    def run(self):
        output = self.output()
        dimensions = []
        self.output().makedirs()
        with self.output().open('w') as outfile:
            section = None
            with self.input().open() as infile:
                for line in infile:
                    line = line.strip()
                    if line.startswith('VALUES'):
                        section = 'values'
                        dimensions.append([line.split('"')[1], [], 0])
                        line = line.split('=')[1]

                    if section == 'values':
                        dimensions[-1][1].extend([l.strip(';" ') for l in line.split(',') if l])
                        if line.endswith(';'):
                            section = None
                        continue

                    if line.startswith('DATA='):
                        section = 'data'
                        headers = [d[0] for d in dimensions[0:-1]]
                        headers.extend([h.strip(';" ') for h in dimensions.pop()[1] if h])
                        writer = csv.DictWriter(outfile, headers)
                        writer.writeheader()
                        continue

                    if section == 'data':
                        if line.startswith(';'):
                            continue

                        values = {}
                        for dimname, dimvalues, dimcnt in dimensions:
                            try:
                                values[dimname] = dimvalues[dimcnt]
                            except IndexError:
                                import pdb
                                pdb.set_trace()
                                print dimcnt
                        i = len(dimensions) - 1
                        while dimensions[i][2] + 2 > len(dimensions[i][1]):
                            dimensions[i][2] = 0
                            i -= 1
                        dimensions[i][2] += 1

                        for i, d in enumerate(line.split(' ')):
                            values[headers[len(dimensions) + i]] = d

                        writer.writerow(values)


    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self), '0001.csv'))
