import os
import urllib.request
from luigi import Task, LocalTarget
from lib.logger import get_logger
from tasks.util import (classpath, unqualified_task_id)
from tasks.foot_traffic.quadkey import tile2bounds, quadkey2tile

LOGGER = get_logger(__name__)


class DownloadData(Task):

    URL = 'YOUR_URL_HERE'

    def run(self):
        self.output().makedirs()
        urllib.request.urlretrieve(self.URL, self.output().path)

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self),
                                        unqualified_task_id(self.task_id) + '.csv'))


class AddLatLngData(Task):
    def requires(self):
        return DownloadData()

    def _point_position(self, z, x, y):
        lon0, lat0, lon1, lat1 = tile2bounds(z, x, y)
        return (lon0 + lon1) / 2, (lat0 + lat1) / 2

    def run(self):
        with open(self.output().path, 'w') as outfile, open(self.input().path, 'r', encoding='utf-8') as infile:
            i = 0
            for line in infile:
                line = line.split(',')
                quadkey = line[0]
                z, x, y = quadkey2tile(quadkey)
                lon, lat = self._point_position(z, x, y)
                line.append(lon)
                line.append(lat)
                outline = [line[0]] + [lon] + [lat] + [line[1]] + [line[2]] + [line[3]]
                outfile.write(','.join([str(c) for c in outline]))

                i = i + 1
                if i % 10000 == 0:
                    LOGGER.info('Written {i} lines'.format(i=i))

    def output(self):
        return LocalTarget(os.path.join('tmp', classpath(self),
                                        unqualified_task_id(self.task_id) + '.csv'))
