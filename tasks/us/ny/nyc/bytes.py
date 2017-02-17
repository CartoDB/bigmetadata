import subprocess

from tasks.util import pg_cursor, classpath
from luigi import Task, Parameter, LocalTarget


class MapPLUTO(Task):
    pass


class DownloadPLUTO(Task):

    url = 'http://www.nyc.gov/html/dcp/download/bytes/nyc_pluto_{version}.zip'
    version = Parameter()

    def run(self):
        self.output().makedirs()
        subprocess.check_call('curl "{url}" > {output}'.format(
            url=self.url.format(version=self.version),
            output=self.output().path
        ), shell=True)

    def output(self):
        return LocalTarget(path=classpath(self) + self.version)


class UnzipPLUTO(Task):

    version = Parameter()

    def requires(self):
        return DownloadPLUTO(version=self.version)

    def run(self):
        subprocess.check_call('unzip {input}'.format(input=self.input().path),
                              shell=True)

    def output(self):
        return LocalTarget(is_tmp=True)


class PLUTO(Task):
    version = Parameter()

    def requires(self):
        return DownloadPLUTO(version=self.version)

    def run(self):
        pass

    def output(self):
        return DefaultPostgresTarget(table=id_, update_id=id_)
