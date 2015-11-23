#!/usr/bin/env python

'''
Bigmetadata tasks

tasks to download and create metadata
'''

import requests
from luigi import Task, Parameter, LocalTarget, DateParameter, run


class DownloadTask(Task):
    url = Parameter()
    name = Parameter()
    date_checked = DateParameter()

    def run(self):
        out = self.output().open('w')
        requests.get(self.url)
        out.close()

    def output(self):
        return LocalTarget('tmp/{}'.format(self.name))


class ACSDownloadTask(Task):

    URLS = dict(
        ('acs2013_5yr', 'http://www2.census.gov/acs2013_5yr/summaryfile/Sequence_Number_and_Table_Number_Lookup.txt', ),
        ('acs2013_3yr', 'http://www2.census.gov/acs2013_3yr/summaryfile/Sequence_Number_and_Table_Number_Lookup.txt', ),
        ('acs2013_1yr', 'http://www2.census.gov/acs2013_1yr/summaryfile/Sequence_Number_and_Table_Number_Lookup.txt', ),
        ('acs2012_5yr', 'http://www2.census.gov/acs2012_5yr/summaryfile/Sequence_Number_and_Table_Number_Lookup.txt', ),
        ('acs2012_3yr', 'http://www2.census.gov/acs2012_3yr/summaryfile/Sequence_Number_and_Table_Number_Lookup.txt', ),
        ('acs2012_1yr', 'http://www2.census.gov/acs2012_1yr/summaryfile/Sequence_Number_and_Table_Number_Lookup.txt', ),
        ('acs2011_5yr', 'http://www2.census.gov/acs2011_5yr/summaryfile/Sequence_Number_and_Table_Number_Lookup.txt', ),
        ('acs2011_3yr', 'http://www2.census.gov/acs2011_3yr/summaryfile/Sequence_Number_and_Table_Number_Lookup.txt', ),
        ('acs2011_1yr', 'http://www2.census.gov/acs2011_1yr/summaryfile/Sequence_Number_and_Table_Number_Lookup.txt', ),
        ('acs2010_5yr', 'http://www2.census.gov/acs2010_5yr/summaryfile/Sequence_Number_and_Table_Number_Lookup.txt', ),
        ('acs2010_3yr', 'http://www2.census.gov/acs2010_3yr/summaryfile/Sequence_Number_and_Table_Number_Lookup.txt', ),
        ('acs2010_1yr', 'http://www2.census.gov/acs2010_1yr/summaryfile/Sequence_Number_and_Table_Number_Lookup.txt', ),
    )

    def require(self):
        return [DownloadTask(name=name, url=url) for name, url in ACSDownloadTask.URLS]


if __name__ == '__main__':
    run()
