#!/usr/bin/env python

'''
Bigmetadata tasks

tasks to download and create metadata
'''
from luigi import Parameter, Task


class LoadSocrataCSV(Task):

    domain = Parameter()
    dataset = Parameter()

    def requires(self):
        pass

    def target(self):
        pass
