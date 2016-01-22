#!/usr/bin/env python

'''
Bigmetadata tasks

tasks to download and create metadata
'''

from luigi import Task, Parameter, LocalTarget


class DownloadDMA(Task):

    URL = 'https://andrew.cartodb.com/api/v2/sql?q=select+*+from+andrew.dma_master_polygons&format=csv'

    def run(self):
        pass

    def output(self):
        pass


class DMA(Task):

    def requires(self):
        return DownloadDMA()

    def run(self):
        pass

