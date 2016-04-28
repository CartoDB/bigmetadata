#!/usr/bin/env python

'''
Bigmetadata tasks

tasks to download and create metadata
'''

from tasks.carto import Import as CartoImport


class DMA(CartoImport):

    subdomain = 'andrew'
    table = 'dma_master_polygons'
