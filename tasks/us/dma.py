#!/usr/bin/env python

'''
Bigmetadata tasks

tasks to download and create metadata
'''

from tasks.tags import SubsectionTags, SectionTags
from tasks.meta import (GEOM_REF, current_session, GEOM_NAME, OBSColumn)
from tasks.util import ColumnsTask, TableTask, Carto2TempTableTask, MetaWrapper

from collections import OrderedDict


class ImportDMA(Carto2TempTableTask):

    subdomain = 'andrew'
    table = 'dma_master_polygons'


class DMAColumns(ColumnsTask):

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
        }

    def version(self):
        return 2

    def columns(self):
        inputs = self.input()
        boundary = inputs['subsections']['boundary']
        united_states = inputs['sections']['united_states']

        the_geom = OBSColumn(
            name='Television Market Area',
            description='A television market area, also called a DMA, is '
            'a group of counties in the United States covered by a specific '
            'group of television stations.  There are 210 DMAs in the United '
            'States.',
            type='Geometry',
            weight=6,
            tags=[united_states, boundary],
        )
        dma_code = OBSColumn(
            name='Television Market Area code',
            type='Numeric',
            weight=0,
            targets={the_geom: GEOM_REF},
        )
        dma_name = OBSColumn(
            name='Name of Television Market Area',
            type='Text',
            weight=0,
            targets={the_geom: GEOM_NAME},
        )

        return OrderedDict([
            ('the_geom', the_geom),
            ('dma_code', dma_code),
            ('dma_name', dma_name),
        ])


class DMA(TableTask):

    def requires(self):
        return {
            'meta': DMAColumns(),
            'data': ImportDMA(),
        }

    def version(self):
        return 5

    def timespan(self):
        return '2013'

    def columns(self):
        return self.input()['meta']

    def populate(self):
        session = current_session()
        session.execute('INSERT INTO {output} '
                        'SELECT the_geom, dma_code, dma_name '
                        'FROM {input} '.format(
                            output=self.output().table,
                            input=self.input()['data'].table
                        ))
