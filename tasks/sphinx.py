'''
Sphinx functions for luigi bigmetadata tasks.
'''

import os
import re

from jinja2 import Environment, PackageLoader
from luigi import (WrapperTask, Task, LocalTarget, BooleanParameter, Parameter,
                   DateParameter)
from luigi.s3 import S3Target
from tasks.util import shell
from tasks.meta import current_session, OBSTag, OBSColumn
from tasks.carto import GenerateStaticImage, ImagesForMeasure, GenerateThumb

from datetime import date
from time import time
from tasks.util import LOGGER

ENV = Environment(loader=PackageLoader('catalog', 'templates'))

def strip_tag_id(tag_id):
    '''
    Strip leading `tags.` when it exists.
    '''
    return tag_id.replace('tags.', '')

ENV.filters['strip_tag_id'] = strip_tag_id

SECTION_TEMPLATE = ENV.get_template('section.html')
SUBSECTION_TEMPLATE = ENV.get_template('subsection.html')
LICENSES_TEMPLATE = ENV.get_template('licenses.html')
SOURCES_TEMPLATE = ENV.get_template('sources.html')


class GenerateRST(Task):

    force = BooleanParameter(default=False)
    format = Parameter()
    preview = BooleanParameter(default=False)
    images = BooleanParameter(default=False)

    def __init__(self, *args, **kwargs):
        super(GenerateRST, self).__init__(*args, **kwargs)
        if self.force:
            shell('rm -rf catalog/source/*/*')
        shell('cp -R catalog/img catalog/source/')
        shell('mkdir -p catalog/img_thumb')
        shell('cp -R catalog/img_thumb catalog/source/')

    def requires(self):
        session = current_session()
        requirements = {}
        for section_subsection, _ in self.output().iteritems():
            section_id, subsection_id = section_subsection

            # this is much faster with a gin index on numer_tags
            resp = session.execute('''
                SELECT DISTINCT numer_id
                FROM observatory.obs_meta
                WHERE numer_tags ? 'section/{section_id}'
                  AND numer_tags ? 'subsection/{subsection_id}'
                ORDER BY numer_id
            '''.format(section_id=section_id,
                       subsection_id=subsection_id))
            #if self.images:
            #    for row in resp:
            #        column_id = row[0]
            #        if column_id.startswith('uk'):
            #            if self.format == 'pdf':
            #                img = GenerateThumb(measure=column_id, force=False)
            #            else:
            #                img = ImagesForMeasure(measure=column_id, force=False)
            #            requirements[column_id] = img

        return requirements

    def output(self):
        targets = {}
        session = current_session()
        i = 0
        for section in session.query(OBSTag).filter(OBSTag.type == 'section'):
            targets[(section.id, 'tags.boundary')] = LocalTarget(
                'catalog/source/{section}/boundary.rst'.format(
                    section=strip_tag_id(section.id)))
            for subsection in session.query(OBSTag).filter(OBSTag.type == 'subsection'):
                i += 1
                if i > 10 and self.preview:
                    break
                targets[(section.id, subsection.id)] = LocalTarget(
                    'catalog/source/{section}/{subsection}.rst'.format(
                        section=strip_tag_id(section.id),
                        subsection=strip_tag_id(subsection.id)))

        targets[('licenses', None)] = LocalTarget('catalog/source/licenses.rst')
        targets[('sources', None)] = LocalTarget('catalog/source/sources.rst')
        return targets

    def template_globals(self):
        image_path = '../img_thumb' if self.format == 'pdf' else '../img'
        return {
            'IMAGE_PATH': image_path
        }

    def build_licenses(self, target):
        session = current_session()
        fhandle = target.open('w')
        fhandle.write(LICENSES_TEMPLATE.render(
            licenses=session.query(OBSTag).filter(
                OBSTag.type == 'license').order_by(OBSTag.name),
            **self.template_globals()
        ).encode('utf8'))
        fhandle.close()

    def build_sources(self, target):
        session = current_session()
        fhandle = target.open('w')
        fhandle.write(SOURCES_TEMPLATE.render(
            sources=session.query(OBSTag).filter(
                OBSTag.type == 'source').order_by(OBSTag.name),
            **self.template_globals()
        ).encode('utf8'))
        fhandle.close()

    def run(self):
        session = current_session()
        for section_subsection, target in self.output().iteritems():
            before = time()
            section_id, subsection_id = section_subsection

            if section_id == 'licenses':
                self.build_licenses(target)
                continue
            elif section_id == 'sources':
                self.build_sources(target)
                continue

            section = session.query(OBSTag).get(section_id)
            subsection = session.query(OBSTag).get(subsection_id)

            LOGGER.info('%s:', section_subsection)

            if subsection_id == 'tags.boundary':
                resp = session.execute('''
                    SELECT DISTINCT c.id
                    FROM observatory.obs_tag section_t,
                         observatory.obs_column_tag section_ct,
                         observatory.obs_tag subsection_t,
                         observatory.obs_column_tag subsection_ct,
                         observatory.obs_column c
                    WHERE section_t.id = section_ct.tag_id
                      AND subsection_t.id = subsection_ct.tag_id
                      AND c.id = section_ct.column_id
                      AND c.id = subsection_ct.column_id
                      AND subsection_t.id = '{subsection_id}'
                      AND section_t.id = '{section_id}'
                      AND subsection_t.type = 'subsection'
                      AND section_t.type = 'section'
                    GROUP BY c.id
                    ORDER BY c.id
                '''.format(section_id=section_id,
                           subsection_id=subsection_id))
            else:
                resp = session.execute('''
                    SELECT DISTINCT numer_id
                    FROM observatory.obs_meta
                    WHERE numer_tags ? 'section/{section_id}'
                      AND numer_tags ? 'subsection/{subsection_id}'
                    ORDER BY numer_id
                '''.format(section_id=section_id,
                           subsection_id=subsection_id))

            target.makedirs()
            fhandle = target.open('w')

            columns = []
            for col_id in resp:
                col = session.query(OBSColumn).get(col_id)

                # tags with denominators will appear beneath that denominator
                if not col.has_denominators():
                    columns.append(col)

                # unless the denominator is not in this subsection
                else:
                    add_to_columns = True
                    for denominator in col.denominators():
                        if subsection in denominator.tags:
                            add_to_columns = False
                            break
                    if add_to_columns:
                        columns.append(col)

            columns.sort(lambda x, y: cmp(x.name, y.name))

            with open('catalog/source/{}.rst'.format(strip_tag_id(section_id)), 'w') \
                    as section_fhandle:
                section_fhandle.write(SECTION_TEMPLATE.render(
                    section=section, **self.template_globals()))
            if columns:
                fhandle.write(SUBSECTION_TEMPLATE.render(
                    subsection=subsection, columns=columns, format=self.format,
                    **self.template_globals()
                ).encode('utf8'))
            else:
                fhandle.write('')
            fhandle.close()
            after = time()
            LOGGER.info('%s, %s', section_subsection, after - before)


class Catalog(Task):

    force = BooleanParameter(default=False)
    format = Parameter(default='html')
    preview = BooleanParameter(default=False)
    images = BooleanParameter(default=False)

    def requires(self):
        return GenerateRST(force=self.force, format=self.format, preview=self.preview,
                           images=self.images)

    def complete(self):
        return False

    def run(self):
        shell('cd catalog && make {}'.format(self.format))


class PDFCatalogToS3(Task):

    timestamp = DateParameter(default=date.today())
    force = BooleanParameter(significant=False)

    def __init__(self, **kwargs):
        if kwargs.get('force'):
            try:
                shell('aws s3 rm s3://data-observatory/observatory.pdf')
            except:
                pass
        super(PDFCatalogToS3, self).__init__()

    def run(self):
        for target in self.output():
            shell('aws s3 cp catalog/build/observatory.pdf {output} '
                  '--acl public-read'.format(
                      output=target.path
                  ))

    def output(self):
        return [
            S3Target('s3://data-observatory/observatory.pdf'),
            S3Target('s3://data-observatory/observatory-{timestamp}.pdf'.format(
                timestamp=self.timestamp
            )),
        ]
