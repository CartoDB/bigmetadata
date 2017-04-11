'''
Sphinx functions for luigi bigmetadata tasks.
'''

from jinja2 import Environment, PackageLoader
from luigi import (WrapperTask, Task, LocalTarget, BooleanParameter, Parameter,
                   DateParameter)
from luigi.s3 import S3Target
from tasks.util import shell
from tasks.meta import current_session, OBSTag, OBSColumn
from tasks.carto import (GenerateStaticImage, ImagesForMeasure, GenerateThumb,
                         OBSMetaToLocal)

from datetime import date
from time import time
from tasks.util import LOGGER

import json
import os

ENV = Environment(loader=PackageLoader('catalog', 'templates'))

def strip_tag_id(tag_id):
    '''
    Strip leading `tags.` when it exists.
    '''
    return tag_id.replace('tags.', '')

ENV.filters['strip_tag_id'] = strip_tag_id

SECTION_TEMPLATE = ENV.get_template('section.html')
SUBSECTION_TEMPLATE = ENV.get_template('subsection.html')
COLUMN_TEMPLATE = ENV.get_template('column.html')
LICENSES_TEMPLATE = ENV.get_template('licenses.html')
SOURCES_TEMPLATE = ENV.get_template('sources.html')


class GenerateRST(Task):

    force = BooleanParameter(default=False)
    format = Parameter()
    images = BooleanParameter(default=False)

    def __init__(self, *args, **kwargs):
        super(GenerateRST, self).__init__(*args, **kwargs)
        if self.force:
            shell('rm -rf catalog/source/*/*')
        #shell('cp -R catalog/img catalog/source/')
        #shell('mkdir -p catalog/img_thumb')
        #shell('cp -R catalog/img_thumb catalog/source/')

    def requires(self):
        #session = current_session()
        requirements = {
            'meta': OBSMetaToLocal()
        }
        #for section_subsection, _ in self.output().iteritems():
        #    section_id, subsection_id = section_subsection

            # this is much faster with a gin index on numer_tags
            #resp = session.execute('''
            #    SELECT DISTINCT numer_id
            #    FROM observatory.obs_meta
            #    WHERE numer_tags ? 'section/{section_id}'
            #      AND numer_tags ? 'subsection/{subsection_id}'
            #    ORDER BY numer_id
            #'''.format(section_id=section_id,
            #           subsection_id=subsection_id))
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

    def complete(self):
        return getattr(self, '_complete', False)

    def output(self):
        targets = {}
        session = current_session()

        resp = session.execute('''
          WITH subquery AS (SELECT
            foo.geom_id,
            CASE WHEN foo.key LIKE 'section%' THEN foo.key ELSE NULL END section,
            CASE WHEN foo.key LIKE 'subsection%' THEN foo.key ELSE NULL END subsection
            FROM observatory.obs_meta_geom,
               LATERAL (SELECT geom_id, * FROM jsonb_each(geom_tags)) foo),
          subquery2 as (SELECT
            geom_id,
            REPLACE(MAX(section), 'section/', '') section,
            REPLACE(MAX(subsection), 'subsection/', '') subsection
            FROM subquery GROUP BY geom_id)
          SELECT DISTINCT UNNEST(section_tags), UNNEST(subsection_tags)
          FROM observatory.obs_meta
          UNION ALL
          SELECT DISTINCT section, subsection
          FROM subquery2
          WHERE section IS NOT NULL
            AND subsection IS NOT NULL
                               ''')
        for section_id, subsection_id in resp:
            targets[(section_id, subsection_id)] = LocalTarget(
                'catalog/source/{section}/{subsection}.rst'.format(
                    section=strip_tag_id(section_id),
                    subsection=strip_tag_id(subsection_id)))

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
                parents_resp = session.execute('''
                   SELECT DISTINCT c.id, ARRAY[]::text[] as children
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
                column_children_ids = parents_resp.fetchall()
                # Obtain full column data for every column ID in this section/subsection
                all_column_ids = set()
                for parent_id, youngest_parents in column_children_ids:
                    all_column_ids.add(parent_id)
                    if youngest_parents:
                        for subparent_id, children_ids in youngest_parents.iteritems():
                            all_column_ids.add(parent_id)
                            all_column_ids.update(children_ids)

                all_columns_resp = session.execute('''
                    SELECT c.id,
                           FIRST(c.name),
                           FIRST(c.description),
                           FIRST(c.type),
                           FIRST(c.extra),
                           FIRST(c.aggregate),
                           JSONB_Object_Agg(t.id, t.name),
                           ARRAY[]::Text[] denoms,
                           ARRAY[]::Text[]
                    FROM observatory.obs_column c,
                         observatory.obs_column_tag ct,
                         observatory.obs_tag t
                    WHERE c.id = ANY(ARRAY['{all_column_ids}'])
                      AND ct.column_id = c.id
                      AND ct.tag_id = t.id
                    GROUP BY c.id
                '''.format(all_column_ids="', '".join(all_column_ids)))
            else:
                # Obtain top-level IDs for this section/subsection
                parents_resp = session.execute('''
                    WITH RECURSIVE children(parent_id, children, lvl, path) AS (
                        -- Select root children corresponding to certain tags
                        SELECT numer_id parent_id, ARRAY[]::Text[] as children, 1 lvl, numer_id path
                        FROM observatory.obs_meta_numer children
                        WHERE numer_tags ? 'subsection/{subsection_id}'
                          AND numer_tags ? 'section/{section_id}'
                          AND numer_weight > 0
                        UNION
                        SELECT DISTINCT parent.denom_id parent_id,
                                        ARRAY_APPEND(children, children.parent_id::Text) children,
                                        lvl + 1 lvl,
                                        parent.denom_id || '/' || path
                        FROM observatory.obs_meta parent, children
                        WHERE parent.numer_id = children.parent_id
                    ) -- SELECT * FROM children;
                    -- For each child, we want the parent with the maximum lvl
                    , parents AS (
                        SELECT UNNEST(children) child_id,
                          FIRST(parent_id ORDER BY lvl DESC) oldest_parent_id,
                          FIRST(parent_id ORDER BY lvl ASC) youngest_parent_id,
                          max(lvl) lvl,
                          FIRST(path ORDER BY lvl DESC) path
                        FROM children
                        GROUP BY UNNEST(children)
                    )
                    -- We only want to return those parent IDs, along with any children who have no
                    -- parents (orphans)
                    , oldest_youngest AS (
                        SELECT Coalesce(oldest_parent_id, child_id) oldest_parent_id,
                               youngest_parent_id,
                               JSONB_OBJECT_AGG(child_id, path) children
                        FROM parents
                        GROUP BY Coalesce(oldest_parent_id, child_id), youngest_parent_id
                        ORDER BY Coalesce(oldest_parent_id, child_id), youngest_parent_id
                    )
                    SELECT oldest_parent_id,
                           JSONB_Object_Agg(youngest_parent_id, children)
                               FILTER (WHERE youngest_parent_id IS NOT NULL) youngest_parents
                    FROM oldest_youngest
                    GROUP BY oldest_parent_id
                    ORDER BY oldest_parent_id;
                '''.format(section_id=section_id,
                           subsection_id=subsection_id))

                column_children_ids = parents_resp.fetchall()

                # Obtain full column data for every column ID in this section/subsection
                all_column_ids = set()
                for parent_id, youngest_parents in column_children_ids:
                    all_column_ids.add(parent_id)
                    if youngest_parents:
                        for subparent_id, children_ids in youngest_parents.iteritems():
                            all_column_ids.add(parent_id)
                            all_column_ids.update(children_ids.keys())

                all_columns_resp = session.execute('''
                    SELECT numer_id,
                           numer_name,
                           numer_description,
                           numer_type,
                           numer_extra,
                           numer_aggregate,
                           numer_tags,
                           n.denoms,
                           ARRAY_AGG(DISTINCT ARRAY[
                            geom_id, geom_name, timespan_id,
                            geom_tags::Text
                           ])
                    FROM observatory.obs_meta_numer n,
                         observatory.obs_meta_geom g,
                         observatory.obs_meta_timespan t
                    WHERE numer_id = ANY (ARRAY['{all_column_ids}'])
                      AND geom_id = ANY(n.geoms)
                      AND timespan_id = ANY(n.timespans)
                    GROUP BY numer_id
                '''.format(all_column_ids="', '".join(all_column_ids)))

            all_columns = {}
            for col in all_columns_resp:
                geom_timespans = {}
                for gt in col[8]:
                    if gt[0] in geom_timespans:
                        geom_timespans[gt[0]]['timespans'].append(gt[2])
                    else:
                        geom_timespans[gt[0]] = {
                            'geom_id': gt[0],
                            'geom_name': gt[1],
                            'timespans': [gt[2]],
                            'geom_tags': json.loads(gt[3])
                        }
                all_columns[col[0]] = {
                    'id': col[0],
                    'name': col[1],
                    'description': col[2],
                    'type': col[3],
                    'extra': col[4],
                    'aggregate': col[5],
                    'tags': col[6],
                    'licenses': [tag_id.split('/')[1]
                                 for tag_id, tag_name in col[6].iteritems()
                                 if tag_id.startswith('license/')],
                    'sources': [tag_id.split('/')[1]
                                for tag_id, tag_name in col[6].iteritems()
                                if tag_id.startswith('source/')],
                    'denoms': col[7],
                    'geom_timespans': geom_timespans
                }

            target.makedirs()
            fhandle = target.open('w')

            # TODO aren't we duplicating this?
            with open('catalog/source/{}.rst'.format(strip_tag_id(section_id)), 'w') \
                    as section_fhandle:
                section_fhandle.write(SECTION_TEMPLATE.render(
                    section=section, **self.template_globals()))

            fhandle.write(SUBSECTION_TEMPLATE.render(
                subsection=subsection,
                column_children_ids=column_children_ids,
                all_columns=all_columns,
                format=self.format,
                **self.template_globals()
            ).encode('utf8'))

            fhandle.close()

            subsection_path = 'catalog/source/{section}/{subsection}/'.format(
                section=strip_tag_id(section_id),
                subsection=strip_tag_id(subsection_id)
            )
            if not os.path.exists(subsection_path):
                os.makedirs(subsection_path)

            for column_id, children in column_children_ids:

                with open('catalog/source/{section}/{subsection}/{column}.rst'.format(
                    section=strip_tag_id(section_id),
                    subsection=strip_tag_id(subsection_id),
                    column=column_id
                ), 'w') as column_fhandle:
                    column_fhandle.write(COLUMN_TEMPLATE.render(
                        col=all_columns[column_id], **self.template_globals()).encode('utf8'))

                if not children:
                    continue
                for _, subchild_id_paths in children.iteritems():
                    for subchild_id, subchild_path in subchild_id_paths.iteritems():

                        # Create each intermediate column in hierarchy
                        subchild_path_split = subchild_path.split('/')

                        dirpath = 'catalog/source/{section}/{subsection}/{dirpath}'.format(
                            section=strip_tag_id(section_id),
                            subsection=strip_tag_id(subsection_id),
                            dirpath='/'.join(subchild_path_split[0:-1])
                        )
                        if not os.path.exists(dirpath):
                            os.makedirs(dirpath)

                        for i in xrange(1, len(subchild_path_split)):
                            intermediate_id = subchild_path_split[i]
                            with open('catalog/source/{section}/{subsection}/{intermediate_path}/{intermediate_id}.rst'.format(
                                section=strip_tag_id(section_id),
                                subsection=strip_tag_id(subsection_id),
                                intermediate_path='/'.join(subchild_path_split[0:i]),
                                intermediate_id=intermediate_id
                            ), 'w') as subcolumn_fhandle:
                                subcolumn_fhandle.write(COLUMN_TEMPLATE.render(
                                    col=all_columns[intermediate_id], **self.template_globals()).encode('utf8'))

                        with open('catalog/source/{section}/{subsection}/{path}.rst'.format(
                            section=strip_tag_id(section_id),
                            subsection=strip_tag_id(subsection_id),
                            path=subchild_path
                        ), 'w') as subcolumn_fhandle:
                            subcolumn_fhandle.write(COLUMN_TEMPLATE.render(
                                col=all_columns[subchild_id], **self.template_globals()).encode('utf8'))
        self._complete = True


class Catalog(Task):

    force = BooleanParameter(default=False)
    format = Parameter(default='html')
    images = BooleanParameter(default=False)

    def requires(self):
        return  GenerateRST(force=self.force, format=self.format,
                               images=self.images)

    def complete(self):
        return getattr(self, '_complete', False)

    def run(self):
        shell("SPHINXOPTS='-j 4' cd catalog && make {}".format(self.format))
        #shell("cd catalog && make {}".format(self.format))
        self._complete = True


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
