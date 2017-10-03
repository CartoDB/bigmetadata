'''
Sphinx functions for luigi bigmetadata tasks.
'''

from jinja2 import Environment, PackageLoader
from luigi import (Task, LocalTarget, BoolParameter, Parameter, DateParameter)
from luigi.contrib.s3 import S3Target
from tasks.util import shell, PostgresTarget, LOGGER
from tasks.meta import current_session, OBSTag, catalog_latlng
from tasks.carto import OBSMetaToLocal

from datetime import date

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

    force = BoolParameter(default=False)
    format = Parameter()
    section = Parameter(default=None)

    def __init__(self, *args, **kwargs):
        super(GenerateRST, self).__init__(*args, **kwargs)
        if self.force:
            shell('rm -rf catalog/source/*/*')

    def requires(self):
        requirements = {
            'meta': OBSMetaToLocal(force=True)
        }
        return requirements

    def output(self):
        tables = ['obs_meta', 'obs_meta_geom']
        if not all([PostgresTarget('observatory', t, non_empty=False).exists() for t in tables]):
            return []

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
            if self.section:
                if not section_id.startswith(self.section):
                    continue
            targets[(section_id, subsection_id)] = LocalTarget(
                'catalog/source/{section}/{subsection}.rst'.format(
                    section=strip_tag_id(section_id),
                    subsection=strip_tag_id(subsection_id)))

        targets[('licenses', None)] = LocalTarget('catalog/source/licenses.rst')
        targets[('sources', None)] = LocalTarget('catalog/source/sources.rst')

        return targets

    def template_globals(self):
        return {
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
                column_tree, all_columns = self._boundaries_tree(section_id, subsection_id)
            else:
                column_tree, all_columns = self._numerators_tree(section_id, subsection_id)

            subsection_path = 'catalog/source/{section}/{subsection}/'.format(
                section=strip_tag_id(section_id),
                subsection=strip_tag_id(subsection_id)
            )
            if not os.path.exists(subsection_path):
                os.makedirs(subsection_path)
            with open('catalog/source/{}.rst'.format(strip_tag_id(section_id)), 'w') \
                    as section_fhandle:
                section_fhandle.write(SECTION_TEMPLATE.render(
                    section=section, **self.template_globals()))

            target.makedirs()
            fhandle = target.open('w')
            fhandle.write(SUBSECTION_TEMPLATE.render(
                subsection=subsection,
                format=self.format,
                **self.template_globals()
            ).encode('utf8'))

            fhandle.close()

            if not all_columns:
                continue

            self._write_column_tree(
                [strip_tag_id(section_id), strip_tag_id(subsection_id)],
                column_tree,
                all_columns
            )

    def _boundaries_tree(self, section_id, subsection_id):
        boundaries_list_result = current_session().execute('''
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
        '''.format(section_id=section_id, subsection_id=subsection_id))
        boundary_ids = [row[0] for row in boundaries_list_result.fetchall()]

        boundaries_detail_result = current_session().execute('''
            SELECT c.id,
                    FIRST(c.name),
                    FIRST(c.description),
                    FIRST(c.type),
                    FIRST(ctab.extra),
                    FIRST(c.aggregate),
                    JSONB_Object_Agg(t.type || '/' || t.id, t.name),
                    'name' suggested_name,
                    FIRST(tab.timespan) timespan,
                    ARRAY[]::Text[] denoms,
                    ARRAY[]::Text[],
                    ST_AsText(ST_Envelope(FIRST(tab.the_geom))) envelope
            FROM observatory.obs_column c,
                    observatory.obs_column_tag ct,
                    observatory.obs_tag t,
                    observatory.obs_column_table ctab,
                    observatory.obs_table tab
            WHERE c.id = ANY(ARRAY['{}'])
                AND ct.column_id = c.id
                AND ct.tag_id = t.id
                AND c.id = ctab.column_id
                AND tab.id = ctab.table_id
            GROUP BY 1, 8
        '''.format("', '".join(boundary_ids)))
        boundary_data = self._parse_columns(boundaries_detail_result)
        return {k: {} for k in boundary_data.keys()}, boundary_data

    def _numerators_tree(self, section_id, subsection_id):
        numerator_paths_result = current_session().execute('''
            WITH RECURSIVE children(numer_id, path) AS (
                SELECT numer_id, ARRAY[]::Text[]
                FROM observatory.obs_meta_numer children
                WHERE numer_tags ? 'subsection/{subsection_id}'
                    AND numer_tags ? 'section/{section_id}'
                    AND numer_weight > 0
                UNION
                SELECT parent.denom_id,
                    children.numer_id || children.path
                FROM observatory.obs_meta parent, children
                WHERE parent.numer_id = children.numer_id
                ) SELECT path from children WHERE numer_id IS NULL;
        '''.format(section_id=section_id, subsection_id=subsection_id))

        numerator_tree = {}
        numerator_ids = set()
        for row in numerator_paths_result.fetchall():
            node = numerator_tree
            for mid in row[0]:
                numerator_ids.add(mid)
                if mid not in node:
                    node[mid] = {}
                node = node[mid]

        numerator_details_result = current_session().execute('''
            SELECT numer_id,
                    numer_name,
                    numer_description,
                    numer_type,
                    numer_extra,
                    numer_aggregate,
                    numer_tags,
                    numer_colname suggested_name,
                    numer_timespan timespan,
                    ARRAY_AGG(DISTINCT ARRAY[
                    denom_reltype,
                    denom_id,
                    denom_name
                    ]) denoms,
                    ARRAY_AGG(DISTINCT ARRAY[
                    geom_id, geom_name, numer_timespan,
                    geom_tags::Text
                    ]) geom_timespans,
                    FIRST(ST_AsText(ST_Envelope(the_geom))) envelope
            FROM observatory.obs_meta
            WHERE numer_id = ANY (ARRAY['{}'])
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
        '''.format("', '".join(numerator_ids)))
        return numerator_tree, self._parse_columns(numerator_details_result)

    def _parse_columns(self, all_columns_result):
        all_columns = {}
        for col in all_columns_result:
            geom_timespans = {}
            for gt in col[10]:
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
                'latlng': catalog_latlng(col[0]),
                'name': col[1],
                'description': col[2],
                'type': col[3],
                'extra': col[4],
                'aggregate': col[5],
                'tags': col[6],
                'suggested_name': col[7],
                'timespan': col[8],
                'licenses': [tag_id.split('/')[1]
                             for tag_id, tag_name in col[6].iteritems()
                             if tag_id.startswith('license/')],
                'sources': [tag_id.split('/')[1]
                            for tag_id, tag_name in col[6].iteritems()
                            if tag_id.startswith('source/')],
                'denoms': col[9],
                'geom_timespans': geom_timespans,
                'envelope': col[11]
            }
        return all_columns

    def _write_column_tree(self, path, tree, all_columns):
        for column_id, subtree in tree.iteritems():
            column_path = path + [column_id]
            self._write_column(
                column_path,
                all_columns[column_id],
                len(subtree)
            )
            if subtree:
                os.makedirs('catalog/source/' + '/'.join(column_path))
                self._write_column_tree(column_path, subtree, all_columns)

    def _write_column(self, path, column, numchildren):
        with open('catalog/source/{path}.rst'.format(path='/'.join(path)), 'w') as column_file:
            column_file.write(COLUMN_TEMPLATE.render(
                intermediate_path='/'.join(path[:-1]),
                numchildren=numchildren,
                col=column, **self.template_globals()).encode('utf8'))

class Catalog(Task):

    force = BoolParameter(default=False)
    format = Parameter(default='html')
    parallel_workers = Parameter(default=4)

    def requires(self):
        return GenerateRST(force=self.force, format=self.format)

    def complete(self):
        return getattr(self, '_complete', False)

    def run(self):
        shell("cd catalog && make SPHINXOPTS='-j {0}' {1}".format(self.parallel_workers, self.format))
        self._complete = True


class PDFCatalogToS3(Task):

    timestamp = DateParameter(default=date.today())
    force = BoolParameter(significant=False)

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
