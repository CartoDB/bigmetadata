'''
Sphinx functions for luigi bigmetadata tasks.
'''

import jinja2
import re
from luigi import WrapperTask, Task, LocalTarget, BooleanParameter
from tasks.util import shell
from tasks.meta import session_scope, BMDTag
from tasks.us.census.tiger import HIGH_WEIGHT_COLUMNS


TEMPLATE = jinja2.Template(
    '''.. {{ tag.title }}:

{{ tag.name }}
===========================================================================

{{ tag.description }}

.. contents::
   :depth: 10

{% for col in columns %}
{{ col.name }}
----------------------------------------------------------------------------

{# preview map #}
{% if col.tables %}
.. rst-class:: cartodb-static-map

{{ col.slug_name }}
{% endif %}

:description: {{ col.description }}

{% for coltarget in col.targets %}
    :{{ coltarget.reltype }}:

    {{ coltarget.column.id }}
{% endfor %}

{% for table in col.tables %}

{% endfor %}
{% endfor %}

''')

#{% for col in columns %}
#.. docutils:column:: {{ col._source.name }}
#
#   {{ col._source.description }}
#
#Times & dates available:
#{% endfor %}

#                           '''
#Can be found in:
#
#{% for table in col.tables %}
#  * {{ table.id }}
#      {% for resolution in table.resolutions %}
#        * {{ resolution.id }}, {{ resolution.sample }}, {{ resolution.error }}</li>
#      {% endfor %}
#{% endfor %}
#    '''


class GenerateRST(Task):

    force = BooleanParameter(default=False)

    def __init__(self, *args, **kwargs):
        super(GenerateRST, self).__init__(*args, **kwargs)
        if self.force:
            shell('rm -rf catalog/source/data/*')

    def output(self):
        targets = {}
        with session_scope() as session:
            for tag in session.query(BMDTag):
                targets[tag.id] = LocalTarget('catalog/source/data/{tag}.rst'.format(tag=tag.id))
        return targets

    def run(self):
        with session_scope() as session:
            for tag_id, target in self.output().iteritems():
                fhandle = target.open('w')

                tag = session.query(BMDTag).get(tag_id)
                tag.columns.sort(lambda x, y: -x.column.weight.__cmp__(y.column.weight))

                columns = []
                # augment column with additional characteristics
                for coltag in tag.columns:
                    col = coltag.column
                    columns.append(col)

                    #_id = re.sub(r'^data/', '', col.id)
                    #col['slug_name'] = slug_column(col['_source']['name'])

                    # # TODO more precise margin of error
                    # # right now we just average all the national margins of error
                    # # to give an overview
                    # national_margins = []
                    # for table in col['_source'].get('tables', []):
                    #     for resolution in table.get('resolutions', []):
                    #         if resolution['id'].endswith('us/census/tiger/nation'):
                    #             national_margins += [resolution['error']]
                    # if national_margins:
                    #     col['margin_of_error'] = sum(national_margins) / len(national_margins)
                    #     if col['margin_of_error'] < 0.2:
                    #         col['margin_of_error_cat'] = 'low'
                    #     elif col['margin_of_error'] < 1:
                    #         col['margin_of_error_cat'] = 'medium'
                    #     else:
                    #         col['margin_of_error_cat'] = 'high'
                    #
                    # # break tables into year/resolution columns
                    # tables_by_year = {}
                    # tables_by_resolution = {}
                    # for table in col['_source'].get('tables', []):
                    #     year = str(table.get('dct_temporal_sm')).split()[-1]
                    #     if year not in tables_by_year:
                    #         tables_by_year[year] = {}
                    #     for resolution in table.get('resolutions', []):
                    #         short_res = resolution['id'].split('/')[-1]
                    #         if short_res not in HIGH_WEIGHT_COLUMNS:
                    #             continue
                    #         table['sample'] = table['title'].split(' ')[0]\
                    #                 .split('_')[1]
                    #         if resolution.get('error', 1) < 0.2:
                    #             table['style'] = '**'
                    #         elif resolution.get('error', 1) < 1:
                    #             table['style'] = ''
                    #         else:
                    #             table['style'] = '*'

                    #         if short_res not in tables_by_year[year]:
                    #             tables_by_year[year][short_res] = []
                    #         tables_by_year[year][short_res].append(table)
                    #         if short_res not in tables_by_resolution:
                    #             tables_by_resolution[short_res] = {}
                    #         if year not in tables_by_resolution[short_res]:
                    #             tables_by_resolution[short_res][year] = []
                    #         tables_by_resolution[short_res][year].append(table)

                    # col['tables_by_year'] = tables_by_year
                    # col['tables_by_resolution'] = tables_by_resolution

                fhandle.write(TEMPLATE.render(tag=tag, columns=columns).encode('utf8'))
                fhandle.close()


class Sphinx(Task):

    force = BooleanParameter(default=False)

    def requires(self):
        return GenerateRST(force=self.force)

    def complete(self):
        return False

    def run(self):
        shell('cd catalog && make html')

