'''
Sphinx functions for luigi bigmetadata tasks.
'''

import jinja2
import re
from luigi import WrapperTask, Task, LocalTarget, BooleanParameter
from tasks.util import shell, elastic_conn
from tasks.us.census.tiger import HIGH_WEIGHT_COLUMNS


TEMPLATE = jinja2.Template(
    '''.. {{ tag.title }}:

{{ tag.title }}
===========================================================================

{{ tag.short_description }}

.. contents::
   :depth: 10

{% for col in columns %}
{{ col._source.name }}
----------------------------------------------------------------------------

:description: {{ col._source.description }}

{% if col._source.relationships and col._source.relationships.denominator %}
:denominator:

    {{ col._source.relationships.denominator }}
{% endif %}

{% if col._source.tables %}
{% if 'margin_of_error' in col %}
:margin of error: {{ '%2.2f' | format(col.margin_of_error)  }}% ({{ col.margin_of_error_cat }})

{% endif %}
:resolutions available: (**low error**, medium error, *high error*)

.. list-table::
    :header-rows: 1
    :stub-columns: 1


    * -
    {% for year, resolutions in col.tables_by_year|dictsort %}
      - {{ year }}
    {% endfor %}
    {% for resolution, years in col.tables_by_resolution|dictsort %}
    * - {{ resolution }}
      {% for year, _ in col.tables_by_year|dictsort  %}
      - {% for table in years.get(year, [])|sort(attribute='sample') %}{{ table.style }}{{ table.sample }}{{ table.style }}, {% endfor %}
      {% endfor %}
    {% endfor %}

{#
{% for resolution, years in col.resolutions|dictsort %}
    * {{ resolution }}: {{ years|sort|join(', ') }}
{% endfor %}
#}

{% endif %}

:bigmetadata source: `View <{{ col.gh_view_url }}>`_, `Edit <{{ col.gh_edit_url }}>`_

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


class JSON2RST(Task):

    force = BooleanParameter(default=False)

    TAGS = {
        'denominators': {
            'title': u'Denominators',
            'short_description': u'Use these to provide a baseline for comparison between different areas.',
            'long_description': u'',
        },
        'housing': {
            'title': 'Housing',
            'short_description': u'What type of housing exists and how do people live in it?',
            'long_description': u'',
        },
        'income_education_employment': {
            'title': 'Income, Education & Employment',
            'short_description': u'',
            'long_description': u'',
        },
        'language': {
            'title': 'Language',
            'short_description': u'What languages do people speak?',
            'long_description': u'',
        },
        'race_age_gender': {
            'title': 'Race, Age & Gender',
            'short_description': u'Basic demographic breakdowns.',
            'long_description': u'',
        },
        'transportation': {
            'title': 'Transportation',
            'short_description': u'How do people move from place to place?',
            'long_description': u'',
        },
        'boundary': {
            'title': 'Boundaries',
            'short_description': u'Use these to provide regions for sound comparison and analysis.',
            'long_description': u'',
        }
    }

    def __init__(self, *args, **kwargs):
        super(JSON2RST, self).__init__(*args, **kwargs)
        if self.force:
            shell('rm -rf catalog/source/data/*')

    def columns_for_tag(self, tag):
        econn = elastic_conn()
        return econn.search(
            doc_type='column',
            body={
                "filter": {
                    "bool": {
                        "must": [{
                            "range": {
                                "weight": {
                                    "from": 2, "to": 10
                                }
                            },
                        }, {
                            "missing": {
                                "field": "extra.margin_of_error"
                            },
                        }, {
                            "match": {
                                "tags": tag
                            }
                        }]
                    }
                },
                "sort": [
                    {"name.raw": {"order": "asc"}}
                ]
            },
            size=10000)['hits']['hits']

    def output(self):
        targets = {}
        for tag in self.TAGS.keys():
            targets[tag] = LocalTarget('catalog/source/data/{tag}.rst'.format(tag=tag))
        return targets

    def run(self):
        for tag, target in self.output().iteritems():
            #col = self.columns()[_id]
            columns = self.columns_for_tag(tag)
            fhandle = target.open('w')

            # augment column with additional characteristics
            for col in columns:
                _id = re.sub(r'^data/', '', col['_source']['id'])
                col['gh_view_url'] = 'https://github.com/talos/bmd-data/tree/master/{}'.format(_id)
                col['gh_edit_url'] = 'https://github.com/talos/bmd-data/edit/master/{}'.format(_id)

                # TODO more precise margin of error
                # right now we just average all the national margins of error
                # to give an overview
                national_margins = []
                for table in col['_source'].get('tables', []):
                    for resolution in table.get('resolutions', []):
                        if resolution['id'].endswith('us/census/tiger/nation'):
                            national_margins += [resolution['error']]
                if national_margins:
                    col['margin_of_error'] = sum(national_margins) / len(national_margins)
                    if col['margin_of_error'] < 0.2:
                        col['margin_of_error_cat'] = 'low'
                    elif col['margin_of_error'] < 1:
                        col['margin_of_error_cat'] = 'medium'
                    else:
                        col['margin_of_error_cat'] = 'high'

                # break tables into year/resolution columns
                tables_by_year = {}
                tables_by_resolution = {}
                for table in col['_source'].get('tables', []):
                    year = str(table.get('dct_temporal_sm')).split()[-1]
                    if year not in tables_by_year:
                        tables_by_year[year] = {}
                    for resolution in table.get('resolutions', []):
                        short_res = resolution['id'].split('/')[-1]
                        if short_res not in HIGH_WEIGHT_COLUMNS:
                            continue
                        table['sample'] = table['title'].split(' ')[0]\
                                .split('_')[1]
                        if resolution.get('error', 1) < 0.2:
                            table['style'] = '**'
                        elif resolution.get('error', 1) < 1:
                            table['style'] = ''
                        else:
                            table['style'] = '*'

                        if short_res not in tables_by_year[year]:
                            tables_by_year[year][short_res] = []
                        tables_by_year[year][short_res].append(table)
                        if short_res not in tables_by_resolution:
                            tables_by_resolution[short_res] = {}
                        if year not in tables_by_resolution[short_res]:
                            tables_by_resolution[short_res][year] = []
                        tables_by_resolution[short_res][year].append(table)

                col['tables_by_year'] = tables_by_year
                col['tables_by_resolution'] = tables_by_resolution

            fhandle.write(TEMPLATE.render(tag=self.TAGS[tag], columns=columns).encode('utf8'))
            fhandle.close()


class Sphinx(Task):

    force = BooleanParameter(default=False)

    def requires(self):
        return JSON2RST(force=self.force)

    def complete(self):
        return False

    def run(self):
        shell('cd catalog && make html')

