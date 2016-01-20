'''
Sphinx functions for luigi bigmetadata tasks.
'''

import jinja2
import re
from luigi import WrapperTask, Task, LocalTarget, BooleanParameter
from tasks.util import shell, elastic_conn


TEMPLATE = jinja2.Template(
    '''.. {{ tag }}:

{{ tag_desc }}
===========================================================================

.. contents::
   :depth: 10

{% for col in columns %}
{{ col._source.name }}
----------------------------------------------------------------------------

:description: {{ col._source.description }}

{% if col._source.relationships.denominator %}
:denominator:

    {{ col._source.relationships.denominator }}
{% endif %}

:dates available:

    {% for table in col._source.tables %}{{ table.dct_temporal_sm }}, {% endfor %}

:resolutions available:

    {% for res in col._source.tables[0].resolutions %}{{ res.id }}, {% endfor %}
    {#
    {% for table in col._source.tables %}
    {% for res in table.resolutions %}{{ res.id }}, {% endfor %}
    {% endfor %}
    #}

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

    TAGS = {'denominators': 'Denominators',
            'housing': 'Housing',
            'income_education_employment': 'Income, Education & Employment',
            'language': 'Language',
            'race_age_gender': 'Race, Age & Gender',
            'transportation': 'Transportation'}

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
            fhandle.write(TEMPLATE.render(tag=tag, tag_desc=self.TAGS[tag],
                                          columns=columns).encode('utf8'))
            fhandle.close()


class Sphinx(Task):

    force = BooleanParameter(default=False)

    def requires(self):
        return JSON2RST(force=self.force)

    def complete(self):
        return False

    def run(self):
        shell('cd catalog && make html')

