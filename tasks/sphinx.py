'''
Sphinx functions for luigi bigmetadata tasks.
'''

import re
from jinja2 import Environment, PackageLoader
from luigi import WrapperTask, Task, LocalTarget, BooleanParameter
from tasks.util import shell
from tasks.meta import session_scope, BMDTag

env = Environment(loader=PackageLoader('catalog', 'templates'))
TAG_TEMPLATE = env.get_template('tag.html')


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

                fhandle.write(TAG_TEMPLATE.render(tag=tag).encode('utf8'))
                fhandle.close()


class Sphinx(Task):

    force = BooleanParameter(default=False)

    def requires(self):
        return GenerateRST(force=self.force)

    def complete(self):
        return False

    def run(self):
        shell('cd catalog && make html')

