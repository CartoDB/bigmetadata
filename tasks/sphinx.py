'''
Sphinx functions for luigi bigmetadata tasks.
'''

import re
from jinja2 import Environment, PackageLoader
from luigi import WrapperTask, Task, LocalTarget, BooleanParameter
from tasks.util import shell
from tasks.meta import session_scope, OBSTag


env = Environment(loader=PackageLoader('catalog', 'templates'))

def test_filter(arg):
    pass

env.filters['test'] = test_filter

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
            for tag in session.query(OBSTag):
                targets[tag.id] = LocalTarget('catalog/source/data/{tag}.rst'.format(tag=tag.id))
        return targets

    def run(self):
        with session_scope() as session:
            for tag_id, target in self.output().iteritems():
                fhandle = target.open('w')

                tag = session.query(OBSTag).get(tag_id)
                columns = [c for c in tag.columns]
                columns.sort(lambda x, y: -x.weight.__cmp__(y.weight))

                fhandle.write(TAG_TEMPLATE.render(tag=tag, columns=columns).encode('utf8'))
                fhandle.close()


class Sphinx(Task):

    force = BooleanParameter(default=False)

    def requires(self):
        return GenerateRST(force=self.force)

    def complete(self):
        return False

    def run(self):
        shell('cd catalog && make html')

