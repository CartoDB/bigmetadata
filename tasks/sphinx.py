'''
Sphinx functions for luigi bigmetadata tasks.
'''

import os
import re

from jinja2 import Environment, PackageLoader
from luigi import WrapperTask, Task, LocalTarget, BooleanParameter, Parameter
from tasks.util import shell
from tasks.meta import current_session, OBSTag
from tasks.carto import GenerateStaticImage, ImagesForMeasure


ENV = Environment(loader=PackageLoader('catalog', 'templates'))

#def test_filter(arg):
#    pass
#
#env.filters['test'] = test_filter

SECTION_TEMPLATE = ENV.get_template('section.html')
SUBSECTION_TEMPLATE = ENV.get_template('subsection.html')


class GenerateRST(Task):

    force = BooleanParameter(default=False)
    format = Parameter()
    preview = BooleanParameter(default=False)

    def __init__(self, *args, **kwargs):
        super(GenerateRST, self).__init__(*args, **kwargs)
        if self.force:
            shell('rm -rf catalog/source/*/*')
        shell('cp -R catalog/img/* catalog/source/img/')


    def requires(self):
        session = current_session()
        requirements = {}
        for section_subsection, _ in self.output().iteritems():
            section_id, subsection_id = section_subsection
            subsection = session.query(OBSTag).get(subsection_id)
            if '.. cartofigure:: ' in subsection.description:
                viz_id = re.search(r'\.\. cartofigure:: (\S+)', subsection.description).groups()[0]
                requirements[viz_id] = GenerateStaticImage(viz_id)
            for column in subsection.columns:
                if column.type.lower() == 'numeric' and column.weight > 0:
                    requirements[column.id] = ImagesForMeasure(
                        measure=column.id, force=False)

        return requirements

    def output(self):
        targets = {}
        session = current_session()
        i = 0
        for section in session.query(OBSTag).filter(OBSTag.type == 'section'):
            for subsection in session.query(OBSTag).filter(OBSTag.type == 'subsection'):
                i += 1
                if i > 1 and self.preview:
                    break
                targets[(section.id, subsection.id)] = LocalTarget(
                    'catalog/source/{section}/{subsection}.rst'.format(
                        section=section.id,
                        subsection=subsection.id))
        return targets

    def run(self):
        session = current_session()
        for section_subsection, target in self.output().iteritems():
            section_id, subsection_id = section_subsection
            section = session.query(OBSTag).get(section_id)
            subsection = session.query(OBSTag).get(subsection_id)
            target.makedirs()
            fhandle = target.open('w')

            if '.. cartofigure:: ' in subsection.description:
                viz_id = re.search(r'\.\. cartofigure:: (\S+)', subsection.description).groups()[0]
                viz_path = os.path.join('../', *self.input()[viz_id].path.split(os.path.sep)[2:])
                subsection.description = re.sub(r'\.\. cartofigure:: (\S+)',
                                                '.. figure:: {}'.format(viz_path),
                                                subsection.description)
            columns = []
            for col in subsection.columns:
                if section not in col.tags:
                    continue

                if col.weight < 1:
                    continue

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

            with open('catalog/source/{}.rst'.format(section.id), 'w') as section_fhandle:
                section_fhandle.write(SECTION_TEMPLATE.render(section=section))
            if columns:
                fhandle.write(SUBSECTION_TEMPLATE.render(
                    subsection=subsection, columns=columns, format=self.format).encode('utf8'))
            else:
                fhandle.write('')
            fhandle.close()


class Catalog(Task):

    force = BooleanParameter(default=False)
    format = Parameter(default='html')
    preview = BooleanParameter(default=False)

    def requires(self):
        return GenerateRST(force=self.force, format=self.format, preview=self.preview)

    def complete(self):
        return False

    def run(self):
        shell('cd catalog && make {}'.format(self.format))
