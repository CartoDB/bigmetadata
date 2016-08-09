from luigi import Task, Parameter, LocalTarget

import os

class TestTask(Task):

    time = Parameter()

    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('foobar')

    def output(self):
        return LocalTarget(os.path.join('tmp', 'test-' + self.time + '.txt'))
