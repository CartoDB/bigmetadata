'''
Sphinx functions for luigi bigmetadata tasks.
'''

from luigi import WrapperTask, Task
from tasks.util import shell


class JSON2RST(Task):

    #def requires(self):
    #    pass
    def complete(self):
        '''
        Regenerate every time
        '''
        return False

    def run(self):
        pass

    def output(self):
        pass


class Sphinx(WrapperTask):

    def requires(self):
        return JSON2RST()

    def run(self):
        shell('cd catalog && make html')

