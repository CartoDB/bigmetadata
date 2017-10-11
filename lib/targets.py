import os
import shutil

from luigi import Target, LocalTarget

from tasks.util import classpath


class DirectoryTarget(Target):
    def __init__(self, task):
        self.path = os.path.join('tmp', classpath(task), task.task_id)
        self._target = LocalTarget(self.path)

    def exists(self):
        return self._target.exists()

    def temporary_path(self):
        '''Wraps luigi manager to automatically create and destroy directories.'''
        class _Manager(object):
            _target = self._target

            def __enter__(self):
                self._manager = self._target.temporary_path()
                self._temp_path = self._manager.__enter__()
                os.makedirs(self._temp_path)
                return self._temp_path

            def __exit__(self, exc_type, exc_value, traceback):
                if exc_type is None:
                    # There were no exceptions
                    self._manager.__exit__(exc_type, exc_value, traceback)
                else:
                    # On error, clear the temp directory
                    shutil.rmtree(self._temp_path)

                return False  # False means we don't suppress the exception

        return _Manager()
