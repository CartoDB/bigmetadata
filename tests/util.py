'''
Util functions for tests
'''


def runtask(task):
    '''
    Run deps of tasks then the task
    '''
    if task.complete():
        return
    for dep in task.deps():
        runtask(dep)
    task.run()
