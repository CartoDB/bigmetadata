from tests.util import recreate_db


# The below is run at import-stage

recreate_db('test')

def setup():
    '''
    Package-level setup (via nose)
    '''
    pass


def teardown():
    '''
    Package-level teardown (via nose)
    '''
    pass
