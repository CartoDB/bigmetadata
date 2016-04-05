'''
Test sphinx implementation
'''

from nose.tools import (assert_equals, with_setup, assert_raises, assert_in,
                        assert_is_none)
from tasks.meta import session_scope, OBSTag, OBSColumn, Base, setup, teardown
from tasks.sphinx import GenerateRST


def populate(session):
    # tag_denominator = OBSTag()
    # tag_boundary = OBSTag()
    # population_column = OBSColumn()
    # subset_column = OBSColumn()
    # session.add(tag_denominator)
    # session.add(tag_boundary)
    # session.add(population_column)
    # session.add(subset_column)
    pass


@with_setup(setup, teardown)
def test_columns_for_tag():
    with session_scope() as session:
        populate(session)

    #columns = columns_for_tag()
