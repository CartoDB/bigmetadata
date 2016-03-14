'''
Test sphinx implementation
'''

from nose.tools import (assert_equals, with_setup, assert_raises, assert_in,
                        assert_is_none)
from tasks.meta import session_scope, BMDTag, BMDColumn, Base
from tasks.sphinx import GenerateRST


def setup():
    Base.metadata.drop_all()
    Base.metadata.create_all()


def teardown():
    Base.metadata.drop_all()


def populate(session):
    # tag_denominator = BMDTag()
    # tag_boundary = BMDTag()
    # population_column = BMDColumn()
    # subset_column = BMDColumn()
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
