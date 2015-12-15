import unittest
import nose

from tasks.us.census.acs import ACSColumn


def test_acs_column():
    col = ACSColumn(column_id='foo',
                    column_title='foo',
                    column_parent_path='foo',
                    parent_column_id='foo',
                    table_title='foo',
                    universe='foo',
                    denominator='foo',
                    tags='foo',
                    moe='foo')
    self.assertEquals(col.name, 'foobar')
