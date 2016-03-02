from nose.tools import assert_equals, with_setup, assert_raises, assert_in
from tasks.util import (slug_column, ColumnTarget, ColumnsTask, TableTask,
                        TableTarget)
from tasks.meta import (session_scope, BMDColumn, Base, BMDColumnTable, BMDTag,
                        BMDColumnTag, BMDTable, metadata)


def setup():
    Base.metadata.drop_all()
    Base.metadata.create_all()


def teardown():
    Base.metadata.drop_all()


def test_slug_column():
    assert_equals(slug_column('Population'), 'pop')
    assert_equals(slug_column('Population 5 Years and Over'), 'pop_5_years_and_over')
    assert_equals(slug_column('Workers 16 Years and Over'), 'workers_16_years_and_over')
    assert_equals(slug_column('Population for Whom Poverty Status Is Determined'),
                  'pop_poverty_status_determined')
    assert_equals(slug_column('Commuters by Car, truck, or van'), 'commuters_by_car_truck_or_van')
    assert_equals(slug_column('Aggregate travel time to work (in minutes)'),
                  'aggregate_travel_time_to_work_in_minutes')
    assert_equals(slug_column('Hispanic or Latino Population'),
                  'hispanic_or_latino_pop')
    assert_equals(slug_column('Median Household Income (In the past 12 Months)'),
                  'median_household_income')


@with_setup(setup, teardown)
def test_column_target_create_update():
    col = ColumnTarget(BMDColumn(id='foobar',
                                 type='Numeric',
                                 name="Total Population",
                                 description='The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.',
                                 aggregate='sum',
                                 weight=10))

    # Does not exist in DB til we update_or_create
    with session_scope() as session:
        assert_equals(session.query(BMDColumn).count(), 0)
        col.update_or_create(session)
        assert_equals(session.query(BMDColumn).count(), 1)

    # Can update_or_create all we want
    with session_scope() as session:
        assert_equals(session.query(BMDColumn).count(), 1)
        col.update_or_create(session)
        assert_equals(session.query(BMDColumn).count(), 1)

    # Can overwrite the existing column
    col = ColumnTarget(BMDColumn(id='foobar',
                                 type='Numeric',
                                 name="foobar",
                                 description='foo-bar-baz',
                                 aggregate='sum',
                                 weight=10))

    with session_scope() as session:
        assert_equals(session.query(BMDColumn).count(), 1)
        col.update_or_create(session)
        assert_equals(col._column.name, 'foobar')
        assert_equals(col._column.description, 'foo-bar-baz')
        assert_equals(session.query(BMDColumn).count(), 1)

    # Should auto-qualify column id
    with session_scope() as session:
        rawcol = session.query(BMDColumn).get('foobar')
        assert_equals(rawcol.name, 'foobar')
        assert_equals(rawcol.description, 'foo-bar-baz')


@with_setup(setup, teardown)
def test_column_target_relations_create_update():
    col = ColumnTarget(BMDColumn(id='foobar',
                                 type='Numeric',
                                 name="Total Population",
                                 description='The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.',
                                 aggregate='sum',
                                 weight=10))

    with session_scope() as session:
        assert_equals(session.query(BMDColumn).count(), 0)
        col.update_or_create(session)
        rawcol = col._column
        tag = BMDTag(id='tag', name='some tag', description='some tag')
        session.add(tag)
        coltag = BMDColumnTag(column=rawcol, tag=tag)
        session.add(coltag)
        table = BMDTable(id='table', tablename='foobar')
        session.add(table)
        coltable = BMDColumnTable(column=rawcol, table=table, colname='col')
        session.add(coltable)

    with session_scope() as session:
        assert_equals(session.query(BMDTag).count(), 1)
        assert_equals(session.query(BMDColumnTag).count(), 1)
        assert_equals(session.query(BMDColumn).count(), 1)
        assert_equals(session.query(BMDColumnTable).count(), 1)
        assert_equals(session.query(BMDTable).count(), 1)

    col._column.name = 'foo bar baz'

    with session_scope() as session:
        col.update_or_create(session)

    with session_scope() as session:
        rawcol = session.query(BMDColumn).get('foobar')
        assert_equals(rawcol.name, 'foo bar baz')
        assert_equals(session.query(BMDTag).count(), 1)
        assert_equals(session.query(BMDColumnTag).count(), 1)
        assert_equals(session.query(BMDColumn).count(), 1)
        assert_equals(session.query(BMDColumnTable).count(), 1)
        assert_equals(session.query(BMDTable).count(), 1)


@with_setup(setup, teardown)
def test_column_target_many_inits():
    col = ColumnTarget(BMDColumn(id='foobar',
                                 type='Numeric',
                                 name="Total Population",
                                 description='The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.',
                                 aggregate='sum',
                                 weight=10))

    with session_scope() as session:
        assert_equals(session.query(BMDColumn).count(), 0)
        col.update_or_create(session)
        assert_equals(session.query(BMDColumn).count(), 1)

    col = ColumnTarget(BMDColumn(id='foobar',
                                 type='Numeric',
                                 name="Total Population",
                                 description='The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.',
                                 aggregate='sum',
                                 weight=10))

    with session_scope() as session:
        assert_equals(session.query(BMDColumn).count(), 1)
        col.update_or_create(session)
        assert_equals(session.query(BMDColumn).count(), 1)


#@with_setup(setup, teardown)
#def test_table_target_many_inits():
#    pop_col = ColumnTarget(BMDColumn(id='population',
#                                     type='Numeric',
#                                     name="Total Population",
#                                     description='The total number of all',
#                                     aggregate='sum',
#                                     weight=10))
#    foo_col = ColumnTarget(BMDColumn(id='foobar',
#                                     type='Numeric',
#                                     name="Foo Bar",
#                                     description='moo boo foo',
#                                     aggregate='median',
#                                     weight=8))
#    with session_scope() as session:
#        pop_col.update_or_create(session)
#        foo_col.update_or_create(session)
#
#    with session_scope() as session:
#        assert_equals(session.query(BMDColumn).count(), 2)
#
#    columns = {
#        'population': pop_col,
#        'foobar': foo_col
#    }
#    table = TableTarget(BMDTable(id='foobar', tablename='foobar'), columns)
#
#    with session_scope() as session:
#        assert_equals(session.query(BMDTable).count(), 0)
#        assert_equals(session.query(BMDColumn).count(), 2)
#        table.update_or_create(session)
#        assert_equals(session.query(BMDColumn).count(), 2)
#        assert_equals(session.query(BMDTable).count(), 1)
#        assert_in('foobar', metadata.tables)
#        sqlalchemy_table = metadata.tables['foobar']
#        assert_equals(len(sqlalchemy_table.columns), 2)
#
#    # new session, old object
#    with session_scope() as session:
#        assert_equals(session.query(BMDTable).count(), 1)
#        table.update_or_create(session)
#        assert_equals(session.query(BMDTable).count(), 1)
#        assert_in('foobar', metadata.tables)
#        sqlalchemy_table = metadata.tables['foobar']
#        assert_equals(len(sqlalchemy_table.columns), 2)
#
#    # new session, new object
#    table = TableTarget(BMDTable(id='foobar', tablename='foobar'), columns)
#    with session_scope() as session:
#        assert_equals(session.query(BMDTable).count(), 1)
#        table.update_or_create(session)
#        assert_equals(session.query(BMDTable).count(), 1)
#        assert_in('foobar', metadata.tables)
#        sqlalchemy_table = metadata.tables['foobar']
#        assert_equals(len(sqlalchemy_table.columns), 2)
#
#
#@with_setup(setup, teardown)
#def test_columns_task_fails_no_columns():
#    class TestColumnsTask(ColumnsTask):
#        pass
#
#    task = TestColumnsTask()
#    with assert_raises(NotImplementedError):
#        task.run()


@with_setup(setup, teardown)
def test_columns_task_creates_columns_only_when_run():

    class TestColumnsTask(ColumnsTask):
        def columns(self):
            return [
                BMDColumn(id='population',
                          type='Numeric',
                          name="Total Population",
                          description='The total number of all',
                          aggregate='sum',
                          weight=10),
                BMDColumn(id='foobar',
                          type='Numeric',
                          name="Foo Bar",
                          description='moo boo foo',
                          aggregate='median',
                          weight=8),
            ]

    task = TestColumnsTask()
    with session_scope() as session:
        assert_equals(session.query(BMDColumn).count(), 0)
    task.run()
    with session_scope() as session:
        assert_equals(session.query(BMDColumn).count(), 2)
        assert_equals(task.output()['population'].get(session).id, '"test_util".population')
        assert_equals(task.output()['foobar'].get(session).id, '"test_util".foobar')
    assert_equals(True, task.complete())

    table = BMDTable(id='table', tablename='tablename')
    with session_scope() as session:
        coltable = BMDColumnTable(column=task.output()['population'].get(session),
                                  table=table, colname='colnamaste')
        session.add(table)
        session.add(coltable)

    with session_scope() as session:
        assert_equals(session.query(BMDColumnTable).count(), 1)


#@with_setup(setup, teardown)
#def test_table_task_creates_columns_when_run():
#
#    class TestColumnsTask(ColumnsTask):
#
#        def columns(self):
#            return [
#                BMDColumn(id='population',
#                          type='Numeric',
#                          name="Total Population",
#                          description='The total number of all',
#                          aggregate='sum',
#                          weight=10),
#                BMDColumn(id='foobar',
#                          type='Numeric',
#                          name="Foo Bar",
#                          description='moo boo foo',
#                          aggregate='median',
#                          weight=8),
#            ]
#
#    class TestTableTask(TableTask):
#
#        def requires(self):
#            return {
#                'meta': TestColumnsTask()
#            }
#
#        def columns(self, session):
#            return {
#                'population': self.input()['meta']['population'].get(session),
#                'foobar': self.input()['meta']['foobar'].get(session)
#            }
#
#        def runsession(self, session):
#            pass
#
#    task = TestTableTask(BMDTable(id='tableid', tablename='table'))
#    assert_equals(False, task.complete())
#    task.run()
#    assert_equals(True, task.complete())
#
#    with session_scope() as session:
#        assert_equals(session.query(BMDColumn).count(), 2)
#        assert_equals(session.query(BMDColumnTable).count(), 2)
#        assert_equals(session.query(BMDTable).count(), 1)
#        assert_in('')
