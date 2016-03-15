from collections import OrderedDict
from luigi import Parameter
from nose.tools import (assert_equals, with_setup, assert_raises, assert_in,
                        assert_is_none)
from tasks.util import (underscore_slugify, ColumnTarget, ColumnsTask, TableTask,
                        TableTarget)
from tasks.meta import (session_scope, BMDColumn, Base, BMDColumnTable, BMDTag,
                        BMDColumnTag, BMDColumnToColumn, BMDTable, metadata)


def setup():
    Base.metadata.drop_all()
    Base.metadata.create_all()


def teardown():
    Base.metadata.drop_all()


def test_underscore_slugify():
    assert_equals(underscore_slugify('"path.to.schema"."ClassName(param1=100, param2=foobar)"'),
                  'path_to_schema_class_name_param1_100_param2_foobar'
                 )

#def test_slug_column():
#    assert_equals(slug_column('Population'), 'pop')
#    assert_equals(slug_column('Population 5 Years and Over'), 'pop_5_years_and_over')
#    assert_equals(slug_column('Workers 16 Years and Over'), 'workers_16_years_and_over')
#    assert_equals(slug_column('Population for Whom Poverty Status Is Determined'),
#                  'pop_poverty_status_determined')
#    assert_equals(slug_column('Commuters by Car, truck, or van'), 'commuters_by_car_truck_or_van')
#    assert_equals(slug_column('Aggregate travel time to work (in minutes)'),
#                  'aggregate_travel_time_to_work_in_minutes')
#    assert_equals(slug_column('Hispanic or Latino Population'),
#                  'hispanic_or_latino_pop')
#    assert_equals(slug_column('Median Household Income (In the past 12 Months)'),
#                  'median_household_income')


@with_setup(setup, teardown)
def test_column_target_create_update():
    col = ColumnTarget('tests', 'foobar', BMDColumn(
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
    col = ColumnTarget('tests', 'foobar', BMDColumn(
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
        rawcol = session.query(BMDColumn).get('"tests".foobar')
        assert_equals(rawcol.name, 'foobar')
        assert_equals(rawcol.description, 'foo-bar-baz')


@with_setup(setup, teardown)
def test_column_target_relations_create_update():
    col = ColumnTarget("tests", "foobar", BMDColumn(
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
        rawcol = session.query(BMDColumn).get('"tests".foobar')
        assert_equals(rawcol.name, 'foo bar baz')
        assert_equals(session.query(BMDTag).count(), 1)
        assert_equals(session.query(BMDColumnTag).count(), 1)
        assert_equals(session.query(BMDColumn).count(), 1)
        assert_equals(session.query(BMDColumnTable).count(), 1)
        assert_equals(session.query(BMDTable).count(), 1)


@with_setup(setup, teardown)
def test_column_target_many_inits():
    col = ColumnTarget("tests", "foobar", BMDColumn(
        type='Numeric',
        name="Total Population",
        description='The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.',
        aggregate='sum',
        weight=10))

    with session_scope() as session:
        assert_equals(session.query(BMDColumn).count(), 0)
        col.update_or_create(session)
        assert_equals(session.query(BMDColumn).count(), 1)

    col = ColumnTarget("tests", "foobar", BMDColumn(
        type='Numeric',
        name="Total Population",
        description='The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.',
        aggregate='sum',
        weight=10))

    with session_scope() as session:
        assert_equals(session.query(BMDColumn).count(), 1)
        col.update_or_create(session)
        assert_equals(session.query(BMDColumn).count(), 1)


@with_setup(setup, teardown)
def test_table_target_many_inits():
    pop_col = ColumnTarget("tests", "population", BMDColumn(
        type='Numeric',
        name="Total Population",
        description='The total number of all',
        aggregate='sum',
        weight=10))
    foo_col = ColumnTarget("tests", "foo", BMDColumn(
        type='Numeric',
        name="Foo Bar",
        description='moo boo foo',
        aggregate='median',
        weight=8))
    with session_scope() as session:
        pop_col.update_or_create(session)
        foo_col.update_or_create(session)

    with session_scope() as session:
        assert_equals(session.query(BMDColumn).count(), 2)

    columns = {
        'population': pop_col,
        'foobar': foo_col
    }
    table_target = TableTarget('test', 'foobar', BMDTable(), columns)
    table_id = 'test.foobar'

    with session_scope() as session:
        assert_equals(False, table_target.exists())
        assert_equals(session.query(BMDTable).count(), 0)
        assert_equals(session.query(BMDColumn).count(), 2)
        table_target.update_or_create(session)
        assert_equals(session.query(BMDColumn).count(), 2)
        assert_equals(session.query(BMDTable).count(), 1)
        assert_in(table_id, metadata.tables)
        sqlalchemy_table = metadata.tables[table_id]
        assert_equals(len(sqlalchemy_table.columns), 2)

    assert_equals(True, table_target.exists())
    assert_equals(table_target.table.schema, 'test')
    assert_equals(table_target.table.name, 'foobar')

    # new session, old object
    with session_scope() as session:
        assert_equals(True, table_target.exists())
        assert_equals(session.query(BMDTable).count(), 1)
        table_target.update_or_create(session)
        assert_equals(session.query(BMDTable).count(), 1)
        assert_in(table_id, metadata.tables)
        sqlalchemy_table = metadata.tables[table_id]
        assert_equals(len(sqlalchemy_table.columns), 2)
        assert_equals(True, table_target.exists())

    # new session, new object
    table_target = TableTarget('test', 'foobar', BMDTable(), columns)
    with session_scope() as session:
        assert_equals(True, table_target.exists())
        assert_equals(session.query(BMDTable).count(), 1)
        table_target.update_or_create(session)
        assert_equals(session.query(BMDTable).count(), 1)
        assert_in(table_id, metadata.tables)
        sqlalchemy_table = metadata.tables[table_id]
        assert_equals(len(sqlalchemy_table.columns), 2)
        assert_equals(True, table_target.exists())


@with_setup(setup, teardown)
def test_columns_task_fails_no_columns():
    class TestColumnsTask(ColumnsTask):
        pass

    task = TestColumnsTask()
    with assert_raises(NotImplementedError):
        task.run()


@with_setup(setup, teardown)
def test_columns_task_creates_columns_only_when_run():

    task = TestColumnsTask()
    with session_scope() as session:
        assert_equals(session.query(BMDColumn).count(), 0)
    task.run()
    with session_scope() as session:
        assert_equals(session.query(BMDColumn).count(), 2)
        assert_equals(session.query(BMDColumnToColumn).count(), 1)
        assert_equals(task.output()['pop'].get(session).id, '"test_util".population')
        assert_equals(task.output()['foobar'].get(session).id, '"test_util".foobar')
        pop = session.query(BMDColumn).get('"test_util".population')
        foobar = session.query(BMDColumn).get('"test_util".foobar')
        assert_equals(len(pop.source_columns), 1)
        assert_equals(len(foobar.target_columns), 1)
        assert_equals(pop.source_columns[0].source.id, foobar.id)
        assert_equals(foobar.target_columns[0].target.id, pop.id)

    assert_equals(True, task.complete())

    table = BMDTable(id='table', tablename='tablename')
    with session_scope() as session:
        coltable = BMDColumnTable(column=task.output()['pop'].get(session),
                                  table=table, colname='colnamaste')
        session.add(table)
        session.add(coltable)

    with session_scope() as session:
        assert_equals(session.query(BMDColumnTable).count(), 1)


class TestColumnsTask(ColumnsTask):

    def columns(self):
        pop_column = BMDColumn(id='population',
                               type='Numeric',
                               name="Total Population",
                               description='The total number of all',
                               aggregate='sum',
                               weight=10)
        return OrderedDict({
            'pop': pop_column,
            'foobar': BMDColumn(id='foobar',
                                type='Numeric',
                                name="Foo Bar",
                                description='moo boo foo',
                                aggregate='median',
                                weight=8,
                                target_columns=[BMDColumnToColumn(reltype='denominator',
                                                                  target=pop_column)]
                               ),
        })


class TestTableTask(TableTask):

    alpha = Parameter(default=1996)
    beta = Parameter(default=5000)

    def requires(self):
        return {
            'meta': TestColumnsTask()
        }

    def columns(self):
        return {
            'population': self.input()['meta']['pop'],
            'foobar': self.input()['meta']['foobar']
        }

    def bounds(self):
        return None

    def timespan(self):
        return ''

    def runsession(self, session):
        pass


@with_setup(setup, teardown)
def test_table_task_creates_columns_when_run():

    task = TestTableTask()
    assert_equals(False, task.complete())
    for dep in task.deps():
        dep.run()
    task.run()
    assert_equals(True, task.complete())

    with session_scope() as session:
        assert_equals(session.query(BMDColumn).count(), 2)
        assert_equals(session.query(BMDColumnTable).count(), 2)
        assert_equals(session.query(BMDTable).count(), 1)
        assert_in(task.table.fullname, metadata.tables)


@with_setup(setup, teardown)
def test_table_task_table():

    task = TestTableTask()
    for dep in task.deps():
        dep.run()

    task.run()

    with session_scope() as session:
        assert_equals('"{schema}".{name}'.format(schema=task.table.schema,
                                                 name=task.table.name),
                      task.output().get(session).id)


@with_setup(setup, teardown)
def test_table_task_replaces_data():

    task = TestTableTask()
    for dep in task.deps():
        dep.run()
    task.run()

    with session_scope() as session:
        assert_equals(session.query(task.table).count(), 0)
        session.execute('INSERT INTO "{schema}"."{tablename}" VALUES (100, 100)'.format(
            schema=task.table.schema,
            tablename=task.table.name))
        assert_equals(session.query(task.table).count(), 1)

    task.run()

    with session_scope() as session:
        assert_equals(session.query(task.table).count(), 0)

@with_setup(setup, teardown)
def test_table_task_qualifies_table_name_schema():

    task = TestTableTask()
    for dep in task.deps():
        dep.run()
    task.run()

    assert_equals(task.table.schema, 'test_util')
    assert_equals(task.table.name, 'test_table_task_alpha_1996_beta_5000')
