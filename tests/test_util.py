from collections import OrderedDict
from luigi import Parameter
from nose.tools import (assert_equals, with_setup, assert_raises, assert_in,
                        assert_is_none)
from tasks.util import (underscore_slugify, ColumnTarget, ColumnsTask, TableTask,
                        TableTarget, TagTarget, TagsTask)
from tasks.meta import (OBSColumn, Base, OBSColumnTable, OBSTag,
                        OBSTable, OBSColumnTag, OBSColumnToColumn, metadata)
from tests.util import runtask, session_scope
from tests.util import setup, teardown



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
    col = ColumnTarget('tests', 'foobar', OBSColumn(
        type='Numeric',
        name="Total Population",
        description='The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.',
        aggregate='sum',
        weight=10))

    # Does not exist in DB til we update_or_create
    with session_scope() as session:
        assert_equals(session.query(OBSColumn).count(), 0)
        col.update_or_create(session)
        assert_equals(session.query(OBSColumn).count(), 1)

    # Can update_or_create all we want
    with session_scope() as session:
        assert_equals(session.query(OBSColumn).count(), 1)
        col.update_or_create(session)
        assert_equals(session.query(OBSColumn).count(), 1)

    # Can overwrite the existing column
    col = ColumnTarget('tests', 'foobar', OBSColumn(
        type='Numeric',
        name="foobar",
        description='foo-bar-baz',
        aggregate='sum',
        weight=10))

    with session_scope() as session:
        assert_equals(session.query(OBSColumn).count(), 1)
        col.update_or_create(session)
        assert_equals(col._column.name, 'foobar')
        assert_equals(col._column.description, 'foo-bar-baz')
        assert_equals(session.query(OBSColumn).count(), 1)

    # Should auto-qualify column id
    with session_scope() as session:
        rawcol = session.query(OBSColumn).get('"tests".foobar')
        assert_equals(rawcol.name, 'foobar')
        assert_equals(rawcol.description, 'foo-bar-baz')


@with_setup(setup, teardown)
def test_column_target_relations_create_update():
    col = ColumnTarget("tests", "foobar", OBSColumn(
        type='Numeric',
        name="Total Population",
        description='The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.',
        aggregate='sum',
        weight=10))

    with session_scope() as session:
        assert_equals(session.query(OBSColumn).count(), 0)
        col.update_or_create(session)
        rawcol = col._column
        tag = OBSTag(id='tag', name='some tag', description='some tag', type='some type')
        session.add(tag)
        rawcol.tags.append(TagTarget(tag))
        session.add(rawcol)
        table = OBSTable(id='table', tablename='foobar')
        session.add(table)
        coltable = OBSColumnTable(column=rawcol, table=table, colname='col')
        session.add(coltable)

    with session_scope() as session:
        assert_equals(session.query(OBSTag).count(), 1)
        assert_equals(session.query(OBSColumnTag).count(), 1)
        assert_equals(session.query(OBSColumn).count(), 1)
        assert_equals(session.query(OBSColumnTable).count(), 1)
        assert_equals(session.query(OBSTable).count(), 1)

    col._column.name = 'foo bar baz'

    with session_scope() as session:
        col.update_or_create(session)

    with session_scope() as session:
        rawcol = session.query(OBSColumn).get('"tests".foobar')
        assert_equals(rawcol.name, 'foo bar baz')
        assert_equals(session.query(OBSTag).count(), 1)
        assert_equals(session.query(OBSColumnTag).count(), 1)
        assert_equals(session.query(OBSColumn).count(), 1)
        assert_equals(session.query(OBSColumnTable).count(), 1)
        assert_equals(session.query(OBSTable).count(), 1)


@with_setup(setup, teardown)
def test_column_target_many_inits():
    col = ColumnTarget("tests", "foobar", OBSColumn(
        type='Numeric',
        name="Total Population",
        description='The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.',
        aggregate='sum',
        weight=10))

    with session_scope() as session:
        assert_equals(session.query(OBSColumn).count(), 0)
        col.update_or_create(session)
        assert_equals(session.query(OBSColumn).count(), 1)

    col = ColumnTarget("tests", "foobar", OBSColumn(
        type='Numeric',
        name="Total Population",
        description='The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.',
        aggregate='sum',
        weight=10))

    with session_scope() as session:
        assert_equals(session.query(OBSColumn).count(), 1)
        col.update_or_create(session)
        assert_equals(session.query(OBSColumn).count(), 1)


@with_setup(setup, teardown)
def test_table_target_many_inits():
    pop_col = ColumnTarget("tests", "population", OBSColumn(
        type='Numeric',
        name="Total Population",
        description='The total number of all',
        aggregate='sum',
        weight=10))
    foo_col = ColumnTarget("tests", "foo", OBSColumn(
        type='Numeric',
        name="Foo Bar",
        description='moo boo foo',
        aggregate='median',
        weight=8))
    with session_scope() as session:
        pop_col.update_or_create(session)
        foo_col.update_or_create(session)

    with session_scope() as session:
        assert_equals(session.query(OBSColumn).count(), 2)

    columns = {
        'population': pop_col,
        'foobar': foo_col
    }
    table_target = TableTarget('test', 'foobar', OBSTable(), columns)
    table_id = 'test.foobar'

    with session_scope() as session:
        assert_equals(False, table_target.exists())
        assert_equals(session.query(OBSTable).count(), 0)
        assert_equals(session.query(OBSColumn).count(), 2)
        table_target.update_or_create(session)
        assert_equals(session.query(OBSColumn).count(), 2)
        assert_equals(session.query(OBSTable).count(), 1)
        assert_in(table_id, metadata.tables)
        sqlalchemy_table = metadata.tables[table_id]
        assert_equals(len(sqlalchemy_table.columns), 2)

    table_target.exists()
    assert_equals(True, table_target.exists())
    assert_equals(table_target.table.schema, 'test')
    assert_equals(table_target.table.name, 'foobar')

    # new session, old object
    with session_scope() as session:
        assert_equals(True, table_target.exists())
        assert_equals(session.query(OBSTable).count(), 1)
        table_target.update_or_create(session)
        assert_equals(session.query(OBSTable).count(), 1)
        assert_in(table_id, metadata.tables)
        sqlalchemy_table = metadata.tables[table_id]
        assert_equals(len(sqlalchemy_table.columns), 2)
        assert_equals(True, table_target.exists())

    # new session, new object
    table_target = TableTarget('test', 'foobar', OBSTable(), columns)
    with session_scope() as session:
        assert_equals(True, table_target.exists())
        assert_equals(session.query(OBSTable).count(), 1)
        table_target.update_or_create(session)
        assert_equals(session.query(OBSTable).count(), 1)
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
        runtask(task)


@with_setup(setup, teardown)
def test_columns_task_creates_columns_only_when_run():

    task = TestColumnsTask()
    with session_scope() as session:
        assert_equals(session.query(OBSColumn).count(), 0)
    runtask(task)
    with session_scope() as session:
        assert_equals(session.query(OBSColumn).count(), 2)
        assert_equals(session.query(OBSColumnToColumn).count(), 1)
        assert_equals(task.output()['pop'].get(session).id, '"test_util".population')
        assert_equals(task.output()['foobar'].get(session).id, '"test_util".foobar')
        pop = session.query(OBSColumn).get('"test_util".population')
        foobar = session.query(OBSColumn).get('"test_util".foobar')
        assert_equals(len(pop.sources), 1)
        assert_equals(len(foobar.targets), 1)
        assert_equals(pop.sources.keys()[0].id, foobar.id)
        assert_equals(foobar.targets.keys()[0].id, pop.id)

    assert_equals(True, task.complete())

    table = OBSTable(id='table', tablename='tablename')
    with session_scope() as session:
        coltable = OBSColumnTable(column=task.output()['pop'].get(session),
                                  table=table, colname='colnamaste')
        session.add(table)
        session.add(coltable)

    with session_scope() as session:
        assert_equals(session.query(OBSColumnTable).count(), 1)



class TestTagsTask(TagsTask):
    def tags(self):
        return [
            OBSTag(id='denominator',
                   name='Denominator',
                   type='catalog',
                   description='Use these to provide a baseline for comparison between different areas.'),
            OBSTag(id='population',
                   name='Population',
                   type='catalog',
                   description='')
        ]


class TestColumnsTask(ColumnsTask):

    def requires(self):
        return {
            'tags': TestTagsTask()
        }

    def columns(self):
        tags = self.input()['tags']
        pop_column = OBSColumn(id='population',
                               type='Numeric',
                               name="Total Population",
                               description='The total number of all',
                               aggregate='sum',
                               tags=[
                                   tags['denominator'],
                                   tags['population']
                               ],
                               weight=10)
        return OrderedDict({
            'pop': pop_column,
            'foobar': OBSColumn(id='foobar',
                                type='Numeric',
                                name="Foo Bar",
                                description='moo boo foo',
                                aggregate='median',
                                weight=8,
                                tags=[tags['population']],
                                targets={
                                    pop_column: 'denominator'
                                }
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

    def populate(self):
        pass


@with_setup(setup, teardown)
def test_table_task_creates_columns_when_run():

    task = TestTableTask()
    assert_equals(False, task.complete())
    runtask(task)
    assert_equals(True, task.complete())

    with session_scope() as session:
        assert_equals(session.query(OBSColumn).count(), 2)
        assert_equals(session.query(OBSColumnTable).count(), 2)
        assert_equals(session.query(OBSTable).count(), 1)
        assert_in(task.table.fullname, metadata.tables)


@with_setup(setup, teardown)
def test_table_task_table():

    task = TestTableTask()
    runtask(task)

    with session_scope() as session:
        assert_equals('"{schema}".{name}'.format(schema=task.table.schema,
                                                 name=task.table.name),
                      task.output().get(session).id)


@with_setup(setup, teardown)
def test_table_task_replaces_data():

    task = TestTableTask()
    runtask(task)

    with session_scope() as session:
        assert_equals(session.query(task.table).count(), 0)
        session.execute('INSERT INTO "{schema}"."{tablename}" VALUES (100, 100)'.format(
            schema=task.table.schema,
            tablename=task.table.name))
        assert_equals(session.query(task.table).count(), 1)

    runtask(task)

    with session_scope() as session:
        assert_equals(session.query(task.table).count(), 1)

@with_setup(setup, teardown)
def test_table_task_qualifies_table_name_schema():

    task = TestTableTask()
    runtask(task)

    assert_equals(task.table.schema, 'test_util')
    assert_equals(task.table.name, 'test_table_task_alpha_1996_beta_5000')
