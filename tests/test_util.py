from collections import OrderedDict
from luigi import Parameter
from nose.tools import (assert_equals, with_setup, assert_raises, assert_in,
                        assert_is_none, assert_true, assert_false,
                        assert_almost_equals)
from tests.util import runtask, session_scope, setup, teardown, FakeTask

from tasks.util import (underscore_slugify, ColumnTarget, ColumnsTask, TableTask,
                        TableTarget, TagTarget, TagsTask, PostgresTarget,
                        generate_tile_summary)
from tasks.meta import (OBSColumn, Base, OBSColumnTable, OBSTag, current_session,
                        OBSTable, OBSColumnTag, OBSColumnToColumn, metadata)



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
    col = ColumnTarget(OBSColumn(
        id='tests.foobar',
        type='Numeric',
        name="Total Population",
        description='The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.',
        aggregate='sum',
        weight=10), FakeTask())

    # Does not exist in DB til we update_or_create
    with session_scope() as session:
        assert_equals(session.query(OBSColumn).count(), 0)
        col.update_or_create()
        assert_equals(session.query(OBSColumn).count(), 1)

    # Can update_or_create all we want
    with session_scope() as session:
        assert_equals(session.query(OBSColumn).count(), 1)
        col.update_or_create()
        assert_equals(session.query(OBSColumn).count(), 1)

    # Can overwrite the existing column
    col = ColumnTarget(OBSColumn(
        id='tests.foobar',
        type='Numeric',
        name="foobar",
        description='foo-bar-baz',
        aggregate='sum',
        weight=10), FakeTask())

    with session_scope() as session:
        assert_equals(session.query(OBSColumn).count(), 1)
        col.update_or_create()
        assert_equals(col._column.name, 'foobar')
        assert_equals(col._column.description, 'foo-bar-baz')
        assert_equals(session.query(OBSColumn).count(), 1)

    # Should auto-qualify column id
    with session_scope() as session:
        rawcol = session.query(OBSColumn).get('tests.foobar')
        assert_equals(rawcol.name, 'foobar')
        assert_equals(rawcol.description, 'foo-bar-baz')


@with_setup(setup, teardown)
def test_column_target_relations_create_update():
    col = ColumnTarget(OBSColumn(
        id='tests.foobar',
        type='Numeric',
        name="Total Population",
        description='The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.',
        aggregate='sum',
        weight=10), FakeTask())

    with session_scope() as session:
        assert_equals(session.query(OBSColumn).count(), 0)
        col.update_or_create()
        rawcol = col._column
        tag = OBSTag(id='tag', name='some tag', description='some tag', type='some type')
        session.add(tag)
        rawcol.tags.append(TagTarget(tag, FakeTask()))
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
        col.update_or_create()

    with session_scope() as session:
        rawcol = session.query(OBSColumn).get('tests.foobar')
        assert_equals(rawcol.name, 'foo bar baz')
        assert_equals(session.query(OBSTag).count(), 1)
        assert_equals(session.query(OBSColumnTag).count(), 1)
        assert_equals(session.query(OBSColumn).count(), 1)
        assert_equals(session.query(OBSColumnTable).count(), 1)
        assert_equals(session.query(OBSTable).count(), 1)


@with_setup(setup, teardown)
def test_column_target_many_inits():
    col = ColumnTarget(OBSColumn(
        id='tests.foobar',
        type='Numeric',
        name="Total Population",
        description='The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.',
        aggregate='sum',
        weight=10), FakeTask())

    with session_scope() as session:
        assert_equals(session.query(OBSColumn).count(), 0)
        col.update_or_create()
        assert_equals(session.query(OBSColumn).count(), 1)

    col = ColumnTarget(OBSColumn(
        id='tests.foobar',
        type='Numeric',
        name="Total Population",
        description='The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.',
        aggregate='sum',
        weight=10), FakeTask())

    with session_scope() as session:
        assert_equals(session.query(OBSColumn).count(), 1)
        col.update_or_create()
        assert_equals(session.query(OBSColumn).count(), 1)


@with_setup(setup, teardown)
def test_table_target_many_inits():
    pop_col = ColumnTarget(OBSColumn(
        id='tests.population',
        type='Numeric',
        name="Total Population",
        description='The total number of all',
        aggregate='sum',
        weight=10), FakeTask())
    foo_col = ColumnTarget(OBSColumn(
        id='tests.foo',
        type='Numeric',
        name="Foo Bar",
        description='moo boo foo',
        aggregate='median',
        weight=8), FakeTask())
    pop_col.update_or_create()
    foo_col.update_or_create()

    assert_equals(current_session().query(OBSColumn).count(), 2)

    columns = {
        'population': pop_col,
        'foobar': foo_col
    }
    table_target = TableTarget('test', 'foobar', OBSTable(), columns, FakeTask())

    assert_equals(False, table_target.exists())
    assert_equals(current_session().query(OBSTable).count(), 0)
    assert_equals(current_session().query(OBSColumn).count(), 2)
    table_target.update_or_create_table()
    table_target.update_or_create_metadata()
    assert_equals(current_session().query(OBSColumn).count(), 2)
    assert_equals(current_session().query(OBSTable).count(), 1)
    obs_table = table_target.get(current_session())
    tablename = 'observatory.' + obs_table.tablename
    assert_in(tablename, metadata.tables)
    sqlalchemy_table = metadata.tables[tablename]
    assert_equals(len(sqlalchemy_table.columns), 2)

    assert_equals(table_target.exists(), False)

    # should 'exist' once rows inserted
    current_session().execute('INSERT INTO {tablename} VALUES (0, 0)'.format(
        tablename=tablename))
    current_session().commit()

    assert_equals(table_target.exists(), True)

    # new session, old object
    assert_equals(True, table_target.exists())
    assert_equals(current_session().query(OBSTable).count(), 1)

    current_session().rollback()
    table_target.update_or_create_table()
    table_target.update_or_create_metadata()
    assert_equals(current_session().query(OBSTable).count(), 1)
    assert_in(tablename, metadata.tables)
    sqlalchemy_table = metadata.tables[tablename]
    assert_equals(len(sqlalchemy_table.columns), 2)

    # forcing update_or_create_table again will end up wiping the table
    assert_equals(False, table_target.exists())


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
        assert_equals(task.output()['pop'].get(session).id, 'test_util.population')
        assert_equals(task.output()['foobar'].get(session).id, 'test_util.foobar')
        pop = session.query(OBSColumn).get('test_util.population')
        foobar = session.query(OBSColumn).get('test_util.foobar')
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


@with_setup(setup, teardown)
def test_columns_task_with_tags_def_one_tag():

    class TestColumnsTaskWithOneTag(TestColumnsTask):

        def requires(self):
            return {
                'tags': TestTagsTask(),
                'section': SectionTagsTask(),
            }

        def tags(self, input_, col_key, col):
            return input_['section']['section']

    task = TestColumnsTaskWithOneTag()
    runtask(task)
    output = task.output()
    for coltarget in output.values():
        assert_in('section', [t.name for t in coltarget._column.tags],
                  'Section tag not added from tags method')


@with_setup(setup, teardown)
def test_columns_task_with_tags_def_two_tags():

    class TestColumnsTaskWithTwoTags(TestColumnsTask):

        def requires(self):
            return {
                'tags': TestTagsTask(),
                'section': SectionTagsTask(),
            }

        def tags(self, input_, col_key, col):
            return [input_['section']['section'],
                    input_['section']['subsection']]

    task = TestColumnsTaskWithTwoTags()
    runtask(task)
    output = task.output()
    for coltarget in output.values():
        assert_in('section', [t.name for t in coltarget._column.tags],
                  'Section tag not added from tags method')
        assert_in('subsection', [t.name for t in coltarget._column.tags],
                  'Subsection tag not added from tags method')


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


class SectionTagsTask(TagsTask):
    def tags(self):
        return [
            OBSTag(id='section',
                   name='section',
                   type='section',
                   description=''),
            OBSTag(id='subsection',
                   name='subsection',
                   type='subsection',
                   description=''),
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


class TestColumnsTaskWithTwoTags(TestColumnsTask):

    def requires(self):
        return {
            'tags': TestTagsTask(),
            'section': SectionTagsTask(),
        }

    def tags(self, input_, col_key, col):
        return [input_['section']['section'], input_['section']['subsection']]


class TestColumnsTaskDependingOnCol(TestColumnsTask):

    def requires(self):
        return {
            'tags': TestTagsTask(),
            'section': SectionTagsTask(),
        }

    def tags(self, input_, col_key, col):
        if col_key == 'foobar':
            return input_['section']['section']


class TestTableTask(TableTask):

    alpha = Parameter(default='1996')
    beta = Parameter(default='5000')

    def requires(self):
        return {
            'meta': TestColumnsTask()
        }

    def columns(self):
        return {
            'population': self.input()['meta']['pop'],
            'foobar': self.input()['meta']['foobar']
        }

    def timespan(self):
        return ''

    def populate(self):
        session = current_session()
        session.execute('INSERT INTO {output} VALUES (100, 100)'.format(
            output=self.output().table))


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
        assert_in(task.output().table, metadata.tables)


@with_setup(setup, teardown)
def test_table_task_replaces_data():

    task = TestTableTask()
    runtask(task)

    table = task.output().table

    with session_scope() as session:
        assert_equals(session.execute(
            'select count(*) from ' + table).fetchone()[0], 1)

    runtask(task)

    with session_scope() as session:
        assert_equals(session.execute(
            'select count(*) from ' + table).fetchone()[0], 1)


@with_setup(setup, teardown)
def test_table_task_increment_version_runs_again():
    task = TestTableTask()
    runtask(task)
    output = task.output()
    assert_true(output.exists())

    task = TestTableTask()
    task.version = lambda: 10
    output = task.output()
    assert_false(output.exists())

    current_session().rollback()
    runtask(task)
    assert_true(output.exists())


@with_setup(setup, teardown)
def test_postgres_target_existencess():
    '''
    PostgresTarget existenceness should be 0 if table DNE, 1 if it exists sans \
    rows, and 2 if it has rows in it.
    '''
    session = current_session()

    target = PostgresTarget('public', 'to_be')
    assert_equals(target._existenceness(), 0)
    assert_equals(target.empty(), False)
    assert_equals(target.exists(), False)

    session.execute('CREATE TABLE to_be (id INT)')
    session.commit()
    assert_equals(target._existenceness(), 1)
    assert_equals(target.empty(), True)
    assert_equals(target.exists(), False)

    session.execute('INSERT INTO to_be VALUES (1)')
    session.commit()
    assert_equals(target._existenceness(), 2)
    assert_equals(target.empty(), False)
    assert_equals(target.exists(), True)


def fake_table_for_rasters(geometries):
    session = current_session()
    table_id = 'foo_table'
    column_id = 'foo_column'
    tablename = 'observatory.foo'
    colname = 'the_geom'
    session.execute('''
        CREATE TABLE {tablename} ({colname} geometry(geometry, 4326))
    '''.format(tablename=tablename, colname=colname))
    session.execute('''
        INSERT INTO {tablename} ({colname}) VALUES {geometries}
    '''.format(tablename=tablename, colname=colname,
               geometries=', '.join(["(ST_SetSRID('{}'::geometry, 4326))".format(g) for g in geometries])
              ))
    session.execute('''
        INSERT INTO observatory.obs_table (id, tablename, version, the_geom)
        SELECT '{table_id}', '{tablename}', 1,
                (SELECT ST_SetSRID(ST_Extent({colname}), 4326) FROM {tablename})
    '''.format(table_id=table_id, tablename=tablename, colname=colname))
    generate_tile_summary(session, table_id, column_id, tablename, colname)
    session.commit()


@with_setup(setup, teardown)
def test_generate_tile_summary_singlegeom():
    '''
    generate_tile_summary should handle a single geometry properly.
    '''
    fake_table_for_rasters([
        'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'
    ])
    session = current_session()
    assert_almost_equals(session.execute('''
        SELECT (ST_SummaryStatsAgg(tile, 2, false)).sum
        FROM observatory.obs_column_table_tile
    ''').fetchone()[0], 1, 1)
    assert_almost_equals(session.execute('''
        SELECT (ST_SummaryStatsAgg(tile, 1, false)).sum
        FROM observatory.obs_column_table_tile_simple
    ''').fetchone()[0], 1, 1)


@with_setup(setup, teardown)
def test_generate_tile_summary_thousandgeom():
    '''
    generate_tile_summary should handle 1000 geometries properly.
    '''
    fake_table_for_rasters(100 * [
        'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'
    ])
    session = current_session()
    assert_almost_equals(session.execute('''
        SELECT (ST_SummaryStatsAgg(tile, 2, false)).sum
        FROM observatory.obs_column_table_tile
    ''').fetchone()[0], 100, 1)
    assert_almost_equals(session.execute('''
        SELECT (ST_SummaryStatsAgg(tile, 1, false)).sum
        FROM observatory.obs_column_table_tile_simple
    ''').fetchone()[0], 100, 1)
