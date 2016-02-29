'''
Test metadata functions
'''

from tasks.util import shell

# TODO clean this up in a more general init script
try:
    shell('createdb test')
except:
    pass

from nose.tools import assert_equals, with_setup
from tasks.meta import (BMDColumnTable, BMDColumn, BMDColumnToColumn, BMDTable,
                        BMDTag, BMDColumnTag, Base)
from tasks.util import session_scope


#class Tags():
#    denominator = BMDTag(
#        id='denominator',
#        name='Denominator',
#        description='Use these to provide a baseline for comparison between different areas.')
#    population = BMDTag(
#        id='population',
#        name='Population',
#        description='')
#
#
#class Columns():
#    total_pop = BMDColumn(
#        id='B01001001',
#        type='Numeric',
#        name="Total Population",
#        description='The total number of all people living in a given geographic area.  This is a very useful catch-all denominator when calculating rates.',
#        aggregate='sum',
#        weight=10,
#        tags=[BMDColumnTag(tag=Tags.denominator),
#              BMDColumnTag(tag=Tags.population)]
#    )
#    male_pop = BMDColumn(
#        id='B01001002',
#        type='Numeric',
#        name="Male Population",
#        description="The number of people within each geography who are male.",
#        aggregate='sum',
#        weight=8,
#        target_columns=[BMDColumnToColumn(target=total_pop, reltype='denominator')],
#        tags=[BMDColumnTag(tag=Tags.population)]
#    )
#    female_pop = BMDColumn(
#        id='B01001026',
#        type='Numeric',
#        name="Female Population",
#        description="The number of people within each geography who are female.",
#        aggregate='sum',
#        weight=8,
#        target_columns=[BMDColumnToColumn(target=total_pop, reltype='denominator')],
#        tags=[BMDColumnTag(tag=Tags.population)]
#    )

def setup():
    Base.metadata.drop_all()
    Base.metadata.create_all()

def populate():
    with session_scope() as session:
        population_tag = BMDTag(id='population', name='population')
        session.add(population_tag)
        datacols = {
            'median_rent': BMDColumn(id='"us.census.acs".median_rent', type='numeric'),
            'total_pop': BMDColumn(id='"us.census.acs".total_pop', type='numeric'),
            'male_pop': BMDColumn(id='"us.census.acs".male_pop', type='numeric'),
            'female_pop': BMDColumn(id='"us.census.acs".female_pop', type='numeric'),
        }
        for numerator_col in ('male_pop', 'female_pop', ):
            session.add(BMDColumnToColumn(source=datacols[numerator_col],
                                          target=datacols['total_pop'],
                                          reltype='denominator'))
        tract_geoid = BMDColumn(id='"us.census.acs".tract_2013_geoid', type='text')
        puma_geoid = BMDColumn(id='"us.census.acs".puma_2013_geoid', type='text')
        tables = {
            'tract': BMDTable(id='"us.census.acs".extract_2013_5yr_tract',
                              tablename='us_census_acs2013_5yr_tract'),
            'puma': BMDTable(id='"us.census.acs".extract_2013_5yr_puma',
                             tablename='us_census_acs2013_5yr_puma')
        }
        session.add(BMDColumnTable(table=tables['tract'],
                                   column=tract_geoid,
                                   colname='geoid'))
        session.add(BMDColumnTable(table=tables['puma'],
                                   column=puma_geoid,
                                   colname='geoid'))
        for colname, datacol in datacols.iteritems():
            if colname.endswith('pop'):
                session.add(BMDColumnTag(tag=population_tag,
                                         column=datacol))
            for table in tables.values():
                coltable = BMDColumnTable(column=datacol,
                                          table=table,
                                          colname=colname)
                session.add(coltable)
            session.add(datacol)
        for table in tables.values():
            session.add(table)


def teardown():
    Base.metadata.drop_all()


@with_setup(setup, teardown)
def test_columns_in_tables():
    '''
    Tables can refer to columns.
    '''
    populate()
    with session_scope() as session:
        table = session.query(BMDTable).get('"us.census.acs".extract_2013_5yr_puma')
        assert_equals(5, len(table.columns))


@with_setup(setup, teardown)
def test_tables_in_columns():
    '''
    Columns can refer to tables.
    '''
    populate()
    with session_scope() as session:
        column = session.query(BMDColumn).get('"us.census.acs".median_rent')
        assert_equals(2, len(column.tables))


@with_setup(setup, teardown)
def test_tags_in_columns():
    '''
    Columns can refer to tags.
    '''
    populate()
    with session_scope() as session:
        column = session.query(BMDColumn).get('"us.census.acs".total_pop')
        assert_equals(['population'], [tag.tag.name for tag in column.tags])


@with_setup(setup, teardown)
def test_columns_in_tags():
    '''
    Tags can refer to columns.
    '''
    populate()
    with session_scope() as session:
        tag = session.query(BMDTag).get('population')
        assert_equals(3, len(tag.columns))


@with_setup(setup, teardown)
def test_column_to_column_target():
    '''
    Columns can refer to other columns as a target.
    '''
    populate()
    with session_scope() as session:
        column = session.query(BMDColumn).get('"us.census.acs".female_pop')
        assert_equals(0, len(column.source_columns))
        assert_equals(1, len(column.target_columns))

        target = column.target_columns[0]
        assert_equals(target.reltype, 'denominator')
        assert_equals(target.target.id, '"us.census.acs".total_pop')
