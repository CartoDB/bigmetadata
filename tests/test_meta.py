'''
Test metadata functions
'''


from nose.tools import assert_equals, with_setup, assert_raises

from tests.util import setup, teardown, session_scope, EMPTY_RASTER, FakeTask

from tasks.meta import (OBSColumnTable, OBSColumn, OBSTable, OBSColumnTableTile,
                        OBSTag, OBSColumnTag, Base, current_session)
from tasks.util import ColumnTarget, TagTarget, shell


def populate():
    with session_scope() as session:
        population_tag = OBSTag(id='population', name='population', type='catalog')
        source_tag = OBSTag(id='us_census', name='US Census', type='source')
        session.add(population_tag)
        session.add(source_tag)
        datacols = {
            'median_rent': OBSColumn(id='"us.census.acs".median_rent', type='numeric'),
            'total_pop': OBSColumn(id='"us.census.acs".total_pop', type='numeric'),
            'male_pop': OBSColumn(id='"us.census.acs".male_pop', type='numeric'),
            'female_pop': OBSColumn(id='"us.census.acs".female_pop', type='numeric'),
        }
        for numerator_col in ('male_pop', 'female_pop', ):
            datacol = datacols[numerator_col]
            datacol.targets[datacols['total_pop']] = 'denominator'
            session.add(datacol)
        tract_geoid = OBSColumn(id='"us.census.acs".tract_2013_geoid', type='text')
        puma_geoid = OBSColumn(id='"us.census.acs".puma_2013_geoid', type='text')
        tract_geom = OBSColumn(id='"us.census.tiger".tract', type='geometry')
        tables = {
            'tract': OBSTable(id='"us.census.acs".extract_2013_5yr_tract',
                              tablename='us_census_acs2013_5yr_tract'),
            'puma': OBSTable(id='"us.census.acs".extract_2013_5yr_puma',
                             tablename='us_census_acs2013_5yr_puma')
        }
        geom_table = OBSTable(id='"us.census.acs".tract_geoms',
                              tablename='tract_geoms')
        session.add(OBSColumnTable(table=tables['tract'],
                                   column=tract_geoid,
                                   colname='geoid'))
        session.add(OBSColumnTable(table=tables['puma'],
                                   column=puma_geoid,
                                   colname='geoid'))
        session.add(OBSColumnTable(table=geom_table,
                                   column=tract_geoid,
                                   colname='geoid'))
        geom_coltable = OBSColumnTable(table=geom_table,
                                       column=tract_geom,
                                       colname='the_geom')
        session.add(geom_coltable)
        session.add(OBSColumnTableTile(column_id=geom_coltable.column.id,
                                       table_id=geom_coltable.table.id,
                                       tile_id=1,
                                       tile=EMPTY_RASTER
                                      ))
        for colname, datacol in datacols.iteritems():
            if colname.endswith('pop'):
                datacol.tags.append(TagTarget(population_tag, FakeTask()))
                datacol.tags.append(TagTarget(source_tag, FakeTask()))
            for table in tables.values():
                coltable = OBSColumnTable(column=datacol,
                                          table=table,
                                          colname=colname)
                session.add(coltable)
            session.add(datacol)
        for table in tables.values():
            session.add(table)


@with_setup(setup, teardown)
def test_columns_in_tables():
    '''
    Tables can refer to columns.
    '''
    populate()
    with session_scope() as session:
        table = session.query(OBSTable).get('"us.census.acs".extract_2013_5yr_puma')
        assert_equals(5, len(table.columns))


@with_setup(setup, teardown)
def test_tables_in_columns():
    '''
    Columns can refer to tables.
    '''
    populate()
    with session_scope() as session:
        column = session.query(OBSColumn).get('"us.census.acs".median_rent')
        assert_equals(2, len(column.tables))


@with_setup(setup, teardown)
def test_tags_in_columns():
    '''
    Columns can refer to tags.
    '''
    populate()
    with session_scope() as session:
        column = session.query(OBSColumn).get('"us.census.acs".total_pop')
        assert_equals(['US Census', 'population'], sorted([tag.name for tag in column.tags]))


@with_setup(setup, teardown)
def test_columns_in_tags():
    '''
    Tags can refer to columns.
    '''
    populate()
    with session_scope() as session:
        tag = session.query(OBSTag).get('population')
        tag2 = session.query(OBSTag).get('us_census')
        assert_equals(3, len(tag.columns))
        assert_equals(3, len(tag2.columns))
        assert_equals(tag.type, 'catalog')
        assert_equals(tag2.type, 'source')


@with_setup(setup, teardown)
def test_column_to_column_target():
    '''
    Columns can refer to other columns as a target.
    '''
    populate()
    with session_scope() as session:
        column = session.query(OBSColumn).get('"us.census.acs".female_pop')
        assert_equals(0, len(column.sources))
        assert_equals(1, len(column.targets))

        target, reltype = column.targets.items()[0]
        assert_equals(target.id, '"us.census.acs".total_pop')
        assert_equals(reltype, 'denominator')


@with_setup(setup, teardown)
def test_delete_column_deletes_relevant_related_objects():
    populate()
    with session_scope() as session:
        assert_equals(session.query(OBSColumn).count(), 7)
        assert_equals(session.query(OBSTable).count(), 3)
        assert_equals(session.query(OBSColumnTable).count(), 12)
        session.delete(session.query(OBSColumn).get('"us.census.acs".median_rent'))
        assert_equals(session.query(OBSColumn).count(), 6)
        assert_equals(session.query(OBSTable).count(), 3)
        assert_equals(session.query(OBSColumnTable).count(), 10)

@with_setup(setup, teardown)
def test_delete_table_deletes_relevant_related_objects():
    populate()
    with session_scope() as session:
        assert_equals(session.query(OBSColumn).count(), 7)
        assert_equals(session.query(OBSTable).count(), 3)
        assert_equals(session.query(OBSColumnTable).count(), 12)
        session.delete(session.query(OBSTable).get('"us.census.acs".extract_2013_5yr_tract'))
        assert_equals(session.query(OBSColumn).count(), 7)
        assert_equals(session.query(OBSTable).count(), 2)
        assert_equals(session.query(OBSColumnTable).count(), 7)


@with_setup(setup, teardown)
def test_delete_tag_deletes_relevant_related_objects():
    populate()
    with session_scope() as session:
        assert_equals(session.query(OBSColumn).count(), 7)
        assert_equals(session.query(OBSColumnTag).count(), 6)
        assert_equals(session.query(OBSTag).count(), 2)
        session.delete(session.query(OBSTag).get('population'))
        assert_equals(session.query(OBSColumn).count(), 7)
        assert_equals(session.query(OBSColumnTag).count(), 3)
        assert_equals(session.query(OBSTag).count(), 1)


@with_setup(setup, teardown)
def test_delete_columntable_removes_tiles():
    populate()
    with session_scope() as session:
        assert_equals(session.query(OBSColumnTableTile).count(), 1)
        session.delete(session.query(OBSColumnTableTile).get(
            ('"us.census.acs".tract_geoms', '"us.census.tiger".tract', 1, )))
        assert_equals(session.query(OBSColumnTableTile).count(), 0)
