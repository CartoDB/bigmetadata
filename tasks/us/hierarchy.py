from luigi import Task, WrapperTask, Parameter, IntParameter, ListParameter
from tasks.us.census.tiger import GeoNamesTable, ShorelineClip, SUMLEVELS
from tasks.base_tasks import TempTableTask
from tasks.meta import current_session
from tasks.targets import ConstraintExistsTarget
from lib.logger import get_logger

LOGGER = get_logger(__name__)


def _union_query(tables, output):
    unions = ['SELECT * FROM {table}'.format(table=table.qualified_tablename)
              for table in tables]
    return 'CREATE TABLE {output} AS {unions}'.format(
        output=output,
        unions=' UNION '.join(unions))


class USHierarchy(Task):
    year = IntParameter()

    def requires(self):
        levels = self._levels()
        LOGGER.info('Levels: {}'.format(levels))
        return {
            'info': USHierarchyInfoUnion(year=self.year, levels=levels),
            'rel': USHierarchyChildParentsUnion(year=self.year, levels=levels)
        }

    def _levels(self):
        sorted_level_infos = reversed(sorted(SUMLEVELS.items(),
                                             key=lambda level_info:
                                             level_info[1][
                                                 'weight']))
        return [level_info[0] for level_info in sorted_level_infos]

    def run(self):
        session = current_session()
        input_ = self.input()
        session.execute('ALTER TABLE {rel_table} ADD '
                        'CONSTRAINT ushierarchy_fk_child '
                        'FOREIGN KEY (child_id, child_level) '
                        'REFERENCES {info_table} (geoid, level) '.format(
                            rel_table=input_['rel'].qualified_tablename,
                            info_table=input_['info'].qualified_tablename))
        session.execute('ALTER TABLE {rel_table} ADD '
                        'CONSTRAINT ushierarchy_fk_parent '
                        'FOREIGN KEY (parent_id, parent_level) '
                        'REFERENCES {info_table} (geoid, level) '.format(
                            rel_table=input_['rel'].qualified_tablename,
                            info_table=input_['info'].qualified_tablename))
        session.commit()

    def output(self):
        table = self.input()['info']
        return ConstraintExistsTarget(table.schema, table.tablename,
                                      'ushierarchy_fk_parent')


class _YearLevelsTask:
    year = IntParameter()
    levels = ListParameter(significant=False)


class USHierarchyInfoUnion(TempTableTask, _YearLevelsTask):

    def requires(self):
        return [USLevelInfo(year=self.year, geography=level)
                for level in self.levels]

    def run(self):
        session = current_session()
        tablename = self.output().qualified_tablename
        session.execute(_union_query(self.input(), tablename))
        alter_sql = 'ALTER TABLE {tablename} ADD PRIMARY KEY (geoid, level)'
        session.execute(alter_sql.format(tablename=tablename))
        session.commit()


class USHierarchyChildParentsUnion(TempTableTask, _YearLevelsTask):

    def requires(self):
        child_parents = self._child_parents()
        LOGGER.info('Child-parents: {}'.format(child_parents))
        return {
            'hierarchy': [USLevelInclusionHierarchy(year=self.year,
                                                    current_geography=
                                                    child_parent[0],
                                                    parent_geographies=
                                                    child_parent[1])
                          for child_parent in child_parents],
            'weight': [USLevelHierarchyWeights(year=self.year,
                                               current_geography=child_parent[
                                                   0],
                                               parent_geographies=child_parent[
                                                   1])
                       for child_parent in child_parents]
        }

    def _child_parents(self):
        child_parents = []
        previous = None
        for idx, level in enumerate(self.levels):
            if previous:
                parents = self.levels[idx:]
                if parents:
                    child_parents.append([previous, parents])
            previous = level
        return child_parents

    def run(self):
        session = current_session()
        tablename = self.output().qualified_tablename
        session.execute(_union_query(self.input()['hierarchy'], tablename))
        delete_sql = 'DELETE FROM {tablename} WHERE parent_id IS NULL'
        session.execute(delete_sql.format(tablename=tablename))
        alter_sql = 'ALTER TABLE {tablename} ADD PRIMARY KEY ' \
                    '(child_id, child_level, parent_id, parent_level)'
        session.execute(alter_sql.format(tablename=tablename))
        session.commit()


GEOGRAPHIES_ABBREVIATIONS = {
    'school_district_secondary': 'sds',
    'school_district_unified': 'sdu',
    'school_district_elementary': 'sde'
}


def _abbreviation(geography):
    return GEOGRAPHIES_ABBREVIATIONS.get(geography, geography)


def abbr_tablename(target, geographies, year):
    abbrs = [_abbreviation(geography) for geography in geographies]

    if [x for x in zip(abbrs, geographies) if x[0] != x[1]]:
        splits = target.split('_')
        target = '_'.join([splits[0], '_'.join(abbrs), str(year), splits[-1]])

    return target


class USLevelInclusionHierarchy(WrapperTask):
    year = IntParameter()
    current_geography = Parameter()
    parent_geographies = ListParameter()

    def requires(self):
        return {
            'level': USLevelHierarchy(year=self.year,
                                      current_geography=self.current_geography,
                                      parent_geographies=self.parent_geographies)
        }

    def output(self):
        return self.input()['level']


class USLevelHierarchyWeights(Task):
    year = IntParameter()
    current_geography = Parameter()
    parent_geographies = ListParameter()

    UNWEIGHTED_CHILD_SQL = """
        SELECT DISTINCT child_id, child_level
        FROM {table}
        WHERE weight = 1
        GROUP BY child_id, child_level
        HAVING count(1) > 1
    """

    def requires(self):
        return {
            'level': USLevelHierarchy(year=self.year,
                                      current_geography=self.current_geography,
                                      parent_geographies=self.parent_geographies),
            'current_geom': ShorelineClip(year=self.year,
                                          geography=self.current_geography),
            'parent_geoms': [ShorelineClip(year=self.year,
                                           geography=parent_geography) for
                             parent_geography in self.parent_geographies]
        }

    def run(self):
        session = current_session()
        sql = '''
            UPDATE {table}
            SET weight = ST_Area(
                ST_Intersection(cgt.the_geom, pgt.the_geom), False)
            FROM
                observatory.{current_geom_table} cgt,
                observatory.{parent_geom_table} pgt
            WHERE cgt.geoid = {table}.child_id
              AND pgt.geoid = {table}.parent_id
              AND (child_id, child_level) IN (
              {unweighted_child_sql}
        )
        '''
        table = self.input()['level'].qualified_tablename
        for parent_geom in self.input()['parent_geoms']:
            session.execute(
                sql.format(
                    table=table,
                    current_geom_table=self.input()['current_geom'].get(
                        session).tablename,
                    parent_geom_table=parent_geom.get(
                        session).tablename,
                    unweighted_child_sql=self.UNWEIGHTED_CHILD_SQL.format(
                        table=table)
                )
            )
        session.commit()

    def complete(self):
        sql = self.UNWEIGHTED_CHILD_SQL.format(
            table=self.input()['level'].qualified_tablename)
        try:
            return len(current_session().execute(sql).fetchall()) == 0
        except Exception as e:
            # Table doesn't exist yet
            LOGGER.error("ERROR running {}: {}".format(sql, e))
            return False

    def output(self):
        return self.input()['level']


class USLevelHierarchy(TempTableTask):
    year = IntParameter()
    current_geography = Parameter()
    parent_geographies = ListParameter()

    def requires(self):
        return {
            'current_info': USLevelInfo(year=self.year,
                                        geography=self.current_geography),
            'current_geom': ShorelineClip(year=self.year,
                                          geography=self.current_geography),
            'parents_infos': [USLevelInfo(year=self.year,
                                          geography=parent_geography) for
                              parent_geography in self.parent_geographies],
            'parents_geoms': [ShorelineClip(year=self.year,
                                            geography=parent_geography) for
                              parent_geography in self.parent_geographies]
        }

    def target_tablename(self):
        return abbr_tablename(super(USLevelHierarchy, self).target_tablename(),
                              [self.current_geography, 'parents'],
                              self.year)

    def run(self):
        session = current_session()
        input_ = self.input()
        current_info_tablename = input_['current_info'].qualified_tablename
        current_geom_table = input_['current_geom'].get(session)
        parent_info_tablename = input_['parents_infos'][0].qualified_tablename
        parent_geom_table = input_['parents_geoms'][0].get(session)

        create_table_sql = '''
        CREATE TABLE {output_table} AS
        {child_parent_sql}
        '''

        # First creation will link child with direct parents and leave nulls
        # for those that don't have.
        session.execute(create_table_sql.format(
            output_table=self.output().qualified_tablename,
            child_parent_sql=self._CHILD_PARENT_SQL.format(
                current_info_table=current_info_tablename,
                current_geom_table=current_geom_table.tablename,
                parent_info_table=parent_info_tablename,
                parent_geom_table=parent_geom_table.tablename)
        ))

        inputs = list(zip(input_['parents_infos'], input_['parents_geoms']))
        for parent_info_geom in inputs[1:]:
            # For those without parents, insert the next ones
            parent_info_tablename = parent_info_geom[0].qualified_tablename
            parent_geom_table = parent_info_geom[1].get(session)
            fill_parents_sql = '''
                INSERT INTO {output_table}
                (child_id, child_level, parent_id, parent_level, weight)
                {child_parent_sql}
                INNER JOIN {output_table} ot ON ot.child_id = cit.geoid
                                            AND ot.child_level = cit.level
                WHERE ot.parent_id IS NULL
            '''
            session.execute(fill_parents_sql.format(
                output_table=self.output().qualified_tablename,
                child_parent_sql=self._CHILD_PARENT_SQL.format(
                    current_info_table=current_info_tablename,
                    current_geom_table=current_geom_table.tablename,
                    parent_info_table=parent_info_tablename,
                    parent_geom_table=parent_geom_table.tablename)))

            # ... and then, delete the rows with null parents for those
            # child that have any parent
            delete_non_orphans = '''
                DELETE FROM {output_table} ot
                WHERE ot.parent_id IS NULL
                AND (child_id, child_level) IN (
                    SELECT child_id, child_level
                    FROM {output_table}
                    WHERE parent_id IS NOT NULL
            )
            '''
            session.execute(delete_non_orphans.format(
                output_table=self.output().qualified_tablename))

        session.commit()

    _CHILD_PARENT_SQL = '''
        SELECT
            cit.geoid AS child_id,
            cit.level AS child_level,
            pgt.geoid AS parent_id,
            pit.level AS parent_level,
            1.0::FLOAT AS weight
        FROM {current_info_table} cit
        INNER JOIN observatory.{current_geom_table} cgt ON cit.geoid = cgt.geoid
        LEFT JOIN observatory.{parent_geom_table} pgt
            ON ST_Within(ST_PointOnSurface(cgt.the_geom), pgt.the_geom)
        LEFT JOIN {parent_info_table} pit ON pgt.geoid = pit.geoid
    '''


class _YearGeographyTask:
    year = IntParameter()
    geography = Parameter()


class USLevelInfo(WrapperTask, _YearGeographyTask):
    GEOGRAPHIES_WITHOUT_GEONAMES = ['zcta5']

    def requires(self):
        if self.geography in self.GEOGRAPHIES_WITHOUT_GEONAMES:
            return USLevelInfoFromShorelineClip(year=self.year,
                                                geography=self.geography)
        else:
            return USLevelInfoFromGeoNames(year=self.year,
                                           geography=self.geography)

    def output(self):
        return self.input()


class USLevelInfoFromShorelineClip(TempTableTask, _YearGeographyTask):

    def requires(self):
        return ShorelineClip(year=self.year, geography=self.geography)

    def run(self):
        session = current_session()

        names_table = self.input().get(session).tablename
        output_table = self.output().qualified_tablename
        output_tablename = self.output().tablename

        sql = '''
            CREATE TABLE {output_table} AS
            SELECT n.geoid geoid, '{geography}' as level, n.geoid as name, {year} as year
            FROM observatory.{names_table} n
        '''
        session.execute(
            sql.format(output_table=output_table,
                       geography=self.geography,
                       year=self.year,
                       names_table=names_table))
        session.execute('''
            CREATE INDEX {output_tablename}_idx ON {output_table} (geoid)
        '''.format(output_table=output_table,
                   output_tablename=output_tablename))
        session.commit()


class USLevelInfoFromGeoNames(TempTableTask, _YearGeographyTask):

    def requires(self):
        return GeoNamesTable(year=self.year, geography=self.geography)

    def target_tablename(self):
        return abbr_tablename(
            super(USLevelInfoFromGeoNames, self).target_tablename(),
            [self.geography], self.year)

    def run(self):
        session = current_session()

        names_table = self.input().get(session).tablename
        output_table = self.output().qualified_tablename
        output_tablename = self.output().tablename

        sql = '''
            CREATE TABLE {output_table} AS
            SELECT n.geoidsc geoid, '{geography}' as level, n.geoname, {year} as year
            FROM observatory.{names_table} n
        '''
        session.execute(
            sql.format(output_table=output_table,
                       geography=self.geography,
                       year=self.year,
                       names_table=names_table))
        session.execute('''
            CREATE INDEX {output_tablename}_idx ON {output_table} (geoid)
        '''.format(output_table=output_table,
                   output_tablename=output_tablename))
        session.commit()
