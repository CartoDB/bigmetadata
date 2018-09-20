from luigi import Task, WrapperTask, Parameter, IntParameter, ListParameter
from tasks.us.census.tiger import GeoNamesTable, ShorelineClip, SUMLEVELS
from tasks.base_tasks import TempTableTask
from tasks.meta import current_session
from lib.logger import get_logger

LOGGER = get_logger(__name__)


def _union(tables, output):
    unions = ['SELECT * FROM {table}'.format(table=table.qualified_tablename)
              for table in tables]
    return 'CREATE TABLE {output} AS {unions}'.format(
        output=output,
        unions=' UNION '.join(unions))


class USHierarchy(Task):
    year = IntParameter()

    def requires(self):
        levels = self._levels()
        return {
            'info': USHierarchyInfoUnion(year=self.year, levels=levels),
            'rel': USHierarchyChildParentsUnion(year=self.year, levels=levels)
        }

    def _levels(self):
        level_infos = [(level, info) for level, info in SUMLEVELS.items()]

        sorted_level_infos = sorted(level_infos,
                                    key=lambda level_info: level_info[1][
                                        'weight'])
        return [level_info[0] for level_info in sorted_level_infos]

    def run(self):
        session = current_session()
        session.execute('ALTER TABLE {rel_table} ADD '
                        'CONSTRAINT ushierarchy_fk_child '
                        'FOREIGN KEY (child_id, child_level) '
                        'REFERENCES {info_table} (geoid, level) '.format(
            rel_table=self.input()['rel'].qualified_tablename,
            info_table=self.input()['info'].qualified_tablename))
        session.execute('ALTER TABLE {rel_table} ADD '
                        'CONSTRAINT ushierarchy_fk_parent '
                        'FOREIGN KEY (parent_id, parent_level) '
                        'REFERENCES {info_table} (geoid, level) '.format(
            rel_table=self.input()['rel'].qualified_tablename,
            info_table=self.input()['info'].qualified_tablename))
        session.commit()

    def complete(self):
        session = current_session()
        sql = "SELECT 1 FROM information_schema.constraint_column_usage " \
              "WHERE table_schema = '{schema}' " \
              "  AND table_name = '{table}' " \
              "  AND constraint_name = '{constraint}'"
        table = self.input()['info']
        check = sql.format(schema=table.schema,
                           table=table.tablename,
                           constraint='ushierarchy_fk_parent')
        result = session.execute(check).fetchall()
        return len(result) > 0


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
        session.execute(_union(self.input(), tablename))
        alter_sql = 'ALTER TABLE {tablename} ADD PRIMARY KEY (geoid, level)'
        session.execute(alter_sql.format(tablename=tablename))
        session.commit()


class USHierarchyChildParentsUnion(TempTableTask, _YearLevelsTask):

    def requires(self):
        child_parents = self._child_parents()
        return [USLevelInclusionHierarchy(year=self.year,
                                          current_geography=child_parent[0],
                                          parent_geography=child_parent[1])
                for child_parent in child_parents]

    def _child_parents(self):
        child_parents = []
        previous = None
        for idx, level in enumerate(self.levels):
            if previous:
                child_parents.append([level, previous])
            previous = level
        return child_parents

    def run(self):
        session = current_session()
        tablename = self.output().qualified_tablename
        session.execute(_union(self.input(), tablename))
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


class USLevelInclusionHierarchy(Task):
    year = IntParameter()
    current_geography = Parameter()
    parent_geography = Parameter()

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
                                      parent_geography=self.parent_geography),
            'current_geom': ShorelineClip(year=self.year,
                                          geography=self.current_geography),
            'parent_geom': ShorelineClip(year=self.year,
                                         geography=self.parent_geography)
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
        session.execute(
            sql.format(
                table=table,
                current_geom_table=self.input()['current_geom'].get(
                    session).tablename,
                parent_geom_table=self.input()['parent_geom'].get(
                    session).tablename,
                unweighted_child_sql=self.UNWEIGHTED_CHILD_SQL.format(
                    table=table)
            )
        )
        session.commit()

    def complete(self):
        return len(current_session().execute(self.UNWEIGHTED_CHILD_SQL.format(
            table=self.input()['level'].qualified_tablename)).fetchall()) == 0

    def output(self):
        return self.input()['level']



class USLevelHierarchy(TempTableTask):
    year = IntParameter()
    current_geography = Parameter()
    parent_geography = Parameter()

    def requires(self):
        return {
            'current_info': USLevelInfo(year=self.year,
                                        geography=self.current_geography),
            'current_geom': ShorelineClip(year=self.year,
                                          geography=self.current_geography),
            'parent_info': USLevelInfo(year=self.year,
                                       geography=self.parent_geography),
            'parent_geom': ShorelineClip(year=self.year,
                                         geography=self.parent_geography)
        }

    def target_tablename(self):
        return abbr_tablename(super(USLevelHierarchy, self).target_tablename(),
                              [self.current_geography, self.parent_geography],
                              self.year)

    def run(self):
        session = current_session()
        input = self.input()
        current_info_tablename = input['current_info'].qualified_tablename
        current_geom_table = input['current_geom'].get(session)
        parent_info_tablename = input['parent_info'].qualified_tablename
        parent_geom_table = input['parent_geom'].get(session)

        sql = '''
        CREATE TABLE {output_table} AS
        SELECT
            cit.geoid AS child_id,
            cit.level AS child_level,
            pgt.geoid AS parent_id,
            pit.level AS parent_level,
            1.0::FLOAT AS weight
        FROM {current_info_table} cit
        INNER JOIN observatory.{current_geom_table} cgt ON cit.geoid = cgt.geoid
        INNER JOIN observatory.{parent_geom_table} pgt
            ON ST_Within(ST_PointOnSurface(cgt.the_geom), pgt.the_geom)
        INNER JOIN {parent_info_table} pit ON pgt.geoid = pit.geoid
        '''

        session.execute(sql.format(
            output_table=self.output().qualified_tablename,
            current_info_table=current_info_tablename,
            current_geom_table=current_geom_table.tablename,
            parent_info_table=parent_info_tablename,
            parent_geom_table=parent_geom_table.tablename
        ))
        session.commit()


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
