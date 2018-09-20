from luigi import WrapperTask, Parameter, IntParameter, ListParameter
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


class USHierarchy(WrapperTask):
    year = IntParameter()

    def requires(self):
        levels = self._levels()
        return [USHierarchyInfoUnion(year=self.year, levels=levels),
                USHierarchyChildParentsUnion(year=self.year, levels=levels)]

    def _levels(self):
        level_infos = [(level, info) for level, info in SUMLEVELS.items()]

        sorted_level_infos = sorted(level_infos,
                                    key=lambda level_info: level_info[1][
                                        'weight'])
        return [level_info[0] for level_info in sorted_level_infos]


class _YearLevelsTask:
    year = IntParameter()
    levels = ListParameter(significant=False)


class USHierarchyInfoUnion(TempTableTask, _YearLevelsTask):

    def requires(self):
        return [USLevelInfo(year=self.year, geography=level)
                for level in self.levels]

    def run(self):
        session = current_session()
        session.execute(
            _union(self.input(), self.output().qualified_tablename))
        session.commit()


class USHierarchyChildParentsUnion(TempTableTask, _YearLevelsTask):

    def requires(self):
        child_parents = self._child_parents()
        return [USLevelHierarchy(year=self.year,
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
        session.execute(
            _union(self.input(), self.output().qualified_tablename))
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
        current_info_tablename = (
            self.input()['current_info']).qualified_tablename
        current_geom_table = (self.input()['current_geom']).get(session)
        parent_geom_table = (self.input()['parent_geom']).get(session)

        # ST_Area(ST_Intersection(cgt.the_geom, pgt.the_geom), FALSE) AS weight
        # cit.level AS child_level,
        # pgt.level AS parent_level,
        sql = '''
        CREATE TABLE {output_table} AS
        SELECT
            cit.geoid AS child_id,
            pgt.geoid AS parent_id,
            1 AS weight
        FROM {current_info_table} cit
        INNER JOIN observatory.{current_geom_table} cgt ON cit.geoid = cgt.geoid
        INNER JOIN observatory.{parent_geom_table} pgt
            ON ST_Within(ST_PointOnSurface(cgt.the_geom), pgt.the_geom)
        '''

        session.execute(sql.format(
            output_table=self.output().qualified_tablename,
            current_info_table=current_info_tablename,
            current_geom_table=current_geom_table.tablename,
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
