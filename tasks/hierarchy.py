from luigi import Task, Parameter, IntParameter, ListParameter
from tasks.base_tasks import TempTableTask
from tasks.meta import current_session
from tasks.targets import ConstraintExistsTarget
from tasks.targets import PostgresTarget, PostgresFunctionTarget
from lib.logger import get_logger

LOGGER = get_logger(__name__)


class _CountryTask:
    @property
    def _country(self):
        raise NotImplementedError('_CountryTask must define _country()')


class DenormalizedHierarchy(Task, _CountryTask):
    year = IntParameter()

    def requires(self):
        '''
        Subclasses must override this and return a dictionary with the following elements:
            - 'function': The definition of the `GetParentsFunction`
            - 'rel': The `HierarchyChildParentsUnion` defining the hierarchy relations
        '''
        raise NotImplementedError('DenormalizedHierarchy must define requires()')

    def _create_indexes(self, session):
        LOGGER.info('Creating index on {table}'.format(table=self.output().table))
        query = '''
                CREATE INDEX idx_{tablename} ON {table} (geoid, level);
                '''.format(
                    tablename=self.output().tablename,
                    table=self.output().table,
                )
        session.execute(query)

        LOGGER.info('Clustering table {table}'.format(table=self.output().table))
        query = '''
                CLUSTER {table} USING idx_{tablename};
                '''.format(
                    tablename=self.output().tablename,
                    table=self.output().table,
                )
        session.execute(query)

    def run(self):
        session = current_session()

        LOGGER.info('Creating table {table}'.format(table=self.output().table))
        query = '''
                CREATE TABLE {output} AS
                SELECT child_id as geoid, child_level as level, parent_names.*
                FROM {input} i,
                    {get_parents_function}(child_id, child_level) parent_names
                '''.format(
                    output=self.output().table,
                    input=self.input()['rel'].table,
                    get_parents_function=self.input()['function'].function,
                )
        session.execute(query)

        self._create_indexes(session)
        session.commit()

    def output(self):
        return PostgresTarget('tiler', '{country}_dz_hierarchy_geonames_{year}'.format(country=self._country,
                                                                                       year=self.year))


class GetParentsFunction(Task, _CountryTask):
    year = IntParameter()

    def requires(self):
        '''
        Subclasses must override this and return a dictionary with the following elements:
            - 'rel': The `HierarchyChildParentsUnion` defining the hierarchy relations
            - 'info': The `HierarchyInfoUnion` defining the geonames
        '''
        raise NotImplementedError('GetParentsFunction must define requires()')

    def run(self):
        input_ = self.input()
        rel_table = input_['rel'].table
        schema = self.output().schema
        function = self.output().function_name

        session = current_session()
        levels_query = '''
                SELECT DISTINCT parent_level
                FROM {input_relations}'''.format(input_relations=rel_table)
        levels = [l[0] for l in session.execute(levels_query).fetchall()]

        level_types = ', '.join(['{} text'.format(l) for l in levels])
        cols_type = """
                "{schema}".{function}_levels
            """.format(schema=schema, function=function)

        session.execute('''
                DROP TYPE IF EXISTS {cols_type} CASCADE
        '''.format(cols_type=cols_type))

        session.execute('''
                CREATE TYPE {cols_type} as ({level_types})
        '''.format(cols_type=cols_type, level_types=level_types))

        query = '''
                CREATE OR REPLACE FUNCTION "{schema}".{function}_json (geoid_p TEXT, level_p TEXT)
                RETURNS JSONB
                AS $$
                DECLARE
                    children JSONB DEFAULT NULL;
                BEGIN
                    WITH RECURSIVE children(child_id, child_level, parent_id, parent_level, geoname) AS (
                        SELECT p.child_id, p.child_level, p.parent_id, p.parent_level, n.geoname
                        FROM {input_relations} p
                        LEFT JOIN {input_geonames} n ON p.parent_id = n.geoid AND p.parent_level = n.level
                        WHERE p.child_id = geoid_p AND p.child_level = level_p
                        UNION ALL
                        SELECT p.child_id, p.child_level, p.parent_id, p.parent_level, n.geoname
                        FROM {input_relations} p
                        INNER JOIN children c ON c.parent_id = p.child_id AND c.parent_level = p.child_level
                        LEFT JOIN {input_geonames} n ON p.parent_id = n.geoid AND p.parent_level = n.level
                    )
                    SELECT ('{{' || string_agg('"' || parent_level || '": "' || geoname || '"', ',') || '}}')::JSONB
                        INTO children
                        FROM children
                    WHERE geoname IS NOT NULL;

                    RETURN children;
                END
                $$ LANGUAGE plpgsql PARALLEL SAFE;
                '''.format(
                    schema=schema,
                    function=function,
                    input_relations=rel_table,
                    input_geonames=input_['info'].table,
                )

        LOGGER.debug(query)
        session.execute(query)

        cols_query = '''
                CREATE OR REPLACE FUNCTION "{schema}".{function}(geoid TEXT, level TEXT)
                RETURNS {cols_type}
                AS $$
                DECLARE
                    _output {cols_type};
                BEGIN
                    SELECT * FROM jsonb_populate_record(null::{cols_type},
                                                        "{schema}".{function}_json(geoid, level))
                    INTO _output;

                    RETURN _output;
                END
                $$ LANGUAGE plpgsql PARALLEL SAFE;
                '''.format(
            schema=schema,
            function=function,
            level_types=level_types,
            cols_type=cols_type)
        LOGGER.debug(cols_query)
        session.execute(cols_query)

        session.commit()

    def output(self):
        name = 'get_{country}_parents_{year}'.format(country=self._country,
                                                     year=self.year)
        return PostgresFunctionTarget(self.input()['rel'].schema, name)


class Hierarchy(Task, _CountryTask):
    year = IntParameter()

    def requires(self):
        '''
        Subclasses must override this and return a dictionary with the following elements:
            - 'info': The `HierarchyInfoUnion` defining the geonames
            - 'rel': The `HierarchyChildParentsUnion` defining the hierarchy relations
        '''
        raise NotImplementedError('Hierarchy must define requires()')

    def run(self):
        session = current_session()
        input_ = self.input()

        session.execute('ALTER TABLE {rel_table} DROP CONSTRAINT IF EXISTS '
                        '{country}hierarchy_fk_parent'.format(
            rel_table=input_['rel'].qualified_tablename,
            country=self._country, ))

        session.execute('ALTER TABLE {rel_table} ADD CONSTRAINT '
                        '{country}hierarchy_fk_parent '
                        'FOREIGN KEY (child_id, child_level) '
                        'REFERENCES {info_table} (geoid, level) '.format(
            rel_table=input_['rel'].qualified_tablename,
            info_table=input_['info'].qualified_tablename,
            country=self._country, ))

        session.execute('ALTER TABLE {rel_table} DROP CONSTRAINT IF EXISTS '
                        '{country}hierarchy_fk_child'.format(
            rel_table=input_['rel'].qualified_tablename,
            country=self._country, ))
        session.execute('ALTER TABLE {rel_table} ADD CONSTRAINT '
                        '{country}hierarchy_fk_child '
                        'FOREIGN KEY (parent_id, parent_level) '
                        'REFERENCES {info_table} (geoid, level) '.format(
            rel_table=input_['rel'].qualified_tablename,
            info_table=input_['info'].qualified_tablename,
            country=self._country, ))
        session.commit()

    def output(self):
        table = self.input()['info']
        return ConstraintExistsTarget(table.schema, table.tablename,
                                      '{country}hierarchy_fk_child'.format(country=self._country))


class _YearCountryLevelsTask(_CountryTask):
    year = IntParameter()
    levels = ListParameter(significant=False)


class HierarchyInfoUnion(TempTableTask, _YearCountryLevelsTask):
    def requires(self):
        '''
        Subclasses must override this and return a list of `LevelInfo` (one for each level)
        '''
        raise NotImplementedError('HierarchyInfoUnion must define requires()')

    def _union_query(self, tables, output):
        unions = ['''SELECT geoid, level, string_agg(geoname, ', ') geoname
                     FROM {table}
                     GROUP BY geoid, level'''.format(table=table.qualified_tablename)
                  for table in tables]
        return 'CREATE TABLE {output} AS {unions}'.format(
            output=output,
            unions=' UNION ALL '.join(unions))

    def run(self):
        session = current_session()
        tablename = self.output().qualified_tablename

        session.execute(self._union_query(self.input(), tablename))
        alter_sql = 'ALTER TABLE {tablename} ADD PRIMARY KEY (geoid, level)'
        session.execute(alter_sql.format(tablename=tablename))
        session.commit()


class HierarchyChildParentsUnion(TempTableTask, _YearCountryLevelsTask):
    def requires(self):
        '''
        Subclasses must override this and return a dictionary with the following elements:
            - 'hierarchy': a list of `HierarchyChildParent` (one for each child-parent relation)
        '''
        raise NotImplementedError('HierarchyChildParentsUnion must define requires()')

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

    def _union_query(self, tables, output):
        unions = ['SELECT * FROM {table}'.format(table=table.qualified_tablename)
                  for table in tables]
        return 'CREATE TABLE {output} AS {unions}'.format(
            output=self.output().qualified_tablename,
            unions=' UNION ALL '.join(unions))

    def run(self):
        session = current_session()

        table = self.output().tablename
        qualified_table = self.output().qualified_tablename

        union_sql = self._union_query(self.input()['hierarchy'], qualified_table)
        session.execute(union_sql)

        delete_sql = 'DELETE FROM {qualified_table} WHERE parent_id IS NULL'
        session.execute(delete_sql.format(qualified_table=qualified_table))

        alter_sql = 'ALTER TABLE {qualified_table} ADD PRIMARY KEY ' \
                    '(child_id, child_level, parent_id, parent_level)'
        session.execute(alter_sql.format(qualified_table=qualified_table))

        parent_index_sql = '''CREATE INDEX {table}_parent_idx
                              ON {qualified_table} (parent_id, parent_level)
            '''.format(table=table, qualified_table=qualified_table)
        session.execute(parent_index_sql)

        session.commit()


class HierarchyChildParent(TempTableTask):
    year = IntParameter()
    current_geography = Parameter()
    parent_geographies = ListParameter()

    UNWEIGHTED_CHILD_SQL = """
        SELECT DISTINCT child_id, child_level
        FROM {table}
        WHERE weight = 1
        AND parent_id IS NOT NULL
        GROUP BY child_id, child_level
        HAVING count(1) > 1
    """

    def requires(self):
        '''
        Subclasses must override this and return a dictionary with the following elements:
            - 'level': a `LevelHierarchy`
            - 'current_geom': the `TableTask` with the current geometries
            - 'parent_geoms': the `TableTask` with the parent geometries
        '''
        raise NotImplementedError('HierarchyChildParent must define requires()')

    @property
    def _current_geoid_field(self):
        return 'geoid'

    @property
    def _parent_geoid_field(self):
        return 'geoid'

    def run(self):
        session = current_session()
        sql = '''
            UPDATE {table}
            SET weight = ST_Area(
                ST_Intersection(cgt.the_geom, pgt.the_geom), False)
            FROM
                observatory.{current_geom_table} cgt,
                observatory.{parent_geom_table} pgt
            WHERE cgt.{current_geoid_field} = {table}.child_id
              AND pgt.{parent_geoid_field} = {table}.parent_id
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
                    current_geoid_field=self._current_geoid_field,
                    parent_geoid_field=self._parent_geoid_field,
                    unweighted_child_sql=self.UNWEIGHTED_CHILD_SQL.format(
                        table=table)
                )
            )

        create_sql = '''
            CREATE TABLE {table} AS
            SELECT DISTINCT ON (child_id, child_level)
                child_id, child_level, parent_id, parent_level, weight
            FROM {weighted_table}
            ORDER BY child_id, child_level, weight desc
        '''
        session.execute(create_sql.format(
            table=self.output().qualified_tablename,
            weighted_table=self.input()['level'].qualified_tablename
        ))
        session.commit()

    def complete(self):
        try:
            sql = self.UNWEIGHTED_CHILD_SQL.format(
                table=self.input()['level'].qualified_tablename)
            return len(current_session().execute(sql).fetchall()) == 0
        except Exception as e:
            # Table doesn't exist yet
            LOGGER.debug("ERROR running complete")
            return False


class LevelHierarchy(TempTableTask):
    year = IntParameter()
    current_geography = Parameter()
    parent_geographies = ListParameter()

    def requires(self):
        '''
        Subclasses must override this and return a dictionary with the following elements:
            - 'current_info': a `LevelInfo` with the current level info
            - 'current_geom': the `TableTask` with the current geometries
            - 'parents_infos': a list of `LevelInfo` with level infos for the parents
            - 'parent_geoms': the list of `TableTask` with the parent geometries
        '''
        raise NotImplementedError('LevelHierarchy must define requires()')

    @property
    def _geoid_field(self):
        return 'geoid'

    def run(self):
        session = current_session()
        input_ = self.input()
        current_info = input_['current_info']
        schema = current_info.schema
        current_info_tablename = current_info.qualified_tablename
        current_geom_table = input_['current_geom'].get(session)
        parent_info_tablename = input_['parents_infos'][0].qualified_tablename
        parent_geom_table = input_['parents_geoms'][0].get(session)

        session.execute('CREATE SCHEMA IF NOT EXISTS "{}"'.format(schema))

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
                parent_geom_table=parent_geom_table.tablename,
                geoid_field=self._geoid_field,
                inner_or_left='LEFT',)
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
                geoid_field=self._geoid_field,
                child_parent_sql=self._CHILD_PARENT_SQL.format(
                    current_info_table=current_info_tablename,
                    current_geom_table=current_geom_table.tablename,
                    parent_info_table=parent_info_tablename,
                    parent_geom_table=parent_geom_table.tablename,
                    geoid_field=self._geoid_field,
                    inner_or_left='INNER')))

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
            pgt.{geoid_field} AS parent_id,
            pit.level AS parent_level,
            1.0::FLOAT AS weight
        FROM {current_info_table} cit
        INNER JOIN observatory.{current_geom_table} cgt ON cit.geoid = cgt.{geoid_field}
        {inner_or_left} JOIN observatory.{parent_geom_table} pgt
            ON ST_Within(ST_PointOnSurface(cgt.the_geom), pgt.the_geom)
        {inner_or_left} JOIN {parent_info_table} pit ON pgt.{geoid_field} = pit.geoid
    '''


class _YearGeographyTask:
    year = IntParameter()
    geography = Parameter()


class LevelInfo(TempTableTask, _YearGeographyTask):
    def requires(self):
        '''
        Subclasses must override this and return a `TableTask` containing
        a geoid field and a geoname field.
        '''
        raise NotImplementedError('LevelInfo must define requires()')

    @property
    def _geoid_field(self):
        raise NotImplementedError('LevelInfo must define geoid_field()')

    @property
    def _geoname_field(self):
        raise NotImplementedError('LevelInfo must define geoname_field()')

    def run(self):
        session = current_session()

        input_table = self.input().get(session)
        names_table = input_table.tablename
        schema = self.output().schema
        output_table = self.output().qualified_tablename
        output_tablename = self.output().tablename

        session.execute('CREATE SCHEMA IF NOT EXISTS "{}"'.format(schema))

        query = '''
                CREATE TABLE {output_table} AS
                SELECT n.{geoid_field} geoid, '{geography}' as level, n.{geoname_field} geoname
                FROM observatory.{names_table} n
                '''.format(output_table=output_table,
                           geoid_field=self._geoid_field,
                           geoname_field=self._geoname_field,
                           geography=self.geography,
                           names_table=names_table)
        session.execute(query)

        query = '''
                CREATE INDEX {output_tablename}_idx ON {output_table} (geoid)
                '''.format(output_table=output_table,
                           output_tablename=output_tablename)
        session.execute(query)

        session.commit()
