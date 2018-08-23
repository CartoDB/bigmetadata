from collections import OrderedDict, defaultdict

from luigi import WrapperTask, Parameter

from tasks.meta import current_session, GEOM_REF
from lib.timespan import get_timespan
from tasks.base_tasks import MetaWrapper, TableTask
from tasks.uk.cdrc import OutputAreas, OutputAreaColumns
from tasks.uk.census.metadata import CensusColumns
from tasks.uk.datashare import PostcodeAreas, PostcodeAreasColumns
from tasks.uk.odl import PostcodeDistricts, PostcodeDistrictsColumns, PostcodeSectors, PostcodeSectorsColumns
from tasks.uk.gov import (LowerLayerSuperOutputAreas, LowerLayerSuperOutputAreasColumns,
                          MiddleLayerSuperOutputAreas, MiddleLayerSuperOutputAreasColumns)

from lib.logger import get_logger

from .ons import ImportUKOutputAreas, ImportEnglandWalesLocal
from .scotland import ImportScotland
from .metadata import parse_table, COLUMNS_DEFINITION

LOGGER = get_logger(__name__)


class CensusOutputAreasTableTask(WrapperTask):
    # Which task to use to import an specific table
    REGION_MAPPING = {
        "UK": ImportUKOutputAreas,
        "EW": ImportEnglandWalesLocal,
        "SC": ImportScotland
    }

    table = Parameter()

    def requires(self):
        _, region = parse_table(self.table)
        return self.REGION_MAPPING[region](table=self.table)

    def id_to_column(self, column_id):
        return self.requires().id_to_column(column_id)

    def output_table(self):
        return self.requires().output().table


class CensusOutputAreas(TableTask):
    def requires(self):
        deps = {
            'geom_columns': OutputAreaColumns(),
            'data_columns': CensusColumns(),
            'geo': OutputAreas(),
        }
        for t in self.source_tables():
            deps[t] = CensusOutputAreasTableTask(table=t)

        return deps

    def targets(self):
        return {
            self.input()['geo'].obs_table: GEOM_REF,
        }

    def table_timespan(self):
        return get_timespan('2011')

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()
        cols['GeographyCode'] = input_['geom_columns']['oa_sa']
        cols.update(input_['data_columns'])
        return cols

    def source_tables(self):
        tables = set()
        for col in COLUMNS_DEFINITION.values():
            tables.update([d['table'] for d in col['data']])
        return tables

    def populate(self):
        '''
        Joins data from all UK sub-census dependencies.

        For each column, there are multiple possiblities:
          - The column has a single datasource and field: ``"data" : [{"table": "t", "fields": ["f"]}]``.
            The table will be added to the FROM, and the column to the SELECT
            ``SELECT t.f AS out_name FROM ... FULL JOIN t USING (geographycode)``

          - The column has multiple fields: ``"data" : [{"table": "t", "fields": ["f1", "f2"]}]``.
            The table will be added to the FROM, and an expression adding all columns to the SELECT
            ``SELECT t.f1 + t.f2 AS out_name FROM ... FULL JOIN t USING (geographycode)``

          - The column has multiple tables: ``"data" : [{"table": "t1", "fields": ["f"]}, {"table": "t2", "fields": ["f"]}]``.
            The tables will be added to a CTE that UNIONS them, the CTE to the FROM, and the column to the SELECT
            ``WITH t1t2 AS (SELECT f AS out_name FROM t1 UNION SELECT f as out_name FROM t2)
              SELECT t1t2.out_name FROM ... FULL JOIN t1t2 USING (geographycode)``

          - The combination of both, which works as above, putting the summing expression inside the CTE
        '''
        def table_column_expression(data):
            tabletask = self.requires()[data['table']]
            table = tabletask.output_table()
            column_names = [tabletask.id_to_column(f) for f in data['fields']]
            col_expression = '+'.join(['{table}.{column}'.format(table=table, column=cn) for cn in column_names])

            return table, col_expression

        in_colnames = []  # Column names in source tables
        out_colnames = []  # Column names in destination table
        from_tables = set()  # Set of all source tables / CTEs (FROM generation)
        ctes = defaultdict(lambda: defaultdict(list))  # CTEs to be generated. {t1t2: { t1: [c1, c2], t2: [d1, d2]}}

        # Generate SQL parts for each column
        for k, v in COLUMNS_DEFINITION.items():
            data = v['data']
            if len(data) == 1:
                # Single table source
                table, col_expression = table_column_expression(data[0])

                from_tables.add(table)
                in_colnames.append(col_expression)
                out_colnames.append(k)
            else:
                # Multi data source
                cte_name = '_'.join([d['table'] for d in data]).lower()
                cte = ctes[cte_name]
                for d in data:
                    table, col_expression = table_column_expression(d)
                    cte[table].append('{expression} AS {id}'.format(expression=col_expression, id=k))

                in_colnames.append('{cte_name}.{id}'.format(cte_name=cte_name, id=k))
                out_colnames.append(k)

        # Generate SQL for CTEs
        ctes_sql = []
        for name, cte in ctes.items():
            selects = []
            for table, columns in cte.items():
                selects.append('SELECT geographycode, {cols} FROM {table}'.format(cols=', '.join(columns), table=table))
            ctes_sql.append('{name} AS ({union})'.format(name=name, union=' UNION '.join(selects)))
            from_tables.add(name)

        # Generate FROM clause. Uses FULL JOIN because not all tables are complete.
        tables = list(from_tables)
        from_part = tables[0]
        for t in tables[1:]:
            from_part += ' FULL JOIN {} USING (geographycode)'.format(t)

        stmt = 'WITH {ctes} INSERT INTO {output} (geographycode, {out_colnames}) ' \
               'SELECT geographycode, {in_colnames} ' \
               'FROM {from_part} ' \
               'WHERE geographycode LIKE \'_00%\''.format(
                   ctes=', '.join(ctes_sql),
                   output=self.output().table,
                   out_colnames=', '.join(out_colnames),
                   in_colnames=', '.join(in_colnames),
                   from_part=from_part)

        current_session().execute(stmt)


class CensusPostcodeAreas(TableTask):
    def requires(self):
        deps = {
            'geom_oa_columns': OutputAreaColumns(),
            'geom_pa_columns': PostcodeAreasColumns(),
            'data_columns': CensusColumns(),
            'geo_oa': OutputAreas(),
            'geo_pa': PostcodeAreas(),
            'census': CensusOutputAreas(),
        }

        return deps

    def targets(self):
        return {
            self.input()['geo_pa'].obs_table: GEOM_REF,
        }

    def table_timespan(self):
        return get_timespan('2011')

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()
        cols['GeographyCode'] = input_['geom_pa_columns']['pa_id']
        cols.update(input_['data_columns'])
        return cols

    def populate(self):
        input_ = self.input()
        colnames = [x for x in list(self.columns().keys()) if x != 'GeographyCode']

        stmt = '''
                INSERT INTO {output} (geographycode, {out_colnames})
                SELECT pa_id, {sum_colnames}
                  FROM (
                    SELECT CASE WHEN ST_Within(geo_pa.the_geom, geo_oa.the_geom)
                                    THEN ST_Area(geo_pa.the_geom) / Nullif(ST_Area(geo_oa.the_geom), 0)
                                WHEN ST_Within(geo_oa.the_geom, geo_pa.the_geom)
                                    THEN 1
                                ELSE ST_Area(ST_Intersection(geo_oa.the_geom, geo_pa.the_geom)) / Nullif(ST_Area(geo_oa.the_geom), 0)
                           END area_ratio,
                           pa_id, {in_colnames}
                      FROM {census_table} census,
                           {geo_oa_table} geo_oa,
                           {geo_pa_table} geo_pa
                     WHERE census.geographycode = geo_oa.oa_sa
                       AND ST_Intersects(geo_oa.the_geom, geo_pa.the_geom) = True
                    ) q GROUP BY pa_id
               '''.format(
                    output=self.output().table,
                    sum_colnames=', '.join(['round(sum({x} / area_ratio)) {x}'.format(x=x) for x in colnames]),
                    out_colnames=', '.join(colnames),
                    in_colnames=', '.join(colnames),
                    census_table=input_['census'].table,
                    geo_oa_table=input_['geo_oa'].table,
                    geo_pa_table=input_['geo_pa'].table,
                )

        current_session().execute(stmt)


class CensusPostcodeEntitiesFromOAs(TableTask):
    def requires(self):
        '''
        This method must be overriden in subclasses.
        '''
        raise NotImplementedError('The requires method must be overriden in subclasses')

    def targets(self):
        return {
            self.input()['target_geom'].obs_table: GEOM_REF,
        }

    def table_timespan(self):
        return get_timespan('2011')

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()
        cols['GeographyCode'] = input_['target_geom_columns']['geographycode']
        cols.update(input_['target_data_columns'])
        return cols

    def populate(self):
        input_ = self.input()
        colnames = [x for x in list(self.columns().keys()) if x != 'GeographyCode']

        stmt = '''
                INSERT INTO {output} (geographycode, {out_colnames})
                SELECT geographycode, {sum_colnames}
                  FROM (
                    SELECT CASE WHEN ST_Within(geo_pe.the_geom, geo_oa.the_geom)
                                    THEN ST_Area(geo_pe.the_geom) / Nullif(ST_Area(geo_oa.the_geom), 0)
                                WHEN ST_Within(geo_oa.the_geom, geo_pe.the_geom)
                                    THEN 1
                                ELSE ST_Area(ST_Intersection(geo_oa.the_geom, geo_pe.the_geom)) / Nullif(ST_Area(geo_oa.the_geom), 0)
                           END area_ratio,
                           geo_pe.geographycode, {in_colnames}
                      FROM {census_table} census,
                           {geo_oa_table} geo_oa,
                           {geo_pe_table} geo_pe
                     WHERE census.geographycode = geo_oa.oa_sa
                       AND ST_Intersects(geo_oa.the_geom, geo_pe.the_geom) = True
                    ) q GROUP BY geographycode
               '''.format(
                    output=self.output().table,
                    sum_colnames=', '.join(['round(sum({x} / area_ratio)) {x}'.format(x=x) for x in colnames]),
                    out_colnames=', '.join(colnames),
                    in_colnames=', '.join(colnames),
                    census_table=input_['source_data'].table,
                    geo_oa_table=input_['source_geom'].table,
                    geo_pe_table=input_['target_geom'].table,
                    geo_id='pa_id'
                )

        current_session().execute(stmt)


class CensusPostcodeDistricts(CensusPostcodeEntitiesFromOAs):
    def requires(self):
        deps = {
            'source_geom_columns': OutputAreaColumns(),
            'source_geom': OutputAreas(),
            'source_data_columns': CensusColumns(),
            'source_data': CensusOutputAreas(),
            'target_geom_columns': PostcodeDistrictsColumns(),
            'target_geom': PostcodeDistricts(),
            'target_data_columns': CensusColumns(),
        }

        return deps


class CensusPostcodeSectors(CensusPostcodeEntitiesFromOAs):
    def requires(self):
        deps = {
            'source_geom_columns': OutputAreaColumns(),
            'source_geom': OutputAreas(),
            'source_data_columns': CensusColumns(),
            'source_data': CensusOutputAreas(),
            'target_geom_columns': PostcodeSectorsColumns(),
            'target_geom': PostcodeSectors(),
            'target_data_columns': CensusColumns(),
        }

        return deps


class CensusSOAsFromOAs(TableTask):
    '''
    As the SOAs and OAs layers are coupled and SOAs are bigger than OAs,
    calculating the measurements for the SOAs is a matter of adding up
    the values, so the data for the Super Output Areas is currently extracted
    from the Output Areas.
    '''

    def requires(self):
        '''
        This method must be overriden in subclasses.
        '''
        raise NotImplementedError('The requires method must be overriden in subclasses')

    def targets(self):
        return {
            self.input()['target_geom'].obs_table: GEOM_REF,
        }

    def table_timespan(self):
        return get_timespan('2011')

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()
        cols['GeographyCode'] = input_['target_geom_columns']['geographycode']
        cols.update(input_['target_data_columns'])
        return cols

    def populate(self):
        input_ = self.input()
        colnames = [x for x in list(self.columns().keys()) if x != 'GeographyCode']

        stmt = '''
                INSERT INTO {output} (geographycode, {out_colnames})
                SELECT geo_target.geographycode, {sum_colnames}
                  FROM {data_source} data_source,
                       {geo_source} geo_source,
                       {geo_target} geo_target
                 WHERE geo_source.oa_sa = data_source.geographycode
                   AND ST_Intersects(geo_target.the_geom, ST_PointOnSurface(geo_source.the_geom))
                 GROUP BY geo_target.geographycode
               '''.format(
                    output=self.output().table,
                    out_colnames=', '.join(colnames),
                    sum_colnames=', '.join(['sum({x}) {x}'.format(x=x) for x in colnames]),
                    data_source=input_['source_data'].table,
                    geo_source=input_['source_geom'].table,
                    geo_target=input_['target_geom'].table,
                )

        current_session().execute(stmt)


class CensusLowerSuperOutputAreas(CensusSOAsFromOAs):
    def requires(self):
        deps = {
            'source_geom_columns': OutputAreaColumns(),
            'source_geom': OutputAreas(),
            'source_data_columns': CensusColumns(),
            'source_data': CensusOutputAreas(),
            'target_geom_columns': LowerLayerSuperOutputAreasColumns(),
            'target_geom': LowerLayerSuperOutputAreas(),
            'target_data_columns': CensusColumns(),
        }

        return deps


class CensusMiddleSuperOutputAreas(CensusSOAsFromOAs):
    def requires(self):
        deps = {
            'source_geom_columns': OutputAreaColumns(),
            'source_geom': OutputAreas(),
            'source_data_columns': CensusColumns(),
            'source_data': CensusOutputAreas(),
            'target_geom_columns': MiddleLayerSuperOutputAreasColumns(),
            'target_geom': MiddleLayerSuperOutputAreas(),
            'target_data_columns': CensusColumns(),
        }

        return deps
