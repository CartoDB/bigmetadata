from collections import OrderedDict, defaultdict
import itertools

from tasks.meta import current_session
from tasks.util import MetaWrapper, TableTask
from tasks.uk.cdrc import OutputAreas, OutputAreaColumns
from tasks.uk.census.metadata import CensusColumns

from .ons import ImportUK, ImportEnglandWalesLocal
from .scotland import ImportScotland
from .metadata import parse_table, COLUMNS_DEFINITION


class Census(TableTask):
    REGION_MAPPING = {
        "UK": ImportUK,
        "EW": ImportEnglandWalesLocal,
        "SC": ImportScotland
    }

    def requires(self):
        deps = {
            'geom_columns': OutputAreaColumns(),
            'data_columns': CensusColumns(),
        }
        for t in self.source_tables():
            deps[t] = self.task_for_table(t)
        return deps

    def timespan(self):
        return 2011

    def columns(self):
        cols = OrderedDict()
        input_ = self.input()
        cols['GeographyCode'] = input_['geom_columns']['oa_sa']
        cols.update(input_['data_columns'])
        return cols

    def source_tables(self):
        return set(itertools.chain(*[[d['table'] for d in col['data']] for col in COLUMNS_DEFINITION.values()]))

    def task_for_table(self, table):
        _, region = parse_table(table)
        return self.REGION_MAPPING[region](table=table)

    def populate(self):
        def column_expression(table, fields):
            return '+'.join(['{table}.{column}'.format(table=table, column=cn) for cn in fields])

        tables = [self.requires()[table].output().table for table in self.source_tables()]

        in_colnames = []
        out_colnames = []
        from_tables = set()
        ctes = defaultdict(lambda: defaultdict(list))
        for k, v in COLUMNS_DEFINITION.items():
            data = v['data']
            if len(data) == 1:
                # Single table source
                tabletask = self.requires()[data[0]['table']]
                table = tabletask.output().table
                column_names = [tabletask.id_to_column(f) for f in data[0]['fields']]
                from_tables.add(table)

                in_colnames.append(column_expression(table, column_names))
                out_colnames.append(k)
            else:
                # Multi data source
                cte_name = '_'.join([d['table'] for d in data]).lower()
                cte = ctes[cte_name]
                for d in data:
                    tabletask = self.requires()[d['table']]
                    table = tabletask.output().table
                    column_names = [tabletask.id_to_column(f) for f in d['fields']]

                    cte[table].append(column_expression(table, column_names) + ' AS ' + k)
                in_colnames.append(cte_name + '.' + k)
                out_colnames.append(k)

        ctes_sql = []
        for name, cte in ctes.items():
            selects = []
            for table, columns in cte.items():
                selects.append('SELECT geographycode, ' + ', '.join(columns) + ' FROM ' + table)
            ctes_sql.append('{name} AS ({union})'.format(name=name, union=' UNION '.join(selects)))
            from_tables.add(name)

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
        print(stmt)
        current_session().execute(stmt)


class CensusWrapper(MetaWrapper):
    def tables(self):
        yield Census()
        yield OutputAreas()
