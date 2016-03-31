'''
Tasks to sync data locally to CartoDB
'''

from tasks.meta import session_scope, BMDTable, Base
from tasks.util import TableToCarto, underscore_slugify

from luigi import WrapperTask, BooleanParameter, Parameter


class SyncMetadata(WrapperTask):

    force = BooleanParameter(default=True)

    def requires(self):
        for tablename, _ in Base.metadata.tables.iteritems():
            if tablename.startswith('bmd_'):
                yield TableToCarto(table=tablename, outname=tablename, force=self.force)


def should_upload(table):
    '''
    Determine whether a table has any tagged columns.  If so, it should be
    uploaded, otherwise it should be ignored.
    '''
    for coltable in table.columns:
        if coltable.column.tags:
            return True
    return False


class SyncData(WrapperTask):
    '''
    Upload a single BMD table to cartodb by ID
    '''
    force = BooleanParameter(default=True)
    schema = Parameter()
    table = Parameter()

    def requires(self):
        table_id = '"{schema}".{table}'.format(schema=self.schema,
                                               table=underscore_slugify(self.table))
        with session_scope() as session:
            table = session.query(BMDTable).get(table_id)
            tablename = table.tablename
        return TableToCarto(table=table_id, outname=tablename, force=self.force)


class SyncAllData(WrapperTask):

    force = BooleanParameter(default=False)

    def requires(self):
        tables = {}
        with session_scope() as session:
            for table in session.query(BMDTable):
                if should_upload(table):
                    tables[table.id] = table.tablename

        for table_id, tablename in tables.iteritems():
            yield TableToCarto(table=table_id, outname=tablename, force=self.force)
