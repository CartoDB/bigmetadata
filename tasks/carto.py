'''
Tasks to sync data locally to CartoDB
'''

from tasks.meta import session_scope, BMDTable, Base
from tasks.util import TableToCarto

from luigi import Task, WrapperTask, BooleanParameter


class SyncMetadata(Task):

    force = BooleanParameter(default=False)

    def requires(self):
        for tablename, _ in Base.metadata.tables.iteritems():
            yield TableToCarto(table=tablename, outname=tablename, force=self.force)


class SyncData(WrapperTask):

    force = BooleanParameter(default=False)

    def requires(self):
        tables = {}
        with session_scope() as session:
            for table in session.query(BMDTable):
                tables[table.id] = table.tablename

        for table_id, tablename in tables.iteritems():
            yield TableToCarto(table=table_id, outname=tablename, force=self.force)
