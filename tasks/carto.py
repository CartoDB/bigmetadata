'''
Tasks to sync data locally to CartoDB
'''

from tasks.meta import current_session, OBSTable, Base
from tasks.util import TableToCarto, underscore_slugify

from luigi import WrapperTask, BooleanParameter, Parameter, Task


class SyncMetadata(WrapperTask):

    force = BooleanParameter(default=True)

    def requires(self):
        for tablename, _ in Base.metadata.tables.iteritems():
            if tablename.startswith('obs_'):
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
    Upload a single OBS table to cartodb by ID
    '''
    force = BooleanParameter(default=True)
    schema = Parameter()
    table = Parameter()

    def requires(self):
        table_id = '"{schema}".{table}'.format(schema=self.schema,
                                               table=underscore_slugify(self.table))
        session = current_session()
        table = session.query(OBSTable).get(table_id)
        tablename = table.tablename
        return TableToCarto(table=table_id, outname=tablename, force=self.force)


class SyncAllData(WrapperTask):

    force = BooleanParameter(default=False)

    def requires(self):
        tables = {}
        session = current_session()
        for table in session.query(OBSTable):
            if should_upload(table):
                tables[table.id] = table.tablename

        for table_id, tablename in tables.iteritems():
            yield TableToCarto(table=table_id, outname=tablename, force=self.force)


class PurgeMetadata(Task):
    '''
    Purge local metadata that no longer has tasks linking to it
    '''
    pass


class PurgeData(Task):
    '''
    Purge local data that no longer has tasks linking to it.
    '''
    pass


class PurgeRemoteData(Task):
    '''
    Purge remote data that is no longer available locally
    '''
    pass


class TestData(Task):
    '''
    See if a dataset has been uploaded & is in sync
    '''
    pass


class TestAllData(Task):
    '''
    See if all datasets have been uploaded & are in sync
    '''

    pass


class TestMetadata(Task):
    '''
    Make sure all metadata is uploaded & in sync
    '''
    pass

