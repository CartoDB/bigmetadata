'''
Tasks to sync data locally to CartoDB
'''

from tasks.meta import current_session, OBSTable, Base
from tasks.util import TableToCarto, underscore_slugify, query_cartodb

from luigi import WrapperTask, BooleanParameter, Parameter, Task
from nose.tools import assert_equal


def extract_dict_a_from_b(a, b):
    return dict([(k, b[k]) for k in a.keys() if k in b.keys()])


def metatables():
    for tablename, table in Base.metadata.tables.iteritems():
        if tablename.startswith('obs_'):
            yield tablename, table


class SyncMetadata(WrapperTask):

    force = BooleanParameter(default=True)

    def requires(self):
        for tablename, _ in metatables():
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


class PurgeFunctions(Task):
    '''
    Purge remote functions
    '''
    pass


class PurgeMetadataTags(Task):
    '''
    Purge local metadata tables that no longer have tasks linking to them
    '''
    pass


class PurgeMetadataColumns(Task):
    '''
    Purge local metadata tables that no longer have tasks linking to them
    '''
    pass


class PurgeMetadataTables(Task):
    '''
    Purge local metadata tables that no longer have tasks linking to them
    '''
    pass


class PurgeMetadata(WrapperTask):
    '''
    Purge local metadata that no longer has tasks linking to it
    '''

    def requires(self):
        yield PurgeMetadataTags()
        yield PurgeMetadataColumns()
        yield PurgeMetadataTables()


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
    See if a dataset has been uploaded & is in sync (at the least, has
    the same number of rows & columns as local).
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

    def run(self):
        session = current_session()
        for tablename, table in metatables():
            pkey = [c.name for c in table.primary_key]

            resp = query_cartodb('select * from {tablename}'.format(
                tablename=tablename))
            for remote_row in resp.json()['rows']:
                uid = dict([
                    (k, remote_row[k]) for k in pkey
                ])
                local_vals = [unicode(v) for v in session.query(table).filter_by(**uid).one()]
                local_row = dict(zip([col.name for col in table.columns], local_vals))
                remote_row = dict([(k, unicode(v)) for k, v in remote_row.iteritems()])
                try:
                    assert_equal(local_row, extract_dict_a_from_b(local_row, remote_row))
                except Exception as err:
                    import pdb
                    pdb.set_trace()
                    print err

        self._complete = True

    def complete(self):
        return hasattr(self, '_complete') and self._complete is True
