'''
Load data from /columns and /tables into elasticsearch using /mappings
'''

import elasticsearch
from elasticsearch.helpers import bulk
import ujson as json
import os
import sys
from bigmetadata.utils import elastic_conn, get_logger

LOGGER = get_logger(__name__)
ES_NAME = 'bigmetadata'
ES_COLUMN = 'column'
ES_TABLE = 'table'

CACHE = {
    ES_COLUMN: {},
    'CNT': 0
}

def index_column(path):
    '''
    Load json at path into idx.
    '''
    # kill columns/ , tables/ etc.
    column_id = os.path.join(*path.split(os.path.sep)[1:]).replace('.json', '')
    #if econn.exists(ES_NAME, ES_COLUMN, id=column_id):
    #    return
    with open(path) as column_file:
        column = json.load(column_file)
    column_with_id = column.copy()
    column_with_id['id'] = column_id
    column_with_id['tables'] = []
    CACHE[ES_COLUMN][column_id] = column_with_id
    #econn.index(ES_NAME, ES_COLUMN, id=column_id, body=body)
    #return [{
    #    '_op_type': 'index',
    #    '_index': ES_NAME,
    #    '_type': ES_COLUMN,
    #    '_id': column_id,
    #    '_source': column
    #}]


def index_table(path):
    '''
    Add tables to columns.
    '''
    # kill columns/ , tables/ etc.
    ops = []
    table_id = os.path.join(*path.split(os.path.sep)[1:]).replace('.json', '')
    with open(path) as table_file:
        table = json.load(table_file)
    
    # Replace real column with just an ID
    columns = table.pop('columns')
    table_for_column = table.copy()

    table['columns'] = [c['id'] for c in columns]
    table_for_column['id'] = table_id

    #econn.index(ES_NAME, ES_TABLE, id=table_id, body=body)
    for column in columns:
        # Add this table to the column
        tfcc = table_for_column.copy()
        tfcc['resolutions'] = column.get('resolutions', [])
        tfcc['resolutions_nested'] = column.get('resolutions', [])
        CACHE[ES_COLUMN][column['id']]['tables'].append(tfcc)
        
        # Add the columntable
        #ops.append({
        #    '_op_type': 'index',
        #    '_index': ES_NAME,
        #    '_type': ES_COLUMNTABLE,
        #    '_id': table_id + column_id,
        #    '_source': {
        #        "column": CACHE[ES_COLUMN][column_id],
        #        "table": table_with_id,
        #        "resolutions": column.get('resolutions', []),
        #        "resolutions_nested": column.get('resolutions', [])
        #    }
        #})

    # Add the table
    ops.append({
        '_op_type': 'index',
        '_index': ES_NAME,
        '_type': ES_TABLE,
        '_id': table_id,
        '_source': table
    })

    return ops

    #for column in table_body.pop('columns'):

    #    # Create column if it does not exist
    #    if not econn.exists(ES_NAME, ES_COLUMN, id=column['id']):
    #        with open(os.path.join(ES_COLUMN + u's', column['id'] + u'.json')) as column_file:
    #            column_body = json.load(column_file)
    #        econn.index(ES_NAME, ES_COLUMN, id=column['id'], body=column_body)
    #    econn.update(ES_NAME, ES_COLUMN, id=column['id'], body={
    #        "script": "ctx._source.tables+=new_table",
    #        "params": {
    #            "new_table": column_body
    #        }
    #    })
    #    import pdb
    #    pdb.set_trace()

    #path = os.path.join(*path.split(os.path.sep)[1:])  # kill columns/ , tables/ etc.

    # upload each column in an ES_TABLE into a table entry in ES_COLUMN
    #for column in body.pop('columns'):

    #    columntableid = os.path.join(path, column['id'])
    #    LOGGER.info('%s: %s', es_op.im_func.func_name, columntableid)
    #    econn.index(ES_NAME, ES_COLUMNTABLE, id=columntableid,
    #          body=columnbody, parent=column['id'])

    #econn.index(ES_NAME, doc_type, id=path, body=body)

    #LOGGER.info('%s: %s', es_op.im_func.func_name, path)


def load():
    for doc_type in (ES_COLUMN, ES_TABLE, ):

        # upload columns and tables
        for dirpath, _, filenames in os.walk(doc_type + 's'):
            for filename in filenames:
                if filename.endswith('.json'):
                    CACHE['CNT'] += 1
                    if CACHE['CNT'] % 1000 == 0:
                        LOGGER.warn(CACHE['CNT'])

                    if doc_type == ES_TABLE:
                        ops = index_table(os.path.join(dirpath, filename))
                    elif doc_type == ES_COLUMN:
                        ops = index_column(os.path.join(dirpath, filename))
                    if ops:
                        for op_ in ops:
                            yield op_


def run():

    # TODO how do we handle iterative loading?
    DESTRUCTIVE = True

    econn = elastic_conn(ES_NAME, LOGGER)
    if DESTRUCTIVE == True:
        try:
            econn.indices.delete(index=ES_NAME)
            econn = elastic_conn(ES_NAME, LOGGER)
        except elasticsearch.exceptions.NotFoundError:
            pass

        for doc_type in (ES_COLUMN, ES_TABLE, ):
            mapping_path = os.path.join('mappings', doc_type + '.json')
            econn.indices.put_mapping(index=ES_NAME, doc_type=doc_type,
                                      body=json.load(open(mapping_path, 'r')))

    bulk(econn, (op for op in load()), chunk_size=1000)
    bulk(econn, [{
        '_op_type': 'index',
        '_index': ES_NAME,
        '_type': ES_COLUMN,
        '_id': col_id,
        '_source': column
    } for col_id, column in CACHE[ES_COLUMN].iteritems()], chunk_size=1000)


if __name__ == '__main__':
    run(*sys.argv[1:])
