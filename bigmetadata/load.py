'''
Load data from /columns and /tables into elasticsearch using /mappings
'''

import elasticsearch
import json
import os
import sys
from bigmetadata.utils import elastic_conn, get_logger

LOGGER = get_logger(__name__)
ES_NAME = 'bigmetadata'
ES_COLUMN = 'column'
ES_TABLE = 'table'

def index_json(idx, doc_type, path):
    '''
    Load json at path into idx.
    '''
    with open(path, 'r') as column_file:
        body = json.load(column_file)
    if idx.exists(ES_NAME, doc_type, id=path):
        es_op = idx.update
        body = {'doc': body}
    else:
        es_op = idx.index
    es_op(ES_NAME, doc_type, id=path, body=body)
    LOGGER.info('%s: %s', es_op.im_func.func_name, path)

def run():

    # TODO how do we handle iterative loading?
    DESTRUCTIVE = False

    econn = elastic_conn(ES_NAME, LOGGER)
    if DESTRUCTIVE == True:
        try:
            econn.indices.delete(index=ES_NAME)
            econn = elastic_conn(ES_NAME, LOGGER)
        except elasticsearch.exceptions.NotFoundError:
            pass
    for doc_type in (ES_TABLE, ES_COLUMN):
        mapping_path = os.path.join('mappings', doc_type + '.json')
        econn.indices.put_mapping(index=ES_NAME, doc_type=doc_type,
                                  body=json.load(open(mapping_path, 'r')))

        for dirpath, _, filenames in os.walk(doc_type + 's'):
            for filename in filenames:
                if filename.endswith('.json'):
                    index_json(econn, doc_type, os.path.join(dirpath, filename))


if __name__ == '__main__':
    run(*sys.argv[1:])
