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
ES_COLUMN_DOC_TYPE = 'column'

def load_column(idx, path):
    '''
    Load column json at path into idx.
    '''
    with open(path, 'r') as column_file:
        body = json.load(column_file)
    if idx.exists(ES_NAME, ES_COLUMN_DOC_TYPE, id=path):
        es_op = idx.update
        body = {'doc': body}
    else:
        es_op = idx.index
    es_op(ES_NAME, ES_COLUMN_DOC_TYPE, id=path, body=body)
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
    econn.indices.put_mapping(index=ES_NAME, doc_type=ES_COLUMN_DOC_TYPE,
                              body=json.load(open(os.path.join('mappings', 'column.json'), 'r')))
    for dirpath, dirnames, filenames in os.walk('columns'):
        for filename in filenames:
            if filename.endswith('.json'):
                load_column(econn, os.path.join(dirpath, filename))


if __name__ == '__main__':
    run(*sys.argv[1:])
