
import os
import elasticsearch
import logging
import sys
import time

def elastic_conn(index_name, logger=None):
    '''
    Obtain an index with specified name.  Waits for elasticsearch to start.
    Returns elasticsearch.
    '''
    elastic = elasticsearch.Elasticsearch([{
        'host': os.environ.get('ES_HOST', 'localhost'),
        'port': os.environ.get('ES_PORT', '9200')
    }])
    while True:
        try:
            elastic.indices.create(index=index_name, ignore=400)  # pylint: disable=unexpected-keyword-arg
            break
        except elasticsearch.exceptions.ConnectionError:
            if logger:
                logger.info('waiting for elasticsearch')
            time.sleep(1)
    return elastic


def get_logger(name):
    '''
    Obtain a logger outputing to stderr with specified name. Defaults to INFO
    log level.
    '''
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(message)s'))
    logger.addHandler(handler)
    return logger
