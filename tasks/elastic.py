'''
Load data from /columns and /tables into elasticsearch using /mappings
'''

from luigi import Task, Parameter
from elasticsearch.helpers import bulk
import elasticsearch
import ujson as json
import os
import sys

from tasks.util import elastic_conn, get_logger


class Index(Task):

    destructive = Parameter(default=True)

    LOGGER = get_logger(__name__)
    ES_NAME = 'bigmetadata'
    ES_COLUMN = 'column'
    ES_TABLE = 'table'

    CACHE = {
        ES_COLUMN: {},
        'NAMES': set(),  # TODO cleaner way to not include duplicate names
        'CNT': 0
    }


    def index_column(self, path):
        '''
        Load json at path into idx.
        '''
        column_id = path
        #if econn.exists(ES_NAME, ES_COLUMN, id=column_id):
        #    return
        with open(path) as column_file:
            column = json.load(column_file)
        column_with_id = column.copy()
        column_with_id['id'] = column_id
        column_with_id['tables'] = []
        if int(column.get('weight', 0)) > 2 and column['name'] not in self.CACHE['NAMES']:
            self.CACHE['NAMES'].add(column['name'])
            self.CACHE[self.ES_COLUMN][column_id] = column_with_id
        #econn.index(ES_NAME, ES_COLUMN, id=column_id, body=body)
        #return [{
        #    '_op_type': 'index',
        #    '_index': ES_NAME,
        #    '_type': ES_COLUMN,
        #    '_id': column_id,
        #    '_source': column
        #}]


    def index_table(self, path):
        '''
        Add tables to columns.
        '''
        ops = []
        table_id = path
        with open(path) as table_file:
            table = json.load(table_file)

        # Replace real column with just an ID
        columns = table.pop('columns')
        table_for_column = table.copy()

        table['columns'] = [c['id'] for c in columns]
        table_for_column['id'] = table_id

        #econn.index(ES_NAME, ES_TABLE, id=table_id, body=body)
        for column in columns:
            # Add this table to the column, if we care about the column
            if column['id'] in self.CACHE[self.ES_COLUMN]:
                tfcc = table_for_column.copy()
                tfcc['resolutions'] = column.get('resolutions', [])
                tfcc['resolutions_nested'] = column.get('resolutions', [])
                self.CACHE[self.ES_COLUMN][column['id']]['tables'].append(tfcc)

        # Add the table
        ops.append({
            '_op_type': 'index',
            '_index': self.ES_NAME,
            '_type': self.ES_TABLE,
            '_id': table_id,
            '_source': table
        })

        return ops


    def load(self):
        for doc_type in (self.ES_COLUMN, self.ES_TABLE, ):

            # upload columns and tables
            for dirpath, _, filenames in os.walk(os.path.join('data', doc_type + 's')):
                for filename in sorted(filenames):
                    if filename.endswith('.json'):
                        self.CACHE['CNT'] += 1
                        if self.CACHE['CNT'] % 1000 == 0:
                            self.LOGGER.warn(self.CACHE['CNT'])

                        if doc_type == self.ES_TABLE:
                            ops = self.index_table(os.path.join(dirpath, filename))
                        elif doc_type == self.ES_COLUMN:
                            ops = self.index_column(os.path.join(dirpath, filename))
                        if ops:
                            for op_ in ops:
                                yield op_


    def run(self):

        # TODO how do we handle iterative loading?

        econn = elastic_conn(self.ES_NAME, self.LOGGER)
        if self.destructive is True:
            try:
                econn.indices.delete(index=self.ES_NAME)
                econn = elastic_conn(self.ES_NAME, self.LOGGER)
            except elasticsearch.exceptions.NotFoundError:
                pass

            for doc_type in (self.ES_COLUMN, self.ES_TABLE, ):
                mapping_path = os.path.join('mappings', doc_type + '.json')
                econn.indices.put_mapping(index=self.ES_NAME, doc_type=doc_type,
                                          body=json.load(open(mapping_path, 'r')))

        bulk(econn, (op for op in self.load()), chunk_size=1000)
        bulk(econn, [{
            '_op_type': 'index',
            '_index': self.ES_NAME,
            '_type': self.ES_COLUMN,
            '_id': col_id,
            '_source': column
        } for col_id, column in self.CACHE[self.ES_COLUMN].iteritems()], chunk_size=1000)
