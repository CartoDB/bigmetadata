'''
meta.py

functions for persisting metadata about tables loaded via ETL
'''

import os
import re

from sqlalchemy import (Column, Integer, String, Boolean, MetaData,
                        create_engine, event)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import DDL

#from tasks.util import pg_cursor

#Base = declarative_base()


connection = create_engine('postgres://{user}:{password}@{host}:{port}/{db}'.format(
    user=os.environ.get('PGUSER', 'postgres'),
    password=os.environ.get('PGPASSWORD', ''),
    host=os.environ.get('PGHOST', 'localhost'),
    port=os.environ.get('PGPORT', '5432'),
    db=os.environ.get('PGDATABASE', 'postgres')
))
metadata = MetaData(connection)


# Create necessary schemas
event.listen(metadata, 'before_create', DDL('CREATE SCHEMA %(schema)s IF NOT EXISTS'))

#   # For example, a single census identifier like b01001001
#   class ColumnDefinition(Base):
#       id = Column(String, primary_key=True) # fully-qualified id like 'us/census/acs/b01001001'
#   
#       colname = Column(String) # human-readable column name to provide in export
#   
#       type = Column(String) # postgres type, IE numeric, string, geometry, etc.
#       description = Column(String) # human-readable description to provide in
#                                    # bigmetadata
#       is_geoid = Column(Boolean) # Is this column used to reference a geom?
#   
#       parent = None # TODO
#       ancestors = None # TODO
#   
#       weight = Column(Integer, default=0)
#       aggregate = Column(String) # what aggregate operation to use when adding
#                                  # these together across geoms: AVG, SUM etc.
#   
#       tables = None # TODO relationship to TableDefinition via ColumnTable
#   
#   
#   # We should have one of these for every table we load in through the ETL
#   class TableDefinition(Base):
#       id = Column(String, primary_key=True) # fully-qualified id like 'us/census/acs/acs5yr_2011/seq0001'
#   
#       description = Column(String)
#       columns = None # TODO many-to-many to ColumnDefinitions via ColumnTable
#       geom_column = None # TODO many-to-one to ColumnDefinition 
#   
#   
#   # A connection between a table and a column
#   class ColumnTable(Base):
#       id = Column(Integer, primary_key=True)
#       column_id = None # TODO relation to a column
#       table_id = None # TODO relation to a table
#   
#       metadata = Column(JSON)
#   
#   
#   # A collection of ColumnTables that are to be exported to carto
#   class ExportTable(Base):
#       id = Column(Integer, primary_key=True)
#   
#       # TODO how do we derive other data in here?
#       #
#   
#    columns = None # TODO relationship to ColumnDefinitions via ColumnTable

def metafy(table):
    '''
    Add Column and Table data based off of 
    '''
    cursor = pg_cursor()
