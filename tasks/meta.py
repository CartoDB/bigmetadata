'''
meta.py

functions for persisting metadata about tables loaded via ETL
'''

import os
import re

from sqlalchemy import (Column, Integer, String, Boolean, MetaData,
                        create_engine, event, ForeignKey, PrimaryKeyConstraint,
                        ForeignKeyConstraint)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, composite
from sqlalchemy.schema import DDL

connection = create_engine('postgres://{user}:{password}@{host}:{port}/{db}'.format(
    user=os.environ.get('PGUSER', 'postgres'),
    password=os.environ.get('PGPASSWORD', ''),
    host=os.environ.get('PGHOST', 'localhost'),
    port=os.environ.get('PGPORT', '5432'),
    db=os.environ.get('PGDATABASE', 'postgres')
))

metadata = MetaData(connection)

# Add information schemas
#metadata.reflect(schema='information_schema', views=True)

#Autobase = automap_base()
#Autobase.prepare(connection, reflect=True)

# Create necessary schemas
event.listen(metadata, 'before_create', DDL('CREATE SCHEMA IF NOT EXISTS %(schema)s').execute_if(
    callable_=lambda ddl, target, bind, tables, **kwargs: 'schema' in ddl.context
))

Base = declarative_base(metadata=metadata)

# create a configured "Session" class
Session = sessionmaker(bind=connection)


# A connection between a table and a column
class BMDColumnTable(Base):

    __tablename__ = 'bmd_column_table'

    column_id = Column(String, ForeignKey('bmd_column.id'), primary_key=True)
    table_id = Column(String, ForeignKey('bmd_table.id'), primary_key=True)

    colname = Column(String, nullable=False)

    column = relationship("BMDColumn", back_populates="tables")
    table = relationship("BMDTable", back_populates="columns")

    extra = Column(JSON)


# For example, a single census identifier like b01001001
# This has a one-to-many relation with ColumnTable and extends our existing
# columns through that table
# TODO get the source/target relations working through
class BMDColumnToColumn(Base):
    __tablename__ = 'bmd_column_to_column'

    source_id = Column(String, ForeignKey('bmd_column.id'), primary_key=True)
    target_id = Column(String, ForeignKey('bmd_column.id'), primary_key=True)
    source = relationship("BMDColumn", back_populates='target_columns',
                          foreign_keys=[source_id])
    target = relationship("BMDColumn", back_populates='source_columns',
                          foreign_keys=[target_id])

    reltype = Column(String, primary_key=True)


class BMDColumn(Base):
    __tablename__ = 'bmd_column'

    id = Column(String, primary_key=True) # fully-qualified id like 'us/census/acs/b01001001'

    type = Column(String, nullable=False) # postgres type, IE numeric, string, geometry, etc.
    description = Column(String) # human-readable description to provide in
                                 # bigmetadata

    weight = Column(Integer, default=0)
    aggregate = Column(String) # what aggregate operation to use when adding
                               # these together across geoms: AVG, SUM etc.

    tables = relationship("BMDColumnTable", back_populates="column")
    tags = relationship("BMDColumnTag", back_populates="column")

    source_columns = relationship("BMDColumnToColumn", back_populates='source',
                                  foreign_keys=[BMDColumnToColumn.source_id])
    target_columns = relationship("BMDColumnToColumn", back_populates='target',
                                  foreign_keys=[BMDColumnToColumn.target_id])
    #primaryjoin=
    #'and_(ColumnTableInfoMetadata.table_schema==ColumnTableInfo.table_schema, '
    #'ColumnTableInfoMetadata.table_name==ColumnTableInfo.table_name,'
    #'ColumnTableInfoMetadata.column_name==ColumnTableInfo.column_name)',


# We should have one of these for every table we load in through the ETL
class BMDTable(Base):
    __tablename__ = 'bmd_table'

    id = Column(String, primary_key=True) # fully-qualified id like 'us/census/acs/acs5yr_2011/seq0001'

    columns = relationship("BMDColumnTable", back_populates="table")

    tablename = Column(String, nullable=False)
    timespan = Column(String)
    bounds = Column(String)
    description = Column(String)


class BMDTag(Base):
    __tablename__ = 'bmd_tag'

    id = Column(String, primary_key=True)

    name = Column(String, nullable=False)
    description = Column(String)

    columns = relationship("BMDColumnTag", back_populates="tag")


class BMDColumnTag(Base):
    __tablename__ = 'bmd_column_tag'

    column_id = Column(String, ForeignKey('bmd_column.id'), primary_key=True)
    tag_id = Column(String, ForeignKey('bmd_tag.id'), primary_key=True)

    column = relationship("BMDColumn", back_populates='tags')
    tag = relationship("BMDTag", back_populates='columns')


def fromkeys(d, l):
    '''
    Similar to the builtin dict `fromkeys`, except remove keys with a value
    of None
    '''
    d = d.fromkeys(l)
    return dict((k, v) for k, v in d.iteritems() if v is not None)
#
#TableInfoMetadata.__table__.drop(connection)
#ColumnTableInfoMetadata.__table__.drop(connection)
#ColumnInfoMetadata.__table__.drop(connection)

Base.metadata.drop_all()
Base.metadata.create_all()
