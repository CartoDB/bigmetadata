'''
meta.py

functions for persisting metadata about tables loaded via ETL
'''

from contextlib import contextmanager
import os
import re

from luigi import Task, BooleanParameter, Target

from sqlalchemy import (Column, Integer, String, Boolean, MetaData,
                        create_engine, event, ForeignKey, PrimaryKeyConstraint,
                        ForeignKeyConstraint, Table)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, composite
from sqlalchemy.orm.exc import NoResultFound


connection = create_engine('postgres://{user}:{password}@{host}:{port}/{db}'.format(
    user=os.environ.get('PGUSER', 'postgres'),
    password=os.environ.get('PGPASSWORD', ''),
    host=os.environ.get('PGHOST', 'localhost'),
    port=os.environ.get('PGPORT', '5432'),
    db=os.environ.get('PGDATABASE', 'postgres')
))

metadata = MetaData(connection)


# Create necessary schemas
#event.listen(metadata, 'before_create', DDL('CREATE SCHEMA IF NOT EXISTS %(schema)s').execute_if(
#    callable_=lambda ddl, target, bind, tables, **kwargs: 'schema' in ddl.context
#))

Base = declarative_base(metadata=metadata)

# create a configured "Session" class
Session = sessionmaker(bind=connection)


from sqlalchemy import func
from sqlalchemy.types import UserDefinedType


class Geometry(UserDefinedType):
    def get_col_spec(self):
        return "GEOMETRY"

    def bind_expression(self, bindvalue):
        return func.ST_GeomFromText(bindvalue, type_=self)

    def column_expression(self, col):
        return func.ST_AsText(col, type_=self)


# A connection between a table and a column
class BMDColumnTable(Base):

    __tablename__ = 'bmd_column_table'

    column_id = Column(String, ForeignKey('bmd_column.id'), primary_key=True)
    table_id = Column(String, ForeignKey('bmd_table.id'), primary_key=True)

    colname = Column(String, nullable=False)

    column = relationship("BMDColumn", back_populates="tables", cascade="all,delete")
    table = relationship("BMDTable", back_populates="columns", cascade="all,delete")

    extra = Column(JSON)


# For example, a single census identifier like b01001001
# This has a one-to-many relation with ColumnTable and extends our existing
# columns through that table
# TODO get the source/target relations working through
class BMDColumnToColumn(Base):
    __tablename__ = 'bmd_column_to_column'

    source_id = Column(String, ForeignKey('bmd_column.id'), primary_key=True)
    target_id = Column(String, ForeignKey('bmd_column.id'), primary_key=True)

    reltype = Column(String, primary_key=True)


class BMDColumn(Base):
    __tablename__ = 'bmd_column'

    id = Column(String, primary_key=True) # fully-qualified id like '"us.census.acs".b01001001'

    type = Column(String, nullable=False) # postgres type, IE numeric, string, geometry, etc.
    name = Column(String) # human-readable name to provide in bigmetadata

    description = Column(String) # human-readable description to provide in
                                 # bigmetadata

    weight = Column(Integer, default=0)
    aggregate = Column(String) # what aggregate operation to use when adding
                               # these together across geoms: AVG, SUM etc.

    tables = relationship("BMDColumnTable", back_populates="column", cascade="all,delete")
    tags = relationship("BMDColumnTag", back_populates="column", cascade="all,delete")

    source_columns = relationship("BMDColumnToColumn", backref='target',
                                  foreign_keys='BMDColumnToColumn.source_id',
                                  cascade="all,delete")
    target_columns = relationship("BMDColumnToColumn", backref='source',
                                  foreign_keys='BMDColumnToColumn.target_id',
                                  cascade="all,delete")


# We should have one of these for every table we load in through the ETL
class BMDTable(Base):
    __tablename__ = 'bmd_table'

    id = Column(String, primary_key=True) # fully-qualified id like '"us.census.acs".extract_year_2013_sample_5yr'

    columns = relationship("BMDColumnTable", back_populates="table",
                           cascade="all,delete")

    tablename = Column(String, nullable=False)
    timespan = Column(String)
    bounds = Column(String)
    description = Column(String)


class BMDTag(Base):
    __tablename__ = 'bmd_tag'

    id = Column(String, primary_key=True)

    name = Column(String, nullable=False)
    description = Column(String)

    columns = relationship("BMDColumnTag", back_populates="tag", cascade="all,delete")


class BMDColumnTag(Base):
    __tablename__ = 'bmd_column_tag'

    column_id = Column(String, ForeignKey('bmd_column.id'), primary_key=True)
    tag_id = Column(String, ForeignKey('bmd_tag.id'), primary_key=True)

    column = relationship("BMDColumn", back_populates='tags', cascade="all,delete")
    tag = relationship("BMDTag", back_populates='columns', cascade="all,delete")


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


#class BMD(object):
#
#    def __init__(self, session):
#        # update existing columns without destroying relationships
#        #with session_scope() as session:
#        #    for attrname, obj in type(self).__dict__.iteritems():
#        #        if not attrname.startswith('__'):
#        #            obj.id = self.qualify(obj.id)
#        #            existing_obj = session.query(type(obj)).get(obj.id)
#        #            if existing_obj:
#        #                #for k, v in obj.__dict__.iteritems():
#        #                #    if not k.startswith('__'):
#        #                #        setattr(
#        #                session.delete(existing_obj)
#
#        # create new columns, or update existing without destroying
#        # relationships
#        #with session_scope() as session:
#        for attrname, obj in type(self).__dict__.iteritems():
#            if not attrname.startswith('__'):
#                get_or_update = self.build_get_or_update(obj)
#                setattr(self, attrname, get_or_update)
#                get_or_update(session)
#
#    def build_get_or_update(self, bmd_obj):
#        def get_or_update(session):
#            existing_obj = session.query(type(bmd_obj)).get(self.qualify(
#                self.qualify(bmd_obj.id)))
#            if existing_obj:
#                for key, val in bmd_obj.__dict__.iteritems():
#                    if not key.startswith('__'):
#                        setattr(existing_obj, key, val)
#                return existing_obj
#            else:
#                session.add(bmd_obj)
#                bmd_obj.id = self.qualify(bmd_obj.id)
#                return bmd_obj
#        return get_or_update
#
#    def qualify(self, n):
#        if n.startswith('"'):
#            return n
#        return '"{classpath}".{n}'.format(classpath=classpath(self), n=n)


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

#Base.metadata.drop_all()
Base.metadata.create_all()
