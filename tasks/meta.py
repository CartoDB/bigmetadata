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
                        ForeignKeyConstraint, Table, exc, func)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, composite
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.types import UserDefinedType



def get_engine():
    engine = create_engine('postgres://{user}:{password}@{host}:{port}/{db}'.format(
        user=os.environ.get('PGUSER', 'postgres'),
        password=os.environ.get('PGPASSWORD', ''),
        host=os.environ.get('PGHOST', 'localhost'),
        port=os.environ.get('PGPORT', '5432'),
        db=os.environ.get('PGDATABASE', 'postgres')
    ))

    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        connection_record.info['pid'] = os.getpid()

    @event.listens_for(engine, "checkout")
    def checkout(dbapi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info['pid'] != pid:
            connection_record.connection = connection_proxy.connection = None
            raise exc.DisconnectionError(
                "Connection record belongs to pid %s, "
                "attempting to check out in pid %s" %
                (connection_record.info['pid'], pid)
            )
    return engine


metadata = MetaData(get_engine())
Base = declarative_base(metadata=metadata)

Session = sessionmaker(bind=get_engine())


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

    column = relationship("BMDColumn", back_populates='tags')
    tag = relationship("BMDTag", back_populates='columns')


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = sessionmaker(bind=get_engine())()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def fromkeys(d, l):
    '''
    Similar to the builtin dict `fromkeys`, except remove keys with a value
    of None
    '''
    d = d.fromkeys(l)
    return dict((k, v) for k, v in d.iteritems() if v is not None)


Base.metadata.create_all()
