'''
meta.py

functions for persisting metadata about tables loaded via ETL
'''

import os
import re

from luigi import Task, BooleanParameter, Target

from sqlalchemy import (Column, Integer, String, Boolean, MetaData,
                        create_engine, event, ForeignKey, PrimaryKeyConstraint,
                        ForeignKeyConstraint, Table, types)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, composite
from sqlalchemy.schema import DDL

from contextlib import contextmanager
from tasks.util import classpath, camel_to_underscore, slug_column


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


class BMD(object):

    def __init__(self):
        # delete existing columns if they overlap
        with session_scope() as session:
            for attrname, obj in type(self).__dict__.iteritems():
                if not attrname.startswith('__'):
                    obj.id = self.qualify(obj.id)
                    existing_obj = session.query(type(obj)).get(obj.id)
                    if existing_obj:
                        session.delete(existing_obj)

        # create new columns
        with session_scope() as session:
            for attrname, obj in type(self).__dict__.iteritems():
                if not attrname.startswith('__'):
                    session.add(obj)
                    setattr(self, attrname, obj)

    def qualify(self, n):
        return '"{classpath}".{n}'.format(classpath=classpath(self), n=n)


def fromkeys(d, l):
    '''
    Similar to the builtin dict `fromkeys`, except remove keys with a value
    of None
    '''
    d = d.fromkeys(l)
    return dict((k, v) for k, v in d.iteritems() if v is not None)


class SessionTask(Task):
    '''
    A Task whose `runession` and `columns` methods should be overriden, and
    executes creating a single output table defined by its name, path, and
    defined columns.
    '''

    force = BooleanParameter(default=False)

    #def columns(self):
    #    return NotImplementedError('Must implement columns method that returns '
    #                               'an iterable sequence of columns for the '
    #                               'table generated by this task.')


    def runsession(self, session):
        return NotImplementedError('Must implement runsession method that '
                                   'populates the table')

    def run(self):
        with session_scope() as session:
            self.output().create() # TODO in session
            self.runsession(session)

    def output(self):
        target = TableTarget(self, self.columns)
        #if self.force:
        #    target.table.drop()
        #    self.force = False
        return target


class TableTarget(Target):

    def __init__(self, task, columntables, timespan=None, bounds=None,
                 description=None):
        task_id = camel_to_underscore(task.task_id)
        task_id = task_id.replace('force=_false', '')
        task_id = task_id.replace('force=_true', '')
        self.tablename = re.sub(r'[^a-z0-9]+', '_', task_id).strip('_')
        self.schema = classpath(task)
        self.table_id = '"{schema}".{tablename}'.format(schema=self.schema,
                                                        tablename=self.tablename)
        self.columntables = columntables
        self.timespan = timespan
        self.bounds = bounds
        self.description = description

    def table(self, session):
        #return session.query(Table
        import pdb
        pdb.set_trace()

    def exists(self):
        return False # TODO
        #return self.table().exists()

    def create(self, **kwargs):
        columns = []

        with session_scope() as session:
            existing_table = session.query(BMDTable).get(self.table_id)
            if existing_table:
                session.delete(existing_table)

        with session_scope() as session:
            session.execute('CREATE SCHEMA IF NOT EXISTS "{schema}"'.format(
                schema=self.schema))
            self.metatable = BMDTable(id=self.table_id,
                                      tablename=slug_column(self.table_id),
                                      bounds=self.bounds, timespan=self.timespan,
                                      description=self.description)
            for columntable in self.columntables:
                session.add(columntable)
                session.add(columntable.column)
                columntable.table = self.metatable
                columns.append(Column(columntable.colname,
                                      getattr(types, columntable.column.type)))
        table = Table(self.tablename, metadata, *columns, schema=self.schema,
                      extend_existing=True)
        if table.exists():
            table.drop()
        table.create(**kwargs)

#
#TableInfoMetadata.__table__.drop(connection)
#ColumnTableInfoMetadata.__table__.drop(connection)
#ColumnInfoMetadata.__table__.drop(connection)

#Base.metadata.drop_all()
Base.metadata.create_all()
