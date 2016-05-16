'''
meta.py

functions for persisting metadata about tables loaded via ETL
'''

import os
import re

import luigi
from luigi import Task, BooleanParameter, Target, Event

from sqlalchemy import (Column, Integer, Text, Boolean, MetaData, Numeric,
                        create_engine, event, ForeignKey, PrimaryKeyConstraint,
                        ForeignKeyConstraint, Table, exc, func, UniqueConstraint)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, composite, backref
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.collections import (attribute_mapped_collection,
                                        InstrumentedList, MappedCollection,
                                        _SerializableAttrGetter, collection)
from sqlalchemy.types import UserDefinedType


def natural_sort_key(s, _nsre=re.compile('([0-9]+)')):
    return [int(text) if text.isdigit() else text.lower()
            for text in re.split(_nsre, s)]


_engine = create_engine('postgres://{user}:{password}@{host}:{port}/{db}'.format(
    user=os.environ.get('PGUSER', 'postgres'),
    password=os.environ.get('PGPASSWORD', ''),
    host=os.environ.get('PGHOST', 'localhost'),
    port=os.environ.get('PGPORT', '5432'),
    db=os.environ.get('PGDATABASE', 'postgres')
))

def get_engine():

    @event.listens_for(_engine, "connect")
    def connect(dbapi_connection, connection_record):
        connection_record.info['pid'] = os.getpid()

    @event.listens_for(_engine, "checkout")
    def checkout(dbapi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info['pid'] != pid:
            connection_record.connection = connection_proxy.connection = None
            raise exc.DisconnectionError(
                "Connection record belongs to pid %s, "
                "attempting to check out in pid %s" %
                (connection_record.info['pid'], pid)
            )
    return _engine


metadata = MetaData(bind=get_engine(), schema='observatory')
Base = declarative_base(metadata=metadata)


class Geometry(UserDefinedType):
    def get_col_spec(self):
        return "GEOMETRY"

    def bind_expression(self, bindvalue):
        return func.ST_GeomFromText(bindvalue, type_=self)

    def column_expression(self, col):
        return func.ST_AsText(col, type_=self)


# A connection between a table and a column
class OBSColumnTable(Base):

    __tablename__ = 'obs_column_table'

    column_id = Column(Text, ForeignKey('obs_column.id', ondelete='cascade'), primary_key=True)
    table_id = Column(Text, ForeignKey('obs_table.id', ondelete='cascade'), primary_key=True)

    colname = Column(Text, nullable=False)

    column = relationship("OBSColumn", back_populates="tables")
    table = relationship("OBSTable", back_populates="columns")

    extra = Column(JSON)

    colname_constraint = UniqueConstraint(table_id, colname)


def tag_creator(tagtarget):
    tag = tagtarget.get(current_session()) or tagtarget._tag
    coltag = OBSColumnTag(tag=tag, tag_id=tag.id)
    if tag in current_session():
        current_session().expunge(tag)
    return coltag


def targets_creator(coltarget_or_col, reltype):
    # internal to task
    if isinstance(coltarget_or_col, OBSColumn):
        col = coltarget_or_col
    # from required task
    else:
        col = coltarget_or_col.get(current_session())
    return OBSColumnToColumn(target=col, reltype=reltype)


def sources_creator(coltarget_or_col, reltype):
    # internal to task
    if isinstance(coltarget_or_col, OBSColumn):
        col = coltarget_or_col
    # from required task
    else:
        col = coltarget_or_col.get(current_session())
    return OBSColumnToColumn(source=col, reltype=reltype)


class PatchedMappedCollection(MappedCollection):

    @collection.remover
    @collection.internally_instrumented
    def remove(self, value, _sa_initiator=None):
        key = self.keyfunc(value)
        if key not in self and None in self:
            key = None
        if self[key] != value:
            raise Exception(
                "Can not remove '%s': collection holds '%s' for key '%s'. "
                "Possible cause: is the MappedCollection key function "
                "based on mutable properties or properties that only obtain "
                "values after flush?" %
                (value, self[key], key))
        self.__delitem__(key, _sa_initiator)


target_collection = lambda: PatchedMappedCollection(_SerializableAttrGetter("target"))
source_collection = lambda: PatchedMappedCollection(_SerializableAttrGetter("source"))


class OBSColumnToColumn(Base):
    __tablename__ = 'obs_column_to_column'

    source_id = Column(Text, ForeignKey('obs_column.id', ondelete='cascade'), primary_key=True)
    target_id = Column(Text, ForeignKey('obs_column.id', ondelete='cascade'), primary_key=True)

    reltype = Column(Text, primary_key=True)

    source = relationship('OBSColumn',
                          foreign_keys=[source_id],
                          backref=backref(
                              "tgts",
                              collection_class=target_collection,
                              cascade="all, delete-orphan",
                          ))
    target = relationship('OBSColumn',
                          foreign_keys=[target_id],
                          backref=backref(
                              "srcs",
                              collection_class=source_collection,
                              cascade="all, delete-orphan",
                          ))


# For example, a single census identifier like b01001001
class OBSColumn(Base):
    __tablename__ = 'obs_column'

    id = Column(Text, primary_key=True) # fully-qualified id like '"us.census.acs".b01001001'

    type = Column(Text, nullable=False) # postgres type, IE numeric, string, geometry, etc.
    name = Column(Text) # human-readable name to provide in bigmetadata

    description = Column(Text) # human-readable description to provide in
                                 # bigmetadata

    weight = Column(Integer, default=0)
    aggregate = Column(Text) # what aggregate operation to use when adding
                               # these together across geoms: AVG, SUM etc.

    tables = relationship("OBSColumnTable", back_populates="column", cascade="all,delete")
    tags = association_proxy('column_column_tags', 'tag', creator=tag_creator)

    targets = association_proxy('tgts', 'reltype', creator=targets_creator)
    sources = association_proxy('srcs', 'reltype', creator=sources_creator)

    version = Column(Numeric, default=0, nullable=False)
    extra = Column(JSON)

    def should_index(self):
        return 'geom_ref' in self.targets.values()

    def children(self):
        children = [col for col, reltype in self.sources.iteritems() if reltype == 'denominator']
        children.sort(key=lambda x: natural_sort_key(x.name))
        return children

    def has_children(self):
        '''
        Returns True if this column has children, false otherwise.
        '''
        return len(self.children()) > 0

    def has_denominator(self):
        '''
        Returns True if this column has no denominator, False otherwise.
        '''
        return 'denominator' in self.targets.values()

    def denominator(self):
        '''
        Return the denominator of this column.
        '''
        if not self.has_denominator():
            return None
        return [(k, v) for k, v in  self.targets.iteritems() if v == 'denominator'][0][0]

    def summable(self):
        '''
        Returns True if we can sum this column and calculate for arbitrary
        areas.
        '''
        return self.aggregate == 'sum'


# We should have one of these for every table we load in through the ETL
class OBSTable(Base):
    __tablename__ = 'obs_table'

    id = Column(Text, primary_key=True) # fully-qualified id like '"us.census.acs".extract_year_2013_sample_5yr'

    columns = relationship("OBSColumnTable", back_populates="table",
                           cascade="all,delete")

    tablename = Column(Text, nullable=False)
    timespan = Column(Text)
    bounds = Column(Text)
    description = Column(Text)

    version = Column(Numeric, default=0, nullable=False)


class OBSTag(Base):
    __tablename__ = 'obs_tag'

    id = Column(Text, primary_key=True)

    name = Column(Text, nullable=False)
    type = Column(Text, nullable=False)
    description = Column(Text)

    columns = association_proxy('tag_column_tags', 'column')

    version = Column(Numeric, default=0, nullable=False)


class OBSColumnTag(Base):
    __tablename__ = 'obs_column_tag'

    column_id = Column(Text, ForeignKey('obs_column.id', ondelete='cascade'), primary_key=True)
    tag_id = Column(Text, ForeignKey('obs_tag.id', ondelete='cascade'), primary_key=True)

    column = relationship("OBSColumn",
                          foreign_keys=[column_id],
                          backref=backref('column_column_tags',
                                          cascade='all, delete-orphan')
                         )
    tag = relationship("OBSTag",
                       foreign_keys=[tag_id],
                       backref=backref('tag_column_tags',
                                       cascade='all, delete-orphan')
                      )


class CurrentSession(object):

    def __init__(self):
        self._session = None
        self._pid = None

    def begin(self):
        if not self._session:
            self._session = sessionmaker(bind=get_engine())()
        self._pid = os.getpid()

    def get(self):
        # If we forked, there would be a PID mismatch and we need a new
        # connection
        if self._pid != os.getpid():
            self._session = None
            print 'FORKED: {} not {}'.format(self._pid, os.getpid())
        if not self._session:
            self.begin()
        return self._session

    def commit(self):
        if self._pid != os.getpid():
            raise Exception('cannot commit forked connection')
        if not self._session:
            return
        try:
            self._session.commit()
        except:
            self._session.rollback()
            self._session.expunge_all()
            raise
        finally:
            self._session.close()
            self._session = None

    def rollback(self):
        if self._pid != os.getpid():
            raise Exception('cannot rollback forked connection')
        if not self._session:
            return
        try:
            self._session.rollback()
        except:
            raise
        finally:
            self._session.expunge_all()
            self._session.close()
            self._session = None


_current_session = CurrentSession()


def current_session():
    return _current_session.get()


#@luigi.Task.event_handler(Event.SUCCESS)
def session_commit(task):
    '''
    commit the global session
    '''
    print 'commit'
    try:
        _current_session.commit()
    except Exception as err:
        print err
        raise


#@luigi.Task.event_handler(Event.FAILURE)
def session_rollback(task, exception):
    '''
    rollback the global session
    '''
    print 'rollback: {}'.format(exception)
    _current_session.rollback()


def fromkeys(d, l):
    '''
    Similar to the builtin dict `fromkeys`, except remove keys with a value
    of None
    '''
    d = d.fromkeys(l)
    return dict((k, v) for k, v in d.iteritems() if v is not None)


Base.metadata.create_all()
