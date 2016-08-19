'''
meta.py

functions for persisting metadata about tables loaded via ETL
'''

import os
import re
import weakref

import luigi
from luigi import Task, BooleanParameter, Target, Event

from sqlalchemy import (Column, Integer, Text, Boolean, MetaData, Numeric,
                        create_engine, event, ForeignKey, PrimaryKeyConstraint,
                        ForeignKeyConstraint, Table, exc, func, UniqueConstraint)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, composite, backref
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.collections import attribute_mapped_collection
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
    '''
    Glues together :class:`~.meta.OBSColumn` and :class:`~.meta.OBSTable`.
    If this object exists, the related column should exist in the related
    table, and can be selected with :attr:`~.OBSColumnTable.colname`.

    Unique along both ``(column_id, table_id)`` and ``(table_id, colname)``.

    These should *never* be created manually.  Their creation and removal
    is handled automatically as part of
    :meth:`util.TableTarget.update_or_create_metadata`.

    .. py:attribute:: column_id

       ID of the linked :class:`~.meta.OBSColumn`.

    .. py:attribute:: table_id

       ID of the linked :class:`~.meta.OBSTable`.

    .. py:attribute:: colname

       Column name for selecting this column in a table.

    .. py:attribute:: column

       The linked :class:`~.meta.OBSColumn`.

    .. py:attribute:: table

       The linked :class:`~.meta.OBSTable`.

    .. py:attribute:: extra

       Extra JSON information about the data.  This could include statistics
       like max, min, average, etc.
    '''

    __tablename__ = 'obs_column_table'

    column_id = Column(Text, ForeignKey('obs_column.id', ondelete='cascade'), primary_key=True)
    table_id = Column(Text, ForeignKey('obs_table.id', ondelete='cascade'), primary_key=True)

    colname = Column(Text, nullable=False)

    column = relationship("OBSColumn", back_populates="tables")
    table = relationship("OBSTable", back_populates="columns")

    extra = Column(JSON)

    colname_constraint = UniqueConstraint(table_id, colname)


def tag_creator(tagtarget):
    if tagtarget is None:
        raise Exception('None passed to tagtarget')
    tag = tagtarget.get(current_session()) or tagtarget._tag
    coltag = OBSColumnTag(tag=tag, tag_id=tag.id)
    if tag in current_session():
        current_session().expunge(tag)
    return coltag


def targets_creator(coltarget_or_col, reltype):
    # we should never see these committed, they are a side effect of output()
    # being run before parent tasks can generate requirements
    # they would violate constraints
    if coltarget_or_col is None:
        raise Exception('None passed to coltarget_or_col')
    # internal to task
    elif isinstance(coltarget_or_col, OBSColumn):
        col = coltarget_or_col
    # from required task
    else:
        col = coltarget_or_col.get(current_session())
    return OBSColumnToColumn(target=col, reltype=reltype)


class OBSColumnToColumn(Base):
    '''
    Relates one column to another.  For example, a ``Text`` column may contain
    identifiers that are unique to geometries in another ``Geometry`` column.
    In that case, an :class:`~.meta.OBSColumnToColumn` object of
    :attr:`~.meta.OBSColumnToColumn.reltype` :data:`~.meta.GEOM_REF` should
    indicate the relationship, and make relational joins possible between
    tables that have both columns with those that only have one.

    These should *never* be created manually.  Their creation should
    be handled automatically from specifying :attr:`.OBSColumn.targets`.

    These are unique on ``(source_id, target_id)``.

    .. py:attribute:: source_id

       ID of the linked source :class:`~.meta.OBSColumn`.

    .. py:attribute:: target_id

       ID of the linked target :class:`~.meta.OBSColumn`.

    .. py:attribute:: reltype

       required text specifying the relation type.  Examples are
       :data:`~.meta.GEOM_REF` and `~.meta.DENOMINATOR`.

    .. py:attribute:: source

       The linked source :class:`~.meta.OBSColumn`.

    .. py:attribute:: target

       The linked target :class:`~.meta.OBSColumn`.
    '''
    __tablename__ = 'obs_column_to_column'

    source_id = Column(Text, ForeignKey('obs_column.id', ondelete='cascade'), primary_key=True)
    target_id = Column(Text, ForeignKey('obs_column.id', ondelete='cascade'), primary_key=True)

    reltype = Column(Text, primary_key=True)

    source = relationship('OBSColumn',
                          foreign_keys=[source_id],
                          backref=backref(
                              "tgts",
                              collection_class=attribute_mapped_collection("target"),
                              cascade="all, delete-orphan",
                          ))
    target = relationship('OBSColumn', foreign_keys=[target_id])



# For example, a single census identifier like b01001001
class OBSColumn(Base):
    '''
    Describes the characteristics of data in a column, which can exist in
    multiple physical tables.

    These should *only* be instantiated to generate the return value of
    :meth:`.ColumnsTask.columns`.  Any other usage could have unexpected
    consequences.

    .. py:attribute:: id

       The unique identifier for this column.  Should be qualified by
       the module name of the class that created it.  Is automatically
       generated by :class:`~.util.ColumnsTask` if left unspecified,
       and automatically qualified by module name either way.

    .. py:attribute:: type

       The type of this column -- for example, ``Text``, ``Numeric``,
       ``Geometry``, etc.  This is used to generate a schema for any
       table using this column.

    .. py:attribute:: name

       The human-readable name of this column.  This is used in the
       catalog and API.

    .. py:attribute:: description

       The human-readable description of this column.  THis is
       used in the catalog.

    .. py:attribute:: weight

       A numeric weight for this column.  Higher weights are favored
       in certain rankings done by the API. Defaults to zero, which hides
       the column from catalog.

    .. py:attribute:: aggregate

       How this column can be aggregated.  For example,
       populations can be summed, so a population column should
       be ``sum``. Should be left blank if it is not to
       aggregate.

    .. py:attribute:: tables

       Iterable of all linked :class:`~.meta.OBSColumnTable`s, which could be
       traversed to find all tables with this column.

    .. py:attribute:: tags

       Iterable of all linked :class:`~.meta.OBSColumnTag`s, which could be
       traversed to find all tags applying to this column.

    .. py:attribute:: targets

       Dict of all related columns.  Format is ``<OBSColumn>: <reltype>``.

    .. py:attribute:: version

       A version control number, used to determine whether the column and its
       metadata should be updated.

    .. py:attribute:: extra

       Arbitrary additional information about this column stored as JSON.

    '''
    __tablename__ = 'obs_column'

    id = Column(Text, primary_key=True) # fully-qualified id like '"us.census.acs".b01001001'

    type = Column(Text, nullable=False) # postgres type, IE numeric, string, geometry, etc.
    name = Column(Text) # human-readable name to provide in bigmetadata

    description = Column(Text) # human-readable description to provide in
                                 # bigmetadata

    weight = Column(Numeric, default=0)
    aggregate = Column(Text) # what aggregate operation to use when adding
                               # these together across geoms: AVG, SUM etc.

    tables = relationship("OBSColumnTable", back_populates="column", cascade="all,delete")
    tags = association_proxy('column_column_tags', 'tag', creator=tag_creator)

    targets = association_proxy('tgts', 'reltype', creator=targets_creator)

    version = Column(Numeric, default=0, nullable=False)
    extra = Column(JSON)

    @property
    def sources(self):
        sources = {}
        session = current_session()
        for c2c in session.query(OBSColumnToColumn).filter_by(target_id=self.id):
            sources[c2c.source] = c2c.reltype
        return sources

    @property
    def index_type(self):
        if hasattr(self, '_index_type'):
            pass
        elif 'geom_ref' in self.targets.values():
            self._index_type = 'btree'
        elif self.type.lower() == 'geometry':
            self._index_type = 'gist'
        else:
            self._index_type = None

        return self._index_type

    def children(self):
        children = [col for col, reltype in self.sources.iteritems() if reltype == 'denominator']
        children.sort(key=lambda x: natural_sort_key(x.name))
        return children

    def has_children(self):
        '''
        Returns True if this column has children, False otherwise.
        '''
        return len(self.children()) > 0

    def has_denominators(self):
        '''
        Returns True if this column has no denominator, False otherwise.
        '''
        return 'denominator' in self.targets.values()

    def has_catalog_image(self):
        '''
        Returns True if this column has a pre-generated image for the catalog.
        '''
        return os.path.exists(os.path.join('catalog', 'img', self.id + '.png'))

    def denominators(self):
        '''
        Return the :class:`~.meta.OBSColumn` denominator of this column.
        '''
        if not self.has_denominators():
            return []
        return [k for k, v in self.targets.iteritems() if v == 'denominator']

    def unit(self):
        '''
        Return the :class:`~.meta.OBSTag` unit of this column.
        '''
        units = [tag for tag in self.tags if tag.type == 'unit']
        if units:
            return units[0]

    def summable(self):
        '''
        Returns True if we can sum this column and calculate for arbitrary
        areas.
        '''
        return self.aggregate == 'sum'


class OBSTable(Base):
    '''
    Describes a physical table in our database.

    These should *never* be instantiated manually.  They are automatically
    created by :meth:`~.util.TableTask.output`.  The unique key is
    :attr:~.meta.OBSTable.id:.

    .. py:attribute:: id

       The unique identifier for this table.  Is always qualified by
       the module name of the class that created it.

    .. py:attribute:: columns

       An iterable of all the :class:`~.meta.OBSColumnTable` instances
       contained in this table.

    .. py:attribute:: tablename

       The automatically generated name of this table, which can be used
       directly in select statements.  Of the format ``obs_<hash>``.

    .. py:attribute:: timespan

       A string containing information about the timespan this table applies
       to.  Obtained from :meth:`~.util.TableTask.timespan`.

    .. py:attribute:: the_geom

       A simple geometry approximating the boundaries of the data in this
       table.

    .. py:attribute:: description

       A description of the table.  Not used.

    .. py:attribute:: version

       A version control number, used to determine whether the table and its
       metadata should be updated.

    '''
    __tablename__ = 'obs_table'

    id = Column(Text, primary_key=True) # fully-qualified id like 'us.census.acs.extract_2013_5yr_state_18357fba'

    columns = relationship("OBSColumnTable", back_populates="table",
                           cascade="all,delete")

    tablename = Column(Text, nullable=False)
    timespan = Column(Text)
    the_geom = Column(Geometry)
    description = Column(Text)

    version = Column(Numeric, default=0, nullable=False)


class OBSTag(Base):
    '''
    Tags permit arbitrary groupings of columns.

    They should only be created as part of a :meth:`~.util.TagsTask.tags`
    implementation.

    .. py:attribute:: id

       The unique identifier for this table.  Is always qualified by
       the module name of the :class:`~.util.TagsTask` that created it, and
       should never be specified manually.

    .. py:attribute:: name

       The name of this tag.  This is exposed in the API and user interfaces.

    .. py:attribute:: type

       The type of this tag.  This is used to provide more information about
       what the tag means.  Examples are ``section``, ``subsection``,
       ``license``, ``unit``, although any arbitrary type can be defined.

    .. py:attribute:: description

       Description of this tag.  This may be exposed in the API and catalog.

    .. py:attribute:: columns

       An iterable of all the :class:`~.meta.OBSColumn`s tagged with this tag.

    .. py:attribute:: version

       A version control number, used to determine whether the tag and its
       metadata should be updated.

    '''
    __tablename__ = 'obs_tag'

    id = Column(Text, primary_key=True)

    name = Column(Text, nullable=False)
    type = Column(Text, nullable=False)
    description = Column(Text)

    columns = association_proxy('tag_column_tags', 'column')

    version = Column(Numeric, default=0, nullable=False)


class OBSColumnTag(Base):
    '''
    Glues together :class:`~.meta.OBSColumn` and :class:`~.meta.OBSTag`.
    If this object exists, the related column should be tagged with the related
    tag.

    Unique along ``(column_id, tag_id)``.

    These should *never* be created manually.  Their creation and removal
    is handled automatically as part of
    :meth:`util.TableTarget.update_or_create_metadata`.

    .. py:attribute:: column_id

       ID of the linked :class:`~.meta.OBSColumn`.

    .. py:attribute:: tag_id

       ID of the linked :class:`~.meta.OBSTag`.

    .. py:attribute:: column

       The linked :class:`~.meta.OBSColumn`.

    .. py:attribute:: tag

       The linked :class:`~.meta.OBSTag`.
    '''
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


class OBSDumpVersion(Base):
    '''
    This table contains one entry for every generated dump.  Automatically
    updated by :class:`~.util.Dump`.
    '''
    __tablename__ = 'obs_dump_version'

    dump_id = Column(Text, primary_key=True)


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
    '''
    Returns the session relevant to the currently operating :class:`Task`, if
    any.  Outside the context of a :class:`Task`, this can still be used for
    manual session management.
    '''
    return _current_session.get()


#@luigi.Task.event_handler(Event.SUCCESS)
def session_commit(task):
    '''
    commit the global session
    '''
    print 'commit {}'.format(task.task_id if task else '')
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
    print 'rollback {}: {}'.format(task.task_id if task else '', exception)
    _current_session.rollback()


def fromkeys(d, l):
    '''
    Similar to the builtin dict `fromkeys`, except remove keys with a value
    of None
    '''
    d = d.fromkeys(l)
    return dict((k, v) for k, v in d.iteritems() if v is not None)

DENOMINATOR = 'denominator'
GEOM_REF = 'geom_ref'
GEOM_NAME = 'geom_name'

_engine.execute('CREATE SCHEMA IF NOT EXISTS observatory')
Base.metadata.create_all()
