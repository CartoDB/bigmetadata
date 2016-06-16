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


class OBSColumnToColumn(Base):
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

    version = Column(Numeric, default=0, nullable=False)
    extra = Column(JSON)

    @property
    def sources(self):
        sources = {}
        session = current_session()
        for c2c in session.query(OBSColumnToColumn).filter_by(target_id=self.id):
            sources[c2c.source] = c2c.reltype
        return sources

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
        Return the denominator of this column.
        '''
        if not self.has_denominators():
            return []
        return [k for k, v in self.targets.iteritems() if v == 'denominator']

    def unit(self):
        '''
        Return the unit of this column
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


# We should have one of these for every table we load in through the ETL
class OBSTable(Base):
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

DENOMINATOR = 'denominator'
GEOM_REF = 'geom_ref'
GEOM_NAME = 'geom_name'

Base.metadata.create_all()

OBS_META_QUERY = '''
SELECT numer_c.id numer_id,
       denom_c.id denom_id,
       geom_c.id geom_id,
       MAX(numer_c.name) numer_name,
       MAX(denom_c.name) denom_name,
       MAX(geom_c.name) geom_name,
       MAX(numer_c.description) numer_description,
       MAX(denom_c.description) denom_description,
       MAX(geom_c.description) geom_description,
       MAX(numer_c.aggregate) numer_aggregate,
       MAX(denom_c.aggregate) denom_aggregate,
       MAX(geom_c.aggregate) geom_aggregate,
       MAX(numer_c.type) numer_type,
       MAX(denom_c.type) denom_type,
       MAX(geom_c.type) geom_type,
       MAX(numer_data_ct.colname) numer_colname,
       MAX(denom_data_ct.colname) denom_colname,
       MAX(geom_geom_ct.colname) geom_colname,
       MAX(numer_geomref_ct.colname) numer_geomref_colname,
       MAX(denom_geomref_ct.colname) denom_geomref_colname,
       MAX(geom_geomref_ct.colname) geom_geomref_colname,
       MAX(numer_t.tablename) numer_tablename,
       MAX(denom_t.tablename) denom_tablename,
       MAX(geom_t.tablename) geom_tablename,
       MAX(numer_t.timespan) numer_timespan,
       MAX(denom_t.timespan) denom_timespan,
       MAX(geom_t.timespan) geom_timespan,
       MAX(geom_t.the_geom) the_geom,
       ARRAY_AGG(DISTINCT s_tag.id) section_tags,
       ARRAY_AGG(DISTINCT ss_tag.id) subsection_tags,
       ARRAY_AGG(DISTINCT unit_tag.id) unit_tags
FROM obs_column_table numer_data_ct,
     obs_table numer_t, obs_column_table numer_geomref_ct,
     obs_column geomref_c, obs_column_to_column geomref_c2c,
     obs_column geom_c, obs_column_table geom_geom_ct,
     obs_column_table geom_geomref_ct,
     obs_table geom_t,
     obs_column_tag ss_ctag, obs_tag ss_tag,
     obs_column_tag s_ctag, obs_tag s_tag,
     obs_column numer_c
  LEFT JOIN (
    obs_column_to_column denom_c2c
    JOIN obs_column denom_c ON denom_c2c.target_id = denom_c.id
    JOIN obs_column_table denom_data_ct ON denom_data_ct.column_id = denom_c.id
    JOIN obs_table denom_t ON denom_data_ct.table_id = denom_t.id
    JOIN obs_column_table denom_geomref_ct ON denom_geomref_ct.table_id = denom_t.id
  ) ON denom_c2c.source_id = numer_c.id
  LEFT JOIN (
     obs_column_tag unit_ctag
     JOIN obs_tag unit_tag ON unit_tag.id = unit_ctag.tag_id
  ) ON numer_c.id = unit_ctag.column_id
WHERE numer_c.id = numer_data_ct.column_id
  AND numer_data_ct.table_id = numer_t.id
  AND numer_t.id = numer_geomref_ct.table_id
  AND numer_geomref_ct.column_id = geomref_c.id
  AND geomref_c2c.reltype = 'geom_ref'
  AND geomref_c.id = geomref_c2c.source_id
  AND geom_c.id = geomref_c2c.target_id
  AND geom_geomref_ct.column_id = geomref_c.id
  AND geom_geomref_ct.table_id = geom_t.id
  AND geom_geom_ct.column_id = geom_c.id
  AND geom_geom_ct.table_id = geom_t.id
  AND geom_c.type ILIKE 'geometry'
  AND numer_c.type NOT ILIKE 'geometry'
  AND numer_t.id != geom_t.id
  AND numer_c.id != geomref_c.id
  AND (unit_tag.type = 'unit' OR unit_tag.type IS NULL)
  AND ss_tag.type = 'subsection'
  AND s_tag.type = 'section'
  AND unit_ctag.column_id = numer_c.id
  AND unit_ctag.tag_id = unit_tag.id
  AND ss_ctag.column_id = numer_c.id
  AND ss_ctag.tag_id = ss_tag.id
  AND s_ctag.column_id = numer_c.id
  AND s_ctag.tag_id = s_tag.id
  AND (denom_c2c.reltype = 'denominator' OR denom_c2c.reltype IS NULL)
  AND (denom_geomref_ct.column_id = geomref_c.id OR denom_geomref_ct.column_id IS NULL)
  AND (denom_t.timespan = numer_t.timespan OR denom_t.timespan IS NULL)
GROUP BY numer_c.id, denom_c.id, geom_c.id,
         numer_t.id, denom_t.id, geom_t.id
'''
