'''
meta.py

functions for persisting metadata about tables loaded via ETL
'''

import os
import re
import inspect
import functools

from luigi import Target

from sqlalchemy import (Column, Integer, Text, MetaData, Numeric, cast,
                        create_engine, event, ForeignKey, ForeignKeyConstraint,
                        exc, func, UniqueConstraint, Index)
from sqlalchemy.dialects.postgresql import JSON, DATERANGE
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import relationship, sessionmaker, backref
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.types import UserDefinedType

from lib.logger import get_logger
from logging import DEBUG
import asyncpg

LOGGER = get_logger(__name__)


class CurrentSession(object):

    def __init__(self, async_conn=False):
        self._session = None
        self._pid = None
        self._async_conn = False
        # https://docs.python.org/3/howto/logging.html#optimization
        self.debug_logging_enabled = LOGGER.isEnabledFor(DEBUG)

    def begin(self):
        if not self._session:
            self._session = sessionmaker(bind=get_engine())()
        self._pid = os.getpid()

    def get(self):
        # If we forked, there would be a PID mismatch and we need a new
        # connection
        if self._pid != os.getpid():
            self._session = None
            LOGGER.debug('FORKED: {} not {}'.format(self._pid, os.getpid()))
        if not self._session:
            self.begin()
        try:
            self._session.execute("SELECT 1")
            # Useful for debugging. This is called thousands of times
            if self.debug_logging_enabled:
                curframe = inspect.currentframe()
                calframe = inspect.getouterframes(curframe, 2)
                callers = ' <- '.join([c[3] for c in calframe[1:9]])
                # TODO: don't know why, `.debug` won't work at this point
                LOGGER.info('callers: {}'.format(callers))
        except:
            self._session = None
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


def natural_sort_key(s, _nsre=re.compile('([0-9]+)')):
    return [int(text) if text.isdigit() else text.lower()
            for text in re.split(_nsre, s)]


async def async_pool(user=None, password=None, host=None, port=None,
                     db=None, max_connections=150, timeout=60):
    user = user or os.environ.get('PGUSER', 'postgres')
    password = password or os.environ.get('PGPASSWORD', '')
    host = host or os.environ.get('PGHOST', 'localhost')
    port = port or os.environ.get('PGPORT', '5432')
    db = db or os.environ.get('PGDATABASE', 'postgres')
    db_pool = await asyncpg.create_pool(user=user, password=password,
                                        database=db, host=host,
                                        port=port,
                                        command_timeout=timeout,
                                        max_size=max_connections)
    return db_pool


def get_engine(user=None, password=None, host=None, port=None,
               db=None, readonly=False):

    engine = create_engine('postgres://{user}:{password}@{host}:{port}/{db}'.format(
        user=user or os.environ.get('PGUSER', 'postgres'),
        password=password or os.environ.get('PGPASSWORD', ''),
        host=host or os.environ.get('PGHOST', 'localhost'),
        port=port or os.environ.get('PGPORT', '5432'),
        db=db or os.environ.get('PGDATABASE', 'postgres')
    ))

    @event.listens_for(engine, 'begin')
    def receive_begin(conn):
        if readonly:
            conn.execute('SET TRANSACTION READ ONLY')

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


def catalog_latlng(column_id):
    # TODO we should do this from metadata instead of hardcoding
    if column_id == 'whosonfirst.wof_disputed_geom':
        return (76.57, 33.78)
    elif column_id == 'whosonfirst.wof_marinearea_geom':
        return (-68.47, 43.33)
    elif column_id in ('us.census.tiger.school_district_elementary',
                       'us.census.tiger.school_district_secondary',
                       'us.census.tiger.school_district_elementary_clipped',
                       'us.census.tiger.school_district_secondary_clipped'):
        return (40.7025, -73.7067)
    elif column_id.startswith('uk'):
        if 'WA' in column_id:
            return (51.468, -3.184)
        else:
            return (51.514, -0.0888)
    elif column_id.startswith('es'):
        return (42.822, -2.511)
    elif column_id.startswith('us.zillow'):
        return (28.330, -81.354)
    elif column_id.startswith('mx.'):
        return (19.413, -99.170)
    elif column_id.startswith('th.'):
        return (13.725, 100.492)
    # cols for French Guyana only
    elif column_id in ('fr.insee.P12_RP_CHOS', 'fr.insee.P12_RP_HABFOR'
                       , 'fr.insee.P12_RP_EAUCH', 'fr.insee.P12_RP_BDWC'
                       , 'fr.insee.P12_RP_MIDUR', 'fr.insee.P12_RP_CLIM'
                       , 'fr.insee.P12_RP_MIBOIS', 'fr.insee.P12_RP_CASE'
                       , 'fr.insee.P12_RP_TTEGOU', 'fr.insee.P12_RP_ELEC'
                       , 'fr.insee.P12_ACTOCC15P_ILT45D'
                       , 'fr.insee.P12_RP_CHOS', 'fr.insee.P12_RP_HABFOR'
                       , 'fr.insee.P12_RP_EAUCH', 'fr.insee.P12_RP_BDWC'
                       , 'fr.insee.P12_RP_MIDUR', 'fr.insee.P12_RP_CLIM'
                       , 'fr.insee.P12_RP_MIBOIS', 'fr.insee.P12_RP_CASE'
                       , 'fr.insee.P12_RP_TTEGOU', 'fr.insee.P12_RP_ELEC'
                       , 'fr.insee.P12_ACTOCC15P_ILT45D'):
        return (4.938, -52.329)
    elif column_id.startswith('fr.'):
        return (48.860, 2.361)
    elif column_id.startswith('ca.'):
        return (43.655, -79.379)
    elif column_id.startswith('us.census.'):
        return (40.7, -73.9)
    elif column_id.startswith('us.dma.'):
        return (40.7, -73.9)
    elif column_id.startswith('us.ihme.'):
        return (40.7, -73.9)
    elif column_id.startswith('us.bls.'):
        return (40.7, -73.9)
    elif column_id.startswith('us.qcew.'):
        return (40.7, -73.9)
    elif column_id.startswith('whosonfirst.'):
        return (40.7, -73.9)
    elif column_id.startswith('eu.'):
        return (52.522, 13.406)
    elif column_id.startswith('us.epa.'):
        return (40.7, -73.9)
    elif column_id.startswith('br.'):
        return (-43.19, -22.9)
    elif column_id.startswith('au.'):
        return (-33.880, 151.213)
    else:
        raise Exception('No catalog point set for {}'.format(column_id))

def _unique(session, cls, hashfunc, queryfunc, constructor, arg, kw):
    cache = getattr(session, '_unique_cache', None)
    if cache is None:
        session._unique_cache = cache = {}

    key = (cls, hashfunc(*arg))
    if key in cache:
        return cache[key]
    else:
        with session.no_autoflush:
            q = session.query(cls)
            q = queryfunc(q, *arg)
            obj = q.first()
            if not obj:
                obj = constructor(**kw)
                session.add(obj)
        cache[key] = obj
        return obj

class UniqueMixin(object):
    @classmethod
    def unique_hash(cls, *arg, **kw):
        raise NotImplementedError()

    @classmethod
    def unique_filter(cls, query, *arg, **kw):
        raise NotImplementedError()

    @classmethod
    def as_unique(cls, session, *arg, **kw):
        return _unique(
                    session,
                    cls,
                    cls.unique_hash,
                    cls.unique_filter,
                    cls,
                    arg, kw
               )


class Raster(UserDefinedType):

    def get_col_spec(self):
        return "Raster"

    def bind_expression(self, bindvalue):
        return cast(bindvalue, Raster)

    def column_expression(self, col):
        return cast(col, Raster)


class Geometry(UserDefinedType):

    def get_col_spec(self):
        return "GEOMETRY (GEOMETRY, 4326)"

    def bind_expression(self, bindvalue):
        return func.ST_GeomFromText(bindvalue, 4326, type_=self)

    def column_expression(self, col):
        return func.ST_AsText(col, type_=self)


class Linestring(UserDefinedType):

    def get_col_spec(self):
        return "GEOMETRY (LINESTRING, 4326)"

    def bind_expression(self, bindvalue):
        return func.ST_GeomFromText(bindvalue, 4326, type_=self)

    def column_expression(self, col):
        return func.ST_AsText(col, type_=self)


class Point(UserDefinedType):

    def get_col_spec(self):
        return "GEOMETRY (POINT, 4326)"

    def bind_expression(self, bindvalue):
        return func.ST_GeomFromText(bindvalue, 4326, type_=self)

    def column_expression(self, col):
        return func.ST_AsText(col, type_=self)


if os.environ.get('READTHEDOCS') == 'True':
    metadata = None
    Base = declarative_base()
else:
    metadata = MetaData(bind=get_engine(), schema='observatory')
    Base = declarative_base(metadata=metadata)


# A connection between a table and a column
class OBSColumnTable(Base):
    '''
    Glues together :class:`~.meta.OBSColumn` and :class:`~.meta.OBSTable`.
    If this object exists, the related column should exist in the related
    table, and can be selected with :attr:`~.OBSColumnTable.colname`.

    Unique along both ``(column_id, table_id)`` and ``(table_id, colname)``.

    These should *never* be created manually.  Their creation and removal
    is handled automatically as part of
    :meth:`targets.TableTarget.update_or_create_metadata`.

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

    #tiles = relationship("OBSColumnTableTile", back_populates='column_table',
    #                     cascade='all,delete')

    extra = Column(JSON)

    colname_constraint = UniqueConstraint(table_id, colname)


def tag_creator(tagtarget, session):
    if tagtarget is None:
        raise Exception('None passed to tagtarget')
    tag = tagtarget.get(session) or tagtarget._tag
    coltag = OBSColumnTag(tag=tag, tag_id=tag.id)
    if tag in session:
        session.expunge(tag)
    return coltag


def coltocoltargets_creator(coltarget_or_col, reltype):
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


def tabletotabletargets_creator(target, reltype):
    return OBSTableToTable(target=target, reltype=reltype)


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
                          lazy='subquery',
                          backref=backref(
                              "tgts",
                              collection_class=attribute_mapped_collection("target"),
                              cascade="all, delete-orphan",
                          ))
    target = relationship('OBSColumn', foreign_keys=[target_id])


class OBSTableToTable(Base):
    '''
    Relates one table to another.

    These should *never* be created manually.  Their creation should
    be handled automatically from specifying :attr:`.OBSTable.targets`.

    These are unique on ``(source_id, target_id)``.

    .. py:attribute:: source_id

       ID of the linked source :class:`~.meta.OBSTable`.

    .. py:attribute:: target_id

       ID of the linked target :class:`~.meta.OBSTable`.

    .. py:attribute:: reltype

       required text specifying the relation type.

    .. py:attribute:: source

       The linked source :class:`~.meta.OBSTable`.

    .. py:attribute:: target

       The linked target :class:`~.meta.OBSTable`.
    '''
    __tablename__ = 'obs_table_to_table'

    source_id = Column(Text, ForeignKey('obs_table.id', ondelete='cascade'), primary_key=True)
    target_id = Column(Text, ForeignKey('obs_table.id', ondelete='cascade'), primary_key=True)

    reltype = Column(Text, primary_key=True)

    source = relationship('OBSTable',
                          foreign_keys=[source_id],
                          backref=backref(
                              "tgts",
                              collection_class=attribute_mapped_collection("target"),
                              cascade="all, delete-orphan",
                          ))
    target = relationship('OBSTable', foreign_keys=[target_id])


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
       generated by :class:`~.tasks.ColumnsTask` if left unspecified,
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

    weight = Column(Numeric, default=1)
    aggregate = Column(Text) # what aggregate operation to use when adding
                               # these together across geoms: AVG, SUM etc.

    tables = relationship("OBSColumnTable", back_populates="column", cascade="all,delete")
    session = current_session()
    # Passing the same session object to the creator saves thousands
    # of `select 1` queries, because each use of the session was pinging
    tag_creator_with_session = functools.partial(tag_creator, session=session)
    tags = association_proxy('column_column_tags', 'tag', creator=tag_creator_with_session)

    targets = association_proxy('tgts', 'reltype', creator=coltocoltargets_creator)

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
        elif 'geom_ref' in list(self.targets.values()):
            self._index_type = 'btree'
        elif self.type.lower() == 'geometry':
            self._index_type = 'gist'
        else:
            self._index_type = None

        return self._index_type

    def children(self):
        children = [col for col, reltype in self.sources.items() if reltype == DENOMINATOR]
        children.sort(key=lambda x: natural_sort_key(x.name))
        return children

    def is_cartographic(self):
        '''
        Returns True if this column is a geometry that can be used for cartography.
        '''
        for tag in self.tags:
            if 'cartographic_boundary' in tag.id:
                return True
        return False

    def is_interpolation(self):
        '''
        Returns True if this column is a geometry that can be used for interpolation.
        '''
        for tag in self.tags:
            if 'interpolation_boundary' in tag.id:
                return True
        return False

    def is_geomref(self):
        '''
        Returns True if the column is a geomref, else Null
        '''
        return GEOM_REF in list(self.targets.values())

    def has_children(self):
        '''
        Returns True if this column has children, False otherwise.
        '''
        return len(self.children()) > 0

    def has_denominators(self):
        '''
        Returns True if this column has no denominator, False otherwise.
        '''
        return DENOMINATOR in list(self.targets.values())

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
        return [k for k, v in self.targets.items() if v == DENOMINATOR]

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

    def catalog_lonlat(self):
        '''
        Return tuple (longitude, latitude) for the catalog for this measurement.
        '''
        return catalog_lonlat(self.id)

    def geom_timespans(self):
        '''
        Return a dict of geom columns and timespans that this measure is
        available for.
        '''
        geom_timespans = {}
        for table in self.tables:
            table = table.table
            geomref_column = table.geomref_column()
            geom_column = [t for t, rt in geomref_column.targets.items()
                           if rt == GEOM_REF][0]
            if geom_column not in geom_timespans:
                geom_timespans[geom_column] = []
            geom_timespans[geom_column].append(table.timespan)
        return geom_timespans

    def source_tags(self):
        '''
        Return source tags.
        '''
        return [tag for tag in self.tags if tag.type.lower() == 'source']

    def license_tags(self):
        '''
        Return license tags.
        '''
        return [tag for tag in self.tags if tag.type.lower() == 'license']


class OBSTable(Base):
    '''
    Describes a physical table in our database.

    These should *never* be instantiated manually.  They are automatically
    created by :meth:`~.tasks.TableTask.output`.  The unique key is
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

       An OBSTimespan instance containing information about the timespan
       this table applies to.
       Obtained from :meth:`~.tasks.TableTask.timespan`.

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

    id = Column(Text, primary_key=True)  # fully-qualified id like 'us.census.acs.extract_2013_5yr_state_18357fba'

    columns = relationship("OBSColumnTable", back_populates="table",
                           cascade="all,delete")

    tablename = Column(Text, nullable=False)
    timespan = Column(Text, ForeignKey('obs_timespan.id'))
    table_timespan = relationship("OBSTimespan")

    the_geom = Column(Geometry)
    description = Column(Text)

    targets = association_proxy('tgts', 'reltype', creator=tabletotabletargets_creator)

    version = Column(Numeric, default=0, nullable=False)

    def geom_column(self):
        '''
        Return the column geometry column for this table, if it has one.

        Returns None if there is none.
        '''
        for col in self.columns:
            if lower(col.type) == 'geometry':
                return col

    def geomref_column(self):
        '''
        Return the geomref column for this table, if it has one.

        Returns None if there is none.
        '''
        for col in self.columns:
            col = col.column
            if col.is_geomref():
                return col


class OBSTag(Base):
    '''
    Tags permit arbitrary groupings of columns.

    They should only be created as part of a :meth:`~.tasks.TagsTask.tags`
    implementation.

    .. py:attribute:: id

       The unique identifier for this table.  Is always qualified by
       the module name of the :class:`~.tasks.TagsTask` that created it, and
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
    :meth:`targets.TableTarget.update_or_create_metadata`.

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


class OBSColumnTableTile(Base):
    '''
    This table contains column/table summary data using raster tiles.
    '''
    __tablename__ = 'obs_column_table_tile'

    table_id = Column(Text, primary_key=True)
    column_id = Column(Text, primary_key=True)
    tile_id = Column(Integer, primary_key=True)

    tile = Column(Raster)

    #column_table = relationship('OBSColumnTable', back_populates="tiles")

    constraint = ForeignKeyConstraint(
        ('table_id', 'column_id', ),
        ('obs_column_table.table_id', 'obs_column_table.column_id', ),
        ondelete='cascade'
    )


class OBSColumnTableTileSimple(Base):
    '''
    This table contains column/table summary data using raster tiles.
    '''
    __tablename__ = 'obs_column_table_tile_simple'

    table_id = Column(Text, primary_key=True)
    column_id = Column(Text, primary_key=True)
    tile_id = Column(Integer, primary_key=True)

    tile = Column(Raster)

    tct_constraint = UniqueConstraint(table_id, column_id, tile_id,
                                      name='obs_column_table_tile_simple_tct')
    cvxhull_restraint = Index('obs_column_table_simple_chtile',
                              func.ST_ConvexHull(tile),
                              postgresql_using='gist')


class OBSTimespan(UniqueMixin, Base):
    '''
    Describes a timespan table in our database.
    '''
    __tablename__ = 'obs_timespan'

    id = Column(Text, primary_key=True)  # fully-qualified id
    alias = Column(Text)  # alias for backwards compatibility
    name = Column(Text)  # human-readable name
    description = Column(Text)  # human-readable description
    timespan = Column(DATERANGE)
    weight = Column(Integer, default=0)

    @classmethod
    def unique_hash(cls, id):
        return id

    @classmethod
    def unique_filter(cls, query, id):
        return query.filter(OBSTimespan.id == id)

#@luigi.Task.event_handler(Event.SUCCESS)
def session_commit(task, session=_current_session):
    '''
    commit the global session
    '''
    LOGGER.info('commit {}'.format(task.task_id if task else ''))
    try:
        session.commit()
    except IntegrityError as iex:
        # if re.search(r'obs_timespan_pkey', iex.orig.args[0]):
        #     import ipdb; ipdb.set_trace()
        #     print("Avoided integrity for timespan entity: {}".format(iex.orig.args[0]))
        #     return
        LOGGER.warning(iex)
        raise
    except Exception as err:
        LOGGER.error(err)
        raise


#@luigi.Task.event_handler(Event.FAILURE)
def session_rollback(task, exception):
    '''
    rollback the global session
    '''
    LOGGER.info('rollback {}: {}'.format(task.task_id if task else '', exception))
    _current_session.rollback()


def fromkeys(d, l):
    '''
    Similar to the builtin dict `fromkeys`, except remove keys with a value
    of None
    '''
    d = d.fromkeys(l)
    return dict((k, v) for k, v in d.items() if v is not None)


DENOMINATOR = 'denominator'
UNIVERSE = 'universe'
GEOM_REF = 'geom_ref'
GEOM_NAME = 'geom_name'

if not os.environ.get('READTHEDOCS') == 'True':
    _engine = get_engine()
    _engine.execute('CREATE SCHEMA IF NOT EXISTS observatory')
    _engine.execute('''
        CREATE OR REPLACE FUNCTION public.first_agg ( anyelement, anyelement )
        RETURNS anyelement LANGUAGE SQL IMMUTABLE STRICT AS $$
                SELECT $1;
        $$;

        -- And then wrap an aggregate around it
        DROP AGGREGATE IF EXISTS public.FIRST (anyelement);
        CREATE AGGREGATE public.FIRST (
                sfunc    = public.first_agg,
                basetype = anyelement,
                stype    = anyelement
        );
    ''')
    Base.metadata.create_all()

class UpdatedMetaTarget(Target):
    def exists(self):
        session = current_session()

        # identify tags that have appeared, changed or disappeared
        # version shifts won't crop up in and of themselves, but name changes will
        changed_tags = '''
        with tags as (
            SELECT jsonb_each_text(numer_tags)
            FROM observatory.obs_meta_numer
            UNION ALL
            SELECT jsonb_each_text(denom_tags)
            FROM observatory.obs_meta_denom
            UNION ALL
            SELECT jsonb_each_text(geom_tags)
            FROM observatory.obs_meta_geom
        ),
        distinct_tags as (
            SELECT DISTINCT
            SPLIT_PART((jsonb_each_text).key, '/', 1) AS type,
            SPLIT_PART((jsonb_each_text).key, '/', 2) AS id,
            ((jsonb_each_text).value)::TEXT AS name
            FROM tags
        ),
        meta as (
            SELECT distinct t.id, t.type, t.name
            FROM observatory.obs_tag t
            JOIN observatory.obs_column_tag ct ON t.id = ct.tag_id
            JOIN observatory.obs_column c ON ct.column_id = c.id
            JOIN observatory.obs_column_table ctab ON c.id = ctab.column_id
        )
        SELECT meta.id, meta.type, meta.name,
                distinct_tags.id, distinct_tags.type, distinct_tags.name
        FROM distinct_tags FULL JOIN meta
        ON meta.id = distinct_tags.id AND
            meta.type = distinct_tags.type AND
            meta.name = distinct_tags.name
        WHERE meta.id IS NULL OR distinct_tags.id IS NULL
        '''
        resp = session.execute(changed_tags)
        if resp.fetchone():
            return False

        # identify columns that have appeared, changed, or disappeared
        changed_columns = '''
        WITH columns AS (
            SELECT DISTINCT c.id, c.version
            FROM observatory.obs_column c
                LEFT JOIN observatory.obs_column_to_column c2c ON c.id = c2c.source_id,
                observatory.obs_column_tag ct,
                observatory.obs_tag t,
                observatory.obs_column_table ctab
            WHERE c.id = ct.column_id
            AND ct.tag_id = t.id
            AND c.type NOT ILIKE 'geometry%'
            AND c.weight > 0
            AND (c2c.reltype != 'geom_ref' OR c2c.reltype IS NULL)
            AND c.id = ctab.column_id
        ),
        meta AS (
            SELECT numer_id, numer_version FROM observatory.obs_meta_numer
        )
        SELECT id, version, numer_id, numer_version
        FROM columns FULL JOIN meta
        ON id = numer_id AND version = numer_version
        WHERE numer_id IS NULL OR id IS NULL;
        '''
        resp = session.execute(changed_columns)
        if resp.fetchone():
            return False

        # identify tables that have appeared, changed, or disappeared
        changed_tables = '''
        with numer_tables as (
            select distinct tab.id, tab.version
            from observatory.obs_table tab,
                observatory.obs_column_table ctab,
                observatory.obs_column c
                    left join observatory.obs_column_to_column c2c on c.id = c2c.source_id,
                observatory.obs_column_tag ct,
                observatory.obs_tag t
                where c.id = ct.column_id
                    and ct.tag_id = t.id
                    and ctab.column_id = c.id
                    and ctab.table_id = tab.id
                    and c.type not ilike 'geometry%'
                    and c.weight > 0
                    and (c2c.reltype != 'geom_ref' or c2c.reltype is null)
        ),
        geom_tables as (
            select tab.id, tab.version,
                    rank() over (partition by c.id order by tab.timespan desc)
            from observatory.obs_table tab,
                observatory.obs_column_table ctab,
                observatory.obs_column c,
                observatory.obs_column_to_column c2c,
                observatory.obs_column_tag ct,
                observatory.obs_tag t,
                observatory.obs_column c2,
                observatory.obs_column_table ctab2,
                numer_tables
                where c.id = ct.column_id
                    and ct.tag_id = t.id
                    and ctab.column_id = c.id
                    and ctab.table_id = tab.id
                    and c.type ilike 'geometry%'
                    and c.weight > 0
                    and c2c.reltype = 'geom_ref'
                    and c.id = c2c.target_id
                    and c2.id = c2c.source_id
                    and c2.id = ctab2.column_id
                    and numer_tables.id = ctab2.table_id
            group by c.id, tab.id, tab.version
        ),
        numer_meta as (
            select distinct numer_tid, numer_t_version
            from observatory.obs_meta
        ),
        geom_meta as (
            select distinct geom_tid, geom_t_version
            from observatory.obs_meta
        )
        select distinct id, version, numer_tid, numer_t_version
        from numer_tables full join numer_meta on
            id = numer_tid and version = numer_t_version
        where numer_tid is null or id is null
        union all
        select distinct id, version, geom_tid, geom_t_version
        from geom_tables full join geom_meta on
            id = geom_tid and version = geom_t_version
        where rank = 1 and (geom_tid is null or id is null)
        ;
        '''
        resp = session.execute(changed_tables)
        if resp.fetchone():
            return False
        return True
