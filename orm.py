#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Implements the Active Domain Object pattern
extended with SQLAlchemy database reflection.
"""


import sys
import collections
from logging import getLogger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import MetaData, or_, and_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.automap import automap_base, generate_relationship
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Query, interfaces
from sqlalchemy.util import KeyedTuple


__author__ = 'fgiuba'
__email__ = 'federico.giuba@copangroup.com'
__all__ = [
    'automap',
    'CScopedSessionPool',
    'CAutomappingActiveDomainObject',
    'CAutomappingMetaClass',
    'CAutomappingBase',
    'to_list_of_dict',
]


class CScopedSessionPool(object):
    """
    Manages the session pool and related engines and sessionmakers.
    All sessions produced are scoped_session: the factory (sessionmaker)
    always returns the same session instance until you remove it
    from the sessions registry objects with get_sessionmaker('engine_name').remove().
    """

    _default_engine = None
    engines = dict()
    _sessionmakers = dict()

    def new_engine(self, engine_name, url, set_as_default=False, **kwargs):
        """
        Creates new engine and adds it to the session pool.
        :param engine_name: identifier of the new engine
        :param url: URL of the databate
        :param set_as_default: if True, set the engine as default
        :param kwargs: kwargs for create_engine()
        """
        echo = kwargs.pop('echo', True)
        engine = create_engine(url, echo=echo, **kwargs)
        self.add_engine(engine_name, engine, set_as_default)

    def add_engine(self, engine_name, engine, set_as_default=False):
        """
        Adds engine to the session pool.
        :param engine_name: identifier of the new engine
        :param engine: engine to add
        :param set_as_default: if True, se the engine as default
        """
        if engine_name in self.engines:
            raise Exception(u"Engine {0} already registered".format(engine_name))
        self.engines[engine_name] = engine
        if not self._default_engine or set_as_default:
            self._default_engine = engine_name
        self._sessionmakers[engine_name] = scoped_session(sessionmaker(
            bind=engine,
            autoflush=False,
        ))

    def get_engine(self, engine_name):
        """
        Return the engine associated to engine_name.
        :param engine_name: engine identifier
        :return: engine
        :rtype: sqlalchemy.engine.Engine
        """
        self._check_engine(engine_name)
        return self.engines[engine_name]

    def set_default_engine(self, engine_name):
        """
        Set an engine as default.
        :param engine_name: engine identifier
        """
        self._check_engine(engine_name)
        self._default_engine = engine_name

    def get_default_engine(self):
        """
        Return the default engine.
        :return: default engine
        :rtype: sqlalchemy.engine.Engine
        """
        return self._default_engine

    def get_session(self, engine_name=None):
        """
        Return the session associated to the engine.
        :param engine_name: engine identifier
        :return: scoped session
        :rtype: sqlalchemy.orm.session.Session
        """
        if not engine_name:
            self._check_default_engine()
            engine_name = self._default_engine
        self._check_engine(engine_name)
        session = self._sessionmakers[engine_name]()
        return session

    def get_sessionmaker(self, engine_name=None):
        """
        Return the session factory associated to the engine.
        :param engine_name: engine identifier
        :return: current sessionmaker
        :rtype: sqlalchemy.orm.session.sessionmakerX
        """
        if engine_name is None:
            self._check_default_engine()
            return self._sessionmakers[self._default_engine]
        else:
            self._check_engine(engine_name)
            return self._sessionmakers[engine_name]

    def commit(self, engine_name):
        """
        Commit changes in the session identified by engine_name to the DB.
        :param engine_name: engine identifier
        """
        session = self.get_session(engine_name)
        try:
            session.commit()
        except SQLAlchemyError:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            try:
                session.rollback()
            except SQLAlchemyError:
                pass
            raise exc_type, exc_value, exc_traceback

    def flush(self, engine_name):
        """
        Flush changes in the session identified by engine_name to the DB.
        :param engine_name: engine identifier
        """
        try:
            self.get_session(engine_name).flush()
        except SQLAlchemyError:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            raise exc_type, exc_value, exc_traceback

    def rollback(self, engine_name):
        session = self.get_session(engine_name)
        try:
            session.rollback()
        except SQLAlchemyError:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            raise exc_type, exc_value, exc_traceback

    def _check_engine(self, engine_name):
        """
        Check if engine_name exists.
        :param engine_name: engine identifier
        """
        if engine_name not in self.engines:
            raise Exception(u"Engine {0} not registered".format(engine_name))

    def _check_default_engine(self):
        """
        Check if a default engine is defined.
        """
        if not self._default_engine:
            raise Exception(u"No default engine available")


SessionPool = CScopedSessionPool()


class CAutomappingActiveDomainObject(object):
    """
    Represents a mapped table and manages interactions with DB
    """

    __enginename__ = None
    _logger = getLogger('RastServer')  # TODO: fix logging

    @classmethod
    def query(cls, *entities, **kwargs):
        """
        Performs a simple query retrieving the session from the specified session pool.
        @param entities: other tables to query (CAutomappingActiveDomainObject)
        @param kwargs: passed to query()
        @return: resulting Query object or None if the query result is empty
        @rtype: sqlalchemy.orm.query.Query or None
        """
        session = cls._get_session()
        return session.query(cls, *entities, **kwargs)

    @classmethod
    def load(cls, **kwargs):
        """
        Return query result filtered by conditions in kwargs.
        Uses Query.filter_by() method.
        kwargs must be like: attribute_name='value'
        Conditions are combined with AND operator.
        @param kwargs: condition on attributes
        @return: filtered Query object
        @rtype: sqlalchemy.orm.query.Query
        """
        session = cls._get_session()
        return session.query(cls).filter_by(**kwargs)

    @classmethod
    def load_and(cls, **kwargs):
        """
        Return query result filtered by conditions in kwargs.
        Implements and_() method.
        kwargs must belike: attribute_name='value'
        Conditions are combined with AND operator.
        @param kwargs: condition on attributes
        @return: filtered Query object
        @rtype: sqlalchemy.orm.query.Query
        """
        session = cls._get_session()
        conditions = []
        for (column_name, values) in kwargs.iteritems():
            column = getattr(cls, column_name)
            if isinstance(values, basestring):
                values = [values, ]
            conditions.append(column.in_(values))
        instances = session.query(cls).filter(and_(*conditions))
        return instances

    @classmethod
    def load_or(cls, **kwargs):
        """
        Return query result filtered by conditions in kwargs.
        Implements or_() method.
        kwargs must be like: attribute_name='value'
        Conditions are combined with OR operator.
        @param kwargs: condition on attributes
        @return: filtered Query object
        @rtype: sqlalchemy.orm.query.Query
        """
        session = cls._get_session()
        conditions = []
        for (column_name, values) in kwargs.iteritems():
            column = getattr(cls, column_name)
            if isinstance(values, basestring):
                values = [values, ]
            conditions.append(column.in_(values))
        instances = session.query(cls).filter(or_(*conditions))
        return instances

    @classmethod
    def load_pk(cls, key):
        """
        Return an instance based on the given primary key identifier, or None if not found.
        get() provides direct access to the identity map of the owning Session.
        If the given primary key identifier is present in the local identity map,
        the object is returned directly from this collection and no SQL is emitted,
        unless the object has been marked fully expired.
        If not present, a SELECT is performed in order to locate the object.
        @param key: primary key
        @return: instance identified by primary key
        @rtype: sqlalchemy.orm.query.Query
        """
        if key is None:
            return None
        session = cls._get_session()
        return session.query(cls).get(key)

    @classmethod
    def first(cls, **kwargs):
        """
        Return the first query result.
        Conditions on attributes can be passed with kwargs.
        @param kwargs: condition on attributes
        @return: instance as result of the query
        """
        return cls.load(**kwargs).first()

    @classmethod
    def count(cls, **kwargs):
        """
        Return the query result cardinality.
        Conditions on attributes can be passed with kwargs.
        @param kwargs: condition on attributes
        @return: number of tuples in query result
        @rtype: int
        """
        return cls.load(**kwargs).count()

    def save(self):
        """
        Places instance in the Session.
        """
        session = self._get_session()
        session.add(self)

    @classmethod
    def save_by(cls, **kwargs):
        """
        Creates new instance from kwargs and adds it to the session.
        Its state will be persisted to the database on the next flush operation.
        Repeated calls to add() will be ignored.
        @param kwargs: arguments for creating new instance
        """
        session = cls._get_session()
        session.add(cls(**kwargs))

    @classmethod
    def save_all(cls, items):
        """
        Places items in the Session.
        Their states will be persisted to the database on the next flush operation.
        Repeated calls to add() will be ignored.
        Items can be both dict or CAutomappingActiveDomainObject
        @param items: list of instances to be added
        """
        if not isinstance(items, collections.Iterable):
            items = [items, ]
        for item in items:
            if isinstance(item, dict):
                cls.save_by(**item)
            elif isinstance(item, cls):
                item.save()
            else:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                raise exc_type, exc_value, exc_traceback

    def merge(self, load=True):
        """
        Transfers state from an outside object into a new
        or already existing instance within a session.
        @param load: check also database for primary key
        """
        session = self._get_session()
        session.merge(self, load=load)

    @classmethod
    def merge_by(cls, load=True, **kwargs):
        """
        Creates new instance from kwargs and merges it to the session.
        Its state will be persisted to the database on the next flush operation.
        Repeated calls to add() will be ignored.
        @param load: check also database for primary key
        @param kwargs: arguments for creating new instance
        """
        session = cls._get_session()
        session.merge(cls(**kwargs), load=load)

    @classmethod
    def merge_all(cls, items, load=True):
        """
        Transfers state from an outside object into a new
        or already existing instance within a session.
        @param items: items to be merged
        @param load: check also database for primary key
        """
        if not isinstance(items, collections.Iterable):
            items = [items, ]
        for item in items:
            if isinstance(item, dict):
                cls.merge_by(load, **item)
            elif isinstance(item, cls):
                item.merge(load)
            else:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                raise exc_type, exc_value, exc_traceback

    def delete(self):
        """
        Delete instance.
        """
        session = self._get_session()
        session.delete(self)

    @classmethod
    def delete_by(cls, **kwargs):
        """
        Query for items with condition expressed in kwargs,
        then mark those items as deleted.
        @param kwargs: query conditions
        """
        instances = cls.load(**kwargs)
        cls.delete_all(instances)

    @classmethod
    def delete_all(cls, items):
        """
        Places one or more instances into the Session’s list of objects to be marked as deleted.
        @param items: items to be deleted
        """
        if not isinstance(items, collections.Iterable):
            items = [items, ]
        session = cls._get_session()
        for item in items:
            session.delete(item)

    @classmethod
    def inspect(cls):
        """
        Return class inspect info.
        @return: inspect info
        @rtype: sqlalchemy.orm.mapper.Mapper
        """
        return inspect(cls)

    @classmethod
    def flush(cls):
        """
        Flush current session.
        """
        session = cls._get_session()
        try:
            session.flush()
        except SQLAlchemyError:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            raise exc_type, exc_value, exc_traceback

    @classmethod
    def commit(cls):
        """
        Commit current session.
        """
        session = cls._get_session()
        try:
            session.commit()
        except SQLAlchemyError:
            session.rollback()
            exc_type, exc_value, exc_traceback = sys.exc_info()
            raise exc_type, exc_value, exc_traceback
            # try:
            #     pass
            # except SQLAlchemyError:
            #     raise exc_type, exc_value, exc_traceback

    @classmethod
    def _get_session(cls):
        """
        Return session from default session pool.
        @return: session
        @rtype: sqlalchemy.orm.session.Session
        """
        engine_name = cls.__enginename__
        if not engine_name:
            engine_name = SessionPool.get_default_engine()
        session = SessionPool.get_session(engine_name)
        return session

    @classmethod
    def inspect(cls):
        """
        Return class inspect info.
        @return: inspect info
        @rtype: sqlalchemy.orm.mapper.Mapper
        """
        return inspect(cls)

    def __iter__(self, with_prefix=False):
        """
        Convert CAutomappingActiveDomainObject to dict.
        WARNING: this method loads all table columns from DB,
        so DO NOT USE TOGETHER WITH 'load_only' or with any other lazy loading approaches
        beacuse it will affect query performances.
        @return: dictionary with mapped attributes
        @rtype: dict
        """
        prefix = '{}.'.format(str(self.__table__.name)) if with_prefix else ''
        for column in self.__table__.columns:
            value = getattr(self, column.name)
            value = str(value) if isinstance(value, unicode) else value
            yield (prefix + str(column.name), value)

    def __str__(self):
        return str(dict(self))

    def __call__(self):
        """
        Inspect the object and print some info
        (table name, attributes with values, relationships)
        """
        string = 'TABLE: {}\n'.format(self.__table__)
        for i, tup in enumerate(self):
            string += '├─ ' if i < len(dict(self))-1 else '└─ '
            string += '{}: {} {}\n'.format(tup[0], tup[1], type(tup[1]))
        string += 'RELATIONSHIPS: {}\n'.format(self.__table__)
        mapper = self.inspect().relationships
        for i, rel in enumerate(mapper):
            string += '├─ ' if i < len(dict(mapper))-1 else '└─ '
            string += '{}, {}\n'.format(rel, rel.direction.name)
        print string


def to_list_of_dict(fn):
    """
    Decorator. Convert Query objects into list of dicts.
    """
    def decorator(*args, **kwargs):
        query = fn(*args, **kwargs)
        if not len([q for q in query]) or not isinstance(query, Query):
            # no elements or invalid
            return None
        elif isinstance(query[0], KeyedTuple):
            # list of tuples of caado (from joined query)
            res = []
            for tup in query:
                merged_dict = {}
                for caado in tup:
                    # here we merge attributes from different tables into one dict
                    # so, to avoid duplicate names, we use the name of the table
                    # as prefix for attribute names (i.e. 'table_name.attribute.name')
                    for k, v in caado.__iter__(with_prefix=True):
                        merged_dict[k] = v
                res.append(merged_dict)
            return res
        else:
            # simple query object
            return [dict(caado) for caado in query]
    return decorator


class CAutomappingMetaClass(type):

    """
    Metaclass that implements the automapping schema.
    Produce a class object (not an instance) representing a DB mapped table.
    """

    def __new__(mcs, name, bases, namespace):
        """
        Maps a db table into an active domain class object.
        @param name: name of the class to be
        @param bases: base classes (tuple)
        @param namespace: dict with attributes declared in the class to be
        @return: mapped class
        """
        Base = bases[1].__baseclass__
        engine_name = bases[1].__enginename__
        mapped_klass = getattr(Base.classes, namespace['__tablename__'])
        for key, value in namespace.iteritems():
            if key not in ['__tablename__']:
                setattr(mapped_klass, key, value)
        for key, value in bases[0].__dict__.iteritems():
            if key not in ['__dict__', '__enginename__']:
                setattr(mapped_klass, key, value)
        setattr(mapped_klass, '__enginename__', engine_name)
        mapped_klass.__name__ = name
        return mapped_klass


class CAutomappingBase(object):

    def __init__(self, engine_name):
        self.engine = SessionPool.get_engine(engine_name)

    def reflect(self, only=None):
        metadata = MetaData()
        metadata.reflect(self.engine, only=only)
        Base = automap_base(metadata=metadata)
        Base.prepare(
            classname_for_table=classname_for_table,
            name_for_scalar_relationship=name_for_scalar_relationship,
            name_for_collection_relationship=name_for_collection_relationship,
            generate_relationship=_gen_relationship,
        )
        return Base


def _gen_relationship(base, direction, return_fn, attrname, local_cls, referred_cls, **kwargs):
    """
    Maps relationship between tables.
    This override force the ORM to follow the Postgres ON DELETE behavior:
    ORM policy will be reflected from the database schema level.
    """
    if direction is interfaces.ONETOMANY:
        kwargs['cascade'] = 'all, delete-orphan'  # delete orphan element in current session
        kwargs['passive_deletes'] = True  # follow postgres policy for ON DELETE events
    return generate_relationship(base, direction, return_fn, attrname, local_cls, referred_cls, **kwargs)


def classname_for_table(base, tablename, table):
    """
    Return the class name that should be used, given the name of a table.
    Change it if you don't like default SQLAlchemy automatic naming scheme.
    @return: string name for table
    @rtype: basestring
    """
    return str(tablename)


def name_for_scalar_relationship(base, local_cls, referred_cls, constraint):
    """
    Return the attribute name that should be used to refer
    from one class to another, for a scalar object reference.
    Change it if you don't like default SQLAlchemy automatic naming scheme.
    @return: string name for scalar relationship
    @rtype: basestring
    """
    name = referred_cls.__name__.lower() + "_ref"
    return name


def name_for_collection_relationship(base, local_cls, referred_cls, constraint):
    """
    Return the attribute name that should be used to refer
    from one class to another, for a collection reference.
    Change it if you don't like default SQLAlchemy automatic naming scheme.
    @return: string name for collection relationship
    @rtype: basestring
    """
    return referred_cls.__name__.lower() + "_col"


def automap(db_name, endpoint, **kwargs):

    only = kwargs.pop('only', None)
    echo = kwargs.pop('echo', False)

    # create the engine if necessary
    if db_name not in SessionPool.engines:
        SessionPool.new_engine(db_name, endpoint, echo=echo)

    # sqlalchemy reflection
    Base = CAutomappingBase(db_name).reflect()

    tables = {}
    for Table in Base.classes:
        table_name = Table.__name__
        if only is None or table_name in only:
            namespace = {
                '__enginename__': db_name,
                '__tablename__': table_name,
            }
            for key, value in namespace.iteritems():
                if key not in ['__tablename__']:
                    setattr(Table, key, value)
            for key, value in CAutomappingActiveDomainObject.__dict__.iteritems():
                if key not in ['__dict__', '__enginename__', '__tablename__']:
                    setattr(Table, key, value)
            tables[table_name] = Table
    return tables


if __name__ == '__main__':
    pass
