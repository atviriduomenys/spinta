from __future__ import annotations

import dataclasses
import datetime
import re
from typing import Any

import pymongo

from spinta import exceptions
from spinta.auth import authorized
from spinta.backends.mongo.ufuncs.components import MongoQueryBuilder, Recurse, Lower, Negative, Positive
from spinta.components import Property, Page
from spinta.core.enums import Action
from spinta.core.ufuncs import Bind
from spinta.core.ufuncs import Expr
from spinta.core.ufuncs import ufunc
from spinta.exceptions import EmptyStringSearch
from spinta.exceptions import FieldNotInResource
from spinta.types.datatype import Array
from spinta.types.datatype import DataType
from spinta.types.datatype import Date
from spinta.types.datatype import DateTime
from spinta.types.datatype import Integer
from spinta.types.datatype import Number
from spinta.types.datatype import Object
from spinta.types.datatype import PrimaryKey
from spinta.types.datatype import String
from spinta.ufuncs.querybuilder.ufuncs import Star
from spinta.utils.data import take


@ufunc.resolver(MongoQueryBuilder, str)
def op(env, arg: str):
    if arg == '*':
        return Star()
    else:
        raise NotImplementedError


@ufunc.resolver(MongoQueryBuilder, Bind, Bind, name='getattr')
def getattr_(env, field, attr):
    if field.name in env.model.properties:
        prop = env.model.properties[field.name]
    else:
        raise FieldNotInResource(env.model, property=field.anem)
    return env.call('getattr', prop.dtype, attr)


@ufunc.resolver(MongoQueryBuilder, Object, Bind)
def getattr(env, dtype, attr):
    if attr.name in dtype.properties:
        return dtype.properties[attr.name].dtype
    else:
        raise FieldNotInResource(dtype, property=attr.name)


@ufunc.resolver(MongoQueryBuilder, Array, Bind)
def getattr(env, dtype, attr):
    return env.call('getattr', dtype.items.dtype, attr)


@ufunc.resolver(MongoQueryBuilder, Expr)
def select(env, expr):
    keys = [str(k) for k in expr.args]
    args, kwargs = expr.resolve(env)
    args = list(zip(keys, args)) + list(kwargs.items())

    env.select = {}

    if args:
        for key, arg in args:
            selected = env.call('select', arg)
            if selected is not None:
                env.select[key] = selected
    else:
        env.call('select', Star())

    if not (len(args) == 1 and args[0][0] == '_page'):
        assert env.select, args


@ufunc.resolver(MongoQueryBuilder, Star)
def select(env, arg: Star) -> None:
    for prop in take(env.model.properties).values():
        # if authorized(env.context, prop, (Action.GETALL, Action.SEARCH)):
        # TODO: This line above should come from a getall(request),
        #       because getall can be used internally for example for
        #       writes.
        env.select[prop.place] = env.call('select', prop.dtype)


@ufunc.resolver(MongoQueryBuilder, Bind)
def select(env, field):
    if field.name == '_page':
        return None

    prop = env.resolve_property(field)
    if authorized(env.context, prop, Action.SEARCH):
        return env.call('select', prop.dtype)
    else:
        raise FieldNotInResource(env.model, property=field.name)


@dataclasses.dataclass
class Selected:
    item: Any
    prop: Property = None


@ufunc.resolver(MongoQueryBuilder, DataType)
def select(env, dtype):
    table = env.backend.get_table(env.model)
    column = env.backend.get_column(table, dtype.prop, select=True)
    return Selected(column, dtype.prop)


@ufunc.resolver(MongoQueryBuilder, Page)
def select(env, page):
    return_selected = []
    for item in page.by.values():
        selected = env.call('select', item.prop.dtype)
        return_selected.append(selected)
    return return_selected


@ufunc.resolver(MongoQueryBuilder, int)
def limit(env, n):
    env.limit = n


@ufunc.resolver(MongoQueryBuilder, int)
def offset(env, n):
    env.offset = n


@ufunc.resolver(MongoQueryBuilder, Expr, name='and')
def and_(env, expr):
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    return env.call('and', args)


@ufunc.resolver(MongoQueryBuilder, list, name='and')
def and_(env, args):
    if len(args) > 1:
        return {'$and': args}
    elif args:
        return args[0]


@ufunc.resolver(MongoQueryBuilder, Expr, name='or')
def or_(env, expr):
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    return env.call('or', args)


@ufunc.resolver(MongoQueryBuilder, list, name='or')
def or_(env, args):
    if len(args) > 1:
        return {'$or': args}
    elif args:
        return args[0]


COMPARE = [
    'eq',
    'ne',
    'lt',
    'le',
    'gt',
    'ge',
    'startswith',
    'contains',
]


@ufunc.resolver(MongoQueryBuilder, DataType, object, names=[
    'eq', 'lt', 'le', 'gt', 'ge', 'contains', 'startswith',
])
def compare(env, op, dtype, value):
    raise exceptions.InvalidValue(dtype, op=op, value=type(value))


@ufunc.resolver(MongoQueryBuilder, PrimaryKey, (object, type(None)), names=[
    'eq', 'lt', 'le', 'gt', 'ge',
])
def compare(env, op, dtype, value):
    return _mongo_compare(op, '__id', value)


@ufunc.resolver(MongoQueryBuilder, DataType, type(None))
def eq(env, dtype, value):
    return {dtype.prop.place: None}


@ufunc.resolver(MongoQueryBuilder, String, (str, re.Pattern))
def eq(env, dtype, value):
    return _mongo_compare('eq', dtype.prop.place, value)


@ufunc.resolver(MongoQueryBuilder, (Integer, Number), (int, float), names=[
    'eq', 'lt', 'le', 'gt', 'ge',
])
def compare(env, op, dtype, value):
    return _mongo_compare(op, dtype.prop.place, value)


@ufunc.resolver(MongoQueryBuilder, DateTime, str, names=[
    'eq', 'lt', 'le', 'gt', 'ge',
])
def compare(env, op, dtype, value):
    value = datetime.datetime.fromisoformat(value)
    return _mongo_compare(op, dtype.prop.place, value)


@ufunc.resolver(MongoQueryBuilder, Date, str, names=[
    'eq', 'lt', 'le', 'gt', 'ge',
])
def compare(env, op, dtype, value):
    value = datetime.date.fromisoformat(value)
    value = datetime.datetime.combine(value, datetime.datetime.min.time())
    return _mongo_compare(op, dtype.prop.place, value)


MONGO_OPERATORS = {
    'lt': '$lt',
    'le': '$lte',
    'gt': '$gt',
    'ge': '$gte',
}


def _mongo_compare(op, column, value):
    if op == 'eq':
        return {column: value}
    else:
        return {column: {MONGO_OPERATORS[op]: value}}


@ufunc.resolver(MongoQueryBuilder, Lower, str)
def eq(env, fn, value):
    value = re.escape(value)
    value = re.compile(f'^{value}$', re.IGNORECASE)
    return env.call('eq', fn.dtype, value)


@ufunc.resolver(MongoQueryBuilder, DataType, object)
def ne(env, dtype, value):
    key = dtype.prop.place
    return {
        '$and': [
            {key: {'$ne': value, '$exists': True}},
            {key: {'$ne': None, '$exists': True}},
        ]
    }


@ufunc.resolver(MongoQueryBuilder, PrimaryKey, object)
def ne(env, dtype, value):
    return {
        '$and': [
            {'__id': {'$ne': value, '$exists': True}},
            {'__id': {'$ne': None, '$exists': True}},
        ]
    }


@ufunc.resolver(MongoQueryBuilder, DateTime, str)
def ne(env, dtype, value):
    value = datetime.datetime.fromisoformat(value)
    return env.call('ne', dtype, value)


@ufunc.resolver(MongoQueryBuilder, Date, str)
def ne(env, dtype, value):
    value = datetime.date.fromisoformat(value)
    value = datetime.datetime.combine(value, datetime.datetime.min.time())
    return env.call('ne', dtype, value)


@ufunc.resolver(MongoQueryBuilder, Lower, str)
def ne(env, fn, value):
    key = fn.dtype.prop.place
    value = re.escape(value)
    value = re.compile(f'^{value}$', re.IGNORECASE)
    return {
        '$and': [
            {key: {'$not': value, '$exists': True}},
            {key: {'$ne': None, '$exists': True}},
        ]
    }


def _ensure_non_empty(op, s):
    if s == '':
        raise EmptyStringSearch(op=op)


@ufunc.resolver(MongoQueryBuilder, String, str)
def contains(env, dtype, value):
    _ensure_non_empty('contains', value)
    value = re.escape(value)
    value = re.compile(value)
    return {dtype.prop.place: value}


@ufunc.resolver(MongoQueryBuilder, Array, (object, type(None)), names=[
    'eq', 'ne', 'lt', 'le', 'gt', 'ge', 'contains', 'startswith',
])
def compare(env, op, dtype, value):
    return env.call(op, dtype.items.dtype, value)


@ufunc.resolver(MongoQueryBuilder, Lower, str)
def contains(env, fn, value):
    _ensure_non_empty('contains', value)
    value = re.escape(value)
    value = re.compile(value, re.IGNORECASE)
    return {fn.dtype.prop.place: value}


@ufunc.resolver(MongoQueryBuilder, PrimaryKey, str)
def contains(env, dtype, value):
    _ensure_non_empty('contains', value)
    value = re.escape(value)
    value = re.compile(value)
    return {'__id': value}


@ufunc.resolver(MongoQueryBuilder, String, str)
def startswith(env, dtype, value):
    _ensure_non_empty('startswith', value)
    value = re.escape(value)
    value = re.compile('^' + value)
    return {dtype.prop.place: value}


@ufunc.resolver(MongoQueryBuilder, Lower, str)
def startswith(env, fn, value):
    _ensure_non_empty('startswith', value)
    value = re.escape(value)
    value = re.compile('^' + value, re.IGNORECASE)
    return {fn.dtype.prop.place: value}


@ufunc.resolver(MongoQueryBuilder, PrimaryKey, str)
def startswith(env, dtype, value):
    _ensure_non_empty('startswith', value)
    value = re.escape(value)
    value = re.compile('^' + value)
    return {'__id': value}


FUNCS = [
    'lower',
    'upper',
    'recurse',
]


@ufunc.resolver(MongoQueryBuilder, Bind, names=FUNCS)
def func(env, name, field):
    prop = env.resolve_property(field)
    return env.call(name, prop.dtype)


@ufunc.resolver(MongoQueryBuilder, String)
def lower(env, dtype):
    return Lower(dtype)


@ufunc.resolver(MongoQueryBuilder, Recurse)
def lower(env, recurse):
    return Recurse([env.call('lower', arg) for arg in recurse.args])


@ufunc.resolver(MongoQueryBuilder, Bind)
def recurse(env, field):
    if field.name in env.model.leafprops:
        return Recurse([prop.dtype for prop in env.model.leafprops[field.name]])
    else:
        raise exceptions.FieldNotInResource(env.model, property=field.name)


@ufunc.resolver(MongoQueryBuilder, Recurse, object, names=[
    'eq', 'ne', 'lt', 'le', 'gt', 'ge', 'contains', 'startswith',
])
def recurse(env, op, recurse, value):
    return env.call('or', [
        env.call(op, arg, value)
        for arg in recurse.args
    ])


@ufunc.resolver(MongoQueryBuilder, Expr, name='any')
def any_(env, expr):
    args, kwargs = expr.resolve(env)
    op, field, *args = args
    if isinstance(op, Bind):
        op = op.name
    return env.call('or', [
        env.call(op, field, arg)
        for arg in args
    ])


@ufunc.resolver(MongoQueryBuilder, Expr)
def sort(env, expr):
    args, kwargs = expr.resolve(env)
    env.sort = [
        env.call('sort', arg) for arg in args
    ]


@ufunc.resolver(MongoQueryBuilder, Bind)
def sort(env, field):
    prop = env.resolve_property(field)
    return env.call('asc', prop.dtype)


@ufunc.resolver(MongoQueryBuilder, Positive)
def sort(env, sign):
    return env.call('asc', sign.arg)


@ufunc.resolver(MongoQueryBuilder, Negative)
def sort(env, sign):
    return env.call('desc', sign.arg)


@ufunc.resolver(MongoQueryBuilder, DataType)
def asc(env, dtype):
    return dtype.prop.place, pymongo.ASCENDING


@ufunc.resolver(MongoQueryBuilder, DataType)
def desc(env, dtype):
    return dtype.prop.place, pymongo.DESCENDING


@ufunc.resolver(MongoQueryBuilder, PrimaryKey)
def asc(env, dtype):
    return '__id', pymongo.ASCENDING


@ufunc.resolver(MongoQueryBuilder, PrimaryKey)
def desc(env, dtype):
    return '__id', pymongo.DESCENDING


@ufunc.resolver(MongoQueryBuilder, Bind)
def negative(env, field: Bind) -> Negative:
    prop = env.resolve_property(field)
    return Negative(prop.dtype)


@ufunc.resolver(MongoQueryBuilder, Bind)
def positive(env, field: Bind) -> Positive:
    prop = env.resolve_property(field)
    return Positive(prop.dtype)


@ufunc.resolver(MongoQueryBuilder, DataType)
def negative(env, dtype) -> Negative:
    return Negative(dtype)


@ufunc.resolver(MongoQueryBuilder, DataType)
def positive(env, dtype) -> Positive:
    return Positive(dtype)
