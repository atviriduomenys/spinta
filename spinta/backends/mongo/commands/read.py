from typing import List, Optional, Union

import re
import typing

import pymongo

from starlette.requests import Request

from spinta import commands
from spinta import exceptions
from spinta.components import Context, Model, Property, Action, UrlParams
from spinta.renderer import render
from spinta.types.datatype import DataType, File, Object
from spinta.utils.schema import is_valid_sort_key
from spinta.exceptions import ItemDoesNotExist, UnavailableSubresource
from spinta.hacks.recurse import _replace_recurse
from spinta.backends import log_getall, log_getone
from spinta.backends.mongo.components import Mongo


@commands.getone.register(Context, Request, Model, Mongo)
async def getone(
    context: Context,
    request: Request,
    model: Model,
    backend: Mongo,
    *,
    action: Action,
    params: UrlParams,
):
    commands.authorize(context, action, model)
    data = getone(context, model, backend, id_=params.pk)
    hidden_props = [prop.name for prop in model.properties.values() if prop.hidden]
    log_getone(context, data, select=params.select, hidden=hidden_props)
    data = commands.prepare_data_for_response(
        context,
        action,
        model,
        backend,
        data,
        select=params.select,
    )
    return render(context, request, model, params, data, action=action)


@commands.getone.register(Context, Model, Mongo)
def getone(
    context: Context,
    model: Model,
    backend: Mongo,
    *,
    id_: str,
):
    table = backend.db[model.model_type()]
    data = table.find_one({'__id': id_})
    if data is None:
        raise ItemDoesNotExist(model, id=id_)
    data['_id'] = data['__id']
    data['_type'] = model.model_type()
    return commands.cast_backend_to_python(context, model, backend, data)


@commands.getone.register(Context, Request, Property, DataType, Mongo)
async def getone(
    context: Context,
    request: Request,
    prop: Property,
    dtype: DataType,
    backend: Mongo,
    *,
    action: Action,
    params: UrlParams,
):
    raise UnavailableSubresource(prop=prop.name, prop_type=prop.dtype.name)


@commands.getone.register(Context, Request, Property, (Object, File), Mongo)
async def getone(
    context: Context,
    request: Request,
    prop: Property,
    dtype: (Object, File),
    backend: Mongo,
    *,
    action: Action,
    params: UrlParams,
):
    commands.authorize(context, action, prop)
    data = getone(context, prop, dtype, backend, id_=params.pk)
    log_getone(context, data)
    data = commands.prepare_data_for_response(context, Action.GETONE, prop.dtype, backend, data)
    return render(context, request, prop, params, data, action=action)


@commands.getone.register(Context, Property, Object, Mongo)
def getone(
    context: Context,
    prop: Property,
    dtype: Object,
    backend: Mongo,
    *,
    id_: str,
):
    table = backend.db[prop.model.model_type()]
    data = table.find_one({'__id': id_}, {
        '__id': 1,
        '_revision': 1,
        prop.name: 1,
    })
    if data is None:
        raise ItemDoesNotExist(prop, id=id_)
    result = {
        '_id': data['__id'],
        '_revision': data['_revision'],
        '_type': prop.model_type(),
        prop.name: (data.get(prop.name) or {}),
    }
    return commands.cast_backend_to_python(context, prop, backend, result)


@commands.getone.register(Context, Property, File, Mongo)
def getone(
    context: Context,
    prop: Property,
    dtype: File,
    backend: Mongo,
    *,
    id_: str,
):
    table = backend.db[prop.model.model_type()]
    data = table.find_one({'__id': id_}, {
        '__id': 1,
        '_revision': 1,
        prop.name: 1,
    })
    if data is None:
        raise ItemDoesNotExist(prop, id=id_)
    # merge file property data with defaults
    file_data = {
        **{
            '_content_type': None,
            '_id': None,
            '_size': None,
        },
        **data.get(prop.name, {}),
    }
    result = {
        '_id': data['__id'],
        '_revision': data['_revision'],
        '_type': prop.model_type(),
        prop.name: file_data,
    }
    return commands.cast_backend_to_python(context, prop, backend, result)


@commands.getall.register(Context, Request, Model, Mongo)
async def getall(
    context: Context,
    request: Request,
    model: Model,
    backend: Mongo,
    *,
    action: Action,
    params: UrlParams,
):
    commands.authorize(context, action, model)
    data = commands.getall(
        context, model, model.backend,
        action=action,
        select=params.select,
        sort=params.sort,
        offset=params.offset,
        limit=params.limit,
        count=params.count,
        query=params.query,
    )
    hidden_props = [prop.name for prop in model.properties.values() if prop.hidden]
    data = log_getall(context, data, select=params.select, hidden=hidden_props)
    data = (
        commands.prepare_data_for_response(
            context,
            action,
            model,
            backend,
            row,
            select=params.select,
        )
        for row in data
    )
    return render(context, request, model, params, data, action=action)


@commands.getall.register(Context, Model, Mongo)
def getall(
    context: Context,
    model: Model,
    backend: Mongo,
    *,
    action: Action = Action.GETALL,
    select: typing.List[str] = None,
    sort: typing.Dict[str, dict] = None,
    offset: int = None,
    limit: int = None,
    # XXX: Deprecated, count should be part of `select`.
    count: bool = False,
    query: typing.List[typing.Dict[str, str]] = None,
):
    table = backend.db[model.model_type()]

    qb = QueryBuilder(context, model, backend, table)
    cursor = qb.build(select, sort, offset, limit, query)

    for row in cursor:
        row['_id'] = row.pop('__id')
        row['_type'] = model.model_type()
        yield commands.cast_backend_to_python(context, model, backend, row)


class QueryBuilder:
    compops = (
        'eq',
        'ge',
        'gt',
        'le',
        'lt',
        'ne',
        'contains',
        'startswith',
    )

    def __init__(
        self,
        context: Context,
        model: Model,
        backend: Mongo,
        table,
    ):
        self.context = context
        self.model = model
        self.backend = backend
        self.table = table
        self.select = []
        self.where = []

    def build(
        self,
        select: typing.List[str] = None,
        sort: typing.Dict[str, dict] = None,
        offset: int = None,
        limit: int = None,
        query: Optional[List[dict]] = None,
    ) -> dict:
        cursor = self.table.find(self.op_and(*(query or [])))

        if limit is not None:
            cursor = cursor.limit(limit)

        if offset is not None:
            cursor = cursor.skip(offset)

        if sort:
            direction = {
                'positive': pymongo.ASCENDING,
                'negative': pymongo.DESCENDING,
            }
            nsort = []
            for k in sort:
                # Optional sort direction: sort(+key) or sort(key)
                if isinstance(k, dict) and k['name'] in direction:
                    d = direction[k['name']]
                    k = k['args'][0]
                else:
                    d = direction['positive']

                if not is_valid_sort_key(k, self.model):
                    raise exceptions.FieldNotInResource(self.model, property=k)

                if k == '_id':
                    k = '__id'

                nsort.append((k, d))
            cursor = cursor.sort(nsort)

        return cursor

    def resolve_recurse(self, arg):
        name = arg['name']
        if name in self.compops:
            return _replace_recurse(self.model, arg, 0)
        if name == 'any':
            return _replace_recurse(self.model, arg, 1)
        return arg

    def resolve(self, args: Optional[List[dict]]) -> None:
        for arg in (args or []):
            arg = self.resolve_recurse(arg)
            name = arg['name']
            opargs = arg.get('args', ())
            method = getattr(self, f'op_{name}', None)
            if method is None:
                raise exceptions.UnknownOperator(self.model, operator=name)
            if name in self.compops:
                yield self.comparison(name, method, *opargs)
            else:
                yield method(*opargs)

    def resolve_property(self, key: Union[str, tuple]) -> Property:
        if key not in self.model.flatprops:
            raise exceptions.FieldNotInResource(self.model, property=key)
        return self.model.flatprops[key]

    def resolve_value(self, op, prop: Property, value: Union[str, dict]) -> object:
        return commands.load_search_params(self.context, prop.dtype, self.backend, {
            'name': op,
            'args': [prop.place, value]
        })

    def comparison(self, op, method, key, value):
        lower = False
        if isinstance(key, dict) and key['name'] == 'lower':
            lower = True
            key = key['args'][0]

        if isinstance(key, tuple):
            key = '.'.join(key)

        prop = self.resolve_property(key)
        value = self.resolve_value(op, prop, value)

        if key == '_id':
            key = '__id'
        elif key != '_revision' and key.startswith('_'):
            raise exceptions.FieldNotInResource(self.model, property=key)

        return method(key, value, lower)

    def op_group(self, *args: List[dict]):
        args = list(self.resolve(args))
        assert len(args) == 1, "Group with multiple args are not supported here."
        return args[0]

    def op_and(self, *args: List[dict]):
        args = list(self.resolve(args))
        if len(args) > 1:
            return {'$and': args}
        if len(args) == 1:
            return args[0]
        else:
            return {}

    def op_or(self, *args: List[dict]):
        args = list(self.resolve(args))
        if len(args) > 1:
            return {'$or': args}
        if len(args) == 1:
            return args[0]
        else:
            return {}

    def op_eq(self, key, value, lower=False):
        if lower:
            # TODO: I don't know how to lower case values in mongo.
            value = re.compile('^' + value + '$', re.IGNORECASE)
        return {key: value}

    def op_ge(self, key, value, lower=False):
        return {key: {'$gte': value}}

    def op_gt(self, key, value, lower=False):
        return {key: {'$gt': value}}

    def op_le(self, key, value, lower=False):
        return {key: {'$lte': value}}

    def op_lt(self, key, value, lower=False):
        return {key: {'$lt': value}}

    def op_ne(self, key, value, lower=False):
        # MongoDB's $ne operator does not consume regular expresions for values,
        # whereas `$not` requires an expression.
        # Thus if our search value is regular expression - search with $not, if
        # not - use $ne
        if lower:
            # TODO: I don't know how to lower case values in mongo.
            value = re.escape(value)
            value = re.compile('^' + value + '$', re.IGNORECASE)
            return {
                '$and': [
                    {key: {'$not': value, '$exists': True}},
                    {key: {'$ne': None, '$exists': True}},
                ],
            }
        else:
            return {
                '$and': [
                    {key: {'$ne': value, '$exists': True}},
                    {key: {'$ne': None, '$exists': True}},
                ]
            }

    def op_contains(self, key, value, lower=False):
        try:
            value = re.escape(value)
            value = re.compile(value, re.IGNORECASE)
        except TypeError:
            # in case value is not a string - then just search for that value directly
            # XXX: Let's not guess, but check schema instead.
            pass
        return {key: value}

    def op_startswith(self, key, value, lower=False):
        # https://stackoverflow.com/a/3483399
        try:
            value = re.escape(value)
            value = re.compile('^' + value + '.*', re.IGNORECASE)
        except TypeError:
            # in case value is not a string - then just search for that value directly
            # XXX: Let's not guess, but check schema instead.
            pass
        return {key: value}
