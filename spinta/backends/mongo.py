from typing import List, Optional, Union

import contextlib
import datetime
import re
import typing
import types

import pymongo

from starlette.requests import Request

from spinta import commands
from spinta.backends import Backend
from spinta.components import Context, Manifest, Model, Property, Action, UrlParams, DataStream, DataItem
from spinta.config import RawConfig
from spinta.renderer import render
from spinta.types.datatype import Date, DataType, File, Object
from spinta.utils.schema import strip_metadata
from spinta.commands import (
    authorize,
    getall,
    getone,
    load,
    migrate,
    prepare,
    wait,
    wipe,
)
from spinta.exceptions import (
    ItemDoesNotExist,
    UniqueConstraint,
    UnavailableSubresource,
)
from spinta import exceptions
from spinta.migrations import (
    get_schema_from_changes,
    get_schema_changes,
    get_new_schema_version
)
from spinta.hacks.recurse import _replace_recurse


class Mongo(Backend):
    # Instance of this class will be created when application starts. First if
    # will be loaded from configuration using `load` command, then it will be
    # prepared from manifest declarations using `prepare` command.
    #
    # Backend also must have a `transaction` method which must return read or
    # write transaction object containing an active `connection` to database.
    metadata = {
        'name': 'mongo',
        'properties': {
            'dsn': {'type': 'string', 'required': True},
            'db': {'type': 'string', 'required': True},
        },
    }

    @contextlib.contextmanager
    def transaction(self, write=False):
        with self.engine.begin() as connection:
            if write:
                # TODO: get a real transaction id
                transaction_id = 1
                yield WriteTransaction(connection, transaction_id)
            else:
                yield ReadTransaction(connection)


class ReadTransaction:

    def __init__(self, connection):
        self.connection = connection


class WriteTransaction(ReadTransaction):

    def __init__(self, connection, id):
        super().__init__(connection)
        self.id = id
        self.errors = 0


@load.register()
def load(context: Context, backend: Mongo, config: RawConfig):
    # Load Mongo client using configuration.
    backend.dsn = config.get('backends', backend.name, 'dsn', required=True)
    backend.db_name = config.get('backends', backend.name, 'db', required=True)

    backend.client = pymongo.MongoClient(backend.dsn)
    backend.db = backend.client[backend.db_name]


@wait.register()
def wait(context: Context, backend: Mongo, config: RawConfig, *, fail: bool = False):
    return True


@prepare.register()
def prepare(context: Context, backend: Mongo, manifest: Manifest):
    # Mongo does not need any table or database preparations
    pass


@commands.new_schema_version.register()
def new_schema_version(
    context: Context,
    backend: Mongo,
    model: Model,
    *,
    versions: List[dict],
):
    old, new, nextval = get_schema_from_changes(versions)
    changes = get_schema_changes(old, new)
    if changes:
        migrate = {}
        version = get_new_schema_version(old, changes, migrate, nextval)
        return version
    else:
        return {}


@migrate.register()
def migrate(context: Context, backend: Mongo):
    # Migrate schema changes.
    pass


@commands.check_unique_constraint.register()
def check_unique_constraint(
    context: Context,
    data: DataItem,
    dtype: DataType,
    prop: Property,
    backend: Mongo,
    value: object,
):
    model = prop.model
    table = backend.db[model.model_type()]
    # XXX: Probably we should move this out of mongo backend and implement
    #      this on spinta.commands.write. For example, read_existing_data
    #      could try to read existing record if `_id` or any other unique
    #      field is given. Also this would fix case, when multiple
    #      properties are given as unique constraint.
    if prop.name == '_id':
        name = '__id'
    else:
        name = prop.name
    # TODO: Add support for nested properties.
    # FIXME: Exclude currently saved value.
    #        In case of an update, exclude currently saved value from
    #        uniqueness check.
    if data.action in (Action.UPDATE, Action.PATCH):
        if name == '__id' and value == data.saved['_id']:
            return

        result = table.find_one({
            '$and': [{name: value},
                     {'__id': {'$ne': data.saved['_id']}}],
        })
    else:
        result = table.find_one({name: value})
    if result is not None:
        raise UniqueConstraint(prop, value=value)


@commands.insert.register()
async def insert(
    context: Context,
    model: Model,
    backend: Mongo,
    *,
    dstream: DataStream,
    stop_on_error: bool = True,
):
    table = backend.db[model.model_type()]
    async for data in dstream:
        # TODO: Insert batches in a single query, using `insert_many`.
        patch = {
            k: v for k, v in data.patch.items() if not k.startswith('_')
        }
        table.insert_one({
            '__id': data.patch['_id'],
            '_revision': data.patch['_revision'],
            **patch,
        })
        data.saved = data.patch.copy()
        yield data


@commands.update.register()
async def update(
    context: Context,
    model: Model,
    backend: Mongo,
    *,
    dstream: dict,
    stop_on_error: bool = True,
):
    table = backend.db[model.model_type()]
    async for data in dstream:
        if not data.patch:
            yield data
            continue

        # FIXME: this is technically a hack, as it does not employ
        # mongo's partial update feature, i.e. we patch whole underlining
        # nested structure if there were changed in the nested structure.
        # We do this, because partial updates do not work for arrays.
        # We must implement a function wich does partial updates, for non-array
        # nested attributes, but if there's an array - then do full patch.
        patch = commands.build_full_data_patch_for_nested_attrs(
            context,
            model,
            patch=strip_metadata(data.patch),
            saved=strip_metadata(data.saved),
        )

        values = {k: v for k, v in patch.items() if not k.startswith('_')}
        values['_revision'] = data.patch['_revision']
        if '_id' in data.patch:
            values['__id'] = data.patch['_id']

        result = table.update_one(
            {
                '__id': data.saved['_id'],
                '_revision': data.saved['_revision'],
            },
            {'$set': values}
        )
        if result.matched_count == 0:
            raise ItemDoesNotExist(
                model,
                id=data.saved['_id'],
                revision=data.saved['_revision'],
            )
        assert result.matched_count == 1 and result.modified_count == 1, (
            f"matched: {result.matched_count}, modified: {result.modified_count}"
        )
        yield data


@commands.delete.register()
async def delete(
    context: Context,
    model: Model,
    backend: Mongo,
    *,
    dstream: dict,
    stop_on_error: bool = True,
):
    table = backend.db[model.model_type()]
    async for data in dstream:
        result = table.delete_one({
            '__id': data.saved['_id'],
            '_revision': data.saved['_revision'],
        })
        if result.deleted_count == 0:
            # FIXME: Respect stop_on_error flag.
            raise ItemDoesNotExist(
                model,
                id=data.saved['_id'],
                revision=data.saved['_revision'],
            )
        yield data


@getone.register()
async def getone(
    context: Context,
    request: Request,
    model: Model,
    backend: Mongo,
    *,
    action: Action,
    params: UrlParams,
):
    authorize(context, action, model)
    data = getone(context, model, backend, id_=params.pk)
    data = prepare(context, action, model, backend, data, select=params.select)
    return render(context, request, model, params, data, action=action)


@getone.register()
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
    return data


@getone.register()
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


@getone.register()
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
    authorize(context, action, prop)
    data = getone(context, prop, dtype, backend, id_=params.pk)
    pdata = data.pop(prop.name)
    data = {
        **data,
        **pdata,
    }
    data = prepare(context, Action.GETONE, prop.dtype, backend, data)
    return render(context, request, prop, params, data, action=action)


@getone.register()
def getone(
    context: Context,
    prop: Property,
    dtype: Object,
    backend: Mongo,
    *,
    id_: str,
):
    type_ = prop.model.model_type()
    table = backend.db[type_]
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
        '_type': type_,
        prop.name: (data.get(prop.name) or {}),
    }
    return result


@getone.register()
def getone(
    context: Context,
    prop: Property,
    dtype: File,
    backend: Mongo,
    *,
    id_: str,
):
    type_ = prop.model.model_type()
    table = backend.db[type_]
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
        '_type': type_,
        prop.name: data.get(prop.name, {
            '_content_type': None,
            '_id': None
        }),
    }
    return result


@getall.register()
async def getall(
    context: Context,
    request: Request,
    model: Model,
    backend: Mongo,
    *,
    action: Action,
    params: UrlParams,
):
    # show: typing.List[str] = None,
    # sort: typing.List[typing.Dict[str, str]] = None,
    # offset=None, limit=None,
    # count: bool = False,
    # query_params: typing.List[typing.Dict[str, str]] = None,
    # search: bool = False,

    authorize(context, action, model)
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
    return render(context, request, model, params, data, action=action)


@getall.register()  # noqa
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
        yield prepare(context, action, model, backend, row, select=select)


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
                '+': pymongo.ASCENDING,
                '-': pymongo.DESCENDING,
            }
            # Optional sort direction: sort(+key) or sort(key)
            sort = ((('+',) + k) if len(k) == 1 else k for k in sort)
            nsort = []
            for d, k in sort:
                if k == '_id':
                    k = '__id'
                nsort.append((k, direction[d]))
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
        elif key.startswith('_'):
            raise exceptions.FieldNotInResource(self.model, property=key)

        return method(key, value, lower)

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
            value = re.compile(value, re.IGNORECASE)
        except TypeError:
            # in case value is not a string - then just search for that value directly
            # XXX: Let's not guess, but check schema instead.
            pass
        return {key: value}

    def op_startswith(self, key, value, lower=False):
        # https://stackoverflow.com/a/3483399
        try:
            value = re.compile('^' + value + '.*', re.IGNORECASE)
        except TypeError:
            # in case value is not a string - then just search for that value directly
            # XXX: Let's not guess, but check schema instead.
            pass
        return {key: value}


@wipe.register()
def wipe(context: Context, model: Model, backend: Mongo):
    table_main = backend.db[model.model_type()]
    table_changelog = backend.db[model.model_type() + '__changelog']
    table_main.delete_many({})
    table_changelog.delete_many({})


@prepare.register()
def prepare(context: Context, dtype: Date, backend: Mongo, value: datetime.date) -> datetime.datetime:
    # prepares date values for Mongo store, they must be converted to datetime
    return datetime.datetime.combine(value, datetime.datetime.min.time())


@commands.create_changelog_entry.register()
async def create_changelog_entry(
    context: Context,
    model: (Model, Property),
    backend: Mongo,
    *,
    dstream: types.AsyncGeneratorType,
) -> None:
    transaction = context.get('transaction')
    if isinstance(model, Model):
        table = backend.db[model.model_type() + '__changelog']
    else:
        table = backend.db[model.model.model_type() + '__changelog']
    async for data in dstream:
        table.insert_one({
            '__id': data.saved['_id'] if data.saved else data.patch['_id'],
            '_revision': data.patch['_revision'] if data.patch else data.saved['_revision'],
            '_op': data.action.value,
            '_transaction': transaction.id,
            '_created': datetime.datetime.now(),
            **{k: v for k, v in data.patch.items() if not k.startswith('_')},
        })
        yield data
