import contextlib
import datetime
import re
import typing
import types

import pymongo

from starlette.requests import Request

from spinta import commands
from spinta.backends import Backend, complex_data_check
from spinta.components import Context, Manifest, Model, Property, Action, UrlParams, DataStream, DataItem
from spinta.common import NA
from spinta.config import RawConfig
from spinta.renderer import render
from spinta.types.datatype import Date, DataType
from spinta.commands import (
    authorize,
    dump,
    getall,
    getone,
    load,
    load_search_params,
    migrate,
    prepare,
    wait,
    wipe,
)
from spinta.exceptions import (
    ItemDoesNotExist,
    UniqueConstraint,
)
from spinta import exceptions


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


@migrate.register()
def migrate(context: Context, backend: Mongo):
    # Migrate schema changes.
    pass


@complex_data_check.register()
def complex_data_check(
    context: Context,
    data: DataItem,
    dtype: DataType,
    prop: Property,
    backend: Mongo,
    value: object,
) -> None:
    if dtype.unique and value is not NA:
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
            result = table.find_one({
                name: value,
                '__id': {'$ne': data.saved['_id']},
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
        table.insert_one({
            '__id': data.patch['_id'],
            '_revision': data.patch['_revision'],
            **{k: v for k, v in data.patch.items() if not k.startswith('_')},
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

        values = {k: v for k, v in data.patch.items() if not k.startswith('_')}
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
        assert result.matched_count == 1 and result.modified_count == 1, (
            f"matched: {result.matched_count}, modified: {result.modified_count}"
        )
        yield data


@commands.delete.register()
def delete(
    context: Context,
    model: Model,
    backend: Mongo,
    *,
    id_: str,
):
    table = backend.db[model.model_type()]
    result = table.delete_one({'id': id_})
    if result.deleted_count == 0:
        raise ItemDoesNotExist(model, id=id_)


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
    backend: Mongo,
    *,
    action: Action,
    params: UrlParams,
):
    authorize(context, action, prop)
    data = getone(context, prop, backend, id_=params.pk)
    data = dump(context, backend, prop.dtype, data)
    return render(context, request, prop, params, data, action=action)


@getone.register()
def getone(
    context: Context,
    prop: Property,
    backend: Mongo,
    *,
    id_: str,
):
    table = backend.db[prop.model.model_type()]
    data = table.find_one({'__id': id_}, {prop.name: 1})
    if data is None:
        raise ItemDoesNotExist(prop, id=id_)
    data['_id'] = data['__id']
    return data.get(prop.name)


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

    search_expressions = []
    query = query or []
    for qp in query:
        key = qp['args'][0]

        # TODO: Fix RQL parser to support `foo.bar=baz` notation.
        key = '.'.join(key) if isinstance(key, tuple) else key

        if key not in model.flatprops:
            raise exceptions.FieldNotInResource(model, property=key)

        prop = model.flatprops[key]
        name = qp['name']

        if key == '_id':
            key = '__id'
        elif key.startswith('_'):
            raise exceptions.FieldNotInResource(model, property=key)

        # for search to work on MongoDB, values must be compatible for
        # Mongo's BSON consumption, thus we need to use chained load and prepare
        value = load_search_params(context, prop.dtype, backend, qp)

        # in case value is not a string - then just search for that value directly
        if isinstance(value, str):
            re_value = re.compile('^' + value + '$', re.IGNORECASE)
        else:
            re_value = value

        if name == 'eq':
            search_expressions.append({
                key: re_value
            })
        elif name == 'gt':
            search_expressions.append({
                key: {'$gt': re_value}
            })
        elif name == 'ge':
            search_expressions.append({
                key: {'$gte': re_value}
            })
        elif name == 'lt':
            search_expressions.append({
                key: {'$lt': re_value}
            })
        elif name == 'le':
            search_expressions.append({
                key: {'$lte': re_value}
            })
        elif name == 'ne':
            # MongoDB's $ne operator does not consume regular expresions for values,
            # whereas `$not` requires an expression.
            # Thus if our search value is regular expression - search with $not, if
            # not - use $ne
            if isinstance(re_value, re.Pattern):
                search_expressions.append({
                    key: {'$not': re_value}
                })
            else:
                search_expressions.append({
                    key: {'$ne': re_value}
                })
        elif name == 'contains':
            try:
                re_value = re.compile(value, re.IGNORECASE)
            except TypeError:
                # in case value is not a string - then just search for that value directly
                # XXX: Let's not guess, but check schema instead.
                re_value = value

            search_expressions.append({
                key: re_value
            })
        elif name == 'startswith':
            # https://stackoverflow.com/a/3483399
            try:
                re_value = re.compile('^' + value + '.*', re.IGNORECASE)
            except TypeError:
                # in case value is not a string - then just search for that value directly
                # XXX: Let's not guess, but check schema instead.
                re_value = value

            search_expressions.append({
                key: re_value
            })
        else:
            raise exceptions.UnknownOperator(prop, operator=name)

    search_query = {}

    # search expressions cannot be empty
    if search_expressions:
        search_query['$and'] = search_expressions

    cursor = table.find(search_query)

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
        cursor = cursor.sort([
            (k, direction[d]) for d, k in sort
        ])

    for row in cursor:
        row['_id'] = row.pop('__id')
        yield prepare(context, action, model, backend, row, select=select)


@wipe.register()
def wipe(context: Context, model: Model, backend: Mongo):
    authorize(context, Action.WIPE, model)
    table = backend.db[model.model_type()]
    return table.delete_many({})


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
