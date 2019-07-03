import contextlib
import re
import typing

from datetime import date, datetime

import pymongo

from starlette.exceptions import HTTPException
from starlette.requests import Request

from spinta.backends import Backend, check_model_properties
from spinta.commands import load, prepare, migrate, check, push, getone, getall, wipe, wait, authorize, dump, gen_object_id
from spinta.components import Context, Manifest, Model, Property, Action, UrlParams
from spinta.config import RawConfig
from spinta.types.type import Date
from spinta.utils.idgen import get_new_id
from spinta.exceptions import NotFound
from spinta import commands
from spinta.auth import check_scope
from spinta.utils.changes import get_patch_changes
from spinta.utils.response import get_request_data
from spinta.renderer import render


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


@check.register()
def check(context: Context, model: Model, backend: Mongo, data: dict, *, action: Action):
    check_model_properties(context, model, backend, data, action)


@push.register()
async def push(
    context: Context,
    request: Request,
    model: Model,
    backend: Mongo,
    *,
    action: Action,
    params: UrlParams,
):
    authorize(context, action, model)

    if action == Action.DELETE:
        data = {}
    else:
        data = await get_request_data(request)
        data = load(context, model, data)
        check(context, model, backend, data, action=action)
        data = prepare(context, model, data, action=action)

    transaction = context.get('transaction')  # noqa

    if action == Action.INSERT:
        data['id'] = commands.insert(context, model, backend, data=data)

    elif action == Action.UPSERT:
        data['id'] = commands.upsert(context, model, backend, data=data)

    elif action == Action.UPDATE:
        data['id'] = params.id
        # FIXME: check if revision given in `data` matches the revision in database
        # related to SPLAT-60
        data['revision'] = get_new_id('revision id')
        commands.update(context, model, backend, id_=params.id, data=data)

    elif action == Action.PATCH:
        data['id'] = params.id
        data['revision'] = get_new_id('revision id')
        commands.patch(context, model, backend, id_=params.id, data=data)

    elif action == Action.DELETE:
        data['id'] = params.id
        data['revision'] = get_new_id('revision id')
        commands.delete(context, model, backend, id_=params.id)

    else:
        raise Exception(f"Unknown action: {action!r}.")

    data = prepare(context, action, model, backend, data)

    status_code = 201 if action == Action.INSERT else 200
    return render(context, request, model, action, params, data, status_code=status_code)


@commands.insert.register()
def insert(
    context: Context,
    model: Model,
    backend: Mongo,
    *,
    data: dict,
):
    table = backend.db[model.get_type_value()]

    if 'id' in data:
        check_scope(context, 'set_meta_fields')
    else:
        data['id'] = gen_object_id(context, backend, model)

    if 'revision' in data:
        raise HTTPException(status_code=400, detail="cannot create 'revision'")
    # FIXME: before creating revision check if there's no collision clash
    data['revision'] = get_new_id('revision id')

    table.insert_one(data)

    return data['id']


@commands.upsert.register()
def upsert(
    context: Context,
    model: Model,
    backend: Mongo,
    *,
    key: typing.List[str],
    data: dict,
):
    table = backend.db[model.get_type_value()]

    condition = []
    for k in key:
        condition.append({k: data[k]})

    row = table.find_one({'$and': condition})

    if row is None:
        if 'id' in data:
            id_ = data['id']
        else:
            id_ = commands.gen_object_id(context, backend, model)

        if 'revision' in data.keys():
            raise HTTPException(status_code=400, detail="cannot create 'revision'")
        data['revision'] = get_new_id('revision id')

        table.insert_one(data)

    else:
        id_ = row['_id']

        data = _patch(table, id_, row, data)

        if data is None:
            # Nothing changed.
            return None

    # Track changes.
    # TODO: add to changelog

    return id_


@commands.patch.register()
def patch(
    context: Context,
    model: Model,
    backend: Mongo,
    *,
    id_: str,
    data: dict,
):
    table = backend.db[model.get_type_value()]

    row = table.find_one({'id': id_})
    if row is None:
        type_ = model.get_type_value()
        raise NotFound(f"Object {type_!r} with id {id_!r} not found.")

    data = _patch(table, id_, row, data)

    if data is None:
        # Nothing changed.
        return None

    # Track changes.
    # TODO: add to changelog

    return data


def _patch(table, id_, row, data):
    changes = get_patch_changes(row, data)

    if not changes:
        # Nothing to update.
        return None

    result = table.update_one(
        {'id': id_},
        {'$set': changes},
    )

    assert result.matched_count == 1, (
        f"matched: {result.matched_count}, modified: {result.modified_count}"
    )

    return changes


@commands.delete.register()
def delete(
    context: Context,
    model: Model,
    backend: Mongo,
    *,
    id_: str,
):
    table = backend.db[model.get_type_value()]
    table.delete_one({'id': id_})


@push.register()
async def push(
    context: Context,
    request: Request,
    prop: Property,
    backend: Mongo,
    *,
    action: Action,
    params: UrlParams,
    ref: bool = False,
):
    """

    Args:
        ref: Update reference or data refered by reference.

            This applies only to direct property updates. Update reference is
            enabled, when property is named like this `prop:ref`, the `:ref`
            suffix tells, that reference should be updated and `ref` argument is
            set to True.

            Most properties do not have references, but some do. So this only
            applies to properties like File with external backend, ForeignKey
            and etc.

    """
    if action == Action.INSERT:
        raise HTTPException(status_code=400, detail=f"Can't POST to a property, use PUT or PATCH instead.")

    authorize(context, action, prop)

    data = await get_request_data(request)

    data = load(context, prop.type, data)
    check(context, prop.type, prop, backend, data, data=None, action=action)
    data = prepare(context, prop.type, backend, data)

    if action == Action.UPDATE:
        commands.update(context, prop, backend, id_=params.id, data=data)
    elif action == Action.PATCH:
        commands.patch(context, prop, backend, id_=params.id, data=data)
    elif action == Action.DELETE:
        commands.delete(context, prop, backend, id_=params.id)
    else:
        raise Exception(f"Unknown action {action}.")

    data = dump(context, backend, prop.type, data)
    return render(context, request, prop, action, params, data)


@commands.update.register()
def update(
    context: Context,
    model: Model,
    backend: Mongo,
    *,
    id_: str,
    data: dict,
):
    table = backend.db[model.get_type_value()]
    result = table.update_one(
        {'id': id_},
        {'$set': data}
    )
    assert result.matched_count == 1 and result.modified_count == 1, (
        f"matched: {result.matched_count}, modified: {result.modified_count}"
    )


@commands.update.register()  # noqa
def update(
    context: Context,
    prop: Property,
    backend: Mongo,
    *,
    id_: str,
    data: dict,
):
    table = backend.db[prop.model.get_type_value()]
    result = table.update_one(
        {'id': id_},
        {'$set': {prop.name: data}}
    )
    assert result.matched_count == 1, (
        f"matched: {result.matched_count}, modified: {result.modified_count}"
    )


@commands.patch.register()  # noqa
def patch(
    context: Context,
    prop: Property,
    backend: Mongo,
    *,
    id_: str,
    data: dict,
):
    table = backend.db[prop.model.get_type_value()]
    row = table.find_one({'id': id_}, {prop.name: 1})
    if row is None:
        type_ = prop.model.get_type_value()
        raise NotFound(f"Object {type_!r} with id {id_!r} not found.")

    data = _patch_property(table, id_, prop, row, data)

    if data is None:
        # Nothing changed.
        return None

    # Track changes.
    # TODO: add to changelog

    return data


def _patch_property(table, id_, prop, row, data):
    changes = get_patch_changes(row[prop.name], data[prop.name])

    if not changes:
        # Nothing to update.
        return None

    result = table.update_one(
        {'id': id_},
        {'$set': {
            prop.name: changes,
        }},
    )

    if result.matched_count == 0:
        type_ = prop.model.get_type_value()
        raise NotFound(f"Object {type_!r} with id {id_!r} not found.")

    assert result.matched_count == 1, (
        f"matched: {result.matched_count}, modified: {result.modified_count}"
    )

    return changes


@commands.delete.register()  # noqa
def delete(
    context: Context,
    prop: Property,
    backend: Mongo,
    *,
    id_: str,
):
    table = backend.db[prop.model.get_type_value()]
    result = table.update_one(
        {'id': id_},
        {'$set': {
            prop.name: None,
        }},
    )
    assert result.matched_count == 1, (
        f"matched: {result.matched_count}, modified: {result.modified_count}"
    )

@getone.register()
async def getone(
    context: Context,
    request: Request,
    model: Model,
    backend: Mongo,
    *,
    action: Action,
    params: UrlParams,
    ref: bool = False,
):
    authorize(context, action, model)
    data = getone(context, model, backend, id_=params.id)
    data = prepare(context, action, model, backend, data)
    return render(context, request, model, action, params, data)


@getone.register()
def getone(
    context: Context,
    model: Model,
    backend: Mongo,
    *,
    id_: str,
):
    table = backend.db[model.get_type_value()]
    data = table.find_one({'id': id_})
    if data is None:
        type_ = model.get_type_value()
        raise NotFound(f"Object {type_!r} with id {id_!r} not found.")
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
    ref: bool = False,
):
    authorize(context, action, prop)
    data = getone(context, prop, backend, id_=params.id)
    data = dump(context, backend, prop.type, data)
    return render(context, request, prop, action, params, data)


@getone.register()
def getone(
    context: Context,
    prop: Property,
    backend: Mongo,
    *,
    id_: str,
):
    table = backend.db[prop.model.get_type_value()]
    data = table.find_one({'id': id_}, {prop.name: 1})
    if data is None:
        type_ = prop.model.get_type_value()
        raise NotFound(f"Object {type_!r} with id {id_!r} not found.")
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
        show=params.show,
        sort=params.sort,
        offset=params.offset,
        limit=params.limit,
        count=params.count,
        query=params.query,
    )
    return render(context, request, model, action, params, data)


@getall.register()  # noqa
def getall(
    context: Context,
    model: Model,
    backend: Mongo,
    *,
    action: Action = Action.GETALL,
    show: typing.List[str] = None,
    sort: typing.Dict[str, dict] = None,
    offset: int = None,
    limit: int = None,
    count: bool = False,
    query: typing.List[typing.Dict[str, str]] = None,
):
    table = backend.db[model.get_type_value()]

    search_expressions = []
    query = query or []
    for qp in query:
        if qp['key'] not in model.flatprops:
            raise HTTPException(status_code=400, detail=f"Unknown property {qp['key']!r}.")

        prop = model.flatprops[qp['key']]

        # for search to work on MongoDB, values must be compatible for
        # Mongo's BSON consumption, thus we need to use chained load and prepare
        value = load(context, prop.type, qp['value'])
        value = prepare(context, prop.type, backend, value)

        # in case value is not a string - then just search for that value directly
        if isinstance(value, str):
            re_value = re.compile('^' + value + '$', re.IGNORECASE)
        else:
            re_value = value

        operator = qp.get('operator')

        if operator == 'exact':
            search_expressions.append({
                qp['key']: re_value
            })
        elif operator == 'gt':
            search_expressions.append({
                qp['key']: {
                    '$gt': re_value
                }
            })
        elif operator == 'gte':
            search_expressions.append({
                qp['key']: {
                    '$gte': re_value
                }
            })
        elif operator == 'lt':
            search_expressions.append({
                qp['key']: {
                    '$lt': re_value
                }
            })
        elif operator == 'lte':
            search_expressions.append({
                qp['key']: {
                    '$lte': re_value
                }
            })
        elif operator == 'ne':
            # MongoDB's $ne operator does not consume regular expresions for values,
            # whereas `$not` requires an expression.
            # Thus if our search value is regular expression - search with $not, if
            # not - use $ne
            if isinstance(re_value, re.Pattern):
                search_expressions.append({
                    qp['key']: {
                        '$not': re_value
                    }
                })
            else:
                search_expressions.append({
                    qp['key']: {
                        '$ne': re_value
                    }
                })
        elif operator == 'contains':
            try:
                re_value = re.compile(value, re.IGNORECASE)
            except TypeError:
                # in case value is not a string - then just search for that value directly
                re_value = value

            search_expressions.append({
                qp['key']: re_value
            })
        elif operator == 'startswith':
            # https://stackoverflow.com/a/3483399
            try:
                re_value = re.compile('^' + value + '.*', re.IGNORECASE)
            except TypeError:
                # in case value is not a string - then just search for that value directly
                re_value = value

            search_expressions.append({
                qp['key']: re_value
            })

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
        cursor = cursor.sort([
            (
                sort_key['name'],
                pymongo.ASCENDING if sort_key['ascending'] else
                pymongo.DESCENDING,
            )
            for sort_key in sort
        ])

    for row in cursor:
        yield prepare(context, action, model, backend, row, show=show)


@wipe.register()
def wipe(context: Context, model: Model, backend: Mongo):
    authorize(context, Action.WIPE, model)
    table = backend.db[model.get_type_value()]
    return table.delete_many({})


@prepare.register()
def prepare(context: Context, type: Date, backend: Mongo, value: date) -> datetime:
    # prepares date values for Mongo store, they must be converted to datetime
    return datetime(value.year, value.month, value.day)
