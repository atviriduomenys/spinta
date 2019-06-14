import contextlib
import re
import typing

from datetime import date, datetime

import pymongo
from starlette.exceptions import HTTPException

from spinta.backends import Backend, check_model_properties
from spinta.commands import load, prepare, migrate, check, push, getone, getall, wipe, wait, authorize, dump, gen_object_id
from spinta.components import Context, Manifest, Model, Property, Action
from spinta.config import RawConfig
from spinta.types.type import Date
from spinta.utils.idgen import get_new_id
from spinta.utils.nestedstruct import get_nested_property_type, build_show_tree
from spinta.exceptions import NotFound


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
def push(context: Context, model: Model, backend: Mongo, data: dict, *, action: Action):
    authorize(context, action, model, data=data)

    # load and check if data is a valid for it's model
    data = load(context, model, data)
    check(context, model, backend, data, action=action)
    data = prepare(context, model, data, action=action)

    # Push data to Mongo backend, this can be an insert, update or delete. If
    # `id` is not given, it is an insert if `id` is given, it is an update.
    #
    # Deletes are not yet implemented, but for deletes `data` must equal to
    # `{'id': 1, _delete: True}`.
    #
    # Also this must return inserted/updated/deleted id.
    #
    # Also this command must write information to changelog after change is
    # done.
    transaction = context.get('transaction')  # noqa
    model_collection = backend.db[model.get_type_value()]

    # FIXME: before creating revision check if there's not collision clash
    revision_id = get_new_id('revision id')
    data['revision'] = revision_id

    if action == Action.INSERT:
        data['id'] = gen_object_id(context, backend, model)
        model_collection.insert_one(data)
    elif action == Action.UPDATE or action == Action.PATCH:
        result = model_collection.update_one(
            {'id': data['id']},
            {'$set': data}
        )
        assert result.matched_count == 1 and result.modified_count == 1, (
            f"matched: {result.matched_count}, modified: {result.modified_count}"
        )
    elif action == Action.DELETE:
        model_collection.delete_one({'id': data['id']})
    else:
        raise Exception(f"Unknown action: {action!r}.")

    return prepare(context, action, model, backend, data)


@getone.register()
def getone(context: Context, model: Model, backend: Mongo, id: str, *, prop: Property = None):
    authorize(context, Action.GETONE, model)
    model_collection = backend.db[model.get_type_value()]
    if prop:
        row = model_collection.find_one({"id": id}, {prop.name: 1})
    else:
        row = model_collection.find_one({"id": id})
    if row is None:
        model_type = model.get_type_value()
        raise NotFound(f"Model {model_type!r} with id {id!r} not found.")
    if prop:
        return dump(context, backend, prop.type, row.get(prop.name))
    else:
        return prepare(context, Action.GETONE, model, backend, row)


# TODO: refactor keyword arguments into a list of query parameters, like `query_params`
@getall.register()
def getall(
    context: Context, model: Model, backend: Mongo, *,
    show: typing.List[str] = None,
    sort: typing.List[typing.Dict[str, str]] = None,
    offset=None, limit=None,
    count: bool = False,
    query_params: typing.List[typing.Dict[str, str]] = None,
    search: bool = False,
):
    if query_params is None:
        query_params = []

    action = Action.SEARCH if search else Action.GETALL

    authorize(context, action, model)

    # Yield all available entries.
    model_collection = backend.db[model.get_type_value()]

    search_expressions = []
    for qp in query_params:
        value_type = get_nested_property_type(model.properties, qp['key'])

        if value_type is None:
            # FIXME: proper error message
            raise HTTPException(status_code=400, detail="attribute does not exist")
        else:
            # for search to work on MongoDB, values must be compatible for
            # Mongo's BSON consumption, thus we need to use chained load and prepare
            value = load(context, value_type, qp['value'])
            value = prepare(context, value_type, backend, value)

        # in case value is not a string - then just search for that value directly
        if isinstance(value, str):
            re_value = re.compile('^' + value + '$', re.IGNORECASE)
        else:
            re_value = value

        if qp.get('operator') == 'exact':
            search_expressions.append({
                qp['key']: re_value
            })
        elif qp.get('operator') == 'gt':
            search_expressions.append({
                qp['key']: {
                    '$gt': re_value
                }
            })
        elif qp.get('operator') == 'gte':
            search_expressions.append({
                qp['key']: {
                    '$gte': re_value
                }
            })
        elif qp.get('operator') == 'lt':
            search_expressions.append({
                qp['key']: {
                    '$lt': re_value
                }
            })
        elif qp.get('operator') == 'lte':
            search_expressions.append({
                qp['key']: {
                    '$lte': re_value
                }
            })
        elif qp.get('operator') == 'ne':
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
        elif qp.get('operator') == 'contains':
            try:
                re_value = re.compile(value, re.IGNORECASE)
            except TypeError:
                # in case value is not a string - then just search for that value directly
                re_value = value

            search_expressions.append({
                qp['key']: re_value
            })
        elif qp.get('operator') == 'startswith':
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
        # TODO: implement `$or` operator support
        operator = '$and'
        search_query[operator] = search_expressions

    cursor = model_collection.find(search_query)

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

    transaction = context.get('transaction')
    # Delete all data for a given model
    model_collection = backend.db[model.get_type_value()]
    return model_collection.delete_many({})


@prepare.register()
def prepare(context: Context, type: Date, backend: Mongo, value: date) -> datetime:
    # prepares date values for Mongo store, they must be converted to datetime
    return datetime(value.year, value.month, value.day)


@prepare.register()
def prepare(context: Context, action: Action, model: Model, backend: Mongo, value: dict, *,
            show: typing.List[str] = None) -> dict:

    # FIXME: whole this function is mostly identical to a one for postgresql.

    if action in (Action.GETALL, Action.SEARCH, Action.GETONE):
        value = {**value, 'type': model.get_type_value()}
        result = {}

        if show is not None:
            unknown_properties = set(show) - {
                name
                for name, prop in model.flatprops.items()
                if not prop.hidden
            }
            if unknown_properties:
                raise NotFound("Unknown properties for show: %s" % ', '.join(sorted(unknown_properties)))
            show = build_show_tree(show)

        for prop in model.properties.values():
            if prop.hidden:
                continue
            if show is None or prop.place in show:
                result[prop.name] = dump(context, backend, prop.type, value.get(prop.name), show=show)

        return result

    elif action in (Action.INSERT, Action.UPDATE):
        result = {}
        for prop in model.properties.values():
            if prop.hidden:
                continue
            result[prop.name] = dump(context, backend, prop.type, value.get(prop.name))
        result['type'] = model.get_type_value()
        return result

    elif action == Action.PATCH:
        result = {}
        for k, v in value.items():
            prop = model.properties[k]
            result[prop.name] = dump(context, backend, prop.type, v)
        result['type'] = model.get_type_value()
        return result

    elif action == Action.DELETE:
        return {
            'id': value['id'],
            'type': model.get_type_value(),
        }

    else:
        raise Exception(f"Unknown action {action}.")
