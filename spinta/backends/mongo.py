import contextlib
import typing

from datetime import date, datetime

import pymongo
from bson.objectid import ObjectId
from starlette.exceptions import HTTPException

from spinta.backends import Backend, check_model_properties
from spinta.commands import load, prepare, migrate, check, push, get, getall, wipe, wait, authorize, dump
from spinta.components import Context, Manifest, Model, Action
from spinta.config import RawConfig
from spinta.types.type import Date
from spinta.utils.idgen import get_new_id
from spinta.utils.nestedstruct import get_nested_property_type


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
    data = prepare(context, model, data)

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
    transaction = context.get('transaction')
    model_collection = backend.db[model.get_type_value()]

    # FIXME: before creating revision check if there's not collision clash
    revision_id = get_new_id('revision id')
    data['revision'] = revision_id

    if 'id' in data:
        result = model_collection.update_one(
            {'_id': ObjectId(data['id'])},
            {'$set': data}
        )
        assert result.matched_count == 1 and result.modified_count == 1
        data_id = data['id']
    else:
        data_id = model_collection.insert_one(data).inserted_id

    # parse `ObjectId` to string and add it to our object
    data['_id'] = str(data_id)

    return prepare(context, action, model, backend, data)


@get.register()
def get(context: Context, model: Model, backend: Mongo, id: str):
    authorize(context, Action.GETONE, model)
    model_collection = backend.db[model.get_type_value()]
    row = model_collection.find_one({"_id": ObjectId(id)})
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
):
    if query_params is None:
        query_params = []

    authorize(context, Action.GETALL, model)

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

        if qp.get('operator') == 'exact':
            search_expressions.append({
                qp['key']: value
            })
        elif qp.get('operator') == 'gt':
            search_expressions.append({
                qp['key']: {
                    '$gt': value
                }
            })
        elif qp.get('operator') == 'gte':
            search_expressions.append({
                qp['key']: {
                    '$gte': value
                }
            })
        elif qp.get('operator') == 'lt':
            search_expressions.append({
                qp['key']: {
                    '$lt': value
                }
            })
        elif qp.get('operator') == 'lte':
            search_expressions.append({
                qp['key']: {
                    '$lte': value
                }
            })
        elif qp.get('operator') == 'ne':
            search_expressions.append({
                qp['key']: {
                    '$ne': value
                }
            })
        elif qp.get('operator') == 'contains':
            # FIXME: what to do if value for regex search is not a string?
            # Should `find` call be wrapped in try/except?
            search_expressions.append({
                qp['key']: {
                    '$regex': value
                }
            })
        elif qp.get('operator') == 'startswith':
            # https://stackoverflow.com/a/3483399
            search_expressions.append({
                qp['key']: {
                    '$regex': f'^{value}.*',
                }
            })

    search_query = {}
    # search expressions cannot be empty
    if search_expressions:
        # TODO: implement `$or` operator support
        operator = '$and'
        search_query[operator] = search_expressions

    for row in model_collection.find(search_query):
        yield prepare(context, Action.GETALL, model, backend, row, show=show)


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
    if show is None:
        show = ['id']
    if action in (Action.GETALL, Action.GETONE, Action.INSERT, Action.UPDATE):
        result = {}
        for prop in model.properties.values():
            # TODO: `prepare` should be called for each property.
            if action == Action.GETALL:
                if prop.name in show:
                    result[prop.name] = dump(context, backend, prop.type, value.get(prop.name))
            else:
                result[prop.name] = dump(context, backend, prop.type, value.get(prop.name))
                result['type'] = model.get_type_value()
        result['id'] = str(value['_id'])
        return result
    else:
        raise Exception(f"Unknown action {action}.")
