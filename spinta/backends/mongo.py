import contextlib

import pymongo
from bson.objectid import ObjectId

from spinta.commands import load, prepare, migrate, check, push, get, getall, wipe
from spinta.components import Context, BackendConfig, Manifest, Model
from spinta.backends import Backend


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
def load(context: Context, backend: Mongo, config: BackendConfig):
    # Load Mongo client using configuration.
    backend.name = config.name
    backend.db_name = config.db_name
    backend.client = pymongo.MongoClient(config.dsn)
    backend.db = backend.client[backend.db_name]


@prepare.register()
def prepare(context: Context, backend: Mongo, manifest: Manifest):
    # Mongo does not need any table or database preparations
    pass


@migrate.register()
def migrate(context: Context, backend: Mongo):
    # Migrate schema changes.
    pass


@check.register()
def check(context: Context, model: Model, backend: Mongo, data: dict):
    # Check data before insert/update.
    transaction = context.get('transaction')


@push.register()
def push(context: Context, model: Model, backend: Mongo, data: dict):
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
    data_id = model_collection.insert_one(data).inserted_id
    return data_id


@get.register()
def get(context: Context, model: Model, backend: Mongo, id: ObjectId):
    transaction = context.get('transaction')
    model_collection = backend.db[model.get_type_value()]
    return model_collection.find_one({"_id": ObjectId(id)})


@getall.register()
def getall(context: Context, model: Model, backend: Mongo):
    transaction = context.get('transaction')
    # Yield all available entries.


@wipe.register()
def wipe(context: Context, model: Model, backend: Mongo):
    transaction = context.get('transaction')
    # Delete all data for a given model
    model_collection = backend.db[model.get_type_value()]
    return model_collection.delete_many({})
