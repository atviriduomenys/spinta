import contextlib

import pymongo

from spinta.backends.components import BackendFeatures
from spinta.components import Model
from spinta.components import Property
from spinta.backends.components import Backend


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

    features = {
        BackendFeatures.WRITE,
    }

    @contextlib.contextmanager
    def transaction(self, write=False):
        with self.begin() as connection:
            if write:
                # TODO: get a real transaction id
                transaction_id = 1
                yield WriteTransaction(connection, transaction_id)
            else:
                yield ReadTransaction(connection)

    @contextlib.contextmanager
    def begin(self):
        yield self

    def get_table(
        self,
        model: Model,
        name: str = None,
    ) -> pymongo.collection.Collection:
        return self.db[model.model_type()]

    def get_column(
        self,
        table: pymongo.collection.Collection,
        prop: Property,
        *,
        select=False,
    ) -> str:
        return prop.place


class ReadTransaction:

    def __init__(self, connection):
        self.connection = connection


class WriteTransaction(ReadTransaction):

    def __init__(self, connection, id):
        super().__init__(connection)
        self.id = id
        self.errors = 0
