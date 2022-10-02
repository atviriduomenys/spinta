import datetime
import uuid
from typing import Dict

import contextlib

from spinta.typing import ObjectData
from spinta.backends.components import Backend
from spinta.backends.components import BackendFeatures


class Memory(Backend):
    metadata = {
        'name': 'memory',
    }

    features = {
        BackendFeatures.WRITE,
    }

    data: Dict[
        str,                # table
        Dict[
            str,            # id
            ObjectData,     # data
        ]
    ]

    def __init__(self):
        self.data = {}

    @contextlib.contextmanager
    def transaction(self, write=False):
        if write:
            obj = self.insert({
                '_type': '_txn',
                'datetime': datetime.datetime.utcnow(),
                'client_type': '',
                'client_id': '',
                'errors': 0,
            })
            yield WriteTransaction(self, obj['_id'])
        else:
            yield ReadTransaction(self)

    @contextlib.contextmanager
    def begin(self):
        yield self

    def bootstrapped(self):
        return True

    def create(self, table: str):
        if table not in self.data:
            self.data[table] = {}

    def insert(self, obj: ObjectData):
        obj = obj.copy()

        table = obj['_type']
        if table not in self.data:
            raise RuntimeError(f"Table {table!r} does not exist.")

        if '_id' not in obj:
            obj['_id'] = str(uuid.uuid4())

        pk = obj['_id']
        self.data[table][pk] = obj

        return obj


class ReadTransaction:
    id: str
    errors: int
    connection: Memory

    def __init__(self, connection: Memory):
        self.connection = connection


class WriteTransaction(ReadTransaction):

    def __init__(self, connection: Memory, id_: str):
        super().__init__(connection)
        self.id = id_
        self.errors = 0
