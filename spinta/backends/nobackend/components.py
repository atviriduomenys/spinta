import datetime
import uuid
from typing import Dict

import contextlib

from spinta.typing import ObjectData
from spinta.backends import Backend
from spinta.backends.components import BackendFeatures


class NoBackend(Backend):
    name = 'no backend'
    metadata = {
        'name': 'no backend',
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
