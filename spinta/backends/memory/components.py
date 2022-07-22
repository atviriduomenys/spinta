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
    def begin(self):
        yield self

    def bootstrapped(self):
        return True

    def create(self, table: str):
        if table not in self.data:
            self.data[table] = {}

    def insert(self, *objects: ObjectData):
        for obj in objects:
            obj = obj.copy()

            table = obj['_type']
            if table not in self.data:
                raise RuntimeError(f"Table {table!r} does not exist.")

            pk = obj['_id']
            self.data[table][pk] = obj
