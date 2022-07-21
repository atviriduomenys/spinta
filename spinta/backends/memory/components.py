from typing import Dict

import contextlib

from spinta.typing import ObjectData
from spinta.components import Model
from spinta.backends.components import Backend
from spinta.backends.components import BackendFeatures


class Memory(Backend):
    metadata = {
        'name': 'memory',
    }

    features = {
        BackendFeatures.WRITE,
    }

    db: Dict[
        str,                # model
        Dict[
            str,            # _id
            ObjectData,     # data
        ]
    ]

    def __init__(self):
        self.db = {}

    @contextlib.contextmanager
    def begin(self):
        yield self

    def bootstrapped(self):
        return True

    def add_model(self, model: Model):
        if model.name not in self.db:
            self.db[model.name] = {}

    def add(self, *objects: ObjectData):
        for obj in objects:
            obj = obj.copy()

            model = obj['_type']
            if model not in self.db:
                raise RuntimeError(f"Model {model} does not exist.")

            pk = obj['_id']
            self.db[model][pk] = obj
