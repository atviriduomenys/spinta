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

    def __init__(self):
        self.data = {}

    @contextlib.contextmanager
    def begin(self):
        yield self

    def bootstrapped(self):
        return True
