import contextlib
from typing import TextIO

from spinta.datasets.components import ExternalBackend


class SqlDump(ExternalBackend):
    type: str = 'sql'
    stream: TextIO

    @contextlib.contextmanager
    def transaction(self, write=False):
        raise NotImplementedError

    @contextlib.contextmanager
    def begin(self):
        raise NotImplementedError

