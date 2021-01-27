import contextlib
from pathlib import Path
from typing import TextIO

from spinta.datasets.components import ExternalBackend


class SqlDump(ExternalBackend):
    """
    If SQL dump is given via stdin, then path is None and stream is assigned to
    stdin. Otherwise stream is None and path should be defined.
    """

    type: str = 'sqldump'
    path: Path = None
    stream: TextIO = None

    @contextlib.contextmanager
    def transaction(self, write=False):
        raise NotImplementedError

    @contextlib.contextmanager
    def begin(self):
        raise NotImplementedError

