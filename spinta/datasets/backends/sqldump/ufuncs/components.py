from dataclasses import dataclass
from pathlib import Path

from spinta.core.ufuncs import Env


class PrepareFileResource(Env):
    path: Path

    def init(self, path: Path):
        return self(path=path)


@dataclass
class File:
    path: Path = None
    encoding: str = 'utf-8'

    def open(self):
        return open(self.path, 'r', encoding=self.encoding)
