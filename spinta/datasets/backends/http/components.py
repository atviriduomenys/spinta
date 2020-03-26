from typing import IO

import pathlib
import contextlib

import requests

from spinta.utils.refs import get_ref_id
from spinta.external import ExternalBackend


class Cache:
    path: pathlib.Path

    def __init__(self, path: pathlib.Path):
        self.path = path

    def getpath(self, key):
        return self.path / key[:2] / key[2:4] / key[4:]

    def get(self, key: object):
        key = get_ref_id(key)
        path = self.getpath(key)
        if path.exists():
            return path

    @contextlib.contextmanager
    def set(self, key: object, *, text: bool = False):
        key = get_ref_id(key)
        path = self.getpath(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        mode = 'wt' if text else 'wb'
        encoding = 'utf-8' if text else None
        with path.open(mode, encoding=encoding) as f:
            yield f


class Http(ExternalBackend):
    cache: Cache
    session: requests.Session

    def open(self, url: str, *, text: bool = False) -> IO:
        path = self.cache.get(url)
        if path is None:
            with self.session.get(url, stream=True) as r:
                with self.cache.set(url, text=text) as f:
                    chunks = r.iter_content(chunk_size=8192, decode_unicode=text)
                    for chunk in filter(None, chunks):
                        f.write(chunk)
            path = self.cache.get(url)

        if text:
            return path.open('rt')
        else:
            return path.open('rb')
