from tempfile import NamedTemporaryFile
from contextlib import contextmanager

import requests

from spinta.protocols import Protocol


class HttpProtocol(Protocol):

    def __init__(self):
        self._session = requests.Session()

    @contextmanager
    def open(self, url, text=False, cache=True):
        mode = 'w+t' if text else 'w+b'
        encoding = 'utf-8' if text else None
        with NamedTemporaryFile(mode, encoding=encoding) as f:
            with self._session.get(url, stream=True) as r:
                chunks = r.iter_content(chunk_size=8192, decode_unicode=text)
                for chunk in filter(None, chunks):
                    f.write(chunk)
            f.seek(0)
            yield f
