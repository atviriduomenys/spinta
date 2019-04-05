from tempfile import NamedTemporaryFile
from contextlib import contextmanager

import requests

from spinta.commands import Command
from spinta.backends.postgresql.cache import Cache


class HttpProtocol(Command):
    metadata = {
        'name': 'read.http',
        'components': {
            (Dataset, ''),
            (requests.Session, ''),
            (Cache, ''),
        },
    }

    @contextmanager
    def execute(self):
        url = self.args.url
        text = self.args.text
        mode = 'w+t' if text else 'w+b'
        encoding = 'utf-8' if text else None
        with NamedTemporaryFile(mode, encoding=encoding) as f:
            cache = self.components.get(Cache)
            cached = cache.get(url)
            with cached:
                for chunk in cached:
                    f.write(chunk)
            if not cached:
                session = self.components.get(requests.Session)
                with session.get(url, stream=True) as r:
                    with cache.create(r) as c:
                        chunks = r.iter_content(chunk_size=8192, decode_unicode=text)
                        for chunk in filter(None, chunks):
                            f.write(chunk)
                            c.write(chunk)
            f.seek(0)
            yield f
