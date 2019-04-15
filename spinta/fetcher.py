from contextlib import contextmanager
from pathlib import Path

from spinta.utils.refs import get_ref_id
from spinta.components import Context


def _get_cache_path(base: Path, key: str) -> Path:
    return base / key[:2] / key[2:4] / key[4:]


class Cache:

    def __init__(self, path: Path):
        self.path = path

    def get(self, key: object):
        key = get_ref_id(key)
        path = _get_cache_path(self.path, key)
        if path.exists():
            return path

    @contextmanager
    def set(self, key: object, *, text: bool = False):
        key = get_ref_id(key)
        path = _get_cache_path(self.path, key)
        path.parent.mkdir(parents=True, exist_ok=True)
        mode = 'wt' if text else 'wb'
        encoding = 'utf-8' if text else None
        with path.open(mode, encoding=encoding) as f:
            yield f


def fetch(context: Context, url: str, *, text: bool = False) -> Path:
    cache = context.get('cache')
    path = cache.get(url)
    if path is not None:
        return path

    requests = context.get('requests')
    with requests.get(url, stream=True) as r:
        with cache.set(url, text=text) as f:
            chunks = r.iter_content(chunk_size=8192, decode_unicode=text)
            for chunk in filter(None, chunks):
                f.write(chunk)

    return cache.get(url)
