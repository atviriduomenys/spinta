from pathlib import Path

from spinta.core.ufuncs import Bind
from spinta.core.ufuncs import Expr
from spinta.core.ufuncs import ufunc
from spinta.datasets.backends.sqldump.ufuncs.components import File
from spinta.datasets.backends.sqldump.ufuncs.components import PrepareFileResource
from spinta.exceptions import UnknownBind


@ufunc.resolver(PrepareFileResource, Expr)
def file(env: PrepareFileResource, expr: Expr) -> File:
    args, kwargs = expr.resolve(env)
    return env.call('file', *args, **kwargs)


@ufunc.resolver(PrepareFileResource, Bind)
def file(env: PrepareFileResource, path: Bind, **kwargs) -> File:
    if path.name == 'self':
        return env.call('file', env.path, **kwargs)
    else:
        raise UnknownBind(name=path.name)


@ufunc.resolver(PrepareFileResource, Path)
def file(env: PrepareFileResource, path: Path, *, encoding='utf-8') -> File:
    return File(path, encoding=encoding)
