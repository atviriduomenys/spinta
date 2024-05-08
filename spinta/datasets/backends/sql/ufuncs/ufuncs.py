import base64
from decimal import Decimal
from typing import Optional, Any, overload

from spinta.core.ufuncs import ufunc, Bind, Expr
from spinta.datasets.backends.sql.commands.query import SqlQueryBuilder, Selected
from spinta.datasets.backends.sql.ufuncs.components import Engine, SqlResultBuilder, FileSelected
from spinta.datasets.backends.sql.ufuncs.components import SqlResource
from spinta.exceptions import UnknownBind, UnableToCast
from spinta.types.datatype import Integer, String
from spinta.types.file.components import FileData


@ufunc.resolver(SqlResource)
def connect(env: SqlResource, **kwargs) -> Engine:
    for key in kwargs.keys():
        if key not in ("schema", "encoding"):
            raise UnknownBind(name=key)
    return Engine(env.dsn, **kwargs)


@ufunc.resolver(SqlResource, Bind)
def connect(env: SqlResource, bind: Bind, **kwargs) -> Engine:
    if bind.name == 'self':
        return env.call('connect', env.dsn, **kwargs)
    else:
        raise UnknownBind(name=bind.name)


@ufunc.resolver(SqlResource, str)
def connect(env: SqlResource, dsn: str, **kwargs) -> Engine:
    return Engine(dsn, **kwargs)


@overload
@ufunc.resolver(SqlResultBuilder)
def cast(env: SqlResultBuilder) -> Any:
    return env.call('cast', env.prop.dtype, env.this)


@overload
@ufunc.resolver(SqlResultBuilder, String, int)
def cast(env: SqlResultBuilder, dtype: String, value: int) -> str:
    return str(value)


@overload
@ufunc.resolver(SqlResultBuilder, String, type(None))
def cast(env: SqlResultBuilder, dtype: String, value: Optional[Any]) -> str:
    return ''


@overload
@ufunc.resolver(SqlResultBuilder, Integer, Decimal)
def cast(env: SqlResultBuilder, dtype: Integer, value: Decimal) -> int:
    return env.call('cast', dtype, float(value))


@overload
@ufunc.resolver(SqlResultBuilder, Integer, float)
def cast(env: SqlResultBuilder, dtype: Integer, value: float) -> int:
    if value % 1 > 0:
        raise UnableToCast(dtype, value=value, type=dtype.name)
    else:
        return int(value)


@ufunc.resolver(SqlResultBuilder, Expr)
def file(env: SqlResultBuilder, expr: Expr) -> FileData:
    """Post query file data processor

    Will be called with _FileSelected kwargs and no args.
    """
    kwargs: FileSelected
    args, kwargs = expr.resolve(env)
    assert len(args) == 0, args
    name = env.data[kwargs['name'].item]
    content = env.data[kwargs['content'].item]
    if isinstance(content, str):
        content = content.encode('utf-8')
    if content is not None:
        content = base64.b64encode(content).decode()
    return {
        '_id': name,
        # TODO: Content probably should not be returned if not explicitly
        #       requested in select list.
        '_content': content,
    }


@ufunc.resolver(SqlQueryBuilder, Expr)
def swap(env: SqlQueryBuilder, expr: Expr):
    args, kwargs = expr.resolve(env)
    return Expr('swap', *args, **kwargs)


@ufunc.resolver(SqlResultBuilder, Expr)
def swap(env: SqlResultBuilder, expr: Expr):
    args, kwargs = expr.resolve(env)
    return env.call('swap', *args, *kwargs)


@overload
@ufunc.resolver(SqlResultBuilder, Selected, Selected)
def point(env: SqlResultBuilder, x: Selected, y: Selected) -> Expr:
    x = env.data[x.item]
    y = env.data[y.item]
    return f'POINT ({x} {y})'
