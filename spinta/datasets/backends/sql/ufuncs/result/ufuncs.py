import base64
from decimal import Decimal
from typing import Any, overload, Optional

import shapely
from shapely.geometry.base import BaseGeometry

from spinta.core.ufuncs import ufunc, Expr
from spinta.datasets.backends.sql.ufuncs.components import FileSelected
from spinta.datasets.backends.sql.ufuncs.result.components import SqlResultBuilder
from spinta.exceptions import UnableToCast
from spinta.types.datatype import Integer, String
from spinta.types.file.components import FileData
from spinta.types.geometry.components import Geometry


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


@ufunc.resolver(SqlResultBuilder, Expr)
def swap(env: SqlResultBuilder, expr: Expr):
    args, kwargs = expr.resolve(env)
    return env.call('swap', *args, *kwargs)


@ufunc.resolver(SqlResultBuilder)
def flip(env: SqlResultBuilder):
    return env.call('flip', env.prop.dtype, env.this)


@ufunc.resolver(SqlResultBuilder, Geometry, str)
def flip(env: SqlResultBuilder, dtype: Geometry, value: str):
    values = value.split(';', 1)
    shape: BaseGeometry = shapely.from_wkt(values[-1])
    inverted: BaseGeometry = shapely.ops.transform(lambda x, y: (y, x), shape)
    inverted: str = shapely.to_wkt(inverted)
    if len(values) > 1:
        return ';'.join([*values[:-1], inverted])
    return inverted
