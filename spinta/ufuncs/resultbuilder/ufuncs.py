from spinta.core.ufuncs import ufunc, Expr
from spinta.types.geometry.components import Geometry
from spinta.ufuncs.querybuilder.components import Selected
from spinta.ufuncs.resultbuilder.components import ResultBuilder

import shapely
from shapely.geometry.base import BaseGeometry


@ufunc.resolver(ResultBuilder)
def count(env: ResultBuilder):
    pass


@ufunc.resolver(ResultBuilder, Selected, Selected)
def point(env: ResultBuilder, x: Selected, y: Selected) -> str:
    x = env.data[x.item]
    y = env.data[y.item]
    return f"POINT ({x} {y})"


@ufunc.resolver(ResultBuilder, Expr)
def split(env: ResultBuilder, expr: Expr):
    args, kwargs = expr.resolve(env)
    return env.call("split", *args, **kwargs)


@ufunc.resolver(ResultBuilder, str)
def split(env: ResultBuilder, separator: str):
    return env.this.split(separator)


@ufunc.resolver(ResultBuilder, Expr)
def flip(env: ResultBuilder, expr: Expr):
    args, kwargs = expr.resolve(env)
    return env.call("flip", *args, **kwargs)


@ufunc.resolver(ResultBuilder)
def flip(env: ResultBuilder):
    return env.call("flip", env.prop.dtype, env.this)


@ufunc.resolver(ResultBuilder, Geometry, str)
def flip(env: ResultBuilder, dtype: Geometry, value: str):
    values = value.split(";", 1)
    shape: BaseGeometry = shapely.wkt.loads(values[-1])
    inverted: BaseGeometry = shapely.ops.transform(lambda x, y: (y, x), shape)
    inverted: str = shapely.wkt.dumps(inverted, trim=True)
    if len(values) > 1:
        return ";".join([*values[:-1], inverted])
    return inverted


@ufunc.resolver(ResultBuilder, Expr)
def swap(env: ResultBuilder, expr: Expr):
    args, kwargs = expr.resolve(env)
    return env.call("swap", *args, *kwargs)
