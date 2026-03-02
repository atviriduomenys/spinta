import binascii
from spinta.components import Property
from spinta.types.datatype import DataType, String, PrimaryKey, Denorm, Ref

from spinta.core.ufuncs import ufunc, Expr,  Negative, Bind, GetAttr
from spinta.exceptions import InvalidBase64String, NotImplementedFeature
from spinta.types.datatype import String, Binary
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
    return env.call("split", env.this, *args, **kwargs)


@ufunc.resolver(ResultBuilder, type(None), str)
def split(env: ResultBuilder, data: type(None), separator: str):
    return None


@ufunc.resolver(ResultBuilder, str, str)
def split(env: ResultBuilder, data: str, separator: str):
    return data.split(separator)


@ufunc.resolver(ResultBuilder, object, str)
def split(env: ResultBuilder, data: object, separator: str):
    if hasattr(data, "split"):
        return data.split(separator)
    raise NotImplementedFeature(env.prop, feature=f"Ability to split '{type(data)}' type")


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


@ufunc.resolver(ResultBuilder, Expr)
def base64(env: ResultBuilder, expr: Expr) -> str:
    return env.call("base64", env.prop.dtype)


@ufunc.resolver(ResultBuilder, String)
def base64(env: ResultBuilder, dtype: String) -> str:
    try:
        return env.call("base64", env.this).decode("utf-8")
    except (binascii.Error, ValueError):
        raise InvalidBase64String(env.prop.model, property=env.prop.name, value=env.this)


@ufunc.resolver(ResultBuilder, Binary)
def base64(env: ResultBuilder, dtype: Binary) -> bytes:
    try:
        return env.call("base64", env.this)
    except (binascii.Error, ValueError):
        raise InvalidBase64String(env.prop.model, property=env.prop.name, value=env.this)

#
# @ufunc.resolver(ResultBuilder, Bind, Bind, name="getattr")
# def getattr_(env: ResultBuilder, field: Bind, attr: Bind):
#     breakpoint()
#     prop = env.prop.model.properties[field.name]
#     return env.call("getattr", prop, attr)
#
#
# @ufunc.resolver(ResultBuilder, Property, Bind, name="getattr")
# def getattr_(env: ResultBuilder, prop: Property, attr: Bind):
#     return env.call("getattr", prop.dtype, attr)
#
#
# @ufunc.resolver(ResultBuilder, DataType, Bind, name="getattr")
# def getattr_(env: ResultBuilder, dtype: DataType, attr: Bind):
#     raise ValueError
#
#
# @ufunc.resolver(ResultBuilder, Ref, Bind, name="getattr")
# def getattr_(env: ResultBuilder, dtype: Ref, attr: Bind):
#     breakpoint()
#     # handle error
#     return GetAttr(obj.name, attr)

# @ufunc.resolver(ResultBuilder, Bind, Bind, name="getattr")
# def getattr_(env: ResultBuilder, field: Bind, attr: Bind):
#     key = f"{field.name}.{attr.name}"
#     prop = env.prop.model.flatprops.get(key)
#     if prop and prop.external and prop.external.name:
#         return env.data.get(prop.external.name)
#     return None
#
# @ufunc.resolver(ResultBuilder, Bind, Bind, Bind, name="getattr")
# def getattr_(env: ResultBuilder, source: Bind, obj: Bind, attr: Bind):
#     raise SourceOrPrepareNotAllowed(source=str(source))
