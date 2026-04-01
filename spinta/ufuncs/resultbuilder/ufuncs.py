import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import binascii

from spinta.core.ufuncs import ufunc, Expr
from spinta.exceptions import InvalidBase64String, NotImplementedFeature, UnableToCast
from spinta.types.datatype import String, Binary, Integer, Date, Time, DateTime, Number, Boolean, DataType
from spinta.types.geometry.components import Geometry
from spinta.ufuncs.querybuilder.components import Selected
from spinta.ufuncs.resultbuilder.components import ResultBuilder

import shapely
from shapely.geometry.base import BaseGeometry

from spinta.utils.config import asbool


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


@ufunc.resolver(ResultBuilder)
def cast(env: ResultBuilder) -> Any:
    return env.call("cast", env.prop.dtype, env.this)


@ufunc.resolver(ResultBuilder, Expr)
def cast(env: ResultBuilder, expr: Expr) -> Any:
    return env.call("cast", env.prop.dtype, env.this)


@ufunc.resolver(ResultBuilder, String, int)
def cast(env: ResultBuilder, dtype: String, value: int) -> str:
    return str(value)


@ufunc.resolver(ResultBuilder, String, type(None))
def cast(env: ResultBuilder, dtype: String, value: None) -> str:
    return ""


@ufunc.resolver(ResultBuilder, Integer, Decimal)
def cast(env: ResultBuilder, dtype: Integer, value: Decimal) -> int:
    return env.call("cast", dtype, float(value))


@ufunc.resolver(ResultBuilder, Integer, float)
def cast(env: ResultBuilder, dtype: Integer, value: float) -> int:
    if value % 1 > 0:
        raise UnableToCast(dtype, value=value, type=dtype.name)
    else:
        return int(value)


@ufunc.resolver(ResultBuilder, String, str)
def cast(env: ResultBuilder, dtype: String, value: str) -> str:
    return value


@ufunc.resolver(ResultBuilder, Integer, str)
def cast(env: ResultBuilder, dtype: Integer, value: str) -> int:
    if value.removeprefix("-").isdigit() or value.removeprefix("+").isdigit():
        return int(value)
    else:
        raise UnableToCast(dtype, value=value, type=dtype.name)


@ufunc.resolver(ResultBuilder, Number, str)
def cast(env: ResultBuilder, dtype: Number, value: str) -> float:
    try:
        return float(Decimal(value))
    except InvalidOperation:
        raise UnableToCast(dtype, value=value, type=dtype.name)


@ufunc.resolver(ResultBuilder, Boolean, str)
def cast(env: ResultBuilder, dtype: Boolean, value: str) -> bool:
    try:
        return asbool(value)
    except ValueError:
        raise UnableToCast(dtype, value=value, type=dtype.name)


@ufunc.resolver(ResultBuilder, Date, str)
def cast(env: ResultBuilder, dtype: Date, value: str) -> datetime.date:
    try:
        return datetime.date.fromisoformat(value)
    except ValueError:
        pass

    try:
        return datetime.datetime.fromisoformat(value).date()
    except ValueError:
        raise UnableToCast(dtype, value=value, type=dtype.name)


@ufunc.resolver(ResultBuilder, Time, str)
def cast(env: ResultBuilder, dtype: Time, value: str) -> datetime.time:
    try:
        return datetime.time.fromisoformat(value)
    except ValueError:
        pass

    try:
        return datetime.datetime.fromisoformat(value).time()
    except ValueError:
        raise UnableToCast(dtype, value=value, type=dtype.name)


@ufunc.resolver(ResultBuilder, DateTime, str)
def cast(env: ResultBuilder, dtype: DateTime, value: str) -> datetime.datetime:
    try:
        return datetime.datetime.fromisoformat(value)
    except ValueError:
        raise UnableToCast(dtype, value=value, type=dtype.name)


@ufunc.resolver(ResultBuilder, DataType, object)
def cast(env: ResultBuilder, dtype: DataType, value: Any) -> None:
    raise NotImplementedFeature(dtype, feature=f'Prepare method "cast()" for data type {dtype.name}')
