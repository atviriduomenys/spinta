from typing import Any
from typing import overload

from spinta import commands
from spinta.components import Context
from spinta.types.datatype import DataType
from spinta.types.datatype import Integer
from spinta.types.datatype import String
from spinta.types.datatype import Boolean
from spinta.dimensions.enum.components import EnumItem
from spinta.exceptions import InvalidValue
from spinta.utils.schema import NotAvailable


@overload
@commands.check.register(Context, EnumItem, DataType, object)
def check(
    context: Context,
    item: EnumItem,
    dtype: DataType,
    value: Any,
):
    raise InvalidValue(dtype, error=(
        f"Given enum value {value} of {type(value)} type does not match "
        f"property type, which is {dtype.name!r}."
    ))


@overload
@commands.check.register(Context, EnumItem, DataType, type(None))
def check(
    context: Context,
    item: EnumItem,
    dtype: DataType,
    value: None,
):
    pass


@overload
@commands.check.register(Context, EnumItem, DataType, NotAvailable)
def check(
    context: Context,
    item: EnumItem,
    dtype: DataType,
    value: NotAvailable,
):
    pass


@overload
@commands.check.register(Context, EnumItem, Integer, int)
def check(
    context: Context,
    item: EnumItem,
    dtype: Integer,
    value: int,
):
    pass


@overload
@commands.check.register(Context, EnumItem, Boolean, bool)
def check(
    context: Context,
    item: EnumItem,
    dtype: Boolean,
    value: bool,
):
    pass


@overload
@commands.check.register(Context, EnumItem, String, str)
def check(
    context: Context,
    item: EnumItem,
    dtype: Integer,
    value: str,
):
    pass
