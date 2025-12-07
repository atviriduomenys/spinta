from typing import Any
from typing import overload

from spinta import commands
from spinta.components import Context
from spinta.core.ufuncs import Expr
from spinta.core.ufuncs import NoOp
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
) -> None:
    raise InvalidValue(
        dtype,
        error=(
            f"Given enum value {value} of {type(value)} type does not match property type, which is {dtype.name!r}."
        ),
    )


@overload
@commands.check.register(Context, EnumItem, DataType, NoOp)
def check(
    context: Context,
    item: EnumItem,
    dtype: DataType,
    value: NoOp,
) -> None:
    pass


@overload
@commands.check.register(Context, EnumItem, DataType, Expr)
def check(
    context: Context,
    item: EnumItem,
    dtype: DataType,
    value: Expr,
):
    pass


@overload
@commands.check.register(Context, EnumItem, DataType, type(None))
def check(
    context: Context,
    item: EnumItem,
    dtype: DataType,
    value: None,
) -> None:
    pass


@overload
@commands.check.register(Context, EnumItem, DataType, NotAvailable)
def check(
    context: Context,
    item: EnumItem,
    dtype: DataType,
    value: NotAvailable,
) -> None:
    pass


@overload
@commands.check.register(Context, EnumItem, Integer, int)
def check(
    context: Context,
    item: EnumItem,
    dtype: Integer,
    value: int,
) -> None:
    pass


@overload
@commands.check.register(Context, EnumItem, Boolean, bool)
def check(
    context: Context,
    item: EnumItem,
    dtype: Boolean,
    value: bool,
) -> None:
    pass


@overload
@commands.check.register(Context, EnumItem, String, str)
def check(
    context: Context,
    item: EnumItem,
    dtype: String,
    value: str,
) -> None:
    pass
