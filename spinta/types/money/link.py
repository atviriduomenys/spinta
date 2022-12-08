from spinta import commands
from spinta.components import Context
from spinta.exceptions import UndefinedEnum
from spinta.types.money.components import Money

from spinta.types.helpers import set_dtype_backend


@commands.link.register(Context, Money)
def link(context: Context, dtype: Money) -> None:
    set_dtype_backend(dtype)
    enum = dtype.prop.given.enum
    if enum:
        if len(enum) > 3:
            raise(UndefinedEnum(dtype.prop, name=enum))
