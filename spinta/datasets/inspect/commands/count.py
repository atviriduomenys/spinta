from spinta import commands
from spinta.backends import Backend
from spinta.components import Context, Model, Property
from spinta.dimensions.enum.components import EnumItem

# Backends, that do not support counting, leave `count` empty.


@commands.count_rows.register(Context, Backend, Model)
def count_rows(context: Context, backend: Backend, model: Model) -> None:
    return None


@commands.count_values.register(Context, Backend, Model, Property)
def count_values(context: Context, backend: Backend, model: Model, prop: Property) -> None:
    return None


@commands.count_enum_value.register(Context, Backend, Model, Property, EnumItem)
def count_enum_value(
    context: Context,
    backend: Backend,
    model: Model,
    prop: Property,
    item: EnumItem,
) -> None:
    return None
