from __future__ import annotations

from spinta import commands
from spinta.components import Context, Property
from spinta.types.datatype import Ref


@commands.resolve_property.register(Context, Ref, str)
def resolve_property(context: Context, dtype: Ref, prop: str) -> Property | None:
    if isinstance(dtype.model, str):
        dtype.model = commands.get_model(context, dtype.prop.model.manifest, dtype.model)

    return commands.resolve_property(dtype.model, prop)
