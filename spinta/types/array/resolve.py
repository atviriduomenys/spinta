from __future__ import annotations

from spinta import commands
from spinta.components import Property
from spinta.types.datatype import Array


@commands.resolve_property.register(Array, str)
def resolve_property(dtype: Array, prop: str) -> Property | None:
    return commands.resolve_property(dtype.items, prop)
