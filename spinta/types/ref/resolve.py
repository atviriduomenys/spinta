from __future__ import annotations

from spinta import commands
from spinta.components import Property
from spinta.types.datatype import Ref


@commands.resolve_property.register(Ref, str)
def resolve_property(dtype: Ref, prop: str) -> Property | None:
    return commands.resolve_property(dtype.model, prop)
