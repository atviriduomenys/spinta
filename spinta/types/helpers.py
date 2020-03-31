from __future__ import annotations

from typing import TYPE_CHECKING, Iterable

from spinta import exceptions

if TYPE_CHECKING:
    from spinta.types.datatype import DataType


def check_no_extra_keys(dtype: DataType, schema: Iterable, data: Iterable):
    unknown = set(data) - set(schema)
    if unknown:
        raise exceptions.MultipleErrors(
            exceptions.FieldNotInResource(
                dtype.prop,
                property=f'{dtype.prop.place}.{prop}',
            )
            for prop in sorted(unknown)
        )
