from __future__ import annotations

from typing import List, Any

from spinta import commands
from spinta.components import Context
from spinta.datasets.backends.sql.components import Sql
from spinta.types.datatype import Array


@commands.cast_backend_to_python.register(Context, Array, Sql, list)
def cast_backend_to_python(context: Context, dtype: Array, backend: Sql, data: List[Any], **kwargs) -> List[Any]:
    # Edge case, when using intermediate table with Sql backend, None values get returned as [None] instead of None
    if dtype.model is not None and len(data) == 1 and data[0] is None:
        return commands.cast_backend_to_python(context, dtype.items.dtype, backend, None, **kwargs)

    if data and dtype:
        data = [commands.cast_backend_to_python(context, dtype.items.dtype, backend, v, **kwargs) for v in data]
        if all(v is None for v in data):
            return None
        return data

    return data
