from typing import Any
from typing import Dict
from typing import overload

from spinta import commands
from spinta.components import Action
from spinta.components import Context
from spinta.formats.components import Format
from spinta.formats.html.components import Cell
from spinta.formats.html.components import Html
from spinta.types.text.components import Text


@overload
@commands.prepare_dtype_for_response.register(Context, Format, Text, dict)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: Text,
    value: Dict[str, str],
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return value


@overload
@commands.prepare_dtype_for_response.register(Context, Html, Text, dict)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: Text,
    value: Dict[str, str],
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return {
        k: Cell(v)
        for k, v in value.items()
    }
