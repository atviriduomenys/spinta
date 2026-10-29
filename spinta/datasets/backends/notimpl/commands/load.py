from typing import Any

from spinta import commands
from spinta.components import Context
from spinta.datasets.backends.notimpl.components import BackendNotImplemented


@commands.load.register(Context, BackendNotImplemented, dict)
def load(
    context: Context,
    backend: BackendNotImplemented,
    config: dict[str, Any],
) -> None:
    pass
