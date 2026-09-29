from typing import Any, Dict

from spinta import commands
from spinta.components import Context
from spinta.datasets.backends.notimpl.components import BackendNotImplemented


@commands.load.register(Context, BackendNotImplemented, dict)
def load(
    context: Context,
    backend: BackendNotImplemented,
    config: Dict[str, Any],
) -> None:
    pass
