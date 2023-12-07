from typing import Dict, Any

from spinta import commands
from spinta.backends.nobackend.components import NoBackend
from spinta.components import Context


@commands.load.register(Context, NoBackend, dict)
def load(
    context: Context,
    backend: NoBackend,
    config: Dict[str, Any],
) -> None:
    pass
