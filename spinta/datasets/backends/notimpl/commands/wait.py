from spinta import commands
from spinta.components import Context
from spinta.datasets.backends.notimpl.components import BackendNotImplemented


@commands.wait.register(Context, BackendNotImplemented)
def wait(
    context: Context,
    backend: BackendNotImplemented,
    *,
    fail: bool = False,
) -> bool:
    return True
