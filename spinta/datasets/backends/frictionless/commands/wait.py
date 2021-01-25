from spinta import commands
from spinta.components import Context
from spinta.datasets.backends.frictionless.components import FrictionlessBackend


@commands.wait.register(Context, FrictionlessBackend)
def wait(context: Context, backend: FrictionlessBackend, *, fail: bool = False) -> bool:
    return True
