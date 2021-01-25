from spinta import commands
from spinta.components import Context
from spinta.backends.mongo.components import Mongo


@commands.wait.register(Context, Mongo)
def wait(context: Context, backend: Mongo, *, fail: bool = False) -> bool:
    # TODO: Implement real check.
    return True
