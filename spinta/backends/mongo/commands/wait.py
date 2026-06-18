from spinta import commands
from spinta.backends.mongo.components import Mongo
from spinta.components import Context


@commands.wait.register(Context, Mongo)
def wait(context: Context, backend: Mongo, *, fail: bool = False) -> bool:
    # TODO: Implement real check.
    return True
