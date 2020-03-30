from spinta import commands
from spinta.core.config import RawConfig
from spinta.components import Context
from spinta.backends.mongo.components import Mongo


@commands.wait.register()
def wait(context: Context, backend: Mongo, rc: RawConfig, *, fail: bool = False):
    return True
