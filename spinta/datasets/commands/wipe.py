from spinta import commands
from spinta.components import Context
from spinta.datasets.components import Entity, ExternalBackend


@commands.wipe.register(Context, Entity, ExternalBackend)
def wipe(context: Context, entity: Entity, backend: ExternalBackend):
    raise NotImplementedError
