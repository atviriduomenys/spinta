from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.memory.components import Memory


@commands.prepare.register(Context, Memory, Manifest)
def prepare(context: Context, backend: Memory, manifest: Manifest):
    for model in manifest.models.values():
        backend.create(get_table_name(model))
        backend.create(get_table_name(model, TableType.CHANGELOG))
