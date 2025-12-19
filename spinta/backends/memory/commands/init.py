from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_identifier
from spinta.backends.memory.components import Memory


@commands.prepare.register(Context, Memory, Manifest)
def prepare(context: Context, backend: Memory, manifest: Manifest, **kwargs):
    for model in commands.get_models(context, manifest).values():
        backend.create(get_table_identifier(model).logical_qualified_name)
        backend.create(get_table_identifier(model, TableType.CHANGELOG).logical_qualified_name)
