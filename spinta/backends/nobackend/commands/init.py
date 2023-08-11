from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.nobackend.components import NoBackend


@commands.prepare.register(Context, NoBackend, Manifest)
def prepare(context: Context, backend: NoBackend, manifest: Manifest):
    for model in manifest.models.values():
        backend.create(get_table_name(model))
        backend.create(get_table_name(model, TableType.CHANGELOG))
