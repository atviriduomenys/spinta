from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.memory.components import Memory
import yaml


@commands.prepare.register(Context, Memory, Manifest)
def prepare(context: Context, backend: Memory, manifest: Manifest, **kwargs):
    for model in commands.get_models(context, manifest).values():
        backend.create(get_table_name(model))
        backend.create(get_table_name(model, TableType.CHANGELOG))
    if hasattr(backend, "dsn"):
        with open(backend.dsn, "r") as file:
            data = yaml.safe_load(file)

        if data is not None:
            for record in data:
                backend.insert(record)
