from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.components import Context
from spinta.types.datatype import Object
from spinta.migrations import SchemaVersion


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, Object)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    current: Object,
):
    pr = []
    for name, prop in current.properties.items():
        pr += commands.freeze(context, version, backend, prop.dtype)
    return pr
