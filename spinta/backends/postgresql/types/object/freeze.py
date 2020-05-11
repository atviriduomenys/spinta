from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.components import Context
from spinta.types.datatype import Object
from spinta.migrations import SchemaVersion


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, type(None), Object)
def freeze(  # noqa
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: type(None),
    current: Object,
):
    pr = []
    for name, prop in current.properties.items():
        pr += commands.freeze(context, version, backend, None, prop.dtype)
    return pr
