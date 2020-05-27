from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.components import Context, Model
from spinta.types.datatype import Object
from spinta.migrations import SchemaVersion


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, Model, Object)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: Model,
    current: Object,
):
    for _, prop in current.properties.items():
        commands.freeze(context, version, backend, freezed, prop.dtype)


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, type(None), Object)
def freeze(
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


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, Object, Object)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: Object,
    current: Object,
):
    for name in set(freezed.properties) | set(current.properties):
        freezed_dtype = freezed.prop.model
        if name in freezed.properties:
            freezed_dtype = freezed.properties[name].dtype

        current_dtype = current.prop.model
        if name in current.properties:
            current_dtype = current.properties[name].dtype

        commands.freeze(
            context,
            version,
            backend,
            freezed_dtype,
            current_dtype,
        )
