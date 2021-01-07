from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Object
from spinta.backends.postgresql.components import PostgreSQL


@commands.wipe.register(Context, Object, PostgreSQL)
def wipe(context: Context, dtype: Object, backend: PostgreSQL):
    for prop in dtype.properties.values():
        wipe(context, prop.dtype, backend)
