from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.components import Context
from spinta.types.datatype import ArrayBackRef


@commands.prepare.register(Context, PostgreSQL, ArrayBackRef)
def prepare(context: Context, backend: PostgreSQL, dtype: ArrayBackRef, **kwargs):
    return None