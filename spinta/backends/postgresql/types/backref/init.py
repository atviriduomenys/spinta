from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.components import Context
from spinta.types.datatype import BackRef


@commands.prepare.register(Context, PostgreSQL, BackRef)
def prepare(context: Context, backend: PostgreSQL, dtype: BackRef, **kwargs):
    return None
