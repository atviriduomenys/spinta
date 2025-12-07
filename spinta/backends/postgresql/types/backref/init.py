from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.type import validate_type_assignment
from spinta.components import Context
from spinta.types.datatype import BackRef


@commands.prepare.register(Context, PostgreSQL, BackRef)
def prepare(context: Context, backend: PostgreSQL, dtype: BackRef, **kwargs):
    validate_type_assignment(context, backend, dtype)
    return None
