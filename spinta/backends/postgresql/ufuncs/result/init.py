from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.ufuncs.result.components import PgResultBuilder
from spinta.components import Context


@commands.get_result_builder.register(Context, PostgreSQL)
def get_result_builder(
    context: Context,
    backend: PostgreSQL
):
    return PgResultBuilder(context)
