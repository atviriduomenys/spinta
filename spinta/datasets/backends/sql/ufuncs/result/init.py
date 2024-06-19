from spinta import commands
from spinta.components import Context
from spinta.datasets.backends.sql.components import Sql
from spinta.datasets.backends.sql.ufuncs.result.components import SqlResultBuilder


@commands.get_result_builder.register(Context, Sql)
def get_result_builder(
    context: Context,
    backend: Sql
):
    return SqlResultBuilder(context)
