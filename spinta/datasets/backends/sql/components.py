from typing import Iterable

from spinta import commands
from spinta.external import External
from spinta.components import Context, Model
from spinta.core.ufuncs import Expr


class SQL(External):
    pass


@commands.getall.register()
async def getall(
    context: Context,
    model: Model,
    backend: SQL,
    *,
    query: Expr,
    paramset: Iterable[dict],
):
    connection = context.get('transaction').connection
    for params in paramset:
        for row in connection.execute(query):
            row = {
                '_type': model.model_type(),
                **row,
            }
            yield commands.cast_backend_to_python(context, model, backend, row)
