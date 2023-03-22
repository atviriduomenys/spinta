from typing import overload, Any
from typing import Iterator

from spinta.datasets.helpers import isexpr, paginate
from spinta.typing import ObjectData
from spinta import commands
from spinta.core.ufuncs import Expr
from spinta.components import Context
from spinta.components import Model
from spinta.exceptions import NotFoundError
from spinta.exceptions import ItemDoesNotExist
from spinta.backends.postgresql.components import PostgreSQL
from spinta.utils.nestedstruct import flat_dicts_to_nested
from spinta.backends.postgresql.commands.query import PgQueryBuilder


@overload
@commands.getone.register(Context, Model, PostgreSQL)
def getone(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    id_: str,
) -> ObjectData:
    connection = context.get('transaction').connection
    table = backend.get_table(model)
    try:
        result = backend.get(connection, table, table.c._id == id_)
    except NotFoundError:
        raise ItemDoesNotExist(model, id=id_)
    data = flat_dicts_to_nested(dict(result))
    data['_type'] = model.model_type()
    return commands.cast_backend_to_python(context, model, backend, data)


@commands.getall.register(Context, Model, PostgreSQL)
def getall(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    query: Expr = None,
    is_paginated: bool = False,  # If True, method returns one page of objects
    page: Any = None,            # If set, method returns one page of objects, that are greater than given page value.
                                 # If page is None and is_paginated is True, method returns first page of objects.
) -> Iterator[ObjectData]:
    assert isinstance(query, (Expr, type(None))), query
    connection = context.get('transaction').connection

    builder = PgQueryBuilder(context)
    builder.update(model=model)
    table = backend.get_table(model)
    env = builder.init(backend, table)

    if is_paginated and model.external and \
            model.external.prepare and \
            isexpr(model.external.prepare, {'page'}):
        if isexpr(query, {'limit'}):
            env.resolve(query)
        qry = paginate(env, model.external.prepare, page)
    else:
        expr = env.resolve(query)
        where = env.execute(expr)
        qry = env.build(where)

    conn = connection.execution_options(stream_results=True)
    result = conn.execute(qry)

    for row in result:
        row = flat_dicts_to_nested(dict(row))
        row = {
            '_type': model.model_type(),
            **row,
        }
        row = commands.cast_backend_to_python(context, model, backend, row)
        yield row
