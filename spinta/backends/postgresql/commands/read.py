from typing import Iterator
from typing import overload

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.ufuncs.query.components import PgQueryBuilder
from spinta.components import Context
from spinta.components import Model
from spinta.core.ufuncs import Expr
from spinta.exceptions import ItemDoesNotExist
from spinta.exceptions import NotFoundError
from spinta.typing import ObjectData
from spinta.ufuncs.basequerybuilder.components import QueryParams
from spinta.ufuncs.basequerybuilder.helpers import get_page_values
from spinta.ufuncs.resultbuilder.helpers import get_row_value, backend_result_builder_getter
from spinta.utils.nestedstruct import flat_dicts_to_nested, extract_list_property_names


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
    default_expand: bool = True,
    params: QueryParams = None,
    **kwargs
) -> Iterator[ObjectData]:
    assert isinstance(query, (Expr, type(None))), query
    connection = context.get('transaction').connection

    if default_expand:
        if params is None:
            params = QueryParams()
        params.default_expand = default_expand

    builder = PgQueryBuilder(context)
    builder.update(model=model)
    table = backend.get_table(model)
    env = builder.init(backend, table, params)
    expr = env.resolve(query)
    where = env.execute(expr)
    qry = env.build(where)

    conn = connection.execution_options(stream_results=True)
    result = conn.execute(qry)

    env_selected = env.selected
    is_page_enabled = env.page.page_.enabled
    result_builder_getter = backend_result_builder_getter(context, backend)
    list_keys = extract_list_property_names(model, env_selected.keys())

    for row in result:
        res = {}
        for key, sel in env_selected.items():
            res[key] = get_row_value(context, result_builder_getter, row, sel, False)

        if is_page_enabled:
            res['_page'] = get_page_values(env, row)

        res['_type'] = model.model_type()

        res = flat_dicts_to_nested(res, list_keys=list_keys)
        res = commands.cast_backend_to_python(context, model, backend, res)
        yield res
