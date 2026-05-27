import logging
from typing import Iterator

from spinta import commands
from spinta.backends.helpers import (
    validate_and_return_begin,
    is_custom_id_prop,
)
from spinta.components import Context, Property
from spinta.components import Model
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.sql.components import Sql
from spinta.datasets.backends.sql.helpers import merge_query_with_filters, build_row_result
from spinta.datasets.helpers import decode_id_value
from spinta.datasets.keymaps.components import KeyMap
from spinta.datasets.utils import iterparams
from spinta.typing import ObjectData
from spinta.ufuncs.querybuilder.components import QueryParams
from spinta.ufuncs.resultbuilder.helpers import backend_result_builder_getter

log = logging.getLogger(__name__)


@commands.getall.register(Context, Model, Sql)
def getall(
    context: Context,
    model: Model,
    backend: Sql,
    *,
    query: Expr = None,
    params: QueryParams = None,
    extra_properties: dict[str, Property] = None,
    **kwargs,
) -> Iterator[ObjectData]:
    conn = context.get(f"transaction.{backend.name}")
    builder = backend.query_builder_class(context)
    builder.update(model=model)
    # Merge user passed query with query set in manifest.
    query = merge_query_with_filters(context, model, query)
    query = merge_formulas(model.external.prepare, query)
    query = merge_formulas(query, get_enum_filters(context, model))
    if context.get("config").check_ref_filters:
        query = merge_formulas(query, get_ref_filters(context, model))
    keymap: KeyMap = context.get(f"keymap.{model.keymap.name}")

    result_builder_getter = backend_result_builder_getter(context, backend)

    for model_params in iterparams(context, model, model.manifest):
        table = model.external.name.format(**model_params)
        table = backend.get_table(model, table)
        env = builder.init(backend, table, params)
        env.update(params=model_params)
        expr = env.resolve(query)
        where = env.execute(expr)
        qry = env.build(where)

        is_page_enabled = env.page.page_.enabled

        for row in conn.execute(qry):
            res = build_row_result(
                context, model, backend, row, env, keymap, result_builder_getter, extra_properties, is_page_enabled
            )
            yield res


@commands.getone.register(Context, Model, Sql)
def getone(
    context: Context,
    model: Model,
    backend: Sql,
    *,
    id_: str,
) -> ObjectData:
    _id_prop = model.properties["_id"]
    keymap: KeyMap = context.get(f"keymap.{model.keymap.name}")

    if is_custom_id_prop(_id_prop):
        decoded_id = decode_id_value(_id_prop, id_)
        key = [_id_prop]
        if not _id_prop.external.name:
            key = model.external.pkeys
        pk_filter = {pk.external.name: value for pk, value in zip(key, decoded_id)}
    else:
        _id = keymap.decode(model.name, id_)
        pk_filter = {}
        if isinstance(_id, list):
            pkeys = model.external.pkeys
            for index, pk in enumerate(pkeys):
                pk_filter[pk.name] = _id[index]
        else:
            pk_filter[model.external.pkeys[0].name] = _id

    context.attach(f"transaction.{backend.name}", validate_and_return_begin, context, backend)
    conn = context.get(f"transaction.{backend.name}")

    builder = backend.query_builder_class(context)
    builder.update(model=model)
    result_builder_getter = backend_result_builder_getter(context, backend)

    query = merge_query_with_filters(context, model)

    table = backend.get_table(model, model.external.name)
    env = builder.init(backend, table, None)
    expr = env.resolve(query)
    where = env.execute(expr)
    qry = env.build(where)

    for column_name, column_value in pk_filter.items():
        qry = qry.where(table.c.get(column_name) == column_value)

    result = conn.execute(qry)
    row = result.fetchone()

    return build_row_result(context, model, backend, row, env, keymap, result_builder_getter)
