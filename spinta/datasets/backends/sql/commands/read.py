import logging
from typing import Iterator

from spinta import commands
from spinta.backends.helpers import validate_and_return_begin
from spinta.components import Context
from spinta.components import Model
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.helpers import handle_ref_key_assignment, generate_pk_for_row
from spinta.datasets.backends.sql.components import Sql
from spinta.datasets.backends.sql.ufuncs.query.components import SqlQueryBuilder
from spinta.datasets.helpers import get_enum_filters
from spinta.datasets.helpers import get_ref_filters
from spinta.datasets.keymaps.components import KeyMap
from spinta.datasets.utils import iterparams
from spinta.types.datatype import PrimaryKey
from spinta.types.datatype import Ref
from spinta.typing import ObjectData
from spinta.ufuncs.basequerybuilder.components import QueryParams
from spinta.ufuncs.basequerybuilder.helpers import get_page_values
from spinta.ufuncs.helpers import merge_formulas
from spinta.ufuncs.resultbuilder.helpers import get_row_value, backend_result_builder_getter
from spinta.utils.nestedstruct import flat_dicts_to_nested, extract_list_property_names

log = logging.getLogger(__name__)


@commands.getall.register(Context, Model, Sql)
def getall(
    context: Context,
    model: Model,
    backend: Sql,
    *,
    query: Expr = None,
    params: QueryParams = None,
    **kwargs
) -> Iterator[ObjectData]:
    conn = context.get(f'transaction.{backend.name}')
    builder = SqlQueryBuilder(context)
    builder.update(model=model)
    # Merge user passed query with query set in manifest.
    query = merge_formulas(model.external.prepare, query)
    query = merge_formulas(query, get_enum_filters(context, model))
    query = merge_formulas(query, get_ref_filters(context, model))
    keymap: KeyMap = context.get(f'keymap.{model.keymap.name}')

    result_builder_getter = backend_result_builder_getter(context, backend)

    for model_params in iterparams(context, model, model.manifest):
        table = model.external.name.format(**model_params)
        table = backend.get_table(model, table)
        env = builder.init(backend, table, params)
        env.update(params=model_params)
        expr = env.resolve(query)
        where = env.execute(expr)
        qry = env.build(where)

        env_selected = env.selected
        list_keys = extract_list_property_names(model, env_selected.keys())
        is_page_enabled = env.page.page_.enabled

        for row in conn.execute(qry):
            res = {}

            for key, sel in env_selected.items():
                val = get_row_value(context, result_builder_getter, row, sel)
                if sel.prop:
                    if isinstance(sel.prop.dtype, PrimaryKey):
                        val = generate_pk_for_row(sel.prop.model, row, keymap, val)
                    elif isinstance(sel.prop.dtype, Ref):
                        val = handle_ref_key_assignment(context, keymap, env, val, sel.prop.dtype)
                res[key] = val
            if is_page_enabled:
                res['_page'] = get_page_values(env, row)

            res['_type'] = model.model_type()
            res = flat_dicts_to_nested(res, list_keys=list_keys)
            res = commands.cast_backend_to_python(context, model, backend, res)
            yield res


@commands.getone.register(Context, Model, Sql)
def getone(
    context: Context,
    model: Model,
    backend: Sql,
    *,
    id_: str,
) -> ObjectData:
    keymap: KeyMap = context.get(f'keymap.{model.keymap.name}')
    _id = keymap.decode(model.name, id_)

    # preparing query for retrieving item by pk (single column or multi column)
    query = {}
    if isinstance(_id, list):
        pkeys = model.external.pkeys
        for index, pk in enumerate(pkeys):
            query[pk.name] = _id[index]
    else:
        pk = model.external.pkeys[0].name
        query[pk] = _id

    # building sqlalchemy query
    context.attach(f'transaction.{backend.name}', validate_and_return_begin, context, backend)
    conn = context.get(f'transaction.{backend.name}')
    table = model.external.name
    table = backend.get_table(model, table)

    qry = table.select()
    for column_name, column_value in query.items():
        id_column = table.c.get(column_name)
        qry = qry.where(id_column == column_value)

    # #executing query
    result = conn.execute(qry)
    row = result.fetchone()

    # preparing results
    data = {}

    for field in model.properties:
        if not field.startswith('_'):
            value = row[field]
            data[field] = value

    additional_data = {
        '_type': model.model_type(),
        '_id': id_
    }
    data.update(additional_data)
    data = flat_dicts_to_nested(data)
    return commands.cast_backend_to_python(context, model, backend, data)
