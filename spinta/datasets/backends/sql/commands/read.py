import logging
from typing import Iterator


from spinta import commands
from spinta.backends.helpers import (
    validate_and_return_begin,
    check_if_model_primary_key_is_composite,
    is_custom_id_prop,
    is_custom_revision_prop,
)
from spinta.components import Context, Property
from spinta.components import Model
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.helpers import generate_pk_for_row
from spinta.datasets.backends.sql.components import Sql
from spinta.datasets.helpers import get_enum_filters, decode_id_value, encode_composite_string_id
from spinta.datasets.helpers import get_ref_filters
from spinta.datasets.keymaps.components import KeyMap
from spinta.datasets.utils import iterparams
from spinta.types.datatype import Base32, PrimaryKey
from spinta.typing import ObjectData
from spinta.ufuncs.querybuilder.components import QueryParams
from spinta.ufuncs.querybuilder.helpers import get_page_values
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
    extra_properties: dict[str, Property] = None,
    **kwargs,
) -> Iterator[ObjectData]:
    conn = context.get(f"transaction.{backend.name}")
    builder = backend.query_builder_class(context)
    builder.update(model=model)
    # Merge user passed query with query set in manifest.
    query = merge_formulas(model.external.prepare, query)
    query = merge_formulas(query, get_enum_filters(context, model))
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

        env_selected = env.selected
        list_keys = extract_list_property_names(model, env_selected.keys())
        is_page_enabled = env.page.page_.enabled

        for row in conn.execute(qry):
            res = {}

            for key, sel in env_selected.items():
                val = get_row_value(context, result_builder_getter, row, sel)
                if sel.prop:
                    if isinstance(sel.prop.dtype, PrimaryKey):
                        val = generate_pk_for_row(context, sel.prop.model, row, keymap, val)
                    elif (
                        (is_custom_id_prop(sel.prop) or is_custom_revision_prop(sel.prop))
                        and isinstance(val, (list, tuple))
                        and not isinstance(sel.prop.dtype, Base32)
                    ):
                        val = encode_composite_string_id(val, model.external.pkeys)
                res[key] = val
            if is_page_enabled:
                res["_page"] = get_page_values(env, row)

            res["_type"] = model.model_type()
            res = flat_dicts_to_nested(res, list_keys=list_keys)
            res = commands.cast_backend_to_python(context, model, backend, res, extra_properties=extra_properties)
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
    raw_id = id_
    if is_custom_id_prop(_id_prop):
        decoded_id = decode_id_value(_id_prop, id_)
        key = [_id_prop]
        if not _id_prop.external.name:
            key = model.external.pkeys

        query = {pk.external.name: value for pk, value in zip(key, decoded_id)}

        if isinstance(_id_prop.dtype, Base32):
            raw_id = decoded_id if check_if_model_primary_key_is_composite(model) else decoded_id[0]

    else:
        keymap: KeyMap = context.get(f"keymap.{model.keymap.name}")
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
    context.attach(f"transaction.{backend.name}", validate_and_return_begin, context, backend)
    conn = context.get(f"transaction.{backend.name}")
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
        if not field.startswith("_"):
            value = row[field]
            data[field] = value

    revision = model.properties.get("_revision")
    if revision and revision.explicitly_given and revision.external:
        if revision.external.name:
            data["_revision"] = row[revision.external.name]
        elif isinstance(revision.external.prepare, Expr) and revision.external.prepare.name == "testlist":
            values = [row[model.properties[arg.args[0]].external.name] for arg in revision.external.prepare.args]
            data["_revision"] = values if isinstance(revision.dtype, Base32) else ",".join(str(v) for v in values)

    additional_data = {"_type": model.model_type(), "_id": raw_id}
    data.update(additional_data)
    data = flat_dicts_to_nested(data)
    return commands.cast_backend_to_python(context, model, backend, data)
