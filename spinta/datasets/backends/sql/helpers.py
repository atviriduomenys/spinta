from typing import Any

from spinta import commands
from spinta.backends.helpers import is_custom_id_prop, is_custom_revision_prop
from spinta.components import Context, Model
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.sql.components import Sql
from spinta.datasets.backends.sql.ufuncs.query.components import SqlQueryBuilder
from spinta.datasets.helpers import get_enum_filters, get_ref_filters, encode_composite_string_id
from spinta.types.datatype import Base32
from spinta.typing import ObjectData
from spinta.ufuncs.helpers import merge_formulas
from spinta.ufuncs.querybuilder.helpers import get_page_values
from spinta.ufuncs.resultbuilder.helpers import get_row_value, ResultBuilderGetter
from spinta.utils.nestedstruct import extract_list_property_names, flat_dicts_to_nested


def merge_query_with_filters(context: Context, model: Model, request_query: Expr | None = None) -> Expr:
    query = merge_formulas(model.external.prepare, request_query)
    query = merge_formulas(query, get_enum_filters(context, model))
    query = merge_formulas(query, get_ref_filters(context, model))
    return query


def build_row_result(
    context: Context,
    model: Model,
    backend: Sql,
    row: Any,
    env: SqlQueryBuilder,
    result_builder_getter: ResultBuilderGetter,
    extra_properties: dict | None = None,
    include_page: bool = False,
) -> ObjectData:
    env_selected = env.selected
    list_keys = extract_list_property_names(model, env_selected.keys())

    res = {}
    for key, sel in env_selected.items():
        val = get_row_value(context, result_builder_getter, row, sel)
        if sel.prop:
            if (
                (is_custom_id_prop(sel.prop) or is_custom_revision_prop(sel.prop))
                and isinstance(val, (list, tuple))
                and not isinstance(sel.prop.dtype, Base32)
            ):
                val = encode_composite_string_id(val, model.external.pkeys)
        res[key] = val

    if include_page:
        res["_page"] = get_page_values(env, row)

    res["_type"] = model.model_type()
    res = flat_dicts_to_nested(res, list_keys=list_keys)
    return commands.cast_backend_to_python(context, model, backend, res, extra_properties=extra_properties)
