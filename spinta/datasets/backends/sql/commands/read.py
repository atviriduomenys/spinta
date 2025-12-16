import logging
from typing import Iterator

import sqlalchemy as sa

from spinta import commands
from spinta.backends.helpers import validate_and_return_begin
from spinta.components import Context, Property
from spinta.components import Model
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.helpers import generate_pk_for_row
from spinta.datasets.backends.sql.components import Sql
from spinta.datasets.helpers import get_enum_filters
from spinta.datasets.helpers import get_ref_filters
from spinta.datasets.keymaps.components import KeyMap
from spinta.datasets.utils import iterparams
from spinta.types.datatype import PrimaryKey
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
    keymap: KeyMap = context.get(f"keymap.{model.keymap.name}")
    _id = keymap.decode(model.name, id_)
    log.debug(f"Decoded _id for model {model.name}: {_id}, type: {type(_id)}")

    # Validate keymap decoded successfully
    if _id is None:
        raise ValueError(
            f"Cannot find keymap entry for id '{id_}' in model '{model.name}'. "
            f"The row may have been deleted or the keymap is out of sync."
        )

    # preparing query for retrieving item by pk (single column or multi column)
    query = {}
    if isinstance(_id, list):
        pkeys = model.external.pkeys
        for index, pk in enumerate(pkeys):
            query[pk.name] = _id[index]
    elif isinstance(_id, dict):
        # Handle case where _id is a dict (for models without primary keys)
        query = _id
    else:
        pk = model.external.pkeys[0].name
        query[pk] = _id
    log.debug(f"Query dict for model {model.name}: {query}")

    # building sqlalchemy query
    context.attach(f"transaction.{backend.name}", validate_and_return_begin, context, backend)
    conn = context.get(f"transaction.{backend.name}")
    table = model.external.name
    table = backend.get_table(model, table)

    # Build query with proper column validation
    qry = table.select()
    missing_columns = []
    valid_conditions = []
    # Create mapping from property names to actual database column names
    column_mapping = {}

    for column_name, column_value in query.items():
        id_column = table.c.get(column_name)
        if id_column is None:
            # Try case-insensitive lookup for SAS databases
            if backend.engine.dialect.name == "sas":
                for actual_col in table.c:
                    if actual_col.name.upper() == column_name.upper():
                        id_column = actual_col
                        column_mapping[column_name] = actual_col.name
                        break

            if id_column is None:
                missing_columns.append(column_name)
                continue
        else:
            # Direct match found
            column_mapping[column_name] = id_column.name

        # Build WHERE condition (dialect-specific)
        if backend.engine.dialect.name == "sas":
            # Check the actual value type, not just the column type
            if isinstance(column_value, str):
                # String value - quote it
                formatted_value = f"'{column_value}'"
            elif isinstance(column_value, (int, float)) and not isinstance(column_value, bool):
                # Numeric value - convert to string directly
                formatted_value = str(column_value)
            elif column_value is None:
                # NULL value
                formatted_value = "NULL"
            else:
                # Fallback: treat as string
                formatted_value = f"'{str(column_value)}'"
            condition = sa.text(f"{str(id_column)} = {formatted_value}")
        else:
            condition = id_column == column_value

        valid_conditions.append(condition)

    # Handle missing columns
    if missing_columns:
        raise ValueError(
            f"Columns {missing_columns} from keymap not found in table '{table}' "
            f"for model '{model.name}'. This may indicate:\n"
            f"1. Table schema has changed since keymap was created\n"
            f"2. Manifest column mappings are incorrect\n"
            f"3. Keymap data is corrupted or out of sync"
        )

    # Validate we have at least one condition
    if not valid_conditions:
        raise ValueError(f"No valid columns found to query row for model '{model.name}' with id '{id_}'")

    # Apply all conditions to query
    for condition in valid_conditions:
        qry = qry.where(condition)
    log.debug(f"Generated SQL query for model {model.name}: {qry}")

    # #executing query
    result = conn.execute(qry)
    row = result.fetchone()

    # Validate query results
    if row is None:
        raise ValueError(
            f"No row found for id '{id_}' in model '{model.name}'. "
            f"The row may have been deleted or the query conditions don't match any data."
        )

    # Check for multiple matches (data integrity issue)
    additional_row = result.fetchone()
    if additional_row is not None:
        log.warning(
            f"Multiple rows match the keymap values for id '{id_}' in model '{model.name}'. "
            f"This indicates data integrity issues. Returning first match."
        )

    # preparing results
    data = {}

    for field in model.properties:
        if not field.startswith("_"):
            # Use column mapping to get the actual database column name
            db_column = column_mapping.get(field, field)
            value = row[db_column]
            data[field] = value

    additional_data = {"_type": model.model_type(), "_id": id_}
    data.update(additional_data)
    data = flat_dicts_to_nested(data)
    return commands.cast_backend_to_python(context, model, backend, data)
