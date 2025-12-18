"""
SAS Backend-Specific Read Commands

This module provides SAS-specific implementations of read commands,
handling SAS's unique requirements for case-insensitive column lookups
and value formatting in WHERE clauses.
"""

import logging


from spinta import commands
from spinta.backends.helpers import validate_and_return_begin
from spinta.components import Context
from spinta.components import Model
from spinta.datasets.backends.sql.backends.sas.components import SAS
from spinta.datasets.keymaps.components import KeyMap
from spinta.typing import ObjectData
from spinta.utils.nestedstruct import flat_dicts_to_nested

log = logging.getLogger(__name__)


@commands.getone.register(Context, Model, SAS)
def getone(
    context: Context,
    model: Model,
    backend: SAS,
    *,
    id_: str,
) -> ObjectData:
    """
    Retrieve a single record from a SAS dataset by ID.

    This SAS-specific implementation handles:
    1. Case-insensitive column name lookups
    2. Proper value formatting for SAS SQL syntax (strings, numbers, NULL)
    3. Comprehensive validation and error handling
    4. Column name mapping for result extraction

    Args:
        context: Request context
        model: Model being queried
        backend: SAS backend instance
        id_: Encoded keymap ID

    Returns:
        Dictionary containing the row data with _type and _id metadata

    Raises:
        ValueError: If keymap decoding fails, columns are missing,
                   no row is found, or data integrity issues are detected
    """
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

    # Build query with proper column validation and SAS-specific handling
    qry = table.select()
    missing_columns = []
    valid_conditions = []
    # Create mapping from property names to actual database column names
    column_mapping = {}

    for column_name, column_value in query.items():
        id_column = table.c.get(column_name)
        if id_column is None:
            # Try case-insensitive lookup for SAS databases
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

        # Build WHERE condition using SQLAlchemy expressions
        # DEBUG: Log the condition being built
        log.debug(
            f"Building WHERE condition for column '{column_name}' with value: {column_value} (type: {type(column_value)})"
        )

        # Use proper SQLAlchemy expressions instead of raw SQL text
        if column_value is None:
            # NULL check - generates "column IS NULL"
            condition = id_column.is_(None)
            log.debug(f"Created NULL comparison: {column_name} IS NULL")
        else:
            # Regular comparison - generates "column = value"
            condition = id_column == column_value
            log.debug(f"Created equality comparison: {column_name} = {column_value}")

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
