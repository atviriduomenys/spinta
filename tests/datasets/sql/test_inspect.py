from unittest.mock import MagicMock

import sqlalchemy as sa
from sqlalchemy.dialects import mysql

from spinta.manifests.sql.helpers import _create_mapping, _get_column_type, _read_props, _SchemaMapping, _TableMapping


def test_char_type():
    column = {"name": "test", "type": mysql.CHAR()}
    table = "test"

    assert _get_column_type(column, table) == "string"


def test_tinyblob_type():
    column = {"name": "test", "type": mysql.TINYBLOB()}
    table = "test"

    assert _get_column_type(column, table) == "binary"


def test_blob_type():
    column = {"name": "test", "type": mysql.BLOB()}
    table = "test"

    assert _get_column_type(column, table) == "binary"


def test_mediumblob_type():
    column = {"name": "test", "type": mysql.MEDIUMBLOB()}
    table = "test"

    assert _get_column_type(column, table) == "binary"


def test_longblob_type():
    column = {"name": "test", "type": mysql.LONGBLOB()}
    table = "test"

    assert _get_column_type(column, table) == "binary"


def _make_insp(columns_by_table: dict, foreign_keys: dict = None) -> MagicMock:
    """Build a minimal Inspector mock.

    columns_by_table: {table_name: list-of-col-dicts | Exception subclass}
    foreign_keys:     {table_name: list-of-fk-dicts} (default: empty for all)
    """
    foreign_keys = foreign_keys or {}
    insp = MagicMock()

    def get_columns(table, schema=None):
        result = columns_by_table[table]
        if isinstance(result, type) and issubclass(result, Exception):
            raise result("argument of type 'NoneType' is not iterable")
        if isinstance(result, Exception):
            raise result
        return result

    insp.get_columns.side_effect = get_columns
    insp.get_foreign_keys.side_effect = lambda table, schema=None: foreign_keys.get(table, [])
    return insp


# ---------------------------------------------------------------------------
# _create_mapping
# ---------------------------------------------------------------------------


def test_create_mapping_skips_table_when_get_columns_raises():
    """A table whose get_columns raises any exception must be omitted from the
    mapping; other tables must still be present."""
    insp = _make_insp(
        {
            "good_table": [{"name": "id", "type": sa.Integer()}],
            "bad_table": TypeError,
        }
    )

    mapping = _create_mapping(insp, ["good_table", "bad_table"], schema="dbo", dataset="ds")

    assert "good_table" in mapping
    assert "bad_table" not in mapping


def test_create_mapping_all_tables_ok():
    """Sanity check: when no table raises, all tables appear in the mapping."""
    insp = _make_insp(
        {
            "table_a": [{"name": "col1", "type": sa.Integer()}],
            "table_b": [{"name": "col1", "type": sa.Text()}],
        }
    )

    mapping = _create_mapping(insp, ["table_a", "table_b"], schema="dbo", dataset="ds")

    assert set(mapping.keys()) == {"table_a", "table_b"}


# ---------------------------------------------------------------------------
# _read_props
# ---------------------------------------------------------------------------


def _make_schema_mapping(schema: str, table: str, props: dict) -> dict:
    """Build the mapping dict that _read_props expects."""
    table_mapping = _TableMapping(model=f"ds/{table}", schema=schema, props=props)
    return {schema: _SchemaMapping(schema=schema, dataset="ds", resource="res", tables={table: table_mapping})}


def test_read_props_yields_nothing_when_get_columns_raises():
    """_read_props must return an empty iterator (not crash) when get_columns
    raises any exception (e.g. unresolvable column type)."""
    schema = "dbo"
    table = "bad_table"
    insp = _make_insp({table: TypeError})
    mapping = _make_schema_mapping(schema, table, {})

    result = list(_read_props(insp, table, schema, mapping))

    assert result == []


def test_read_props_yields_props_for_normal_table():
    """Sanity check: _read_props yields properties for a well-formed table."""
    schema = "dbo"
    table = "good_table"
    insp = _make_insp(
        {table: [{"name": "id", "type": sa.Integer(), "comment": ""}]},
        foreign_keys={table: []},
    )
    mapping = _make_schema_mapping(schema, table, {"id": "id"})

    result = list(_read_props(insp, table, schema, mapping))

    assert len(result) == 1
    prop_name, prop_schema = result[0]
    assert prop_name == "id"
    assert prop_schema["type"] == "integer"
