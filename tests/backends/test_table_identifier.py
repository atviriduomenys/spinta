import dataclasses

import pytest

import sqlalchemy as sa

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_identifier, split_logical_name
from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest_and_context


def _table_type_to_dtype(table_type: TableType) -> str:
    if table_type == TableType.LIST:
        return "array"

    if table_type == TableType.FILE:
        return "file"

    return "string"


@dataclasses.dataclass(frozen=True)
class IdentifierCase:
    model: str
    logical_qualified_name: str
    pg_schema_name: str | None
    pg_table_name: str
    pg_qualified_name: str
    pg_escaped_qualified_name: str


CASES = (
    IdentifierCase(
        model="datasets/gov/example/City",
        logical_qualified_name="datasets/gov/example/City",
        pg_schema_name="datasets/gov/example",
        pg_table_name="City",
        pg_qualified_name="datasets/gov/example.City",
        pg_escaped_qualified_name='"datasets/gov/example"."City"',
    ),
    IdentifierCase(
        model="datasets/gov/example/City/:changelog",
        logical_qualified_name="datasets/gov/example/City/:changelog",
        pg_schema_name="datasets/gov/example",
        pg_table_name="City/:changelog",
        pg_qualified_name="datasets/gov/example.City/:changelog",
        pg_escaped_qualified_name='"datasets/gov/example"."City/:changelog"',
    ),
    IdentifierCase(
        model="datasets/gov/example/City/:redirect",
        logical_qualified_name="datasets/gov/example/City/:redirect",
        pg_schema_name="datasets/gov/example",
        pg_table_name="City/:redirect",
        pg_qualified_name="datasets/gov/example.City/:redirect",
        pg_escaped_qualified_name='"datasets/gov/example"."City/:redirect"',
    ),
    IdentifierCase(
        model="datasets/gov/example/City/:list/data",
        logical_qualified_name="datasets/gov/example/City/:list/data",
        pg_schema_name="datasets/gov/example",
        pg_table_name="City/:list/data",
        pg_qualified_name="datasets/gov/example.City/:list/data",
        pg_escaped_qualified_name='"datasets/gov/example"."City/:list/data"',
    ),
    IdentifierCase(
        model="datasets/gov/example/City/:file/data",
        logical_qualified_name="datasets/gov/example/City/:file/data",
        pg_schema_name="datasets/gov/example",
        pg_table_name="City/:file/data",
        pg_qualified_name="datasets/gov/example.City/:file/data",
        pg_escaped_qualified_name='"datasets/gov/example"."City/:file/data"',
    ),
    IdentifierCase(
        model="datasets/gov/example/very/long/name/that/exceeds/postgresql/character/limit/City",
        logical_qualified_name="datasets/gov/example/very/long/name/that/exceeds/postgresql/character/limit/City",
        pg_schema_name="datasets/gov/example/very/long/name/t_71d5ff35_/character/limit",
        pg_table_name="City",
        pg_qualified_name="datasets/gov/example/very/long/name/t_71d5ff35_/character/limit.City",
        pg_escaped_qualified_name='"datasets/gov/example/very/long/name/t_71d5ff35_/character/limit"."City"',
    ),
    IdentifierCase(
        model="datasets/gov/example/CityVeryLongNameThatExceedsPostgresqlCharacterLimitAndNeedsToCompressed",
        logical_qualified_name="datasets/gov/example/CityVeryLongNameThatExceedsPostgresqlCharacterLimitAndNeedsToCompressed",
        pg_schema_name="datasets/gov/example",
        pg_table_name="CityVeryLongNameThatExceedsPostgresql_cbfef662_eedsToCompressed",
        pg_qualified_name="datasets/gov/example.CityVeryLongNameThatExceedsPostgresql_cbfef662_eedsToCompressed",
        pg_escaped_qualified_name='"datasets/gov/example"."CityVeryLongNameThatExceedsPostgresql_cbfef662_eedsToCompressed"',
    ),
    IdentifierCase(
        model="City",
        logical_qualified_name="City",
        pg_schema_name=None,
        pg_table_name="City",
        pg_qualified_name="City",
        pg_escaped_qualified_name='"City"',
    ),
)


@dataclasses.dataclass(frozen=True)
class RemovePrefixCase(IdentifierCase):
    model_only: bool


REMOVE_PREFIX_CASES = (
    RemovePrefixCase(
        model="datasets/gov/example/City",
        model_only=True,
        logical_qualified_name="datasets/gov/example/__City",
        pg_schema_name="datasets/gov/example",
        pg_table_name="__City",
        pg_qualified_name="datasets/gov/example.__City",
        pg_escaped_qualified_name='"datasets/gov/example"."__City"',
    ),
    RemovePrefixCase(
        model="datasets/gov/example/City/:changelog",
        model_only=True,
        logical_qualified_name="datasets/gov/example/__City/:changelog",
        pg_schema_name="datasets/gov/example",
        pg_table_name="__City/:changelog",
        pg_qualified_name="datasets/gov/example.__City/:changelog",
        pg_escaped_qualified_name='"datasets/gov/example"."__City/:changelog"',
    ),
    RemovePrefixCase(
        model="datasets/gov/example/City/:redirect",
        model_only=True,
        logical_qualified_name="datasets/gov/example/__City/:redirect",
        pg_schema_name="datasets/gov/example",
        pg_table_name="__City/:redirect",
        pg_qualified_name="datasets/gov/example.__City/:redirect",
        pg_escaped_qualified_name='"datasets/gov/example"."__City/:redirect"',
    ),
    RemovePrefixCase(
        model="datasets/gov/example/City/:list/data",
        model_only=True,
        logical_qualified_name="datasets/gov/example/__City/:list/data",
        pg_schema_name="datasets/gov/example",
        pg_table_name="__City/:list/data",
        pg_qualified_name="datasets/gov/example.__City/:list/data",
        pg_escaped_qualified_name='"datasets/gov/example"."__City/:list/data"',
    ),
    RemovePrefixCase(
        model="datasets/gov/example/City/:list/data",
        model_only=False,
        logical_qualified_name="datasets/gov/example/City/:list/__data",
        pg_schema_name="datasets/gov/example",
        pg_table_name="City/:list/__data",
        pg_qualified_name="datasets/gov/example.City/:list/__data",
        pg_escaped_qualified_name='"datasets/gov/example"."City/:list/__data"',
    ),
    RemovePrefixCase(
        model="datasets/gov/example/City/:file/data",
        model_only=True,
        logical_qualified_name="datasets/gov/example/__City/:file/data",
        pg_schema_name="datasets/gov/example",
        pg_table_name="__City/:file/data",
        pg_qualified_name="datasets/gov/example.__City/:file/data",
        pg_escaped_qualified_name='"datasets/gov/example"."__City/:file/data"',
    ),
    RemovePrefixCase(
        model="datasets/gov/example/City/:file/data",
        model_only=False,
        logical_qualified_name="datasets/gov/example/City/:file/__data",
        pg_schema_name="datasets/gov/example",
        pg_table_name="City/:file/__data",
        pg_qualified_name="datasets/gov/example.City/:file/__data",
        pg_escaped_qualified_name='"datasets/gov/example"."City/:file/__data"',
    ),
    RemovePrefixCase(
        model="datasets/gov/example/very/long/name/that/exceeds/postgresql/character/limit/City",
        model_only=True,
        logical_qualified_name="datasets/gov/example/very/long/name/that/exceeds/postgresql/character/limit/__City",
        pg_schema_name="datasets/gov/example/very/long/name/t_71d5ff35_/character/limit",
        pg_table_name="__City",
        pg_qualified_name="datasets/gov/example/very/long/name/t_71d5ff35_/character/limit.__City",
        pg_escaped_qualified_name='"datasets/gov/example/very/long/name/t_71d5ff35_/character/limit"."__City"',
    ),
    RemovePrefixCase(
        model="datasets/gov/example/CityVeryLongNameThatExceedsPostgresqlCharacterLimitAndNeedsToCompressed",
        model_only=True,
        logical_qualified_name="datasets/gov/example/__CityVeryLongNameThatExceedsPostgresqlCharacterLimitAndNeedsToCompressed",
        pg_schema_name="datasets/gov/example",
        pg_table_name="__CityVeryLongNameThatExceedsPostgres_bec259d6_eedsToCompressed",
        pg_qualified_name="datasets/gov/example.__CityVeryLongNameThatExceedsPostgres_bec259d6_eedsToCompressed",
        pg_escaped_qualified_name='"datasets/gov/example"."__CityVeryLongNameThatExceedsPostgres_bec259d6_eedsToCompressed"',
    ),
    RemovePrefixCase(
        model="City",
        model_only=True,
        logical_qualified_name="__City",
        pg_schema_name=None,
        pg_table_name="__City",
        pg_qualified_name="__City",
        pg_escaped_qualified_name='"__City"',
    ),
)


@dataclasses.dataclass(frozen=True)
class ChangeTableTypeCase(IdentifierCase):
    table_type: TableType
    table_arg: str | None


CHANGE_TYPE_CASES = (
    ChangeTableTypeCase(
        model="datasets/gov/example/City",
        table_type=TableType.CHANGELOG,
        table_arg=None,
        logical_qualified_name="datasets/gov/example/City/:changelog",
        pg_schema_name="datasets/gov/example",
        pg_table_name="City/:changelog",
        pg_qualified_name="datasets/gov/example.City/:changelog",
        pg_escaped_qualified_name='"datasets/gov/example"."City/:changelog"',
    ),
    ChangeTableTypeCase(
        model="datasets/gov/example/City/:changelog",
        table_type=TableType.REDIRECT,
        table_arg=None,
        logical_qualified_name="datasets/gov/example/City/:redirect",
        pg_schema_name="datasets/gov/example",
        pg_table_name="City/:redirect",
        pg_qualified_name="datasets/gov/example.City/:redirect",
        pg_escaped_qualified_name='"datasets/gov/example"."City/:redirect"',
    ),
    ChangeTableTypeCase(
        model="datasets/gov/example/City/:redirect",
        table_type=TableType.LIST,
        table_arg="data",
        logical_qualified_name="datasets/gov/example/City/:list/data",
        pg_schema_name="datasets/gov/example",
        pg_table_name="City/:list/data",
        pg_qualified_name="datasets/gov/example.City/:list/data",
        pg_escaped_qualified_name='"datasets/gov/example"."City/:list/data"',
    ),
    ChangeTableTypeCase(
        model="datasets/gov/example/City/:list/data",
        table_type=TableType.FILE,
        table_arg="data",
        logical_qualified_name="datasets/gov/example/City/:file/data",
        pg_schema_name="datasets/gov/example",
        pg_table_name="City/:file/data",
        pg_qualified_name="datasets/gov/example.City/:file/data",
        pg_escaped_qualified_name='"datasets/gov/example"."City/:file/data"',
    ),
    ChangeTableTypeCase(
        model="datasets/gov/example/City/:file/data",
        table_type=TableType.MAIN,
        table_arg=None,
        logical_qualified_name="datasets/gov/example/City",
        pg_schema_name="datasets/gov/example",
        pg_table_name="City",
        pg_qualified_name="datasets/gov/example.City",
        pg_escaped_qualified_name='"datasets/gov/example"."City"',
    ),
    ChangeTableTypeCase(
        model="datasets/gov/example/very/long/name/that/exceeds/postgresql/character/limit/City",
        table_type=TableType.CHANGELOG,
        table_arg=None,
        logical_qualified_name="datasets/gov/example/very/long/name/that/exceeds/postgresql/character/limit/City/:changelog",
        pg_schema_name="datasets/gov/example/very/long/name/t_71d5ff35_/character/limit",
        pg_table_name="City/:changelog",
        pg_qualified_name="datasets/gov/example/very/long/name/t_71d5ff35_/character/limit.City/:changelog",
        pg_escaped_qualified_name='"datasets/gov/example/very/long/name/t_71d5ff35_/character/limit"."City/:changelog"',
    ),
    ChangeTableTypeCase(
        model="datasets/gov/example/CityVeryLongNameThatExceedsPostgresqlCharacterLimitAndNeedsToCompressed",
        table_type=TableType.CHANGELOG,
        table_arg=None,
        logical_qualified_name="datasets/gov/example/CityVeryLongNameThatExceedsPostgresqlCharacterLimitAndNeedsToCompressed/:changelog",
        pg_schema_name="datasets/gov/example",
        pg_table_name="CityVeryLongNameThatExceedsPostgresql_0ee8cc41_essed/:changelog",
        pg_qualified_name="datasets/gov/example.CityVeryLongNameThatExceedsPostgresql_0ee8cc41_essed/:changelog",
        pg_escaped_qualified_name='"datasets/gov/example"."CityVeryLongNameThatExceedsPostgresql_0ee8cc41_essed/:changelog"',
    ),
    ChangeTableTypeCase(
        model="City",
        table_type=TableType.CHANGELOG,
        table_arg=None,
        logical_qualified_name="City/:changelog",
        pg_schema_name=None,
        pg_table_name="City/:changelog",
        pg_qualified_name="City/:changelog",
        pg_escaped_qualified_name='"City/:changelog"',
    ),
)


@pytest.fixture(scope="module", params=CASES, ids=lambda case: case.model)
def identifier_case(request) -> IdentifierCase:
    return request.param


def test_get_table_identifier_from_string(
    identifier_case: IdentifierCase,
):
    table_identifier = get_table_identifier(identifier_case.model)
    assert table_identifier.logical_qualified_name == identifier_case.logical_qualified_name
    assert table_identifier.pg_schema_name == identifier_case.pg_schema_name
    assert table_identifier.pg_table_name == identifier_case.pg_table_name
    assert table_identifier.pg_qualified_name == identifier_case.pg_qualified_name
    assert table_identifier.pg_escaped_qualified_name == identifier_case.pg_escaped_qualified_name


def test_get_table_identifier_from_table_comment(
    identifier_case: IdentifierCase,
):
    table = sa.Table("test", sa.MetaData(), comment=identifier_case.model, schema=identifier_case.pg_schema_name)
    table_identifier = get_table_identifier(table)
    assert table_identifier.logical_qualified_name == identifier_case.logical_qualified_name
    assert table_identifier.pg_schema_name == identifier_case.pg_schema_name
    assert table_identifier.pg_table_name == identifier_case.pg_table_name
    assert table_identifier.pg_qualified_name == identifier_case.pg_qualified_name
    assert table_identifier.pg_escaped_qualified_name == identifier_case.pg_escaped_qualified_name


def test_get_table_identifier_from_table_metadata(
    identifier_case: IdentifierCase,
):
    table = sa.Table(identifier_case.pg_table_name, sa.MetaData(), schema=identifier_case.pg_schema_name)
    table_identifier = get_table_identifier(table)
    # Cannot assert full logical_qualified_name, because of the compression possibility.
    assert table_identifier.pg_schema_name == identifier_case.pg_schema_name
    assert table_identifier.pg_table_name == identifier_case.pg_table_name
    assert table_identifier.pg_qualified_name == identifier_case.pg_qualified_name
    assert table_identifier.pg_escaped_qualified_name == identifier_case.pg_escaped_qualified_name


def test_get_table_identifier_from_model_or_prop(
    rc: RawConfig,
    identifier_case: IdentifierCase,
):
    namespace, model, table_type, prop = split_logical_name(identifier_case.model)
    prop_type = _table_type_to_dtype(table_type)
    context, manifest = load_manifest_and_context(
        rc,
        f"""
    d | r | b | m | property     | type    | ref                    | source  | prepare | access
    {f"{namespace}                      |         |                        |         |         |" if namespace else ""}
      |   |   | {model}          |         |  |  |         |
      |   |   |   | data | {prop_type} |                        |  |         | open
    """,
    )
    full_model_path = namespace + "/" + model if namespace else model
    node = commands.get_model(context, manifest, full_model_path)
    if prop:
        node = node.properties[prop]

    table_identifier = get_table_identifier(node, table_type)
    assert table_identifier.logical_qualified_name == identifier_case.logical_qualified_name
    assert table_identifier.pg_schema_name == identifier_case.pg_schema_name
    assert table_identifier.pg_table_name == identifier_case.pg_table_name
    assert table_identifier.pg_qualified_name == identifier_case.pg_qualified_name
    assert table_identifier.pg_escaped_qualified_name == identifier_case.pg_escaped_qualified_name


@pytest.mark.parametrize("case", REMOVE_PREFIX_CASES, ids=lambda case: case.logical_qualified_name)
def test_table_identifier_remove_prefix(case: RemovePrefixCase):
    table_identifier = get_table_identifier(case.model)
    table_identifier = table_identifier.apply_removed_prefix(remove_model_only=case.model_only)
    assert table_identifier.logical_qualified_name == case.logical_qualified_name
    assert table_identifier.pg_schema_name == case.pg_schema_name
    assert table_identifier.pg_table_name == case.pg_table_name
    assert table_identifier.pg_qualified_name == case.pg_qualified_name
    assert table_identifier.pg_escaped_qualified_name == case.pg_escaped_qualified_name


@pytest.mark.parametrize("case", CHANGE_TYPE_CASES, ids=lambda case: f"{case.model} -> {case.table_type.value}")
def test_table_identifier_change_table_type(case: ChangeTableTypeCase):
    table_identifier = get_table_identifier(case.model)
    table_identifier = table_identifier.change_table_type(new_type=case.table_type, table_arg=case.table_arg)
    assert table_identifier.logical_qualified_name == case.logical_qualified_name
    assert table_identifier.pg_schema_name == case.pg_schema_name
    assert table_identifier.pg_table_name == case.pg_table_name
    assert table_identifier.pg_qualified_name == case.pg_qualified_name
    assert table_identifier.pg_escaped_qualified_name == case.pg_escaped_qualified_name
