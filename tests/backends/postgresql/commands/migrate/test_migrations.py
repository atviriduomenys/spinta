import itertools
import json

import pytest
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.engine.url import make_url
from sqlalchemy.engine import Engine

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.helpers import TableIdentifier, get_table_identifier
from spinta.backends.postgresql.helpers import get_pg_name
from spinta.cli.helpers.store import load_store
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.exceptions import UnableToCastColumnTypes
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.context import create_test_context
from spinta.testing.migration import (
    add_index,
    add_column_comment,
    add_table_comment,
    add_changelog_table,
    add_redirect_table,
    add_column,
    drop_column,
    get_table_unique_constraint_columns,
    get_table_foreign_key_constraint_columns,
    rename_table,
    rename_changelog,
    rename_column,
    drop_table,
    rename_index,
    rename_redirect,
    drop_index,
    add_schema,
)
from spinta.testing.pytest import MIGRATION_DATABASE
from spinta.testing.tabular import create_tabular_manifest

from sqlalchemy.dialects import postgresql

pg_identifier_preparer = postgresql.dialect().identifier_preparer


def configure_migrate(rc, path, manifest):
    url = make_url(rc.get("backends", "default", "dsn", required=True))
    url = url.set(database=MIGRATION_DATABASE)
    rc = rc.fork(
        {
            "manifests": {
                "default": {
                    "type": "tabular",
                    "path": str(path / "manifest.csv"),
                    "backend": "default",
                    "keymap": "default",
                    "mode": "external",
                },
            },
            "backends": {
                "default": {"type": "postgresql", "dsn": url},
            },
        }
    )
    context = create_test_context(rc, name="pytest/cli")
    override_manifest(context, path, manifest)
    return context, rc


def override_manifest(context: Context, path, manifest):
    path = f"{path}/manifest.csv"
    create_tabular_manifest(context, path, striptable(manifest))


def cleanup_table_list(meta: sa.MetaData, tables: list):
    table_list = []
    for table in tables:
        table_identifier = get_table_identifier(table)
        table_list.append(meta.tables[table_identifier.pg_qualified_name])
    meta.drop_all(tables=table_list)


def float_equals(a: float, b: float, epsilon=1e-9):
    return abs(a - b) < epsilon


def test_migrate_create_simple_datatype_model(
    migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    initial_manifest = """
     d | r | b | m | property   | type    | ref     | source     | prepare
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)
    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])
    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
                     |   |   |      | someInteger  | integer
                     |   |   |      | someNumber   | number
                     |   |   |      | someDate     | date
                     |   |   |      | someDateTime | datetime
                     |   |   |      | someTime     | time
                     |   |   |      | someBoolean  | boolean
                     |   |   |      | someUrl      | url
                     |   |   |      | someUri      | uri
                     |   |   |      | someBinary   | binary
                     |   |   |      | someJson     | json
    """,
    )
    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])

    test_table_identifier = TableIdentifier(schema="migrate/example", base_name="Test")
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_schema(schema='migrate/example')}"
        'CREATE TABLE "migrate/example"."Test" (\n'
        "    _txn UUID, \n"
        "    _created TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _updated TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _id UUID NOT NULL, \n"
        "    _revision TEXT, \n"
        '    "someText" TEXT, \n'
        '    "someInteger" INTEGER, \n'
        '    "someNumber" FLOAT, \n'
        '    "someDate" DATE, \n'
        '    "someDateTime" TIMESTAMP WITHOUT TIME ZONE, \n'
        '    "someTime" TIME WITHOUT TIME ZONE, \n'
        '    "someBoolean" BOOLEAN, \n'
        '    "someUrl" VARCHAR, \n'
        '    "someUri" VARCHAR, \n'
        '    "someBinary" BYTEA, \n'
        '    "someJson" JSONB, \n'
        '    CONSTRAINT "pk_Test" PRIMARY KEY (_id), \n'
        '    CONSTRAINT "uq_Test_someUri" UNIQUE ("someUri")\n'
        ");\n"
        "\n"
        f"{add_index(table_identifier=test_table_identifier, index_name='ix_Test__txn', columns=['_txn'])}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='_txn')}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='_created')}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='_updated')}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='_id')}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='_revision')}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='someText')}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='someInteger')}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='someNumber')}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='someDate')}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='someDateTime')}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='someTime')}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='someBoolean')}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='someUrl')}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='someUri')}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='someBinary')}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='someJson')}"
        f"{add_table_comment(table_identifier=test_table_identifier, comment='migrate/example/Test')}"
        f"{add_changelog_table(table_identifier=test_table_identifier, comment='migrate/example/Test/:changelog')}"
        f"{add_redirect_table(table_identifier=test_table_identifier, comment='migrate/example/Test/:redirect')}"
        "COMMIT;\n"
        "\n"
    )
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert not {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())
    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())

        columns = tables["migrate/example.Test"].columns
        assert {
            "someText",
            "someInteger",
            "someNumber",
            "someDate",
            "someDateTime",
            "someTime",
            "someBoolean",
            "someUrl",
            "someUri",
            "someBinary",
            "someJson",
        }.issubset(columns.keys())

        some_text = columns["someText"]
        assert isinstance(some_text.type, sa.String)
        assert some_text.nullable

        some_integer = columns["someInteger"]
        assert isinstance(some_integer.type, sa.Integer)
        assert some_integer.nullable

        some_number = columns["someNumber"]
        assert isinstance(some_number.type, sa.Float)
        assert some_number.nullable

        some_date = columns["someDate"]
        assert isinstance(some_date.type, sa.Date)
        assert some_date.nullable

        some_date_time = columns["someDateTime"]
        assert isinstance(some_date_time.type, sa.DateTime)
        assert some_date_time.nullable

        some_time = columns["someTime"]
        assert isinstance(some_time.type, sa.Time)
        assert some_time.nullable

        some_boolean = columns["someBoolean"]
        assert isinstance(some_boolean.type, sa.Boolean)
        assert some_boolean.nullable

        some_url = columns["someUrl"]
        assert isinstance(some_url.type, sa.String)
        assert some_url.nullable

        some_uri = columns["someUri"]
        assert isinstance(some_uri.type, sa.String)
        assert some_uri.nullable

        some_binary = columns["someBinary"]
        assert isinstance(some_binary.type, sa.LargeBinary)
        assert some_binary.nullable

        some_json = columns["someJson"]
        assert isinstance(some_json.type, sa.JSON)
        assert some_json.nullable

        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_add_simple_column(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())
        columns = tables["migrate/example.Test"].columns
        assert {"someText"}.issubset(columns.keys())

        some_text = columns["someText"]
        assert isinstance(some_text.type, sa.String)
        assert some_text.nullable

        assert not {"someInteger"}.issubset(columns.keys())

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
                     |   |   |      | someInteger  | integer
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])

    test_table_identifier = TableIdentifier(schema="migrate/example", base_name="Test")
    assert result.output.endswith(
        f"BEGIN;\n\n{add_column(table_identifier=test_table_identifier, column='someInteger', column_type='INTEGER')}COMMIT;\n\n"
    )

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())

        columns = tables["migrate/example.Test"].columns
        assert {"someText"}.issubset(columns.keys())

        assert not {"someInteger"}.issubset(columns.keys())

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())

        columns = tables["migrate/example.Test"].columns
        assert {"someText", "someInteger"}.issubset(columns.keys())

        some_integer = columns["someInteger"]
        assert isinstance(some_integer.type, sa.Integer)
        assert some_integer.nullable

        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_remove_simple_column(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
                     |   |   |      | someInteger  | integer
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())
        columns = tables["migrate/example.Test"].columns
        assert {"someText", "someInteger"}.issubset(columns.keys())

        some_text = columns["someText"]
        assert isinstance(some_text.type, sa.String)
        assert some_text.nullable

        some_integer = columns["someInteger"]
        assert isinstance(some_integer.type, sa.Integer)
        assert some_integer.nullable

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])

    test_table_identifier = TableIdentifier(schema="migrate/example", base_name="Test")
    assert result.output.endswith(
        f"BEGIN;\n\n{drop_column(table_identifier=test_table_identifier, column='someInteger')}COMMIT;\n\n"
    )

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())

        columns = tables["migrate/example.Test"].columns
        assert {"someText", "someInteger"}.issubset(columns.keys())

        some_integer = columns["someInteger"]
        assert isinstance(some_integer.type, sa.Integer)
        assert some_integer.nullable

        assert not {"__someInteger"}.issubset(columns.keys())

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())

        columns = tables["migrate/example.Test"].columns
        assert {"someText", "__someInteger"}.issubset(columns.keys())

        some_integer = columns["__someInteger"]
        assert isinstance(some_integer.type, sa.Integer)
        assert some_integer.nullable

        assert not {"someInteger"}.issubset(columns.keys())
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_multiple_times_remove_simple_column(
    migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    initial_manifest = """
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
                     |   |   |      | someInteger  | integer
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())
        columns = tables["migrate/example.Test"].columns
        assert {"someText", "someInteger"}.issubset(columns.keys())

        some_text = columns["someText"]
        assert isinstance(some_text.type, sa.String)
        assert some_text.nullable

        some_integer = columns["someInteger"]
        assert isinstance(some_integer.type, sa.Integer)
        assert some_integer.nullable

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])

    test_table_identifier = TableIdentifier(schema="migrate/example", base_name="Test")
    assert result.output.endswith(
        f"BEGIN;\n\n{drop_column(table_identifier=test_table_identifier, column='someInteger')}COMMIT;\n\n"
    )

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())

        columns = tables["migrate/example.Test"].columns
        assert {"someText", "someInteger"}.issubset(columns.keys())

        assert not {"__someInteger"}.issubset(columns.keys())

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())

        columns = tables["migrate/example.Test"].columns
        assert {"someText", "__someInteger"}.issubset(columns.keys())

        some_integer = columns["__someInteger"]
        assert isinstance(some_integer.type, sa.Integer)
        assert some_integer.nullable

        assert not {"someInteger"}.issubset(columns.keys())

    override_manifest(
        context,
        tmp_path,
        """
         d               | r | b | m    | property     | type
         migrate/example |   |   |      |              |
                         |   |   | Test |              |
                         |   |   |      | someText     | string
                         |   |   |      | someInteger  | integer
        """,
    )
    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])

    assert result.output.endswith(
        f"BEGIN;\n\n{add_column(table_identifier=test_table_identifier, column='someInteger', column_type='INTEGER')}COMMIT;\n\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())

        columns = tables["migrate/example.Test"].columns
        assert {"someText", "someInteger", "__someInteger"}.issubset(columns.keys())

        some_integer = columns["someInteger"]
        assert isinstance(some_integer.type, sa.Integer)
        assert some_integer.nullable

    override_manifest(
        context,
        tmp_path,
        """
         d               | r | b | m    | property     | type
         migrate/example |   |   |      |              |
                         |   |   | Test |              |
                         |   |   |      | someText     | string
        """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])

    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example"."Test" DROP COLUMN "__someInteger";\n\n'
        f"{drop_column(table_identifier=test_table_identifier, column='someInteger')}"
        "COMMIT;\n"
        "\n"
    )
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())

        columns = tables["migrate/example.Test"].columns
        assert {"someText", "someInteger", "__someInteger"}.issubset(columns.keys())

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])

    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example"."Test" DROP COLUMN "__someInteger";\n\n'
        f"{drop_column(table_identifier=test_table_identifier, column='someInteger')}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())

        columns = tables["migrate/example.Test"].columns
        assert {"someText", "__someInteger"}.issubset(columns.keys())

        some_integer = columns["__someInteger"]
        assert isinstance(some_integer.type, sa.Integer)
        assert some_integer.nullable

        assert not {"someInteger"}.issubset(columns.keys())

        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_add_unique_constraint(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())
        columns = tables["migrate/example.Test"].columns
        assert {"someText"}.issubset(columns.keys())

        some_text = columns["someText"]
        assert isinstance(some_text.type, sa.String)
        assert some_text.nullable

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string unique
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])

    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example"."Test" ADD CONSTRAINT '
        '"uq_Test_someText" UNIQUE ("someText");\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())

        constraint_columns = get_table_unique_constraint_columns(tables["migrate/example.Test"])
        assert not any(columns == ["someText"] for columns in constraint_columns)

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())

        constraint_columns = get_table_unique_constraint_columns(tables["migrate/example.Test"])
        assert any(columns == ["someText"] for columns in constraint_columns)

        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_remove_unique_constraint(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string unique
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())
        columns = tables["migrate/example.Test"].columns
        assert {"someText"}.issubset(columns.keys())

        some_text = columns["someText"]
        assert isinstance(some_text.type, sa.String)
        assert some_text.nullable

        constraint_columns = get_table_unique_constraint_columns(tables["migrate/example.Test"])
        assert any(columns == ["someText"] for columns in constraint_columns)

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])

    assert result.output.endswith(
        'BEGIN;\n\nALTER TABLE "migrate/example"."Test" DROP CONSTRAINT "uq_Test_someText";\n\nCOMMIT;\n\n'
    )

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())

        constraint_columns = get_table_unique_constraint_columns(tables["migrate/example.Test"])
        assert any(columns == ["someText"] for columns in constraint_columns)

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())

        constraint_columns = get_table_unique_constraint_columns(tables["migrate/example.Test"])
        assert not any(columns == ["someText"] for columns in constraint_columns)

        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_create_models_with_base(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b | m    | property     | type | ref | level
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property     | type          | ref                  | level
     migrate/example |   |      |      |              |               |                      |
                     |   |      | Base |              |               | someText, someNumber |
                     |   |      |      | someText     | string        |                      |
                     |   |      |      | someInteger  | integer       |                      |
                     |   |      |      | someNumber   | number        |                      |
                     |   | Base |      |              |               |                      |
                     |   |      | Test |              |               |                      |
                     |   |      |      | someText     |               |                      |

    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])

    base_table_identifier = TableIdentifier(schema="migrate/example", base_name="Base")
    test_table_identifier = TableIdentifier(schema="migrate/example", base_name="Test")
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_schema(schema='migrate/example')}"
        'CREATE TABLE "migrate/example"."Base" (\n'
        "    _txn UUID, \n"
        "    _created TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _updated TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _id UUID NOT NULL, \n"
        "    _revision TEXT, \n"
        '    "someText" TEXT, \n'
        '    "someInteger" INTEGER, \n'
        '    "someNumber" FLOAT, \n'
        '    CONSTRAINT "pk_Base" PRIMARY KEY (_id), \n'
        '    CONSTRAINT "uq_Base_someText_someNumber" UNIQUE '
        '("someText", "someNumber")\n'
        ");\n"
        "\n"
        f"{add_index(table_identifier=base_table_identifier, index_name='ix_Base__txn', columns=['_txn'])}"
        f"{add_column_comment(table_identifier=base_table_identifier, column='_txn')}"
        f"{add_column_comment(table_identifier=base_table_identifier, column='_created')}"
        f"{add_column_comment(table_identifier=base_table_identifier, column='_updated')}"
        f"{add_column_comment(table_identifier=base_table_identifier, column='_id')}"
        f"{add_column_comment(table_identifier=base_table_identifier, column='_revision')}"
        f"{add_column_comment(table_identifier=base_table_identifier, column='someText')}"
        f"{add_column_comment(table_identifier=base_table_identifier, column='someInteger')}"
        f"{add_column_comment(table_identifier=base_table_identifier, column='someNumber')}"
        f"{add_table_comment(table_identifier=base_table_identifier, comment='migrate/example/Base')}"
        f"{add_changelog_table(table_identifier=base_table_identifier, comment='migrate/example/Base/:changelog')}"
        f"{add_redirect_table(table_identifier=base_table_identifier, comment='migrate/example/Base/:redirect')}"
        'CREATE TABLE "migrate/example"."Test" (\n'
        "    _txn UUID, \n"
        "    _created TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _updated TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _id UUID NOT NULL, \n"
        "    _revision TEXT, \n"
        '    CONSTRAINT "pk_Test" PRIMARY KEY (_id), \n'
        '    CONSTRAINT "fk_Test__id_Base" FOREIGN KEY(_id) REFERENCES '
        '"migrate/example"."Base" (_id)\n'
        ");\n"
        "\n"
        f"{add_index(table_identifier=test_table_identifier, index_name='ix_Test__txn', columns=['_txn'])}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='_txn')}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='_created')}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='_updated')}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='_id')}"
        f"{add_column_comment(table_identifier=test_table_identifier, column='_revision')}"
        f"{add_table_comment(table_identifier=test_table_identifier, comment='migrate/example/Test')}"
        f"{add_changelog_table(table_identifier=test_table_identifier, comment='migrate/example/Test/:changelog')}"
        f"{add_redirect_table(table_identifier=test_table_identifier, comment='migrate/example/Test/:redirect')}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {
            "migrate/example.Test",
            "migrate/example.Test/:changelog",
            "migrate/example.Base",
            "migrate/example.Base/:changelog",
        }.issubset(tables.keys())

        columns = get_table_foreign_key_constraint_columns(tables["migrate/example.Test"])
        assert any(
            [["_id"], ["_id"]] == [constraint["column_names"], constraint["referred_column_names"]]
            for constraint in columns
        )

        cleanup_table_list(
            meta,
            [
                "migrate/example/Test",
                "migrate/example/Test/:changelog",
                "migrate/example/Base",
                "migrate/example/Base/:changelog",
            ],
        )


def test_migrate_remove_model(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b    | m    | property       | type     | ref | source
     migrate/example |   |      |      |                |          |     |
                     |   |      | Ref  |                |          |     |
                     |   |      |      | someText       | string   |     |
                     |   |      |      |                |          |     |
                     |   |      | Test |                |          |     |
                     |   |      |      | someText       | string   |     |
                     |   |      |      | someFile       | file     |     |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {
            "migrate/example.Ref",
            "migrate/example.Test",
            "migrate/example.Test/:changelog",
            "migrate/example.Test/:file/someFile",
        }.issubset(tables.keys())

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m   | property       | type     | ref    | source
     migrate/example |   |      |     |                |          |        |
                     |   |      | Ref |                |          |        |
                     |   |      |     | someText       | string   |        |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    test_table_identifier = TableIdentifier(schema="migrate/example", base_name="Test")

    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{drop_table(table_identifier=test_table_identifier, remove_model_only=True)}"
        f"{drop_index(table_identifier=test_table_identifier, index_name='ix_Test__txn')}"
        f"{drop_table(table_identifier=test_table_identifier.change_table_type(new_type=TableType.CHANGELOG), remove_model_only=True)}"
        'ALTER SEQUENCE "migrate/example"."Test/:changelog__id_seq" RENAME TO '
        '"__Test/:changelog__id_seq";\n'
        "\n"
        f"{drop_index(table_identifier=test_table_identifier, index_name='ix_Test/:changelog__txn')}"
        f"{drop_table(table_identifier=test_table_identifier.change_table_type(new_type=TableType.REDIRECT), remove_model_only=True)}"
        f"{drop_index(table_identifier=test_table_identifier, index_name='ix_Test/:redirect_redirect')}"
        f"{drop_table(table_identifier=test_table_identifier.change_table_type(new_type=TableType.FILE, table_arg='someFile'), remove_model_only=True)}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {
            "migrate/example.Ref",
            "migrate/example.Ref/:changelog",
            "migrate/example.Ref/:redirect",
            "migrate/example.__Test",
            "migrate/example.__Test/:changelog",
            "migrate/example.__Test/:redirect",
            "migrate/example.__Test/:file/someFile",
        }.issubset(tables.keys())

        assert not {
            "migrate/example.Test",
            "migrate/example.Test/:changelog",
            "migrate/example.Test/:redirect",
            "migrate/example.Test/:file/someFile",
        }.issubset(tables.keys())

        cleanup_table_list(
            meta,
            [
                "migrate/example/Ref",
                "migrate/example/Ref/:changelog",
                "migrate/example/Ref/:redirect",
                "migrate/example/__Test",
                "migrate/example/__Test/:changelog",
                "migrate/example/__Test/:redirect",
                "migrate/example/__Test/:file/someFile",
            ],
        )


def test_migrate_remove_base_from_model(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
 d               | r | b    | m    | property     | type          | ref                  | level
     migrate/example |   |      |      |              |               |                      |
                     |   |      | Base |              |               | someText, someNumber |
                     |   |      |      | someText     | string        |                      |
                     |   |      |      | someInteger  | integer       |                      |
                     |   |      |      | someNumber   | number        |                      |
                     |   | Base |      |              |               |                      |
                     |   |      | Test |              |               |                      |
                     |   |      |      | someText     | string        |                      |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property     | type          | ref                  | level
     migrate/example |   |      |      |              |               |                      |
                     |   |      | Base |              |               | someText, someNumber |
                     |   |      |      | someText     | string        |                      |
                     |   |      |      | someInteger  | integer       |                      |
                     |   |      |      | someNumber   | number        |                      |
                     |   |      |      |              |               |                      |
                     |   |      | Test |              |               |                      |
                     |   |      |      | someText     | string        |                      |

    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        'BEGIN;\n\nALTER TABLE "migrate/example"."Test" DROP CONSTRAINT "fk_Test__id_Base";\n\nCOMMIT;\n\n'
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {
            "migrate/example.Test",
            "migrate/example.Test/:changelog",
            "migrate/example.Base",
            "migrate/example.Base/:changelog",
        }.issubset(tables.keys())

        columns = get_table_foreign_key_constraint_columns(tables["migrate/example.Test"])
        assert not any(
            [["_id"], ["_id"]] == [constraint["column_names"], constraint["referred_column_names"]]
            for constraint in columns
        )

        cleanup_table_list(
            meta,
            [
                "migrate/example/Test",
                "migrate/example/Test/:changelog",
                "migrate/example/Base",
                "migrate/example/Base/:changelog",
            ],
        )


def test_migrate_rename_model(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b    | m    | property       | type     | ref | source
     migrate/example |   |      |      |                |          |     |
                     |   |      | Ref  |                |          |     |
                     |   |      |      | someText       | string   |     |
                     |   |      |      |                |          |     |
                     |   |      | Test |                |          |     |
                     |   |      |      | someText       | string   |     |
                     |   |      |      | someFile       | file     |     |
                     |   |      |      | someRef        | ref      | Ref |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {
            "migrate/example.Ref",
            "migrate/example.Test",
            "migrate/example.Test/:changelog",
            "migrate/example.Test/:file/someFile",
        }.issubset(tables.keys())
        table = tables["migrate/example.Test"]

        constraints = get_table_foreign_key_constraint_columns(table)
        assert any(constraint["constraint_name"] == "fk_Test_someRef._id_Ref" for constraint in constraints)

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m      | property       | type     | ref    | source
     migrate/example |   |      |        |                |          |        |
                     |   |      | NewRef |                |          |        |
                     |   |      |        | someText       | string   |        |
                     |   |      |        |                |          |        |
                     |   |      | New    |                |          |        |
                     |   |      |        | someText       | string   |        |
                     |   |      |        | someFile       | file     |        |
                     |   |      |        | someRef        | ref      | NewRef |
    """,
    )

    rename_file = {
        "migrate/example/Test": {"": "migrate/example/New"},
        "migrate/example/Ref": {"": "migrate/example/NewRef"},
    }
    path = tmp_path / "rename.json"
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-r", path])
    test_table_identifier = TableIdentifier(schema="migrate/example", base_name="Test")
    new_table_identifier = TableIdentifier(schema="migrate/example", base_name="New")
    ref_table_identifier = TableIdentifier(schema="migrate/example", base_name="Ref")
    new_ref_table_identifier = TableIdentifier(schema="migrate/example", base_name="NewRef")

    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{rename_table(old_table_identifier=ref_table_identifier, new_table_identifier=new_ref_table_identifier)}"
        f"{rename_changelog(old_table_identifier=ref_table_identifier, new_table_identifier=new_ref_table_identifier)}"
        f"{rename_redirect(old_table_identifier=ref_table_identifier, new_table_identifier=new_ref_table_identifier)}"
        f"{rename_index(table_identifier=ref_table_identifier, old_index_name='ix_Ref__txn', new_index_name='ix_NewRef__txn')}"
        f"{rename_table(old_table_identifier=test_table_identifier, new_table_identifier=new_table_identifier)}"
        f"{rename_changelog(old_table_identifier=test_table_identifier, new_table_identifier=new_table_identifier)}"
        f"{rename_redirect(old_table_identifier=test_table_identifier, new_table_identifier=new_table_identifier)}"
        f"{rename_table(old_table_identifier=test_table_identifier.change_table_type(new_type=TableType.FILE, table_arg='someFile'), new_table_identifier=new_table_identifier.change_table_type(new_type=TableType.FILE, table_arg='someFile'))}"
        f"{rename_index(table_identifier=test_table_identifier, old_index_name='ix_Test_someRef._id', new_index_name='ix_New_someRef._id')}"
        f"{rename_index(table_identifier=test_table_identifier, old_index_name='ix_Test__txn', new_index_name='ix_New__txn')}"
        'ALTER TABLE "migrate/example"."New" RENAME CONSTRAINT '
        '"fk_Test_someRef._id_Ref" TO '
        '"fk_New_someRef._id_NewRef";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-r", path])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {
            "migrate/example.New",
            "migrate/example.New/:changelog",
            "migrate/example.New/:redirect",
            "migrate/example.New/:file/someFile",
            "migrate/example.NewRef",
        }.issubset(tables.keys())

        assert not {
            "migrate/example.Test",
            "migrate/example.Test/:changelog",
            "migrate/example.Test/:redirect",
            "migrate/example.Test/:file/someFile",
            "migrate/example.Ref",
        }.issubset(tables.keys())

        table = tables["migrate/example.New"]

        constraints = get_table_foreign_key_constraint_columns(table)
        assert any(constraint["constraint_name"] == "fk_New_someRef._id_NewRef" for constraint in constraints)
        assert not any(constraint["constraint_name"] == "fk_Test_someRef._id_Ref" for constraint in constraints)

        cleanup_table_list(
            meta,
            [
                "migrate/example/New",
                "migrate/example/New/:changelog",
                "migrate/example/New/:redirect",
                "migrate/example/New/:file/someFile",
                "migrate/example/NewRef",
            ],
        )


def test_migrate_rename_property(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Ref  |                |          | someText |
                     |   |      |      | someText       | string   |          |
                     |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | someText       | string   |          |
                     |   |      |      | someFile       | file     |          |
                     |   |      |      | someRef        | ref      | Ref      | 3
                     |   |      |      | someOther      | ref      | Ref      | 4
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {
            "migrate/example.Ref",
            "migrate/example.Test",
            "migrate/example.Test/:changelog",
            "migrate/example.Test/:file/someFile",
        }.issubset(tables.keys())
        table = tables["migrate/example.Test"]
        columns = table.columns
        assert {
            "someText",
            "someRef.someText",
            "someOther._id",
            "someFile._id",
            "someFile._content_type",
            "someFile._size",
            "someFile._bsize",
            "someFile._blocks",
        }.issubset(columns.keys())

        constraints = get_table_foreign_key_constraint_columns(table)
        assert any(constraint["constraint_name"] == "fk_Test_someOther._id_Ref" for constraint in constraints)

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Ref  |                |          | newText  |
                     |   |      |      | newText        | string   |          |
                     |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | newText        | string   |          |
                     |   |      |      | newFile        | file     |          |
                     |   |      |      | newRef         | ref      | Ref      | 3
                     |   |      |      | newOther       | ref      | Ref      | 4
    """,
    )

    rename_file = {
        "migrate/example/Test": {
            "someText": "newText",
            "someFile": "newFile",
            "someRef": "newRef",
            "someOther": "newOther",
        },
        "migrate/example/Ref": {"someText": "newText"},
    }
    path = tmp_path / "rename.json"
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-r", path])

    test_table_identifier = TableIdentifier(schema="migrate/example", base_name="Test")
    ref_table_identifier = TableIdentifier(schema="migrate/example", base_name="Ref")
    assert result.output.endswith(
        "BEGIN;\n\n"
        f"{rename_column(table_identifier=ref_table_identifier, column='someText', new_name='newText')}"
        'ALTER TABLE "migrate/example"."Ref" RENAME CONSTRAINT '
        '"uq_Ref_someText" TO "uq_Ref_newText";\n'
        "\n"
        f"{rename_column(table_identifier=test_table_identifier, column='someText', new_name='newText')}"
        f"{rename_column(table_identifier=test_table_identifier, column='someFile._id', new_name='newFile._id')}"
        f"{rename_column(table_identifier=test_table_identifier, column='someFile._content_type', new_name='newFile._content_type')}"
        f"{rename_column(table_identifier=test_table_identifier, column='someFile._size', new_name='newFile._size')}"
        f"{rename_column(table_identifier=test_table_identifier, column='someFile._bsize', new_name='newFile._bsize')}"
        f"{rename_column(table_identifier=test_table_identifier, column='someFile._blocks', new_name='newFile._blocks')}"
        f"{rename_table(old_table_identifier=test_table_identifier.change_table_type(new_type=TableType.FILE, table_arg='someFile'), new_table_identifier=test_table_identifier.change_table_type(new_type=TableType.FILE, table_arg='newFile'))}"
        f"{rename_index(table_identifier=test_table_identifier, old_index_name='ix_Test_someOther._id', new_index_name='ix_Test_newOther._id')}"
        f"{rename_column(table_identifier=test_table_identifier, column='someRef.someText', new_name='newRef.newText')}"
        f"{rename_column(table_identifier=test_table_identifier, column='someOther._id', new_name='newOther._id')}"
        'ALTER TABLE "migrate/example"."Test" RENAME CONSTRAINT '
        '"fk_Test_someOther._id_Ref" TO '
        '"fk_Test_newOther._id_Ref";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-r", path])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {
            "migrate/example.Test",
            "migrate/example.Test/:changelog",
            "migrate/example.Test/:file/newFile",
            "migrate/example.Ref",
        }.issubset(tables.keys())

        table = tables["migrate/example.Ref"]
        columns = table.columns
        assert {"newText"}.issubset(columns.keys())
        assert not {"someText"}.issubset(columns.keys())

        table = tables["migrate/example.Test"]
        columns = table.columns
        assert {
            "newText",
            "newOther._id",
            "newRef.newText",
            "newFile._id",
            "newFile._content_type",
            "newFile._size",
            "newFile._bsize",
            "newFile._blocks",
        }.issubset(columns.keys())
        assert not {
            "someText",
            "someOther._id",
            "someRef.someText",
            "someFile._id",
            "someFile._content_type",
            "someFile._size",
            "someFile._bsize",
            "someFile._blocks",
        }.issubset(columns.keys())

        constraints = get_table_foreign_key_constraint_columns(table)
        assert any(constraint["constraint_name"] == "fk_Test_newOther._id_Ref" for constraint in constraints)
        assert not any(constraint["constraint_name"] == "fk_Test_someOther._id_Ref" for constraint in constraints)

        cleanup_table_list(
            meta,
            [
                "migrate/example/Test",
                "migrate/example/Test/:changelog",
                "migrate/example/Test/:file/newFile",
                "migrate/example/Ref",
            ],
        )


def test_migrate_long_names(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b | m    | property     | type   | ref
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type           | ref | source
     migrate/example/very/very/long/dataset/name |   |      |      |                |                |     |
                     |   |      | ExtremelyLongModelName |                |                | veryLongPrimaryKeyName |
                     |   |      |      | veryLongPrimaryKeyName       | string         |     |
                     |   |      |      | veryLongGeometryPropertyName | geometry       |     |
                     |   |      |      | veryLongGeometryPropertyNameOther | geometry       |     |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])

    long_table_identifier = TableIdentifier(
        schema="migrate/example/very/very/long/dataset/name", base_name="ExtremelyLongModelName"
    )
    pieces = [
        (
            'CREATE INDEX "ix_ExtremelyLongModelName_veryLongGeometryPropertyNameOther" '
            'ON "migrate/example/very/very/long/dataset/name"."ExtremelyLongModelName" '
            'USING gist ("veryLongGeometryPropertyNameOther");\n'
            "\n"
        ),
        (
            "CREATE INDEX "
            '"ix_ExtremelyLongModelName_veryLongGeometryPropertyName" ON '
            '"migrate/example/very/very/long/dataset/name"."ExtremelyLongModelName" USING '
            'gist ("veryLongGeometryPropertyName");\n'
            "\n"
        ),
    ]

    combos = itertools.permutations(pieces, 2)
    ordered = pieces[0]
    for combo in combos:
        parsed = "".join(combo)
        if parsed in result.output:
            ordered = parsed
            break

    assert result.output.endswith(
        "BEGIN;\n\n"
        f"{add_schema('migrate/example/very/very/long/dataset/name')}"
        "CREATE TABLE "
        '"migrate/example/very/very/long/dataset/name"."ExtremelyLongModelName" (\n'
        "    _txn UUID, \n"
        "    _created TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _updated TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _id UUID NOT NULL, \n"
        "    _revision TEXT, \n"
        '    "veryLongPrimaryKeyName" TEXT, \n'
        '    "veryLongGeometryPropertyName" geometry(GEOMETRY,4326), \n'
        '    "veryLongGeometryPropertyNameOther" geometry(GEOMETRY,4326), \n'
        '    CONSTRAINT "pk_ExtremelyLongModelName" PRIMARY KEY (_id), \n'
        '    CONSTRAINT "uq_ExtremelyLongModelName_veryLongPrimaryKeyName" UNIQUE '
        '("veryLongPrimaryKeyName")\n'
        ");\n"
        "\n"
        f"{add_index(table_identifier=long_table_identifier, index_name='ix_ExtremelyLongModelName__txn', columns=['_txn'])}"
        f"{add_column_comment(table_identifier=long_table_identifier, column='_txn')}"
        f"{add_column_comment(table_identifier=long_table_identifier, column='_created')}"
        f"{add_column_comment(table_identifier=long_table_identifier, column='_updated')}"
        f"{add_column_comment(table_identifier=long_table_identifier, column='_id')}"
        f"{add_column_comment(table_identifier=long_table_identifier, column='_revision')}"
        f"{add_column_comment(table_identifier=long_table_identifier, column='veryLongPrimaryKeyName')}"
        f"{add_column_comment(table_identifier=long_table_identifier, column='veryLongGeometryPropertyName')}"
        f"{add_column_comment(table_identifier=long_table_identifier, column='veryLongGeometryPropertyNameOther')}"
        f"{add_table_comment(table_identifier=long_table_identifier, comment='migrate/example/very/very/long/dataset/name/ExtremelyLongModelName')}"
        f"{ordered}"
        f"{add_changelog_table(table_identifier=long_table_identifier, comment='migrate/example/very/very/long/dataset/name/ExtremelyLongModelName/:changelog')}"
        f"{add_redirect_table(table_identifier=long_table_identifier, comment='migrate/example/very/very/long/dataset/name/ExtremelyLongModelName/:redirect')}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example/very/very/long/dataset/name")
        tables = meta.tables
        assert "migrate/example/very/very/long/dataset/name.ExtremelyLongModelName" in tables

        cleanup_table_list(
            meta,
            [
                "migrate/example/very/very/long/dataset/name/ExtremelyLongModelName",
                "migrate/example/very/very/long/dataset/name/ExtremelyLongModelName/:changelog",
            ],
        )


def test_migrate_rename_already_existing_property(
    migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    initial_manifest = """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | someText       | string   |          |
                     |   |      |      | otherText      | string   |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example.Test"]
        columns = table.columns
        assert {"someText", "otherText"}.issubset(columns.keys())

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | otherText      | string   |          |
    """,
    )

    rename_file = {
        "migrate/example/Test": {
            "someText": "otherText",
        },
    }
    path = tmp_path / "rename.json"
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-r", path])
    test_table_identifier = TableIdentifier(schema="migrate/example", base_name="Test")
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{drop_column(table_identifier=test_table_identifier, column='otherText')}"
        f"{rename_column(table_identifier=test_table_identifier, column='someText', new_name='otherText')}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-r", path])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())

        table = tables["migrate/example.Test"]
        columns = table.columns
        assert {"otherText", "__otherText"}.issubset(columns.keys())
        assert not {"someText"}.issubset(columns.keys())

        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_change_basic_type(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | someInt        | string   |          |
                     |   |      |      | someFloat      | string   |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example.Test"]
        columns = table.columns
        assert {"someInt", "someFloat"}.issubset(columns.keys())
        conn.execute(
            table.insert().values({"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a", "someInt": "1", "someFloat": "1.5"})
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | someInt        | integer  |          |
                     |   |      |      | someFloat      | number   |          |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])

    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example"."Test" ALTER COLUMN "someInt" TYPE INTEGER USING '
        'CAST("migrate/example"."Test"."someInt" AS INTEGER);\n'
        "\n"
        'ALTER TABLE "migrate/example"."Test" ALTER COLUMN "someFloat" TYPE FLOAT USING '
        'CAST("migrate/example"."Test"."someFloat" AS FLOAT);\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(
        rc,
        [
            "migrate",
            f"{tmp_path}/manifest.csv",
        ],
    )
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())

        table = tables["migrate/example.Test"]
        columns = table.columns
        assert {"someFloat", "someInt"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["someInt"] == 1
            assert row["someFloat"] == 1.5

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | someInt        | string   |          |
                     |   |      |      | someFloat      | string   |          |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])

    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example"."Test" ALTER COLUMN "someInt" TYPE TEXT USING '
        'CAST("migrate/example"."Test"."someInt" AS TEXT);\n'
        "\n"
        'ALTER TABLE "migrate/example"."Test" ALTER COLUMN "someFloat" TYPE TEXT USING '
        'CAST("migrate/example"."Test"."someFloat" AS TEXT);\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(
        rc,
        [
            "migrate",
            f"{tmp_path}/manifest.csv",
        ],
    )
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())

        table = tables["migrate/example.Test"]
        columns = table.columns
        assert {"someFloat", "someInt"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["someInt"] == "1"
            assert row["someFloat"] == "1.5"
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_datasets(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string unique
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
                     |   |   |      | newColumn    | integer
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-d", "migrate/example"])
    assert result.exit_code == 0
    test_table_identifier = TableIdentifier(schema="migrate/example", base_name="Test")
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_column(table_identifier=test_table_identifier, column='newColumn', column_type='INTEGER')}"
        'ALTER TABLE "migrate/example"."Test" DROP CONSTRAINT '
        '"uq_Test_someText";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-d", "migrate/example"])
    assert result.exit_code == 0
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert "migrate/example.Test" in tables

        table = tables["migrate/example.Test"]
        columns = table.columns
        assert "newColumn" in columns
        assert "someText" in columns

        cleanup_table_list(meta, ["migrate/example/Test"])


def test_migrate_datasets_multiple_models(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string unique
                     |   |   | Test2|              |
                     |   |   |      | anotherText  | string unique
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
                     |   |   |      | newColumn    | integer
                     |   |   | Test2|              |
                     |   |   |      | anotherText  | string
                     |   |   |      | newColumn2   | integer
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-d", "migrate/example"])
    assert result.exit_code == 0
    test_table_identifier = TableIdentifier(schema="migrate/example", base_name="Test")
    test2_table_identifier = TableIdentifier(schema="migrate/example", base_name="Test2")
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_column(table_identifier=test_table_identifier, column='newColumn', column_type='INTEGER')}"
        'ALTER TABLE "migrate/example"."Test" DROP CONSTRAINT '
        '"uq_Test_someText";\n'
        "\n"
        f"{add_column(table_identifier=test2_table_identifier, column='newColumn2', column_type='INTEGER')}"
        'ALTER TABLE "migrate/example"."Test2" DROP CONSTRAINT '
        '"uq_Test2_anotherText";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-d", "migrate/example"])
    assert result.exit_code == 0
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert "migrate/example.Test" in tables
        assert "migrate/example.Test2" in tables

        table1 = tables["migrate/example.Test"]
        columns1 = table1.columns
        assert "newColumn" in columns1
        assert "someText" in columns1

        table2 = tables["migrate/example.Test2"]
        columns2 = table2.columns
        assert "newColumn2" in columns2
        assert "anotherText" in columns2

        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test2"])


def test_migrate_datasets_invalid(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path, caplog):
    initial_manifest = """
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string unique
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
                     |   |   |      | newColumn    | integer
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-d", "invalid/dataset"], fail=False)
    assert result.exit_code == 1
    assert "Invalid dataset(s) provided: invalid/dataset" in result.stderr


def test_migrate_datasets_single(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b | m    | property     | type
     dataset1        |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string unique
     dataset2        |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | anotherText  | string unique
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m    | property     | type
     dataset1        |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
                     |   |   |      | newColumn    | integer
     dataset2        |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | anotherText  | string
                     |   |   |      | newColumn2   | integer
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-d", "dataset1"])
    assert result.exit_code == 0
    test_table_identifier = TableIdentifier(schema="dataset1", base_name="Test")
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_column(table_identifier=test_table_identifier, column='newColumn', column_type='INTEGER')}"
        'ALTER TABLE dataset1."Test" DROP CONSTRAINT '
        '"uq_Test_someText";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-d", "dataset1"])
    assert result.exit_code == 0
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="dataset1")
        meta.reflect(schema="dataset2")
        tables = meta.tables
        assert "dataset1.Test" in tables
        assert "dataset2.Test" in tables

        table1 = tables["dataset1.Test"]
        columns1 = table1.columns
        assert "newColumn" in columns1
        assert "someText" in columns1

        table2 = tables["dataset2.Test"]
        columns2 = table2.columns
        assert "newColumn2" not in columns2
        assert "anotherText" in columns2

        cleanup_table_list(meta, ["dataset1/Test", "dataset2/Test"])


def test_migrate_datasets_list(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b | m    | property     | type
     dataset1        |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string unique
     dataset2        |   |   |      |              |
                     |   |   | Test2|              |
                     |   |   |      | anotherText  | string unique
     dataset3        |   |   |      |              |
                     |   |   | Test3|              |
                     |   |   |      | thirdText    | string unique
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m    | property     | type
     dataset1        |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
                     |   |   |      | newColumn    | integer
     dataset2        |   |   |      |              |
                     |   |   | Test2|              |
                     |   |   |      | anotherText  | string
                     |   |   |      | newColumn2   | integer
     dataset3        |   |   |      |              |
                     |   |   | Test3|              |
                     |   |   |      | thirdText    | string
                     |   |   |      | newColumn3   | integer
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-d", "dataset1", "-d", "dataset2"])
    assert result.exit_code == 0
    test_table_identifier = TableIdentifier(schema="dataset1", base_name="Test")
    test2_table_identifier = TableIdentifier(schema="dataset2", base_name="Test2")
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_column(table_identifier=test_table_identifier, column='newColumn', column_type='INTEGER')}"
        'ALTER TABLE dataset1."Test" DROP CONSTRAINT "uq_Test_someText";\n'
        "\n"
        f"{add_column(table_identifier=test2_table_identifier, column='newColumn2', column_type='INTEGER')}"
        'ALTER TABLE dataset2."Test2" DROP CONSTRAINT '
        '"uq_Test2_anotherText";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-d", "dataset1", "-d", "dataset2"])
    assert result.exit_code == 0
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="dataset1")
        meta.reflect(schema="dataset2")
        meta.reflect(schema="dataset3")
        tables = meta.tables
        assert "dataset1.Test" in tables
        assert "dataset2.Test2" in tables
        assert "dataset3.Test3" in tables

        table1 = tables["dataset1.Test"]
        columns1 = table1.columns
        assert "newColumn" in columns1
        assert "someText" in columns1

        table2 = tables["dataset2.Test2"]
        columns2 = table2.columns
        assert "newColumn2" in columns2
        assert "anotherText" in columns2

        table3 = tables["dataset3.Test3"]
        columns3 = table3.columns
        assert "newColumn3" not in columns3
        assert "thirdText" in columns3

        cleanup_table_list(meta, ["dataset1/Test", "dataset2/Test2", "dataset3/Test3"])


def test_migrate_incorrect_unique_constraint_name(
    migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    initial_manifest = """
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string unique
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())

        constraint_columns = get_table_unique_constraint_columns(tables["migrate/example.Test"])
        assert any(columns == ["someText"] for columns in constraint_columns)
        # Corrupt unique constraint name

        conn.execute(
            'ALTER TABLE "migrate/example"."Test" RENAME CONSTRAINT "uq_Test_someText" TO "corrupted_unique_constraint"'
        )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example"."Test" RENAME CONSTRAINT '
        'corrupted_unique_constraint TO "uq_Test_someText";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())

        constraint_columns = get_table_unique_constraint_columns(tables["migrate/example.Test"])
        assert any(columns == ["someText"] for columns in constraint_columns)

        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_incorrect_index_name(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b    | m    | property       | type     | ref | source
     migrate/example |   |      |      |                |          |     |
                     |   |      | Ref  |                |          |     |
                     |   |      |      | someText       | string   |     |
                     |   |      |      |                |          |     |
                     |   |      | Test |                |          |     |
                     |   |      |      | someText       | string   |     |
                     |   |      |      | someRef        | ref      | Ref |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {
            "migrate/example.Test",
            "migrate/example.Test/:changelog",
            "migrate/example.Ref",
            "migrate/example.Ref/:changelog",
        }.issubset(tables.keys())

        # Corrupt ref index name
        conn.execute('ALTER INDEX "migrate/example"."ix_Test_someRef._id" RENAME TO "corrupted_index_name"')

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    test_table_identifier = TableIdentifier(schema="migrate/example", base_name="Test")
    assert result.output.endswith(
        f"BEGIN;\n\n{rename_index(table_identifier=test_table_identifier, old_index_name='corrupted_index_name', new_index_name='ix_Test_someRef._id')}COMMIT;\n\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")

        cleanup_table_list(
            meta,
            [
                "migrate/example/Test",
                "migrate/example/Test/:changelog",
                "migrate/example/Ref",
                "migrate/example/Ref/:changelog",
            ],
        )


def test_migrate_incorrect_foreign_key_constraint_name(
    migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    initial_manifest = """
     d               | r | b    | m    | property       | type     | ref | source
     migrate/example |   |      |      |                |          |     |
                     |   |      | Ref  |                |          |     |
                     |   |      |      | someText       | string   |     |
                     |   |      |      |                |          |     |
                     |   |      | Test |                |          |     |
                     |   |      |      | someText       | string   |     |
                     |   |      |      | someRef        | ref      | Ref |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {
            "migrate/example.Test",
            "migrate/example.Test/:changelog",
            "migrate/example.Ref",
            "migrate/example.Ref/:changelog",
        }.issubset(tables.keys())

        table = tables["migrate/example.Test"]

        constraints = get_table_foreign_key_constraint_columns(table)
        assert any(constraint["constraint_name"] == "fk_Test_someRef._id_Ref" for constraint in constraints)

        # Corrupt ref index name
        conn.execute(
            'ALTER TABLE "migrate/example"."Test" RENAME CONSTRAINT "fk_Test_someRef._id_Ref" TO "corrupted_fkey_constraint"'
        )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example"."Test" RENAME CONSTRAINT '
        'corrupted_fkey_constraint TO "fk_Test_someRef._id_Ref";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")

        table = tables["migrate/example.Test"]

        constraints = get_table_foreign_key_constraint_columns(table)
        assert any(constraint["constraint_name"] == "fk_Test_someRef._id_Ref" for constraint in constraints)

        cleanup_table_list(
            meta,
            [
                "migrate/example/Test",
                "migrate/example/Test/:changelog",
                "migrate/example/Ref",
                "migrate/example/Ref/:changelog",
            ],
        )


def test_migrate_invalid_cast_error(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b | m    | property | type   | ref | source
     migrate/example |   |   |      |          |        |     |
                     |   |   | Test |          |        |     |
                     |   |   |      | someDate | number |     |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example.Test"]
        conn.execute(
            table.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "someDate": "10.568",
                }
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m    | property | type | ref | source
     migrate/example |   |   |      |          |      |     |
                     |   |   | Test |          |      |     |
                     |   |   |      | someDate | date |     |
    """,
    )

    with pytest.raises(UnableToCastColumnTypes):
        result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "--raise"], fail=False)
        raise result.exception

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_invalid_cast_warning(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b | m    | property | type   | ref | source
     migrate/example |   |   |      |          |        |     |
                     |   |   | Test |          |        |     |
                     |   |   |      | someDate | number |     |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example.Test"]
        conn.execute(
            table.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "someDate": "10.568",
                }
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m    | property | type | ref | source
     migrate/example |   |   |      |          |      |     |
                     |   |   | Test |          |      |     |
                     |   |   |      | someDate | date |     |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example"."Test" ALTER COLUMN "someDate" TYPE DATE USING '
        'CAST("migrate/example"."Test"."someDate" AS DATE);\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )
    assert (
        "WARNING: Casting 'someDate' (from 'migrate/example/Test' model) column's type from 'DOUBLE PRECISION' to 'DATE' might not be possible."
        in result.stderr
    )

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_unsafe_cast_warning(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b | m    | property | type   | ref | source
     migrate/example |   |   |      |          |        |     |
                     |   |   | Test |          |        |     |
                     |   |   |      | someInt  | string |     |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables
        assert {"migrate/example.Test", "migrate/example.Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example.Test"]
        conn.execute(
            table.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "someInt": "50",
                }
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m    | property | type    | ref | source
     migrate/example |   |   |      |          |         |     |
                     |   |   | Test |          |         |     |
                     |   |   |      | someInt  | integer |     |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "--raise"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example"."Test" ALTER COLUMN "someInt" TYPE INTEGER USING '
        'CAST("migrate/example"."Test"."someInt" AS INTEGER);\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )
    assert (
        "WARNING: Casting 'someInt' (from 'migrate/example/Test' model) column's type from 'TEXT' to 'INTEGER' might not be possible."
        in result.stderr
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "--raise"])
    assert (
        "WARNING: Casting 'someInt' (from 'migrate/example/Test' model) column's type from 'TEXT' to 'INTEGER' might not be possible."
        in result.stderr
    )

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="migrate/example")
        tables = meta.tables

        table = tables["migrate/example.Test"]

        result = conn.execute(table.select())
        for item in result:
            assert item["_id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7e"
            assert item["someInt"] == 50
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_long_name_no_changes(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b | m    | property | type   | ref | source
     datasets/gov/migrate/example/very/long/dataset/name |   |   |      |          |        |     |
                     |   |   | LongModelName |          |        |     |
                     |   |   |      | someInt  | integer |     |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])
    table_identifier = TableIdentifier(
        schema="datasets/gov/migrate/example/very/long/dataset/name", base_name="LongModelName"
    )
    changelog_identifier = table_identifier.change_table_type(new_type=TableType.CHANGELOG)

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema=table_identifier.pg_schema_name)
        tables = meta.tables
        assert {table_identifier.pg_qualified_name, changelog_identifier.pg_qualified_name}.issubset(tables.keys())
        table = tables[table_identifier.pg_qualified_name]
        conn.execute(
            table.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "someInt": 50,
                }
            )
        )

    result = cli.invoke(
        rc,
        [
            "migrate",
            f"{tmp_path}/manifest.csv",
            "-p",
        ],
    )
    assert result.output.endswith("BEGIN;\n\nCOMMIT;\n\n")

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema=table_identifier.pg_schema_name)
        tables = meta.tables

        table = tables[table_identifier.pg_qualified_name]
        result = conn.execute(table.select())
        for item in result:
            assert item["_id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7e"
            assert item["someInt"] == 50
        cleanup_table_list(meta, [table_identifier.logical_qualified_name, changelog_identifier.logical_qualified_name])


def test_migrate_long_name_rename(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b | m             | property | type    | ref     | source
     datasets/gov/migrate/example/very/long/dataset/name |   |   |      |          |        |     |
                     |   |   | LongModelName |          |         | someInt |
                     |   |   |               | someInt  | integer |         |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    rename_file = {
        "datasets/gov/migrate/example/very/long/dataset/name/LongModelName": {
            "": "datasets/gov/migrate/example/very/long/dataset/new/EvenLongerModelName",
            "someInt": "actualInt",
        },
    }
    path = tmp_path / "rename.json"
    path.write_text(json.dumps(rename_file))

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m             | property | type    | ref     | source
     datasets/gov/migrate/example/very/long/dataset/new |   |   |      |          |        |     |
                     |   |   | EvenLongerModelName |          |         | actualInt |
                     |   |   |               | actualInt | integer |         |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-r", path])
    long_table_identifier = TableIdentifier(
        schema="datasets/gov/migrate/example/very/long/dataset/name", base_name="LongModelName"
    )
    even_longer_table_identifier = TableIdentifier(
        schema="datasets/gov/migrate/example/very/long/dataset/new", base_name="EvenLongerModelName"
    )
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_schema(schema='datasets/gov/migrate/example/very/long/dataset/new')}"
        f"{rename_table(old_table_identifier=long_table_identifier, new_table_identifier=even_longer_table_identifier, comment='datasets/gov/migrate/example/very/long/dataset/new/EvenLongerModelName')}"
        f"{rename_changelog(old_table_identifier=long_table_identifier, new_table_identifier=even_longer_table_identifier, comment='datasets/gov/migrate/example/very/long/dataset/new/EvenLongerModelName/:changelog')}"
        f"{rename_redirect(old_table_identifier=long_table_identifier, new_table_identifier=even_longer_table_identifier, comment='datasets/gov/migrate/example/very/long/dataset/new/EvenLongerModelName/:redirect')}"
        f"{rename_column(table_identifier=even_longer_table_identifier, column='someInt', new_name='actualInt')}"
        f"{rename_index(table_identifier=even_longer_table_identifier, old_index_name='ix_LongModelName__txn', new_index_name='ix_EvenLongerModelName__txn')}"
        "ALTER TABLE "
        '"datasets/gov/migrate/example/very/long/dataset/new"."EvenLongerModelName" '
        'RENAME CONSTRAINT "uq_LongModelName_someInt" TO '
        '"uq_EvenLongerModelName_actualInt";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema=even_longer_table_identifier.pg_schema_name)
        tables = meta.tables

        renamed_changelog = even_longer_table_identifier.change_table_type(new_type=TableType.CHANGELOG)

        table = tables[even_longer_table_identifier.pg_qualified_name]
        assert renamed_changelog.pg_qualified_name in tables
        result = conn.execute(table.select())
        for item in result:
            assert item["_id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7e"
            assert item["someInt"] == 50
        cleanup_table_list(
            meta, [even_longer_table_identifier.logical_qualified_name, renamed_changelog.logical_qualified_name]
        )


def test_migrate_reserved_model_additional_tables(
    migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    initial_manifest = """
     d               | r | b | m    | property     | type | ref | level
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)
    store = load_store(context, verbose=False, ensure_config_dir=True)
    manifest = store.manifest

    commands.load(context, manifest)
    commands.link(context, manifest)

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    models = commands.get_models(context, manifest)
    reserved_models = [model for model in models.keys() if model.startswith("_")]
    namespaces = set(models[model].ns.name or "public" for model in reserved_models)
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        for namespace in namespaces:
            pg_name = get_pg_name(namespace)
            meta.reflect(schema=pg_name)

        meta.reflect()
        tables = meta.tables

        for model in reserved_models:
            table_identifier = get_table_identifier(model)
            assert table_identifier.pg_qualified_name in tables
            assert table_identifier.change_table_type(new_type=TableType.CHANGELOG).pg_qualified_name not in tables
            assert table_identifier.change_table_type(new_type=TableType.REDIRECT).pg_qualified_name not in tables


def test_migrate_specific_dataset_long_name(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d                                                   | r | b | m             | property | type    | ref     | source
     datasets/gov/migrate/example/very/long/dataset/name |   |   |               |          |         |         |
                                                         |   |   | LongModelName |          |         | someInt |
                                                         |   |   |               | someInt  | integer |         |
     datasets/gov/migrate/normal                         |   |   |               |          |         |         |
                                                         |   |   | ModelName     |          |         | someInt |
                                                         |   |   |               | someInt  | integer |         |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])
    table_identifier = TableIdentifier(
        schema="datasets/gov/migrate/example/very/long/dataset/name", base_name="LongModelName"
    )
    changelog_identifier = table_identifier.change_table_type(new_type=TableType.CHANGELOG)

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema=table_identifier.pg_schema_name)
        tables = meta.tables
        assert {table_identifier.pg_qualified_name, changelog_identifier.pg_qualified_name}.issubset(tables.keys())
        table = tables[table_identifier.pg_qualified_name]
        conn.execute(
            table.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "someInt": 50,
                }
            )
        )

    rename_file = {
        "datasets/gov/migrate/example/very/long/dataset/name/LongModelName": {
            "someInt": "otherInt",
        },
        "datasets/gov/migrate/normal/ModelName": {
            "": "datasets/gov/migrate/normal/LongName",
        },
    }
    path = tmp_path / "rename.json"
    path.write_text(json.dumps(rename_file))

    override_manifest(
        context,
        tmp_path,
        """
     d                                                   | r | b | m             | property | type    | ref      | source
     datasets/gov/migrate/example/very/long/dataset/name |   |   |               |          |         |          |
                                                         |   |   | LongModelName |          |         | otherInt |
                                                         |   |   |               | otherInt | integer |          |
     datasets/gov/migrate/normal                         |   |   |               |          |         |          |
                                                         |   |   | LongName      |          |         | someInt  |
                                                         |   |   |               | someInt  | integer |          |
    """,
    )

    result = cli.invoke(
        rc,
        [
            "migrate",
            f"{tmp_path}/manifest.csv",
            "-p",
            "-r",
            path,
            "-d",
            "datasets/gov/migrate/example/very/long/dataset/name",
        ],
    )
    long_table_identifier = TableIdentifier(
        schema="datasets/gov/migrate/example/very/long/dataset/name", base_name="LongModelName"
    )
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{rename_column(table_identifier=long_table_identifier, column='someInt', new_name='otherInt')}"
        "ALTER TABLE "
        '"datasets/gov/migrate/example/very/long/dataset/name"."LongModelName" RENAME '
        'CONSTRAINT "uq_LongModelName_someInt" TO "uq_LongModelName_otherInt";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(
        rc,
        [
            "migrate",
            f"{tmp_path}/manifest.csv",
            "-r",
            path,
            "-d",
            "datasets/gov/migrate/example/very/long/dataset/name",
        ],
    )
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect(schema="datasets/gov/migrate/normal")
        meta.reflect(schema="datasets/gov/migrate/example/very/long/dataset/name")
        tables = meta.tables

        renamed_identifier = get_table_identifier("datasets/gov/migrate/normal/LongName")
        renamed_changelog_identifier = renamed_identifier.change_table_type(new_type=TableType.CHANGELOG)
        assert renamed_identifier.pg_qualified_name not in tables
        assert renamed_changelog_identifier.pg_qualified_name not in tables

        table = tables[long_table_identifier.pg_qualified_name]
        assert "otherInt" in table.columns
        assert "someInt" not in table.columns
