import itertools
import json

import pytest
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.engine.url import make_url
from sqlalchemy.engine import Engine

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.postgresql.helpers import get_pg_name
from spinta.backends.postgresql.helpers.name import get_pg_table_name
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
        table_list.append(meta.tables[get_pg_name(table)])
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

    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'CREATE TABLE "migrate/example/Test" (\n'
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
        '    CONSTRAINT "pk_migrate/example/Test" PRIMARY KEY (_id), \n'
        '    CONSTRAINT "uq_migrate/example/Test_someUri" UNIQUE ("someUri")\n'
        ");\n"
        "\n"
        f"{add_index(index_name='ix_migrate/example/Test__txn', table='migrate/example/Test', columns=['_txn'])}"
        f"{add_column_comment(table='migrate/example/Test', column='_txn')}"
        f"{add_column_comment(table='migrate/example/Test', column='_created')}"
        f"{add_column_comment(table='migrate/example/Test', column='_updated')}"
        f"{add_column_comment(table='migrate/example/Test', column='_id')}"
        f"{add_column_comment(table='migrate/example/Test', column='_revision')}"
        f"{add_column_comment(table='migrate/example/Test', column='someText')}"
        f"{add_column_comment(table='migrate/example/Test', column='someInteger')}"
        f"{add_column_comment(table='migrate/example/Test', column='someNumber')}"
        f"{add_column_comment(table='migrate/example/Test', column='someDate')}"
        f"{add_column_comment(table='migrate/example/Test', column='someDateTime')}"
        f"{add_column_comment(table='migrate/example/Test', column='someTime')}"
        f"{add_column_comment(table='migrate/example/Test', column='someBoolean')}"
        f"{add_column_comment(table='migrate/example/Test', column='someUrl')}"
        f"{add_column_comment(table='migrate/example/Test', column='someUri')}"
        f"{add_column_comment(table='migrate/example/Test', column='someBinary')}"
        f"{add_column_comment(table='migrate/example/Test', column='someJson')}"
        f"{add_table_comment(table='migrate/example/Test', comment='migrate/example/Test')}"
        f"{add_changelog_table(table='migrate/example/Test/:changelog', comment='migrate/example/Test/:changelog')}"
        f"{add_redirect_table(table='migrate/example/Test/:redirect', comment='migrate/example/Test/:redirect')}"
        "COMMIT;\n"
        "\n"
    )
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert not {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        columns = tables["migrate/example/Test"].columns
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
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        columns = tables["migrate/example/Test"].columns
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

    assert result.output.endswith(
        f"BEGIN;\n\n{add_column(table='migrate/example/Test', column='someInteger', column_type='INTEGER')}COMMIT;\n\n"
    )

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        columns = tables["migrate/example/Test"].columns
        assert {"someText"}.issubset(columns.keys())

        assert not {"someInteger"}.issubset(columns.keys())

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        columns = tables["migrate/example/Test"].columns
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
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        columns = tables["migrate/example/Test"].columns
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

    assert result.output.endswith(
        f"BEGIN;\n\n{drop_column(table='migrate/example/Test', column='someInteger')}COMMIT;\n\n"
    )

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        columns = tables["migrate/example/Test"].columns
        assert {"someText", "someInteger"}.issubset(columns.keys())

        some_integer = columns["someInteger"]
        assert isinstance(some_integer.type, sa.Integer)
        assert some_integer.nullable

        assert not {"__someInteger"}.issubset(columns.keys())

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        columns = tables["migrate/example/Test"].columns
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
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        columns = tables["migrate/example/Test"].columns
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

    assert result.output.endswith(
        f"BEGIN;\n\n{drop_column(table='migrate/example/Test', column='someInteger')}COMMIT;\n\n"
    )

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        columns = tables["migrate/example/Test"].columns
        assert {"someText", "someInteger"}.issubset(columns.keys())

        assert not {"__someInteger"}.issubset(columns.keys())

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        columns = tables["migrate/example/Test"].columns
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
        f"BEGIN;\n\n{add_column(table='migrate/example/Test', column='someInteger', column_type='INTEGER')}COMMIT;\n\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        columns = tables["migrate/example/Test"].columns
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
        'ALTER TABLE "migrate/example/Test" DROP COLUMN "__someInteger";\n\n'
        f"{drop_column(table='migrate/example/Test', column='someInteger')}"
        "COMMIT;\n"
        "\n"
    )
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        columns = tables["migrate/example/Test"].columns
        assert {"someText", "someInteger", "__someInteger"}.issubset(columns.keys())

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])

    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example/Test" DROP COLUMN "__someInteger";\n\n'
        f"{drop_column(table='migrate/example/Test', column='someInteger')}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        columns = tables["migrate/example/Test"].columns
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
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        columns = tables["migrate/example/Test"].columns
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
        'ALTER TABLE "migrate/example/Test" ADD CONSTRAINT '
        '"uq_migrate/example/Test_someText" UNIQUE ("someText");\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        constraint_columns = get_table_unique_constraint_columns(tables["migrate/example/Test"])
        assert not any(columns == ["someText"] for columns in constraint_columns)

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        constraint_columns = get_table_unique_constraint_columns(tables["migrate/example/Test"])
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
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        columns = tables["migrate/example/Test"].columns
        assert {"someText"}.issubset(columns.keys())

        some_text = columns["someText"]
        assert isinstance(some_text.type, sa.String)
        assert some_text.nullable

        constraint_columns = get_table_unique_constraint_columns(tables["migrate/example/Test"])
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
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example/Test" DROP CONSTRAINT '
        '"uq_migrate/example/Test_someText";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        constraint_columns = get_table_unique_constraint_columns(tables["migrate/example/Test"])
        assert any(columns == ["someText"] for columns in constraint_columns)

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        constraint_columns = get_table_unique_constraint_columns(tables["migrate/example/Test"])
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

    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'CREATE TABLE "migrate/example/Base" (\n'
        "    _txn UUID, \n"
        "    _created TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _updated TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _id UUID NOT NULL, \n"
        "    _revision TEXT, \n"
        '    "someText" TEXT, \n'
        '    "someInteger" INTEGER, \n'
        '    "someNumber" FLOAT, \n'
        '    CONSTRAINT "pk_migrate/example/Base" PRIMARY KEY (_id), \n'
        '    CONSTRAINT "uq_migrate/example/Base_someText_someNumber" UNIQUE '
        '("someText", "someNumber")\n'
        ");\n"
        "\n"
        f"{add_index(index_name='ix_migrate/example/Base__txn', table='migrate/example/Base', columns=['_txn'])}"
        f"{add_column_comment(table='migrate/example/Base', column='_txn')}"
        f"{add_column_comment(table='migrate/example/Base', column='_created')}"
        f"{add_column_comment(table='migrate/example/Base', column='_updated')}"
        f"{add_column_comment(table='migrate/example/Base', column='_id')}"
        f"{add_column_comment(table='migrate/example/Base', column='_revision')}"
        f"{add_column_comment(table='migrate/example/Base', column='someText')}"
        f"{add_column_comment(table='migrate/example/Base', column='someInteger')}"
        f"{add_column_comment(table='migrate/example/Base', column='someNumber')}"
        f"{add_table_comment(table='migrate/example/Base', comment='migrate/example/Base')}"
        f"{add_changelog_table(table='migrate/example/Base/:changelog', comment='migrate/example/Base/:changelog')}"
        f"{add_redirect_table(table='migrate/example/Base/:redirect', comment='migrate/example/Base/:redirect')}"
        'CREATE TABLE "migrate/example/Test" (\n'
        "    _txn UUID, \n"
        "    _created TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _updated TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _id UUID NOT NULL, \n"
        "    _revision TEXT, \n"
        '    CONSTRAINT "pk_migrate/example/Test" PRIMARY KEY (_id), \n'
        '    CONSTRAINT "fk_migrate/example/Base__id" FOREIGN KEY(_id) REFERENCES '
        '"migrate/example/Base" (_id)\n'
        ");\n"
        "\n"
        f"{add_index(index_name='ix_migrate/example/Test__txn', table='migrate/example/Test', columns=['_txn'])}"
        f"{add_column_comment(table='migrate/example/Test', column='_txn')}"
        f"{add_column_comment(table='migrate/example/Test', column='_created')}"
        f"{add_column_comment(table='migrate/example/Test', column='_updated')}"
        f"{add_column_comment(table='migrate/example/Test', column='_id')}"
        f"{add_column_comment(table='migrate/example/Test', column='_revision')}"
        f"{add_table_comment(table='migrate/example/Test', comment='migrate/example/Test')}"
        f"{add_changelog_table(table='migrate/example/Test/:changelog', comment='migrate/example/Test/:changelog')}"
        f"{add_redirect_table(table='migrate/example/Test/:redirect', comment='migrate/example/Test/:redirect')}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Base",
            "migrate/example/Base/:changelog",
        }.issubset(tables.keys())

        columns = get_table_foreign_key_constraint_columns(tables["migrate/example/Test"])
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
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Ref",
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Test/:file/someFile",
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
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{drop_table(table='migrate/example/Test', remove_model_only=True)}"
        f"{drop_index(index_name='ix_migrate/example/Test__txn')}"
        f"{drop_table(table='migrate/example/Test/:changelog', remove_model_only=True)}"
        'ALTER SEQUENCE "migrate/example/Test/:changelog__id_seq" RENAME TO '
        '"migrate/example/__Test/:changelog__id_seq";\n'
        "\n"
        f"{drop_index(index_name='ix_migrate/example/Test/:changelog__txn')}"
        f"{drop_table(table='migrate/example/Test/:redirect', remove_model_only=True)}"
        f"{drop_index(index_name='ix_migrate/example/Test/:redirect_redirect')}"
        f"{drop_table(table='migrate/example/Test/:file/someFile', remove_model_only=True)}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Ref",
            "migrate/example/Ref/:changelog",
            "migrate/example/Ref/:redirect",
            "migrate/example/__Test",
            "migrate/example/__Test/:changelog",
            "migrate/example/__Test/:redirect",
            "migrate/example/__Test/:file/someFile",
        }.issubset(tables.keys())

        assert not {
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Test/:redirect",
            "migrate/example/Test/:file/someFile",
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
        'BEGIN;\n\nALTER TABLE "migrate/example/Test" DROP CONSTRAINT "fk_migrate/example/Base__id";\n\nCOMMIT;\n\n'
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Base",
            "migrate/example/Base/:changelog",
        }.issubset(tables.keys())

        columns = get_table_foreign_key_constraint_columns(tables["migrate/example/Test"])
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
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Ref",
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Test/:file/someFile",
        }.issubset(tables.keys())
        table = tables["migrate/example/Test"]

        constraints = get_table_foreign_key_constraint_columns(table)
        assert any(constraint["constraint_name"] == "fk_migrate/example/Test_someRef._id" for constraint in constraints)

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
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{rename_table(table='migrate/example/Ref', new_name='migrate/example/NewRef')}"
        f"{rename_changelog(table='migrate/example/Ref/:changelog', new_name='migrate/example/NewRef/:changelog')}"
        f"{rename_redirect(table='migrate/example/Ref/:redirect', new_name='migrate/example/NewRef/:redirect')}"
        f"{rename_index(old_index_name='ix_migrate/example/Ref__txn', new_index_name='ix_migrate/example/NewRef__txn')}"
        f"{rename_table(table='migrate/example/Test', new_name='migrate/example/New')}"
        f"{rename_changelog(table='migrate/example/Test/:changelog', new_name='migrate/example/New/:changelog')}"
        f"{rename_redirect(table='migrate/example/Test/:redirect', new_name='migrate/example/New/:redirect')}"
        f"{rename_table(table='migrate/example/Test/:file/someFile', new_name='migrate/example/New/:file/someFile')}"
        f"{rename_index(old_index_name='ix_migrate/example/Test_someRef._id', new_index_name='ix_migrate/example/New_someRef._id')}"
        f"{rename_index(old_index_name='ix_migrate/example/Test__txn', new_index_name='ix_migrate/example/New__txn')}"
        'ALTER TABLE "migrate/example/New" RENAME CONSTRAINT '
        '"fk_migrate/example/Test_someRef._id" TO '
        '"fk_migrate/example/New_someRef._id";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-r", path])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/New",
            "migrate/example/New/:changelog",
            "migrate/example/New/:redirect",
            "migrate/example/New/:file/someFile",
            "migrate/example/NewRef",
        }.issubset(tables.keys())

        assert not {
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Test/:redirectmigrate/example/Test/:file/someFile",
            "migrate/example/Ref",
        }.issubset(tables.keys())

        table = tables["migrate/example/New"]

        constraints = get_table_foreign_key_constraint_columns(table)
        assert any(constraint["constraint_name"] == "fk_migrate/example/New_someRef._id" for constraint in constraints)
        assert not any(
            constraint["constraint_name"] == "fk_migrate/example/Test_someRef._id" for constraint in constraints
        )

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
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Ref",
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Test/:file/someFile",
        }.issubset(tables.keys())
        table = tables["migrate/example/Test"]
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
        assert any(
            constraint["constraint_name"] == "fk_migrate/example/Test_someOther._id" for constraint in constraints
        )

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

    assert result.output.endswith(
        "BEGIN;\n\n"
        f"{rename_column(table='migrate/example/Ref', column='someText', new_name='newText')}"
        'ALTER TABLE "migrate/example/Ref" RENAME CONSTRAINT '
        '"uq_migrate/example/Ref_someText" TO "uq_migrate/example/Ref_newText";\n'
        "\n"
        f"{rename_column(table='migrate/example/Test', column='someText', new_name='newText')}"
        f"{rename_column(table='migrate/example/Test', column='someFile._id', new_name='newFile._id')}"
        f"{rename_column(table='migrate/example/Test', column='someFile._content_type', new_name='newFile._content_type')}"
        f"{rename_column(table='migrate/example/Test', column='someFile._size', new_name='newFile._size')}"
        f"{rename_column(table='migrate/example/Test', column='someFile._bsize', new_name='newFile._bsize')}"
        f"{rename_column(table='migrate/example/Test', column='someFile._blocks', new_name='newFile._blocks')}"
        f"{rename_table(table='migrate/example/Test/:file/someFile', new_name='migrate/example/Test/:file/newFile')}"
        f"{rename_index(old_index_name='ix_migrate/example/Test_someOther._id', new_index_name='ix_migrate/example/Test_newOther._id')}"
        f"{rename_column(table='migrate/example/Test', column='someRef.someText', new_name='newRef.newText')}"
        f"{rename_column(table='migrate/example/Test', column='someOther._id', new_name='newOther._id')}"
        'ALTER TABLE "migrate/example/Test" RENAME CONSTRAINT '
        '"fk_migrate/example/Test_someOther._id" TO '
        '"fk_migrate/example/Test_newOther._id";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-r", path])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Test/:file/newFile",
            "migrate/example/Ref",
        }.issubset(tables.keys())

        table = tables["migrate/example/Ref"]
        columns = table.columns
        assert {"newText"}.issubset(columns.keys())
        assert not {"someText"}.issubset(columns.keys())

        table = tables["migrate/example/Test"]
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
        assert any(
            constraint["constraint_name"] == "fk_migrate/example/Test_newOther._id" for constraint in constraints
        )
        assert not any(
            constraint["constraint_name"] == "fk_migrate/example/Test_someOther._id" for constraint in constraints
        )

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

    pieces = [
        (
            "CREATE INDEX "
            '"ix_migrate/example/very/very/long/dat_d5eeba2c_ropertyNameOther" ON '
            '"migrate/example/very/very/long/datase_0f562213_elyLongModelName" USING gist '
            '("veryLongGeometryPropertyNameOther");\n'
            "\n"
        ),
        (
            "CREATE INDEX "
            '"ix_migrate/example/very/very/long/dat_4b7a633e_etryPropertyName" ON '
            '"migrate/example/very/very/long/datase_0f562213_elyLongModelName" USING gist '
            '("veryLongGeometryPropertyName");\n'
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
        "BEGIN;\n"
        "\n"
        "CREATE TABLE "
        '"migrate/example/very/very/long/datase_0f562213_elyLongModelName" (\n'
        "    _txn UUID, \n"
        "    _created TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _updated TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _id UUID NOT NULL, \n"
        "    _revision TEXT, \n"
        '    "veryLongPrimaryKeyName" TEXT, \n'
        '    "veryLongGeometryPropertyName" geometry(GEOMETRY,4326), \n'
        '    "veryLongGeometryPropertyNameOther" geometry(GEOMETRY,4326), \n'
        "    CONSTRAINT "
        '"pk_migrate/example/very/very/long/dat_f2de534c_elyLongModelName" PRIMARY '
        "KEY (_id), \n"
        "    CONSTRAINT "
        '"uq_migrate/example/very/very/long/dat_9d7c795e_ngPrimaryKeyName" UNIQUE '
        '("veryLongPrimaryKeyName")\n'
        ");\n"
        "\n"
        f"{add_index(index_name='ix_migrate/example/very/very/long/dat_31c24f29_ngModelName__txn', table='migrate/example/very/very/long/datase_0f562213_elyLongModelName', columns=['_txn'])}"
        f"{add_column_comment(table='migrate/example/very/very/long/datase_0f562213_elyLongModelName', column='_txn')}"
        f"{add_column_comment(table='migrate/example/very/very/long/datase_0f562213_elyLongModelName', column='_created')}"
        f"{add_column_comment(table='migrate/example/very/very/long/datase_0f562213_elyLongModelName', column='_updated')}"
        f"{add_column_comment(table='migrate/example/very/very/long/datase_0f562213_elyLongModelName', column='_id')}"
        f"{add_column_comment(table='migrate/example/very/very/long/datase_0f562213_elyLongModelName', column='_revision')}"
        f"{add_column_comment(table='migrate/example/very/very/long/datase_0f562213_elyLongModelName', column='veryLongPrimaryKeyName')}"
        f"{add_column_comment(table='migrate/example/very/very/long/datase_0f562213_elyLongModelName', column='veryLongGeometryPropertyName')}"
        f"{add_column_comment(table='migrate/example/very/very/long/datase_0f562213_elyLongModelName', column='veryLongGeometryPropertyNameOther')}"
        f"{add_table_comment(table='migrate/example/very/very/long/datase_0f562213_elyLongModelName', comment='migrate/example/very/very/long/dataset/name/ExtremelyLongModelName')}"
        f"{ordered}"
        f"{add_changelog_table(table='migrate/example/very/very/long/datase_d087b1e4_lName/:changelog', comment='migrate/example/very/very/long/dataset/name/ExtremelyLongModelName/:changelog')}"
        f"{add_redirect_table(table='migrate/example/very/very/long/datase_7adc3c9c_elName/:redirect', comment='migrate/example/very/very/long/dataset/name/ExtremelyLongModelName/:redirect')}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        table_name = get_pg_name("migrate/example/very/very/long/dataset/name/ExtremelyLongModelName")
        assert table_name in tables

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
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example/Test"]
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

    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{drop_column(table='migrate/example/Test', column='otherText')}"
        f"{rename_column(table='migrate/example/Test', column='someText', new_name='otherText')}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-r", path])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        table = tables["migrate/example/Test"]
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
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example/Test"]
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
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someInt" TYPE INTEGER USING '
        'CAST("migrate/example/Test"."someInt" AS INTEGER);\n'
        "\n"
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someFloat" TYPE FLOAT USING '
        'CAST("migrate/example/Test"."someFloat" AS FLOAT);\n'
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
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        table = tables["migrate/example/Test"]
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
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someInt" TYPE TEXT USING '
        'CAST("migrate/example/Test"."someInt" AS TEXT);\n'
        "\n"
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someFloat" TYPE TEXT USING '
        'CAST("migrate/example/Test"."someFloat" AS TEXT);\n'
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
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        table = tables["migrate/example/Test"]
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
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_column(table='migrate/example/Test', column='newColumn', column_type='INTEGER')}"
        'ALTER TABLE "migrate/example/Test" DROP CONSTRAINT '
        '"uq_migrate/example/Test_someText";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-d", "migrate/example"])
    assert result.exit_code == 0
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert "migrate/example/Test" in tables

        table = tables["migrate/example/Test"]
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
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_column(table='migrate/example/Test', column='newColumn', column_type='INTEGER')}"
        'ALTER TABLE "migrate/example/Test" DROP CONSTRAINT '
        '"uq_migrate/example/Test_someText";\n'
        "\n"
        f"{add_column(table='migrate/example/Test2', column='newColumn2', column_type='INTEGER')}"
        'ALTER TABLE "migrate/example/Test2" DROP CONSTRAINT '
        '"uq_migrate/example/Test2_anotherText";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-d", "migrate/example"])
    assert result.exit_code == 0
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert "migrate/example/Test" in tables
        assert "migrate/example/Test2" in tables

        table1 = tables["migrate/example/Test"]
        columns1 = table1.columns
        assert "newColumn" in columns1
        assert "someText" in columns1

        table2 = tables["migrate/example/Test2"]
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
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_column(table='dataset1/Test', column='newColumn', column_type='INTEGER')}"
        'ALTER TABLE "dataset1/Test" DROP CONSTRAINT '
        '"uq_dataset1/Test_someText";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-d", "dataset1"])
    assert result.exit_code == 0
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert "dataset1/Test" in tables
        assert "dataset2/Test" in tables

        table1 = tables["dataset1/Test"]
        columns1 = table1.columns
        assert "newColumn" in columns1
        assert "someText" in columns1

        table2 = tables["dataset2/Test"]
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
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_column(table='dataset1/Test', column='newColumn', column_type='INTEGER')}"
        'ALTER TABLE "dataset1/Test" DROP CONSTRAINT "uq_dataset1/Test_someText";\n'
        "\n"
        f"{add_column(table='dataset2/Test2', column='newColumn2', column_type='INTEGER')}"
        'ALTER TABLE "dataset2/Test2" DROP CONSTRAINT '
        '"uq_dataset2/Test2_anotherText";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-d", "dataset1", "-d", "dataset2"])
    assert result.exit_code == 0
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert "dataset1/Test" in tables
        assert "dataset2/Test2" in tables
        assert "dataset3/Test3" in tables

        table1 = tables["dataset1/Test"]
        columns1 = table1.columns
        assert "newColumn" in columns1
        assert "someText" in columns1

        table2 = tables["dataset2/Test2"]
        columns2 = table2.columns
        assert "newColumn2" in columns2
        assert "anotherText" in columns2

        table3 = tables["dataset3/Test3"]
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
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        constraint_columns = get_table_unique_constraint_columns(tables["migrate/example/Test"])
        assert any(columns == ["someText"] for columns in constraint_columns)
        # Corrupt unique constraint name

        conn.execute(
            'ALTER TABLE "migrate/example/Test" RENAME CONSTRAINT "uq_migrate/example/Test_someText" TO "corrupted_unique_constraint"'
        )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example/Test" RENAME CONSTRAINT '
        'corrupted_unique_constraint TO "uq_migrate/example/Test_someText";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        constraint_columns = get_table_unique_constraint_columns(tables["migrate/example/Test"])
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
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Ref",
            "migrate/example/Ref/:changelog",
        }.issubset(tables.keys())

        # Corrupt ref index name
        conn.execute('ALTER INDEX "ix_migrate/example/Test_someRef._id" RENAME TO "corrupted_index_name"')

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        f"BEGIN;\n\n{rename_index(old_index_name='corrupted_index_name', new_index_name='ix_migrate/example/Test_someRef._id')}COMMIT;\n\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()

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
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Ref",
            "migrate/example/Ref/:changelog",
        }.issubset(tables.keys())

        table = tables["migrate/example/Test"]

        constraints = get_table_foreign_key_constraint_columns(table)
        assert any(constraint["constraint_name"] == "fk_migrate/example/Test_someRef._id" for constraint in constraints)

        # Corrupt ref index name
        conn.execute(
            'ALTER TABLE "migrate/example/Test" RENAME CONSTRAINT "fk_migrate/example/Test_someRef._id" TO "corrupted_fkey_constraint"'
        )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example/Test" RENAME CONSTRAINT '
        'corrupted_fkey_constraint TO "fk_migrate/example/Test_someRef._id";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()

        table = tables["migrate/example/Test"]

        constraints = get_table_foreign_key_constraint_columns(table)
        assert any(constraint["constraint_name"] == "fk_migrate/example/Test_someRef._id" for constraint in constraints)

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
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example/Test"]
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
        meta.reflect()
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
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example/Test"]
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
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someDate" TYPE DATE USING '
        'CAST("migrate/example/Test"."someDate" AS DATE);\n'
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
        meta.reflect()
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
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example/Test"]
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
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someInt" TYPE INTEGER USING '
        'CAST("migrate/example/Test"."someInt" AS INTEGER);\n'
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
        meta.reflect()
        tables = meta.tables

        table = tables["migrate/example/Test"]

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
    table_name = get_pg_table_name("datasets/gov/migrate/example/very/long/dataset/name/LongModelName")
    changelog_name = get_pg_table_name(
        "datasets/gov/migrate/example/very/long/dataset/name/LongModelName", TableType.CHANGELOG
    )

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {table_name, changelog_name}.issubset(tables.keys())
        table = tables[table_name]
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
        meta.reflect()
        tables = meta.tables

        table = tables[table_name]

        result = conn.execute(table.select())
        for item in result:
            assert item["_id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7e"
            assert item["someInt"] == 50
        cleanup_table_list(meta, [table_name, changelog_name])


def test_migrate_long_name_rename(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b | m             | property | type    | ref     | source
     datasets/gov/migrate/example/very/long/dataset/name |   |   |      |          |        |     |
                     |   |   | LongModelName |          |         | someInt |
                     |   |   |               | someInt  | integer |         |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])
    table_name = get_pg_table_name("datasets/gov/migrate/example/very/long/dataset/name/LongModelName")
    changelog_name = get_pg_table_name(
        "datasets/gov/migrate/example/very/long/dataset/name/LongModelName", TableType.CHANGELOG
    )

    # with migration_db.connect() as conn:
    #     meta = sa.MetaData(conn)
    #     meta.reflect()
    #     tables = meta.tables
    #     assert {table_name, changelog_name}.issubset(tables.keys())
    #     table = tables[table_name]
    #     conn.execute(
    #         table.insert().values(
    #             {
    #                 "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
    #                 "someInt": 50,
    #             }
    #         )
    #     )

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

    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{rename_table(table='datasets/gov/migrate/example/very/lon_2895f72f_me/LongModelName', new_name='datasets/gov/migrate/example/very/lon_e74a0ea2_nLongerModelName', comment='datasets/gov/migrate/example/very/long/dataset/name/EvenLongerModelName')}"
        f"{rename_changelog(table='datasets/gov/migrate/example/very/lon_c3b41b6d_lName/:changelog', new_name='datasets/gov/migrate/example/very/lon_5bf0f407_lName/:changelog', comment='datasets/gov/migrate/example/very/long/dataset/name/EvenLongerModelName/:changelog')}"
        f"{rename_redirect(table='datasets/gov/migrate/example/very/lon_a8ca9fad_elName/:redirect', new_name='datasets/gov/migrate/example/very/lon_6f0bc85c_elName/:redirect', comment='datasets/gov/migrate/example/very/long/dataset/name/EvenLongerModelName/:redirect')}"
        f"{rename_column(table='datasets/gov/migrate/example/very/lon_e74a0ea2_nLongerModelName', column='someInt', new_name='actualInt')}"
        f"{rename_index(old_index_name='ix_datasets/gov/migrate/example/very/_71ac6294_ngModelName__txn', new_index_name='ix_datasets/gov/migrate/example/very/_7d25af54_erModelName__txn')}"
        "ALTER TABLE "
        '"datasets/gov/migrate/example/very/lon_e74a0ea2_nLongerModelName" RENAME '
        'CONSTRAINT "uq_datasets/gov/migrate/example/very/_97f03b18_odelName_someInt" '
        'TO "uq_datasets/gov/migrate/example/very/_d7fdbe46_elName_actualInt";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables

        renamed = get_pg_table_name("datasets/gov/migrate/example/very/long/dataset/name/EvenLongerModelName")
        renamed_changelog = get_pg_table_name(
            "datasets/gov/migrate/example/very/long/dataset/name/EvenLongerModelName", TableType.CHANGELOG
        )

        table = tables[renamed]
        assert renamed_changelog in tables
        result = conn.execute(table.select())
        for item in result:
            assert item["_id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7e"
            assert item["someInt"] == 50
        cleanup_table_list(meta, [renamed, renamed_changelog])


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
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables

        for model in reserved_models:
            assert model in tables
            assert get_pg_table_name(model, TableType.CHANGELOG) not in tables
            assert get_pg_table_name(model, TableType.REDIRECT) not in tables


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
    table_name = get_pg_table_name("datasets/gov/migrate/example/very/long/dataset/name/LongModelName")
    changelog_name = get_pg_table_name(
        "datasets/gov/migrate/example/very/long/dataset/name/LongModelName", TableType.CHANGELOG
    )

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {table_name, changelog_name}.issubset(tables.keys())
        table = tables[table_name]
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
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{rename_column(table='datasets/gov/migrate/example/very/lon_2895f72f_me/LongModelName', column='someInt', new_name='otherInt')}"
        "ALTER TABLE "
        '"datasets/gov/migrate/example/very/lon_2895f72f_me/LongModelName" RENAME '
        'CONSTRAINT "uq_datasets/gov/migrate/example/very/_97f03b18_odelName_someInt" '
        'TO "uq_datasets/gov/migrate/example/very/_f8eac1ec_delName_otherInt";\n'
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
        meta.reflect()
        tables = meta.tables

        renamed = get_pg_table_name("datasets/gov/migrate/normal/LongName")
        renamed_changelog = get_pg_table_name("datasets/gov/migrate/normal/LongName", TableType.CHANGELOG)
        assert renamed not in tables
        assert renamed_changelog not in tables

        table_name = get_pg_table_name("datasets/gov/migrate/example/very/long/dataset/name/LongModelName")
        table = tables[table_name]
        assert "otherInt" in table.columns
        assert "someInt" not in table.columns
