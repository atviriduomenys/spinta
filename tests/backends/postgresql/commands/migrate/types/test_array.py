import json
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_identifier
from spinta.core.config import RawConfig
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.migration import (
    add_column_comment,
    add_table_comment,
    add_index,
    add_changelog_table,
    add_redirect_table,
    drop_column,
    drop_index,
    drop_table,
    drop_constraint,
    rename_column,
    rename_table,
    rename_index,
    rename_constraint,
    add_schema,
)
from tests.backends.postgresql.commands.migrate.test_migrations import (
    override_manifest,
    cleanup_table_list,
    configure_migrate,
)


def test_migrate_create_models_with_array_type(
    migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    initial_manifest = """
     d               | r | b | m    | property     | type | ref | source
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type    | ref                  | source
     migrate/example |   |      |      |                |         |                      |
                     |   |      | Test |                |         | someText, someNumber |
                     |   |      |      | someText       | string  |                      |
                     |   |      |      | someInteger    | integer |                      |
                     |   |      |      | someNumber     | number  |                      |
                     |   |      |      | new[]          | integer |                      |
    """,
    )
    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    table_identifier = get_table_identifier("migrate/example/Test")
    list_table_identifier = table_identifier.change_table_type(new_type=TableType.LIST, table_arg="new")
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
        '    "new" JSONB, \n'
        '    CONSTRAINT "pk_Test" PRIMARY KEY (_id), \n'
        '    CONSTRAINT "uq_Test_someText_someNumber" UNIQUE '
        '("someText", "someNumber")\n'
        ");\n"
        "\n"
        f"{add_index(table_identifier=table_identifier, index_name='ix_Test__txn', columns=['_txn'])}"
        f"{add_column_comment(table_identifier=table_identifier, column='_txn')}"
        f"{add_column_comment(table_identifier=table_identifier, column='_created')}"
        f"{add_column_comment(table_identifier=table_identifier, column='_updated')}"
        f"{add_column_comment(table_identifier=table_identifier, column='_id')}"
        f"{add_column_comment(table_identifier=table_identifier, column='_revision')}"
        f"{add_column_comment(table_identifier=table_identifier, column='someText')}"
        f"{add_column_comment(table_identifier=table_identifier, column='someInteger')}"
        f"{add_column_comment(table_identifier=table_identifier, column='someNumber')}"
        f"{add_column_comment(table_identifier=table_identifier, column='new')}"
        f"{add_table_comment(table_identifier=table_identifier, comment='migrate/example/Test')}"
        'CREATE TABLE "migrate/example"."Test/:list/new" (\n'
        "    _txn UUID, \n"
        "    _rid UUID, \n"
        '    "new" INTEGER, \n'
        '    CONSTRAINT "fk_Test/:list/new__rid_Test" FOREIGN KEY(_rid) '
        'REFERENCES "migrate/example"."Test" (_id) ON DELETE CASCADE\n'
        ");\n"
        "\n"
        f"{add_index(table_identifier=list_table_identifier, index_name='ix_Test/:list/new__txn', columns=['_txn'])}"
        f"{add_column_comment(table_identifier=list_table_identifier, column='_txn')}"
        f"{add_column_comment(table_identifier=list_table_identifier, column='_rid')}"
        f"{add_column_comment(table_identifier=list_table_identifier, column='new')}"
        f"{add_table_comment(table_identifier=list_table_identifier, comment='migrate/example/Test/:list/new')}"
        f"{add_changelog_table(table_identifier=table_identifier, comment='migrate/example/Test/:changelog')}"
        f"{add_redirect_table(table_identifier=table_identifier, comment='migrate/example/Test/:redirect')}"
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
            "migrate/example.Test/:list/new",
        }.issubset(tables.keys())
        table = tables["migrate/example.Test"]
        columns = table.columns
        assert {
            "someText",
            "someInteger",
            "someNumber",
            "new",
        }.issubset(columns.keys())

        cleanup_table_list(
            meta,
            [
                "migrate/example/Test",
                "migrate/example/Test/:changelog",
                "migrate/example/Test/:list/new",
            ],
        )


def test_migrate_add_array_type(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b    | m    | property       | type    | ref                  | source
     migrate/example |   |      |      |                |         |                      |
                     |   |      | Test |                |         | someText, someNumber |
                     |   |      |      | someText       | string  |                      |
                     |   |      |      | someInteger    | integer |                      |
                     |   |      |      | someNumber     | number  |                      |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type    | ref                  | source
     migrate/example |   |      |      |                |         |                      |
                     |   |      | Test |                |         | someText, someNumber |
                     |   |      |      | someText       | string  |                      |
                     |   |      |      | someInteger    | integer |                      |
                     |   |      |      | someNumber     | number  |                      |
                     |   |      |      | new[]          | integer |                      |
    """,
    )
    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    table_identifier = get_table_identifier("migrate/example/Test")
    list_table_identifier = table_identifier.change_table_type(new_type=TableType.LIST, table_arg="new")
    assert result.output.endswith(
        "BEGIN;\n\n"
        'ALTER TABLE "migrate/example"."Test" ADD COLUMN "new" JSONB;\n'
        "\n"
        f"{add_column_comment(table_identifier=table_identifier, column='new')}"
        'CREATE TABLE "migrate/example"."Test/:list/new" (\n'
        "    _txn UUID, \n"
        "    _rid UUID, \n"
        '    "new" INTEGER, \n'
        '    CONSTRAINT "fk_Test/:list/new__rid_Test" FOREIGN KEY(_rid) '
        'REFERENCES "migrate/example"."Test" (_id) ON DELETE CASCADE\n'
        ");\n"
        "\n"
        f"{add_index(table_identifier=list_table_identifier, index_name='ix_Test/:list/new__txn', columns=['_txn'])}"
        f"{add_column_comment(table_identifier=list_table_identifier, column='_txn')}"
        f"{add_column_comment(table_identifier=list_table_identifier, column='_rid')}"
        f"{add_column_comment(table_identifier=list_table_identifier, column='new')}"
        f"{add_table_comment(table_identifier=list_table_identifier, comment='migrate/example/Test/:list/new')}"
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
            "migrate/example.Test/:list/new",
        }.issubset(tables.keys())
        table = tables["migrate/example.Test"]
        columns = table.columns
        assert {
            "someText",
            "someInteger",
            "someNumber",
            "new",
        }.issubset(columns.keys())

        cleanup_table_list(
            meta,
            [
                "migrate/example/Test",
                "migrate/example/Test/:changelog",
                "migrate/example/Test/:list/new",
            ],
        )


def test_migrate_remove_array_type(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b    | m    | property       | type    | ref                  | source
     migrate/example |   |      |      |                |         |                      |
                     |   |      | Test |                |         | someText, someNumber |
                     |   |      |      | someText       | string  |                      |
                     |   |      |      | someInteger    | integer |                      |
                     |   |      |      | someNumber     | number  |                      |
                     |   |      |      | new[]          | integer |                      |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type    | ref                  | source
     migrate/example |   |      |      |                |         |                      |
                     |   |      | Test |                |         | someText, someNumber |
                     |   |      |      | someText       | string  |                      |
                     |   |      |      | someInteger    | integer |                      |
                     |   |      |      | someNumber     | number  |                      |
    """,
    )
    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    table_identifier = get_table_identifier("migrate/example/Test")
    list_table_identifier = table_identifier.change_table_type(new_type=TableType.LIST, table_arg="new")
    removed_list_table_identifier = list_table_identifier.apply_removed_prefix()
    assert result.output.endswith(
        "BEGIN;\n\n"
        f"{drop_column(table_identifier=table_identifier, column='new')}"
        f"{drop_table(table_identifier=list_table_identifier)}"
        f"{drop_constraint(table_identifier=removed_list_table_identifier, constraint_name='fk_Test/:list/new__rid_Test')}"
        f"{drop_index(table_identifier=removed_list_table_identifier, index_name='ix_Test/:list/new__txn')}"
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
            "migrate/example.Test/:list/__new",
        }.issubset(tables.keys())
        table = tables["migrate/example.Test"]
        columns = table.columns
        assert {
            "someText",
            "someInteger",
            "someNumber",
            "__new",
        }.issubset(columns.keys())

        cleanup_table_list(
            meta,
            [
                "migrate/example/Test",
                "migrate/example/Test/:changelog",
                "migrate/example/Test/:list/__new",
            ],
        )


def test_migrate_rename_array(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b    | m    | property       | type    | ref                  | source
     migrate/example |   |      |      |                |         |                      |
                     |   |      | Test |                |         | someText, someNumber |
                     |   |      |      | someText       | string  |                      |
                     |   |      |      | someInteger    | integer |                      |
                     |   |      |      | someNumber     | number  |                      |
                     |   |      |      | new[]          | integer |                      |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type    | ref                  | source
     migrate/example |   |      |      |                |         |                      |
                     |   |      | Test |                |         | someText, someNumber |
                     |   |      |      | someText       | string  |                      |
                     |   |      |      | someInteger    | integer |                      |
                     |   |      |      | someNumber     | number  |                      |
                     |   |      |      | renamed[]      | integer |                      |
    """,
    )

    rename_file = {
        "migrate/example/Test": {"new": "renamed"},
    }
    path = tmp_path / "rename.json"
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-r", path])
    table_identifier = get_table_identifier("migrate/example/Test")
    old_list_table_identifier = table_identifier.change_table_type(new_type=TableType.LIST, table_arg="new")
    new_list_table_identifier = table_identifier.change_table_type(new_type=TableType.LIST, table_arg="renamed")
    assert result.output.endswith(
        "BEGIN;\n\n"
        f"{rename_column(table_identifier=table_identifier, column='new', new_name='renamed')}"
        f"{rename_table(old_table_identifier=old_list_table_identifier, new_table_identifier=new_list_table_identifier)}"
        f"{rename_index(table_identifier=new_list_table_identifier, old_index_name='ix_Test/:list/new__txn', new_index_name='ix_Test/:list/renamed__txn')}"
        f"{rename_constraint(table_identifier=new_list_table_identifier, constraint_name='fk_Test/:list/new__rid_Test', new_constraint_name='fk_Test/:list/renamed__rid_Test')}"
        f"{rename_column(table_identifier=new_list_table_identifier, column='new', new_name='renamed')}"
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
            "migrate/example.Test/:list/renamed",
        }.issubset(tables.keys())
        table = tables["migrate/example.Test"]
        columns = table.columns
        assert {
            "someText",
            "someInteger",
            "someNumber",
            "renamed",
        }.issubset(columns.keys())

        cleanup_table_list(
            meta,
            [
                "migrate/example/Test",
                "migrate/example/Test/:changelog",
                "migrate/example/Test/:list/renamed",
            ],
        )


def test_migrate_change_array_subtype(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b    | m    | property       | type    | ref                  | source
     migrate/example |   |      |      |                |         |                      |
                     |   |      | Test |                |         | someText, someNumber |
                     |   |      |      | someText       | string  |                      |
                     |   |      |      | someInteger    | integer |                      |
                     |   |      |      | someNumber     | number  |                      |
                     |   |      |      | new[]          | integer |                      |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type    | ref                  | source
     migrate/example |   |      |      |                |         |                      |
                     |   |      | Test |                |         | someText, someNumber |
                     |   |      |      | someText       | string  |                      |
                     |   |      |      | someInteger    | integer |                      |
                     |   |      |      | someNumber     | number  |                      |
                     |   |      |      | new[]          | string  |                      |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n\n"
        'ALTER TABLE "migrate/example"."Test/:list/new" ALTER COLUMN "new" TYPE TEXT '
        'USING CAST("migrate/example"."Test/:list/new"."new" AS TEXT);\n'
        "\n"
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
            "migrate/example.Test/:list/new",
        }.issubset(tables.keys())
        table = tables["migrate/example.Test"]
        columns = table.columns
        assert {
            "someText",
            "someInteger",
            "someNumber",
            "new",
        }.issubset(columns.keys())

        cleanup_table_list(
            meta,
            [
                "migrate/example/Test",
                "migrate/example/Test/:changelog",
                "migrate/example/Test/:list/new",
            ],
        )
