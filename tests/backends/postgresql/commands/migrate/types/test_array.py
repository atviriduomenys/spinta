import json
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.engine.url import URL

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
)
from tests.backends.postgresql.commands.migrate.test_migrations import (
    cleanup_tables,
    override_manifest,
    cleanup_table_list,
    configure_migrate,
)


def test_migrate_create_models_with_array_type(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
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
        '    "new" JSONB, \n'
        '    CONSTRAINT "pk_migrate/example/Test" PRIMARY KEY (_id), \n'
        '    CONSTRAINT "uq_migrate/example/Test_someText_someNumber" UNIQUE '
        '("someText", "someNumber")\n'
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
        f"{add_column_comment(table='migrate/example/Test', column='new')}"
        f"{add_table_comment(table='migrate/example/Test', comment='migrate/example/Test')}"
        'CREATE TABLE "migrate/example/Test/:list/new" (\n'
        "    _txn UUID, \n"
        "    _rid UUID, \n"
        '    "new" INTEGER, \n'
        '    CONSTRAINT "fk_migrate/example/Test/:list/new__rid" FOREIGN KEY(_rid) '
        'REFERENCES "migrate/example/Test" (_id) ON DELETE CASCADE\n'
        ");\n"
        "\n"
        f"{add_index(index_name='ix_migrate/example/Test/:list/new__txn', table='migrate/example/Test/:list/new', columns=['_txn'])}"
        f"{add_column_comment(table='migrate/example/Test/:list/new', column='_txn')}"
        f"{add_column_comment(table='migrate/example/Test/:list/new', column='_rid')}"
        f"{add_column_comment(table='migrate/example/Test/:list/new', column='new')}"
        f"{add_table_comment(table='migrate/example/Test/:list/new', comment='migrate/example/Test/:list/new')}"
        f"{add_changelog_table(table='migrate/example/Test/:changelog', comment='migrate/example/Test/:changelog')}"
        f"{add_redirect_table(table='migrate/example/Test/:redirect', comment='migrate/example/Test/:redirect')}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Test/:list/new",
        }.issubset(tables.keys())
        table = tables["migrate/example/Test"]
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


def test_migrate_add_array_type(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
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

    assert result.output.endswith(
        "BEGIN;\n\n"
        'ALTER TABLE "migrate/example/Test" ADD COLUMN "new" JSONB;\n'
        "\n"
        f"{add_column_comment(table='migrate/example/Test', column='new')}"
        'CREATE TABLE "migrate/example/Test/:list/new" (\n'
        "    _txn UUID, \n"
        "    _rid UUID, \n"
        '    "new" INTEGER, \n'
        '    CONSTRAINT "fk_migrate/example/Test/:list/new__rid" FOREIGN KEY(_rid) '
        'REFERENCES "migrate/example/Test" (_id) ON DELETE CASCADE\n'
        ");\n"
        "\n"
        f"{add_index(index_name='ix_migrate/example/Test/:list/new__txn', table='migrate/example/Test/:list/new', columns=['_txn'])}"
        f"{add_column_comment(table='migrate/example/Test/:list/new', column='_txn')}"
        f"{add_column_comment(table='migrate/example/Test/:list/new', column='_rid')}"
        f"{add_column_comment(table='migrate/example/Test/:list/new', column='new')}"
        f"{add_table_comment(table='migrate/example/Test/:list/new', comment='migrate/example/Test/:list/new')}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Test/:list/new",
        }.issubset(tables.keys())
        table = tables["migrate/example/Test"]
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


def test_migrate_remove_array_type(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
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
    assert result.output.endswith(
        "BEGIN;\n\n"
        f"{drop_column(table='migrate/example/Test', column='new')}"
        f"{drop_table(table='migrate/example/Test/:list/new')}"
        f"{drop_constraint(constraint_name='fk_migrate/example/Test/:list/new__rid', table='migrate/example/Test/:list/__new')}"
        f"{drop_index(index_name='ix_migrate/example/Test/:list/new__txn')}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Test/:list/__new",
        }.issubset(tables.keys())
        table = tables["migrate/example/Test"]
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


def test_migrate_rename_array(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
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
    assert result.output.endswith(
        "BEGIN;\n\n"
        f"{rename_column(table='migrate/example/Test', column='new', new_name='renamed')}"
        f"{rename_table(table='migrate/example/Test/:list/new', new_name='migrate/example/Test/:list/renamed')}"
        f"{rename_index(old_index_name='ix_migrate/example/Test/:list/new__txn', new_index_name='ix_migrate/example/Test/:list/renamed__txn')}"
        f"{rename_constraint(table='migrate/example/Test/:list/renamed', constraint_name='fk_migrate/example/Test/:list/new__rid', new_constraint_name='fk_migrate/example/Test/:list/renamed__rid')}"
        f"{rename_column(table='migrate/example/Test/:list/renamed', column='new', new_name='renamed')}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Test/:list/renamed",
        }.issubset(tables.keys())
        table = tables["migrate/example/Test"]
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


def test_migrate_change_array_subtype(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
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
        'ALTER TABLE "migrate/example/Test/:list/new" ALTER COLUMN "new" TYPE TEXT '
        'USING CAST("migrate/example/Test/:list/new"."new" AS TEXT);\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Test/:list/new",
        }.issubset(tables.keys())
        table = tables["migrate/example/Test"]
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
