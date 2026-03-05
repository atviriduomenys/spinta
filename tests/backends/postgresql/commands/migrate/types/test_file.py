from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.engine.url import URL

from spinta.core.config import RawConfig
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.migration import (
    drop_column,
    add_column_comment,
    add_table_comment,
    add_index,
    add_changelog_table,
    add_redirect_table,
    drop_table,
)
from tests.backends.postgresql.commands.migrate.test_migrations import (
    cleanup_tables,
    override_manifest,
    cleanup_table_list,
    configure_migrate,
)


def test_migrate_create_models_with_file_type(
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
                     |   |      |      | flag           | file    |                      |
                     |   |      |      | new            | file    |                      | 
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
        '    "flag._id" VARCHAR, \n'
        '    "flag._content_type" VARCHAR, \n'
        '    "flag._size" BIGINT, \n'
        '    "flag._bsize" INTEGER, \n'
        '    "flag._blocks" UUID[], \n'
        '    "new._id" VARCHAR, \n'
        '    "new._content_type" VARCHAR, \n'
        '    "new._size" BIGINT, \n'
        '    "new._bsize" INTEGER, \n'
        '    "new._blocks" UUID[], \n'
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
        f"{add_column_comment(table='migrate/example/Test', column='flag._id')}"
        f"{add_column_comment(table='migrate/example/Test', column='flag._content_type')}"
        f"{add_column_comment(table='migrate/example/Test', column='flag._size')}"
        f"{add_column_comment(table='migrate/example/Test', column='flag._bsize')}"
        f"{add_column_comment(table='migrate/example/Test', column='flag._blocks')}"
        f"{add_column_comment(table='migrate/example/Test', column='new._id')}"
        f"{add_column_comment(table='migrate/example/Test', column='new._content_type')}"
        f"{add_column_comment(table='migrate/example/Test', column='new._size')}"
        f"{add_column_comment(table='migrate/example/Test', column='new._bsize')}"
        f"{add_column_comment(table='migrate/example/Test', column='new._blocks')}"
        f"{add_table_comment(table='migrate/example/Test', comment='migrate/example/Test')}"
        'CREATE TABLE "migrate/example/Test/:file/flag" (\n'
        "    _id UUID NOT NULL, \n"
        "    _block BYTEA, \n"
        '    CONSTRAINT "pk_migrate/example/Test/:file/flag" PRIMARY KEY (_id)\n'
        ");\n\n"
        f"{add_column_comment(table='migrate/example/Test/:file/flag', column='_id')}"
        f"{add_column_comment(table='migrate/example/Test/:file/flag', column='_block')}"
        f"{add_table_comment(table='migrate/example/Test/:file/flag', comment='migrate/example/Test/:file/flag')}"
        'CREATE TABLE "migrate/example/Test/:file/new" (\n'
        "    _id UUID NOT NULL, \n"
        "    _block BYTEA, \n"
        '    CONSTRAINT "pk_migrate/example/Test/:file/new" PRIMARY KEY (_id)\n'
        ");\n\n"
        f"{add_column_comment(table='migrate/example/Test/:file/new', column='_id')}"
        f"{add_column_comment(table='migrate/example/Test/:file/new', column='_block')}"
        f"{add_table_comment(table='migrate/example/Test/:file/new', comment='migrate/example/Test/:file/new')}"
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
            "migrate/example/Test/:file/flag",
            "migrate/example/Test/:file/new",
        }.issubset(tables.keys())
        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {
            "someText",
            "someInteger",
            "someNumber",
            "flag._id",
            "flag._content_type",
            "flag._size",
            "flag._bsize",
            "flag._blocks",
            "new._id",
            "new._content_type",
            "new._size",
            "new._bsize",
            "new._blocks",
        }.issubset(columns.keys())

        cleanup_table_list(
            meta,
            [
                "migrate/example/Test",
                "migrate/example/Test/:changelog",
                "migrate/example/Test/:file/flag",
                "migrate/example/Test/:file/new",
            ],
        )


def test_migrate_remove_file_type(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m    | property       | type    | ref                  | source
     migrate/example |   |      |      |                |         |                      |
                     |   |      | Test |                |         | someText, someNumber |
                     |   |      |      | someText       | string  |                      |
                     |   |      |      | someInteger    | integer |                      |
                     |   |      |      | someNumber     | number  |                      |
                     |   |      |      | flag           | file    |                      |
                     |   |      |      | new            | file    |                      | 
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
                     |   |      |      | flag           | file    |                      |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{drop_column(table='migrate/example/Test', column='new._id')}"
        f"{drop_column(table='migrate/example/Test', column='new._content_type')}"
        f"{drop_column(table='migrate/example/Test', column='new._size')}"
        f"{drop_column(table='migrate/example/Test', column='new._bsize')}"
        f"{drop_column(table='migrate/example/Test', column='new._blocks')}"
        f"{drop_table(table='migrate/example/Test/:file/new')}"
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
            "migrate/example/Test/:file/flag",
            "migrate/example/Test/:file/__new",
        }.issubset(tables.keys())
        assert not {"migrate/example/Test/:file/new"}.issubset(tables.keys())
        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {
            "someText",
            "someInteger",
            "someNumber",
            "flag._id",
            "flag._content_type",
            "flag._size",
            "flag._bsize",
            "flag._blocks",
            "__new._id",
            "__new._content_type",
            "__new._size",
            "__new._bsize",
            "__new._blocks",
        }.issubset(columns.keys())
        assert not {"new._id", "new._content_type", "new._size", "new._bsize", "new._blocks"}.issubset(columns.keys())

        cleanup_table_list(
            meta,
            [
                "migrate/example/Test",
                "migrate/example/Test/:changelog",
                "migrate/example/Test/:file/flag",
                "migrate/example/Test/:file/__new",
            ],
        )
