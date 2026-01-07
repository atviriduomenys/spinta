from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_identifier
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
    add_schema,
)
from tests.backends.postgresql.commands.migrate.test_migrations import (
    override_manifest,
    cleanup_table_list,
    configure_migrate,
)


def test_migrate_create_models_with_file_type(
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
                     |   |      |      | flag           | file    |                      |
                     |   |      |      | new            | file    |                      | 
    """,
    )
    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    table_identifier = get_table_identifier("migrate/example/Test")
    flag_table_identifier = table_identifier.change_table_type(new_type=TableType.FILE, table_arg="flag")
    new_table_identifier = table_identifier.change_table_type(new_type=TableType.FILE, table_arg="new")
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
        f"{add_column_comment(table_identifier=table_identifier, column='flag._id')}"
        f"{add_column_comment(table_identifier=table_identifier, column='flag._content_type')}"
        f"{add_column_comment(table_identifier=table_identifier, column='flag._size')}"
        f"{add_column_comment(table_identifier=table_identifier, column='flag._bsize')}"
        f"{add_column_comment(table_identifier=table_identifier, column='flag._blocks')}"
        f"{add_column_comment(table_identifier=table_identifier, column='new._id')}"
        f"{add_column_comment(table_identifier=table_identifier, column='new._content_type')}"
        f"{add_column_comment(table_identifier=table_identifier, column='new._size')}"
        f"{add_column_comment(table_identifier=table_identifier, column='new._bsize')}"
        f"{add_column_comment(table_identifier=table_identifier, column='new._blocks')}"
        f"{add_table_comment(table_identifier=table_identifier, comment='migrate/example/Test')}"
        'CREATE TABLE "migrate/example"."Test/:file/flag" (\n'
        "    _id UUID NOT NULL, \n"
        "    _block BYTEA, \n"
        '    CONSTRAINT "pk_Test/:file/flag" PRIMARY KEY (_id)\n'
        ");\n\n"
        f"{add_column_comment(table_identifier=flag_table_identifier, column='_id')}"
        f"{add_column_comment(table_identifier=flag_table_identifier, column='_block')}"
        f"{add_table_comment(table_identifier=flag_table_identifier, comment='migrate/example/Test/:file/flag')}"
        'CREATE TABLE "migrate/example"."Test/:file/new" (\n'
        "    _id UUID NOT NULL, \n"
        "    _block BYTEA, \n"
        '    CONSTRAINT "pk_Test/:file/new" PRIMARY KEY (_id)\n'
        ");\n\n"
        f"{add_column_comment(table_identifier=new_table_identifier, column='_id')}"
        f"{add_column_comment(table_identifier=new_table_identifier, column='_block')}"
        f"{add_table_comment(table_identifier=new_table_identifier, comment='migrate/example/Test/:file/new')}"
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
            "migrate/example.Test/:file/flag",
            "migrate/example.Test/:file/new",
        }.issubset(tables.keys())
        table = tables["migrate/example.Test"]
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


def test_migrate_remove_file_type(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
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
    table_identifier = get_table_identifier("migrate/example/Test")
    file_table_identifier = table_identifier.change_table_type(new_type=TableType.FILE, table_arg="new")
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{drop_column(table_identifier=table_identifier, column='new._id')}"
        f"{drop_column(table_identifier=table_identifier, column='new._content_type')}"
        f"{drop_column(table_identifier=table_identifier, column='new._size')}"
        f"{drop_column(table_identifier=table_identifier, column='new._bsize')}"
        f"{drop_column(table_identifier=table_identifier, column='new._blocks')}"
        f"{drop_table(table_identifier=file_table_identifier)}"
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
            "migrate/example.Test/:file/flag",
            "migrate/example.Test/:file/__new",
        }.issubset(tables.keys())
        assert not {"migrate/example.Test/:file/new"}.issubset(tables.keys())
        table = tables["migrate/example.Test"]
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
