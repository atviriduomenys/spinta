from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.engine.url import URL

from spinta.core.config import RawConfig
from spinta.testing.cli import SpintaCliRunner
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
        'CREATE TABLE "migrate/example/Test/:file/flag" (\n'
        "    _id UUID NOT NULL, \n"
        "    _block BYTEA, \n"
        '    CONSTRAINT "pk_migrate/example/Test/:file/flag" PRIMARY KEY (_id)\n'
        ");\n"
        "\n"
        'CREATE TABLE "migrate/example/Test/:file/new" (\n'
        "    _id UUID NOT NULL, \n"
        "    _block BYTEA, \n"
        '    CONSTRAINT "pk_migrate/example/Test/:file/new" PRIMARY KEY (_id)\n'
        ");\n"
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
        'CREATE INDEX "ix_migrate/example/Test__txn" ON "migrate/example/Test" '
        "(_txn);\n"
        "\n"
        'CREATE TABLE "migrate/example/Test/:changelog" (\n'
        "    _id BIGSERIAL NOT NULL, \n"
        "    _revision VARCHAR, \n"
        "    _txn UUID, \n"
        "    _rid UUID, \n"
        "    datetime TIMESTAMP WITHOUT TIME ZONE, \n"
        "    action VARCHAR(8), \n"
        "    data JSONB, \n"
        '    CONSTRAINT "pk_migrate/example/Test/:changelog" PRIMARY KEY (_id)\n'
        ");\n"
        "\n"
        'CREATE INDEX "ix_migrate/example/Test/:changelog__txn" ON '
        '"migrate/example/Test/:changelog" (_txn);\n'
        "\n"
        'CREATE TABLE "migrate/example/Test/:redirect" (\n'
        "    _id UUID NOT NULL, \n"
        "    redirect UUID, \n"
        '    CONSTRAINT "pk_migrate/example/Test/:redirect" PRIMARY KEY (_id)\n'
        ");\n"
        "\n"
        'CREATE INDEX "ix_migrate/example/Test/:redirect_redirect" ON '
        '"migrate/example/Test/:redirect" (redirect);\n'
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
        'ALTER TABLE "migrate/example/Test" RENAME "new._id" TO "__new._id";\n'
        "\n"
        'ALTER TABLE "migrate/example/Test" RENAME "new._content_type" TO '
        '"__new._content_type";\n'
        "\n"
        'ALTER TABLE "migrate/example/Test" RENAME "new._size" TO "__new._size";\n'
        "\n"
        'ALTER TABLE "migrate/example/Test" RENAME "new._bsize" TO "__new._bsize";\n'
        "\n"
        'ALTER TABLE "migrate/example/Test" RENAME "new._blocks" TO "__new._blocks";\n'
        "\n"
        'ALTER TABLE "migrate/example/Test/:file/new" RENAME TO '
        '"migrate/example/Test/:file/__new";\n'
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
