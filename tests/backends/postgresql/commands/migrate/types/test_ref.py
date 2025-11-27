import json
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.engine.url import URL

from spinta.core.config import RawConfig
from spinta.exceptions import (
    MigrateScalarToRefTooManyKeys,
    MigrateScalarToRefTypeMissmatch,
    UnableToFindPrimaryKeysNoUniqueConstraints,
    UnableToFindPrimaryKeysMultipleUniqueConstraints,
)
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.migration import (
    add_index,
    add_column_comment,
    add_table_comment,
    add_changelog_table,
    add_redirect_table,
    drop_column,
    add_column,
    rename_column,
)
from tests.backends.postgresql.commands.migrate.test_migrations import (
    cleanup_tables,
    override_manifest,
    cleanup_table_list,
    configure_migrate,
    get_table_unique_constraint_columns,
    get_table_foreign_key_constraint_columns,
)


def test_migrate_create_models_with_ref(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b | m    | property     | type | ref | level
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m      | property     | type          | ref                  | level
     migrate/example |   |   |        |              |               |                      |
                     |   |   | Test   |              |               | someText, someNumber |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someInteger  | integer       |                      |
                     |   |   |        | someNumber   | number        |                      |
                     |   |   |        |              |               |                      |
                     |   |   | RefOne |              |               |                      |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someRef      | ref           | Test                 | 4
                     |   |   |        |              |               |                      |
                     |   |   | RefTwo |              |               |                      |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someRef      | ref           | Test                 | 3
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    # Adjust index order, since it can be random
    order = (
        'CREATE INDEX "ix_migrate/example/RefOne_someRef._id" ON '
        '"migrate/example/RefOne" ("someRef._id");\n'
        "\n"
        'CREATE INDEX "ix_migrate/example/RefOne__txn" ON "migrate/example/RefOne" '
        "(_txn);\n"
        "\n"
    )
    if order not in result.output:
        order = (
            f"{add_index(index_name='ix_migrate/example/RefOne__txn', table='migrate/example/RefOne', columns=['_txn'])}"
            f"{add_index(index_name='ix_migrate/example/RefOne_someRef._id', table='migrate/example/RefOne', columns=['someRef._id'])}"
        )
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
        f"{add_table_comment(table='migrate/example/Test', comment='migrate/example/Test')}"
        f"{add_changelog_table(table='migrate/example/Test/:changelog')}"
        f"{add_redirect_table(table='migrate/example/Test/:redirect')}"
        'CREATE TABLE "migrate/example/RefOne" (\n'
        "    _txn UUID, \n"
        "    _created TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _updated TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _id UUID NOT NULL, \n"
        "    _revision TEXT, \n"
        '    "someText" TEXT, \n'
        '    "someRef._id" UUID, \n'
        '    CONSTRAINT "pk_migrate/example/RefOne" PRIMARY KEY (_id), \n'
        '    CONSTRAINT "fk_migrate/example/RefOne_someRef._id" FOREIGN '
        'KEY("someRef._id") REFERENCES "migrate/example/Test" (_id)\n'
        ");\n"
        "\n"
        f"{order}"
        f"{add_column_comment(table='migrate/example/RefOne', column='_txn')}"
        f"{add_column_comment(table='migrate/example/RefOne', column='_created')}"
        f"{add_column_comment(table='migrate/example/RefOne', column='_updated')}"
        f"{add_column_comment(table='migrate/example/RefOne', column='_id')}"
        f"{add_column_comment(table='migrate/example/RefOne', column='_revision')}"
        f"{add_column_comment(table='migrate/example/RefOne', column='someText')}"
        f"{add_column_comment(table='migrate/example/RefOne', column='someRef._id')}"
        f"{add_table_comment(table='migrate/example/RefOne', comment='migrate/example/RefOne')}"
        f"{add_changelog_table(table='migrate/example/RefOne/:changelog')}"
        f"{add_redirect_table(table='migrate/example/RefOne/:redirect')}"
        'CREATE TABLE "migrate/example/RefTwo" (\n'
        "    _txn UUID, \n"
        "    _created TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _updated TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _id UUID NOT NULL, \n"
        "    _revision TEXT, \n"
        '    "someText" TEXT, \n'
        '    "someRef.someText" TEXT, \n'
        '    "someRef.someNumber" FLOAT, \n'
        '    CONSTRAINT "pk_migrate/example/RefTwo" PRIMARY KEY (_id)\n'
        ");\n"
        "\n"
        f"{add_index(index_name='ix_migrate/example/RefTwo__txn', table='migrate/example/RefTwo', columns=['_txn'])}"
        f"{add_column_comment(table='migrate/example/RefTwo', column='_txn')}"
        f"{add_column_comment(table='migrate/example/RefTwo', column='_created')}"
        f"{add_column_comment(table='migrate/example/RefTwo', column='_updated')}"
        f"{add_column_comment(table='migrate/example/RefTwo', column='_id')}"
        f"{add_column_comment(table='migrate/example/RefTwo', column='_revision')}"
        f"{add_column_comment(table='migrate/example/RefTwo', column='someText')}"
        f"{add_column_comment(table='migrate/example/RefTwo', column='someRef.someText')}"
        f"{add_column_comment(table='migrate/example/RefTwo', column='someRef.someNumber')}"
        f"{add_table_comment(table='migrate/example/RefTwo', comment='migrate/example/RefTwo')}"
        f"{add_changelog_table(table='migrate/example/RefTwo/:changelog')}"
        f"{add_redirect_table(table='migrate/example/RefTwo/:redirect')}"
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
            "migrate/example/RefOne",
            "migrate/example/RefOne/:changelog",
            "migrate/example/RefTwo",
            "migrate/example/RefTwo/:changelog",
        }.issubset(tables.keys())
        columns = tables["migrate/example/Test"].columns
        assert {"someText", "someNumber", "someInteger"}.issubset(columns.keys())

        columns = tables["migrate/example/RefOne"].columns
        assert {"someText", "someRef._id"}.issubset(columns.keys())
        assert not {"someRef.someText", "someRef.someNumber"}.issubset(columns.keys())

        columns = get_table_foreign_key_constraint_columns(tables["migrate/example/RefOne"])
        assert any(
            [["someRef._id"], ["_id"]] == [constraint["column_names"], constraint["referred_column_names"]]
            for constraint in columns
        )

        columns = tables["migrate/example/RefTwo"].columns
        assert {"someText", "someRef.someText", "someRef.someNumber"}.issubset(columns.keys())
        assert not {"someRef._id"}.issubset(columns.keys())

        columns = get_table_foreign_key_constraint_columns(tables["migrate/example/RefTwo"])
        assert not any(
            [["someRef._id"], ["_id"]] == [constraint["column_names"], constraint["referred_column_names"]]
            for constraint in columns
        )

        cleanup_table_list(
            meta,
            [
                "migrate/example/Test",
                "migrate/example/Test/:changelog",
                "migrate/example/RefOne",
                "migrate/example/RefOne/:changelog",
                "migrate/example/RefTwo",
                "migrate/example/RefTwo/:changelog",
            ],
        )


def test_migrate_remove_ref_column(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b | m      | property     | type          | ref                  | level
     migrate/example |   |   |        |              |               |                      |
                     |   |   | Test   |              |               | someText, someNumber |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someInteger  | integer       |                      |
                     |   |   |        | someNumber   | number        |                      |
                     |   |   |        |              |               |                      |
                     |   |   | RefOne |              |               |                      |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someRef      | ref           | Test                 | 4
                     |   |   |        |              |               |                      |
                     |   |   | RefTwo |              |               |                      |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someRef      | ref           | Test                 | 3
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/RefOne",
            "migrate/example/RefOne/:changelog",
            "migrate/example/RefTwo",
            "migrate/example/RefTwo/:changelog",
        }.issubset(tables.keys())
        columns = tables["migrate/example/Test"].columns
        assert {"someText", "someNumber", "someInteger"}.issubset(columns.keys())

        columns = tables["migrate/example/RefOne"].columns
        assert {"someText", "someRef._id"}.issubset(columns.keys())

        columns = get_table_foreign_key_constraint_columns(tables["migrate/example/RefOne"])
        assert any(
            [["someRef._id"], ["_id"]] == [constraint["column_names"], constraint["referred_column_names"]]
            for constraint in columns
        )

        columns = tables["migrate/example/RefTwo"].columns
        assert {"someText", "someRef.someText", "someRef.someNumber"}.issubset(columns.keys())

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m      | property     | type          | ref                  | level
     migrate/example |   |   |        |              |               |                      |
                     |   |   | Test   |              |               | someText, someNumber |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someInteger  | integer       |                      |
                     |   |   |        | someNumber   | number        |                      |
                     |   |   |        |              |               |                      |
                     |   |   | RefOne |              |               |                      |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        |              |               |                      |
                     |   |   | RefTwo |              |               |                      |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someRef      | ref           | Test                 | 3
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{drop_column(table='migrate/example/RefOne', column='someRef._id')}"
        'DROP INDEX "ix_migrate/example/RefOne_someRef._id";\n'
        "\n"
        'ALTER TABLE "migrate/example/RefOne" DROP CONSTRAINT '
        '"fk_migrate/example/RefOne_someRef._id";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables

        columns = tables["migrate/example/RefOne"].columns
        assert {"someText", "__someRef._id"}.issubset(columns.keys())
        assert not {"someRef._id"}.issubset(columns.keys())

        columns = get_table_foreign_key_constraint_columns(tables["migrate/example/RefOne"])
        assert not any(
            [["someRef._id"], ["_id"]] == [constraint["column_names"], constraint["referred_column_names"]]
            for constraint in columns
        )
        assert not any(
            [["__someRef._id"], ["_id"]] == [constraint["column_names"], constraint["referred_column_names"]]
            for constraint in columns
        )

    override_manifest(
        context,
        tmp_path,
        """
         d               | r | b | m      | property     | type          | ref                  | level
         migrate/example |   |   |        |              |               |                      |
                         |   |   | Test   |              |               | someText, someNumber |
                         |   |   |        | someText     | string        |                      |
                         |   |   |        | someInteger  | integer       |                      |
                         |   |   |        | someNumber   | number        |                      |
                         |   |   |        |              |               |                      |
                         |   |   | RefOne |              |               |                      |
                         |   |   |        | someText     | string        |                      |
                         |   |   |        |              |               |                      |
                         |   |   | RefTwo |              |               |                      |
                         |   |   |        | someText     | string        |                      |
        """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{drop_column(table='migrate/example/RefTwo', column='someRef.someText')}"
        f"{drop_column(table='migrate/example/RefTwo', column='someRef.someNumber')}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables

        columns = tables["migrate/example/RefTwo"].columns
        assert {"someText", "__someRef.someText", "__someRef.someNumber"}.issubset(columns.keys())
        assert not {"someRef.someText", "someRef.someNumber"}.issubset(columns.keys())

        cleanup_table_list(
            meta,
            [
                "migrate/example/Test",
                "migrate/example/Test/:changelog",
                "migrate/example/RefOne",
                "migrate/example/RefOne/:changelog",
                "migrate/example/RefTwo",
                "migrate/example/RefTwo/:changelog",
            ],
        )


def test_migrate_adjust_ref_levels(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b | m      | property     | type          | ref                  | level
     migrate/example |   |   |        |              |               |                      |
                     |   |   | Test   |              |               | someText, someNumber |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someInteger  | integer       |                      |
                     |   |   |        | someNumber   | number        |                      |
                     |   |   |        |              |               |                      |
                     |   |   | Ref    |              |               |                      |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someRef      | ref           | Test                 | 4
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])
    insert_values = [
        {"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "someText": "test1", "someInteger": 0, "someNumber": 1.0},
        {"_id": "b574111d-bd2f-4c94-9249-d9a0de49bd5b", "someText": "test2", "someInteger": 1, "someNumber": 2.0},
        {"_id": "1686c00c-0c59-413a-aa30-f5605488cc77", "someText": "test3", "someInteger": 0, "someNumber": 1.0},
    ]
    ref_insert = [
        {"_id": "350e7a87-28a5-4645-a659-2daa8e4bbe55", "someRef._id": "197109d9-add8-49a5-ab19-3ddc7589ce7e"},
        {"_id": "72d5dc33-a074-43f0-882f-e06abd34113b", "someRef._id": "b574111d-bd2f-4c94-9249-d9a0de49bd5b"},
        {"_id": "478be0be-6ab9-4c03-8551-53d881567743", "someRef._id": "1686c00c-0c59-413a-aa30-f5605488cc77"},
    ]

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        table = tables["migrate/example/Test"]
        for item in insert_values:
            conn.execute(table.insert().values(item))

        result = conn.execute(table.select())
        for i, item in enumerate(result):
            item = dict(item)
            assert item["_id"] == insert_values[i]["_id"]
            assert item["someText"] == insert_values[i]["someText"]
            assert item["someInteger"] == insert_values[i]["someInteger"]
            assert item["someNumber"] == insert_values[i]["someNumber"]

        assert {
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Ref",
            "migrate/example/Ref/:changelog",
        }.issubset(tables.keys())
        columns = table.columns
        assert {"someText", "someNumber", "someInteger"}.issubset(columns.keys())

        table = tables["migrate/example/Ref"]
        for item in ref_insert:
            conn.execute(table.insert().values(item))

        result = conn.execute(table.select())
        for i, item in enumerate(result):
            item = dict(item)
            assert item["_id"] == ref_insert[i]["_id"]
            assert item["someRef._id"] == insert_values[i]["_id"]
        columns = table.columns
        assert {"someText", "someRef._id"}.issubset(columns.keys())

        columns = get_table_foreign_key_constraint_columns(table)
        assert any(
            [["someRef._id"], ["_id"]] == [constraint["column_names"], constraint["referred_column_names"]]
            for constraint in columns
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m      | property     | type          | ref                  | level
     migrate/example |   |   |        |              |               |                      |
                     |   |   | Test   |              |               | someText, someNumber |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someInteger  | integer       |                      |
                     |   |   |        | someNumber   | number        |                      |
                     |   |   |        |              |               |                      |
                     |   |   | Ref    |              |               |                      |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someRef      | ref           | Test                 | 3
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_column(table='migrate/example/Ref', column='someRef.someText', column_type='TEXT')}"
        f"{add_column(table='migrate/example/Ref', column='someRef.someNumber', column_type='FLOAT')}"
        'UPDATE "migrate/example/Ref" SET '
        '"someRef.someText"="migrate/example/Test"."someText", '
        '"someRef.someNumber"="migrate/example/Test"."someNumber" FROM '
        '"migrate/example/Test" WHERE "migrate/example/Ref"."someRef._id" = '
        '"migrate/example/Test"._id;\n'
        "\n"
        f"{drop_column(table='migrate/example/Ref', column='someRef._id')}"
        'DROP INDEX "ix_migrate/example/Ref_someRef._id";\n'
        "\n"
        'ALTER TABLE "migrate/example/Ref" DROP CONSTRAINT '
        '"fk_migrate/example/Ref_someRef._id";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        table = tables["migrate/example/Ref"]
        columns = table.columns
        assert {"someText", "__someRef._id", "someRef.someText", "someRef.someNumber"}.issubset(columns.keys())
        assert not {"someRef._id"}.issubset(columns.keys())

        columns = get_table_foreign_key_constraint_columns(table)
        assert not any(
            [["someRef._id"], ["_id"]] == [constraint["column_names"], constraint["referred_column_names"]]
            for constraint in columns
        )
        assert not any(
            [["__someRef._id"], ["_id"]] == [constraint["column_names"], constraint["referred_column_names"]]
            for constraint in columns
        )

        result = conn.execute(table.select())
        for i, item in enumerate(result):
            item = dict(item)
            assert item["_id"] == ref_insert[i]["_id"]
            assert item["someRef.someText"] == insert_values[i]["someText"]
            assert item["someRef.someNumber"] == insert_values[i]["someNumber"]

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m      | property     | type          | ref                  | level
     migrate/example |   |   |        |              |               |                      |
                     |   |   | Test   |              |               | someText, someNumber |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someInteger  | integer       |                      |
                     |   |   |        | someNumber   | number        |                      |
                     |   |   |        |              |               |                      |
                     |   |   | Ref    |              |               |                      |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someRef      | ref           | Test                 | 4
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])

    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example/Ref" ADD COLUMN "someRef._id" UUID;\n'
        "\n"
        f"{add_index(index_name='ix_migrate/example/Ref_someRef._id', table='migrate/example/Ref', columns=['someRef._id'])}"
        f"{add_column_comment(table='migrate/example/Ref', column='someRef._id')}"
        'UPDATE "migrate/example/Ref" SET "someRef._id"="migrate/example/Test"._id '
        'FROM "migrate/example/Test" WHERE "migrate/example/Ref"."someRef.someText" = '
        '"migrate/example/Test"."someText" AND '
        '"migrate/example/Ref"."someRef.someNumber" = '
        '"migrate/example/Test"."someNumber";\n'
        "\n"
        f"{drop_column(table='migrate/example/Ref', column='someRef.someText')}"
        f"{drop_column(table='migrate/example/Ref', column='someRef.someNumber')}"
        'ALTER TABLE "migrate/example/Ref" ADD CONSTRAINT '
        '"fk_migrate/example/Ref_someRef._id" FOREIGN KEY("someRef._id") REFERENCES '
        '"migrate/example/Test" (_id);\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables

        table = tables["migrate/example/Ref"]
        columns = table.columns
        assert {"someText", "__someRef._id", "someRef._id", "__someRef.someText", "__someRef.someNumber"}.issubset(
            columns.keys()
        )
        assert not {"someRef.someText", "someRef.someNumber"}.issubset(columns.keys())

        columns = get_table_foreign_key_constraint_columns(table)
        assert any(
            [["someRef._id"], ["_id"]] == [constraint["column_names"], constraint["referred_column_names"]]
            for constraint in columns
        )

        result = conn.execute(table.select())
        for i, item in enumerate(result):
            item = dict(item)
            assert item["_id"] == ref_insert[i]["_id"]
            assert item["someRef._id"] == insert_values[i]["_id"]

        cleanup_table_list(
            meta,
            [
                "migrate/example/Test",
                "migrate/example/Test/:changelog",
                "migrate/example/Ref",
                "migrate/example/Ref/:changelog",
            ],
        )


def test_migrate_model_ref_unique_constraint(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b | m    | property     | type   | ref
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m     | property     | type          | ref
     migrate/example |   |   |       |              |               |
                     |   |   | Test  |              |               | someText
                     |   |   |       | someText     | string        |
                     |   |   |       |              |               |
                     |   |   | Multi |              |               | someText, someNumber
                     |   |   |       | someText     | string        |
                     |   |   |       | someInteger  | integer       |
                     |   |   |       | someNumber   | number unique |
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
        '    CONSTRAINT "pk_migrate/example/Test" PRIMARY KEY (_id), \n'
        '    CONSTRAINT "uq_migrate/example/Test_someText" UNIQUE ("someText")\n'
        ");\n"
        "\n"
        f"{add_index(index_name='ix_migrate/example/Test__txn', table='migrate/example/Test', columns=['_txn'])}"
        f"{add_column_comment(table='migrate/example/Test', column='_txn')}"
        f"{add_column_comment(table='migrate/example/Test', column='_created')}"
        f"{add_column_comment(table='migrate/example/Test', column='_updated')}"
        f"{add_column_comment(table='migrate/example/Test', column='_id')}"
        f"{add_column_comment(table='migrate/example/Test', column='_revision')}"
        f"{add_column_comment(table='migrate/example/Test', column='someText')}"
        f"{add_table_comment(table='migrate/example/Test', comment='migrate/example/Test')}"
        f"{add_changelog_table(table='migrate/example/Test/:changelog')}"
        f"{add_redirect_table(table='migrate/example/Test/:redirect')}"
        'CREATE TABLE "migrate/example/Multi" (\n'
        "    _txn UUID, \n"
        "    _created TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _updated TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _id UUID NOT NULL, \n"
        "    _revision TEXT, \n"
        '    "someText" TEXT, \n'
        '    "someInteger" INTEGER, \n'
        '    "someNumber" FLOAT, \n'
        '    CONSTRAINT "pk_migrate/example/Multi" PRIMARY KEY (_id), \n'
        '    CONSTRAINT "uq_migrate/example/Multi_someNumber" UNIQUE ("someNumber"), \n'
        '    CONSTRAINT "uq_migrate/example/Multi_someText_someNumber" UNIQUE '
        '("someText", "someNumber")\n'
        ");\n\n"
        f"{add_index(index_name='ix_migrate/example/Multi__txn', table='migrate/example/Multi', columns=['_txn'])}"
        f"{add_column_comment(table='migrate/example/Multi', column='_txn')}"
        f"{add_column_comment(table='migrate/example/Multi', column='_created')}"
        f"{add_column_comment(table='migrate/example/Multi', column='_updated')}"
        f"{add_column_comment(table='migrate/example/Multi', column='_id')}"
        f"{add_column_comment(table='migrate/example/Multi', column='_revision')}"
        f"{add_column_comment(table='migrate/example/Multi', column='someText')}"
        f"{add_column_comment(table='migrate/example/Multi', column='someInteger')}"
        f"{add_column_comment(table='migrate/example/Multi', column='someNumber')}"
        f"{add_table_comment(table='migrate/example/Multi', comment='migrate/example/Multi')}"
        f"{add_changelog_table(table='migrate/example/Multi/:changelog')}"
        f"{add_redirect_table(table='migrate/example/Multi/:redirect')}"
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
            "migrate/example/Multi",
            "migrate/example/Multi/:changelog",
        }.issubset(tables.keys())

        table_test = tables["migrate/example/Test"]
        columns_test = table_test.columns
        assert {"someText"}.issubset(columns_test.keys())
        constraint_columns = get_table_unique_constraint_columns(table_test)
        assert any(columns == ["someText"] for columns in constraint_columns)

        table_multi = tables["migrate/example/Multi"]
        columns_multi = table_multi.columns
        assert {"someText", "someInteger", "someNumber"}.issubset(columns_multi.keys())
        constraint_columns = get_table_unique_constraint_columns(table_multi)
        assert any(columns == ["someNumber"] for columns in constraint_columns)
        assert any(sorted(columns) == sorted(["someNumber", "someText"]) for columns in constraint_columns)

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m     | property     | type          | ref
     migrate/example |   |   |       |              |               |
                     |   |   | Test  |              |               |
                     |   |   |       | someText     | string        |
                     |   |   |       |              |               |
                     |   |   | Multi |              |               | someText, someNumber, someInteger
                     |   |   |       | someText     | string        |
                     |   |   |       | someInteger  | integer       |
                     |   |   |       | someNumber   | number unique |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example/Test" DROP CONSTRAINT '
        '"uq_migrate/example/Test_someText";\n'
        "\n"
        'ALTER TABLE "migrate/example/Multi" ADD CONSTRAINT '
        '"uq_migrate/example/Multi_someText_someNumber_someInteger" UNIQUE '
        '("someText", "someNumber", "someInteger");\n'
        "\n"
        'ALTER TABLE "migrate/example/Multi" DROP CONSTRAINT '
        '"uq_migrate/example/Multi_someText_someNumber";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Multi",
            "migrate/example/Multi/:changelog",
        }.issubset(tables.keys())

        table_test = tables["migrate/example/Test"]
        constraint_columns = get_table_unique_constraint_columns(table_test)
        assert any(columns == ["someText"] for columns in constraint_columns)

        table_multi = tables["migrate/example/Multi"]
        constraint_columns = get_table_unique_constraint_columns(table_multi)
        assert any(columns == ["someNumber"] for columns in constraint_columns)
        assert any(sorted(columns) == sorted(["someNumber", "someText"]) for columns in constraint_columns)

        assert not any(
            sorted(columns) == sorted(["someNumber", "someText", "someInteger"]) for columns in constraint_columns
        )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Multi",
            "migrate/example/Multi/:changelog",
        }.issubset(tables.keys())

        table_test = tables["migrate/example/Test"]
        constraint_columns = get_table_unique_constraint_columns(table_test)
        assert not any(columns == ["someText"] for columns in constraint_columns)

        table_multi = tables["migrate/example/Multi"]
        constraint_columns = get_table_unique_constraint_columns(table_multi)
        assert any(columns == ["someNumber"] for columns in constraint_columns)
        assert any(
            sorted(columns) == sorted(["someNumber", "someText", "someInteger"]) for columns in constraint_columns
        )

        assert not any(sorted(columns) == sorted(["someNumber", "someText"]) for columns in constraint_columns)

        cleanup_table_list(
            meta,
            [
                "migrate/example/Test",
                "migrate/example/Test/:changelog",
                "migrate/example/Multi",
                "migrate/example/Multi/:changelog",
            ],
        )


def test_migrate_scalar_to_ref_simple_level_4(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | integer  |          |
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())
        city = tables["migrate/example/City"]
        country = tables["migrate/example/Country"]
        assert {"id", "country"}.issubset(city.columns.keys())
        assert {"id"}.issubset(country.columns.keys())

        conn.execute(city.insert().values({"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "id": 0, "country": 0}))

        conn.execute(
            country.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                    "id": 0,
                }
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | ref      | Country  | 4
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example/City" ADD COLUMN "country._id" UUID;\n'
        "\n"
        f"{add_index(index_name='ix_migrate/example/City_country._id', table='migrate/example/City', columns=['country._id'])}"
        f"{add_column_comment(table='migrate/example/City', column='country._id')}"
        'UPDATE "migrate/example/City" SET '
        '"country._id"="migrate/example/Country"._id FROM "migrate/example/Country" '
        'WHERE "migrate/example/City".country = "migrate/example/Country".id;\n'
        "\n"
        f"{drop_column(table='migrate/example/City', column='country')}"
        'ALTER TABLE "migrate/example/City" ADD CONSTRAINT '
        '"fk_migrate/example/City_country._id" FOREIGN KEY("country._id") REFERENCES '
        '"migrate/example/Country" (_id);\n'
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
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())

        table = tables["migrate/example/City"]
        columns = table.columns
        assert {"id", "country._id", "__country"}.issubset(columns.keys())
        assert not {"country"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["country._id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7a"
            assert row["id"] == 0
            assert row["__country"] == 0
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_scalar_to_ref_simple_level_4_self_reference(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | Object  |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | object         | integer  |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Object",
            "migrate/example/Object/:changelog",
        }.issubset(tables.keys())
        obj = tables["migrate/example/Object"]
        assert {"id", "object"}.issubset(obj.columns.keys())

        conn.execute(
            obj.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                    "id": 0,
                }
            )
        )

        conn.execute(obj.insert().values({"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "id": 1, "object": 0}))

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | Object  |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | object         | ref      | Object   | 4      |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example/Object" ADD COLUMN "object._id" UUID;\n'
        "\n"
        f"{add_index(index_name='ix_migrate/example/Object_object._id', table='migrate/example/Object', columns=['object._id'])}"
        f"{add_column_comment(table='migrate/example/Object', column='object._id')}"
        'UPDATE "migrate/example/Object" SET '
        '"object._id"="migrate/example/Object_1"._id FROM "migrate/example/Object" AS '
        '"migrate/example/Object_1" WHERE "migrate/example/Object".object = '
        '"migrate/example/Object_1".id;\n'
        "\n"
        f"{drop_column(table='migrate/example/Object', column='object')}"
        'ALTER TABLE "migrate/example/Object" ADD CONSTRAINT '
        '"fk_migrate/example/Object_object._id" FOREIGN KEY("object._id") REFERENCES '
        '"migrate/example/Object" (_id);\n'
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
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Object", "migrate/example/Object/:changelog"}.issubset(tables.keys())

        table = tables["migrate/example/Object"]
        columns = table.columns
        assert {"id", "object._id", "__object"}.issubset(columns.keys())
        assert not {"object"}.issubset(columns.keys())

        result = conn.execute(table.select())
        result_mapping = [
            {"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a", "id": 0, "object._id": None, "__object": None},
            {
                "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                "id": 1,
                "object._id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                "__object": 0,
            },
        ]
        for i, row in enumerate(result):
            for key, value in result_mapping[i].items():
                assert row[key] == value

        cleanup_table_list(meta, ["migrate/example/Object", "migrate/example/Object/:changelog"])


def test_migrate_scalar_to_ref_simple_level_3(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | integer  |          |
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())
        city = tables["migrate/example/City"]
        country = tables["migrate/example/Country"]
        assert {"id", "country"}.issubset(city.columns.keys())
        assert {"id"}.issubset(country.columns.keys())

        conn.execute(city.insert().values({"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "id": 0, "country": 0}))

        conn.execute(
            country.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                    "id": 0,
                }
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | ref      | Country  | 3
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_column(table='migrate/example/City', column='country.id', column_type='INTEGER')}"
        'UPDATE "migrate/example/City" SET "country.id"="migrate/example/Country".id '
        'FROM "migrate/example/Country" WHERE "migrate/example/City".country = '
        '"migrate/example/Country".id;\n'
        "\n"
        f"{drop_column(table='migrate/example/City', column='country')}"
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
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())

        table = tables["migrate/example/City"]
        columns = table.columns
        assert {"id", "country.id", "__country"}.issubset(columns.keys())
        assert not {"country"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["country.id"] == 0
            assert row["id"] == 0
            assert row["__country"] == 0
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_scalar_to_ref_simple_level_3_self_reference(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | Object  |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | object         | integer  |          |        |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Object",
            "migrate/example/Object/:changelog",
        }.issubset(tables.keys())
        obj = tables["migrate/example/Object"]
        assert {"id", "object"}.issubset(obj.columns.keys())

        conn.execute(obj.insert().values({"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a", "id": 0}))

        conn.execute(obj.insert().values({"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "id": 1, "object": 0}))

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | Object  |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | object         | ref      | Object   | 3
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_column(table='migrate/example/Object', column='object.id', column_type='INTEGER')}"
        'UPDATE "migrate/example/Object" SET '
        '"object.id"="migrate/example/Object_1".id FROM "migrate/example/Object" AS '
        '"migrate/example/Object_1" WHERE "migrate/example/Object".object = '
        '"migrate/example/Object_1".id;\n'
        "\n"
        f"{drop_column(table='migrate/example/Object', column='object')}"
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
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Object", "migrate/example/Object/:changelog"}.issubset(tables.keys())

        table = tables["migrate/example/Object"]
        columns = table.columns
        assert {"id", "object.id", "__object"}.issubset(columns.keys())
        assert not {"object"}.issubset(columns.keys())

        result = conn.execute(table.select())
        result_mapping = [
            {"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a", "id": 0, "object.id": None, "__object": None},
            {"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "id": 1, "object.id": 0, "__object": 0},
        ]
        for i, row in enumerate(result):
            for key, value in result_mapping[i].items():
                assert row[key] == value

        cleanup_table_list(meta, ["migrate/example/Object", "migrate/example/Object/:changelog"])


def test_migrate_scalar_to_ref_level_3_error(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | integer  |          |
                     |   |      | Country |                |          | id, name |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | name           | string   |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())
        city = tables["migrate/example/City"]
        country = tables["migrate/example/Country"]
        assert {"id", "country"}.issubset(city.columns.keys())
        assert {"id"}.issubset(country.columns.keys())

        conn.execute(city.insert().values({"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "id": 0, "country": 0}))

        conn.execute(
            country.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                    "id": 0,
                }
            )
        )

        override_manifest(
            context,
            tmp_path,
            """
         d               | r | b    | m       | property       | type     | ref      | level
         migrate/example |   |      |         |                |          |          |
                         |   |      | City    |                |          | id       |
                         |   |      |         | id             | integer  |          |
                         |   |      |         | country        | ref      | Country  | 3
                         |   |      | Country |                |          | id, name |
                         |   |      |         | id             | integer  |          |
                         |   |      |         | name           | string   |          |
        """,
        )

        result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"], fail=False)
        assert result.exit_code != 0
        assert isinstance(result.exception, MigrateScalarToRefTooManyKeys)

        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_scalar_to_ref_level_3_type_error(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | string   |          |
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | name           | string   |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())
        city = tables["migrate/example/City"]
        country = tables["migrate/example/Country"]
        assert {"id", "country"}.issubset(city.columns.keys())
        assert {"id"}.issubset(country.columns.keys())

        conn.execute(city.insert().values({"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "id": 0, "country": "0"}))

        conn.execute(
            country.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                    "id": 0,
                }
            )
        )

        override_manifest(
            context,
            tmp_path,
            """
         d               | r | b    | m       | property       | type     | ref      | level
         migrate/example |   |      |         |                |          |          |
                         |   |      | City    |                |          | id       |
                         |   |      |         | id             | integer  |          |
                         |   |      |         | country        | ref      | Country  | 3
                         |   |      | Country |                |          | id       |
                         |   |      |         | id             | integer  |          |
                         |   |      |         | name           | string   |          |
        """,
        )

        result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"], fail=False)
        assert result.exit_code != 0
        assert isinstance(result.exception, MigrateScalarToRefTypeMissmatch)

        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_scalar_to_ref_level_4_error(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | integer  |          |
                     |   |      | Country |                |          | id, name |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | name           | string   |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())
        city = tables["migrate/example/City"]
        country = tables["migrate/example/Country"]
        assert {"id", "country"}.issubset(city.columns.keys())
        assert {"id"}.issubset(country.columns.keys())

        conn.execute(city.insert().values({"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "id": 0, "country": 0}))

        conn.execute(country.insert().values({"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a", "id": 0, "name": "test"}))

        override_manifest(
            context,
            tmp_path,
            """
         d               | r | b    | m       | property       | type     | ref      | level
         migrate/example |   |      |         |                |          |          |
                         |   |      | City    |                |          | id       |
                         |   |      |         | id             | integer  |          |
                         |   |      |         | country        | ref      | Country  | 4
                         |   |      | Country |                |          | id, name |
                         |   |      |         | id             | integer  |          |
                         |   |      |         | name           | string   |          |
        """,
        )
        result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"], fail=False)
        assert result.exit_code != 0
        assert isinstance(result.exception, MigrateScalarToRefTooManyKeys)
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_scalar_to_ref_level_4_type_error(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | string   |          |
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | name           | string   |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())
        city = tables["migrate/example/City"]
        country = tables["migrate/example/Country"]
        assert {"id", "country"}.issubset(city.columns.keys())
        assert {"id"}.issubset(country.columns.keys())

        conn.execute(city.insert().values({"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "id": 0, "country": "0"}))

        conn.execute(country.insert().values({"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a", "id": 0, "name": "test"}))

        override_manifest(
            context,
            tmp_path,
            """
         d               | r | b    | m       | property       | type     | ref      | level
         migrate/example |   |      |         |                |          |          |
                         |   |      | City    |                |          | id       |
                         |   |      |         | id             | integer  |          |
                         |   |      |         | country        | ref      | Country  | 4
                         |   |      | Country |                |          | id       |
                         |   |      |         | id             | integer  |          |
                         |   |      |         | name           | string   |          |
        """,
        )
        result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"], fail=False)
        assert result.exit_code != 0
        assert isinstance(result.exception, MigrateScalarToRefTypeMissmatch)
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_ref_to_scalar_simple_level_3(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | ref      | Country  | 3
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())
        city = tables["migrate/example/City"]
        country = tables["migrate/example/Country"]
        assert {"id", "country.id"}.issubset(city.columns.keys())
        assert {"id"}.issubset(country.columns.keys())
        conn.execute(
            country.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                    "id": 0,
                }
            )
        )
        conn.execute(city.insert().values({"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "id": 0, "country.id": 0}))

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | integer  |          |  
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        f"BEGIN;\n\n{rename_column(table='migrate/example/City', column='country.id', new_name='country')}COMMIT;\n\n"
    )

    cli.invoke(
        rc,
        [
            "migrate",
            f"{tmp_path}/manifest.csv",
        ],
    )
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())

        table = tables["migrate/example/City"]
        columns = table.columns
        assert {"id", "country"}.issubset(columns.keys())
        assert not {"country.id"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["id"] == 0
            assert row["country"] == 0
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_ref_to_scalar_simple_level_3_self_reference(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | Object  |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | object         | ref      | Object   | 3       |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Object",
            "migrate/example/Object/:changelog",
        }.issubset(tables.keys())
        obj = tables["migrate/example/Object"]
        assert {"id", "object.id"}.issubset(obj.columns.keys())
        conn.execute(
            obj.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                    "id": 0,
                }
            )
        )
        conn.execute(obj.insert().values({"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "id": 1, "object.id": 0}))

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | Object  |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | object         | integer  |          |           |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        f"BEGIN;\n\n{rename_column(table='migrate/example/Object', column='object.id', new_name='object')}COMMIT;\n\n"
    )

    cli.invoke(
        rc,
        [
            "migrate",
            f"{tmp_path}/manifest.csv",
        ],
    )
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Object", "migrate/example/Object/:changelog"}.issubset(tables.keys())

        table = tables["migrate/example/Object"]
        columns = table.columns
        assert {"id", "object"}.issubset(columns.keys())
        assert not {"object.id"}.issubset(columns.keys())

        result = conn.execute(table.select())
        result_mapping = [
            {"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a", "id": 0, "object": None},
            {"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "id": 1, "object": 0},
        ]
        for i, row in enumerate(result):
            for key, value in result_mapping[i].items():
                assert row[key] == value
        cleanup_table_list(meta, ["migrate/example/Object", "migrate/example/Object/:changelog"])


def test_migrate_ref_to_scalar_advanced_level_3_rename(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | ref      | Country  | 3
                     |   |      | Country |                |          | id, name, code |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | name           | string   |          |
                     |   |      |         | code           | string   |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())
        city = tables["migrate/example/City"]
        country = tables["migrate/example/Country"]
        assert {"id", "country.id"}.issubset(city.columns.keys())
        assert {"id"}.issubset(country.columns.keys())
        conn.execute(
            country.insert().values(
                {"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a", "id": 0, "name": "Lithuania", "code": "LT"}
            )
        )
        conn.execute(
            city.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "id": 0,
                    "country.id": 0,
                    "country.name": "Lithuania",
                    "country.code": "LT",
                }
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country_id     | integer  |          |  
                     |   |      |         | country_name   | string   |          |  
                     |   |      |         | country_code   | string   |          |  
                     |   |      | Country |                |          | id, name, code |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | name           | string   |          |
                     |   |      |         | code           | string   |          |
    """,
    )
    rename_file = {
        "migrate/example/City": {
            "country.id": "country_id",
            "country.name": "country_name",
            "country.code": "country_code",
        },
    }
    path = tmp_path / "rename.json"
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-r", path])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{rename_column(table='migrate/example/City', column='country.id', new_name='country_id')}"
        f"{rename_column(table='migrate/example/City', column='country.name', new_name='country_name')}"
        f"{rename_column(table='migrate/example/City', column='country.code', new_name='country_code')}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-r", path])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())

        table = tables["migrate/example/City"]
        columns = table.columns
        assert {"id", "country_id", "country_name", "country_code"}.issubset(columns.keys())
        assert not {"country.id", "country.name", "country.code"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["id"] == 0
            assert row["country_id"] == 0
            assert row["country_name"] == "Lithuania"
            assert row["country_code"] == "LT"
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_ref_to_scalar_advanced_level_3_rename_with_delete(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | ref      | Country  | 3
                     |   |      | Country |                |          | id, name, code |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | name           | string   |          |
                     |   |      |         | code           | string   |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())
        city = tables["migrate/example/City"]
        country = tables["migrate/example/Country"]
        assert {"id", "country.id"}.issubset(city.columns.keys())
        assert {"id"}.issubset(country.columns.keys())
        conn.execute(
            country.insert().values(
                {"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a", "id": 0, "name": "Lithuania", "code": "LT"}
            )
        )
        conn.execute(
            city.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "id": 0,
                    "country.id": 0,
                    "country.name": "Lithuania",
                    "country.code": "LT",
                }
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country_id     | integer  |          |  
                     |   |      |         | country_name   | string   |          |  
                     |   |      |         | country_code   | string   |          |  
                     |   |      | Country |                |          | id, name, code |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | name           | string   |          |
                     |   |      |         | code           | string   |          |
    """,
    )
    rename_file = {
        "migrate/example/City": {
            "country.id": "country_id",
            "country.name": "country_name",
        },
    }
    path = tmp_path / "rename.json"
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-r", path])

    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{rename_column(table='migrate/example/City', column='country.id', new_name='country_id')}"
        f"{rename_column(table='migrate/example/City', column='country.name', new_name='country_name')}"
        f"{drop_column(table='migrate/example/City', column='country.code')}"
        f"{add_column(table='migrate/example/City', column='country_code', column_type='TEXT')}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-r", path])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())

        table = tables["migrate/example/City"]
        columns = table.columns
        assert {"id", "country_id", "country_name", "country_code", "__country.code"}.issubset(columns.keys())
        assert not {"country.id", "country.name", "country.code"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["id"] == 0
            assert row["country_id"] == 0
            assert row["country_name"] == "Lithuania"
            assert row["country_code"] is None
            assert row["__country.code"] == "LT"
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_ref_to_scalar_simple_level_4(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | ref      | Country  | 4
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())
        city = tables["migrate/example/City"]
        country = tables["migrate/example/Country"]
        assert {"id", "country._id"}.issubset(city.columns.keys())
        assert {"id"}.issubset(country.columns.keys())
        conn.execute(
            country.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                    "id": 0,
                }
            )
        )
        conn.execute(
            city.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "id": 0,
                    "country._id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                }
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | integer  |          |  
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_column(table='migrate/example/City', column='country', column_type='INTEGER')}"
        'UPDATE "migrate/example/City" SET country="migrate/example/Country".id FROM '
        '"migrate/example/Country" WHERE "migrate/example/City"."country._id" = '
        '"migrate/example/Country"._id;\n'
        "\n"
        f"{drop_column(table='migrate/example/City', column='country._id')}"
        'DROP INDEX "ix_migrate/example/City_country._id";\n'
        "\n"
        'ALTER TABLE "migrate/example/City" DROP CONSTRAINT '
        '"fk_migrate/example/City_country._id";\n'
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
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())

        table = tables["migrate/example/City"]
        columns = table.columns
        assert {"id", "__country._id", "country"}.issubset(columns.keys())
        assert not {"country._id"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["__country._id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7a"
            assert row["id"] == 0
            assert row["country"] == 0
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_ref_to_scalar_simple_level_4_self_reference(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | Object  |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | object         | ref      | Object   | 4     |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/Object",
            "migrate/example/Object/:changelog",
        }.issubset(tables.keys())
        obj = tables["migrate/example/Object"]
        assert {"id", "object._id"}.issubset(obj.columns.keys())
        conn.execute(
            obj.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                    "id": 0,
                }
            )
        )
        conn.execute(
            obj.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "id": 1,
                    "object._id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                }
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | Object  |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | object         | integer  |          |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_column(table='migrate/example/Object', column='object', column_type='INTEGER')}"
        'UPDATE "migrate/example/Object" SET object="migrate/example/Object_1".id '
        'FROM "migrate/example/Object" AS "migrate/example/Object_1" WHERE '
        '"migrate/example/Object"."object._id" = "migrate/example/Object_1"._id;\n'
        "\n"
        f"{drop_column(table='migrate/example/Object', column='object._id')}"
        'DROP INDEX "ix_migrate/example/Object_object._id";\n'
        "\n"
        'ALTER TABLE "migrate/example/Object" DROP CONSTRAINT '
        '"fk_migrate/example/Object_object._id";\n'
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
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Object", "migrate/example/Object/:changelog"}.issubset(tables.keys())

        table = tables["migrate/example/Object"]
        columns = table.columns
        assert {"id", "__object._id", "object"}.issubset(columns.keys())
        assert not {"object._id"}.issubset(columns.keys())

        result = conn.execute(table.select())
        result_mapping = [
            {"__object._id": None, "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a", "id": 0, "object": None},
            {
                "__object._id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                "id": 1,
                "object": 0,
            },
        ]
        for i, row in enumerate(result):
            for key, value in result_mapping[i].items():
                assert row[key] == value
        cleanup_table_list(meta, ["migrate/example/Object", "migrate/example/Object/:changelog"])


def test_migrate_ref_to_scalar_level_4_error(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | ref      | Country  | 4
                     |   |      | Country |                |          | id, name, code |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | name           | string   |          |
                     |   |      |         | code           | string   |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())
        city = tables["migrate/example/City"]
        country = tables["migrate/example/Country"]
        assert {"id", "country._id"}.issubset(city.columns.keys())
        assert {"id"}.issubset(country.columns.keys())
        conn.execute(
            country.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                    "id": 0,
                }
            )
        )
        conn.execute(
            city.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "id": 0,
                    "country._id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                }
            )
        )

        override_manifest(
            context,
            tmp_path,
            """
         d               | r | b    | m       | property       | type     | ref      | level
         migrate/example |   |      |         |                |          |          |
                         |   |      | City    |                |          | id       |
                         |   |      |         | id             | integer  |          |
                         |   |      |         | country        | integer  |          |  
                         |   |      | Country |                |          | id, name, code |
                         |   |      |         | id             | integer  |          |
                         |   |      |         | name           | string   |          |
                         |   |      |         | code           | string   |          |
        """,
        )

        result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"], fail=False)
        assert result.exit_code != 0
        assert isinstance(result.exception, MigrateScalarToRefTooManyKeys)
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_ref_to_scalar_no_unique_constraints_error(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | ref      | Country  | 4
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())
        city = tables["migrate/example/City"]
        country = tables["migrate/example/Country"]
        assert {"id", "country._id"}.issubset(city.columns.keys())
        assert {"id"}.issubset(country.columns.keys())
        conn.execute(
            country.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                    "id": 0,
                }
            )
        )
        conn.execute(
            city.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "id": 0,
                    "country._id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                }
            )
        )

        # Corrupt data by deleting `UniqueConstraints`
        conn.execute('ALTER TABLE "migrate/example/Country" DROP CONSTRAINT "uq_migrate/example/Country_id"')

        override_manifest(
            context,
            tmp_path,
            """
         d               | r | b    | m       | property       | type     | ref      | level
         migrate/example |   |      |         |                |          |          |
                         |   |      | City    |                |          | id       |
                         |   |      |         | id             | integer  |          |
                         |   |      |         | country        | integer  |          |  
                         |   |      | Country |                |          | id       |
                         |   |      |         | id             | integer  |          |
        """,
        )

        with pytest.raises(UnableToFindPrimaryKeysNoUniqueConstraints):
            result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"], fail=False)
            assert result.exit_code != 0
            raise result.exception

        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_ref_to_scalar_too_many_unique_constraints_error(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | ref      | Country  | 4
                     |   |      | Country |                |          | id       |
                     |   |      |         |                | unique   | name     |
                     |   |      |         |                | unique   | code     |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | name           | string   |          |
                     |   |      |         | code           | string   |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())
        city = tables["migrate/example/City"]
        country = tables["migrate/example/Country"]
        assert {"id", "country._id"}.issubset(city.columns.keys())
        assert {"id"}.issubset(country.columns.keys())
        conn.execute(
            country.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                    "id": 0,
                }
            )
        )
        conn.execute(
            city.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "id": 0,
                    "country._id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                }
            )
        )

        override_manifest(
            context,
            tmp_path,
            """
        d               | r | b    | m       | property       | type     | ref      | level
        migrate/example |   |      |         |                |          |          |
                        |   |      | City    |                |          | id       |
                        |   |      |         | id             | integer  |          |
                        |   |      |         | country        | integer  |          |
                        |   |      | Country |                |          |          |
                        |   |      |         |                | unique   | name     |
                        |   |      |         |                | unique   | code     |
                        |   |      |         | id             | integer  |          |
                        |   |      |         | name           | string   |          |
                        |   |      |         | code           | string   |          |
        """,
        )

        with pytest.raises(UnableToFindPrimaryKeysMultipleUniqueConstraints):
            result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"], fail=False)
            assert result.exit_code != 0
            raise result.exception

        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_ref_level_3_no_pkey_ignore(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b | m    | property     | type | ref | level
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m      | property     | type          | ref  | level
     migrate/example |   |   |        |              |               |      |
                     |   |   | Test   |              |               |      |
                     |   |   |        | someText     | string        |      |
                     |   |   |        | someInteger  | integer       |      |
                     |   |   |        | someNumber   | number        |      |
                     |   |   |        |              |               |      |
                     |   |   | Ref    |              |               |      |
                     |   |   |        | someText     | string        |      |
                     |   |   |        | someRef      | ref           | Test | 3
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
        '    CONSTRAINT "pk_migrate/example/Test" PRIMARY KEY (_id)\n'
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
        f"{add_table_comment(table='migrate/example/Test', comment='migrate/example/Test')}"
        f"{add_changelog_table(table='migrate/example/Test/:changelog')}"
        f"{add_redirect_table(table='migrate/example/Test/:redirect')}"
        'CREATE TABLE "migrate/example/Ref" (\n'
        "    _txn UUID, \n"
        "    _created TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _updated TIMESTAMP WITHOUT TIME ZONE, \n"
        "    _id UUID NOT NULL, \n"
        "    _revision TEXT, \n"
        '    "someText" TEXT, \n'
        '    "someRef._id" UUID, \n'
        '    CONSTRAINT "pk_migrate/example/Ref" PRIMARY KEY (_id)\n'
        ");\n"
        "\n"
        f"{add_index(index_name='ix_migrate/example/Ref__txn', table='migrate/example/Ref', columns=['_txn'])}"
        f"{add_column_comment(table='migrate/example/Ref', column='_txn')}"
        f"{add_column_comment(table='migrate/example/Ref', column='_created')}"
        f"{add_column_comment(table='migrate/example/Ref', column='_updated')}"
        f"{add_column_comment(table='migrate/example/Ref', column='_id')}"
        f"{add_column_comment(table='migrate/example/Ref', column='_revision')}"
        f"{add_column_comment(table='migrate/example/Ref', column='someText')}"
        f"{add_column_comment(table='migrate/example/Ref', column='someRef._id')}"
        f"{add_table_comment(table='migrate/example/Ref', comment='migrate/example/Ref')}"
        f"{add_changelog_table(table='migrate/example/Ref/:changelog')}"
        f"{add_redirect_table(table='migrate/example/Ref/:redirect')}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        table = tables["migrate/example/Test"]

        assert {
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Ref",
            "migrate/example/Ref/:changelog",
        }.issubset(tables.keys())
        columns = table.columns
        assert {"someText", "someNumber", "someInteger"}.issubset(columns.keys())

        table = tables["migrate/example/Ref"]
        columns = table.columns
        assert {"someText", "someRef._id"}.issubset(columns.keys())

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith("BEGIN;\n\nCOMMIT;\n\n")

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
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


def test_migrate_adjust_ref_levels_no_pkey(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b | m      | property     | type          | ref  | level
     migrate/example |   |   |        |              |               |      |
                     |   |   | Test   |              |               |      |
                     |   |   |        | someText     | string        |      |
                     |   |   |        | someInteger  | integer       |      |
                     |   |   |        | someNumber   | number        |      |
                     |   |   |        |              |               |      |
                     |   |   | Ref    |              |               |      |
                     |   |   |        | someText     | string        |      |
                     |   |   |        | someRef      | ref           | Test | 3
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])
    insert_values = [
        {"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "someText": "test1", "someInteger": 0, "someNumber": 1.0},
        {"_id": "b574111d-bd2f-4c94-9249-d9a0de49bd5b", "someText": "test2", "someInteger": 1, "someNumber": 2.0},
        {"_id": "1686c00c-0c59-413a-aa30-f5605488cc77", "someText": "test3", "someInteger": 0, "someNumber": 1.0},
    ]
    ref_insert = [
        {"_id": "350e7a87-28a5-4645-a659-2daa8e4bbe55", "someRef._id": "197109d9-add8-49a5-ab19-3ddc7589ce7e"},
        {"_id": "72d5dc33-a074-43f0-882f-e06abd34113b", "someRef._id": "b574111d-bd2f-4c94-9249-d9a0de49bd5b"},
        {"_id": "478be0be-6ab9-4c03-8551-53d881567743", "someRef._id": "1686c00c-0c59-413a-aa30-f5605488cc77"},
    ]

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        table = tables["migrate/example/Test"]
        for item in insert_values:
            conn.execute(table.insert().values(item))

        result = conn.execute(table.select())
        for i, item in enumerate(result):
            item = dict(item)
            assert item["_id"] == insert_values[i]["_id"]
            assert item["someText"] == insert_values[i]["someText"]
            assert item["someInteger"] == insert_values[i]["someInteger"]
            assert item["someNumber"] == insert_values[i]["someNumber"]

        assert {
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Ref",
            "migrate/example/Ref/:changelog",
        }.issubset(tables.keys())
        columns = table.columns
        assert {"someText", "someNumber", "someInteger"}.issubset(columns.keys())

        table = tables["migrate/example/Ref"]
        for item in ref_insert:
            conn.execute(table.insert().values(item))

        result = conn.execute(table.select())
        for i, item in enumerate(result):
            item = dict(item)
            assert item["_id"] == ref_insert[i]["_id"]
            assert item["someRef._id"] == insert_values[i]["_id"]
        columns = table.columns
        assert {"someText", "someRef._id"}.issubset(columns.keys())

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m      | property     | type          | ref  | level
     migrate/example |   |   |        |              |               |      |
                     |   |   | Test   |              |               |      |
                     |   |   |        | someText     | string        |      |
                     |   |   |        | someInteger  | integer       |      |
                     |   |   |        | someNumber   | number        |      |
                     |   |   |        |              |               |      |
                     |   |   | Ref    |              |               |      |
                     |   |   |        | someText     | string        |      |
                     |   |   |        | someRef      | ref           | Test | 4
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])

    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_index(index_name='ix_migrate/example/Ref_someRef._id', table='migrate/example/Ref', columns=['someRef._id'])}"
        'ALTER TABLE "migrate/example/Ref" ADD CONSTRAINT '
        '"fk_migrate/example/Ref_someRef._id" FOREIGN KEY("someRef._id") REFERENCES '
        '"migrate/example/Test" (_id);\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        table = tables["migrate/example/Ref"]
        columns = table.columns
        assert {"someText", "someRef._id"}.issubset(columns.keys())

        columns = get_table_foreign_key_constraint_columns(table)
        assert any(
            [["someRef._id"], ["_id"]] == [constraint["column_names"], constraint["referred_column_names"]]
            for constraint in columns
        )

        result = conn.execute(table.select())
        for i, item in enumerate(result):
            item = dict(item)
            assert item["_id"] == ref_insert[i]["_id"]
            assert item["someRef._id"] == insert_values[i]["_id"]

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m      | property     | type          | ref  | level
     migrate/example |   |   |        |              |               |      |
                     |   |   | Test   |              |               |      |
                     |   |   |        | someText     | string        |      |
                     |   |   |        | someInteger  | integer       |      |
                     |   |   |        | someNumber   | number        |      |
                     |   |   |        |              |               |      |
                     |   |   | Ref    |              |               |      |
                     |   |   |        | someText     | string        |      |
                     |   |   |        | someRef      | ref           | Test | 3
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'DROP INDEX "ix_migrate/example/Ref_someRef._id";\n'
        "\n"
        'ALTER TABLE "migrate/example/Ref" DROP CONSTRAINT '
        '"fk_migrate/example/Ref_someRef._id";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables

        table = tables["migrate/example/Ref"]
        columns = table.columns
        assert {"someText", "someRef._id"}.issubset(columns.keys())

        columns = get_table_foreign_key_constraint_columns(table)
        assert not any(
            [["someRef._id"], ["_id"]] == [constraint["column_names"], constraint["referred_column_names"]]
            for constraint in columns
        )

        result = conn.execute(table.select())
        for i, item in enumerate(result):
            item = dict(item)
            assert item["_id"] == ref_insert[i]["_id"]
            assert item["someRef._id"] == insert_values[i]["_id"]

        cleanup_table_list(
            meta,
            [
                "migrate/example/Test",
                "migrate/example/Test/:changelog",
                "migrate/example/Ref",
                "migrate/example/Ref/:changelog",
            ],
        )


def test_migrate_adjust_ref_levels_no_pkey_previously_given_key(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b | m      | property     | type          | ref            | level
     migrate/example |   |   |        |              |               |                |
                     |   |   | Test   |              |               |                |
                     |   |   |        | someText     | string        |                |
                     |   |   |        | someInteger  | integer       |                |
                     |   |   |        | someNumber   | number        |                |
                     |   |   |        |              |               |                |
                     |   |   | Ref    |              |               |                |
                     |   |   |        | someText     | string        |                |
                     |   |   |        | someRef      | ref           | Test[someText] | 3
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])
    insert_values = [
        {"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "someText": "test1", "someInteger": 0, "someNumber": 1.0},
        {"_id": "b574111d-bd2f-4c94-9249-d9a0de49bd5b", "someText": "test2", "someInteger": 1, "someNumber": 2.0},
        {"_id": "1686c00c-0c59-413a-aa30-f5605488cc77", "someText": "test3", "someInteger": 0, "someNumber": 1.0},
    ]
    ref_insert = [
        {"_id": "350e7a87-28a5-4645-a659-2daa8e4bbe55", "someRef.someText": "test1"},
        {"_id": "72d5dc33-a074-43f0-882f-e06abd34113b", "someRef.someText": "test2"},
        {"_id": "478be0be-6ab9-4c03-8551-53d881567743", "someRef.someText": "test3"},
    ]

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        table = tables["migrate/example/Test"]
        for item in insert_values:
            conn.execute(table.insert().values(item))

        result = conn.execute(table.select())
        for i, item in enumerate(result):
            item = dict(item)
            assert item["_id"] == insert_values[i]["_id"]
            assert item["someText"] == insert_values[i]["someText"]
            assert item["someInteger"] == insert_values[i]["someInteger"]
            assert item["someNumber"] == insert_values[i]["someNumber"]

        assert {
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Ref",
            "migrate/example/Ref/:changelog",
        }.issubset(tables.keys())
        columns = table.columns
        assert {"someText", "someNumber", "someInteger"}.issubset(columns.keys())

        table = tables["migrate/example/Ref"]
        for item in ref_insert:
            conn.execute(table.insert().values(item))

        result = conn.execute(table.select())
        for i, item in enumerate(result):
            item = dict(item)
            assert item["_id"] == ref_insert[i]["_id"]
            assert item["someRef.someText"] == insert_values[i]["someText"]
        columns = table.columns
        assert {"someText", "someRef.someText"}.issubset(columns.keys())

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m      | property     | type          | ref  | level
     migrate/example |   |   |        |              |               |      |
                     |   |   | Test   |              |               |      |
                     |   |   |        | someText     | string        |      |
                     |   |   |        | someInteger  | integer       |      |
                     |   |   |        | someNumber   | number        |      |
                     |   |   |        |              |               |      |
                     |   |   | Ref    |              |               |      |
                     |   |   |        | someText     | string        |      |
                     |   |   |        | someRef      | ref           | Test | 4
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example/Ref" ADD COLUMN "someRef._id" UUID;\n'
        "\n"
        f"{add_index(index_name='ix_migrate/example/Ref_someRef._id', table='migrate/example/Ref', columns=['someRef._id'])}"
        f"{add_column_comment(table='migrate/example/Ref', column='someRef._id')}"
        'UPDATE "migrate/example/Ref" SET "someRef._id"="migrate/example/Test"._id '
        'FROM "migrate/example/Test" WHERE "migrate/example/Ref"."someRef.someText" = '
        '"migrate/example/Test"."someText";\n'
        "\n"
        f"{drop_column(table='migrate/example/Ref', column='someRef.someText')}"
        'ALTER TABLE "migrate/example/Ref" ADD CONSTRAINT '
        '"fk_migrate/example/Ref_someRef._id" FOREIGN KEY("someRef._id") REFERENCES '
        '"migrate/example/Test" (_id);\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        table = tables["migrate/example/Ref"]
        columns = table.columns
        assert {"someText", "someRef._id"}.issubset(columns.keys())

        columns = get_table_foreign_key_constraint_columns(table)
        assert any(
            [["someRef._id"], ["_id"]] == [constraint["column_names"], constraint["referred_column_names"]]
            for constraint in columns
        )

        result = conn.execute(table.select())
        for i, item in enumerate(result):
            item = dict(item)
            assert item["_id"] == ref_insert[i]["_id"]
            assert item["someRef._id"] == insert_values[i]["_id"]

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m      | property     | type          | ref            | level
     migrate/example |   |   |        |              |               |                |
                     |   |   | Test   |              |               |                |
                     |   |   |        | someText     | string        |                |
                     |   |   |        | someInteger  | integer       |                |
                     |   |   |        | someNumber   | number        |                |
                     |   |   |        |              |               |                |
                     |   |   | Ref    |              |               |                |
                     |   |   |        | someText     | string        |                |
                     |   |   |        | someRef      | ref           | Test[someText] | 3
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_column(table='migrate/example/Ref', column='someRef.someText', column_type='TEXT')}"
        'UPDATE "migrate/example/Ref" SET '
        '"someRef.someText"="migrate/example/Test"."someText" FROM '
        '"migrate/example/Test" WHERE "migrate/example/Ref"."someRef._id" = '
        '"migrate/example/Test"._id;\n'
        "\n"
        f"{drop_column(table='migrate/example/Ref', column='someRef._id')}"
        'DROP INDEX "ix_migrate/example/Ref_someRef._id";\n'
        "\n"
        'ALTER TABLE "migrate/example/Ref" DROP CONSTRAINT '
        '"fk_migrate/example/Ref_someRef._id";\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables

        table = tables["migrate/example/Ref"]
        columns = table.columns
        assert {"someText", "someRef.someText"}.issubset(columns.keys())

        columns = get_table_foreign_key_constraint_columns(table)
        assert not any(
            [["someRef._id"], ["_id"]] == [constraint["column_names"], constraint["referred_column_names"]]
            for constraint in columns
        )

        result = conn.execute(table.select())
        for i, item in enumerate(result):
            item = dict(item)
            assert item["_id"] == ref_insert[i]["_id"]
            assert item["someRef.someText"] == insert_values[i]["someText"]

        cleanup_table_list(
            meta,
            [
                "migrate/example/Test",
                "migrate/example/Test/:changelog",
                "migrate/example/Ref",
                "migrate/example/Ref/:changelog",
            ],
        )


def test_migrate_adjust_ref_levels_no_pkey_previously_given_key_with_denorm(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b | m      | property            | type    | ref            | level
     migrate/example |   |   |        |                     |         |                |
                     |   |   | Test   |                     |         |                |
                     |   |   |        | someText            | string  |                |
                     |   |   |        | someInteger         | integer |                |
                     |   |   |        | someNumber          | number  |                |
                     |   |   |        |                     |         |                |
                     |   |   | Ref    |                     |         |                |
                     |   |   |        | someText            | string  |                |
                     |   |   |        | someRef             | ref     | Test[someText] | 3
                     |   |   |        | someRef.new         | string  |                | 
                     |   |   |        | someRef.someInteger |         |                | 
                     |   |   |        | someRef.someNumber  | number  |                | 
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])
    insert_values = [
        {"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "someText": "test1", "someInteger": 0, "someNumber": 1.0},
        {"_id": "b574111d-bd2f-4c94-9249-d9a0de49bd5b", "someText": "test2", "someInteger": 1, "someNumber": 2.0},
        {"_id": "1686c00c-0c59-413a-aa30-f5605488cc77", "someText": "test3", "someInteger": 0, "someNumber": 1.0},
    ]
    ref_insert = [
        {"_id": "350e7a87-28a5-4645-a659-2daa8e4bbe55", "someRef.someText": "test1"},
        {"_id": "72d5dc33-a074-43f0-882f-e06abd34113b", "someRef.someText": "test2"},
        {"_id": "478be0be-6ab9-4c03-8551-53d881567743", "someRef.someText": "test3"},
    ]

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        table = tables["migrate/example/Test"]
        for item in insert_values:
            conn.execute(table.insert().values(item))

        result = conn.execute(table.select())
        for i, item in enumerate(result):
            item = dict(item)
            assert item["_id"] == insert_values[i]["_id"]
            assert item["someText"] == insert_values[i]["someText"]
            assert item["someInteger"] == insert_values[i]["someInteger"]
            assert item["someNumber"] == insert_values[i]["someNumber"]

        assert {
            "migrate/example/Test",
            "migrate/example/Test/:changelog",
            "migrate/example/Ref",
            "migrate/example/Ref/:changelog",
        }.issubset(tables.keys())
        columns = table.columns
        assert {"someText", "someNumber", "someInteger"}.issubset(columns.keys())

        table = tables["migrate/example/Ref"]
        for item in ref_insert:
            conn.execute(table.insert().values(item))

        result = conn.execute(table.select())
        for i, item in enumerate(result):
            item = dict(item)
            assert item["_id"] == ref_insert[i]["_id"]
            assert item["someRef.someText"] == insert_values[i]["someText"]
        columns = table.columns
        assert {"someText", "someRef.someText"}.issubset(columns.keys())

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m      | property            | type    | ref            | level
     migrate/example |   |   |        |                     |         |                |
                     |   |   | Test   |                     |         |                |
                     |   |   |        | someText            | string  |                |
                     |   |   |        | someInteger         | integer |                |
                     |   |   |        | someNumber          | number  |                |
                     |   |   |        |                     |         |                |
                     |   |   | Ref    |                     |         |                |
                     |   |   |        | someText            | string  |                |
                     |   |   |        | someRef             | ref     | Test[someText] | 4
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example/Ref" ADD COLUMN "someRef._id" UUID;\n'
        "\n"
        f"{add_index(index_name='ix_migrate/example/Ref_someRef._id', table='migrate/example/Ref', columns=['someRef._id'])}"
        f"{add_column_comment(table='migrate/example/Ref', column='someRef._id')}"
        'UPDATE "migrate/example/Ref" SET "someRef._id"="migrate/example/Test"._id '
        'FROM "migrate/example/Test" WHERE "migrate/example/Ref"."someRef.someText" = '
        '"migrate/example/Test"."someText";\n'
        "\n"
        f"{drop_column(table='migrate/example/Ref', column='someRef.someText')}"
        'ALTER TABLE "migrate/example/Ref" ADD CONSTRAINT '
        '"fk_migrate/example/Ref_someRef._id" FOREIGN KEY("someRef._id") REFERENCES '
        '"migrate/example/Test" (_id);\n'
        "\n"
        f"{drop_column(table='migrate/example/Ref', column='someRef.new')}"
        f"{drop_column(table='migrate/example/Ref', column='someRef.someNumber')}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        table = tables["migrate/example/Ref"]
        columns = table.columns
        assert {"someText", "someRef._id"}.issubset(columns.keys())

        columns = get_table_foreign_key_constraint_columns(table)
        assert any(
            [["someRef._id"], ["_id"]] == [constraint["column_names"], constraint["referred_column_names"]]
            for constraint in columns
        )

        result = conn.execute(table.select())
        for i, item in enumerate(result):
            item = dict(item)
            assert item["_id"] == ref_insert[i]["_id"]
            assert item["someRef._id"] == insert_values[i]["_id"]

        cleanup_table_list(
            meta,
            [
                "migrate/example/Test",
                "migrate/example/Test/:changelog",
                "migrate/example/Ref",
                "migrate/example/Ref/:changelog",
            ],
        )
