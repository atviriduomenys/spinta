import json
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import URL

from spinta.core.config import RawConfig
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.migration import add_column, drop_column, rename_column, add_column_comment, add_index
from tests.backends.postgresql.commands.migrate.test_migrations import (
    cleanup_tables,
    configure_migrate,
    override_manifest,
    cleanup_table_list,
    get_table_foreign_key_constraint_columns,
)


def test_migrate_do_nothing_ref_4_denorm(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | ref      | Country  | 4
                     |   |      |         | country.name   | string   |          |
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
        assert {"id", "country._id", "country.name"}.issubset(city.columns.keys())
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
                    "country.name": "Lithuania",
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
                     |   |      |         | country.name   | string   |          |
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | name           | string   |          |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith("BEGIN;\n\nCOMMIT;\n\n")

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
        assert {"id", "country._id", "country.name"}.issubset(columns.keys())
        assert not {"__country._id"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["id"] == 0
            assert row["country._id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7a"
            assert row["country.name"] == "Lithuania"
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_do_nothing_ref_3_denorm(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | ref      | Country  | 3
                     |   |      |         | country.name   | string   |          |
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
        assert {"id", "country.id", "country.name"}.issubset(city.columns.keys())
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
                {"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "id": 0, "country.id": 0, "country.name": "Lithuania"}
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
                     |   |      |         | country.name   | string   |          |
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | name           | string   |          |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith("BEGIN;\n\nCOMMIT;\n\n")

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
        assert {"id", "country.id", "country.name"}.issubset(columns.keys())
        assert not {"__country.id"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["id"] == 0
            assert row["country.id"] == 0
            assert row["country.name"] == "Lithuania"
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_adjust_ref_levels_with_denorm(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b | m      | property            | type          | ref                  | level
     migrate/example |   |   |        |                     |               |                      |
                     |   |   | Test   |                     |               | someText, someNumber |
                     |   |   |        | someText            | string        |                      |
                     |   |   |        | someInteger         | integer       |                      |
                     |   |   |        | someNumber          | number        |                      |
                     |   |   |        |                     |               |                      |
                     |   |   | Ref    |                     |               |                      |
                     |   |   |        | someText            | string        |                      |
                     |   |   |        | someRef             | ref           | Test                 | 4
                     |   |   |        | someRef.id          | integer       |                      | 
                     |   |   |        | someRef.text        | string        |                      | 
                     |   |   |        | someRef.someInteger |               |                      | 
                     
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])
    insert_values = [
        {"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "someText": "test1", "someInteger": 0, "someNumber": 1.0},
        {"_id": "b574111d-bd2f-4c94-9249-d9a0de49bd5b", "someText": "test2", "someInteger": 1, "someNumber": 2.0},
        {"_id": "1686c00c-0c59-413a-aa30-f5605488cc77", "someText": "test3", "someInteger": 0, "someNumber": 1.0},
    ]
    ref_insert = [
        {
            "_id": "350e7a87-28a5-4645-a659-2daa8e4bbe55",
            "someRef._id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
            "someRef.id": 0,
            "someRef.text": "test0",
        },
        {
            "_id": "72d5dc33-a074-43f0-882f-e06abd34113b",
            "someRef._id": "b574111d-bd2f-4c94-9249-d9a0de49bd5b",
            "someRef.id": 1,
            "someRef.text": "test1",
        },
        {
            "_id": "478be0be-6ab9-4c03-8551-53d881567743",
            "someRef._id": "1686c00c-0c59-413a-aa30-f5605488cc77",
            "someRef.id": 2,
            "someRef.text": "test2",
        },
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
            assert item["someRef.id"] == i
            assert item["someRef.text"] == f"test{i}"
        columns = table.columns
        assert {"someText", "someRef._id", "someRef.id", "someRef.text"}.issubset(columns.keys())

        columns = get_table_foreign_key_constraint_columns(table)
        assert any(
            [["someRef._id"], ["_id"]] == [constraint["column_names"], constraint["referred_column_names"]]
            for constraint in columns
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m      | property            | type          | ref                  | level
     migrate/example |   |   |        |                     |               |                      |
                     |   |   | Test   |                     |               | someText, someNumber |
                     |   |   |        | someText            | string        |                      |
                     |   |   |        | someInteger         | integer       |                      |
                     |   |   |        | someNumber          | number        |                      |
                     |   |   |        |                     |               |                      |
                     |   |   | Ref    |                     |               |                      |
                     |   |   |        | someText            | string        |                      |
                     |   |   |        | someRef             | ref           | Test                 | 3
                     |   |   |        | someRef.id          | integer       |                      | 
                     |   |   |        | someRef.text        | string        |                      | 
                     |   |   |        | someRef.someInteger |               |                      | 
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
        assert {
            "someText",
            "__someRef._id",
            "someRef.someText",
            "someRef.someNumber",
            "someRef.id",
            "someRef.text",
        }.issubset(columns.keys())
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
            assert item["someRef.id"] == i
            assert item["someRef.text"] == f"test{i}"

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m      | property            | type          | ref                  | level
     migrate/example |   |   |        |                     |               |                      |
                     |   |   | Test   |                     |               | someText, someNumber |
                     |   |   |        | someText            | string        |                      |
                     |   |   |        | someInteger         | integer       |                      |
                     |   |   |        | someNumber          | number        |                      |
                     |   |   |        |                     |               |                      |
                     |   |   | Ref    |                     |               |                      |
                     |   |   |        | someText            | string        |                      |
                     |   |   |        | someRef             | ref           | Test                 | 4
                     |   |   |        | someRef.id          | integer       |                      | 
                     |   |   |        | someRef.text        | string        |                      | 
                     |   |   |        | someRef.someInteger |               |                      | 
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
        assert {
            "someText",
            "__someRef._id",
            "someRef._id",
            "__someRef.someText",
            "__someRef.someNumber",
            "someRef.id",
            "someRef.text",
        }.issubset(columns.keys())
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
            assert item["someRef.id"] == i
            assert item["someRef.text"] == f"test{i}"

        cleanup_table_list(
            meta,
            [
                "migrate/example/Test",
                "migrate/example/Test/:changelog",
                "migrate/example/Ref",
                "migrate/example/Ref/:changelog",
            ],
        )


def test_migrate_ref_4_add_denorm(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | ref      | Country  | 4
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
                     |   |      |         | country        | ref      | Country  | 4
                     |   |      |         | country.name   | string   |          |
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | name           | string   |          |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        f"BEGIN;\n\n{add_column(table='migrate/example/City', column='country.name', column_type='TEXT')}COMMIT;\n\n"
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
        assert {"id", "country._id", "country.name"}.issubset(columns.keys())
        assert not {"__country._id"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["id"] == 0
            assert row["country._id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7a"
            assert row["country.name"] is None
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_ref_4_remove_denorm(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | ref      | Country  | 4
                     |   |      |         | country.name   | string   |          |
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
        assert {"id", "country._id", "country.name"}.issubset(city.columns.keys())
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
                    "country.name": "test",
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
                     |   |      |         | name           | string   |          |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        f"BEGIN;\n\n{drop_column(table='migrate/example/City', column='country.name')}COMMIT;\n\n"
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
        assert {"id", "country._id", "__country.name"}.issubset(columns.keys())
        assert not {"__country._id", "country.name"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["id"] == 0
            assert row["country._id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7a"
            assert row["__country.name"] == "test"
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_ref_4_rename_denorm(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | ref      | Country  | 4
                     |   |      |         | country.name   | string   |          |
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
        assert {"id", "country._id", "country.name"}.issubset(city.columns.keys())
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
                    "country.name": "test",
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
                     |   |      |         | country.test   | string   |          |
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | name           | string   |          |
    """,
    )

    rename_file = {
        "migrate/example/City": {
            "country.name": "country.test",
        },
    }
    path = tmp_path / "rename.json"
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-r", path])
    assert result.output.endswith(
        f"BEGIN;\n\n{rename_column(table='migrate/example/City', column='country.name', new_name='country.test')}COMMIT;\n\n"
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
        assert {"id", "country._id", "country.test"}.issubset(columns.keys())
        assert not {"__country._id", "country.name"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["id"] == 0
            assert row["country._id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7a"
            assert row["country.test"] == "test"
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_ref_nesting_do_nothing(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | ref      | Country  | 4
                     |   |      |         | country.ctr    | ref      | Country  | 4
                     |   |      |         | country.ctr.id | integer  |          | 
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
        assert {"id", "country._id", "country.ctr._id", "country.ctr.id"}.issubset(city.columns.keys())
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
                    "country.ctr._id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                    "country.ctr.id": 0,
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
                     |   |      |         | country.ctr    | ref      | Country  | 4
                     |   |      |         | country.ctr.id | integer  |          |
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | name           | string   |          |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith("BEGIN;\n\nCOMMIT;\n\n")

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
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
        assert {"id", "country._id", "country.ctr._id", "country.ctr.id"}.issubset(columns.keys())
        assert not {"__country._id", "__country.ctr._id", "__country.ctr.id"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["id"] == 0
            assert row["country._id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7a"
            assert row["country.ctr._id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7a"
            assert row["country.ctr.id"] == 0
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_ref_nested_update(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | ref      | Country  | 4
                     |   |      |         | country.ctr    | ref      | Country  | 4
                     |   |      |         | country.ctr.id | integer  |          | 
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
        assert {"id", "country._id", "country.ctr._id", "country.ctr.id"}.issubset(city.columns.keys())
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
                    "country.ctr._id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
                    "country.ctr.id": 0,
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
                     |   |      |         | country.ctr    | ref      | Country  | 4
                     |   |      |         | country.ctr.id | string   |          |
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | name           | string   |          |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example/City" ALTER COLUMN "country.ctr.id" TYPE TEXT '
        'USING CAST("migrate/example/City"."country.ctr.id" AS TEXT);\n'
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
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())

        table = tables["migrate/example/City"]
        columns = table.columns
        assert {"id", "country._id", "country.ctr._id", "country.ctr.id"}.issubset(columns.keys())
        assert not {"__country._id", "__country.ctr._id", "__country.ctr.id"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["id"] == 0
            assert row["country._id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7a"
            assert row["country.ctr._id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7a"
            assert row["country.ctr.id"] == "0"
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_ref_nested_ref_to_scalar(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | ref      | Country  | 4
                     |   |      |         | country.ctr    | ref      | Country  | 4
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
        assert {"id", "country._id", "country.ctr._id"}.issubset(city.columns.keys())
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
                    "country.ctr._id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
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
                     |   |      |         | country.ctr    | integer  |          | 
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | name           | string   |          |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_column(table='migrate/example/City', column='country.ctr', column_type='INTEGER')}"
        'UPDATE "migrate/example/City" SET "country.ctr"="migrate/example/Country".id '
        'FROM "migrate/example/Country" WHERE '
        '"migrate/example/City"."country.ctr._id" = "migrate/example/Country"._id;\n'
        "\n"
        f"{drop_column(table='migrate/example/City', column='country.ctr._id')}"
        'DROP INDEX "ix_migrate/example/City_country.ctr._id";\n'
        "\n"
        'ALTER TABLE "migrate/example/City" DROP CONSTRAINT '
        '"fk_migrate/example/City_country.ctr._id";\n'
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
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())

        table = tables["migrate/example/City"]
        columns = table.columns
        assert {"id", "country._id", "country.ctr", "__country.ctr._id"}.issubset(columns.keys())
        assert not {"__country._id", "country.ctr._id"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["id"] == 0
            assert row["country._id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7a"
            assert row["country.ctr"] == 0
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


@pytest.mark.skip("Text does not support nesting (in manifest creation)")
def test_migrate_ref_nested_text(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property            | type     | ref      | level
     migrate/example |   |      |         |                     |          |          |
                     |   |      | City    |                     |          | id       |
                     |   |      |         | id                  | integer  |          |
                     |   |      |         | country             | ref      | Country  | 4
                     |   |      |         | country.ctr         | ref      | Country  | 3
                     |   |      |         | country.ctr.name@lt | string   |          |
                     |   |      |         | country.ctr.name@en | string   |          |
                     |   |      | Country |                     |          | id       |
                     |   |      |         | id                  | integer  |          |
                     |   |      |         | name                | string   |          |
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
        assert {"id", "country._id", "country.ctr.id", "country.ctr.name"}.issubset(city.columns.keys())
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
                    "country.ctr.id": 0,
                    "country.ctr.name": {"lt": "pavadinimas", "en": "name"},
                }
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m       | property            | type     | ref      | level
     migrate/example |   |      |         |                     |          |          |
                     |   |      | City    |                     |          | id       |
                     |   |      |         | id                  | integer  |          |
                     |   |      |         | country             | ref      | Country  | 4
                     |   |      |         | country.ctr         | ref      | Country  | 3
                     |   |      |         | country.ctr.name@lt | string   |          |
                     |   |      |         | country.ctr.name@en | string   |          |
                     |   |      | Country |                     |          | id       |
                     |   |      |         | id                  | integer  |          |
                     |   |      |         | name                | string   |          |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_column(table='migrate/example/City', column='country.ctr', column_type='INTEGER')}"
        'UPDATE "migrate/example/City" SET "country.ctr"="migrate/example/Country".id '
        'FROM "migrate/example/Country" WHERE '
        '"migrate/example/City"."country.ctr._id" = "migrate/example/Country"._id;\n'
        "\n"
        f"{drop_column(table='migrate/example/City', column='country.ctr._id')}"
        'DROP INDEX "ix_migrate/example/City_country.ctr._id";\n'
        "\n"
        'ALTER TABLE "migrate/example/City" DROP CONSTRAINT '
        '"fk_migrate/example/City_country.ctr._id";\n'
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
            "migrate/example/City",
            "migrate/example/City/:changelog",
            "migrate/example/Country",
            "migrate/example/Country/:changelog",
        }.issubset(tables.keys())

        table = tables["migrate/example/City"]
        columns = table.columns
        assert {"id", "country._id", "country.ctr", "__country.ctr._id"}.issubset(columns.keys())
        assert not {"__country._id", "country.ctr._id"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["id"] == 0
            assert row["country._id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7a"
            assert row["country.ctr"] == 0
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_ref_3_add_denorm(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | ref      | Country  | 3
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
        conn.execute(
            city.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "id": 0,
                    "country.id": 0,
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
                     |   |      |         | country.name   | string   |          |
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | name           | string   |          |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        f"BEGIN;\n\n{add_column(table='migrate/example/City', column='country.name', column_type='TEXT')}COMMIT;\n\n"
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
        assert {"id", "country.id", "country.name"}.issubset(columns.keys())
        assert not {"__country.id"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["id"] == 0
            assert row["country.id"] == 0
            assert row["country.name"] is None
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_ref_3_remove_denorm(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | ref      | Country  | 3
                     |   |      |         | country.name   | string   |          |
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
        assert {"id", "country.id", "country.name"}.issubset(city.columns.keys())
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
                {"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "id": 0, "country.id": 0, "country.name": "test"}
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

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        f"BEGIN;\n\n{drop_column(table='migrate/example/City', column='country.name')}COMMIT;\n\n"
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
        assert {"id", "country.id", "__country.name"}.issubset(columns.keys())
        assert not {"__country.id", "country.name"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["id"] == 0
            assert row["country.id"] == 0
            assert row["__country.name"] == "test"
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_ref_3_rename_denorm(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | ref      | Country  | 3
                     |   |      |         | country.name   | string   |          |
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
        assert {"id", "country.id", "country.name"}.issubset(city.columns.keys())
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
                {"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "id": 0, "country.id": 0, "country.name": "test"}
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
                     |   |      |         | country.test   | string   |          |
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | name           | string   |          |
    """,
    )

    rename_file = {
        "migrate/example/City": {
            "country.name": "country.test",
        },
    }
    path = tmp_path / "rename.json"
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-r", path])
    assert result.output.endswith(
        f"BEGIN;\n\n{rename_column(table='migrate/example/City', column='country.name', new_name='country.test')}COMMIT;\n\n"
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
        assert {"id", "country.id", "country.test"}.issubset(columns.keys())
        assert not {"__country.id", "country.name"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["id"] == 0
            assert row["country.id"] == 0
            assert row["country.test"] == "test"
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )


def test_migrate_object(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | object   |          |
                     |   |      |         | country.name   | string   |          |
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
        assert {"id", "country.name"}.issubset(city.columns.keys())
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
            city.insert().values({"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "id": 0, "country.name": "test"})
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m       | property       | type     | ref      | level
     migrate/example |   |      |         |                |          |          |
                     |   |      | City    |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | country        | object   |          |
                     |   |      |         | country.test   | string   |          |
                     |   |      | Country |                |          | id       |
                     |   |      |         | id             | integer  |          |
                     |   |      |         | name           | string   |          |
    """,
    )

    rename_file = {
        "migrate/example/City": {
            "country.name": "country.test",
        },
    }
    path = tmp_path / "rename.json"
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-r", path])
    assert result.output.endswith(
        f"BEGIN;\n\n{rename_column(table='migrate/example/City', column='country.name', new_name='country.test')}COMMIT;\n\n"
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
        assert {"id", "country.test"}.issubset(columns.keys())
        assert not {"country.name"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["id"] == 0
            assert row["country.test"] == "test"
        cleanup_table_list(
            meta,
            [
                "migrate/example/City",
                "migrate/example/City/:changelog",
                "migrate/example/Country",
                "migrate/example/Country/:changelog",
            ],
        )
