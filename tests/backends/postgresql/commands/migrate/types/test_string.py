import json
import uuid
from pathlib import Path

import pytest
import sqlalchemy as sa
from psycopg2.errors import StringDataRightTruncation
from sqlalchemy.engine.url import URL

from spinta.core.config import RawConfig
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.migration import add_column, drop_column
from tests.backends.postgresql.commands.migrate.test_migrations import (
    cleanup_tables,
    override_manifest,
    cleanup_table_list,
    configure_migrate,
)


def test_migrate_text_to_string_simple(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text@lt        | string   |          |
                     |   |      |      | other@lt       | string   |          |
                     |   |      |      | other@en       | string   |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {"text", "other"}.issubset(columns.keys())

        conn.execute(
            table.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "text": {"lt": "Test"},
                    "other": {"lt": "Testas", "en": "Test"},
                }
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text_lt        | string   |          |
                     |   |      |      | other_lt       | string   |          |
    """,
    )

    rename_file = {
        "migrate/example/Test": {"text@lt": "text_lt", "other@lt": "other_lt"},
    }
    path = tmp_path / "rename.json"
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-r", path])

    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_column(table='migrate/example/Test', column='text_lt', column_type='TEXT')}"
        'UPDATE "migrate/example/Test" SET text_lt=("migrate/example/Test".text ->> '
        "'lt');\n"
        "\n"
        f"{add_column(table='migrate/example/Test', column='other_lt', column_type='TEXT')}"
        'UPDATE "migrate/example/Test" SET other_lt=("migrate/example/Test".other ->> '
        "'lt');\n"
        "\n"
        f"{drop_column(table='migrate/example/Test', column='text')}"
        f"{drop_column(table='migrate/example/Test', column='other')}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-r", path])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {"text_lt", "__text", "other_lt", "__other"}.issubset(columns.keys())
        assert not {"text", "other"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["__text"] == {"lt": "Test"}
            assert row["__other"] == {"lt": "Testas", "en": "Test"}
            assert row["text_lt"] == "Test"
            assert row["other_lt"] == "Testas"
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_text_to_string_direct(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text@lt        | string   |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {"text"}.issubset(columns.keys())

        conn.execute(
            table.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "text": {"lt": "Testas", "en": "Test"},
                }
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text           | string   |          |
    """,
    )

    rename_file = {
        "migrate/example/Test": {},
    }
    path = tmp_path / "rename.json"
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-r", path])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{drop_column(table='migrate/example/Test', column='text')}"
        f"{add_column(table='migrate/example/Test', column='text', column_type='TEXT')}"
        "UPDATE \"migrate/example/Test\" SET text=(__text ->> 'lt');\n"
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-r", path])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {"text", "__text"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["__text"] == {"lt": "Testas", "en": "Test"}
            assert row["text"] == "Testas"
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_text_to_string_multi(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text@lt        | string   |          |
                     |   |      |      | text@en        | string   |          |
                     |   |      |      | text@lv        | string   |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {"text"}.issubset(columns.keys())

        conn.execute(
            table.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "text": {"lt": "LT", "en": "EN", "lv": "LV"},
                }
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text_lt        | string   |          |
                     |   |      |      | text_en        | string   |          |
                     |   |      |      | text_lv        | string   |          |
    """,
    )

    rename_file = {
        "migrate/example/Test": {
            "text@lt": "text_lt",
            "text@en": "text_en",
            "text@lv": "text_lv",
        },
    }
    path = tmp_path / "rename.json"
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-r", path])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_column(table='migrate/example/Test', column='text_lt', column_type='TEXT')}"
        'UPDATE "migrate/example/Test" SET text_lt=("migrate/example/Test".text ->> '
        "'lt');\n"
        "\n"
        f"{add_column(table='migrate/example/Test', column='text_en', column_type='TEXT')}"
        'UPDATE "migrate/example/Test" SET text_en=("migrate/example/Test".text ->> '
        "'en');\n"
        "\n"
        f"{add_column(table='migrate/example/Test', column='text_lv', column_type='TEXT')}"
        'UPDATE "migrate/example/Test" SET text_lv=("migrate/example/Test".text ->> '
        "'lv');\n"
        "\n"
        f"{drop_column(table='migrate/example/Test', column='text')}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-r", path])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {"text_lt", "text_en", "text_lv", "__text"}.issubset(columns.keys())
        assert not {"text"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["__text"] == {"lt": "LT", "en": "EN", "lv": "LV"}
            assert row["text_lt"] == "LT"
            assert row["text_lv"] == "LV"
            assert row["text_en"] == "EN"
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_text_to_string_multi_individual(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text@lt        | string   |          |
                     |   |      |      | text@en        | string   |          |
                     |   |      |      | text@lv        | string   |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {"text"}.issubset(columns.keys())

        conn.execute(
            table.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "text": {"lt": "LT", "en": "EN", "lv": "LV"},
                }
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text_lt        | string   |          |
                     |   |      |      | text@en        | string   |          |
                     |   |      |      | text@lv        | string   |          |
    """,
    )

    rename_file = {
        "migrate/example/Test": {
            "text@lt": "text_lt",
        },
    }
    path = tmp_path / "rename.json"
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-r", path])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        f"{add_column(table='migrate/example/Test', column='text_lt', column_type='TEXT')}"
        'UPDATE "migrate/example/Test" SET text_lt=("migrate/example/Test".text ->> '
        "'lt');\n"
        "\n"
        'UPDATE "migrate/example/Test" SET text=("migrate/example/Test".text - \'lt\' '
        "|| jsonb_build_object('__lt', (\"migrate/example/Test\".text -> 'lt'))) "
        "WHERE \"migrate/example/Test\".text ? 'lt';\n"
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-r", path])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {"text_lt", "text"}.issubset(columns.keys())
        assert not {"__text"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["text"] == {"__lt": "LT", "en": "EN", "lv": "LV"}
            assert row["text_lt"] == "LT"
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_string_custom_length(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)

    initial_manifest = """
     d               | r | b    | m    | property | type   | ref      | level
     migrate/example |   |      |      |          |        |          |
                     |   |      | Test |          |        |          |
                     |   |      |      | text     | string |          |
                     |   |      |      | other    | string |          |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {"text", "other"}.issubset(columns.keys())

        conn.execute(
            table.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "text": "Test",
                    "other": "Testas",
                }
            )
        )
    rc_updated = rc.fork(
        {
            "models": {
                "migrate/example/Test": {
                    "properties": {
                        "text": {
                            "type": {
                                "name": "sqlalchemy.types.String",
                                "length": 10,
                            }
                        }
                    }
                }
            }
        }
    )

    result = cli.invoke(rc_updated, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN text TYPE VARCHAR(10) USING '
        'CAST("migrate/example/Test".text AS VARCHAR(10));\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc_updated, ["migrate", f"{tmp_path}/manifest.csv"])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {"text", "other"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["text"] == "Test"
            assert row["other"] == "Testas"

        with pytest.raises(sa.exc.DataError) as err_info:
            conn.execute(table.insert().values(**{"_id": str(uuid.uuid4()), "text": "12345678910111213"}))
        assert isinstance(err_info.value.orig, StringDataRightTruncation)
