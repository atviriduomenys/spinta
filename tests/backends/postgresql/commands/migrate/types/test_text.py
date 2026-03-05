import json
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.engine.url import URL

from spinta.core.config import RawConfig
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.migration import drop_column, add_column, add_column_comment, rename_column
from tests.backends.postgresql.commands.migrate.test_migrations import (
    cleanup_tables,
    override_manifest,
    cleanup_table_list,
    configure_migrate,
)


def test_migrate_text_full_remove(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
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

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | new            | string   |          |
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
        f"{add_column(table='migrate/example/Test', column='new', column_type='TEXT')}"
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
        assert {"new", "__text"}.issubset(columns.keys())
        assert not {"text"}.issubset(columns.keys())

        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_string_to_text(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text_lt        | string   |          |
                     |   |      |      | text_en        | string   |          |
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
        assert {"text_lt", "text_en"}.issubset(columns.keys())

        conn.execute(
            table.insert().values({"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "text_lt": "LT", "text_en": "EN"})
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text@lt        | string   |          |
                     |   |      |      | text@en        | string   |          |
    """,
    )

    rename_file = {
        "migrate/example/Test": {"text_lt": "text@lt", "text_en": "text@en"},
    }
    path = tmp_path / "rename.json"
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-r", path])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        "ALTER TABLE \"migrate/example/Test\" ADD COLUMN text JSONB DEFAULT '{}' NOT "
        "NULL;\n"
        "\n"
        f"{add_column_comment(table='migrate/example/Test', column='text')}"
        'UPDATE "migrate/example/Test" SET text=("migrate/example/Test".text || '
        "jsonb_build_object('lt', \"migrate/example/Test\".text_lt));\n"
        "\n"
        f"{drop_column(table='migrate/example/Test', column='text_lt')}"
        'UPDATE "migrate/example/Test" SET text=("migrate/example/Test".text || '
        "jsonb_build_object('en', \"migrate/example/Test\".text_en));\n"
        "\n"
        f"{drop_column(table='migrate/example/Test', column='text_en')}"
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
        assert {"text", "__text_lt", "__text_en"}.issubset(columns.keys())
        assert not {"__text", "text_lt", "text_en"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["text"] == {"en": "EN", "lt": "LT"}
            assert row["__text_lt"] == "LT"
            assert row["__text_en"] == "EN"
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_string_to_text_add_additional(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text_lt        | string   |          |
                     |   |      |      | text_en        | string   |          |
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
        assert {"text_lt", "text_en", "text"}.issubset(columns.keys())

        conn.execute(
            table.insert().values(
                {"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "text_lt": "LT", "text_en": "EN", "text": {"lv": "LV"}}
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text@lt        | string   |          |
                     |   |      |      | text@en        | string   |          |
                     |   |      |      | text@lv        | string   |          |
    """,
    )

    rename_file = {
        "migrate/example/Test": {"text_lt": "text@lt", "text_en": "text@en"},
    }
    path = tmp_path / "rename.json"
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-r", path])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'UPDATE "migrate/example/Test" SET text=("migrate/example/Test".text || '
        "jsonb_build_object('lt', \"migrate/example/Test\".text_lt));\n"
        "\n"
        f"{drop_column(table='migrate/example/Test', column='text_lt')}"
        'UPDATE "migrate/example/Test" SET text=("migrate/example/Test".text || '
        "jsonb_build_object('en', \"migrate/example/Test\".text_en));\n"
        "\n"
        f"{drop_column(table='migrate/example/Test', column='text_en')}"
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
        assert {"text", "__text_lt", "__text_en"}.issubset(columns.keys())
        assert not {"__text", "text_lt", "text_en"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["text"] == {"en": "EN", "lt": "LT", "lv": "LV"}
            assert row["__text_lt"] == "LT"
            assert row["__text_en"] == "EN"
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_string_to_text_rename(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text_lt        | string   |          |
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
        assert {"text_lt", "text"}.issubset(columns.keys())

        conn.execute(
            table.insert().values(
                {"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "text_lt": "LT", "text": {"lt": "test"}}
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text@lt        | string   |          |
    """,
    )

    rename_file = {
        "migrate/example/Test": {
            "text_lt": "text@lt",
        },
    }
    path = tmp_path / "rename.json"
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-r", path])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'UPDATE "migrate/example/Test" SET text=("migrate/example/Test".text - \'lt\' '
        "|| jsonb_build_object('__lt', (\"migrate/example/Test\".text -> 'lt'))) "
        "WHERE \"migrate/example/Test\".text ? 'lt';\n"
        "\n"
        'UPDATE "migrate/example/Test" SET text=("migrate/example/Test".text || '
        "jsonb_build_object('lt', \"migrate/example/Test\".text_lt));\n"
        "\n"
        f"{drop_column(table='migrate/example/Test', column='text_lt')}"
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
        assert {"text", "__text_lt"}.issubset(columns.keys())
        assert not {"__text", "text_lt"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["text"] == {"lt": "LT", "__lt": "test"}
            assert row["__text_lt"] == "LT"
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_string_to_text_advanced_with_rename(
    postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text_lt        | string   |          |
                     |   |      |      | text@lt        | string   |          |
                     |   |      |      | text@en        | string   |          |
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
        assert {"text_lt", "text"}.issubset(columns.keys())

        conn.execute(
            table.insert().values(
                {"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "text_lt": "LT", "text": {"lt": "test", "en": "EN"}}
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | other@lt       | string   |          |
    """,
    )

    rename_file = {
        "migrate/example/Test": {"text_lt": "other@lt", "text": "other"},
    }
    path = tmp_path / "rename.json"
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-r", path])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'UPDATE "migrate/example/Test" SET text=("migrate/example/Test".text - \'lt\' '
        "|| jsonb_build_object('__lt', (\"migrate/example/Test\".text -> 'lt'))) "
        "WHERE \"migrate/example/Test\".text ? 'lt';\n"
        "\n"
        'UPDATE "migrate/example/Test" SET text=("migrate/example/Test".text || '
        "jsonb_build_object('lt', \"migrate/example/Test\".text_lt));\n"
        "\n"
        f"{drop_column(table='migrate/example/Test', column='text_lt')}"
        'UPDATE "migrate/example/Test" SET text=("migrate/example/Test".text - \'en\' '
        "|| jsonb_build_object('__en', (\"migrate/example/Test\".text -> 'en'))) "
        "WHERE \"migrate/example/Test\".text ? 'en';\n"
        "\n"
        f"{rename_column(table='migrate/example/Test', column='text', new_name='other')}"
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
        assert {"other", "__text_lt"}.issubset(columns.keys())
        assert not {"__text", "text_lt"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["other"] == {"__lt": "test", "lt": "LT", "__en": "EN"}
            assert row["__text_lt"] == "LT"
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_text_add_empty(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text@lt        | string   |          |
                     |   |      |      | text@en        | string   |          |
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
            table.insert().values({"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "text": {"lt": "LT", "en": "EN"}})
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text@lt        | string   |          |
                     |   |      |      | text@en        | string   |          |
                     |   |      |      | text@lv        | string   |          |
    """,
    )

    result = cli.invoke(
        rc,
        [
            "migrate",
            f"{tmp_path}/manifest.csv",
            "-p",
        ],
    )
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'UPDATE "migrate/example/Test" SET text=("migrate/example/Test".text || '
        "jsonb_build_object('lv', NULL));\n"
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
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {"text"}.issubset(columns.keys())
        assert not {"__text"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["text"] == {"lt": "LT", "en": "EN", "lv": None}
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_text_rename_lang(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
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
                {"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "text": {"lt": "LT", "en": "EN", "lv": "LV"}}
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text@lt        | string   |          |
                     |   |      |      | text@en        | string   |          |
                     |   |      |      | text@sp        | string   |          |
    """,
    )

    rename_file = {
        "migrate/example/Test": {
            "text@lv": "text@sp",
        },
    }
    path = tmp_path / "rename.json"
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p", "-r", path])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'UPDATE "migrate/example/Test" SET text=("migrate/example/Test".text - \'lv\' '
        "|| jsonb_build_object('sp', (\"migrate/example/Test\".text -> 'lv'))) "
        "WHERE \"migrate/example/Test\".text ? 'lv';\n"
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
        assert {"text"}.issubset(columns.keys())
        assert not {"__text"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["text"] == {"lt": "LT", "en": "EN", "sp": "LV"}
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_text_remove_lang(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
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
                {"_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e", "text": {"lt": "LT", "en": "EN", "lv": "LV"}}
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text@lt        | string   |          |
                     |   |      |      | text@en        | string   |          |
    """,
    )

    result = cli.invoke(
        rc,
        [
            "migrate",
            f"{tmp_path}/manifest.csv",
            "-p",
        ],
    )
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'UPDATE "migrate/example/Test" SET text=("migrate/example/Test".text - \'lv\' '
        "|| jsonb_build_object('__lv', (\"migrate/example/Test\".text -> 'lv'))) "
        "WHERE \"migrate/example/Test\".text ? 'lv';\n"
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
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {"text"}.issubset(columns.keys())
        assert not {"__text"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["text"] == {"lt": "LT", "en": "EN", "__lv": "LV"}
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_multi_text_do_nothing(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text@lt        | string   |          |
                     |   |      |      | text@en        | string   |          |
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
                    "text": {
                        "lt": "LT",
                        "en": "EN",
                    },
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
                     |   |      |      | text@lt        | string   |          |
                     |   |      |      | text@en        | string   |          |
    """,
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
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {"text"}.issubset(columns.keys())
        assert not {"__text"}.issubset(columns.keys())

        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_empty_text_do_nothing(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text@lt        | string   |          |
                     |   |      |      | text@en        | string   |          |
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

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text@lt        | string   |          |
                     |   |      |      | text@en        | string   |          |
    """,
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
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())

        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {"text"}.issubset(columns.keys())
        assert not {"__text"}.issubset(columns.keys())

        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])
