import datetime
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.engine.url import URL

from spinta.core.config import RawConfig
from spinta.exceptions import UnableToCastColumnTypes
from spinta.testing.cli import SpintaCliRunner
from tests.backends.postgresql.commands.migrate.test_migrations import (
    configure_migrate,
    cleanup_tables,
    override_manifest,
    cleanup_table_list,
)


def test_migrate_date_to_datetime(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b | m    | property | type | ref | source
     migrate/example |   |   |      |          |      |     |
                     |   |   | Test |          |      |     |
                     |   |   |      | someDate | date | D   |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example/Test"]
        conn.execute(
            table.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "someDate": "2020-01-01",
                }
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m    | property | type     | ref | source
     migrate/example |   |   |      |          |          |     |
                     |   |   | Test |          |          |     |
                     |   |   |      | someDate | datetime | D   |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someDate" TYPE TIMESTAMP '
        'WITHOUT TIME ZONE USING CAST("migrate/example/Test"."someDate" AS TIMESTAMP '
        "WITHOUT TIME ZONE);\n"
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "--raise"])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables

        table = tables["migrate/example/Test"]

        result = conn.execute(table.select())
        for item in result:
            assert item["_id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7e"
            assert item["someDate"] == datetime.datetime(2020, 1, 1)
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_date_to_time_error(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b | m    | property | type | ref | source
     migrate/example |   |   |      |          |      |     |
                     |   |   | Test |          |      |     |
                     |   |   |      | someDate | date | D   |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example/Test"]
        conn.execute(
            table.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "someDate": "2020-01-01",
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
                     |   |   |      | someDate | time |     |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someDate" TYPE TIME WITHOUT '
        'TIME ZONE USING CAST("migrate/example/Test"."someDate" AS TIME WITHOUT TIME '
        "ZONE);\n"
        "\n"
        "COMMIT;\n"
        "\n"
    )

    with pytest.raises(UnableToCastColumnTypes):
        result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "--raise"], fail=False)
        raise result.exception

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_datetime_to_date(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b | m    | property | type     | ref | source
     migrate/example |   |   |      |          |          |     |
                     |   |   | Test |          |          |     |
                     |   |   |      | someDate | datetime | D   |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example/Test"]
        conn.execute(
            table.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "someDate": "2020-01-01T10:20:30",
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

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "--raise"])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables

        table = tables["migrate/example/Test"]

        result = conn.execute(table.select())
        for item in result:
            assert item["_id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7e"
            assert item["someDate"] == datetime.date(2020, 1, 1)
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_datetime_to_time(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b | m    | property | type     | ref | source
     migrate/example |   |   |      |          |          |     |
                     |   |   | Test |          |          |     |
                     |   |   |      | someDate | datetime | D   |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example/Test"]
        conn.execute(
            table.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "someDate": "2020-01-01T10:20:30",
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
                     |   |   |      | someDate | time |     |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someDate" TYPE TIME WITHOUT '
        'TIME ZONE USING CAST("migrate/example/Test"."someDate" AS TIME WITHOUT TIME '
        "ZONE);\n"
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "--raise"])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables

        table = tables["migrate/example/Test"]

        result = conn.execute(table.select())
        for item in result:
            assert item["_id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7e"
            assert item["someDate"] == datetime.time(10, 20, 30)
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_time_to_date_error(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b | m    | property | type | ref | source
     migrate/example |   |   |      |          |      |     |
                     |   |   | Test |          |      |     |
                     |   |   |      | someDate | time | D   |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example/Test"]
        conn.execute(
            table.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "someDate": "10:20:30",
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

    with pytest.raises(UnableToCastColumnTypes):
        result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "--raise"], fail=False)
        raise result.exception

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_time_to_datetime_error(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)
    initial_manifest = """
     d               | r | b | m    | property | type | ref | source
     migrate/example |   |   |      |          |      |     |
                     |   |   | Test |          |      |     |
                     |   |   |      | someDate | time | D   |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example/Test"]
        conn.execute(
            table.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "someDate": "10:20:30",
                }
            )
        )

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b | m    | property | type     | ref | source
     migrate/example |   |   |      |          |          |     |
                     |   |   | Test |          |          |     |
                     |   |   |      | someDate | datetime |     |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someDate" TYPE TIMESTAMP '
        'WITHOUT TIME ZONE USING CAST("migrate/example/Test"."someDate" AS TIMESTAMP '
        "WITHOUT TIME ZONE);\n"
        "\n"
        "COMMIT;\n"
        "\n"
    )

    with pytest.raises(UnableToCastColumnTypes):
        result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "--raise"], fail=False)
        raise result.exception

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])
