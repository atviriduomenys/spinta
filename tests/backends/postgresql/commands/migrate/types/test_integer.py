import uuid
from pathlib import Path

import psycopg2
import pytest
import sqlalchemy as sa
from sqlalchemy.engine.url import URL

from spinta.core.config import RawConfig
from spinta.testing.cli import SpintaCliRunner
from tests.backends.postgresql.commands.migrate.test_migrations import (
    cleanup_tables,
    configure_migrate,
)


def test_migrate_custom_big_integer(postgresql_migration: URL, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    cleanup_tables(postgresql_migration)

    initial_manifest = """
     d               | r | b    | m    | property | type    | ref      | level
     migrate/example |   |      |      |          |         |          |
                     |   |      | Test |          |         | id       |
                     |   |      |      | text     | string  |          |
                     |   |      |      | id       | integer |          |
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
        assert {"text", "id"}.issubset(columns.keys())

        conn.execute(
            table.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "text": "Test",
                    "id": 50,
                }
            )
        )
        with pytest.raises(sa.exc.DataError) as err_info:
            conn.execute(table.insert().values(**{"_id": str(uuid.uuid4()), "id": 3_000_000_000}))
        assert isinstance(err_info.value.orig, psycopg2.errors.NumericValueOutOfRange)

    rc_updated = rc.fork(
        {
            "models": {
                "migrate/example/Test": {
                    "properties": {
                        "id": {
                            "type": "sqlalchemy.types.BigInteger",
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
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN id TYPE BIGINT USING '
        'CAST("migrate/example/Test".id AS BIGINT);\n'
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
        assert {"text", "id"}.issubset(columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row["text"] == "Test"
            assert row["id"] == 50

        conn.execute(table.insert().values(**{"_id": str(uuid.uuid4()), "id": 3_000_000_000}))
