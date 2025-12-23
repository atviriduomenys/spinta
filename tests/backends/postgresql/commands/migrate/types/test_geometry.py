from pathlib import Path

import sqlalchemy as sa
from geoalchemy2.shape import to_shape
from sqlalchemy.engine import Engine

from spinta.core.config import RawConfig
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.migration import drop_index
from tests.backends.postgresql.commands.migrate.test_migrations import (
    configure_migrate,
    float_equals,
    override_manifest,
    cleanup_table_list,
)


def test_migrate_modify_geometry_type(migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    initial_manifest = """
     d               | r | b    | m    | property       | type           | ref | source
     migrate/example |   |      |      |                |                |     |
                     |   |      | Test |                |                |     |
                     |   |      |      | someText       | string         |     |
                     |   |      |      | someGeo        | geometry       |     |
                     |   |      |      | someGeoLt      | geometry(3346) |     |
                     |   |      |      | someGeoWorld   | geometry(4326) |     |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example/Test"]
        conn.execute(
            table.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "someText": "Vilnius",
                    "someGeo": "SRID=4326;POINT(15 15)",
                    "someGeoLt": "SRID=3346;POINT(-471246.92725520115 1678519.8837915037)",
                    "someGeoWorld": "SRID=4326;POINT(15 15)",
                }
            )
        )

        result = conn.execute(table.select())
        for item in result:
            assert item["_id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7e"
            assert item["someText"] == "Vilnius"
            some_geo_values = to_shape(item["someGeo"]).wkt[7:-1].split(" ")
            some_geo_lt_values = to_shape(item["someGeoLt"]).wkt[7:-1].split(" ")
            some_geo_world_values = to_shape(item["someGeoWorld"]).wkt[7:-1].split(" ")
            assert float_equals(float(some_geo_values[0]), 15, epsilon=1e-2)
            assert float_equals(float(some_geo_values[1]), 15, epsilon=1e-2)
            assert float_equals(float(some_geo_lt_values[0]), -471246.92725520115, epsilon=1e-2)
            assert float_equals(float(some_geo_lt_values[1]), 1678519.8837915037, epsilon=1e-2)
            assert float_equals(float(some_geo_world_values[0]), 15, epsilon=1e-2)
            assert float_equals(float(some_geo_world_values[1]), 15, epsilon=1e-2)

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type           | ref | source
     migrate/example |   |      |      |                |                |     |
                     |   |      | Test |                |                |     |
                     |   |      |      | someText       | string         |     |
                     |   |      |      | someGeo        | geometry(3346) |     |
                     |   |      |      | someGeoLt      | geometry       |     |
                     |   |      |      | someGeoWorld   | geometry(3346) |     |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someGeo" TYPE '
        'geometry(GEOMETRY,3346) USING ST_Transform("migrate/example/Test"."someGeo", '
        "3346);\n"
        "\n"
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someGeoLt" TYPE '
        "geometry(GEOMETRY,4326) USING "
        'ST_Transform("migrate/example/Test"."someGeoLt", 4326);\n'
        "\n"
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someGeoWorld" TYPE '
        "geometry(GEOMETRY,3346) USING "
        'ST_Transform("migrate/example/Test"."someGeoWorld", 3346);\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables

        table = tables["migrate/example/Test"]

        result = conn.execute(table.select())
        for item in result:
            assert item["_id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7e"
            assert item["someText"] == "Vilnius"
            some_geo_values = to_shape(item["someGeo"]).wkt[7:-1].split(" ")
            some_geo_lt_values = to_shape(item["someGeoLt"]).wkt[7:-1].split(" ")
            some_geo_world_values = to_shape(item["someGeoWorld"]).wkt[7:-1].split(" ")
            assert float_equals(float(some_geo_values[0]), -471246.92725520115, epsilon=1e-2)
            assert float_equals(float(some_geo_values[1]), 1678519.8837915037, epsilon=1e-2)
            assert float_equals(float(some_geo_lt_values[0]), 15, epsilon=1e-2)
            assert float_equals(float(some_geo_lt_values[1]), 15, epsilon=1e-2)
            assert float_equals(float(some_geo_world_values[0]), -471246.92725520115, epsilon=1e-2)
            assert float_equals(float(some_geo_world_values[1]), 1678519.8837915037, epsilon=1e-2)

        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])


def test_migrate_geometry_to_string_to_geometry(
    migration_db: Engine, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    initial_manifest = """
     d               | r | b    | m    | property       | type           | ref | source
     migrate/example |   |      |      |                |                |     |
                     |   |      | Test |                |                |     |
                     |   |      |      | someText       | string         |     |
                     |   |      |      | someGeo        | geometry(3346) |     |
    """
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

    cli.invoke(rc, ["bootstrap", f"{tmp_path}/manifest.csv"])

    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {"migrate/example/Test", "migrate/example/Test/:changelog"}.issubset(tables.keys())
        table = tables["migrate/example/Test"]
        conn.execute(
            table.insert().values(
                {
                    "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
                    "someText": "Vilnius",
                    "someGeo": "SRID=3346;POINT(-471246.92725520115 1678519.8837915037)",
                }
            )
        )

        result = conn.execute(table.select())
        for item in result:
            assert item["_id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7e"
            assert item["someText"] == "Vilnius"
            some_geo_lt_values = to_shape(item["someGeo"]).wkt[7:-1].split(" ")
            assert float_equals(float(some_geo_lt_values[0]), -471246.92725520115, epsilon=1e-2)
            assert float_equals(float(some_geo_lt_values[1]), 1678519.8837915037, epsilon=1e-2)

    override_manifest(
        context,
        tmp_path,
        """
     d               | r | b    | m    | property       | type           | ref | source
     migrate/example |   |      |      |                |                |     |
                     |   |      | Test |                |                |     |
                     |   |      |      | someText       | string         |     |
                     |   |      |      | someGeo        | string         |     |
    """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someGeo" TYPE TEXT USING '
        'CAST("migrate/example/Test"."someGeo" AS TEXT);\n'
        "\n"
        f"{drop_index(index_name='ix_migrate/example/Test_someGeo')}"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables

        table = tables["migrate/example/Test"]

        result = conn.execute(table.select())
        for item in result:
            assert item["_id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7e"
            assert item["someText"] == "Vilnius"
            assert item["someGeo"] == "0101000020120D0000306382B53BC31CC1F52840E2B79C3941"

    override_manifest(
        context,
        tmp_path,
        """
         d               | r | b    | m    | property       | type           | ref | source
         migrate/example |   |      |      |                |                |     |
                         |   |      | Test |                |                |     |
                         |   |      |      | someText       | string         |     |
                         |   |      |      | someGeo        | geometry(3346) |     |
        """,
    )

    result = cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv", "-p"])
    assert result.output.endswith(
        "BEGIN;\n"
        "\n"
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someGeo" TYPE '
        'geometry(GEOMETRY,3346) USING CAST("migrate/example/Test"."someGeo" AS '
        "geometry(GEOMETRY,3346));\n"
        "\n"
        'CREATE INDEX "ix_migrate/example/Test_someGeo" ON "migrate/example/Test" '
        'USING gist ("someGeo");\n'
        "\n"
        "COMMIT;\n"
        "\n"
    )

    cli.invoke(rc, ["migrate", f"{tmp_path}/manifest.csv"])
    with migration_db.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables

        table = tables["migrate/example/Test"]

        result = conn.execute(table.select())
        for item in result:
            assert item["_id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7e"
            assert item["someText"] == "Vilnius"
            some_geo_lt_values = to_shape(item["someGeo"]).wkt[7:-1].split(" ")
            assert float_equals(float(some_geo_lt_values[0]), -471246.92725520115, epsilon=1e-2)
            assert float_equals(float(some_geo_lt_values[1]), 1678519.8837915037, epsilon=1e-2)

        cleanup_table_list(meta, ["migrate/example/Test", "migrate/example/Test/:changelog"])
