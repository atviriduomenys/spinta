from pathlib import Path

import sqlalchemy as sa
from _pytest.fixtures import FixtureRequest

from spinta.cli.helpers.upgrade.components import Script
from spinta.cli.helpers.script.components import ScriptStatus
from spinta.cli.helpers.script.helpers import script_check_status_message
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.tabular import create_tabular_manifest


def test_upgrade_postgresql_schemas_pass(
    context: Context,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    cli: SpintaCliRunner,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(
            """
        d | r | b | m | property   | type     | ref     | prepare | access | level
        datasets/schemas/cli       |          |         |         |        |
          |   |   | Country        |          | id      |         |        |
          |   |   |   | id         | integer  |         |         | open   |
          |   |   |   | name       | string   |         |         | open   |
          |   |   | City           |          | id      |         |        |
          |   |   |   | id         | integer  |         |         | open   |
          |   |   |   | name       | string   |         |         | open   |
          |   |   |   | country    | ref      | Country |         | open   |
        datasets/schemas/rand/very/long/dataset/name/that/will/compress/no/matter/what |          |         |         |        |
          |   |   | Random         |          | id      |         |        |
          |   |   |   | id         | integer  |         |         | open   |
          |   |   |   | name       | string   |         |         | open   |
          |   |   |   | img        | file     |         |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )

    store = context.get("store")
    backend = store.manifest.backend
    insp = sa.inspect(backend.engine)
    assert {
        "public",
        "datasets/schemas/cli",
        "datasets/schemas/rand/very/long/datas_99a40e82_s/no/matter/what",
    }.issubset(insp.get_schema_names())

    assert {
        "Country",
        "Country/:changelog",
        "Country/:redirect",
        "City",
        "City/:changelog",
        "City/:redirect",
    }.issubset(insp.get_table_names(schema="datasets/schemas/cli"))

    assert {
        "Random",
        "Random/:changelog",
        "Random/:redirect",
    }.issubset(insp.get_table_names(schema="datasets/schemas/rand/very/long/datas_99a40e82_s/no/matter/what"))

    result = cli.invoke(context.get("rc"), ["upgrade", Script.POSTGRESQL_SCHEMAS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.POSTGRESQL_SCHEMAS.value, ScriptStatus.PASSED) in result.stderr


def test_upgrade_postgresql_schemas_required_table(
    context: Context,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    cli: SpintaCliRunner,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(
            """
        d | r | b | m | property   | type     | ref     | prepare | access | level
        datasets/schemas/cli       |          |         |         |        |
          |   |   | Corrupted      |          | id      |         |        |
          |   |   |   | id         | integer  |         |         | open   |
          |   |   |   | name       | string   |         |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )

    store = context.get("store")
    backend = store.manifest.backend
    with backend.begin() as conn:
        conn.execute(
            sa.text('ALTER TABLE "datasets/schemas/cli"."Corrupted" RENAME TO "datasets/schemas/cli/Corrupted"')
        )
        # Ensure that no cached tables exist in public
        conn.execute(sa.text('DROP TABLE IF EXISTS "datasets/schemas/cli/Corrupted"'))
        conn.execute(sa.text('ALTER TABLE "datasets/schemas/cli"."datasets/schemas/cli/Corrupted" SET SCHEMA public'))

    insp = sa.inspect(backend.engine)
    assert {
        "public",
        "datasets/schemas/cli",
    }.issubset(insp.get_schema_names())

    assert {
        "Corrupted/:changelog",
        "Corrupted/:redirect",
    }.issubset(insp.get_table_names(schema="datasets/schemas/cli"))

    assert {
        "datasets/schemas/cli/Corrupted",
    }.issubset(insp.get_table_names())

    assert not {
        "Corrupted",
    }.issubset(insp.get_table_names(schema="datasets/schemas/cli"))

    result = cli.invoke(context.get("rc"), ["upgrade", Script.POSTGRESQL_SCHEMAS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.POSTGRESQL_SCHEMAS.value, ScriptStatus.REQUIRED) in result.stderr

    insp = sa.inspect(backend.engine)
    assert {
        "Corrupted",
        "Corrupted/:changelog",
        "Corrupted/:redirect",
    }.issubset(insp.get_table_names(schema="datasets/schemas/cli"))

    assert not {
        "datasets/schemas/cli/Corrupted",
    }.issubset(insp.get_table_names())


def test_upgrade_postgresql_schemas_required_changelog(
    context: Context,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    cli: SpintaCliRunner,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(
            """
        d | r | b | m | property   | type     | ref     | prepare | access | level
        datasets/schemas/cli       |          |         |         |        |
          |   |   | Corrupted      |          | id      |         |        |
          |   |   |   | id         | integer  |         |         | open   |
          |   |   |   | name       | string   |         |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )

    store = context.get("store")
    backend = store.manifest.backend
    with backend.begin() as conn:
        conn.execute(
            'ALTER TABLE "datasets/schemas/cli"."Corrupted/:changelog" RENAME TO "datasets/schemas/cli/Corrupted/:changelog"'
        )
        # Ensure that no cached tables exist in public
        conn.execute('DROP TABLE IF EXISTS "datasets/schemas/cli/Corrupted/:changelog"')
        conn.execute('ALTER TABLE "datasets/schemas/cli"."datasets/schemas/cli/Corrupted/:changelog" SET SCHEMA public')

    insp = sa.inspect(backend.engine)
    assert {
        "public",
        "datasets/schemas/cli",
    }.issubset(insp.get_schema_names())

    assert {
        "Corrupted",
        "Corrupted/:redirect",
    }.issubset(insp.get_table_names(schema="datasets/schemas/cli"))

    assert {
        "datasets/schemas/cli/Corrupted/:changelog",
    }.issubset(insp.get_table_names())

    assert not {
        "Corrupted/:changelog",
    }.issubset(insp.get_table_names(schema="datasets/schemas/cli"))

    result = cli.invoke(context.get("rc"), ["upgrade", Script.POSTGRESQL_SCHEMAS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.POSTGRESQL_SCHEMAS.value, ScriptStatus.REQUIRED) in result.stderr

    insp = sa.inspect(backend.engine)
    assert {
        "Corrupted",
        "Corrupted/:changelog",
        "Corrupted/:redirect",
    }.issubset(insp.get_table_names(schema="datasets/schemas/cli"))

    assert not {
        "datasets/schemas/cli/Corrupted/:changelog",
    }.issubset(insp.get_table_names())


def test_upgrade_postgresql_schemas_required_redirect(
    context: Context,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    cli: SpintaCliRunner,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(
            """
        d | r | b | m | property   | type     | ref     | prepare | access | level
        datasets/schemas/cli       |          |         |         |        |
          |   |   | Corrupted      |          | id      |         |        |
          |   |   |   | id         | integer  |         |         | open   |
          |   |   |   | name       | string   |         |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )

    store = context.get("store")
    backend = store.manifest.backend
    with backend.begin() as conn:
        conn.execute(
            'ALTER TABLE "datasets/schemas/cli"."Corrupted/:redirect" RENAME TO "datasets/schemas/cli/Corrupted/:redirect"'
        )
        # Ensure that no cached tables exist in public
        conn.execute('DROP TABLE IF EXISTS "datasets/schemas/cli/Corrupted/:redirect"')
        conn.execute('ALTER TABLE "datasets/schemas/cli"."datasets/schemas/cli/Corrupted/:redirect" SET SCHEMA public')

    insp = sa.inspect(backend.engine)
    assert {
        "public",
        "datasets/schemas/cli",
    }.issubset(insp.get_schema_names())

    assert {
        "Corrupted",
        "Corrupted/:changelog",
    }.issubset(insp.get_table_names(schema="datasets/schemas/cli"))

    assert {
        "datasets/schemas/cli/Corrupted/:redirect",
    }.issubset(insp.get_table_names())

    assert not {
        "Corrupted/:redirect",
    }.issubset(insp.get_table_names(schema="datasets/schemas/cli"))

    result = cli.invoke(context.get("rc"), ["upgrade", Script.POSTGRESQL_SCHEMAS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.POSTGRESQL_SCHEMAS.value, ScriptStatus.REQUIRED) in result.stderr

    insp = sa.inspect(backend.engine)
    assert {
        "Corrupted",
        "Corrupted/:changelog",
        "Corrupted/:redirect",
    }.issubset(insp.get_table_names(schema="datasets/schemas/cli"))

    assert not {
        "datasets/schemas/cli/Corrupted/:redirect",
    }.issubset(insp.get_table_names())


def test_upgrade_postgresql_schemas_required_file(
    context: Context,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    cli: SpintaCliRunner,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(
            """
        d | r | b | m | property   | type     | ref     | prepare | access | level
        datasets/schemas/cli       |          |         |         |        |
          |   |   | Corrupted      |          | id      |         |        |
          |   |   |   | id         | integer  |         |         | open   |
          |   |   |   | name       | string   |         |         | open   |
          |   |   |   | data       | file     |         |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )

    store = context.get("store")
    backend = store.manifest.backend
    with backend.begin() as conn:
        conn.execute(
            'ALTER TABLE "datasets/schemas/cli"."Corrupted/:file/data" RENAME TO "datasets/schemas/cli/Corrupted/:file/data"'
        )
        # Ensure that no cached tables exist in public
        conn.execute('DROP TABLE IF EXISTS "datasets/schemas/cli/Corrupted/:file/data"')
        conn.execute('ALTER TABLE "datasets/schemas/cli"."datasets/schemas/cli/Corrupted/:file/data" SET SCHEMA public')

    insp = sa.inspect(backend.engine)
    assert {
        "public",
        "datasets/schemas/cli",
    }.issubset(insp.get_schema_names())

    assert {
        "Corrupted",
        "Corrupted/:changelog",
        "Corrupted/:redirect",
    }.issubset(insp.get_table_names(schema="datasets/schemas/cli"))

    assert {
        "datasets/schemas/cli/Corrupted/:file/data",
    }.issubset(insp.get_table_names())

    assert not {
        "Corrupted/:file/data",
    }.issubset(insp.get_table_names(schema="datasets/schemas/cli"))

    result = cli.invoke(context.get("rc"), ["upgrade", Script.POSTGRESQL_SCHEMAS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.POSTGRESQL_SCHEMAS.value, ScriptStatus.REQUIRED) in result.stderr

    insp = sa.inspect(backend.engine)
    assert {
        "Corrupted",
        "Corrupted/:changelog",
        "Corrupted/:redirect",
        "Corrupted/:file/data",
    }.issubset(insp.get_table_names(schema="datasets/schemas/cli"))

    assert not {
        "datasets/schemas/cli/Corrupted/:file/data",
    }.issubset(insp.get_table_names())


def test_upgrade_postgresql_schemas_required_list(
    context: Context,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    cli: SpintaCliRunner,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(
            """
        d | r | b | m | property   | type     | ref     | prepare | access | level
        datasets/schemas/cli       |          |         |         |        |
          |   |   | Corrupted      |          | id      |         |        |
          |   |   |   | id         | integer  |         |         | open   |
          |   |   |   | name       | string   |         |         | open   |
          |   |   |   | data[]     | string   |         |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )

    store = context.get("store")
    backend = store.manifest.backend
    with backend.begin() as conn:
        conn.execute(
            'ALTER TABLE "datasets/schemas/cli"."Corrupted/:list/data" RENAME TO "datasets/schemas/cli/Corrupted/:list/data"'
        )
        # Ensure that no cached tables exist in public
        conn.execute('DROP TABLE IF EXISTS "datasets/schemas/cli/Corrupted/:list/data"')
        conn.execute('ALTER TABLE "datasets/schemas/cli"."datasets/schemas/cli/Corrupted/:list/data" SET SCHEMA public')

    insp = sa.inspect(backend.engine)
    assert {
        "public",
        "datasets/schemas/cli",
    }.issubset(insp.get_schema_names())

    assert {
        "Corrupted",
        "Corrupted/:changelog",
        "Corrupted/:redirect",
    }.issubset(insp.get_table_names(schema="datasets/schemas/cli"))

    assert {
        "datasets/schemas/cli/Corrupted/:list/data",
    }.issubset(insp.get_table_names())

    assert not {
        "Corrupted/:list/data",
    }.issubset(insp.get_table_names(schema="datasets/schemas/cli"))

    result = cli.invoke(context.get("rc"), ["upgrade", Script.POSTGRESQL_SCHEMAS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.POSTGRESQL_SCHEMAS.value, ScriptStatus.REQUIRED) in result.stderr

    insp = sa.inspect(backend.engine)
    assert {
        "Corrupted",
        "Corrupted/:changelog",
        "Corrupted/:redirect",
        "Corrupted/:list/data",
    }.issubset(insp.get_table_names(schema="datasets/schemas/cli"))

    assert not {
        "datasets/schemas/cli/Corrupted/:list/data",
    }.issubset(insp.get_table_names())


def test_upgrade_postgresql_schemas_required_same_ref(
    context: Context,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    cli: SpintaCliRunner,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(
            """
        d | r | b | m | property   | type     | ref        | prepare | access | level
        datasets/schemas/cli       |          |            |         |        |
          |   |   | RefCountry     |          | id         |         |        |
          |   |   |   | id         | integer  |            |         | open   |
          |   |   |   | name       | string   |            |         | open   |
          |   |   | RefCity        |          | id         |         |        |
          |   |   |   | id         | integer  |            |         | open   |
          |   |   |   | name       | string   |            |         | open   |
          |   |   |   | country    | ref      | RefCountry |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )

    store = context.get("store")
    backend = store.manifest.backend
    with backend.begin() as conn:
        conn.execute('ALTER TABLE "datasets/schemas/cli"."RefCountry" RENAME TO "datasets/schemas/cli/RefCountry"')
        conn.execute(
            'ALTER TABLE "datasets/schemas/cli"."RefCity" RENAME CONSTRAINT "fk_RefCity_country._id_RefCountry" TO "fk_corrupted"'
        )
        conn.execute('ALTER TABLE "datasets/schemas/cli"."RefCity" RENAME TO "datasets/schemas/cli/RefCity"')

        # Ensure that no cached tables exist in public
        conn.execute('DROP TABLE IF EXISTS "datasets/schemas/cli/RefCity"')
        conn.execute('DROP TABLE IF EXISTS "RefCity"')
        conn.execute('ALTER TABLE "datasets/schemas/cli"."datasets/schemas/cli/RefCity" SET SCHEMA public')
        conn.execute('DROP TABLE IF EXISTS "datasets/schemas/cli/RefCountry"')
        conn.execute('DROP TABLE IF EXISTS "RefCountry"')
        conn.execute('ALTER TABLE "datasets/schemas/cli"."datasets/schemas/cli/RefCountry" SET SCHEMA public')

    insp = sa.inspect(backend.engine)
    assert {
        "public",
        "datasets/schemas/cli",
    }.issubset(insp.get_schema_names())

    assert {
        "RefCountry/:changelog",
        "RefCountry/:redirect",
        "RefCity/:changelog",
        "RefCity/:redirect",
    }.issubset(insp.get_table_names(schema="datasets/schemas/cli"))

    assert {
        "datasets/schemas/cli/RefCountry",
        "datasets/schemas/cli/RefCity",
    }.issubset(insp.get_table_names(schema="public"))

    assert {
        "fk_corrupted",
    }.issubset(key["name"] for key in insp.get_foreign_keys("datasets/schemas/cli/RefCity"))

    assert not {
        "fk_RefCity_country._id_RefCountry",
    }.issubset(key["name"] for key in insp.get_foreign_keys("datasets/schemas/cli/RefCity"))

    result = cli.invoke(context.get("rc"), ["upgrade", Script.POSTGRESQL_SCHEMAS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.POSTGRESQL_SCHEMAS.value, ScriptStatus.REQUIRED) in result.stderr
    insp = sa.inspect(backend.engine)

    assert {
        "RefCountry",
        "RefCountry/:changelog",
        "RefCountry/:redirect",
        "RefCity",
        "RefCity/:changelog",
        "RefCity/:redirect",
    }.issubset(insp.get_table_names(schema="datasets/schemas/cli"))

    assert not {
        "datasets/schemas/cli/RefCountry",
        "datasets/schemas/cli/RefCity",
    }.issubset(insp.get_table_names(schema="public"))

    assert not {
        "fk_corrupted",
    }.issubset(key["name"] for key in insp.get_foreign_keys("RefCity", schema="datasets/schemas/cli"))

    assert {
        "fk_RefCity_country._id_RefCountry",
    }.issubset(key["name"] for key in insp.get_foreign_keys("RefCity", schema="datasets/schemas/cli"))

    result = cli.invoke(context.get("rc"), ["upgrade", Script.POSTGRESQL_SCHEMAS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.POSTGRESQL_SCHEMAS.value, ScriptStatus.PASSED) in result.stderr


def test_upgrade_postgresql_schemas_required_different_ref(
    context: Context,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    cli: SpintaCliRunner,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(
            """
        d | r | b | m | property   | type     | ref                             | prepare | access | level
        datasets/schemas/cli       |          |                                 |         |        |
          |   |   | RefCountry     |          | id                              |         |        |
          |   |   |   | id         | integer  |                                 |         | open   |
          |   |   |   | name       | string   |                                 |         | open   |
        datasets/schemas/cli/diff  |          |                                 |         |        |
          |   |   | RefCity        |          | id                              |         |        |
          |   |   |   | id         | integer  |                                 |         | open   |
          |   |   |   | name       | string   |                                 |         | open   |
          |   |   |   | country    | ref      | datasets/schemas/cli/RefCountry |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )

    store = context.get("store")
    backend = store.manifest.backend
    with backend.begin() as conn:
        conn.execute(
            'ALTER TABLE "datasets/schemas/cli/diff"."RefCity" RENAME CONSTRAINT "fk_RefCity_country._id_datasets/schemas/cli.RefCountry" TO "fk_corrupted"'
        )
        conn.execute('ALTER TABLE "datasets/schemas/cli/diff"."RefCity" RENAME TO "datasets/schemas/cli/diff/RefCity"')

        # Ensure that no cached tables exist in public
        conn.execute('DROP TABLE IF EXISTS "datasets/schemas/cli/diff/RefCity"')
        conn.execute('DROP TABLE IF EXISTS "RefCity"')
        conn.execute('ALTER TABLE "datasets/schemas/cli/diff"."datasets/schemas/cli/diff/RefCity" SET SCHEMA public')

    insp = sa.inspect(backend.engine)
    assert {
        "public",
        "datasets/schemas/cli",
        "datasets/schemas/cli/diff",
    }.issubset(insp.get_schema_names())

    assert {
        "RefCity/:changelog",
        "RefCity/:redirect",
    }.issubset(insp.get_table_names(schema="datasets/schemas/cli/diff"))

    assert {
        "datasets/schemas/cli/diff/RefCity",
    }.issubset(insp.get_table_names(schema="public"))

    assert {
        "fk_corrupted",
    }.issubset(key["name"] for key in insp.get_foreign_keys("datasets/schemas/cli/diff/RefCity"))

    assert not {
        "fk_RefCity_country._id_datasets/schemas/cli/RefCountry",
    }.issubset(key["name"] for key in insp.get_foreign_keys("datasets/schemas/cli/diff/RefCity"))

    result = cli.invoke(context.get("rc"), ["upgrade", Script.POSTGRESQL_SCHEMAS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.POSTGRESQL_SCHEMAS.value, ScriptStatus.REQUIRED) in result.stderr
    insp = sa.inspect(backend.engine)

    assert {
        "RefCity",
        "RefCity/:changelog",
        "RefCity/:redirect",
    }.issubset(insp.get_table_names(schema="datasets/schemas/cli/diff"))

    assert not {
        "datasets/schemas/cli/diff/RefCity",
    }.issubset(insp.get_table_names(schema="public"))

    assert not {
        "fk_corrupted",
    }.issubset(key["name"] for key in insp.get_foreign_keys("RefCity", schema="datasets/schemas/cli/diff"))

    assert {
        "fk_RefCity_country._id_datasets/schemas/cli.RefCountry",
    }.issubset(key["name"] for key in insp.get_foreign_keys("RefCity", schema="datasets/schemas/cli/diff"))

    result = cli.invoke(context.get("rc"), ["upgrade", Script.POSTGRESQL_SCHEMAS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.POSTGRESQL_SCHEMAS.value, ScriptStatus.PASSED) in result.stderr
