from pathlib import Path

import sqlalchemy as sa
from _pytest.fixtures import FixtureRequest
from sqlalchemy.engine import Inspector

from spinta.backends.constants import TableType
from spinta.backends.postgresql.helpers.name import get_pg_table_name
from spinta.cli.helpers.upgrade.components import Script
from spinta.cli.helpers.script.components import ScriptStatus
from spinta.cli.helpers.script.helpers import script_check_status_message
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.tabular import create_tabular_manifest


def assert_model_comments(inspector: Inspector, model: str):
    model_compressed = get_pg_table_name(model)
    assert inspector.get_table_comment(model_compressed)["text"] == model
    for column in inspector.get_columns(model_compressed):
        assert column["comment"] == column["name"]


def test_upgrade_postgresql_comments_pass(
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
        datasets/comments/cli      |          |         |         |        |
          |   |   | Country        |          | id      |         |        |
          |   |   |   | id         | integer  |         |         | open   |
          |   |   |   | name       | string   |         |         | open   |
          |   |   | City           |          | id      |         |        |
          |   |   |   | id         | integer  |         |         | open   |
          |   |   |   | name       | string   |         |         | open   |
          |   |   |   | country    | ref      | Country |         | open   |
        datasets/comments/rand/very/long/dataset/name/that/will/compress |          |         |         |        |
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

    assert_model_comments(insp, "datasets/comments/cli/Country")
    assert_model_comments(insp, "datasets/comments/cli/Country/:changelog")
    assert_model_comments(insp, "datasets/comments/cli/Country/:redirect")

    assert_model_comments(insp, "datasets/comments/cli/City")
    assert_model_comments(insp, "datasets/comments/cli/City/:changelog")
    assert_model_comments(insp, "datasets/comments/cli/City/:redirect")

    assert_model_comments(insp, "datasets/comments/rand/very/long/dataset/name/that/will/compress/Random")
    assert_model_comments(insp, "datasets/comments/rand/very/long/dataset/name/that/will/compress/Random/:changelog")
    assert_model_comments(insp, "datasets/comments/rand/very/long/dataset/name/that/will/compress/Random/:redirect")
    assert_model_comments(insp, "datasets/comments/rand/very/long/dataset/name/that/will/compress/Random/:file/img")

    result = cli.invoke(context.get("rc"), ["upgrade", Script.POSTGRESQL_COMMENTS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.POSTGRESQL_COMMENTS.value, ScriptStatus.PASSED) in result.stdout


def test_upgrade_postgresql_comments_required_table(
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
        datasets/comments/req      |          |         |         |        |
          |   |   | Country        |          | id      |         |        |
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
    insp = sa.inspect(backend.engine)

    assert_model_comments(insp, "datasets/comments/req/Country")
    assert_model_comments(insp, "datasets/comments/req/Country/:changelog")
    assert_model_comments(insp, "datasets/comments/req/Country/:redirect")

    with backend.begin() as conn:
        conn.execute(f'''
            COMMENT ON TABLE "{get_pg_table_name("datasets/comments/req/Country")}" IS NULL
        ''')

    result = cli.invoke(context.get("rc"), ["upgrade", Script.POSTGRESQL_COMMENTS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.POSTGRESQL_COMMENTS.value, ScriptStatus.REQUIRED) in result.stdout
    assert_model_comments(insp, "datasets/comments/req/Country")


def test_upgrade_postgresql_comments_required_changelog(
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
        datasets/comments/req      |          |         |         |        |
          |   |   | Country        |          | id      |         |        |
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
    insp = sa.inspect(backend.engine)

    assert_model_comments(insp, "datasets/comments/req/Country")
    assert_model_comments(insp, "datasets/comments/req/Country/:changelog")
    assert_model_comments(insp, "datasets/comments/req/Country/:redirect")

    with backend.begin() as conn:
        conn.execute(f'''
            COMMENT ON TABLE "{get_pg_table_name("datasets/comments/req/Country", TableType.CHANGELOG)}" IS NULL
        ''')

    result = cli.invoke(context.get("rc"), ["upgrade", Script.POSTGRESQL_COMMENTS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.POSTGRESQL_COMMENTS.value, ScriptStatus.REQUIRED) in result.stdout
    assert_model_comments(insp, "datasets/comments/req/Country/:changelog")


def test_upgrade_postgresql_comments_required_redirect(
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
        datasets/comments/req      |          |         |         |        |
          |   |   | Country        |          | id      |         |        |
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
    insp = sa.inspect(backend.engine)

    assert_model_comments(insp, "datasets/comments/req/Country")
    assert_model_comments(insp, "datasets/comments/req/Country/:changelog")
    assert_model_comments(insp, "datasets/comments/req/Country/:redirect")

    with backend.begin() as conn:
        conn.execute(f'''
            COMMENT ON TABLE "{get_pg_table_name("datasets/comments/req/Country", TableType.REDIRECT)}" IS NULL
        ''')

    result = cli.invoke(context.get("rc"), ["upgrade", Script.POSTGRESQL_COMMENTS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.POSTGRESQL_COMMENTS.value, ScriptStatus.REQUIRED) in result.stdout
    assert_model_comments(insp, "datasets/comments/req/Country/:redirect")


def test_upgrade_postgresql_comments_required_file(
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
        datasets/comments/req/file |          |         |         |        |
          |   |   | Country        |          | id      |         |        |
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

    assert_model_comments(insp, "datasets/comments/req/file/Country")
    assert_model_comments(insp, "datasets/comments/req/file/Country/:changelog")
    assert_model_comments(insp, "datasets/comments/req/file/Country/:redirect")
    assert_model_comments(insp, "datasets/comments/req/file/Country/:file/img")

    with backend.begin() as conn:
        conn.execute(f'''
            COMMENT ON TABLE "{get_pg_table_name("datasets/comments/req/file/Country", TableType.FILE, "img")}" IS NULL
        ''')

    result = cli.invoke(context.get("rc"), ["upgrade", Script.POSTGRESQL_COMMENTS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.POSTGRESQL_COMMENTS.value, ScriptStatus.REQUIRED) in result.stdout
    assert_model_comments(insp, "datasets/comments/req/file/Country/:file/img")


def test_upgrade_postgresql_comments_required_column(
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
        datasets/comments/req      |          |         |         |        |
          |   |   | Country        |          | id      |         |        |
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
    insp = sa.inspect(backend.engine)

    assert_model_comments(insp, "datasets/comments/req/Country")
    assert_model_comments(insp, "datasets/comments/req/Country/:changelog")
    assert_model_comments(insp, "datasets/comments/req/Country/:redirect")

    with backend.begin() as conn:
        conn.execute(f'''
            COMMENT ON COLUMN "{get_pg_table_name("datasets/comments/req/Country")}"."name" IS NULL;
            COMMENT ON COLUMN "{get_pg_table_name("datasets/comments/req/Country")}"."_id" IS NULL;
        ''')

    result = cli.invoke(context.get("rc"), ["upgrade", Script.POSTGRESQL_COMMENTS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.POSTGRESQL_COMMENTS.value, ScriptStatus.REQUIRED) in result.stdout
    assert_model_comments(insp, "datasets/comments/req/Country")


def test_upgrade_postgresql_comments_required_table_missmatch(
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
        datasets/comments/rand/very/long/dataset/name/that/will/compress |          |         |         |        |
          |   |   | Country        |          | id      |         |        |
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
    insp = sa.inspect(backend.engine)

    assert_model_comments(insp, "datasets/comments/rand/very/long/dataset/name/that/will/compress/Country")
    assert_model_comments(insp, "datasets/comments/rand/very/long/dataset/name/that/will/compress/Country/:changelog")
    assert_model_comments(insp, "datasets/comments/rand/very/long/dataset/name/that/will/compress/Country/:redirect")

    with backend.begin() as conn:
        conn.execute(f'''
            COMMENT ON TABLE "{get_pg_table_name("datasets/comments/rand/very/long/dataset/name/that/will/compress/Country")}" IS 'datasets/comments/req/Country'
        ''')

    result = cli.invoke(context.get("rc"), ["upgrade", Script.POSTGRESQL_COMMENTS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.POSTGRESQL_COMMENTS.value, ScriptStatus.REQUIRED) in result.stdout
    assert_model_comments(insp, "datasets/comments/rand/very/long/dataset/name/that/will/compress/Country")


def test_upgrade_postgresql_comments_required_column_missmatch(
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
        datasets/comments/req      |          |         |         |        |
          |   |   | Country        |          | id      |         |        |
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
    insp = sa.inspect(backend.engine)

    assert_model_comments(insp, "datasets/comments/req/Country")
    assert_model_comments(insp, "datasets/comments/req/Country/:changelog")
    assert_model_comments(insp, "datasets/comments/req/Country/:redirect")

    with backend.begin() as conn:
        conn.execute(f'''
            COMMENT ON COLUMN "{get_pg_table_name("datasets/comments/req/Country")}"."name" IS 'id'
        ''')

    result = cli.invoke(context.get("rc"), ["upgrade", Script.POSTGRESQL_COMMENTS.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.POSTGRESQL_COMMENTS.value, ScriptStatus.REQUIRED) in result.stdout
    assert_model_comments(insp, "datasets/comments/req/Country")
