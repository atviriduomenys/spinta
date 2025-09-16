from pathlib import Path

import sqlalchemy as sa
from _pytest.fixtures import FixtureRequest

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


def test_upgrade_redirect_pass(
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
        datasets/redirect/cli      |          |         |         |        |
          |   |   | Country        |          | id      |         |        |
          |   |   |   | id         | integer  |         |         | open   |
          |   |   |   | name       | string   |         |         | open   |
          |   |   | City           |          | id      |         |        |
          |   |   |   | id         | integer  |         |         | open   |
          |   |   |   | name       | string   |         |         | open   |
          |   |   |   | country    | ref      | Country |         | open   |
        datasets/redirect/rand     |          |         |         |        |
          |   |   | Random         |          | id      |         |        |
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
    assert insp.has_table(get_pg_table_name("datasets/redirect/cli/Country", TableType.REDIRECT))
    assert insp.has_table(get_pg_table_name("datasets/redirect/cli/City", TableType.REDIRECT))
    assert insp.has_table(get_pg_table_name("datasets/redirect/rand/Random", TableType.REDIRECT))

    result = cli.invoke(rc, ["upgrade", Script.REDIRECT.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.REDIRECT.value, ScriptStatus.PASSED) in result.stdout


def test_upgrade_redirect_required(
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
        datasets/redirect/cli/req  |          |         |         |        |
          |   |   | Country        |          | id      |         |        |
          |   |   |   | id         | integer  |         |         | open   |
          |   |   |   | name       | string   |         |         | open   |
          |   |   | City           |          | id      |         |        |
          |   |   |   | id         | integer  |         |         | open   |
          |   |   |   | name       | string   |         |         | open   |
          |   |   |   | country    | ref      | Country |         | open   |
        datasets/redirect/rand/req |          |         |         |        |
          |   |   | Random         |          | id      |         |        |
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
    country_redirect = get_pg_table_name("datasets/redirect/cli/req/Country", TableType.REDIRECT)
    city_redirect = get_pg_table_name("datasets/redirect/cli/req/City", TableType.REDIRECT)
    random_redirect = get_pg_table_name("datasets/redirect/rand/req/Random", TableType.REDIRECT)

    with backend.begin() as conn:
        conn.execute(f'''
            DROP TABLE IF EXISTS "{country_redirect}";
            DROP TABLE IF EXISTS "{city_redirect}";
            DROP TABLE IF EXISTS "{random_redirect}";
        ''')

    assert not insp.has_table(country_redirect)
    assert not insp.has_table(city_redirect)
    assert not insp.has_table(random_redirect)
    result = cli.invoke(context.get("rc"), ["upgrade", Script.REDIRECT.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.REDIRECT.value, ScriptStatus.REQUIRED) in result.stdout

    assert insp.has_table(country_redirect)
    assert insp.has_table(city_redirect)
    assert insp.has_table(random_redirect)
