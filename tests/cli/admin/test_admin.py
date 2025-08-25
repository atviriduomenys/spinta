import pathlib
from pathlib import Path

import pytest
from _pytest.fixtures import FixtureRequest

from spinta.cli.helpers.admin.components import Script
from spinta.cli.helpers.script.components import ScriptStatus
from spinta.cli.helpers.script.helpers import script_check_status_message
from spinta.cli.helpers.upgrade.registry import upgrade_script_registry
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.exceptions import ScriptNotFound
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.tabular import create_tabular_manifest


def test_admin_invalid_script_name(context, rc, cli: SpintaCliRunner):
    with pytest.raises(ScriptNotFound):
        result = cli.invoke(rc, ["admin", "UNAVAILABLE"], fail=False)
        raise result.exception


def test_admin_all_error(context, rc, cli: SpintaCliRunner, tmp_path: pathlib.Path):
    result = cli.invoke(rc, ["admin", "-c"], fail=False)
    assert result.exit_code == 1
    assert "At least one script needs to be specified" in result.stderr

    for script in upgrade_script_registry.get_all_names():
        assert script_check_status_message(script, ScriptStatus.PASSED) not in result.stdout


def test_admin_multiple(
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
        d | r | b | m | property  | type    | ref                           | prepare | access | level
        datasets/temp             |         |                               |         |        |
          |   |   | Country       |         | id                            |         |        |
          |   |   |   | id        | integer |                               |         | open   |
          |   |   |   | name      | string  |                               |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )
    with open(tmp_path / "modellist.txt", "w") as f:
        f.write("datasets/temp/Country")

    result = cli.invoke(
        context.get("rc"),
        ["admin", Script.CHANGELOG.value, Script.DEDUPLICATE.value, "-c", "--input", f"{tmp_path / 'modellist.txt'}"],
    )
    assert result.exit_code == 0

    assert script_check_status_message(Script.DEDUPLICATE.value, ScriptStatus.PASSED) in result.stdout
    assert script_check_status_message(Script.CHANGELOG.value, ScriptStatus.PASSED) in result.stdout
