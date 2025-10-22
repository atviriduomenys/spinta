from pathlib import Path

from _pytest.fixtures import FixtureRequest
from ruamel.yaml import YAML

from spinta.cli.helpers.admin.components import Script
from spinta.cli.helpers.script.components import ScriptStatus
from spinta.cli.helpers.script.helpers import script_check_status_message
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.client import create_test_client
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.tabular import create_tabular_manifest
from spinta.utils.config import get_limit_path


def test_admin_model_limit_skip_empty(
    context: Context,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    cli: SpintaCliRunner,
):
    rc = rc.fork({"default_limit_bytes": "1k"})

    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(
            """
        d | r | b | m | property      | type    | ref     | prepare | access | level
        datasets/limit/cli/req        |         |         |         |        |
          |   |   | Country           |         | id      |         |        |
          |   |   |   | id            | integer |         |         | open   |
          |   |   |   | name          | string  |         |         | open   |
          |   |   | City              |         | id      |         |        |
          |   |   |   | id            | integer |         |         | open   |
          |   |   |   | name          | string  |         |         | open   |
          |   |   |   | country       | ref     | Country |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )

    config = context.get("config")

    limit_path = get_limit_path(config)
    limit_path.unlink(missing_ok=True)
    assert not limit_path.exists()

    result = cli.invoke(
        context.get("rc"),
        [
            "admin",
            Script.MODEL_LIMIT.value,
        ],
    )
    assert result.exit_code == 0
    assert script_check_status_message(Script.MODEL_LIMIT.value, ScriptStatus.REQUIRED) in result.stdout
    assert limit_path.exists()
    yml = YAML()
    data = yml.load(limit_path)

    assert data == {}


def test_admin_model_limit_skip_too_small(
    context: Context,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    cli: SpintaCliRunner,
):
    rc = rc.fork({"default_limit_bytes": "1k"})

    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(
            """
        d | r | b | m | property      | type    | ref     | prepare | access | level
        datasets/limit/cli/req        |         |         |         |        |
          |   |   | Country           |         | id      |         |        |
          |   |   |   | id            | integer |         |         | open   |
          |   |   |   | name          | string  |         |         | open   |
          |   |   | City              |         | id      |         |        |
          |   |   |   | id            | integer |         |         | open   |
          |   |   |   | name          | string  |         |         | open   |
          |   |   |   | country       | ref     | Country |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )

    app = create_test_client(context)
    app.authorize(
        [
            "spinta_insert",
            "spinta_getone",
            "spinta_delete",
            "spinta_wipe",
            "spinta_search",
            "spinta_set_meta_fields",
            "spinta_move",
            "spinta_getall",
            "spinta_changes",
        ]
    )

    for i in range(100):
        app.post("/datasets/limit/cli/req/Country", json={"id": i, "name": f"Country {i}"})
    app.post("/datasets/limit/cli/req/City", json={"id": 0, "name": "test"})

    config = context.get("config")

    limit_path = get_limit_path(config)
    limit_path.unlink(missing_ok=True)
    assert not limit_path.exists()

    result = cli.invoke(
        context.get("rc"),
        [
            "admin",
            Script.MODEL_LIMIT.value,
        ],
    )
    assert result.exit_code == 0
    assert script_check_status_message(Script.MODEL_LIMIT.value, ScriptStatus.REQUIRED) in result.stdout
    assert limit_path.exists()
    yml = YAML()
    data = yml.load(limit_path)

    assert data == {"datasets/limit/cli/req/Country": 7}


def test_admin_model_limit(
    context: Context,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    cli: SpintaCliRunner,
):
    rc = rc.fork({"default_limit_bytes": "1k"})

    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(
            """
        d | r | b | m | property      | type    | ref     | prepare | access | level
        datasets/limit/cli/req        |         |         |         |        |
          |   |   | Country           |         | id      |         |        |
          |   |   |   | id            | integer |         |         | open   |
          |   |   |   | name          | string  |         |         | open   |
          |   |   | City              |         | id      |         |        |
          |   |   |   | id            | integer |         |         | open   |
          |   |   |   | name          | string  |         |         | open   |
          |   |   |   | country       | ref     | Country |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )

    app = create_test_client(context)
    app.authorize(
        [
            "spinta_insert",
            "spinta_getone",
            "spinta_delete",
            "spinta_wipe",
            "spinta_search",
            "spinta_set_meta_fields",
            "spinta_move",
            "spinta_getall",
            "spinta_changes",
        ]
    )

    for i in range(100):
        resp = app.post("/datasets/limit/cli/req/Country", json={"id": i, "name": f"Country {i}"})
        id_ = resp.json()["_id"]
        app.post("/datasets/limit/cli/req/City", json={"id": i, "name": f"City {i}", "country": {"_id": id_}})
    config = context.get("config")

    limit_path = get_limit_path(config)
    limit_path.unlink(missing_ok=True)
    assert not limit_path.exists()

    result = cli.invoke(
        context.get("rc"),
        [
            "admin",
            Script.MODEL_LIMIT.value,
        ],
    )
    assert result.exit_code == 0
    assert script_check_status_message(Script.MODEL_LIMIT.value, ScriptStatus.REQUIRED) in result.stdout
    assert limit_path.exists()
    yml = YAML()
    data = yml.load(limit_path)

    assert data == {
        "datasets/limit/cli/req/Country": 7,
        "datasets/limit/cli/req/City": 6,
    }
