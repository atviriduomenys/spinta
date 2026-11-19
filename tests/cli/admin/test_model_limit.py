from pathlib import Path
from uuid import uuid4

import pytest
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


@pytest.fixture
def model_limit_context(
    context: Context,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
) -> Context:
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

    return bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )


@pytest.mark.parametrize("output", [None, "limits.yml", "limits.yaml"])
def test_admin_model_limit_skip_empty(
    output: str | None,
    model_limit_context: Context,
    tmp_path: Path,
    cli: SpintaCliRunner,
):
    context = model_limit_context

    output_path = tmp_path / output if output else None
    output_args = ["--output", output_path] if output_path else []

    result = cli.invoke(
        context.get("rc"),
        [
            "admin",
            Script.MODEL_LIMIT.value,
            *output_args,
        ],
    )
    assert result.exit_code == 0
    assert script_check_status_message(Script.MODEL_LIMIT.value, ScriptStatus.REQUIRED) in result.stderr
    yml = YAML(typ="safe")
    if output_path:
        assert not result.stdout
        assert output_path.exists()
        data = yml.load(output_path)
    else:
        data = yml.load(result.stdout)

    assert data == {}


@pytest.mark.parametrize("output", [None, "limits.yml", "limits.yaml"])
def test_admin_model_limit_skip_too_small(
    output: str | None,
    model_limit_context: Context,
    tmp_path: Path,
    cli: SpintaCliRunner,
):
    context = model_limit_context

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

    output_path = tmp_path / output if output else None
    output_args = ["--output", output_path] if output_path else []

    result = cli.invoke(
        context.get("rc"),
        [
            "admin",
            Script.MODEL_LIMIT.value,
            *output_args,
        ],
    )
    assert result.exit_code == 0
    assert script_check_status_message(Script.MODEL_LIMIT.value, ScriptStatus.REQUIRED) in result.stderr
    yml = YAML(typ="safe")
    if output_path:
        assert not result.stdout
        assert output_path.exists()
        data = yml.load(output_path)
    else:
        data = yml.load(result.stdout)

    assert data == {"models.datasets/limit/cli/req/Country.limit": 7}


@pytest.mark.parametrize("output", [None, "limits.yml", "limits.yaml"])
def test_admin_model_limit(
    output: str | None,
    model_limit_context: Context,
    tmp_path: Path,
    cli: SpintaCliRunner,
):
    context = model_limit_context

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
    output_path = tmp_path / output if output else None
    output_args = ["--output", output_path] if output_path else []

    result = cli.invoke(
        context.get("rc"),
        [
            "admin",
            Script.MODEL_LIMIT.value,
            *output_args,
        ],
    )
    assert result.exit_code == 0
    assert script_check_status_message(Script.MODEL_LIMIT.value, ScriptStatus.REQUIRED) in result.stderr
    yml = YAML(typ="safe")
    if output_path:
        assert not result.stdout
        assert output_path.exists()
        data = yml.load(output_path)
    else:
        data = yml.load(result.stdout)

    assert data == {
        "models.datasets/limit/cli/req/Country.limit": 7,
        "models.datasets/limit/cli/req/City.limit": 6,
    }


@pytest.mark.parametrize("output", ["limits.txt", "limits.json", "limits"])
@pytest.mark.parametrize("destructive", [False, True])
def test_admin_model_limit_output_invalid_file_type(
    output: str,
    destructive: bool,
    model_limit_context: Context,
    tmp_path: Path,
    cli: SpintaCliRunner,
):
    context = model_limit_context
    output_path = tmp_path / output
    args = ["admin", Script.MODEL_LIMIT.value, "--output", output_path]
    if destructive:
        args.append("--destructive")

    result = cli.invoke(context.get("rc"), args, fail=False)

    assert result.exit_code == 1
    assert f"Output file '{output_path}' has unsupported format. Use '.yml' or '.yaml'." in result.stderr
    assert not result.stdout
    assert not output_path.exists()


@pytest.mark.parametrize("output", ["limits.yml", "limits.yaml"])
@pytest.mark.parametrize("destructive", [False, True])
def test_admin_model_limit_output_existing_file(
    output: str,
    destructive: bool,
    model_limit_context: Context,
    tmp_path: Path,
    cli: SpintaCliRunner,
):
    context = model_limit_context

    output_path = tmp_path / output
    output_path.write_text("x")
    args = ["admin", Script.MODEL_LIMIT.value, "--output", output_path]
    if destructive:
        args.append("--destructive")

    result = cli.invoke(context.get("rc"), args, fail=destructive)

    assert not result.stdout
    if destructive:
        assert result.exit_code == 0
        assert YAML(typ="safe").load(output_path) == {}
    else:
        assert result.exit_code == 1
        assert f"Output file '{output_path}' already exists. Use --destructive to overwrite." in result.stderr
        assert output_path.read_text() == "x"


def test_admin_model_limit_large_row(model_limit_context: Context, cli: SpintaCliRunner):
    app = create_test_client(model_limit_context)
    app.authmodel("datasets/limit/cli/req/Country", ["insert", "getall", "search"])
    name = "".join(str(uuid4()) for _ in range(100))
    resp = app.post("/datasets/limit/cli/req/Country", json={"id": 1, "name": name})
    assert resp.status_code == 201

    result = cli.invoke(model_limit_context.get("rc"), ["admin", Script.MODEL_LIMIT.value])
    assert YAML(typ="safe").load(result.stdout) == {"models.datasets/limit/cli/req/Country.limit": 1}
