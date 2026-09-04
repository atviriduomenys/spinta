import json

from ruamel.yaml import YAML

from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.tabular import create_tabular_manifest

yaml = YAML(typ="safe")

MANIFEST = striptable("""
id | d | r | b | m | property | type            | ref | level | access | title
   | datasets/gov/rc/jadis/at280/1/at280_israsas |  |     |       |        | AT280
   |   | test                 | memory          |     |       |        |
   |   |   |   | Israsas      |                 | kodas |     |        |
   |   |   |   |   | kodas    | string required |     | 4     | open   |
   |                          |                 |     |       |        |
   | datasets/gov/rc/ntr/n249/1/n249_israsas |   |     |       |        | N249
   |   | test                 | memory          |     |       |        |
   |   |   |   | Israsas      |                 | nr  |       |        |
   |   |   |   |   | nr       | string required |     | 4     | open   |
""")

MANIFEST_WITH_ONE_SERVICE = striptable("""
id | d | r | b | m | property | type            | ref | level | access | title
   | datasets/gov/rc/jadis/at280/1/at280_israsas |  |     |       |        | AT280
   |   | test                 | memory          |     |       |        |
   |   |   |   | Israsas      |                 | kodas |     |        |
   |   |   |   |   | kodas    | string required |     | 4     | open   |
""")

UDTS_CFG = """
info:
  title: JADIS duomenų paslauga
servers:
  - url: https://get.data.gov.lt
    description: Production
  - url: https://test-get.data.gov.lt
    description: Testing
auth:
  token_url: https://get.data.gov.lt/auth/token
"""


def _manifest(context, tmp_path, manifest=MANIFEST):
    path = tmp_path / "manifest.csv"
    create_tabular_manifest(context, path, manifest)
    return path


def _config(tmp_path, config=UDTS_CFG):
    path = tmp_path / "vartai.yml"
    path.write_text(config)
    return path


def test_list(context, rc, cli: SpintaCliRunner, tmp_path):
    path = _manifest(context, tmp_path)

    result = cli.invoke(rc, ["udts", "oas", path, "--list"])

    assert "datasets/gov/rc/jadis/at280/1" in result.stdout
    assert "datasets/gov/rc/jadis/at280/1/at280_israsas" in result.stdout
    assert "datasets/gov/rc/ntr/n249/1" in result.stdout


def test_without_path_and_several_services(context, rc, cli: SpintaCliRunner, tmp_path):
    path = _manifest(context, tmp_path)

    result = cli.invoke(rc, ["udts", "oas", path, "--udts-cfg", _config(tmp_path)], fail=False)

    assert result.exit_code == 1
    assert "more than one data service" in result.stderr
    assert "datasets/gov/rc/ntr/n249/1" in result.stderr


def test_without_path_and_one_service(context, rc, cli: SpintaCliRunner, tmp_path):
    path = _manifest(context, tmp_path, MANIFEST_WITH_ONE_SERVICE)

    result = cli.invoke(rc, ["udts", "oas", path, "--udts-cfg", _config(tmp_path)])

    spec = json.loads(result.stdout)
    assert spec["servers"][0]["url"] == "https://get.data.gov.lt/datasets/gov/rc/jadis/at280/1"
    assert "/at280_israsas/Israsas" in spec["paths"]


def test_unknown_service_path(context, rc, cli: SpintaCliRunner, tmp_path):
    path = _manifest(context, tmp_path)

    result = cli.invoke(
        rc,
        ["udts", "oas", path, "--path", "datasets/gov/rc/jadis/at280/2", "--udts-cfg", _config(tmp_path)],
        fail=False,
    )

    assert result.exit_code == 1
    assert "has no datasets in manifest" in result.stderr
    assert "datasets/gov/rc/jadis/at280/1" in result.stderr


def test_path_is_not_service_level(context, rc, cli: SpintaCliRunner, tmp_path):
    """A path of another shape is a warning, datasets under it are exported."""
    path = _manifest(context, tmp_path)

    result = cli.invoke(
        rc,
        ["udts", "oas", path, "--path", "datasets/gov/rc/jadis/at280/1/at280_israsas", "--udts-cfg", _config(tmp_path)],
    )

    assert "is not an UDTS data service path" in result.stderr
    assert set(json.loads(result.stdout)["paths"]) == {
        "/:version",
        "/:health",
        "/:token",
        "/version",
        "/health",
        "/auth/token",
        "/Israsas",
        "/Israsas/{id}",
    }


def test_output_json(context, rc, cli: SpintaCliRunner, tmp_path):
    path = _manifest(context, tmp_path)
    cfg = tmp_path / "vartai.yml"
    cfg.write_text(UDTS_CFG)
    output = tmp_path / "at280.json"

    cli.invoke(
        rc,
        [
            "udts",
            "oas",
            path,
            "-o",
            output,
            "--path",
            "datasets/gov/rc/jadis/at280/1",
            "--udts-cfg",
            cfg,
        ],
    )

    spec = json.loads(output.read_text())
    assert spec["info"]["title"] == "JADIS duomenų paslauga"
    assert spec["servers"] == [
        {"url": "https://get.data.gov.lt/datasets/gov/rc/jadis/at280/1", "description": "Production"},
        {"url": "https://test-get.data.gov.lt/datasets/gov/rc/jadis/at280/1", "description": "Testing"},
    ]
    assert set(spec["paths"]) == {
        "/:version",
        "/:health",
        "/:token",
        "/version",
        "/health",
        "/auth/token",
        "/at280_israsas/Israsas",
        "/at280_israsas/Israsas/{id}",
    }
    token_url = spec["components"]["securitySchemes"]["UAPI_auth"]["flows"]["clientCredentials"]["tokenUrl"]
    assert token_url == "https://get.data.gov.lt/auth/token"


def test_output_yaml(context, rc, cli: SpintaCliRunner, tmp_path):
    path = _manifest(context, tmp_path)
    output = tmp_path / "at280.yaml"

    cli.invoke(
        rc,
        ["udts", "oas", path, "-o", output, "--path", "datasets/gov/rc/jadis/at280/1", "--udts-cfg", _config(tmp_path)],
    )

    written = output.read_text()
    spec = yaml.load(written)
    assert spec["openapi"] == "3.1.0"
    assert "/at280_israsas/Israsas" in spec["paths"]
    # Shared objects would be written as YAML anchors and aliases, which not
    # every consumer of the specification handles.
    assert "&id" not in written
    assert "*id" not in written


def test_api_version(context, rc, cli: SpintaCliRunner, tmp_path):
    path = _manifest(context, tmp_path)

    result = cli.invoke(
        rc,
        [
            "udts",
            "oas",
            path,
            "--path",
            "datasets/gov/rc/jadis/at280/1",
            "--api-version",
            "2.1.8",
            "--udts-cfg",
            _config(tmp_path),
        ],
    )

    assert json.loads(result.stdout)["info"]["version"] == "2.1.8"


def custom_scope_formatter(context, node, action, udts=False, /):
    """Stands in for a formatter whose UDTS flag is positional-only."""
    assert udts is True
    name = node.model.model_type() if hasattr(node, "model") else node.model_type()
    return f"kita:{name}:{action.value}"


def test_scopes_follow_the_configured_formatter(context, rc, cli: SpintaCliRunner, tmp_path):
    """Spinta authorizes with the formatter of its configuration."""
    path = _manifest(context, tmp_path, MANIFEST_WITH_ONE_SERVICE)
    localrc = rc.fork({"scope_formatter": "tests.cli.test_udts_oas:custom_scope_formatter"})

    result = cli.invoke(localrc, ["udts", "oas", path, "--udts-cfg", _config(tmp_path)])

    spec = json.loads(result.stdout)
    model = "datasets/gov/rc/jadis/at280/1/at280_israsas/Israsas"
    security = spec["paths"]["/at280_israsas/Israsas"]["get"]["security"]
    assert security[0] == {"UAPI_auth": [f"kita:{model}:getall"]}
    assert {"UAPI_auth": [f"kita:{model}:search"]} in security
    assert (
        f"kita:{model}:getone"
        in spec["components"]["securitySchemes"]["UAPI_auth"]["flows"]["clientCredentials"]["scopes"]
    )


def test_configuration_is_required(context, rc, cli: SpintaCliRunner, tmp_path):
    """A data service is published with a name and with its environments."""
    path = _manifest(context, tmp_path, MANIFEST_WITH_ONE_SERVICE)

    result = cli.invoke(rc, ["udts", "oas", path], fail=False)

    assert result.exit_code == 1
    assert "`--udts-cfg` is required" in result.stderr
    assert "udts_cfg.example.yml" in result.stderr


def test_title_is_required(context, rc, cli: SpintaCliRunner, tmp_path):
    path = _manifest(context, tmp_path, MANIFEST_WITH_ONE_SERVICE)
    config = _config(tmp_path, "servers:\n  - url: https://get.data.gov.lt\n")

    result = cli.invoke(rc, ["udts", "oas", path, "--udts-cfg", config], fail=False)

    assert result.exit_code == 1
    assert "`info.title` is required" in result.stderr


def test_token_url_is_required_with_a_relative_server(context, rc, cli: SpintaCliRunner, tmp_path):
    """A token endpoint has to be absolute, and a relative server gives none."""
    path = _manifest(context, tmp_path, MANIFEST_WITH_ONE_SERVICE)
    config = _config(tmp_path, "info:\n  title: JADIS\nservers:\n  - url: /datasets/gov/rc/jadis/at280/1\n")

    result = cli.invoke(rc, ["udts", "oas", path, "--udts-cfg", config], fail=False)

    assert result.exit_code == 1
    assert "`auth.token_url` is required when the first server URL is relative" in result.stderr


def test_at_least_one_server_is_required(context, rc, cli: SpintaCliRunner, tmp_path):
    path = _manifest(context, tmp_path, MANIFEST_WITH_ONE_SERVICE)
    config = _config(tmp_path, "info:\n  title: JADIS\nservers: []\n")

    result = cli.invoke(rc, ["udts", "oas", path, "--udts-cfg", config], fail=False)

    assert result.exit_code == 1
    assert "`servers` is required" in result.stderr


def test_listing_services_needs_no_configuration(context, rc, cli: SpintaCliRunner, tmp_path):
    """`--list` reads the manifest, it writes no document."""
    path = _manifest(context, tmp_path)

    result = cli.invoke(rc, ["udts", "oas", path, "--list"])

    assert "datasets/gov/rc/jadis/at280/1" in result.stdout
