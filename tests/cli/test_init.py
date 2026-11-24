from spinta.manifests.tabular.helpers import render_tabular_manifest
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.manifest import load_manifest_and_context


def test_init(rc, cli: SpintaCliRunner, tmp_path):
    cli.invoke(rc, ["init", tmp_path / "manifest.csv"])
    context, manifest = load_manifest_and_context(rc, tmp_path / "manifest.csv")
    # Empty manifest has no columns with values, so nothing is shown.
    assert render_tabular_manifest(context, manifest) == ""
