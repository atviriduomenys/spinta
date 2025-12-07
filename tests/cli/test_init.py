from spinta.manifests.tabular.helpers import render_tabular_manifest
from spinta.testing.cli import SpintaCliRunner
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.manifest import load_manifest_and_context


def test_init(rc, cli: SpintaCliRunner, tmp_path):
    cli.invoke(rc, ["init", tmp_path / "manifest.csv"])
    context, manifest = load_manifest_and_context(rc, tmp_path / "manifest.csv")
    assert render_tabular_manifest(context, manifest) == striptable("""
       id | d | r | b | m | property | type | ref | source | source.type | prepare | origin | count | level | status | visibility | access | uri | eli | title | description
    """)
