from spinta.manifests.tabular.helpers import render_tabular_manifest
from spinta.testing.cli import SpintaCliRunner
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.manifest import load_manifest


def test_show(rc, cli: SpintaCliRunner, tmp_path):
    cli.invoke(rc, ['init', tmp_path / 'manifest.csv'])
    manifest = load_manifest(rc, tmp_path / 'manifest.csv')
    assert render_tabular_manifest(manifest) == striptable('''
    id | d | r | b | m | property | type | ref | source | prepare | level | access | uri | title | description
    ''')
