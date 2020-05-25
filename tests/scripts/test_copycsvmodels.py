from spinta.testing.tabular import striptable
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.tabular import load_tabular_manifest
from spinta.testing.tabular import render_tabular_manifest
from spinta.scripts.copycsvmodels import main


def test_copycsvmodels(rc, cli, tmpdir):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | b | m | property | source      | type   | ref     | access
    datasets/gov/example     |             |        |         |
      | data                 |             | sql    |         |
      |   |                  |             |        |         |
      |   |   | country      | salis       |        | code    |
      |   |   |   | code     | kodas       | string |         | public
      |   |   |   | name     | pavadinimas | string |         | open
      |   |                  |             |        |         |
      |   |   | city         | miestas     |        | name    |
      |   |   |   | name     | pavadinimas | string |         | open
      |   |   |   | country  | salis       | ref    | country | open
      |   |                  |             |        |         |
      |   |   | capital      | miestas     |        | name    |
      |   |   |   | name     | pavadinimas | string |         |
      |   |   |   | country  | salis       | ref    | country |
    '''))

    cli.invoke(rc, main, [
        '--no-external', '--access', 'open',
        str(tmpdir / 'manifest.csv'),
        str(tmpdir / 'result.csv'),
    ])

    manifest = load_tabular_manifest(rc, tmpdir / 'result.csv')
    cols = ['dataset', 'resource', 'base', 'model', 'property', 'source', 'ref', 'access']
    assert render_tabular_manifest(manifest, cols) == striptable('''
    d | r | b | m | property                 | source | ref                          | access
      |   |                                  |        |                              |
      |   |   | datasets/gov/example/country |        |                              | open
      |   |   |   | name                     |        |                              | open
      |   |                                  |        |                              |
      |   |   | datasets/gov/example/city    |        |                              | open
      |   |   |   | name                     |        |                              | open
      |   |   |   | country                  |        | datasets/gov/example/country | open
    ''')
