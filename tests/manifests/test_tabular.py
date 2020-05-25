from spinta.testing.tabular import striptable
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.tabular import load_tabular_manifest
from spinta.testing.tabular import render_tabular_manifest


def test_loading(postgresql, rc, cli, tmpdir, request):
    table = striptable('''
    id | d | r | b | m | property | source      | prepare   | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |           |        |         |       | open   |     | Example |
       |   | data                 |             |           |        | default |       | open   |     | Data    |
       |   |   |                  |             |           |        |         |       |        |     |         |
       |   |   |   | country      |             | code='lt' |        | code    |       | open   |     | Country |
       |   |   |   |   | code     | kodas       | lower()   | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |           | string |         | 3     | open   |     | Name    |
       |   |   |                  |             |           |        |         |       |        |     |         |
       |   |   |   | city         |             |           |        | name    |       | open   |     | City    |
       |   |   |   |   | name     | pavadinimas |           | string |         | 3     | open   |     | Name    |
       |   |   |   |   | country  | šalis       |           | ref    | country | 4     | open   |     | Country |
    ''')
    create_tabular_manifest(tmpdir / 'manifest.csv', table)
    manifest = load_tabular_manifest(rc, tmpdir / 'manifest.csv')
    assert render_tabular_manifest(manifest) == table
