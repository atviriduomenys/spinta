import pytest

from spinta.testing.context import create_test_context
from spinta.testing.tabular import load_tabular_manifest


@pytest.mark.skip
def test_filter(rc, tmpdir):
    load_tabular_manifest(rc, tmpdir / 'manifest.csv', '''
    d | r | b | m | property | source      | prepare   | type   | ref     | level | access | uri | title   | description
    datasets/gov/example     |             |           |        |         |       |        |     | Example |
      | data                 |             |           | sql    |         |       |        |     | Data    |
      |   |                  |             |           |        |         |       |        |     |         |
      |   |   | country      | salis       | code='lt' |        | code    |       |        |     | Country |
      |   |   |   | code     | kodas       |           | string |         | 3     | open   |     | Code    |
      |   |   |   | name     | pavadinimas |           | string |         | 3     | open   |     | Name    |
    ''')


    context = create_test_context(rc)
    app = create_test_client(context)

    resp = app.get('/datasets/gov/example/country')
    assert listdata(resp, 'code', 'name') == [
        ('lt', 'Lietuva'),
    ]
