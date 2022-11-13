from pytest import FixtureRequest
from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest


def test_money(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property                   | type           | ref
    backends/postgres/dtypes/money             |                |
      |   |   | City                           |                |
      |   |   |   | name                       | string         |
      |   |   |   | salary                     | money          | EUR
    ''', backend=postgresql, request=request)

    model: str = 'backends/postgres/dtypes/money/City'

    app = create_test_client(context)
    app.authmodel(model, [
        'insert',
        'getall',
    ])

    # Write data
    resp = app.post(f'/{model}', json={
        'name': "Vilnius",
        'salary': 100
    })
    assert resp.status_code == 201

    # Read data
    resp = app.get(f'/{model}')
    assert listdata(resp, full=True) == [
        {
            'name': "Vilnius",
            'salary': 100
        }
    ]
