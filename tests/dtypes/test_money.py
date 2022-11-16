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
      |   |   |   | amount                     | money          | EUR
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
        'amount': 100
    })
    assert resp.status_code == 201

    # Read data
    resp = app.get(f'/{model}')
    assert listdata(resp, full=True) == [
        {
            'name': "Vilnius",
            'amount': 100
        }
    ]


def test_money_currency(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property                   | type           | ref
    backends/postgres/dtypes/money             |                |
      |   |   | City                           |                |
      |   |   |   | name                       | string         |
      |   |   |   | amount                     | money          | 
      |   |   |   | currency                   | string         | 
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
        'amount': 100,
        'currency': 'EUR',
    })
    assert resp.status_code == 201

    # Read data
    resp = app.get(f'/{model}')
    assert listdata(resp, full=True) == [
        {
            'name': "Vilnius",
            'amount': 100,
            'currency': 'EUR',
        }
    ]
