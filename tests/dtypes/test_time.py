from pytest import FixtureRequest

from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest


def test_time(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property          | type   | ref
    backends/postgres/dtypes/time     |        |
      |   |   | City                  |        |
      |   |   |   | name              | string |
      |   |   |   | time              | time   |
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authmodel('backends/postgres/dtypes/time/City', [
        'insert',
        'getall',
    ])

    # Write data
    resp = app.post('/backends/postgres/dtypes/time/City', json={
        'name': "Vilnius",
        'time': '00:00:01',
    })
    assert resp.status_code == 201

    # Read data
    resp = app.get('/backends/postgres/dtypes/time/City')
    assert listdata(resp, full=True) == [
        {
            'name': "Vilnius",
            'time': '00:00:01',
        }
    ]
