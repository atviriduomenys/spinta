import pytest

from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import load_manifest_get_context


@pytest.mark.skip()
def test_geometry(rc: RawConfig, postgresql: str):
    context: Context = load_manifest_get_context(rc, '''
    d | r | b | m | property          | type     | ref
    backends/postgres/dtypes/geometry |          |
      |   |   | City                  |          |
      |   |   |   | name              | string   |
      |   |   |   | coordinates       | geometry |
    ''', backend=postgresql)

    app = create_test_client(context)
    app.authmodel('backends/postgres/dtypes/geometry/City', [
        'insert',
        'getall',
    ])

    # Write data
    resp = app.post('/backends/postgres/dtypes/geometry/City', json={
        'name': "Vilnius",
        'coordinates': 'point(54.6870458, 25.2829111)',
    })
    assert resp.status_code == 200

    # Read data
    resp = app.get('/backends/postgres/dtypes/geometry/City')
    assert listdata(resp) == [
        {
            'name': "Vilnius",
            'coordinates': 'point(54.6870458, 25.2829111)',
        }
    ]

