from typing import cast

import sqlalchemy as sa
from pytest import FixtureRequest

from spinta.backends.postgresql.components import PostgreSQL
from spinta.components import Store
from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest


def test_geometry(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property          | type     | ref
    backends/postgres/dtypes/geometry |          |
      |   |   | City                  |          |
      |   |   |   | name              | string   |
      |   |   |   | coordinates       | geometry |
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authmodel('backends/postgres/dtypes/geometry/City', [
        'insert',
        'getall',
    ])

    # Write data
    resp = app.post('/backends/postgres/dtypes/geometry/City', json={
        'name': "Vilnius",
        'coordinates': 'POINT(54.6870458 25.2829111)',
    })
    assert resp.status_code == 201

    # Read data
    resp = app.get('/backends/postgres/dtypes/geometry/City')
    assert listdata(resp, full=True) == [
        {
            'name': "Vilnius",
            'coordinates': 'POINT (54.6870458 25.2829111)',
        }
    ]


def test_geometry_params_point(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, f'''
    d | r | b | m | property                   | type            | ref
    backends/postgres/dtypes/geometry/point    |                 |
      |   |   | City                           |                 |
      |   |   |   | name                       | string          |
      |   |   |   | coordinates                | geometry(point) |
    ''', backend=postgresql, request=request)

    ns: str = 'backends/postgres/dtypes/geometry/point'
    model: str = f'{ns}/City'

    store: Store = context.get('store')
    backend = cast(PostgreSQL, store.backends['default'])
    coordinates = cast(sa.Column, backend.tables[model].columns['coordinates'])
    assert coordinates.type.srid == -1
    assert coordinates.type.geometry_type == 'POINT'

    app = create_test_client(context)
    app.authmodel(model, [
        'insert',
        'getall',
    ])

    # Write data
    resp = app.post(f'/{model}', json={
        'name': "Vilnius",
        'coordinates': 'POINT(54.6870458 25.2829111)',
    })
    assert resp.status_code == 201

    # Read data
    resp = app.get(f'/{model}')
    assert listdata(resp, full=True) == [
        {
            'name': "Vilnius",
            'coordinates': 'POINT (54.6870458 25.2829111)',
        }
    ]


def test_geometry_params_srid(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, f'''
    d | r | b | m | property                   | type           | ref
    backends/postgres/dtypes/geometry/srid     |                |
      |   |   | City                           |                |
      |   |   |   | name                       | string         |
      |   |   |   | coordinates                | geometry(3346) |
    ''', backend=postgresql, request=request)

    ns: str = 'backends/postgres/dtypes/geometry/srid'
    model: str = f'{ns}/City'

    store: Store = context.get('store')
    backend = cast(PostgreSQL, store.backends['default'])
    coordinates = cast(sa.Column, backend.tables[model].columns['coordinates'])
    assert coordinates.type.srid == 3346
    assert coordinates.type.geometry_type == 'GEOMETRY'

    app = create_test_client(context)
    app.authmodel(model, [
        'insert',
        'getall',
    ])

    # Write data
    resp = app.post(f'/{model}', json={
        'name': "Vilnius",
        'coordinates': 'SRID=3346;POINT(582710 6061887)',
    })
    assert resp.status_code == 201

    # Read data
    resp = app.get(f'/{model}')
    assert listdata(resp, full=True) == [
        {
            'name': "Vilnius",
            'coordinates': 'POINT (582710 6061887)',
        }
    ]
