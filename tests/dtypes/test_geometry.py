from typing import cast
from typing import Optional
from pathlib import Path

import sqlalchemy as sa

import pytest
from pytest import FixtureRequest

import shapely.wkt
from geoalchemy2 import shape

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.components import Store
from spinta.components import Action
from spinta.core.config import RawConfig
from spinta.formats.html.components import Cell, Html
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest, load_manifest_and_context
from spinta.testing.request import render_data
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.manifest import load_manifest
from spinta.types.geometry.constants import WGS84, LKS94


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
        'coordinates': 'POINT (54.6870458 25.2829111)',
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


def test_geometry_html(rc: RawConfig):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property                   | type           | ref   | description
    example                                    |                |       |
      |   |   | City                           |                |       |
      |   |   |   | name                       | string         |       |
      |   |   |   | coordinates                | geometry(4326) |       | WGS
    ''')
    result = render_data(
        context, manifest,
        'example/City',
        None,
        accept='text/html',
        data={
            '_id': '19e4f199-93c5-40e5-b04e-a575e81ac373',
            '_revision': 'b6197bb7-3592-4cdb-a61c-5a618f44950c',
            'name': 'Vilnius',
            'coordinates': 'POLYGON',
        },
    )
    assert result == {
        '_id': Cell(
            value='19e4f199',
            link='/example/City/19e4f199-93c5-40e5-b04e-a575e81ac373',
            color=None,
        ),
        'name': Cell(
            value='Vilnius',
        ),
        'coordinates': Cell(
            value='POLYGON',
            link=None,
            color=None,
        ),
    }

osm_url = (
    'https://www.openstreetmap.org/?'
    'mlat=25.273658402751387&'
    'mlon=54.662851967609136'
    '#map=19/25.273658402751387/54.662851967609136'
)

@pytest.mark.parametrize('wkt, srid, link', [
    ('POINT (582170 6059232)', LKS94, osm_url),
    ('POINT (25.273658402751387 54.662851967609136)', WGS84, osm_url),
    ('POINT (582170 6059232)', None, None),
    ('POINT (25.273658402751387 54.662851967609136)', None, None),
])
def test_geometry_coordinate_tansformation(
    rc: RawConfig,
    wkt: str,
    srid: Optional[int],
    link: Optional[str],
):
    if srid:
        dtype = f'geometry(point, {srid})'
    else:
        dtype = 'geometry(point)'

    context, manifest = load_manifest_and_context(rc, f'''
        d | r | b | m | property                   | type    | ref   | description
        example                                    |         |       |
          |   |   | City                           |         |       |
          |   |   |   | name                       | string  |       |
          |   |   |   | coordinates                | {dtype} |       |
        ''')

    model = manifest.models['example/City']
    prop = model.properties['coordinates']

    value = shapely.wkt.loads(wkt)
    value = shape.from_shape(shape=value, srid=srid or -1)

    result = commands.prepare_dtype_for_response(
        context,
        Html(),
        prop.dtype,
        value,
        data={},
        action=Action.GETALL,
        select=None,
    )
    assert result.value == wkt
    assert result.link == link


@pytest.mark.parametrize('wkt, display', [
    ('POINT (25.282 54.681)', 'POINT (25.282 54.681)'),
    ('POLYGON ((25.28 54.68, 25.29 54.69, 25.38 54.64, 25.28 54.68))', 'POLYGON'),
    ('LINESTRING (25.28 54.68, 25.29 54.69)', 'LINESTRING'),
])
def test_geometry_wkt_value_shortening(
    rc: RawConfig,
    wkt: str,
    display: str,
):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property    | type           | ref | description
    example                     |                |     |
      |   |   | City            |                |     |
      |   |   |   | name        | string         |     |
      |   |   |   | coordinates | geometry(4326) |     | WGS
    ''')
    model = manifest.models['example/City']
    prop = model.properties['coordinates']

    value = shapely.wkt.loads(wkt)
    value = shape.from_shape(shape=value)

    result = commands.prepare_dtype_for_response(
        context,
        Html(),
        prop.dtype,
        value,
        data={},
        action=Action.GETALL,
        select=None,
    )
    assert result.value == display


def test_loading(tmp_path: Path, rc: RawConfig):
    table = '''
    d | r | b | m | property | type                  | ref  | access
    datasets/gov/example     |                       |      | open
                             |                       |      |
      |   |   | City         |                       | name | open
      |   |   |   | name     | string                |      | open
      |   |   |   | country  | geometry(point, 3346) |      | open
    '''
    create_tabular_manifest(tmp_path / 'manifest.csv', table)
    manifest = load_manifest(rc, tmp_path / 'manifest.csv')
    assert manifest == table


def test_geometry_params_with_srid_without_srid(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, f'''
        d | r | b | m | property                | type           | access
        backends/postgres/dtypes/geometry/srid  |                | 
          |   |   | Point                       |                | 
          |   |   |   | point                   | geometry       | open
          |   |   | PointLKS94                  |                | 
          |   |   |   | point                   | geometry(3346) | open
          |   |   | PointWGS84                  |                | 
          |   |   |   | point                   | geometry(4326) | open
    ''', backend=postgresql, request=request)

    ns: str = 'backends/postgres/dtypes/geometry/srid'
    model: str = f'{ns}/PointLKS94'
    store: Store = context.get('store')
    backend = cast(PostgreSQL, store.backends['default'])
    coordinates = cast(sa.Column, backend.tables[model].columns['point'])
    assert coordinates.type.srid == 3346
    assert coordinates.type.geometry_type == 'GEOMETRY'

    app = create_test_client(context)
    app.authmodel(model, [
        'insert',
        'getall',
    ])

    resp = app.post(f'/{model}', json={
        'point': 'POINT(582710 6061887)',
    })
    assert resp.status_code == 201

    model: str = f'{ns}/PointWGS84'
    backend = cast(PostgreSQL, store.backends['default'])
    coordinates = cast(sa.Column, backend.tables[model].columns['point'])

    assert coordinates.type.srid == 4326
    assert coordinates.type.geometry_type == 'GEOMETRY'

    app.authmodel(model, [
        'insert',
        'getall',
    ])

    resp = app.post(f'/{model}', json={
        'point': 'POINT(582710 6061887)',
    })
    assert resp.status_code == 201


def test_geometry_delete(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, f'''
        d | r | b | m | property                | type           | access
        backends/postgres/dtypes/geometry/error  |                | 
          |   |   | Point                       |                | 
          |   |   |   | point                   | geometry       | open
          |   |   |   | number                  | integer        | open
    ''', backend=postgresql, request=request)

    ns: str = 'backends/postgres/dtypes/geometry/error'
    model: str = f'{ns}/Point'

    app = create_test_client(context)
    app.authmodel(model, [
        'insert',
        'delete',
    ])

    resp = app.post(f'/{model}', json={
        "point": "Point(50 50)"
    })
    print(resp.json().get("_id"))
    assert resp.status_code == 201

    resp = app.delete(f'/{model}/{resp.json().get("_id")}')
    assert resp.status_code == 204


def test_geometry_insert_without_geometry(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, f'''
        d | r | b | m | property                | type           | access
        backends/postgres/dtypes/geometry/error  |                | 
          |   |   | Point                       |                | 
          |   |   |   | point                   | geometry       | open
          |   |   |   | number                  | integer        | open
    ''', backend=postgresql, request=request)

    ns: str = 'backends/postgres/dtypes/geometry/error'
    model: str = f'{ns}/Point'

    app = create_test_client(context)
    app.authmodel(model, [
        'insert',
    ])

    resp = app.post(f'/{model}', json={
        'number': 0
    })
    assert resp.status_code == 201


def test_geometry_update_without_geometry(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, f'''
        d | r | b | m | property                | type           | access
        backends/postgres/dtypes/geometry/error  |                | 
          |   |   | Point                       |                | 
          |   |   |   | point                   | geometry       | open
          |   |   |   | number                  | integer        | open
    ''', backend=postgresql, request=request)

    ns: str = 'backends/postgres/dtypes/geometry/error'
    model: str = f'{ns}/Point'

    app = create_test_client(context)
    app.authmodel(model, [
        'insert',
        'update',
        'patch'
    ])

    resp = app.post(f'/{model}', json={
        "number": 0,
        "point": "Point(0 0)"
    })
    assert resp.status_code == 201

    resp = app.patch(f'/{model}/{resp.json().get("_id")}', json={
        "_revision": resp.json().get("_revision"),
        "number": 1
    })
    assert resp.status_code == 200

    resp = app.put(f'/{model}/{resp.json().get("_id")}', json={
        "_revision": resp.json().get("_revision"),
        "number": 2
    })
    assert resp.status_code == 200
