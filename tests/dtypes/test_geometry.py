import urllib
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
from spinta.testing.manifest import load_manifest
from spinta.testing.manifest import load_manifest_get_context
from spinta.types.geometry.constants import WGS84, LKS94


@pytest.mark.manifests('internal_sql', 'csv')
def test_geometry(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property          | type     | ref
    backends/postgres/dtypes/geometry |          |
      |   |   | City                  |          |
      |   |   |   | name              | string   |
      |   |   |   | coordinates       | geometry |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )

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


@pytest.mark.manifests('internal_sql', 'csv')
def test_geometry_params_point(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, f'''
    d | r | b | m | property                   | type            | ref
    backends/postgres/dtypes/geometry/point    |                 |
      |   |   | City                           |                 |
      |   |   |   | name                       | string          |
      |   |   |   | coordinates                | geometry(point) |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )

    ns: str = 'backends/postgres/dtypes/geometry/point'
    model: str = f'{ns}/City'

    store: Store = context.get('store')
    backend = cast(PostgreSQL, store.backends['default'])
    coordinates = cast(sa.Column, backend.tables[model].columns['coordinates'])
    assert coordinates.type.srid == 4326
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


@pytest.mark.manifests('internal_sql', 'csv')
def test_geometry_params_srid(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, f'''
    d | r | b | m | property                   | type           | ref
    backends/postgres/dtypes/geometry/srid     |                |
      |   |   | City                           |                |
      |   |   |   | name                       | string         |
      |   |   |   | coordinates                | geometry(3346) |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )

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
        'coordinates': 'SRID=3346;POINT(180000 6000000)',
    })
    assert resp.status_code == 201


@pytest.mark.manifests('internal_sql', 'csv')
def test_geometry_html(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property                   | type           | ref   | description
    example                                    |                |       |
      |   |   | City                           |                |       |
      |   |   |   | name                       | string         |       |
      |   |   |   | coordinates                | geometry(4326) |       | WGS
    ''', manifest_type=manifest_type, tmp_path=tmp_path)
    result = render_data(
        context, manifest,
        'example/City',
        None,
        accept='text/html',
        data={
            '_id': '19e4f199-93c5-40e5-b04e-a575e81ac373',
            '_page': b'encoded',
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
    'mlat=54.68569111173754&'
    'mlon=25.286688302053335'
    '#map=19/54.68569111173754/25.286688302053335'
)


@pytest.mark.manifests('internal_sql', 'csv')
@pytest.mark.parametrize('wkt, srid, link', [
    ('POINT (6061789 582964)', LKS94, osm_url),
    ('POINT (54.68569111173754 25.286688302053335)', WGS84, osm_url),
])
def test_geometry_coordinate_transformation(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
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
        ''', manifest_type=manifest_type, tmp_path=tmp_path)

    model = commands.get_model(context, manifest, 'example/City')
    prop = model.properties['coordinates']

    value = shapely.wkt.loads(wkt)
    value = shape.from_shape(shape=value, srid=srid or 4326)

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


@pytest.mark.manifests('internal_sql', 'csv')
@pytest.mark.parametrize('wkt, display', [
    ('POINT (25.282 54.681)', 'POINT (25.282 54.681)'),
    ('POLYGON ((25.28 54.68, 25.29 54.69, 25.38 54.64, 25.28 54.68))', 'POLYGON'),
    ('LINESTRING (25.28 54.68, 25.29 54.69)', 'LINESTRING'),
])
def test_geometry_wkt_value_shortening(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    wkt: str,
    display: str,
):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property    | type           | ref | description
    example                     |                |     |
      |   |   | City            |                |     |
      |   |   |   | name        | string         |     |
      |   |   |   | coordinates | geometry(4326) |     | WGS
    ''', manifest_type=manifest_type, tmp_path=tmp_path)
    model = commands.get_model(context, manifest, 'example/City')
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


@pytest.mark.manifests('internal_sql', 'csv')
def test_loading(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
):
    table = '''
    d | r | b | m | property | type                  | ref  | access
    datasets/gov/example     |                       |      | open
                             |                       |      |
      |   |   | City         |                       | name | open
      |   |   |   | name     | string                |      | open
      |   |   |   | country  | geometry(point, 3346) |      | open
    '''
    manifest = load_manifest(rc, table, manifest_type=manifest_type, tmp_path=tmp_path)
    assert manifest == table


@pytest.mark.manifests('internal_sql', 'csv')
def test_geometry_params_with_srid_without_srid(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, f'''
        d | r | b | m | property                | type           | access
        backends/postgres/dtypes/geometry/srid  |                | 
          |   |   | Point                       |                | 
          |   |   |   | point                   | geometry       | open
          |   |   | PointLKS94                  |                | 
          |   |   |   | point                   | geometry(3346) | open
          |   |   | PointWGS84                  |                | 
          |   |   |   | point                   | geometry(4326) | open
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )

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
        'point': 'POINT(180000 6000000)',
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
        'point': 'POINT(0 0)',
    })
    assert resp.status_code == 201


@pytest.mark.manifests('internal_sql', 'csv')
@pytest.mark.parametrize('path', [
    # LKS94 (3346) -> WGS84 (4326)  Bell tower of Vilnius Cathedral
    '3346/6061789/582964',
    # WGS84 / Pseudo-Mercator (3857) -> WGS84 (4326)  Bell tower of Vilnius Cathedral
    '3857/2814901/7301104',
    # WGS84 (4326) -> WGS84 (4326)  Bell tower of Vilnius Cathedral
    '4326/54.68569/25.28668',
])
def test_srid_service(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    path: str,
):
    context = load_manifest_get_context(rc, '''
    d | r | b | m | property | type | ref
    ''', manifest_type=manifest_type, tmp_path=tmp_path)

    app = create_test_client(context)

    resp = app.get(f'/_srid/{path}', follow_redirects=False)
    assert resp.status_code == 307

    purl = urllib.parse.urlparse(resp.headers['location'])
    query = urllib.parse.parse_qs(purl.query)
    x, y = query['mlat'][0], query['mlon'][0]
    assert x[:8] == '54.68569'
    assert y[:8] == '25.28668'

    _, x, y = purl.fragment.split('/')
    assert x[:8] == '54.68569'
    assert y[:8] == '25.28668'


@pytest.mark.manifests('internal_sql', 'csv')
def test_geometry_delete(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, f'''
        d | r | b | m | property                | type           | access
        backends/postgres/dtypes/geometry/error  |                | 
          |   |   | Point                       |                | 
          |   |   |   | point                   | geometry       | open
          |   |   |   | number                  | integer        | open
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )

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
    assert resp.status_code == 201

    resp = app.delete(f'/{model}/{resp.json().get("_id")}')
    assert resp.status_code == 204


@pytest.mark.manifests('internal_sql', 'csv')
def test_geometry_insert_without_geometry(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, f'''
        d | r | b | m | property                | type           | access
        backends/postgres/dtypes/geometry/error  |                | 
          |   |   | Point                       |                | 
          |   |   |   | point                   | geometry       | open
          |   |   |   | number                  | integer        | open
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )

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


@pytest.mark.manifests('internal_sql', 'csv')
def test_geometry_update_without_geometry(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, f'''
        d | r | b | m | property                | type           | access
        backends/postgres/dtypes/geometry/error  |                | 
          |   |   | Point                       |                | 
          |   |   |   | point                   | geometry       | open
          |   |   |   | number                  | integer        | open
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )

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


def test_geometry_write_out_of_bounds(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, f'''
        d | r | b | m | property                | type           | access
        backends/postgres/dtypes/geometry/bounds  |                | 
          |   |   | Point                       |                | 
          |   |   |   | point                   | geometry(3346)       | open
          |   |   |   | point_1                 | geometry       | open
          |   |   |   | number                  | integer        | open
    ''', backend=postgresql, request=request)

    ns: str = 'backends/postgres/dtypes/geometry/bounds'
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
    assert resp.status_code == 400
    assert resp.json()['errors'][0]['code'] == 'CoordinatesOutOfRange'

    resp = app.post(f'/{model}', json={
        "number": 0,
        "point_1": "Point(-181 0)"
    })
    assert resp.status_code == 400
    assert resp.json()['errors'][0]['code'] == 'CoordinatesOutOfRange'
