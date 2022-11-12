from typing import cast

import sqlalchemy as sa

import pytest
from pytest import FixtureRequest

import shapely.geometry as geom
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


def test_geometry_html(rc: RawConfig):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property                   | type           | ref
    example                                    |                |
      |   |   | City                           |                |
      |   |   |   | name                       | string         |
      |   |   |   | coordinates                | geometry(4326) |
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


@pytest.mark.parametrize('value, cell', [
    ({'geom_type': 'Point', 'geom_value': [582710, 6061887], 'srid': 4326},
     Cell(value='POINT (582710 6061887)',
          link='https://www.openstreetmap.org/?mlat=582710.0&mlon=6061887.0#map=19/582710.0/6061887.0')),
    ({'geom_type': 'Polygon', 'geom_value': [[[502640, 6035299], [502633, 6035297], [502631, 6035305]]], 'srid': 4326},
     Cell(value='POLYGON', link='https://www.openstreetmap.org/?mlat=502634.6666666667&mlon=6035300.333333333'
                                '#map=19/502634.6666666667/6035300.333333333')),
])
def test_geometry_coordinates_in_wgs_html(rc: RawConfig, value, cell):
    context, manifest = load_manifest_and_context(rc, f'''
        d | r | b | m | property                   | type           | ref   | description
        example                                    |                |       |
          |   |   | City                           |                |       |
          |   |   |   | name                       | string         |       |
          |   |   |   | coordinates                | geometry(4326) |       | WGS
        ''')
    fmt = Html()
    dtype = manifest.models['example/City'].properties['coordinates'].dtype
    geom_func = getattr(geom, value['geom_type'])
    geometry = geom_func(*value['geom_value'])
    wkb_element = shape.from_shape(shape=geometry, srid=value['srid'])

    result = commands.prepare_dtype_for_response(
        context,
        fmt,
        dtype,
        wkb_element,
        data={},
        action=Action.GETALL,
        select=None,
    )
    assert result.value == cell.value
    assert result.link == cell.link
    assert result.color == cell.color


@pytest.mark.parametrize('value, cell', [
    ({'geom_type': 'Point', 'geom_value': [582710, 6061887], 'srid': 3346},
     Cell(value='POINT (25.282777879597916 54.68661318326901)',
          link='https://www.openstreetmap.org/?mlat=25.282777879597916&mlon=54.68661318326901'
               '#map=19/25.282777879597916/54.68661318326901')),
    ({'geom_type': 'Polygon', 'geom_value': [[[502640, 6035290], [502630, 6035290], [502630, 6035300]]], 'srid': 3346},
     Cell(value='POLYGON', link='https://www.openstreetmap.org/?mlat=24.040608716271&mlon=54.454444306004426'
                                '#map=19/24.040608716271/54.454444306004426')),
])
def test_geometry_coordinates_in_lks_html(rc: RawConfig, value, cell):
    context, manifest = load_manifest_and_context(rc, f'''
        d | r | b | m | property                   | type           | ref   | description
        example                                    |                |       |
          |   |   | City                           |                |       |
          |   |   |   | name                       | string         |       |
          |   |   |   | coordinates                | geometry(3346) |       | LKS
        ''')
    fmt = Html()
    dtype = manifest.models['example/City'].properties['coordinates'].dtype
    geom_func = getattr(geom, value['geom_type'])
    geometry = geom_func(*value['geom_value'])
    wkb_element = shape.from_shape(shape=geometry, srid=value['srid'])

    result = commands.prepare_dtype_for_response(
        context,
        fmt,
        dtype,
        wkb_element,
        data={},
        action=Action.GETALL,
        select=None,
    )
    assert result.value == cell.value
    assert result.link == cell.link
    assert result.color == cell.color
