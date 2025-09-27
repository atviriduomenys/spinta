import urllib
from typing import cast
from typing import Optional
from pathlib import Path

import xml.etree.ElementTree as ET

import sqlalchemy as sa

import pytest
from pytest import FixtureRequest

import shapely.wkt
from geoalchemy2 import shape

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.components import Store
from spinta.core.enums import Action
from spinta.core.config import RawConfig
from spinta.formats.html.components import Cell, Html
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest, load_manifest_and_context
from spinta.testing.request import render_data
from spinta.testing.manifest import load_manifest
from spinta.testing.manifest import load_manifest_get_context
from spinta.testing.types.geometry import round_point, round_url
from spinta.types.geometry.constants import WGS84, LKS94
from spinta.types.geometry.helpers import is_crs_always_xy, get_crs_bounding_area


@pytest.mark.manifests("internal_sql", "csv")
def test_geometry(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property          | type     | ref
    backends/postgres/dtypes/geometry |          |
      |   |   | City                  |          |
      |   |   |   | name              | string   |
      |   |   |   | coordinates       | geometry |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authmodel(
        "backends/postgres/dtypes/geometry/City",
        [
            "insert",
            "getall",
        ],
    )

    # Write data
    resp = app.post(
        "/backends/postgres/dtypes/geometry/City",
        json={
            "name": "Vilnius",
            "coordinates": "POINT(54.6870458 25.2829111)",
        },
    )
    assert resp.status_code == 201

    # Read data
    resp = app.get("/backends/postgres/dtypes/geometry/City")
    assert listdata(resp, full=True) == [
        {
            "name": "Vilnius",
            "coordinates": "POINT (54.6870458 25.2829111)",
        }
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_geometry_params_point(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property                   | type            | ref
    backends/postgres/dtypes/geometry/point    |                 |
      |   |   | City                           |                 |
      |   |   |   | name                       | string          |
      |   |   |   | coordinates                | geometry(point) |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    ns: str = "backends/postgres/dtypes/geometry/point"
    model: str = f"{ns}/City"

    store: Store = context.get("store")
    backend = cast(PostgreSQL, store.backends["default"])
    coordinates = cast(sa.Column, backend.tables[model].columns["coordinates"])
    assert coordinates.type.srid == 4326
    assert coordinates.type.geometry_type == "POINT"

    app = create_test_client(context)
    app.authmodel(
        model,
        [
            "insert",
            "getall",
        ],
    )

    # Write data
    resp = app.post(
        f"/{model}",
        json={
            "name": "Vilnius",
            "coordinates": "POINT (54.6870458 25.2829111)",
        },
    )
    assert resp.status_code == 201

    # Read data
    resp = app.get(f"/{model}")
    assert listdata(resp, full=True) == [
        {
            "name": "Vilnius",
            "coordinates": "POINT (54.6870458 25.2829111)",
        }
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_geometry_params_srid(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property                   | type           | ref
    backends/postgres/dtypes/geometry/srid     |                |
      |   |   | City                           |                |
      |   |   |   | name                       | string         |
      |   |   |   | coordinates                | geometry(3346) |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    ns: str = "backends/postgres/dtypes/geometry/srid"
    model: str = f"{ns}/City"
    store: Store = context.get("store")
    backend = cast(PostgreSQL, store.backends["default"])
    coordinates = cast(sa.Column, backend.tables[model].columns["coordinates"])
    assert coordinates.type.srid == 3346
    assert coordinates.type.geometry_type == "GEOMETRY"

    app = create_test_client(context)
    app.authmodel(
        model,
        [
            "insert",
            "getall",
        ],
    )

    # Write data
    resp = app.post(
        f"/{model}",
        json={
            "name": "Vilnius",
            "coordinates": "SRID=3346;POINT(6000000 180000)",
        },
    )
    assert resp.status_code == 201


@pytest.mark.manifests("internal_sql", "csv")
def test_geometry_html(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property                   | type           | ref   | description
    example                                    |                |       |
      |   |   | City                           |                |       |
      |   |   |   | name                       | string         |       |
      |   |   |   | coordinates                | geometry(4326) |       | WGS
    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    result = render_data(
        context,
        manifest,
        "example/City",
        None,
        accept="text/html",
        data={
            "_id": "19e4f199-93c5-40e5-b04e-a575e81ac373",
            "_page": b"encoded",
            "_revision": "b6197bb7-3592-4cdb-a61c-5a618f44950c",
            "name": "Vilnius",
            "coordinates": "POLYGON",
        },
    )
    assert result == {
        "_id": Cell(
            value="19e4f199",
            link="/example/City/19e4f199-93c5-40e5-b04e-a575e81ac373",
            color=None,
        ),
        "name": Cell(
            value="Vilnius",
        ),
        "coordinates": Cell(
            value="POLYGON",
            link=None,
            color=None,
        ),
    }


osm_url = "https://www.openstreetmap.org/?mlat=54.685691&mlon=25.286688#map=19/54.685691/25.286688"


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "wkt, srid, link",
    [
        ("POINT (6061789 582964)", LKS94, osm_url),
        ("POINT (54.685691 25.286688)", WGS84, osm_url),
    ],
)
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
        dtype = f"geometry(point, {srid})"
    else:
        dtype = "geometry(point)"

    context, manifest = load_manifest_and_context(
        rc,
        f"""
        d | r | b | m | property                   | type    | ref   | description
        example                                    |         |       |
          |   |   | City                           |         |       |
          |   |   |   | name                       | string  |       |
          |   |   |   | coordinates                | {dtype} |       |
        """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )

    model = commands.get_model(context, manifest, "example/City")
    prop = model.properties["coordinates"]

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
    assert round_point(result.value, 6) == wkt
    assert round_url(result.link, 6) == link


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "wkt, display",
    [
        ("POINT (25.282 54.681)", "POINT (25.282 54.681)"),
        ("POLYGON ((25.28 54.68, 25.29 54.69, 25.38 54.64, 25.28 54.68))", "POLYGON"),
        ("LINESTRING (25.28 54.68, 25.29 54.69)", "LINESTRING"),
    ],
)
def test_geometry_wkt_value_shortening(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    wkt: str,
    display: str,
):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property    | type           | ref | description
    example                     |                |     |
      |   |   | City            |                |     |
      |   |   |   | name        | string         |     |
      |   |   |   | coordinates | geometry(4326) |     | WGS
    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )
    model = commands.get_model(context, manifest, "example/City")
    prop = model.properties["coordinates"]

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


@pytest.mark.manifests("internal_sql", "csv")
def test_loading(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
):
    table = """
    d | r | b | m | property | type                  | ref  | access
    datasets/gov/example     |                       |      | open
                             |                       |      |
      |   |   | City         |                       | name | open
      |   |   |   | name     | string                |      | open
      |   |   |   | country  | geometry(point, 3346) |      | open
    """
    manifest = load_manifest(rc, table, manifest_type=manifest_type, tmp_path=tmp_path)
    assert manifest == table


@pytest.mark.manifests("internal_sql", "csv")
def test_geometry_params_with_srid_without_srid(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property                | type           | access
        backends/postgres/dtypes/geometry/srid  |                | 
          |   |   | Point                       |                | 
          |   |   |   | point                   | geometry       | open
          |   |   | PointLKS94                  |                | 
          |   |   |   | point                   | geometry(3346) | open
          |   |   | PointWGS84                  |                | 
          |   |   |   | point                   | geometry(4326) | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    ns: str = "backends/postgres/dtypes/geometry/srid"
    model: str = f"{ns}/PointLKS94"
    store: Store = context.get("store")
    backend = cast(PostgreSQL, store.backends["default"])
    coordinates = cast(sa.Column, backend.tables[model].columns["point"])
    assert coordinates.type.srid == 3346
    assert coordinates.type.geometry_type == "GEOMETRY"

    app = create_test_client(context)
    app.authmodel(
        model,
        [
            "insert",
            "getall",
        ],
    )

    resp = app.post(
        f"/{model}",
        json={
            "point": "POINT(6000000 180000)",
        },
    )
    assert resp.status_code == 201

    model: str = f"{ns}/PointWGS84"
    backend = cast(PostgreSQL, store.backends["default"])
    coordinates = cast(sa.Column, backend.tables[model].columns["point"])

    assert coordinates.type.srid == 4326
    assert coordinates.type.geometry_type == "GEOMETRY"

    app.authmodel(
        model,
        [
            "insert",
            "getall",
        ],
    )

    resp = app.post(
        f"/{model}",
        json={
            "point": "POINT(0 0)",
        },
    )
    assert resp.status_code == 201


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "path",
    [
        # LKS94 (3346) -> WGS84 (4326)  Bell tower of Vilnius Cathedral
        "3346/6061789/582964",
        # WGS84 / Pseudo-Mercator (3857) -> WGS84 (4326)  Bell tower of Vilnius Cathedral
        "3857/2814901/7301104",
        # WGS84 (4326) -> WGS84 (4326)  Bell tower of Vilnius Cathedral
        "4326/54.68569/25.28668",
    ],
)
def test_srid_service(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    path: str,
):
    context = load_manifest_get_context(
        rc,
        """
    d | r | b | m | property | type | ref
    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )

    app = create_test_client(context)

    resp = app.get(f"/_srid/{path}", follow_redirects=False)
    assert resp.status_code == 307

    purl = urllib.parse.urlparse(resp.headers["location"])
    query = urllib.parse.parse_qs(purl.query)
    x, y = query["mlat"][0], query["mlon"][0]
    assert x[:8] == "54.68569"
    assert y[:8] == "25.28668"

    _, x, y = purl.fragment.split("/")
    assert x[:8] == "54.68569"
    assert y[:8] == "25.28668"


@pytest.mark.manifests("internal_sql", "csv")
def test_srid_service_negative_values(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
):
    context = load_manifest_get_context(
        rc,
        """
    d | r | b | m | property | type | ref
    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )

    app = create_test_client(context)

    resp = app.get("/_srid/2770/-68600.50/-16700.10", follow_redirects=False)
    assert resp.status_code == 307

    purl = urllib.parse.urlparse(resp.headers["location"])
    query = urllib.parse.parse_qs(purl.query)
    x, y = query["mlat"][0], query["mlon"][0]
    assert x[:8] == "26.92553"
    assert y[:8] == "-138.789"

    _, x, y = purl.fragment.split("/")
    assert x[:8] == "26.92553"
    assert y[:8] == "-138.789"


@pytest.mark.manifests("internal_sql", "csv")
def test_geometry_delete(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property                | type           | access
        backends/postgres/dtypes/geometry/error  |                | 
          |   |   | Point                       |                | 
          |   |   |   | point                   | geometry       | open
          |   |   |   | number                  | integer        | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    ns: str = "backends/postgres/dtypes/geometry/error"
    model: str = f"{ns}/Point"

    app = create_test_client(context)
    app.authmodel(
        model,
        [
            "insert",
            "delete",
        ],
    )

    resp = app.post(f"/{model}", json={"point": "Point(50 50)"})
    assert resp.status_code == 201

    resp = app.delete(f"/{model}/{resp.json().get('_id')}")
    assert resp.status_code == 204


@pytest.mark.manifests("internal_sql", "csv")
def test_geometry_insert_without_geometry(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property                | type           | access
        backends/postgres/dtypes/geometry/error  |                | 
          |   |   | Point                       |                | 
          |   |   |   | point                   | geometry       | open
          |   |   |   | number                  | integer        | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    ns: str = "backends/postgres/dtypes/geometry/error"
    model: str = f"{ns}/Point"

    app = create_test_client(context)
    app.authmodel(
        model,
        [
            "insert",
        ],
    )

    resp = app.post(f"/{model}", json={"number": 0})
    assert resp.status_code == 201


@pytest.mark.manifests("internal_sql", "csv")
def test_geometry_update_without_geometry(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property                | type           | access
        backends/postgres/dtypes/geometry/error  |                | 
          |   |   | Point                       |                | 
          |   |   |   | point                   | geometry       | open
          |   |   |   | number                  | integer        | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    ns: str = "backends/postgres/dtypes/geometry/error"
    model: str = f"{ns}/Point"

    app = create_test_client(context)
    app.authmodel(model, ["insert", "update", "patch"])

    resp = app.post(f"/{model}", json={"number": 0, "point": "Point(0 0)"})
    assert resp.status_code == 201

    resp = app.patch(
        f"/{model}/{resp.json().get('_id')}", json={"_revision": resp.json().get("_revision"), "number": 1}
    )
    assert resp.status_code == 200

    resp = app.put(f"/{model}/{resp.json().get('_id')}", json={"_revision": resp.json().get("_revision"), "number": 2})
    assert resp.status_code == 200


def test_geometry_write_out_of_bounds(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property                | type           | access
        backends/postgres/dtypes/geometry/bounds  |                | 
          |   |   | Point                       |                | 
          |   |   |   | point                   | geometry(3346)       | open
          |   |   |   | point_1                 | geometry       | open
          |   |   |   | number                  | integer        | open
    """,
        backend=postgresql,
        request=request,
    )

    ns: str = "backends/postgres/dtypes/geometry/bounds"
    model: str = f"{ns}/Point"

    app = create_test_client(context)
    app.authmodel(model, ["insert", "update", "patch"])

    resp = app.post(f"/{model}", json={"number": 0, "point": "Point(0 0)"})
    assert resp.status_code == 400
    assert resp.json()["errors"][0]["code"] == "CoordinatesOutOfRange"

    resp = app.post(f"/{model}", json={"number": 0, "point": "Point(400000 6000000)"})
    assert resp.status_code == 400
    assert resp.json()["errors"][0]["code"] == "CoordinatesOutOfRange"

    resp = app.post(f"/{model}", json={"number": 0, "point_1": "Point(-181 0)"})
    assert resp.status_code == 400
    assert resp.json()["errors"][0]["code"] == "CoordinatesOutOfRange"


def test_geometry_write_within_bounds(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property                | type           | access
        backends/postgres/dtypes/geometry/within |                | 
          |   |   | Point                       |                | 
          |   |   |   | point                   | geometry(3346) | open
          |   |   |   | point_1                 | geometry       | open
          |   |   |   | point_2                 | geometry(3857) | open
          |   |   |   | number                  | integer        | open
    """,
        backend=postgresql,
        request=request,
    )

    ns: str = "backends/postgres/dtypes/geometry/within"
    model: str = f"{ns}/Point"

    app = create_test_client(context)
    app.authmodel(model, ["insert", "update", "patch"])

    resp = app.post(f"/{model}", json={"number": 0, "point": "Point(6000000 400000)"})
    assert resp.status_code == 201

    resp = app.post(f"/{model}", json={"number": 0, "point_1": "Point(0 60)"})
    assert resp.status_code == 201

    resp = app.post(f"/{model}", json={"number": 0, "point_2": "Point(-20000000 20000000)"})
    assert resp.status_code == 201


@pytest.mark.parametrize(
    "srid, result",
    [
        # WGS84 (3857) east, north
        (3857, True),
        # OSGB36 (27700), east, north
        (27700, True),
        # RGF93 (2154), east, north
        (2154, True),
        # ETRS89 (3067), east, north
        (3067, True),
        # CH1903+ / LV95 (2056), east, north
        (2056, True),
        # PSAD56 (24893), east, north
        (24893, True),
        # BD72 (31370), east, north
        (31370, True),
        # MGI (31283), north, east
        (31283, False),
        # WGS84 (4326) north, east
        (4326, False),
        # LKS94 (3346), north, east
        (3346, False),
        # Pulkovo 1942 (3120), north, east
        (3120, False),
        # LKS-92 (3059), north, east
        (3059, False),
        # Estonian Coordinate System of 1992 (3300), north, east
        (3300, False),
        # China Geodetic Coordinate System 2000 (4480), north, east
        (4480, False),
    ],
)
def test_geometry_always_xy_function(srid: int, result: bool):
    assert is_crs_always_xy(srid) == result


@pytest.mark.parametrize(
    "srid, bbox",
    [
        # WGS84 (3857) east, north
        (3857, shapely.geometry.box(minx=-20037508.34, maxx=20037508.34, miny=-20048966.1, maxy=20048966.1)),
        # RGF93 (2154), east, north
        (2154, shapely.geometry.box(minx=-378305.81, maxx=1320649.57, miny=6005281.2, maxy=7235612.72)),
        # WGS84 (4326) north, east
        (4326, shapely.geometry.box(minx=-90.0, maxx=90.0, miny=-180, maxy=180)),
        # LKS94 (3346), north, east
        (3346, shapely.geometry.box(minx=5972477.98, maxx=6268543.17, miny=172763.81, maxy=685350.95)),
        # China Geodetic Coordinate System 2000 (4480), north, east
        (4480, shapely.geometry.box(minx=16.7, maxx=53.56, miny=73.62, maxy=134.77)),
    ],
)
def test_geometry_crs_bounding_area(srid: int, bbox: shapely.geometry.Polygon):
    assert get_crs_bounding_area(srid).equals_exact(bbox, tolerance=0.1)


@pytest.mark.manifests("internal_sql", "csv")
def test_geometry_rdf(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property          | type     | ref
    backends/postgres/dtypes/geometry |          |
      |   |   | City                  |          |
      |   |   |   | name              | string   |
      |   |   |   | coordinates       | geometry |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authmodel(
        "backends/postgres/dtypes/geometry/City",
        [
            "insert",
            "getall",
        ],
    )

    # Write data
    resp = app.post(
        "/backends/postgres/dtypes/geometry/City",
        json={
            "name": "Vilnius",
            "coordinates": "POINT(54.6870458 25.2829111)",
        },
    )
    assert resp.status_code == 201

    # Read data
    resp = app.get("/backends/postgres/dtypes/geometry/City/:format/rdf")
    namespaces = {
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "pav": "http://purl.org/pav/",
        "": "https://testserver/",
    }
    root = ET.fromstring(resp.text)
    rdf_description = root.find("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description")
    id_value = rdf_description.attrib["{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about"].split("/")[-1]
    version_value = rdf_description.attrib["{http://purl.org/pav/}version"]
    page_value = rdf_description.find("_page", namespaces).text

    assert (
        resp.text == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns="https://testserver/">\n'
        f'<rdf:Description rdf:about="/backends/postgres/dtypes/geometry/City/{id_value}" rdf:type="backends/postgres/dtypes/geometry/City" pav:version="{version_value}">\n '
        f" <_page>{page_value}</_page>\n "
        f" <name>Vilnius</name>\n "
        f" <coordinates>POINT (54.6870458 25.2829111)</coordinates>\n"
        f"</rdf:Description>\n"
        f"</rdf:RDF>\n"
    )
