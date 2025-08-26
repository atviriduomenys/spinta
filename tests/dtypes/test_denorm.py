import uuid

import pytest
from _pytest.fixtures import FixtureRequest

from spinta.core.config import RawConfig
from spinta.exceptions import InvalidDenormProperty, RefPropTypeMissmatch
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata, send
from spinta.testing.manifest import bootstrap_manifest


def test_denorm_simple_level_4(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property     | type    | ref     | source | level | access
    datasets/denorm/simple/in    |         |         |        |       |
      |   |   | Country          |         | id      |        |       |
      |   |   |   | id           | integer |         |        |       | open
      |   |   |   | name         | string  |         |        |       | open
      |   |   |   |              |         |         |        |       |              
      |   |   | City             |         |         |        |       |
      |   |   |   | name         | string  |         |        |       | open
      |   |   |   | country      | ref     | Country |        | 4     | open
      |   |   |   | country.id   |         |         |        |       | open
      |   |   |   | country.name |         |         |        |       | open
      
    """,
        backend=postgresql,
        request=request,
    )

    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("datasets/denorm/simple/in/Country", ["insert", "getall", "search"])
    lithuania_id = str(uuid.uuid4())
    latvia_id = str(uuid.uuid4())
    app.post(
        "/datasets/denorm/simple/in/Country",
        json={
            "_id": lithuania_id,
            "id": 0,
            "name": "Lithuania",
        },
    )
    app.post(
        "/datasets/denorm/simple/in/Country",
        json={
            "_id": latvia_id,
            "id": 1,
            "name": "Latvia",
        },
    )

    app.authmodel("datasets/denorm/simple/in/City", ["insert", "getall", "search"])

    vilnius_id = str(uuid.uuid4())
    ryga_id = str(uuid.uuid4())
    app.post(
        "/datasets/denorm/simple/in/City", json={"_id": vilnius_id, "name": "Vilnius", "country": {"_id": lithuania_id}}
    )
    app.post("/datasets/denorm/simple/in/City", json={"_id": ryga_id, "name": "Ryga", "country": {"_id": latvia_id}})

    resp = app.get("/datasets/denorm/simple/in/City")
    assert listdata(resp, sort="country.id", full=True) == [
        {"country._id": lithuania_id, "country.id": 0, "country.name": "Lithuania", "name": "Vilnius"},
        {"country._id": latvia_id, "country.id": 1, "country.name": "Latvia", "name": "Ryga"},
    ]

    resp = app.get("/datasets/denorm/simple/in/City?select(country)")
    assert listdata(resp, sort="country.id", full=True) == [
        {
            "country._id": lithuania_id,
            "country.id": 0,
            "country.name": "Lithuania",
        },
        {
            "country._id": latvia_id,
            "country.id": 1,
            "country.name": "Latvia",
        },
    ]

    resp = app.get("/datasets/denorm/simple/in/City?select(country.id)")
    assert listdata(resp, sort="country.id", full=True) == [
        {
            "country.id": 0,
        },
        {
            "country.id": 1,
        },
    ]


def test_denorm_simple_level_3_error(
    rc: RawConfig,
    postgresql: str,
):
    with pytest.raises(InvalidDenormProperty):
        bootstrap_manifest(
            rc,
            """
        d | r | b | m | property     | type    | ref     | source | level | access
        datasets/denorm/simple/ex    |         |         |        |       |
          |   |   | Country          |         | id      |        |       |
          |   |   |   | id           | integer |         |        |       | open
          |   |   |   | name         | string  |         |        |       | open
          |   |   |   |              |         |         |        |       |              
          |   |   | City             |         |         |        |       |
          |   |   |   | name         | string  |         |        |       | open
          |   |   |   | country      | ref     | Country |        | 3     | open
          |   |   |   | country.id   |         |         |        |       | open
          |   |   |   | country.name |         |         |        |       | open
    
        """,
            backend=postgresql,
        )


def test_denorm_simple_level_3_error_type_missmatch(
    rc: RawConfig,
    postgresql: str,
):
    with pytest.raises(RefPropTypeMissmatch):
        bootstrap_manifest(
            rc,
            """
        d | r | b | m | property     | type    | ref     | source | level | access
        datasets/denorm/simple/ex    |         |         |        |       |
          |   |   | Country          |         | id      |        |       |
          |   |   |   | id           | integer |         |        |       | open
          |   |   |   | name         | string  |         |        |       | open
          |   |   |   |              |         |         |        |       |              
          |   |   | City             |         |         |        |       |
          |   |   |   | name         | string  |         |        |       | open
          |   |   |   | country      | ref     | Country |        | 3     | open
          |   |   |   | country.id   | string  |         |        |       | open
          |   |   |   | country.name |         |         |        |       | open
    
        """,
            backend=postgresql,
        )


def test_denorm_simple_level_3(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property     | type    | ref     | source | level | access
    datasets/denorm/simple/ex    |         |         |        |       |
      |   |   | Country          |         | id      |        |       |
      |   |   |   | id           | integer |         |        |       | open
      |   |   |   | name         | string  |         |        |       | open
      |   |   |   |              |         |         |        |       |              
      |   |   | City             |         |         |        |       |
      |   |   |   | name         | string  |         |        |       | open
      |   |   |   | country      | ref     | Country |        | 3     | open
      |   |   |   | country.id   | integer |         |        |       | open
      |   |   |   | country.name |         |         |        |       | open

    """,
        backend=postgresql,
        request=request,
    )

    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("datasets/denorm/simple/ex/Country", ["insert", "getall", "search"])
    lithuania_id = str(uuid.uuid4())
    latvia_id = str(uuid.uuid4())
    app.post(
        "/datasets/denorm/simple/ex/Country",
        json={
            "_id": lithuania_id,
            "id": 0,
            "name": "Lithuania",
        },
    )
    app.post(
        "/datasets/denorm/simple/ex/Country",
        json={
            "_id": latvia_id,
            "id": 1,
            "name": "Latvia",
        },
    )

    app.authmodel("datasets/denorm/simple/ex/City", ["insert", "getall", "search"])

    vilnius_id = str(uuid.uuid4())
    ryga_id = str(uuid.uuid4())
    app.post("/datasets/denorm/simple/ex/City", json={"_id": vilnius_id, "name": "Vilnius", "country": {"id": 0}})
    app.post("/datasets/denorm/simple/ex/City", json={"_id": ryga_id, "name": "Ryga", "country": {"id": 1}})

    resp = app.get("/datasets/denorm/simple/ex/City")
    assert listdata(resp, sort="country.id", full=True) == [
        {"country.id": 0, "country.name": "Lithuania", "name": "Vilnius"},
        {"country.id": 1, "country.name": "Latvia", "name": "Ryga"},
    ]

    resp = app.get("/datasets/denorm/simple/ex/City?select(country)")
    assert listdata(resp, sort="country.id", full=True) == [
        {
            "country.id": 0,
            "country.name": "Lithuania",
        },
        {
            "country.id": 1,
            "country.name": "Latvia",
        },
    ]

    resp = app.get("/datasets/denorm/simple/ex/City?select(country.id)")
    assert listdata(resp, sort="country.id", full=True) == [
        {
            "country.id": 0,
        },
        {
            "country.id": 1,
        },
    ]


def test_denorm_simple_override_and_new_property(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property     | type    | ref     | source | level | access
    datasets/denorm/simple/over  |         |         |        |       |
      |   |   | Country          |         | id      |        |       |
      |   |   |   | id           | integer |         |        |       | open
      |   |   |   | name         | string  |         |        |       | open
      |   |   |   |              |         |         |        |       |              
      |   |   | City             |         |         |        |       |
      |   |   |   | name         | string  |         |        |       | open
      |   |   |   | country      | ref     | Country |        | 4     | open
      |   |   |   | country.id   |         |         |        |       | open
      |   |   |   | country.name | string  |         |        |       | open
      |   |   |   | country.loc  | string  |         |        |       | open

    """,
        backend=postgresql,
        request=request,
    )

    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("datasets/denorm/simple/over/Country", ["insert", "getall", "search"])
    lithuania_id = str(uuid.uuid4())
    latvia_id = str(uuid.uuid4())
    app.post(
        "/datasets/denorm/simple/over/Country",
        json={
            "_id": lithuania_id,
            "id": 0,
            "name": "Lithuania",
        },
    )
    app.post(
        "/datasets/denorm/simple/over/Country",
        json={
            "_id": latvia_id,
            "id": 1,
            "name": "Latvia",
        },
    )

    app.authmodel("datasets/denorm/simple/over/City", ["insert", "getall", "search"])

    vilnius_id = str(uuid.uuid4())
    ryga_id = str(uuid.uuid4())
    app.post(
        "/datasets/denorm/simple/over/City",
        json={
            "_id": vilnius_id,
            "name": "Vilnius",
            "country": {"_id": lithuania_id, "name": "Lietuva", "loc": "10.0 10.5"},
        },
    )
    app.post(
        "/datasets/denorm/simple/over/City",
        json={
            "_id": ryga_id,
            "name": "Ryga",
            "country": {
                "_id": latvia_id,
            },
        },
    )

    resp = app.get("/datasets/denorm/simple/over/City")
    assert listdata(resp, sort="country.id", full=True) == [
        {
            "country._id": lithuania_id,
            "country.id": 0,
            "country.name": "Lietuva",
            "country.loc": "10.0 10.5",
            "name": "Vilnius",
        },
        {"country._id": latvia_id, "country.id": 1, "country.name": None, "country.loc": None, "name": "Ryga"},
    ]

    resp = app.get("/datasets/denorm/simple/over/City?select(country)")
    assert listdata(resp, sort="country.id", full=True) == [
        {
            "country._id": lithuania_id,
            "country.id": 0,
            "country.name": "Lietuva",
            "country.loc": "10.0 10.5",
        },
        {
            "country._id": latvia_id,
            "country.id": 1,
            "country.name": None,
            "country.loc": None,
        },
    ]

    resp = app.get("/datasets/denorm/simple/over/City?select(country.id, country.name, country.loc)")
    assert listdata(resp, sort="country.id", full=True) == [
        {
            "country.id": 0,
            "country.name": "Lietuva",
            "country.loc": "10.0 10.5",
        },
        {
            "country.id": 1,
            "country.name": None,
            "country.loc": None,
        },
    ]


def test_denorm_nested_level_4(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property            | type    | ref     | source | level | access
    datasets/denorm/nested/in           |         |         |        |       |
      |   |   | Planet                  |         | id      |        |       |
      |   |   |   | id                  | integer |         |        |       | open
      |   |   |   | name                | string  |         |        |       | open
      |   |   |   |                     |         |         |        |       |           
      |   |   | Country                 |         | id      |        |       |
      |   |   |   | id                  | integer |         |        |       | open
      |   |   |   | name                | string  |         |        |       | open
      |   |   |   | planet              | ref     | Planet  |        | 4     | open
      |   |   |   |                     |         |         |        |       |              
      |   |   | City                    |         |         |        |       |
      |   |   |   | name                | string  |         |        |       | open
      |   |   |   | country             | ref     | Country |        | 4     | open
      |   |   |   | country.id          |         |         |        |       | open
      |   |   |   | country.name        |         |         |        |       | open
      |   |   |   | country.planet.name |         |         |        |       | open

    """,
        backend=postgresql,
        request=request,
    )

    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("datasets/denorm/nested/in/Planet", ["insert", "getall", "search"])
    earth_id = str(uuid.uuid4())
    mars_id = str(uuid.uuid4())
    app.post(
        "/datasets/denorm/nested/in/Planet",
        json={
            "_id": earth_id,
            "id": 0,
            "name": "Earth",
        },
    )
    app.post(
        "/datasets/denorm/nested/in/Planet",
        json={
            "_id": mars_id,
            "id": 1,
            "name": "Mars",
        },
    )

    app.authmodel("datasets/denorm/nested/in/Country", ["insert", "getall", "search"])

    lithuania_id = str(uuid.uuid4())
    latvia_id = str(uuid.uuid4())
    app.post(
        "/datasets/denorm/nested/in/Country",
        json={"_id": lithuania_id, "id": 0, "name": "Lithuania", "planet": {"_id": earth_id}},
    )
    app.post(
        "/datasets/denorm/nested/in/Country",
        json={"_id": latvia_id, "id": 1, "name": "Latvia", "planet": {"_id": mars_id}},
    )

    app.authmodel("datasets/denorm/nested/in/City", ["insert", "getall", "search"])

    vilnius_id = str(uuid.uuid4())
    ryga_id = str(uuid.uuid4())
    app.post(
        "/datasets/denorm/nested/in/City", json={"_id": vilnius_id, "name": "Vilnius", "country": {"_id": lithuania_id}}
    )
    app.post("/datasets/denorm/nested/in/City", json={"_id": ryga_id, "name": "Ryga", "country": {"_id": latvia_id}})

    resp = app.get("/datasets/denorm/nested/in/City")
    assert listdata(resp, sort="country.id", full=True) == [
        {
            "country._id": lithuania_id,
            "country.id": 0,
            "country.name": "Lithuania",
            "country.planet.name": "Earth",
            "name": "Vilnius",
        },
        {
            "country._id": latvia_id,
            "country.id": 1,
            "country.name": "Latvia",
            "country.planet.name": "Mars",
            "name": "Ryga",
        },
    ]

    resp = app.get("/datasets/denorm/nested/in/City?select(country)")
    assert listdata(resp, sort="country.id", full=True) == [
        {
            "country._id": lithuania_id,
            "country.id": 0,
            "country.name": "Lithuania",
            "country.planet.name": "Earth",
        },
        {
            "country._id": latvia_id,
            "country.id": 1,
            "country.name": "Latvia",
            "country.planet.name": "Mars",
        },
    ]

    resp = app.get("/datasets/denorm/nested/in/City?select(country.id, country.planet)")
    assert listdata(resp, sort="country.id", full=True) == [
        {
            "country.id": 0,
            "country.planet.name": "Earth",
        },
        {
            "country.id": 1,
            "country.planet.name": "Mars",
        },
    ]


def test_denorm_nested_level_4_override(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property            | type    | ref     | source | level | access
    datasets/denorm/nested/over/in      |         |         |        |       |
      |   |   | Planet                  |         | id      |        |       |
      |   |   |   | id                  | integer |         |        |       | open
      |   |   |   | name                | string  |         |        |       | open
      |   |   |   |                     |         |         |        |       |           
      |   |   | Country                 |         | id      |        |       |
      |   |   |   | id                  | integer |         |        |       | open
      |   |   |   | name                | string  |         |        |       | open
      |   |   |   | planet              | ref     | Planet  |        | 4     | open
      |   |   |   |                     |         |         |        |       |              
      |   |   | City                    |         |         |        |       |
      |   |   |   | name                | string  |         |        |       | open
      |   |   |   | country             | ref     | Country |        | 4     | open
      |   |   |   | country.id          |         |         |        |       | open
      |   |   |   | country.name        |         |         |        |       | open
      |   |   |   | country.planet      | ref     | Planet  |        |       | open
      |   |   |   | country.planet.name |         |         |        |       | open

    """,
        backend=postgresql,
        request=request,
    )

    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("datasets/denorm/nested/over/in/Planet", ["insert", "getall", "search"])
    earth_id = str(uuid.uuid4())
    mars_id = str(uuid.uuid4())
    app.post(
        "/datasets/denorm/nested/over/in/Planet",
        json={
            "_id": earth_id,
            "id": 0,
            "name": "Earth",
        },
    )
    app.post(
        "/datasets/denorm/nested/over/in/Planet",
        json={
            "_id": mars_id,
            "id": 1,
            "name": "Mars",
        },
    )

    app.authmodel("datasets/denorm/nested/over/in/Country", ["insert", "getall", "search"])

    lithuania_id = str(uuid.uuid4())
    latvia_id = str(uuid.uuid4())
    app.post(
        "/datasets/denorm/nested/over/in/Country",
        json={"_id": lithuania_id, "id": 0, "name": "Lithuania", "planet": {"_id": earth_id}},
    )
    app.post(
        "/datasets/denorm/nested/over/in/Country",
        json={"_id": latvia_id, "id": 1, "name": "Latvia", "planet": {"_id": mars_id}},
    )

    app.authmodel("datasets/denorm/nested/over/in/City", ["insert", "getall", "search"])

    vilnius_id = str(uuid.uuid4())
    ryga_id = str(uuid.uuid4())
    app.post(
        "/datasets/denorm/nested/over/in/City",
        json={"_id": vilnius_id, "name": "Vilnius", "country": {"_id": lithuania_id}},
    )
    app.post(
        "/datasets/denorm/nested/over/in/City",
        json={"_id": ryga_id, "name": "Ryga", "country": {"_id": latvia_id, "planet": {"_id": earth_id}}},
    )

    resp = app.get("/datasets/denorm/nested/over/in/City")
    assert listdata(resp, sort="country.id", full=True) == [
        {
            "country._id": lithuania_id,
            "country.id": 0,
            "country.name": "Lithuania",
            "country.planet": None,
            "name": "Vilnius",
        },
        {
            "country._id": latvia_id,
            "country.id": 1,
            "country.name": "Latvia",
            "country.planet._id": earth_id,
            "country.planet.name": "Earth",
            "name": "Ryga",
        },
    ]

    resp = app.get("/datasets/denorm/nested/over/in/City?select(country)")
    assert listdata(resp, sort="country.id", full=True) == [
        {
            "country._id": lithuania_id,
            "country.id": 0,
            "country.name": "Lithuania",
            "country.planet": None,
        },
        {
            "country._id": latvia_id,
            "country.id": 1,
            "country.name": "Latvia",
            "country.planet._id": earth_id,
            "country.planet.name": "Earth",
        },
    ]

    resp = app.get("/datasets/denorm/nested/over/in/City?select(country.id, country.planet)")
    assert listdata(resp, sort="country.id", full=True) == [
        {
            "country.id": 0,
            "country.planet": None,
        },
        {
            "country.id": 1,
            "country.planet._id": earth_id,
            "country.planet.name": "Earth",
        },
    ]


def test_denorm_nested_level_3(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property            | type    | ref     | source | level | access
    datasets/denorm/nested/ex           |         |         |        |       |
      |   |   | Planet                  |         | id      |        |       |
      |   |   |   | id                  | integer |         |        |       | open
      |   |   |   | name                | string  |         |        |       | open
      |   |   |   |                     |         |         |        |       |           
      |   |   | Country                 |         | id      |        |       |
      |   |   |   | id                  | integer |         |        |       | open
      |   |   |   | name                | string  |         |        |       | open
      |   |   |   | planet              | ref     | Planet  |        | 3     | open
      |   |   |   |                     |         |         |        |       |              
      |   |   | City                    |         |         |        |       |
      |   |   |   | name                | string  |         |        |       | open
      |   |   |   | country             | ref     | Country |        | 3     | open
      |   |   |   | country.id          | integer |         |        |       | open
      |   |   |   | country.name        |         |         |        |       | open
      |   |   |   | country.planet.name |         |         |        |       | open

    """,
        backend=postgresql,
        request=request,
    )

    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("datasets/denorm/nested/ex/Planet", ["insert", "getall", "search"])
    earth_id = str(uuid.uuid4())
    mars_id = str(uuid.uuid4())
    app.post(
        "/datasets/denorm/nested/ex/Planet",
        json={
            "_id": earth_id,
            "id": 0,
            "name": "Earth",
        },
    )
    app.post(
        "/datasets/denorm/nested/ex/Planet",
        json={
            "_id": mars_id,
            "id": 1,
            "name": "Mars",
        },
    )

    app.authmodel("datasets/denorm/nested/ex/Country", ["insert", "getall", "search"])

    lithuania_id = str(uuid.uuid4())
    latvia_id = str(uuid.uuid4())
    app.post(
        "/datasets/denorm/nested/ex/Country",
        json={"_id": lithuania_id, "id": 0, "name": "Lithuania", "planet": {"id": 0}},
    )
    app.post(
        "/datasets/denorm/nested/ex/Country", json={"_id": latvia_id, "id": 1, "name": "Latvia", "planet": {"id": 1}}
    )

    app.authmodel("datasets/denorm/nested/ex/City", ["insert", "getall", "search"])

    vilnius_id = str(uuid.uuid4())
    ryga_id = str(uuid.uuid4())
    app.post("/datasets/denorm/nested/ex/City", json={"_id": vilnius_id, "name": "Vilnius", "country": {"id": 0}})
    app.post("/datasets/denorm/nested/ex/City", json={"_id": ryga_id, "name": "Ryga", "country": {"id": 1}})

    resp = app.get("/datasets/denorm/nested/ex/City")
    assert listdata(resp, sort="country.id", full=True) == [
        {"country.id": 0, "country.name": "Lithuania", "country.planet.name": "Earth", "name": "Vilnius"},
        {"country.id": 1, "country.name": "Latvia", "country.planet.name": "Mars", "name": "Ryga"},
    ]

    resp = app.get("/datasets/denorm/nested/ex/City?select(country)")
    assert listdata(resp, sort="country.id", full=True) == [
        {
            "country.id": 0,
            "country.name": "Lithuania",
            "country.planet.name": "Earth",
        },
        {
            "country.id": 1,
            "country.name": "Latvia",
            "country.planet.name": "Mars",
        },
    ]

    resp = app.get("/datasets/denorm/nested/ex/City?select(country.id, country.planet)")
    assert listdata(resp, sort="country.id", full=True) == [
        {
            "country.id": 0,
            "country.planet.name": "Earth",
        },
        {
            "country.id": 1,
            "country.planet.name": "Mars",
        },
    ]


def test_denorm_nested_level_3_multi(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property            | type    | ref      | source | level | access
    datasets/denorm/nested/multi        |         |          |        |       |
      |   |   | Planet                  |         | id, name |        |       |
      |   |   |   | id                  | integer |          |        |       | open
      |   |   |   | name                | string  |          |        |       | open
      |   |   |   | code                | string  |          |        |       | open
      |   |   |   |                     |         |          |        |       |           
      |   |   | Country                 |         | id       |        |       |
      |   |   |   | id                  | integer |          |        |       | open
      |   |   |   | name                | string  |          |        |       | open
      |   |   |   | planet              | ref     | Planet   |        | 3     | open
      |   |   |   |                     |         |          |        |       |              
      |   |   | City                    |         |          |        |       |
      |   |   |   | name                | string  |          |        |       | open
      |   |   |   | country             | ref     | Country  |        | 3     | open
      |   |   |   | country.id          | integer |          |        |       | open
      |   |   |   | country.name        |         |          |        |       | open
      |   |   |   | country.planet.name | string  |          |        |       | open
      |   |   |   | country.planet.code |         |          |        |       | open

    """,
        backend=postgresql,
        request=request,
    )

    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("datasets/denorm/nested/multi/Planet", ["insert", "getall", "search"])
    earth_id = str(uuid.uuid4())
    mars_id = str(uuid.uuid4())
    app.post(
        "/datasets/denorm/nested/multi/Planet",
        json={
            "_id": earth_id,
            "id": 0,
            "name": "Earth",
            "code": "ER",
        },
    )
    app.post(
        "/datasets/denorm/nested/multi/Planet",
        json={
            "_id": mars_id,
            "id": 0,
            "name": "Mars",
            "code": "MR",
        },
    )

    app.authmodel("datasets/denorm/nested/multi/Country", ["insert", "getall", "search"])

    lithuania_id = str(uuid.uuid4())
    latvia_id = str(uuid.uuid4())
    estonia_id = str(uuid.uuid4())
    app.post(
        "/datasets/denorm/nested/multi/Country",
        json={"_id": lithuania_id, "id": 0, "name": "Lithuania", "planet": {"id": 0, "name": "Earth"}},
    )
    app.post(
        "/datasets/denorm/nested/multi/Country",
        json={"_id": latvia_id, "id": 1, "name": "Latvia", "planet": {"id": 0, "name": "Mars"}},
    )
    app.post(
        "/datasets/denorm/nested/multi/Country",
        json={"_id": estonia_id, "id": 2, "name": "Estonia", "planet": {"id": 0, "name": "Mars"}},
    )

    app.authmodel("datasets/denorm/nested/multi/City", ["insert", "getall", "search"])

    vilnius_id = str(uuid.uuid4())
    ryga_id = str(uuid.uuid4())
    talin_id = str(uuid.uuid4())
    app.post("/datasets/denorm/nested/multi/City", json={"_id": vilnius_id, "name": "Vilnius", "country": {"id": 0}})
    app.post(
        "/datasets/denorm/nested/multi/City",
        json={"_id": ryga_id, "name": "Ryga", "country": {"id": 1, "planet": {"name": "Earth"}}},
    )
    app.post(
        "/datasets/denorm/nested/multi/City",
        json={"_id": talin_id, "name": "Talin", "country": {"id": 2, "planet": {"name": "Mars"}}},
    )

    resp = app.get("/datasets/denorm/nested/multi/City")
    assert listdata(resp, sort="country.id", full=True) == [
        {"country.id": 0, "country.name": "Lithuania", "country.planet": None, "name": "Vilnius"},
        {
            "country.id": 1,
            "country.name": "Latvia",
            "country.planet.code": "ER",
            "country.planet.name": "Earth",
            "name": "Ryga",
        },
        {
            "country.id": 2,
            "country.name": "Estonia",
            "country.planet.code": "MR",
            "country.planet.name": "Mars",
            "name": "Talin",
        },
    ]

    resp = app.get("/datasets/denorm/nested/multi/City?select(country)")
    assert listdata(resp, sort="country.id", full=True) == [
        {
            "country.id": 0,
            "country.name": "Lithuania",
            "country.planet": None,
        },
        {
            "country.id": 1,
            "country.name": "Latvia",
            "country.planet.code": "ER",
            "country.planet.name": "Earth",
        },
        {
            "country.id": 2,
            "country.name": "Estonia",
            "country.planet.code": "MR",
            "country.planet.name": "Mars",
        },
    ]

    resp = app.get("/datasets/denorm/nested/multi/City?select(country.id, country.planet)")
    assert listdata(resp, sort="country.id", full=True) == [
        {
            "country.id": 0,
            "country.planet": None,
        },
        {
            "country.id": 1,
            "country.planet.code": "ER",
            "country.planet.name": "Earth",
        },
        {
            "country.id": 2,
            "country.planet.code": "MR",
            "country.planet.name": "Mars",
        },
    ]


def test_denorm_nested_advanced(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property                   | type    | ref     | source | level | access
    datasets/denorm/nested/adv                 |         |         |        |       |
      |   |   | Planet                         |         | id      |        |       |
      |   |   |   | id                         | integer |         |        |       | open
      |   |   |   | name                       | string  |         |        |       | open
      |   |   |   |                            |         |         |        |       |           
      |   |   | Country                        |         | id      |        |       |
      |   |   |   | id                         | integer |         |        |       | open
      |   |   |   | name                       | string  |         |        |       | open
      |   |   |   | planet                     | ref     | Planet  |        | 3     | open
      |   |   |   |                            |         |         |        |       |              
      |   |   | City                           |         |         |        |       |
      |   |   |   | name                       | string  |         |        |       | open
      |   |   |   | country                    | ref     | Country |        | 4     | open
      |   |   |   | country.id                 | integer |         |        |       | open
      |   |   |   | country.name               |         |         |        |       | open
      |   |   |   | country.planet.name        |         |         |        |       | open
      |   |   |   | country.planet.misc        | string  |         |        |       | open
      |   |   |   | country.planet.id          | integer |         |        |       | open

    """,
        backend=postgresql,
        request=request,
    )

    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("datasets/denorm/nested/adv/Planet", ["insert", "getall", "search"])
    earth_id = str(uuid.uuid4())
    mars_id = str(uuid.uuid4())
    app.post(
        "/datasets/denorm/nested/adv/Planet",
        json={
            "_id": earth_id,
            "id": 0,
            "name": "Earth",
        },
    )
    app.post(
        "/datasets/denorm/nested/adv/Planet",
        json={
            "_id": mars_id,
            "id": 1,
            "name": "Mars",
        },
    )

    app.authmodel("datasets/denorm/nested/adv/Country", ["insert", "getall", "search"])

    lithuania_id = "ee0e7f86-bd64-406a-9761-4c1902ab60e4"
    latvia_id = "80aa5977-f5d5-425e-a5a0-413afa5ca5c3"
    estonia_id = "0e163fee-40bd-44c2-a46c-28f952733ee7"
    app.post(
        "/datasets/denorm/nested/adv/Country",
        json={"_id": lithuania_id, "id": 0, "name": "Lithuania", "planet": {"id": 0}},
    )
    app.post(
        "/datasets/denorm/nested/adv/Country", json={"_id": latvia_id, "id": 1, "name": "Latvia", "planet": {"id": 1}}
    )
    app.post(
        "/datasets/denorm/nested/adv/Country", json={"_id": estonia_id, "id": 2, "name": "Estonia", "planet": {"id": 1}}
    )

    app.authmodel("datasets/denorm/nested/adv/City", ["insert", "getall", "search"])

    vilnius_id = str(uuid.uuid4())
    ryga_id = str(uuid.uuid4())
    talin_id = str(uuid.uuid4())
    app.post(
        "/datasets/denorm/nested/adv/City",
        json={"_id": vilnius_id, "name": "Vilnius", "country": {"_id": lithuania_id}},
    )
    app.post("/datasets/denorm/nested/adv/City", json={"_id": ryga_id, "name": "Ryga", "country": {"_id": latvia_id}})
    app.post(
        "/datasets/denorm/nested/adv/City",
        json={"_id": talin_id, "name": "Talin", "country": {"_id": estonia_id, "planet": {"id": 0}}},
    )

    resp = app.get("/datasets/denorm/nested/adv/City")
    assert listdata(resp, sort="name", full=True) == [
        {
            "country._id": latvia_id,
            "country.id": None,
            "country.name": "Latvia",
            "country.planet": None,
            "name": "Ryga",
        },
        {
            "country._id": estonia_id,
            "country.id": None,
            "country.name": "Estonia",
            "country.planet.id": 0,
            "country.planet.misc": None,
            "country.planet.name": "Earth",
            "name": "Talin",
        },
        {
            "country._id": lithuania_id,
            "country.id": None,
            "country.name": "Lithuania",
            "country.planet": None,
            "name": "Vilnius",
        },
    ]

    resp = app.get("/datasets/denorm/nested/adv/City?select(country)")
    assert listdata(resp, sort="country._id", full=True) == [
        {
            "country._id": estonia_id,
            "country.id": None,
            "country.name": "Estonia",
            "country.planet.id": 0,
            "country.planet.misc": None,
            "country.planet.name": "Earth",
        },
        {
            "country._id": latvia_id,
            "country.id": None,
            "country.name": "Latvia",
            "country.planet": None,
        },
        {
            "country._id": lithuania_id,
            "country.id": None,
            "country.name": "Lithuania",
            "country.planet": None,
        },
    ]

    resp = app.get("/datasets/denorm/nested/adv/City?select(name, country.planet)")
    assert listdata(resp, sort="name", full=True) == [
        {"country": None, "name": "Ryga"},
        {"country.planet.id": 0, "country.planet.misc": None, "country.planet.name": "Earth", "name": "Talin"},
        {"country": None, "name": "Vilnius"},
    ]


@pytest.mark.skip("Multi nested doesn't work yet")
def test_denorm_nested_override_and_new_property(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property                   | type    | ref     | source | level | access
    datasets/denorm/nested/over                |         |         |        |       |
      |   |   | Planet                         |         | id      |        |       |
      |   |   |   | id                         | integer |         |        |       | open
      |   |   |   | name                       | string  |         |        |       | open
      |   |   |   |                            |         |         |        |       |           
      |   |   | Country                        |         | id      |        |       |
      |   |   |   | id                         | integer |         |        |       | open
      |   |   |   | name                       | string  |         |        |       | open
      |   |   |   | planet                     | ref     | Planet  |        | 4     | open
      |   |   |   | planet.planet              | ref     | Planet  |        | 3     | open
      |   |   |   |                            |         |         |        |       |              
      |   |   | City                           |         |         |        |       |
      |   |   |   | name                       | string  |         |        |       | open
      |   |   |   | country                    | ref     | Country |        | 4     | open
      |   |   |   | country.id                 | integer |         |        |       | open
      |   |   |   | country.name               |         |         |        |       | open
      |   |   |   | country.planet.name        |         |         |        |       | open
      |   |   |   | country.planet.misc        | string  |         |        |       | open
      |   |   |   | country.planet.planet.name |         |         |        |       | open
      |   |   |   | country.planet.planet.id   | integer |         |        |       | open

    """,
        backend=postgresql,
        request=request,
    )

    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])
    app.authmodel("datasets/denorm/nested/over/Planet", ["insert", "getall", "search"])
    earth_id = str(uuid.uuid4())
    mars_id = str(uuid.uuid4())
    app.post(
        "/datasets/denorm/nested/over/Planet",
        json={
            "_id": earth_id,
            "id": 0,
            "name": "Earth",
        },
    )
    app.post(
        "/datasets/denorm/nested/over/Planet",
        json={
            "_id": mars_id,
            "id": 1,
            "name": "Mars",
        },
    )

    app.authmodel("datasets/denorm/nested/over/Country", ["insert", "getall", "search"])

    lithuania_id = str(uuid.uuid4())
    latvia_id = str(uuid.uuid4())
    app.post(
        "/datasets/denorm/nested/over/Country",
        json={"_id": lithuania_id, "id": 0, "name": "Lithuania", "planet": {"id": 0}},
    )
    app.post(
        "/datasets/denorm/nested/over/Country", json={"_id": latvia_id, "id": 1, "name": "Latvia", "planet": {"id": 1}}
    )

    app.authmodel("datasets/denorm/nested/over/City", ["insert", "getall", "search"])

    vilnius_id = str(uuid.uuid4())
    ryga_id = str(uuid.uuid4())
    app.post("/datasets/denorm/nested/over/City", json={"_id": vilnius_id, "name": "Vilnius", "country": {"id": 0}})
    app.post("/datasets/denorm/nested/over/City", json={"_id": ryga_id, "name": "Ryga", "country": {"id": 1}})

    resp = app.get("/datasets/denorm/nested/over/City")
    assert listdata(resp, sort="country.id", full=True) == [
        {"country.id": 0, "country.name": "Lithuania", "country.planet.name": "Earth", "name": "Vilnius"},
        {"country.id": 1, "country.name": "Latvia", "country.planet.name": "Mars", "name": "Ryga"},
    ]

    resp = app.get("/datasets/denorm/nested/over/City?select(country)")
    assert listdata(resp, sort="country.id", full=True) == [
        {
            "country.id": 0,
            "country.name": "Lithuania",
            "country.planet.name": "Earth",
        },
        {
            "country.id": 1,
            "country.name": "Latvia",
            "country.planet.name": "Mars",
        },
    ]

    resp = app.get("/datasets/denorm/nested/over/City?select(country.id, country.planet)")
    assert listdata(resp, sort="country.id", full=True) == [
        {
            "country.id": 0,
            "country.planet.name": "Earth",
        },
        {
            "country.id": 1,
            "country.planet.name": "Mars",
        },
    ]


def test_denorm_override_wipe(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property            | type    | ref     | source | level | access
    datasets/denorm/wipe                |         |         |        |       |
      |   |   | Planet                  |         | id      |        |       |
      |   |   |   | id                  | integer |         |        |       | open
      |   |   |   | name                | string  |         |        |       | open
      |   |   |   |                     |         |         |        |       |           
      |   |   | Country                 |         | id      |        |       |
      |   |   |   | id                  | integer |         |        |       | open
      |   |   |   | name                | string  |         |        |       | open
      |   |   |   | planet              | ref     | Planet  |        | 4     | open
      |   |   |   | planet.name         | string  |         |        |       | open
      |   |   |   | planet.new          | integer |         |        |       | open
      |   |   |   | planet.id           |         |         |        |       | open

    """,
        backend=postgresql,
        request=request,
    )

    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])

    planet_model = "datasets/denorm/wipe/Planet"
    app.authmodel(planet_model, ["insert", "getall", "search"])
    earth_id = str(uuid.uuid4())
    app.post(planet_model, json={"_id": earth_id, "id": 0, "name": "Earth"})

    country_model = "datasets/denorm/wipe/Country"
    app.authmodel(country_model, ["insert", "getall", "search", "patch"])

    lithuania_id = str(uuid.uuid4())
    lt = send(
        app,
        country_model,
        "insert",
        {
            "_id": lithuania_id,
            "id": 0,
            "name": "Lithuania",
            "planet": {"_id": earth_id, "name": "NEW EARTH", "new": 10},
        },
    )
    resp = app.get(country_model)
    assert listdata(resp, "id", "name", "planet") == [
        (0, "Lithuania", {"_id": earth_id, "id": 0, "name": "NEW EARTH", "new": 10})
    ]

    send(app, country_model, "patch", lt, {"planet": None})

    resp = app.get(country_model)
    assert listdata(resp, "id", "name", "planet") == [(0, "Lithuania", None)]


def test_denorm_nested_override_wipe(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property            | type    | ref     | source | level | access
    datasets/denorm/wipe/nested         |         |         |        |       |
      |   |   | Planet                  |         | id      |        |       |
      |   |   |   | id                  | integer |         |        |       | open
      |   |   |   | name                | string  |         |        |       | open
      |   |   |   |                     |         |         |        |       |           
      |   |   | Country                 |         | id      |        |       |
      |   |   |   | id                  | integer |         |        |       | open
      |   |   |   | name                | string  |         |        |       | open
      |   |   |   | planet              | ref     | Planet  |        | 4     | open
      |   |   |   | planet.name         | string  |         |        |       | open
      |   |   |   | planet.new          | integer |         |        |       | open
      |   |   |   | planet.id           |         |         |        |       | open
      |   |   |   |                     |         |         |        |       |           
      |   |   | City                    |         | id      |        |       |
      |   |   |   | id                  | integer |         |        |       | open
      |   |   |   | name                | string  |         |        |       | open
      |   |   |   | country             | ref     | Country |        | 4     | open
      |   |   |   | country.test        | string  |         |        |       | open
      |   |   |   | country.planet.name |         |         |        |       | open
      |   |   |   | country.planet.test | integer |         |        |       | open

    """,
        backend=postgresql,
        request=request,
    )

    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])

    planet_model = "datasets/denorm/wipe/nested/Planet"
    country_model = "datasets/denorm/wipe/nested/Country"
    city_model = "datasets/denorm/wipe/nested/City"

    app.authmodel(planet_model, ["insert", "getall", "search"])
    earth_id = str(uuid.uuid4())
    app.post(planet_model, json={"_id": earth_id, "id": 0, "name": "Earth"})

    app.authmodel(country_model, ["insert", "getall", "search", "patch"])

    lithuania_id = str(uuid.uuid4())
    app.post(
        country_model,
        json={
            "_id": lithuania_id,
            "id": 0,
            "name": "Lithuania",
            "planet": {"_id": earth_id, "name": "NEW EARTH", "new": 10},
        },
    )
    resp = app.get(country_model)
    assert listdata(resp, "id", "name", "planet") == [
        (0, "Lithuania", {"_id": earth_id, "id": 0, "name": "NEW EARTH", "new": 10})
    ]

    app.authmodel(city_model, ["insert", "getall", "search", "patch"])

    vln = send(
        app,
        city_model,
        "insert",
        {"id": 0, "name": "Vilnius", "country": {"_id": lithuania_id, "test": "NEW", "planet": {"test": 0}}},
    )
    resp = app.get(city_model)
    assert listdata(resp, "id", "name", "country") == [
        (
            0,
            "Vilnius",
            {
                "_id": lithuania_id,
                "test": "NEW",
                "planet": {"name": "Earth", "test": 0},
            },
        )
    ]

    send(app, city_model, "patch", vln, {"country": None})

    resp = app.get(city_model)
    assert listdata(resp, "id", "name", "country") == [(0, "Vilnius", None)]


def test_denorm_external_nested_override_wipe(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property            | type    | ref     | source | level | access
    datasets/denorm/wipe/nested/ex      |         |         |        |       |
      |   |   | Planet                  |         | id      |        |       |
      |   |   |   | id                  | integer |         |        |       | open
      |   |   |   | name                | string  |         |        |       | open
      |   |   |   |                     |         |         |        |       |           
      |   |   | Country                 |         | id      |        |       |
      |   |   |   | id                  | integer |         |        |       | open
      |   |   |   | name                | string  |         |        |       | open
      |   |   |   | planet              | ref     | Planet  |        | 4     | open
      |   |   |   | planet.name         | string  |         |        |       | open
      |   |   |   | planet.new          | integer |         |        |       | open
      |   |   |   | planet.id           |         |         |        |       | open
      |   |   |   |                     |         |         |        |       |           
      |   |   | City                    |         | id      |        |       |
      |   |   |   | id                  | integer |         |        |       | open
      |   |   |   | name                | string  |         |        |       | open
      |   |   |   | country             | ref     | Country |        | 3     | open
      |   |   |   | country.test        | string  |         |        |       | open
      |   |   |   | country.planet.name |         |         |        |       | open
      |   |   |   | country.planet.test | integer |         |        |       | open

    """,
        backend=postgresql,
        request=request,
    )

    app = create_test_client(context)
    app.authorize(["spinta_set_meta_fields"])

    planet_model = "datasets/denorm/wipe/nested/ex/Planet"
    country_model = "datasets/denorm/wipe/nested/ex/Country"
    city_model = "datasets/denorm/wipe/nested/ex/City"

    app.authmodel(planet_model, ["insert", "getall", "search"])
    earth_id = str(uuid.uuid4())
    app.post(planet_model, json={"_id": earth_id, "id": 0, "name": "Earth"})

    app.authmodel(country_model, ["insert", "getall", "search", "patch"])

    lithuania_id = str(uuid.uuid4())
    app.post(
        country_model,
        json={
            "_id": lithuania_id,
            "id": 0,
            "name": "Lithuania",
            "planet": {"_id": earth_id, "name": "NEW EARTH", "new": 10},
        },
    )
    resp = app.get(country_model)
    assert listdata(resp, "id", "name", "planet") == [
        (0, "Lithuania", {"_id": earth_id, "id": 0, "name": "NEW EARTH", "new": 10})
    ]

    app.authmodel(city_model, ["insert", "getall", "search", "patch"])

    vln = send(
        app,
        city_model,
        "insert",
        {"id": 0, "name": "Vilnius", "country": {"id": 0, "test": "NEW", "planet": {"test": 0}}},
    )
    resp = app.get(city_model)
    assert listdata(resp, "id", "name", "country") == [
        (
            0,
            "Vilnius",
            {
                "id": 0,
                "test": "NEW",
                "planet": {"name": "Earth", "test": 0},
            },
        )
    ]

    send(app, city_model, "patch", vln, {"country": None})

    resp = app.get(city_model)
    assert listdata(resp, "id", "name", "country") == [(0, "Vilnius", None)]
