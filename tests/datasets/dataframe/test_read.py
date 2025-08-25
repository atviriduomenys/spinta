import json
from pathlib import Path

import pytest
from fsspec import AbstractFileSystem
from fsspec.implementations.memory import MemoryFileSystem

from spinta.core.enums import Mode
from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import prepare_manifest
from spinta.testing.utils import error


@pytest.fixture
def fs():
    fs = MemoryFileSystem()
    yield fs
    MemoryFileSystem.store.clear()


def test_csv_read(rc: RawConfig, fs: AbstractFileSystem):
    fs.pipe("cities.csv", ("šalis,miestas\nlt,Vilnius\nlv,Ryga\nee,Talin").encode("utf-8"))

    context, manifest = prepare_manifest(
        rc,
        """
    d | r | b | m | property | type     | ref  | source              | prepare | access
    example/csv              |          |      |                     |         |
      | csv                  | dask/csv |      | memory://cities.csv |         |
      |   |   | City         |          | name |                     |         |
      |   |   |   | name     | string   |      | miestas             |         | open
      |   |   |   | country  | string   |      | šalis               |         | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/csv/City", ["getall"])

    resp = app.get("/example/csv/City")
    assert listdata(resp, sort=False) == [
        ("lt", "Vilnius"),
        ("lv", "Ryga"),
        ("ee", "Talin"),
    ]


def test_csv_read_unknown_column(rc: RawConfig, fs: AbstractFileSystem):
    fs.pipe("cities.csv", ("šalis,miestas\nlt,Vilnius\nlv,Ryga\nee,Talin").encode("utf-8"))

    context, manifest = prepare_manifest(
        rc,
        """
    d | r | b | m | property | type     | source              | prepare | access
    example/csv              |          |                     |         |
      | csv                  | dask/csv | memory://cities.csv |         |
      |   |   | City         |          |                     |         |
      |   |   |   | name     | string   | miestas             |         | open
      |   |   |   | country  | string   | salis               |         | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/csv/City", ["getall"])

    resp = app.get("/example/csv/City")
    assert error(resp) == "PropertyNotFound"


def test_csv_read_refs(rc: RawConfig, fs: AbstractFileSystem):
    fs.pipe("countries.csv", ("kodas,pavadinimas\nlt,Lietuva\nlv,Latvija\nee,Estija").encode("utf-8"))

    fs.pipe("cities.csv", ("šalis,miestas\nlt,Vilnius\nlv,Ryga\nee,Talin").encode("utf-8"))

    context, manifest = prepare_manifest(
        rc,
        """
    d | r | b | m | property | type     | ref                            | source                 | prepare | access
    example/csv/countries    |          |                                |                        |         |
      | csv                  | dask/csv |                                | memory://countries.csv |         |
      |   |   | Country      |          | code                           |                        |         |
      |   |   |   | code     | string   |                                | kodas                  |         | open
      |   |   |   | name     | string   |                                | pavadinimas            |         | open
    example/csv/cities       |          |                                |                        |         |
      | csv                  | dask/csv |                                | memory://cities.csv    |         |
      |   |   | City         |          | name                           |                        |         |
      |   |   |   | name     | string   |                                | miestas                |         | open
      |   |   |   | country  | ref      | /example/csv/countries/Country | šalis                  |         | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/csv", ["getall"])

    resp = app.get("/example/csv/countries/Country")
    countries = {c["code"]: c["_id"] for c in listdata(resp, "_id", "code", full=True)}
    assert sorted(countries) == ["ee", "lt", "lv"]

    resp = app.get("/example/csv/cities/City")
    assert listdata(resp, sort=False) == [
        (countries["lt"], "Vilnius"),
        (countries["lv"], "Ryga"),
        (countries["ee"], "Talin"),
    ]


def test_csv_read_multiple_models(rc: RawConfig, fs: AbstractFileSystem):
    fs.pipe("countries.csv", ("kodas,pavadinimas,id\nlt,Lietuva,1\nlv,Latvija,2\nee,Estija,3").encode("utf-8"))

    fs.pipe("cities.csv", ("šalis,miestas\nlt,Vilnius\nlv,Ryga\nee,Talin").encode("utf-8"))

    context, manifest = prepare_manifest(
        rc,
        """
    d | r | b | m | property | type     | ref     | source                 | prepare | access
    example/csv/countries    |          |         |                        |         |
      | csv0                 | dask/csv |         | memory://countries.csv |         |
      |   |   | Country      |          | code    |                        |         |
      |   |   |   | code     | string   |         | kodas                  |         | open
      |   |   |   | name     | string   |         | pavadinimas            |         | open
      |   |   |   | id       | integer  |         | id                     |         | open
                             |          |         |                        |         |
      | csv1                 | dask/csv |         | memory://cities.csv    |         |
      |   |   | City         |          | miestas |                        |         |
      |   |   |   | miestas  | string   |         | miestas                |         | open
      |   |   |   | kodas    | string   |         | šalis                  |         | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/csv", ["getall"])

    resp = app.get("/example/csv/countries/Country")
    assert listdata(resp, sort=False) == [
        ("lt", 1, "Lietuva"),
        ("lv", 2, "Latvija"),
        ("ee", 3, "Estija"),
    ]

    resp = app.get("example/csv/countries/City")
    assert listdata(resp, sort=False) == [
        ("lt", "Vilnius"),
        ("lv", "Ryga"),
        ("ee", "Talin"),
    ]


def test_xml_read(rc: RawConfig, tmp_path: Path):
    xml = """
        <cities>
            <city>
                <code>lt</code>
                <name>Vilnius</name>
            </city>
            <city>
                <code>lv</code>
                <name>Ryga</name>
            </city>
            <city>
                <code>ee</code>
                <name>Talin</name>
            </city>
        </cities>
    """
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property | type     | ref  | source              | prepare | access
    example/xml              |          |      |                     |         |
      | xml                  | dask/xml |      | {path}              |         |
      |   |   | City         |          | name | /cities/city        |         |
      |   |   |   | name     | string   |      | name                |         | open
      |   |   |   | country  | string   |      | code                |         | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/City", ["getall"])

    resp = app.get("/example/xml/City")
    assert listdata(resp, sort=False) == [
        ("lt", "Vilnius"),
        ("lv", "Ryga"),
        ("ee", "Talin"),
    ]


def test_xml_read_with_attributes(rc: RawConfig, tmp_path: Path):
    xml = """
        <cities>
            <city code="lt" name="Vilnius"/>
            <city code="lv" name="Ryga"/>
            <city code="ee" name="Talin"/>
        </cities>
    """
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property | type     | ref  | source              | prepare | access
    example/xml              |          |      |                     |         |
      | xml                  | dask/xml |      | {path}              |         |
      |   |   | City         |          | name | /cities/city        |         |
      |   |   |   | name     | string   |      | @name               |         | open
      |   |   |   | country  | string   |      | @code               |         | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/City", ["getall"])

    resp = app.get("/example/xml/City")
    assert listdata(resp, sort=False) == [
        ("lt", "Vilnius"),
        ("lv", "Ryga"),
        ("ee", "Talin"),
    ]


def test_xml_read_refs_level_3(rc: RawConfig, tmp_path: Path):
    xml = """
        <countries>
            <country>
                <code>lt</code>
                <name>Lietuva</name>
                <cities>
                    <city name="Vilnius"/>
                </cities>
            </country>
            <country>
                <code>lv</code>
                <name>Latvija</name>
                <cities>
                    <city name="Ryga"/>
                </cities>
            </country>
            <country>
                <code>ee</code>
                <name>Estija</name>
                <cities>
                    <city name="Talin"/>
                </cities>
            </country>
        </countries>
    """
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
       d | r | b | m | property | type     | ref     | source                               | prepare | access | level
       example/xml              |          |         |                                      |         |        |
         | xml                  | dask/xml |         | {path}                               |         |        |
         |   |   | City         |          | name    | /countries/country/cities/city       |         |        |
         |   |   |   | name     | string   |         | @name                                |         | open   |
         |   |   |   | code     | ref      | Country | ../../code                           |         | open   | 3
                     |  
         |   |   | Country      |          | country | /countries/country                   |         |        |
         |   |   |   | name     | string   |         | name                                 |         | open   |
         |   |   |   | country  | string   |         | code                                 |         | open   |
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml", ["getall"])

    resp = app.get("/example/xml/City")
    assert listdata(resp, sort=False) == [
        ("lt", "Vilnius"),
        ("lv", "Ryga"),
        ("ee", "Talin"),
    ]


def test_xml_read_refs_level_4(rc: RawConfig, tmp_path: Path):
    xml = """
        <countries>
            <country>
                <code>lt</code>
                <name>Lietuva</name>
                <cities>
                    <city name="Vilnius"/>
                </cities>
            </country>
            <country>
                <code>lv</code>
                <name>Latvija</name>
                <cities>
                    <city name="Ryga"/>
                </cities>
            </country>
            <country>
                <code>ee</code>
                <name>Estija</name>
                <cities>
                    <city name="Talin"/>
                </cities>
            </country>
        </countries>
    """
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
       d | r | b | m | property | type     | ref     | source                               | prepare | access | level
       example/xml              |          |         |                                      |         |        |
         | xml                  | dask/xml |         | {path}                               |         |        |
         |   |   | City         |          | name    | /countries/country/cities/city       |         |        |
         |   |   |   | name     | string   |         | @name                                |         | open   |
         |   |   |   | code     | ref      | Country | ../..                                |         | open   | 4
                     |  
         |   |   | Country      |          | country | /countries/country                   |         |        |
         |   |   |   | name     | string   |         | name                                 |         | open   |
         |   |   |   | country  | string   |         | code                                 |         | open   |
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml", ["getall"])

    resp = app.get("/example/xml/Country")
    countries = {c["country"]: c["_id"] for c in listdata(resp, "_id", "country", full=True)}
    assert sorted(countries) == ["ee", "lt", "lv"]

    resp = app.get("/example/xml/City")
    assert listdata(resp, sort=False) == [
        (countries["lt"], "Vilnius"),
        (countries["lv"], "Ryga"),
        (countries["ee"], "Talin"),
    ]


def test_xml_read_multiple_sources(rc: RawConfig, tmp_path: Path):
    xml0 = """
        <cities>
            <city name="Vilnius" code="lt"/>
            <city name="Ryga" code="lv"/>
            <city name="Talin" code="ee"/>
        </cities>
    """
    path0 = tmp_path / "cities.xml"
    path0.write_text(xml0)

    xml1 = """
        <countries>
            <country code="lt" name="Lietuva"/>
            <country code="lv" name="Latvija"/>
            <country code="ee" name="Estija"/>
        </countries>
    """
    path1 = tmp_path / "countries.xml"
    path1.write_text(xml1)

    context, manifest = prepare_manifest(
        rc,
        f"""
       d | r | b | m | property | type     | ref     | source             | prepare | access | level
       example/xml              |          |         |                    |         |        |
         | xml_city             | dask/xml |         | {path0}            |         |        |
         |   |   | City         |          | name    | /cities/city       |         |        |
         |   |   |   | name     | string   |         | @name              |         | open   |
         |   |   |   | code     | string   |         | @code              |         | open   | 3
                     |  
         | xml_country          | dask/xml |         | {path1}            |         |        |
         |   |   | Country      |          | code    | /countries/country |         |        |
         |   |   |   | name     | string   |         | @name              |         | open   |
         |   |   |   | code     | string   |         | @code              |         | open   |
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml", ["getall"])

    resp = app.get("/example/xml/City")
    assert listdata(resp, sort=False) == [
        ("lt", "Vilnius"),
        ("lv", "Ryga"),
        ("ee", "Talin"),
    ]

    resp = app.get("/example/xml/Country")
    assert listdata(resp, sort=False) == [
        ("lt", "Lietuva"),
        ("lv", "Latvija"),
        ("ee", "Estija"),
    ]


def test_xml_read_with_empty(rc: RawConfig, tmp_path: Path):
    xml = """
        <countries>
            <country code="lt"/>
            <country code="lv" name="Latvija"/>
            <country name="Estija"/>
        </countries>"""
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | m              | property | type     | ref     | source                 | level        
           example/xml                |          |         |                        |
             | resource               | dask/xml |         | {path}                 |
                                      |          |         |                        |
             |   | Country |          |          | code    | /countries/country     |
             |   |         | name     | string   |         | @name                  |
             |   |         | code     | string   |         | @code                  |
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/Country", ["getall"])

    resp = app.get("/example/xml/Country")
    assert listdata(resp, sort=False) == [
        ("lt", None),
        ("lv", "Latvija"),
        (None, "Estija"),
    ]


def test_xml_read_with_empty_nested(rc: RawConfig, tmp_path: Path):
    xml = """
        <countries>
            <country code="lt" name="Lietuva"/>
            <country name="Latvija">
                <location>
                    <lon>3</lon>
                </location>
            </country>
            <country code="ee">
                <location>
                    <lon>5</lon>
                    <lat>4</lat>
                </location>
            </country>
        </countries>"""
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | m              | property  | type     | ref     | source                | level        
           example/xml                 |          |         |                       |
             | resource                | dask/xml |         | {path}                |
                                       |          |         |                       |
             |   | Country |           |          | code    | /countries/country     |
             |   |         | name      | string   |         | @name                  |
             |   |         | code      | string   |         | @code                  |
             |   |         | latitude  | integer  |         | location/lat        |
             |   |         | longitude | integer  |         | location/lon          |
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/Country", ["getall"])

    resp = app.get("/example/xml/Country")
    assert listdata(resp, sort=False) == [
        ("lt", None, None, "Lietuva"),
        (None, None, 3, "Latvija"),
        ("ee", 4, 5, None),
    ]


def test_json_read(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "countries": [
            {
                "code": "lt",
                "name": "Lietuva",
            },
            {
                "code": "lv",
                "name": "Latvija",
            },
            {
                "code": "ee",
                "name": "Estija",
            },
        ]
    }
    path = tmp_path / "countries.json"
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | m              | property | type      | ref     | source                | level        
           example/json               |           |         |                       |
             | resource               | dask/json |         | {path}                |
                                      |           |         |                       |
             |   | Country |          |           | code    | countries             |
             |   |         | name     | string    |         | name                  |
             |   |         | code     | string    |         | code                  |
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/json/Country", ["getall"])

    resp = app.get("/example/json/Country")
    assert listdata(resp, sort=False) == [
        ("lt", "Lietuva"),
        ("lv", "Latvija"),
        ("ee", "Estija"),
    ]


def test_json_read_nested(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "countries": [
            {"code": "lt", "name": "Lietuva", "location": {"lat": 0, "lon": 1}},
            {"code": "lv", "name": "Latvija", "location": {"lat": 2, "lon": 3}},
            {"code": "ee", "name": "Estija", "location": {"lat": 4, "lon": 5}},
        ]
    }
    path = tmp_path / "countries.json"
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | m              | property  | type      | ref     | source                | level        
           example/json                |           |         |                       |
             | resource                | dask/json |         | {path}                |
                                       |           |         |                       |
             |   | Country |           |           | code    | countries             |
             |   |         | name      | string    |         | name                  |
             |   |         | code      | string    |         | code                  |
             |   |         | latitude  | integer   |         | location.lat          |
             |   |         | longitude | integer   |         | location.lon          |
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/json/Country", ["getall"])

    resp = app.get("/example/json/Country")
    assert listdata(resp, sort=False) == [
        ("lt", 0, 1, "Lietuva"),
        ("lv", 2, 3, "Latvija"),
        ("ee", 4, 5, "Estija"),
    ]


def test_json_read_multi_nested(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "galaxy": {
            "name": "Milky",
            "solar_system": {
                "name": "Solar",
                "planet": {
                    "name": "Earth",
                    "countries": [
                        {"code": "lt", "name": "Lietuva", "location": {"lat": 0, "lon": 1}},
                        {"code": "lv", "name": "Latvija", "location": {"lat": 2, "lon": 3}},
                        {"code": "ee", "name": "Estija", "location": {"lat": 4, "lon": 5}},
                    ],
                },
            },
        }
    }
    path = tmp_path / "countries.json"
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | m              | property  | type      | ref     | source
           example/json                |           |         |
             | resource                | dask/json |         | {path}
                                       |           |         |
             |   | Country |           |           | code    | galaxy.solar_system.planet.countries
             |   |         | name      | string    |         | name
             |   |         | code      | string    |         | code
             |   |         | latitude  | integer   |         | location.lat
             |   |         | longitude | integer   |         | location.lon
             |   |         | galaxy    | string    |         | ....name
             |   |         | system    | string    |         | ...name
             |   |         | planet    | string    |         | ..name
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/json/Country", ["getall"])

    resp = app.get("/example/json/Country")
    assert listdata(resp, sort=False, full=True) == [
        {
            "name": "Lietuva",
            "code": "lt",
            "latitude": 0,
            "longitude": 1,
            "galaxy": "Milky",
            "system": "Solar",
            "planet": "Earth",
        },
        {
            "name": "Latvija",
            "code": "lv",
            "latitude": 2,
            "longitude": 3,
            "galaxy": "Milky",
            "system": "Solar",
            "planet": "Earth",
        },
        {
            "name": "Estija",
            "code": "ee",
            "latitude": 4,
            "longitude": 5,
            "galaxy": "Milky",
            "system": "Solar",
            "planet": "Earth",
        },
    ]


def test_json_read_blank_node_list(rc: RawConfig, tmp_path: Path):
    json_manifest = [
        {
            "code": "lt",
            "name": "Lietuva",
        },
        {
            "code": "lv",
            "name": "Latvija",
        },
        {
            "code": "ee",
            "name": "Estija",
        },
    ]

    path = tmp_path / "countries.json"
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | m              | property | type      | ref     | source                | level        
           example/json               |           |         |                       |
             | resource               | dask/json |         | {path}                |
                                      |           |         |                       |
             |   | Country |          |           | code    | .                     |
             |   |         | name     | string    |         | name                  |
             |   |         | code     | string    |         | code                  |
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/json/Country", ["getall"])

    resp = app.get("/example/json/Country")
    assert listdata(resp, sort=False) == [
        ("lt", "Lietuva"),
        ("lv", "Latvija"),
        ("ee", "Estija"),
    ]


def test_json_read_blank_node_single_level(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "id": 0,
        "countries": [
            {
                "code": "lt",
                "name": "Lietuva",
            },
            {
                "code": "lv",
                "name": "Latvija",
            },
            {
                "code": "ee",
                "name": "Estija",
            },
        ],
    }

    path = tmp_path / "countries.json"
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | m              | property | type      | ref     | source                | level        
           example/json               |           |         |                       |
             | resource               | dask/json |         | {path}                |
                                      |           |         |                       |
             |   | Country |          |           | code    | countries             |
             |   |         | name     | string    |         | name                  |
             |   |         | code     | string    |         | code                  |
             |   |         | id       | integer   |         | ..id                  |       
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/json/Country", ["getall"])

    resp = app.get("/example/json/Country")
    assert listdata(resp, sort=False) == [
        ("lt", 0, "Lietuva"),
        ("lv", 0, "Latvija"),
        ("ee", 0, "Estija"),
    ]


def test_json_read_blank_node_multi_level(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "id": 0,
        "galaxy": {
            "name": "Milky",
            "solar_system": {
                "name": "Solar",
                "planet": {
                    "name": "Earth",
                    "countries": [
                        {"code": "lt", "name": "Lietuva", "location": {"lat": 0, "lon": 1}},
                        {"code": "lv", "name": "Latvija", "location": {"lat": 2, "lon": 3}},
                        {"code": "ee", "name": "Estija", "location": {"lat": 4, "lon": 5}},
                    ],
                },
            },
        },
    }

    path = tmp_path / "countries.json"
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | m              | property  | type      | ref     | source   
           example/json                |           |         |
             | resource                | dask/json |         | {path}
                                       |           |         |
             |   | Country |           |           | code    | galaxy.solar_system.planet.countries
             |   |         | name      | string    |         | name
             |   |         | code      | string    |         | code
             |   |         | latitude  | integer   |         | location.lat
             |   |         | longitude | integer   |         | location.lon
             |   |         | id        | integer   |         | .....id  
             |   |         | galaxy    | string    |         | ....name
             |   |         | system    | string    |         | ...name
             |   |         | planet    | string    |         | ..name     
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/json/Country", ["getall"])

    resp = app.get("/example/json/Country")
    assert listdata(resp, sort=False, full=True) == [
        {
            "name": "Lietuva",
            "code": "lt",
            "latitude": 0,
            "longitude": 1,
            "galaxy": "Milky",
            "system": "Solar",
            "planet": "Earth",
            "id": 0,
        },
        {
            "name": "Latvija",
            "code": "lv",
            "latitude": 2,
            "longitude": 3,
            "galaxy": "Milky",
            "system": "Solar",
            "planet": "Earth",
            "id": 0,
        },
        {
            "name": "Estija",
            "code": "ee",
            "latitude": 4,
            "longitude": 5,
            "galaxy": "Milky",
            "system": "Solar",
            "planet": "Earth",
            "id": 0,
        },
    ]


def test_json_read_ref_level_3(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "countries": [
            {"code": "lt", "name": "Lietuva", "cities": [{"name": "Vilnius"}]},
            {"code": "lv", "name": "Latvija", "cities": [{"name": "Ryga"}]},
            {"code": "ee", "name": "Estija", "cities": [{"name": "Talin"}]},
        ]
    }
    path = tmp_path / "countries.json"
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | m              | property | type      | ref     | source                | level        
           example/json               |           |         |                       |
             | resource               | dask/json |         | {path}                |
                                      |           |         |                       |
             |   | Country |          |           | code    | countries             |
             |   |         | name     | string    |         | name                  |
             |   |         | code     | string    |         | code                  |
                                      |           |         |                       |
             |   | City    |          |           |         | countries[].cities    |
             |   |         | name     | string    |         | name                  |
             |   |         | country  | ref       | Country | ..                    | 3
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/json/City", ["getall"])

    resp = app.get("/example/json/City")
    assert listdata(resp, sort=False) == [
        ("lt", "Vilnius"),
        ("lv", "Ryga"),
        ("ee", "Talin"),
    ]


def test_json_read_ref_level_4(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "countries": [
            {"code": "lt", "name": "Lietuva", "cities": [{"name": "Vilnius"}]},
            {"code": "lv", "name": "Latvija", "cities": [{"name": "Ryga"}]},
            {"code": "ee", "name": "Estija", "cities": [{"name": "Talin"}]},
        ]
    }
    path = tmp_path / "countries.json"
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | m              | property | type      | ref     | source                | level        
           example/json               |           |         |                       |
             | resource               | dask/json |         | {path}                |
                                      |           |         |                       |
             |   | Country |          |           | code    | countries             |
             |   |         | name     | string    |         | name                  |
             |   |         | code     | string    |         | code                  |
                                      |           |         |                       |
             |   | City    |          |           |         | countries[].cities    |
             |   |         | name     | string    |         | name                  |
             |   |         | country  | ref       | Country | ..                    | 4
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/json", ["getall"])

    resp = app.get("/example/json/Country")
    countries = {c["code"]: c["_id"] for c in listdata(resp, "_id", "code", full=True)}
    assert sorted(countries) == ["ee", "lt", "lv"]

    resp = app.get("/example/json/City")
    assert listdata(resp, sort=False) == [
        (countries["lt"], "Vilnius"),
        (countries["lv"], "Ryga"),
        (countries["ee"], "Talin"),
    ]


def test_json_read_multiple_sources(rc: RawConfig, tmp_path: Path):
    json0 = {
        "countries": [
            {
                "code": "lt",
                "name": "Lietuva",
            },
            {
                "code": "lv",
                "name": "Latvija",
            },
            {
                "code": "ee",
                "name": "Estija",
            },
        ]
    }
    path0 = tmp_path / "cities.json"
    path0.write_text(json.dumps(json0))

    json1 = {
        "cities": [{"code": "lt", "name": "Vilnius"}, {"code": "lv", "name": "Ryga"}, {"code": "ee", "name": "Talin"}]
    }
    path1 = tmp_path / "countries.json"
    path1.write_text(json.dumps(json1))

    context, manifest = prepare_manifest(
        rc,
        f"""
       d | r | b | m | property | type      | ref     | source    | prepare | access | level
       example/json             |           |         |           |         |        |
         | json_city            | dask/json |         | {path1}   |         |        |
         |   |   | City         |           | name    | cities    |         |        |
         |   |   |   | name     | string    |         | name      |         | open   |
         |   |   |   | code     | string    |         | code      |         | open   |
                     |
         | json_country         | dask/json |         | {path0}   |         |        |
         |   |   | Country      |           | code    | countries |         |        |
         |   |   |   | name     | string    |         | name      |         | open   |
         |   |   |   | code     | string    |         | code      |         | open   |
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/json", ["getall"])

    resp = app.get("/example/json/City")
    assert listdata(resp, sort=False) == [
        ("lt", "Vilnius"),
        ("lv", "Ryga"),
        ("ee", "Talin"),
    ]

    resp = app.get("/example/json/Country")
    assert listdata(resp, sort=False) == [
        ("lt", "Lietuva"),
        ("lv", "Latvija"),
        ("ee", "Estija"),
    ]


def test_json_read_with_empty(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "countries": [
            {
                "code": "lt",
            },
            {
                "code": "lv",
                "name": "Latvija",
            },
            {
                "name": "Estija",
            },
        ]
    }
    path = tmp_path / "countries.json"
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | m              | property | type      | ref     | source                | level        
           example/json               |           |         |                       |
             | resource               | dask/json |         | {path}                |
                                      |           |         |                       |
             |   | Country |          |           | code    | countries             |
             |   |         | name     | string    |         | name                  |
             |   |         | code     | string    |         | code                  |
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/json/Country", ["getall"])

    resp = app.get("/example/json/Country")
    assert listdata(resp, sort=False) == [
        ("lt", None),
        ("lv", "Latvija"),
        (None, "Estija"),
    ]


def test_json_read_with_empty_nested(rc: RawConfig, tmp_path: Path):
    json_manifest = {
        "countries": [
            {
                "code": "lt",
                "name": "Lietuva",
            },
            {"name": "Latvija", "location": {"lon": 3}},
            {"code": "ee", "location": {"lat": 4, "lon": 5}},
        ]
    }
    path = tmp_path / "countries.json"
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | m              | property  | type      | ref     | source                | level        
           example/json                |           |         |                       |
             | resource                | dask/json |         | {path}                |
                                       |           |         |                       |
             |   | Country |           |           | code    | countries             |
             |   |         | name      | string    |         | name                  |
             |   |         | code      | string    |         | code                  |
             |   |         | latitude  | integer   |         | location.lat          |
             |   |         | longitude | integer   |         | location.lon          |
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/json/Country", ["getall"])

    resp = app.get("/example/json/Country")
    assert listdata(resp, sort=False) == [
        ("lt", None, None, "Lietuva"),
        (None, None, 3, "Latvija"),
        ("ee", 4, 5, None),
    ]


def test_text_read_from_external_source(rc: RawConfig, fs: AbstractFileSystem):
    fs.pipe("countries.csv", ("šalislt\nlietuva\n").encode("utf-8"))

    context, manifest = prepare_manifest(
        rc,
        """
    d | r | b | m | property    | type     | ref  | source                 | prepare | access
    example/countries           |          |      |                        |         |
      | csv                     | dask/csv |      | memory://countries.csv |         |
      |   |   | Country         |          | name |                        |         |
      |   |   |   | name@lt     | string   |      | šalislt                |         | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/countries", ["getall"])

    resp = app.get("example/countries/Country")
    assert listdata(resp, sort=False) == ["lietuva"]


def test_json_read_parametrize_simple_iterate_pages(rc: RawConfig, tmp_path: Path):
    page_count = 10
    for i in range(1, page_count):
        current_page_file = tmp_path / f"page{i - 1}.json"
        json_manifest = {
            "page": {"next": str(tmp_path / f"page{i}.json") if i != page_count - 1 else None, "name": f"Page {i}"}
        }
        current_page_file.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property    | type      | ref  | source                 | prepare | access
    example/json                |           |      |                        |         |
      | resource                | dask/json |      | {{path}}               |         |
      |   |   |                 | param     | path | {tmp_path / "page0.json"} |         |
      |   |   |                 |           |      | Page                   | read().next |
      |   |   | Page            |           | name | page                   |         |
      |   |   |   | name        | string    |      | name                   |         | open
      |   |   |   | next        | uri       |      | next                   |         | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/json/Page", ["getall"])

    resp = app.get("/example/json/Page")
    assert listdata(resp, "name", sort=False) == [
        "Page 1",
        "Page 2",
        "Page 3",
        "Page 4",
        "Page 5",
        "Page 6",
        "Page 7",
        "Page 8",
        "Page 9",
    ]


def test_json_read_parametrize_iterate_pages_distinct(rc: RawConfig, tmp_path: Path):
    page_count = 10
    for i in range(1, page_count):
        current_page_file = tmp_path / f"page{i - 1}.json"
        json_manifest = {
            "page": {"next": str(tmp_path / f"page{i}.json") if i != page_count - 1 else None, "name": "Page"}
        }
        current_page_file.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property    | type      | ref  | source                 | prepare | access
    example/json                |           |      |                        |         |
      | resource                | dask/json |      | {{path}}               |         |
      |   |   |                 | param     | path | {tmp_path / "page0.json"} |         |
      |   |   |                 |           |      | Page                   | read().next |
      |   |   | Page            |           | name | page                   |         |
      |   |   |   | name        | string    |      | name                   |         | open
      |   |   |   | next        | uri       |      | next                   |         | open
      |   |   | PageDistinct    |           | name | page                   | distinct() |
      |   |   |   | name        | string    |      | name                   |         | open
      |   |   |   | next        | uri       |      | next                   |         | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/json/Page", ["getall"])
    app.authmodel("example/json/PageDistinct", ["getall"])

    resp = app.get("/example/json/Page")
    assert listdata(resp, "name", sort=False) == [
        "Page",
        "Page",
        "Page",
        "Page",
        "Page",
        "Page",
        "Page",
        "Page",
        "Page",
    ]
    resp = app.get("/example/json/PageDistinct")
    assert listdata(resp, "name", sort=False) == ["Page"]


def test_json_read_parametrize_iterate_pages_limit(rc: RawConfig, tmp_path: Path):
    page_count = 10
    for i in range(1, page_count):
        current_page_file = tmp_path / f"page{i - 1}.json"
        json_manifest = {
            "page": {"next": str(tmp_path / f"page{i}.json") if i != page_count - 1 else None, "name": f"Page{i % 3}"}
        }
        current_page_file.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property    | type      | ref  | source                 | prepare | access
    example/json                |           |      |                        |         |
      | resource                | dask/json |      | {{path}}               |         |
      |   |   |                 | param     | path | {tmp_path / "page0.json"} |         |
      |   |   |                 |           |      | Page                   | read().next |
      |   |   | Page            |           | name | page                   |         |
      |   |   |   | name        | string    |      | name                   |         | open
      |   |   |   | next        | uri       |      | next                   |         | open
      |   |   | PageDistinct    |           | name | page                   | distinct() |
      |   |   |   | name        | string    |      | name                   |         | open
      |   |   |   | next        | uri       |      | next                   |         | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/json/Page", ["getall", "search"])
    app.authmodel("example/json/PageDistinct", ["getall", "search"])

    resp = app.get("/example/json/Page?limit(4)")
    assert listdata(resp, "name", sort=False) == ["Page1", "Page2", "Page0", "Page1"]
    resp = app.get("/example/json/PageDistinct?limit(4)")
    assert listdata(resp, "name", sort=False) == ["Page1", "Page0", "Page2"]


def test_xml_read_parametrize_simple_iterate_pages(rc: RawConfig, tmp_path: Path):
    page_count = 10
    for i in range(1, page_count):
        current_page_file = tmp_path / f"page{i - 1}.xml"
        xml_manifest = f"""
        <pages name="Page {i}">
            <next>{str(tmp_path / f"page{i}.xml") if i != page_count - 1 else ""}</next>
        </pages>
        """
        current_page_file.write_text(xml_manifest)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property    | type     | ref  | source                 | prepare | access
    example/xml                 |          |      |                        |         |
      | resource                | dask/xml |      | {{path}}               |         |
      |   |   |                 | param    | path | {tmp_path / "page0.xml"} |         |
      |   |   |                 |          |      | Page                   | read().next |
      |   |   | Page            |          | name | ../pages               |         |
      |   |   |   | name        | string   |      | @name                  |         | open
      |   |   |   | next        | uri      |      | next/text()            |         | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/Page", ["getall"])

    resp = app.get("/example/xml/Page")
    assert listdata(resp, "name", sort=False) == [
        "Page 1",
        "Page 2",
        "Page 3",
        "Page 4",
        "Page 5",
        "Page 6",
        "Page 7",
        "Page 8",
        "Page 9",
    ]


def test_xml_json_combined_read_parametrize_advanced_iterate_pages(rc: RawConfig, tmp_path: Path):
    page_count = 3
    database_names = ["PostgresSQL", "SQLite", "MongoDB"]
    max_count = page_count * len(database_names)
    for db_id, db_name in enumerate(database_names):
        for i in range(1, page_count + 1):
            true_id = i + page_count * db_id
            current_page_file = tmp_path / f"page{true_id - 1}.json"
            json_manifest = {
                "page": {
                    "next": str(tmp_path / f"page{true_id}.json") if true_id != max_count else None,
                    "name": f"Page {true_id}",
                    "database": {"name": db_name, "id": db_id},
                }
            }
            current_page_file.write_text(json.dumps(json_manifest))

    for i, database_name in enumerate(database_names):
        current_page_file = tmp_path / f"database{i}.xml"
        xml_manifest = f'''
        <databases>
            <meta name="{database_name}">
                <context>{database_name}{i}1</context>
            </meta>
            <meta name="{database_name}">
                <context>{database_name}{i}2</context>
            </meta>
            <meta name="{database_name}">
                <context>{database_name}{i}3</context>
            </meta>
        </databases>
        '''
        current_page_file.write_text(xml_manifest)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property    | type      | ref     | source                            | prepare     | access
    example/all                 |           |         |                                   |             |
      | json_resource           | dask/json |         | {{path}}                          |             |
      |   |   |                 | param     | path    | {tmp_path / "page0.json"}         |             |
      |   |   |                 |           |         | Page                              | read().next |
      |   |   | Page            |           | name    | page                              |             |
      |   |   |   | name        | string    |         | name                              |             | open
      |   |   |   | next        | uri       |         | next                              |             | open
      |   |   | Database        |           | name    | page.database                     |             |
      |   |   |   | name        | string    |         | name                              |             | open
      |   |   |   | id          | integer   |         | id                                |             | open
      | xml_resource            | dask/xml  |         | {tmp_path / "database"}{{id}}.xml |             |
      |   |   |                 | param     | id      | Database                          | read().id   |
      |   |   | Meta            |           | context | databases/meta                    | distinct()  |
      |   |   |   | name        | string    |         | @name                             |             | open
      |   |   |   | context     | string    |         | context/text()                    |             | open
      |   |   | MetaNotDistinct |           | context | databases/meta                    |             |
      |   |   |   | name        | string    |         | @name                             |             | open
      |   |   |   | context     | string    |         | context/text()                    |             | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/all/Page", ["getall"])
    app.authmodel("example/all/Database", ["getall"])
    app.authmodel("example/all/Meta", ["getall"])
    app.authmodel("example/all/MetaNotDistinct", ["getall"])

    resp = app.get("/example/all/Page")
    assert listdata(resp, "name", sort=False) == [
        "Page 1",
        "Page 2",
        "Page 3",
        "Page 4",
        "Page 5",
        "Page 6",
        "Page 7",
        "Page 8",
        "Page 9",
    ]
    resp = app.get("/example/all/Database")
    assert listdata(resp, "name", "id", sort=False) == [
        ("PostgresSQL", 0),
        ("PostgresSQL", 0),
        ("PostgresSQL", 0),
        ("SQLite", 1),
        ("SQLite", 1),
        ("SQLite", 1),
        ("MongoDB", 2),
        ("MongoDB", 2),
        ("MongoDB", 2),
    ]
    resp = app.get("/example/all/Meta")
    assert listdata(resp, "name", "context", sort=False) == [
        ("MongoDB", "MongoDB21"),
        ("PostgresSQL", "PostgresSQL01"),
        ("SQLite", "SQLite13"),
        ("PostgresSQL", "PostgresSQL02"),
        ("PostgresSQL", "PostgresSQL03"),
        ("SQLite", "SQLite12"),
        ("MongoDB", "MongoDB23"),
        ("MongoDB", "MongoDB22"),
        ("SQLite", "SQLite11"),
    ]
    resp = app.get("/example/all/MetaNotDistinct")
    assert listdata(resp, "name", "context", sort=False) == [
        ("PostgresSQL", "PostgresSQL01"),
        ("PostgresSQL", "PostgresSQL02"),
        ("PostgresSQL", "PostgresSQL03"),
        ("PostgresSQL", "PostgresSQL01"),
        ("PostgresSQL", "PostgresSQL02"),
        ("PostgresSQL", "PostgresSQL03"),
        ("PostgresSQL", "PostgresSQL01"),
        ("PostgresSQL", "PostgresSQL02"),
        ("PostgresSQL", "PostgresSQL03"),
        ("SQLite", "SQLite11"),
        ("SQLite", "SQLite12"),
        ("SQLite", "SQLite13"),
        ("SQLite", "SQLite11"),
        ("SQLite", "SQLite12"),
        ("SQLite", "SQLite13"),
        ("SQLite", "SQLite11"),
        ("SQLite", "SQLite12"),
        ("SQLite", "SQLite13"),
        ("MongoDB", "MongoDB21"),
        ("MongoDB", "MongoDB22"),
        ("MongoDB", "MongoDB23"),
        ("MongoDB", "MongoDB21"),
        ("MongoDB", "MongoDB22"),
        ("MongoDB", "MongoDB23"),
        ("MongoDB", "MongoDB21"),
        ("MongoDB", "MongoDB22"),
        ("MongoDB", "MongoDB23"),
    ]


def test_csv_read_parametrize_simple_iterate_pages(rc: RawConfig, tmp_path: Path):
    page_count = 10
    for i in range(1, page_count):
        current_page_file = tmp_path / f"page{i - 1}.csv"
        csv_manifest = f"name,next\nPage {i},{f'{i}.csv' if i != page_count - 1 else ''}"
        current_page_file.write_text(csv_manifest)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property    | type     | ref  | source                  | prepare | access
    example/csv                 |          |      |                         |         |
      | resource                | dask/csv |      | {tmp_path}/{{}}{{path}} |         |
      |   |   |                 | param    | path | 0.csv                   |         |
      |   |   |                 |          |      | Page                    | read().next |
      |   |   | Page            |          | name | page                    |         |
      |   |   |   | name        | string   |      | name                    |         | open
      |   |   |   | next        | uri      |      | next                    |         | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/csv/Page", ["getall"])

    resp = app.get("/example/csv/Page")
    assert listdata(resp, "name", sort=False) == [
        "Page 1",
        "Page 2",
        "Page 3",
        "Page 4",
        "Page 5",
        "Page 6",
        "Page 7",
        "Page 8",
        "Page 9",
    ]


def test_xml_json_csv_combined_read_parametrize_advanced_iterate_pages(rc: RawConfig, tmp_path: Path):
    page_count = 3
    database_types = {"SQL": ["PostgresSQL", "SQLite"], "NOSQL": ["MongoDB"]}
    max_count = page_count * len(database_types.keys())
    for db_id, db_type in enumerate(database_types.keys()):
        for i in range(1, page_count + 1):
            true_id = i + page_count * db_id
            current_page_file = tmp_path / f"page{true_id - 1}.json"
            json_manifest = {
                "page": {
                    "next": str(tmp_path / f"page{true_id}.json") if true_id != max_count else None,
                    "name": f"Page {true_id}",
                    "database_type": db_type,
                }
            }
            current_page_file.write_text(json.dumps(json_manifest))

    total_id = 0
    for db_id, (db_type, db_names) in enumerate(database_types.items()):
        current_page_file = tmp_path / f"database_{db_type}.csv"
        csv_manifest = "name,id\n"
        for name in db_names:
            csv_manifest += f"{name},{total_id}\n"
            total_id += 1
        current_page_file.write_text(csv_manifest)

    total_id = 0
    for db_names in database_types.values():
        for name in db_names:
            current_page_file = tmp_path / f"database{total_id}.xml"
            xml_manifest = f'''
                    <databases>
                        <meta name="{name}">
                            <context>{name}{total_id}1</context>
                        </meta>
                        <meta name="{name}">
                            <context>{name}{total_id}2</context>
                        </meta>
                        <meta name="{name}">
                            <context>{name}{total_id}3</context>
                        </meta>
                    </databases>
                    '''
            current_page_file.write_text(xml_manifest)
            total_id += 1

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property      | type      | ref     | source                            | prepare     | access
    example/all                   |           |         |                                   |             |
      | json_resource             | dask/json |         | {{path}}                          |             |
      |   |   |                   | param     | path    | {tmp_path / "page0.json"}         |             |
      |   |   |                   |           |         | Page                              | read().next |
      |   |   | Page              |           | name    | page                              |             |
      |   |   |   | name          | string    |         | name                              |             | open
      |   |   |   | next          | uri       |         | next                              |             | open
      |   |   |   | database_type | string    |         | database_type                     |             | open
      | csv_resource              | dask/csv  |         | {tmp_path}/{{}}_{{type}}.csv      |             |
      |   |   |                   | param     | type    | Page                              | read().database_type |
      |   |   | Database          |           | name    | database                          |             |
      |   |   |   | name          | string    |         | name                              |             | open
      |   |   |   | id            | integer   |         | id                                |             | open
      |   |   | DatabaseDistinct  |           | name    | database                          | distinct()  |
      |   |   |   | name          | string    |         | name                              |             | open
      |   |   |   | id            | integer   |         | id                                |             | open
      | xml_resource              | dask/xml  |         | {tmp_path / "database"}{{id}}.xml |             |
      |   |   |                   | param     | id      | Database                          | read().id   |
      |   |   | Meta              |           | context | databases/meta                    | distinct()  |
      |   |   |   | name          | string    |         | @name                             |             | open
      |   |   |   | context       | string    |         | context/text()                    |             | open
      |   |   | MetaNotDistinct   |           | context | databases/meta                    |             |
      |   |   |   | name          | string    |         | @name                             |             | open
      |   |   |   | context       | string    |         | context/text()                    |             | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/all/Page", ["getall"])
    app.authmodel("example/all/Database", ["getall"])
    app.authmodel("example/all/DatabaseDistinct", ["getall"])
    app.authmodel("example/all/Meta", ["getall"])
    app.authmodel("example/all/MetaNotDistinct", ["getall"])

    resp = app.get("/example/all/Page")
    assert listdata(resp, "name", sort=False) == ["Page 1", "Page 2", "Page 3", "Page 4", "Page 5", "Page 6"]
    resp = app.get("/example/all/Database")
    assert listdata(resp, "name", "id", sort=False) == [
        ("PostgresSQL", 0),
        ("SQLite", 1),
        ("PostgresSQL", 0),
        ("SQLite", 1),
        ("PostgresSQL", 0),
        ("SQLite", 1),
        ("MongoDB", 2),
        ("MongoDB", 2),
        ("MongoDB", 2),
    ]
    resp = app.get("/example/all/DatabaseDistinct")
    assert listdata(resp, "name", "id", sort=False) == [
        ("PostgresSQL", 0),
        ("SQLite", 1),
        ("MongoDB", 2),
    ]
    resp = app.get("/example/all/Meta")
    assert listdata(resp, "name", "context", sort=False) == [
        ("MongoDB", "MongoDB21"),
        ("PostgresSQL", "PostgresSQL01"),
        ("SQLite", "SQLite13"),
        ("PostgresSQL", "PostgresSQL02"),
        ("PostgresSQL", "PostgresSQL03"),
        ("SQLite", "SQLite12"),
        ("MongoDB", "MongoDB23"),
        ("MongoDB", "MongoDB22"),
        ("SQLite", "SQLite11"),
    ]
    resp = app.get("/example/all/MetaNotDistinct")
    assert listdata(resp, "name", "context", sort=False) == [
        ("PostgresSQL", "PostgresSQL01"),
        ("PostgresSQL", "PostgresSQL02"),
        ("PostgresSQL", "PostgresSQL03"),
        ("SQLite", "SQLite11"),
        ("SQLite", "SQLite12"),
        ("SQLite", "SQLite13"),
        ("PostgresSQL", "PostgresSQL01"),
        ("PostgresSQL", "PostgresSQL02"),
        ("PostgresSQL", "PostgresSQL03"),
        ("SQLite", "SQLite11"),
        ("SQLite", "SQLite12"),
        ("SQLite", "SQLite13"),
        ("PostgresSQL", "PostgresSQL01"),
        ("PostgresSQL", "PostgresSQL02"),
        ("PostgresSQL", "PostgresSQL03"),
        ("SQLite", "SQLite11"),
        ("SQLite", "SQLite12"),
        ("SQLite", "SQLite13"),
        ("MongoDB", "MongoDB21"),
        ("MongoDB", "MongoDB22"),
        ("MongoDB", "MongoDB23"),
        ("MongoDB", "MongoDB21"),
        ("MongoDB", "MongoDB22"),
        ("MongoDB", "MongoDB23"),
        ("MongoDB", "MongoDB21"),
        ("MongoDB", "MongoDB22"),
        ("MongoDB", "MongoDB23"),
    ]


def test_json_keymap_ref_keys_valid_order(context, rc, tmp_path, sqlite):
    json_manifest = {
        "planets": [
            {
                "code": "ER",
                "name": "Earth",
                "countries": [{"code": "LT", "name": "Lithuania"}, {"code": "LV", "name": "Latvia"}],
            },
            {"code": "MS", "name": "Mars", "countries": [{"code": "S5", "name": "s58467"}]},
        ]
    }
    path = tmp_path / "countries.json"
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(
        rc,
        f"""                   
            d | r | m | property        | type      | ref                | source              | prepare             | access
        datasets/json/keymap            |           |                    |                     |                     |
          | rs                          | dask/json |                    | {path}              |                     |
          |   | Planet                  |           | code               | planets             |                     | open
          |   |   | code                | string    |                    | code                |                     |
          |   |   | name                | string    |                    | name                |                     |
          |   | Country                 |           | code               | planets[].countries |                     | open
          |   |   | code                | string    |                    | code                |                     |
          |   |   | name                | string    |                    | name                |                     |
          |   |   | planet              | ref       | Planet             | ..                  |                     |
          |   |   | planet_name         | ref       | Planet[name]       | ..name              |                     |
          """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("datasets/json/keymap", ["getall"])

    resp = app.get("/datasets/json/keymap/Planet")
    id_mapping = {data["code"]: data["_id"] for data in resp.json()["_data"]}
    assert listdata(resp, "_id", "code", "name", sort="code", full=True) == [
        {"_id": id_mapping["ER"], "code": "ER", "name": "Earth"},
        {"_id": id_mapping["MS"], "code": "MS", "name": "Mars"},
    ]

    resp = app.get("/datasets/json/keymap/Country")
    assert listdata(
        resp, "code", "name", "planet._id", "planet_name._id", "planet_combine._id", sort="code", full=True
    ) == [
        {
            "code": "LT",
            "name": "Lithuania",
            "planet._id": id_mapping["ER"],
            "planet_name._id": id_mapping["ER"],
        },
        {
            "code": "LV",
            "name": "Latvia",
            "planet._id": id_mapping["ER"],
            "planet_name._id": id_mapping["ER"],
        },
        {
            "code": "S5",
            "name": "s58467",
            "planet._id": id_mapping["MS"],
            "planet_name._id": id_mapping["MS"],
        },
    ]


def test_json_keymap_ref_keys_invalid_order(context, rc, tmp_path, sqlite):
    json_manifest = {
        "planets": [
            {
                "code": "ER",
                "name": "Earth",
                "countries": [{"code": "LT", "name": "Lithuania"}, {"code": "LV", "name": "Latvia"}],
            },
            {"code": "MS", "name": "Mars", "countries": [{"code": "S5", "name": "s58467"}]},
        ]
    }
    path = tmp_path / "countries.json"
    path.write_text(json.dumps(json_manifest))

    context, manifest = prepare_manifest(
        rc,
        f"""                   
                d | r | m | property        | type      | ref                | source              | prepare             | access
            datasets/json/keymap            |           |                    |                     |                     |
              | rs                          | dask/json |                    | {path}              |                     |
              |   | Planet                  |           | code               | planets             |                     | open
              |   |   | code                | string    |                    | code                |                     |
              |   |   | name                | string    |                    | name                |                     |
              |   | Country                 |           | code               | planets[].countries |                     | open
              |   |   | code                | string    |                    | code                |                     |
              |   |   | name                | string    |                    | name                |                     |
              |   |   | planet              | ref       | Planet             | ..                  |                     |
              |   |   | planet_name         | ref       | Planet[name]       | ..name              |                     |
              """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("datasets/json/keymap", ["getall"])
    resp = app.get("datasets/json/keymap/Country")
    data = resp.json()["_data"]
    id_mapping = {"ER": data[0]["planet"]["_id"], "MS": data[2]["planet"]["_id"]}
    assert listdata(
        resp, "code", "name", "planet._id", "planet_name._id", "planet_combine._id", sort="code", full=True
    ) == [
        {
            "code": "LT",
            "name": "Lithuania",
            "planet._id": id_mapping["ER"],
            "planet_name._id": id_mapping["ER"],
        },
        {
            "code": "LV",
            "name": "Latvia",
            "planet._id": id_mapping["ER"],
            "planet_name._id": id_mapping["ER"],
        },
        {
            "code": "S5",
            "name": "s58467",
            "planet._id": id_mapping["MS"],
            "planet_name._id": id_mapping["MS"],
        },
    ]


def test_swap_ufunc(rc: RawConfig, fs: AbstractFileSystem):
    fs.pipe("cities.csv", ("id,name\n1,test\n2,\n3,\n4,null").encode("utf-8"))

    context, manifest = prepare_manifest(
        rc,
        """
    d | r | b | m | property | type     | ref  | source              | prepare            | access
    example/csv              |          |      |                     |                    |
      | csv                  | dask/csv |      | memory://cities.csv |                    |
      |   |   | City         |          | name |                     |                    |
      |   |   |   | id       | string   |      | id                  |                    | open
      |   |   |   | name     | string   |      | name                |                    | open
      |   |   |   |          | enum     |      |                     |                    | open
      |   |   |   |          |          |      |                     |  swap('nan', '---')  | open
      |   |   |   |          |          |      |                     |  swap(null, '---') | open
      |   |   |   |          |          |      |                     |  swap('', '---')   | open
      |   |   |   |          |          |      |                     |  '---'             | open
      |   |   |   |          |          |      |                     |  'test'            | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/csv/City", ["getall"])

    resp = app.get("/example/csv/City")
    assert listdata(resp) == [
        (1, "test"),
        (2, "---"),
        (3, "---"),
        (4, "---"),
    ]
