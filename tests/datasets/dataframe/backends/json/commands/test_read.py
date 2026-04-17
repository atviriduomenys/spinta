import json
from pathlib import Path
from unittest.mock import ANY

import pytest

from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.exceptions import PartialIncorrectProperty
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import prepare_manifest
from spinta.testing.utils import get_error_codes, get_error_context


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
        d | r | m       | property | type      | ref  | source          
        example/json               |           |      |           
          | resource               | dask/json |      | {path}    
                                   |           |      |           
          |   | Country |          |           | code | countries 
          |   |         | name     | string    |      | name      
          |   |         | code     | string    |      | code      
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
        d | r | m          | property  | type      | ref  | source       
           example/json                |           |      |              
             | resource                | dask/json |      | {path}       
                                       |           |      |              
             |   | Country |           |           | code | countries    
             |   |         | name      | string    |      | name         
             |   |         | code      | string    |      | code         
             |   |         | latitude  | integer   |      | location.lat 
             |   |         | longitude | integer   |      | location.lon 
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
        d | r | m          | property  | type      | ref  | source
           example/json                |           |      |
             | resource                | dask/json |      | {path}
                                       |           |      |
             |   | Country |           |           | code | galaxy.solar_system.planet.countries
             |   |         | name      | string    |      | name
             |   |         | code      | string    |      | code
             |   |         | latitude  | integer   |      | location.lat
             |   |         | longitude | integer   |      | location.lon
             |   |         | galaxy    | string    |      | ....name
             |   |         | system    | string    |      | ...name
             |   |         | planet    | string    |      | ..name
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
        d | r | m          | property | type      | ref  | source       
           example/json               |           |      |       
             | resource               | dask/json |      | {path}
                                      |           |      |       
             |   | Country |          |           | code | .     
             |   |         | name     | string    |      | name  
             |   |         | code     | string    |      | code  
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
        d | r | m          | property | type      | ref  | source        
           example/json               |           |      |          
             | resource               | dask/json |      | {path}   
                                      |           |      |          
             |   | Country |          |           | code | countries
             |   |         | name     | string    |      | name     
             |   |         | code     | string    |      | code     
             |   |         | id       | integer   |      | ..id     
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
        d | r | m          | property  | type      | ref  | source   
           example/json                |           |      |
             | resource                | dask/json |      | {path}
                                       |           |      |
             |   | Country |           |           | code | galaxy.solar_system.planet.countries
             |   |         | name      | string    |      | name
             |   |         | code      | string    |      | code
             |   |         | latitude  | integer   |      | location.lat
             |   |         | longitude | integer   |      | location.lon
             |   |         | id        | integer   |      | .....id  
             |   |         | galaxy    | string    |      | ....name
             |   |         | system    | string    |      | ...name
             |   |         | planet    | string    |      | ..name     
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
        d | r | m          | property | type      | ref     | source             | level        
           example/json               |           |         |                    |
             | resource               | dask/json |         | {path}             |
                                      |           |         |                    |
             |   | Country |          |           | code    | countries          |
             |   |         | name     | string    |         | name               |
             |   |         | code     | string    |         | code               |
                                      |           |         |                    |
             |   | City    |          |           |         | countries[].cities |
             |   |         | name     | string    |         | name               |
             |   |         | country  | ref       | Country | ..                 | 3
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
        d | r | m          | property | type      | ref     | source             | level        
           example/json               |           |         |                    |
             | resource               | dask/json |         | {path}             |
                                      |           |         |                    |
             |   | Country |          |           | code    | countries          |
             |   |         | name     | string    |         | name               |
             |   |         | code     | string    |         | code               |
                                      |           |         |                    |
             |   | City    |          |           |         | countries[].cities |
             |   |         | name     | string    |         | name               |
             |   |         | country  | ref       | Country | ..                 | 4
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
       d | r | b | m | property | type      | ref  | source    | access
       example/json             |           |      |           |       
         | json_city            | dask/json |      | {path1}   |       
         |   |   | City         |           | name | cities    |       
         |   |   |   | name     | string    |      | name      | open  
         |   |   |   | code     | string    |      | code      | open  
                     |
         | json_country         | dask/json |      | {path0}   |       
         |   |   | Country      |           | code | countries |       
         |   |   |   | name     | string    |      | name      | open  
         |   |   |   | code     | string    |      | code      | open  
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
        d | r | m          | property | type      | ref  | source          
           example/json               |           |      |          
             | resource               | dask/json |      | {path}   
                                      |           |      |          
             |   | Country |          |           | code | countries
             |   |         | name     | string    |      | name     
             |   |         | code     | string    |      | code     
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
        d | r | m          | property  | type      | ref  | source        
           example/json                |           |      |             
             | resource                | dask/json |      | {path}      
                                       |           |      |             
             |   | Country |           |           | code | countries   
             |   |         | name      | string    |      | name        
             |   |         | code      | string    |      | code        
             |   |         | latitude  | integer   |      | location.lat
             |   |         | longitude | integer   |      | location.lon
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
        d | r | b | m | property | type      | ref  | source                    | prepare     | access
        example/json             |           |      |                           |             |
          | resource             | dask/json |      | {{path}}                  |             |
          |   |   |              | param     | path | {tmp_path / "page0.json"} |             |
          |   |   |              |           |      | Page                      | read().next |
          |   |   | Page         |           | name | page                      |             |
          |   |   |   | name     | string    |      | name                      |             | open
          |   |   |   | next     | uri       |      | next                      |             | open
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
        d | r | b | m | property | type      | ref  | source                    | prepare     | access
        example/json             |           |      |                           |             |
          | resource             | dask/json |      | {{path}}                  |             |
          |   |   |              | param     | path | {tmp_path / "page0.json"} |             |
          |   |   |              |           |      | Page                      | read().next |
          |   |   | Page         |           | name | page                      |             |
          |   |   |   | name     | string    |      | name                      |             | open
          |   |   |   | next     | uri       |      | next                      |             | open
          |   |   | PageDistinct |           | name | page                      | distinct()  |
          |   |   |   | name     | string    |      | name                      |             | open
          |   |   |   | next     | uri       |      | next                      |             | open
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
         d | r | b | m | property | type      | ref  | source                    | prepare     | access
         example/json             |           |      |                           |             |
           | resource             | dask/json |      | {{path}}                  |             |
           |   |   |              | param     | path | {tmp_path / "page0.json"} |             |
           |   |   |              |           |      | Page                      | read().next |
           |   |   | Page         |           | name | page                      |             |
           |   |   |   | name     | string    |      | name                      |             | open
           |   |   |   | next     | uri       |      | next                      |             | open
           |   |   | PageDistinct |           | name | page                      | distinct()  |
           |   |   |   | name     | string    |      | name                      |             | open
           |   |   |   | next     | uri       |      | next                      |             | open
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
        d | r | m | property     | type      | ref          | source              | access
        datasets/json/keymap     |           |              |                     |
          | rs                   | dask/json |              | {path}              |
          |   | Planet           |           | code         | planets             | open
          |   |   | code         | string    |              | code                |
          |   |   | name         | string    |              | name                |
          |   | Country          |           | code         | planets[].countries | open
          |   |   | code         | string    |              | code                |
          |   |   | name         | string    |              | name                |
          |   |   | planet       | ref       | Planet       | ..                  |
          |   |   | planet_name  | ref       | Planet[name] | ..name              |
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
        d | r | m | property  | type      | ref          | source              | access
        datasets/json/keymap  |           |              |                     |
        | rs                  | dask/json |              | {path}              |
        |   | Planet          |           | code         | planets             | open
        |   |   | code        | string    |              | code                |
        |   |   | name        | string    |              | name                |
        |   | Country         |           | code         | planets[].countries | open
        |   |   | code        | string    |              | code                |
        |   |   | name        | string    |              | name                |
        |   |   | planet      | ref       | Planet       | ..                  |
        |   |   | planet_name | ref       | Planet[name] | ..name              |
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


def test_json_read_from_different_resource_property(rc: RawConfig, tmp_path: Path):
    xml = """
        <countries>
            <country>
                <name>Lietuva</name>
                <data>[{"name": "Lietuva", "capital": "Vilnius"}]</data>
            </country>
            <country>
                <name>Latvija</name>
                <data>[{"name": "Latvija", "capital": "Ryga"}]</data>
            </country>
        </countries>
    """
    xml_path = tmp_path / "countries.xml"
    xml_path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property | type      | ref         | source            | access | prepare
        example                  | dataset   |             |                   |        |
          | xml_resource         | dask/xml  |             | {xml_path}        |        |
          |   |   | Country      |           |             | countries/country | open   |
          |   |   |   | name     | string    |             | name              |        |
          |   |   |   | data     | string    |             | data              |        |
          | json_resource        | dask/json |             |                   |        | eval(param(nested_json))
          |   |   |   |          | param     | nested_json | Country           |        | read().data
          |   |   | Data         |           |             | .                 | open   |
          |   |   |   | name     | string    |             | name              |        |
          |   |   |   | capital  | string    |             | capital           |        |

        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Country", ["getall"])
    app.authmodel("example/Data", ["getall"])

    resp = app.get("/example/Data")
    assert listdata(resp, sort=False) == [
        ("Vilnius", "Lietuva"),
        ("Ryga", "Latvija"),
    ]


def test_json_read_from_different_resource_property_with_iterate_pages(rc: RawConfig, tmp_path: Path):
    page1_file = tmp_path / "page1.xml"
    page1_file.write_text(
        f"""
        <countries>
            <country>
                <next>{str(tmp_path / "page2.xml")}</next>
                <name>Lietuva</name>
                <data>[{{"name": "Lietuva", "capital": "Vilnius"}}]</data>
            </country>
        </countries>
    `   """
    )
    page2_file = tmp_path / "page2.xml"
    page2_file.write_text(
        """
        <countries>
            <country>
                <next></next>
                <name>Latvija</name>
                <data>[{"name": "Latvija", "capital": "Ryga"}]</data>
            </country>
        </countries>
        """
    )

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property | type      | ref         | source                   | access | prepare
        example                  | dataset   |             |                          |        |
          | xml_resource         | dask/xml  |             | {{path}}                 |        |
          |   |   |              | param     | path        | {tmp_path / "page1.xml"} |        |
          |   |   |              |           |             | Country                  |        | read().next
          |   |   | Country      |           |             | countries/country        | open   |
          |   |   |   | next     | string    |             | next                     |        |
          |   |   |   | name     | string    |             | name                     |        |
          |   |   |   | data     | string    |             | data                     |        |
          | json_resource        | dask/json |             |                          |        | eval(param(nested_json))
          |   |   |   |          | param     | nested_json | Country                  |        | read().data
          |   |   | Data         |           |             | .                        | open   |
          |   |   |   | name     | string    |             | name                     |        |
          |   |   |   | capital  | string    |             | capital                  |        |
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Country", ["getall"])
    app.authmodel("example/Data", ["getall"])

    resp = app.get("/example/Data")
    assert listdata(resp, sort=False) == [
        ("Vilnius", "Lietuva"),
        ("Ryga", "Latvija"),
    ]


def test_json_read_raise_error_if_neither_resource_source_nor_prepare_given(rc: RawConfig):
    context, manifest = prepare_manifest(
        rc,
        """
        d | r | b | m | property | type      | source  | access
        example                  | dataset   |         |
          | json_resource        | dask/json |         |
          |   |   | Data         |           | .       | open
          |   |   |   | name     | string    | name    |
          |   |   |   | capital  | string    | capital |
        """,
        mode=Mode.external,
    )

    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Data", ["getall"])

    response = app.get("/example/Data")
    assert response.status_code == 500
    assert get_error_codes(response.json()) == ["CannotReadResource"]
    assert get_error_context(response.json(), "CannotReadResource", ["resource"]) == {"resource": "json_resource"}


def test_json_read_filters_results_if_url_query_parameter_is_property_without_prepare(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "countries.json"
    path.write_text(
        json.dumps(
            [
                {"name": "Lietuva", "capital": "Vilnius"},
                {"name": "Latvija", "capital": "Ryga"},
            ]
        )
    )

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | m | property | type      | source  | access
        example              |           |         |
          | json_resource    | dask/json | {path}  |
          |   | Country      |           | .       | open
          |   |   | name     | string    | name    |
          |   |   | capital  | string    | capital |
        """,
        mode=Mode.external,
    )

    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Country", ["getall", "search"])

    response = app.get("example/Country?name='Lietuva'")
    assert listdata(response, sort=False) == [("Vilnius", "Lietuva")]


def test_json_read_error_if_backend_cannot_parse_data(rc: RawConfig, tmp_path: Path):
    xml = """
        <countries>
            <country>
                <name>Lietuva</name>
                <capital>Vilnius</capital>
            </country>
            <country>
                <name>Latvija</name>
                <capital>Ryga</capital>
            </country>
        </countries>
    """
    path = tmp_path / "countries.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | m | property | type      | source  | access
        example              |           |         |
          | json_resource    | dask/json | {path}  |
          |   | Country      |           | .       | open
          |   |   | name     | string    | name    |
          |   |   | capital  | string    | capital |
        """,
        mode=Mode.external,
    )

    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Country", ["getall"])

    response = app.get("example/Country")
    assert response.status_code == 500
    assert get_error_codes(response.json()) == ["UnexpectedErrorReadingData"]


def test_json_composite_ref_two_levels_returns_data(rc: RawConfig, tmp_path: Path):
    json_data = {
        "participant": [
            {"code": "P001", "asset_code": "AT001", "asset_name": "Equipment"},
            {"code": "P002", "asset_code": "AT002", "asset_name": "Building"},
        ]
    }
    path = tmp_path / "test.json"
    path.write_text(json.dumps(json_data))

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property             | type         | ref       | source      | access | level
    example                              |              |           |             |        |
      | data                             | dask/json    |           | {path}      |        |
      |   |   | AssetType                |              | code      | participant |        | 5
      |   |   |   | code                 | string       |           | asset_code  | open   | 5
      |   |   |   | name                 | string       |           | asset_name  | open   | 5
      |   |   | Participant              |              |           | participant |        | 5
      |   |   |   | code                 | string       |           | code        | open   | 5
      |   |   |   | asset_type           | ref required | AssetType | asset_code  | open   | 5
      |   |   |   | asset_type.code      | string       |           | asset_code  | open   | 5
      |   |   |   | asset_type.name      | string       |           | asset_name  | open   | 5
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Participant", ["getall"])
    app.authmodel("example/AssetType", ["getall"])

    asset_type_resp = app.get("/example/AssetType")
    asset_type_ids = [asset_type_object["_id"] for asset_type_object in asset_type_resp.json()["_data"]]

    resp = app.get("/example/Participant")
    assert resp.status_code == 200

    data = resp.json()["_data"]
    assert data == [
        {
            "_type": "example/Participant",
            "_id": ANY,
            "_revision": None,
            "code": "P001",
            "asset_type": {"_id": asset_type_ids[0], "name": "Equipment", "code": "AT001"},
        },
        {
            "_type": "example/Participant",
            "_id": ANY,
            "_revision": None,
            "code": "P002",
            "asset_type": {"_id": asset_type_ids[1], "name": "Building", "code": "AT002"},
        },
    ]


def test_json_composite_ref_three_levels_xyz(rc: RawConfig, tmp_path: Path):
    json_data = {
        "order": [
            {"id": "ORD001", "vendor_code": "VEND001", "country_code": "LT", "country_name": "Lithuania"},
            {"id": "ORD002", "vendor_code": "VEND002", "country_code": "PL", "country_name": "Poland"},
        ]
    }
    path = tmp_path / "test.json"
    path.write_text(json.dumps(json_data))

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                | type         | ref      | source       | access
    example                                 |              |          |              |
      | data                                | dask/json    |          | {path}       |
      |   |   | Country                     |              | code     | order        |
      |   |   |   | code                    | string       |          | country_code | open
      |   |   |   | name                    | string       |          | country_name | open
      |   |   | Vendor                      |              | code     | order        |
      |   |   |   | code                    | string       |          | vendor_code  | open
      |   |   |   | country                 | ref required | Country  | country_code | open
      |   |   |   | country.code            | string       |          | country_code | open
      |   |   |   | country.name            | string       |          | country_name | open
      |   |   | Order                       |              |          | order        |
      |   |   |   | id                      | string       |          | id           | open
      |   |   |   | vendor                  | ref required | Vendor   | vendor_code  | open
      |   |   |   | vendor.country.code     | string       |          | country_code | open
      |   |   |   | vendor.country.name     | string       |          | country_name | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Order", ["getall"])
    app.authmodel("example/Vendor", ["getall"])
    app.authmodel("example/Country", ["getall"])

    vendor_resp = app.get("/example/Vendor")
    vendor_ids = [vendor_object["_id"] for vendor_object in vendor_resp.json()["_data"]]

    country_resp = app.get("/example/Country")
    country_ids = [country_object["_id"] for country_object in country_resp.json()["_data"]]

    resp = app.get("/example/Order")
    assert resp.status_code == 200

    data = resp.json()["_data"]
    assert data == [
        {
            "_type": "example/Order",
            "_id": ANY,
            "_revision": None,
            "id": "ORD001",
            "vendor": {
                "_id": vendor_ids[0],
                "country": {
                    "_id": country_ids[0],
                    "code": "LT",
                    "name": "Lithuania",
                },
            },
        },
        {
            "_type": "example/Order",
            "_id": ANY,
            "_revision": None,
            "id": "ORD002",
            "vendor": {
                "_id": vendor_ids[1],
                "country": {
                    "_id": country_ids[1],
                    "code": "PL",
                    "name": "Poland",
                },
            },
        },
    ]


def test_json_composite_ref_four_levels_xyze(rc: RawConfig, tmp_path: Path):
    json_data = {
        "order": [
            {
                "id": "ORD001",
                "vendor_code": "VEND001",
                "country_code": "LT",
                "region_code": "REG001",
                "region_name": "Vilnius Region",
            },
            {
                "id": "ORD002",
                "vendor_code": "VEND002",
                "country_code": "PL",
                "region_code": "REG002",
                "region_name": "Warsaw Region",
            },
        ]
    }
    path = tmp_path / "test.json"
    path.write_text(json.dumps(json_data))

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                      | type         | ref      | source       | access
    example                                       |              |          |              |
      | data                                      | dask/json    |          | {path}       |
      |   |   | Region                            |              | code     | order        |
      |   |   |   | code                          | string       |          | region_code  | open
      |   |   |   | name                          | string       |          | region_name  | open
      |   |   | Country                           |              | code     | order        |
      |   |   |   | code                          | string       |          | country_code | open
      |   |   |   | region                        | ref required | Region   | region_code  | open
      |   |   |   | region.code                   | string       |          | region_code  | open
      |   |   |   | region.name                   | string       |          | region_name  | open
      |   |   | Vendor                            |              | code     | order        |
      |   |   |   | code                          | string       |          | vendor_code  | open
      |   |   |   | country                       | ref required | Country  | country_code | open
      |   |   |   | country.code                  | string       |          | country_code | open
      |   |   |   | country.region.code           | string       |          | region_code  | open
      |   |   |   | country.region.name           | string       |          | region_name  | open
      |   |   | Order                             |              | id       | order        |
      |   |   |   | id                            | string       |          | id           | open
      |   |   |   | vendor                        | ref required | Vendor   | vendor_code  | open
      |   |   |   | vendor.country.code           | string       |          | country_code | open
      |   |   |   | vendor.country.region.code    | string       |          | region_code  | open
      |   |   |   | vendor.country.region.name    | string       |          | region_name  | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Order", ["getall"])
    app.authmodel("example/Vendor", ["getall"])
    app.authmodel("example/Country", ["getall"])
    app.authmodel("example/Region", ["getall"])

    vendor_resp = app.get("/example/Vendor")
    vendor_ids = [vendor_object["_id"] for vendor_object in vendor_resp.json()["_data"]]

    country_resp = app.get("/example/Country")
    country_ids = [country_object["_id"] for country_object in country_resp.json()["_data"]]

    region_resp = app.get("/example/Region")
    region_ids = [region_object["_id"] for region_object in region_resp.json()["_data"]]

    resp = app.get("/example/Order")
    assert resp.status_code == 200
    data = resp.json()["_data"]

    assert data == [
        {
            "_type": "example/Order",
            "_id": ANY,
            "_revision": None,
            "id": "ORD001",
            "vendor": {
                "_id": vendor_ids[0],
                "country": {
                    "_id": country_ids[0],
                    "code": "LT",
                    "region": {
                        "_id": region_ids[0],
                        "code": "REG001",
                        "name": "Vilnius Region",
                    },
                },
            },
        },
        {
            "_type": "example/Order",
            "_id": ANY,
            "_revision": None,
            "id": "ORD002",
            "vendor": {
                "_id": vendor_ids[1],
                "country": {
                    "_id": country_ids[1],
                    "code": "PL",
                    "region": {
                        "_id": region_ids[1],
                        "code": "REG002",
                        "name": "Warsaw Region",
                    },
                },
            },
        },
    ]


def test_json_incorrect_composite_property(rc: RawConfig, tmp_path: Path):
    json_data = {
        "order": [
            {"id": "ORD001", "vendor_code": "VEND001", "country_code": "LT", "country_name": "Lithuania"},
            {"id": "ORD002", "vendor_code": "VEND002", "country_code": "PL", "country_name": "Poland"},
        ]
    }
    path = tmp_path / "test.json"
    path.write_text(json.dumps(json_data))

    with pytest.raises(PartialIncorrectProperty):
        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                | type         | ref      | source       | access
        example                                 |              |          |              |
          | data                                | dask/json    |          | {path}       |
          |   |   | Country                     |              | code     | order        |
          |   |   |   | code                    | string       |          | country_code | open
          |   |   |   | name                    | string       |          | country_name | open
          |   |   | Vendor                      |              | code     | order        |
          |   |   |   | code                    | string       |          | vendor_code  | open
          |   |   |   | country                 | ref required | Country  | country_code | open
          |   |   |   | country.code            | string       |          | country_code | open
          |   |   |   | country.name            | string       |          | country_name | open
          |   |   | Order                       |              |          | order        |
          |   |   |   | id                      | string       |          | id           | open
          |   |   |   | vendor                  | ref required | Vendor   | vendor_code  | open
          |   |   |   | vendor.incorrect.code   | string       |          | country_code | open
          |   |   |   | vendor.country.name     | string       |          | country_name | open
        """,
            mode=Mode.external,
        )


def test_json_incorrect_composite_property_primary_key_values(rc: RawConfig, tmp_path: Path):
    json_data = {
        "order": [
            {"id": "ORD001", "vendor_code": "VEND001", "country_code": "LT", "country_name": "Lithuania"},
            {"id": "ORD002", "vendor_code": "VEND002", "country_code": "PL", "country_name": "Poland"},
        ]
    }
    path = tmp_path / "test.json"
    path.write_text(json.dumps(json_data))

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                | type         | ref      | source       | access
    example                                 |              |          |              |
      | data                                | dask/json    |          | {path}       |
      |   |   | Country                     |              | code     | order        |
      |   |   |   | code                    | string       |          | country_code | open
      |   |   |   | name                    | string       |          | country_name | open
      |   |   | Vendor                      |              | code     | order        |
      |   |   |   | code                    | string       |          | vendor_code  | open
      |   |   |   | country                 | ref required | Country  | vendor_code  | open
      |   |   |   | country.code            | string       |          | country_code | open
      |   |   |   | country.name            | string       |          | country_name | open
      |   |   | Order                       |              |          | order        |
      |   |   |   | id                      | string       |          | id           | open
      |   |   |   | vendor                  | ref required | Vendor   | vendor_code  | open
      |   |   |   | vendor.country.code     | string       |          | country_code | open
      |   |   |   | vendor.country.name     | string       |          | country_name | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)

    app.authmodel("example/Order", ["getall"])

    resp = app.get("/example/Order")
    assert resp.status_code == 400
    assert resp.json()["errors"][0]["code"] == "NoPrimaryKeyCandidatesFound"


def test_json_composite_ref_level_2_no_id(rc: RawConfig, tmp_path: Path):
    json_data = {
        "order": [
            {"id": "ORD001", "vendor_code": "VEND001", "country_code": "LT"},
            {"id": "ORD002", "vendor_code": "VEND002", "country_code": "PL"},
        ]
    }
    path = tmp_path / "test.json"
    path.write_text(json.dumps(json_data))

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property              | type         | ref     | source       | level | access
    example                               |              |         |              |       |
      | data                              | dask/json    |         | {path}       |       |
      |   |   | Country                   |              | code    | order        |       |
      |   |   |   | code                  | string       |         | country_code | 5     | open
      |   |   | Vendor                    |              | code    | order        |       |
      |   |   |   | code                  | string       |         | vendor_code  | 5     | open
      |   |   |   | country               | ref required | Country | country_code | 2     | open
      |   |   |   | country.code          | string       |         | country_code |       | open
      |   |   | Item                      |              | code    | order        |       |
      |   |   |   | code                  | string       |         | id           | 5     | open
      |   |   |   | vendor                | ref required | Vendor  | vendor_code  | 5     | open
      |   |   |   | vendor.code           | string       |         | vendor_code  |       | open
      |   |   |   | vendor.country.code   | string       |         | country_code |       | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Item", ["getall"])
    app.authmodel("example/Vendor", ["getall"])
    app.authmodel("example/Country", ["getall"])

    vendor_resp = app.get("/example/Vendor")
    vendor_ids = [vendor_id["_id"] for vendor_id in vendor_resp.json()["_data"]]

    resp = app.get("/example/Item")
    assert resp.status_code == 200
    data = resp.json()["_data"]

    assert data == [
        {
            "_type": "example/Item",
            "_id": ANY,
            "_revision": None,
            "code": "ORD001",
            "vendor": {"_id": vendor_ids[0], "code": "VEND001", "country": {"code": "LT"}},
        },
        {
            "_type": "example/Item",
            "_id": ANY,
            "_revision": None,
            "code": "ORD002",
            "vendor": {"_id": vendor_ids[1], "code": "VEND002", "country": {"code": "PL"}},
        },
    ]


def test_json_composite_ref_four_levels_composite_2_level(rc: RawConfig, tmp_path: Path):
    json_data = {
        "order": [
            {
                "id": "ORD001",
                "vendor_code": "VEND001",
                "country_code": "LT",
                "region_code": "REG001",
                "region_name": "Vilnius Region",
            },
            {
                "id": "ORD002",
                "vendor_code": "VEND002",
                "country_code": "PL",
                "region_code": "REG002",
                "region_name": "Warsaw Region",
            },
        ]
    }
    path = tmp_path / "test.json"
    path.write_text(json.dumps(json_data))

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                      | type         | ref      | source       | level      | access
    example                                       |              |          |              |            |
      | data                                      | dask/json    |          | {path}       |            |
      |   |   | Region                            |              | code     | order        |            |
      |   |   |   | code                          | string       |          | region_code  |            | open
      |   |   |   | name                          | string       |          | region_name  |            | open
      |   |   | Country                           |              | code     | order        |            |
      |   |   |   | code                          | string       |          | country_code |            | open
      |   |   |   | region                        | ref required | Region   | region_code  | 2          | open
      |   |   |   | region.code                   | string       |          | region_code  |            | open
      |   |   |   | region.name                   | string       |          | region_name  |            | open
      |   |   | Vendor                            |              | code     | order        |            |
      |   |   |   | code                          | string       |          | vendor_code  |            | open
      |   |   |   | country                       | ref required | Country  | country_code | 2          | open
      |   |   |   | country.code                  | string       |          | country_code |            | open
      |   |   |   | country.region.code           | string       |          | region_code  |            | open
      |   |   |   | country.region.name           | string       |          | region_name  |            | open
      |   |   | Order                             |              | id       | order        |            |
      |   |   |   | id                            | string       |          | id           |            | open
      |   |   |   | vendor                        | ref required | Vendor   | vendor_code  | 2          | open
      |   |   |   | vendor.country.code           | string       |          | country_code |            | open
      |   |   |   | vendor.country.region.code    | string       |          | region_code  |            | open
      |   |   |   | vendor.country.region.name    | string       |          | region_name  |            | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Order", ["getall"])

    resp = app.get("/example/Order")
    assert resp.status_code == 200
    data = resp.json()["_data"]
    assert data == [
        {
            "_type": "example/Order",
            "_id": ANY,
            "_revision": None,
            "id": "ORD001",
            "vendor": {
                "code": "VEND001",
                "country": {"code": "LT", "region": {"code": "REG001", "name": "Vilnius Region"}},
            },
        },
        {
            "_type": "example/Order",
            "_id": ANY,
            "_revision": None,
            "id": "ORD002",
            "vendor": {
                "code": "VEND002",
                "country": {"code": "PL", "region": {"code": "REG002", "name": "Warsaw Region"}},
            },
        },
    ]
