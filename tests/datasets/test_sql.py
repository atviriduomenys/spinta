import logging
import pathlib
import re
from typing import Dict
from typing import Tuple

import pytest
import sqlalchemy as sa
from _pytest.logging import LogCaptureFixture
from requests import PreparedRequest
from responses import POST
from responses import RequestsMock

from spinta.client import add_client_credentials
from spinta.core.config import RawConfig
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.client import create_client, create_rc, configure_remote_server
from spinta.testing.data import listdata
from spinta.testing.datasets import Sqlite
from spinta.testing.datasets import create_sqlite_db
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.utils import error, get_error_codes
from spinta.utils.schema import NA


@pytest.fixture(scope="module")
def translation_db():
    with create_sqlite_db(
        {
            "cities": [
                sa.Column("id", sa.Integer, primary_key=True),
            ],
            "translations": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("lang", sa.Text),
                sa.Column("name", sa.Text),
                sa.Column("city_id", sa.Integer),
            ],
        }
    ) as db:
        db.write(
            "translations",
            [
                {"id": 0, "lang": "lt", "name": "Vilniaus miestas", "city_id": 0},
                {"id": 1, "lang": "en", "name": "City of Vilnius", "city_id": 0},
                {"id": 2, "lang": "lt", "name": "Kauno miestas", "city_id": 1},
                {"id": 3, "lang": "en", "name": "City of Kaunas", "city_id": 1},
            ],
        )
        db.write(
            "cities",
            [
                {"id": 0},
                {"id": 1},
            ],
        )
        yield db


@pytest.fixture(scope="module")
def geodb():
    with create_sqlite_db(
        {
            "salis": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("kodas", sa.Text),
                sa.Column("pavadinimas", sa.Text),
            ],
            "miestas": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("pavadinimas", sa.Text),
                sa.Column("salis", sa.Text),
            ],
            "cities": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("name", sa.Text),
                sa.Column("country", sa.Integer),
            ],
            "test": [sa.Column("id", sa.Integer, primary_key=True), sa.Column("text", sa.Text)],
        }
    ) as db:
        db.write(
            "salis",
            [
                {"kodas": "lt", "pavadinimas": "Lietuva"},
                {"kodas": "lv", "pavadinimas": "Latvija"},
                {"kodas": "ee", "pavadinimas": "Estija"},
            ],
        )
        db.write(
            "miestas",
            [
                {"salis": "lt", "pavadinimas": "Vilnius"},
                {"salis": "lv", "pavadinimas": "Ryga"},
                {"salis": "ee", "pavadinimas": "Talinas"},
            ],
        )
        db.write(
            "cities",
            [
                {"name": "Vilnius", "country": 2},
            ],
        )
        db.write(
            "test",
            [
                {"id": 0, "text": '"TEST"'},
                {"id": 1, "text": 'test "TEST"'},
                {"id": 2, "text": "'TEST'"},
                {"id": 3, "text": "test 'TEST'"},
                {"id": 4, "text": "\"TEST\" 'TEST'"},
            ],
        )
        yield db


@pytest.fixture(scope="module")
def geodb_denorm():
    with create_sqlite_db(
        {
            "PLANET": [
                sa.Column("id", sa.Integer),
                sa.Column("code", sa.Text, primary_key=True),
                sa.Column("name", sa.Text),
            ],
            "COUNTRY": [
                sa.Column("code", sa.Text, primary_key=True),
                sa.Column("name", sa.Text),
                sa.Column("planet", sa.Text),
                sa.Column("planet_name", sa.Text),
            ],
            "CITY": [
                sa.Column("code", sa.Text, primary_key=True),
                sa.Column("name", sa.Text),
                sa.Column("country", sa.Text),
                sa.Column("countryYear", sa.Integer),
                sa.Column("countryName", sa.Text),
                sa.Column("planetName", sa.Text),
            ],
        }
    ) as db:
        db.write(
            "PLANET",
            [
                {"id": 0, "code": "ER", "name": "Earth"},
                {"id": 0, "code": "MR", "name": "Mars"},
                {"id": 0, "code": "JP", "name": "Jupyter"},
            ],
        )
        db.write(
            "COUNTRY",
            [
                {"code": "LT", "name": "Lithuania", "planet": "ER"},
                {"code": "LV", "name": "Latvia", "planet": "MR"},
                {"code": "EE", "name": "Estonia", "planet": "JP"},
            ],
        )
        db.write(
            "CITY",
            [
                {
                    "code": "VLN",
                    "name": "Vilnius",
                    "country": "LT",
                    "countryYear": 1204,
                    "countryName": "Lietuva",
                    "planetName": "Zeme",
                },
                {
                    "code": "RYG",
                    "name": "Ryga",
                    "country": "LV",
                    "countryYear": 1408,
                    "countryName": "Latvia",
                    "planetName": "Marsas",
                },
                {
                    "code": "TLN",
                    "name": "Talin",
                    "country": "EE",
                    "countryYear": 1784,
                    "countryName": "Estija",
                    "planetName": "Jupiteris",
                },
            ],
        )
        yield db


def test_filter(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | source      | prepare   | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |           |        |         |       |        |     | Example |
       |   | data                 |             |           | sql    |         |       |        |     | Data    |
       |   |   |                  |             |           |        |         |       |        |     |         |
       |   |   |   | Country      | salis       | code='lt' |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |           | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |           | string |         | 3     | open   |     | Name    |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Country")
    assert listdata(resp, "code", "name") == [
        ("lt", "Lietuva"),
    ]


def test_filter_join(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | type   | ref     | source      | prepare           | level | access | uri | title   | description
       | datasets/gov/example     |        |         |             |                   |       |        |     | Example |
       |   | data                 | sql    |         |             |                   |       |        |     | Data    |
       |   |   |                  |        |         |             |                   |       |        |     |         |
       |   |   |   | Country      |        | code    | salis       |                   |       |        |     | Country |
       |   |   |   |   | code     | string |         | kodas       |                   | 3     | open   |     | Code    |
       |   |   |   |   | name     | string |         | pavadinimas |                   | 3     | open   |     | Name    |
       |   |   |                  |        |         |             |                   |       |        |     |         |
       |   |   |   | City         |        | name    | miestas     | country.code='lt' |       |        |     | City    |
       |   |   |   |   | name     | string |         | pavadinimas |                   | 3     | open   |     | Name    |
       |   |   |   |   | country  | ref    | Country | salis       |                   | 4     | open   |     | Country |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Country")
    codes = dict(listdata(resp, "_id", "code"))

    resp = app.get("/datasets/gov/example/City?sort(name)")
    data = listdata(resp, "country._id", "name")
    data = [(codes.get(country), city) for country, city in data]
    assert data == [
        ("lt", "Vilnius"),
    ]


def test_filter_join_nested(
    context,
    rc: RawConfig,
    tmp_path: pathlib.Path,
    sqlite: Sqlite,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref     | source   | prepare   | access
    example/join/nested      |        |         |          |           |
      | data                 | sql    |         |          |           |
      |   |   | Country      |        | code    | COUNTRY  | code='lt' |
      |   |   |   | code     | string |         | CODE     |           | open
      |   |   |   | name     | string |         | NAME     |           | open
      |   |   | City         |        | name    | CITY     |           |
      |   |   |   | name     | string |         | NAME     |           | open
      |   |   |   | country  | ref    | Country | COUNTRY  |           | open
      |   |   | District     |        | name    | DISTRICT |           |
      |   |   |   | name     | string |         | NAME     |           | open
      |   |   |   | city     | ref    | City    | CITY     |           | open
    """),
    )

    # Configure local server with SQL backend
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("CODE", sa.Text),
                sa.Column("NAME", sa.Text),
            ],
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY", sa.Text),
            ],
            "DISTRICT": [
                sa.Column("NAME", sa.Text),
                sa.Column("CITY", sa.Text),
            ],
        }
    )
    sqlite.write(
        "COUNTRY",
        [
            {"NAME": "Lithuania", "CODE": "lt"},
        ],
    )
    sqlite.write(
        "CITY",
        [
            {"NAME": "Vilnius", "COUNTRY": "lt"},
        ],
    )
    sqlite.write(
        "DISTRICT",
        [
            {"NAME": "Old town", "CITY": "Vilnius"},
            {"NAME": "New town", "CITY": "Vilnius"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel("example/join/nested", ["search"])
    resp = app.get("/example/join/nested/District?select(city.name, name)")
    assert listdata(resp) == [
        ("Vilnius", "New town"),
        ("Vilnius", "Old town"),
    ]


def test_filter_join_array_value(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | source      | prepare                  | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |                          |        |         |       |        |     | Example |
       |   | data                 |             |                          | sql    |         |       |        |     | Data    |
       |   |   |                  |             |                          |        |         |       |        |     |         |
       |   |   |   | Country      | salis       |                          |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |                          | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |                          | string |         | 3     | open   |     | Name    |
       |   |   |                  |             |                          |        |         |       |        |     |         |
       |   |   |   | City         | miestas     | country.code=['lt','lv'] |        | name    |       |        |     | City    |
       |   |   |   |   | name     | pavadinimas |                          | string |         | 3     | open   |     | Name    |
       |   |   |   |   | country  | salis       |                          | ref    | Country | 4     | open   |     | Country |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Country")
    codes = dict(listdata(resp, "_id", "code"))

    resp = app.get("/datasets/gov/example/City?sort(name)")
    data = listdata(resp, "country._id", "name", sort="name")
    data = [(codes.get(country), city) for country, city in data]
    assert data == [
        ("lv", "Ryga"),
        ("lt", "Vilnius"),
    ]


def test_filter_join_ne_array_value(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | source      | prepare                   | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |                           |        |         |       |        |     | Example |
       |   | data                 |             |                           | sql    |         |       |        |     | Data    |
       |   |   |                  |             |                           |        |         |       |        |     |         |
       |   |   |   | Country      | salis       |                           |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |                           | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |                           | string |         | 3     | open   |     | Name    |
       |   |   |                  |             |                           |        |         |       |        |     |         |
       |   |   |   | City         | miestas     | country.code!=['lt','lv'] |        | name    |       |        |     | City    |
       |   |   |   |   | name     | pavadinimas |                           | string |         | 3     | open   |     | Name    |
       |   |   |   |   | country  | salis       |                           | ref    | Country | 4     | open   |     | Country |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Country")
    codes = dict(listdata(resp, "_id", "code"))

    resp = app.get("/datasets/gov/example/City?sort(name)")
    data = listdata(resp, "country._id", "name", sort="name")
    data = [(codes.get(country), city) for country, city in data]
    assert data == [
        ("ee", "Talinas"),
    ]


def test_filter_multi_column_pk(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | source      | prepare            | type    | ref            | level | access | uri | title   | description
       | keymap                   |             |                    |         |                |       |        |     | Example |
       |   | data                 |             |                    | sql     |                |       |        |     | Data    |
       |   |   |                  |             |                    |         |                |       |        |     |         |
       |   |   |   | Country      | salis       |                    |         | id             |       |        |     | Country |
       |   |   |   |   | id       | id          |                    | integer |                | 3     | open   |     | Code    |
       |   |   |   |   | code     | kodas       |                    | string  |                | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |                    | string  |                | 3     | open   |     | Name    |
       |   |   |                  |             |                    |         |                |       |        |     |         |
       |   |   |   | City         | miestas     | country.code!='ee' |         | name           |       |        |     | City    |
       |   |   |   |   | name     | pavadinimas |                    | string  |                | 3     | open   |     | Name    |
       |   |   |   |   | country  | salis       |                    | ref     | Country[code]  | 4     | open   |     | Country |
    """),
    )
    app = create_client(rc, tmp_path, geodb)

    resp_country = app.get("keymap/Country")
    codes = dict(listdata(resp_country, "_id", "code"))
    resp_city = app.get("keymap/City?sort(name)")
    data = listdata(resp_city, "country._id", "name", sort="name")
    data = [(codes.get(country), city) for country, city in data]
    assert data == [
        ("lv", "Ryga"),
        ("lt", "Vilnius"),
    ]


def test_getall(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | source      | prepare | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |         |        |         |       |        |     | Example |
       |   | data                 |             |         | sql    |         |       |        |     | Data    |
       |   |   |                  |             |         |        |         |       |        |     |         |
       |   |   |   | Country      | salis       |         |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |         | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |         | string |         | 3     | open   |     | Name    |
       |   |   |                  |             |         |        |         |       |        |     |         |
       |   |   |   | City         | miestas     |         |        | name    |       |        |     | City    |
       |   |   |   |   | name     | pavadinimas |         | string |         | 3     | open   |     | Name    |
       |   |   |   |   | country  | salis       |         | ref    | Country | 4     | open   |     | Country |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Country?sort(code)")
    codes = dict(listdata(resp, "_id", "code"))
    assert listdata(resp, "code", "name", "_type") == [
        ("ee", "Estija", "datasets/gov/example/Country"),
        ("lt", "Lietuva", "datasets/gov/example/Country"),
        ("lv", "Latvija", "datasets/gov/example/Country"),
    ]

    resp = app.get("/datasets/gov/example/City?sort(name)")
    data = listdata(resp, "country._id", "name", "_type", sort="name")
    data = [(codes.get(country), city, _type) for country, city, _type in data]
    assert data == [
        ("lv", "Ryga", "datasets/gov/example/City"),
        ("ee", "Talinas", "datasets/gov/example/City"),
        ("lt", "Vilnius", "datasets/gov/example/City"),
    ]


def test_select(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | source      | prepare | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |         |        |         |       |        |     | Example |
       |   | data                 |             |         | sql    |         |       |        |     | Data    |
       |   |   |                  |             |         |        |         |       |        |     |         |
       |   |   |   | Country      | salis       |         |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |         | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |         | string |         | 3     | open   |     | Name    |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Country?select(code,name)")
    assert listdata(resp, "code", "name") == [
        ("ee", "Estija"),
        ("lt", "Lietuva"),
        ("lv", "Latvija"),
    ]


@pytest.mark.skip("TODO")
def test_select_len(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | source      | prepare | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |         |        |         |       |        |     | Example |
       |   | data                 |             |         | sql    |         |       |        |     | Data    |
       |   |   |                  |             |         |        |         |       |        |     |         |
       |   |   |   | Country      | salis       |         |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |         | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |         | string |         | 3     | open   |     | Name    |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Country?select(code,len(name))")
    assert listdata(resp, "code", "len(name)") == [
        ("ee", 6),
        ("lt", 7),
        ("lv", 7),
    ]


def test_filter_len(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | source      | prepare | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |         |        |         |       |        |     | Example |
       |   | data                 |             |         | sql    |         |       |        |     | Data    |
       |   |   |                  |             |         |        |         |       |        |     |         |
       |   |   |   | Country      | salis       |         |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |         | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |         | string |         | 3     | open   |     | Name    |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Country?select(code,name)&len(name)=7&sort(code)")
    assert listdata(resp, "code", "name") == [
        ("lt", "Lietuva"),
        ("lv", "Latvija"),
    ]


def test_private_property(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | Country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     | private |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     | open    |     | Name    |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Country")
    assert listdata(resp) == [
        "Latvija",
        "Lietuva",
    ]


def test_all_private_properties(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | Country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     | private |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     | private |     | Name    |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Country")
    assert error(resp, status=401) == "AuthorizedClientsOnly"


def test_default_access(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | Country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     |         |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     |         |     | Name    |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Country")
    assert error(resp, status=401) == "AuthorizedClientsOnly"


def test_model_open_access(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | Country      | salis       | code!='ee' |        | code    |       | open    |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     |         |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     |         |     | Name    |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Country")
    assert listdata(resp) == [
        ("lt", "Lietuva"),
        ("lv", "Latvija"),
    ]


def test_property_public_access(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | Country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     | public  |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     | open    |     | Name    |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Country")
    assert listdata(resp) == [
        "Latvija",
        "Lietuva",
    ]

    resp = app.get("/datasets/gov/example/Country", headers={"Accept": "text/html"})
    assert listdata(resp) == [
        "Latvija",
        "Lietuva",
    ]


def test_select_protected_property(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | Country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     | public  |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     | open    |     | Name    |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Country?select(code,name)")
    assert error(resp) == "PropertyNotFound"

    resp = app.get("/datasets/gov/example/Country?select(code,name)", headers={"Accept": "text/html"})
    assert error(resp) == "PropertyNotFound"


def test_ns_getall(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | Country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     | public  |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     | open    |     | Name    |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example")
    assert listdata(resp, "name", "title") == [
        ("datasets/gov/example/Country", "Country"),
    ]

    resp = app.get("/datasets/gov/example", headers={"Accept": "text/html"})
    assert listdata(resp, "name", "title") == [
        ("📄 Country", "Country"),
    ]


def test_push(context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property| type   | ref     | source       | access
    datasets/gov/example    |        |         |              |
      | data                | sql    |         |              |
      |   |                 |        |         |              |
      |   |   | Country     |        | code    | salis        |
      |   |   |   | code    | string |         | kodas        | open
      |   |   |   | name    | string |         | pavadinimas  | open
      |   |                 |        |         |              |
      |   |   | City        |        | name    | miestas      |
      |   |   |   | name    | string |         | pavadinimas  | open
      |   |   |   | country | ref    | Country | salis        | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "datasets/gov/example",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
        ],
    )
    assert result.exit_code == 0

    remote.app.authmodel("datasets/gov/example/Country", ["getall"])
    resp = remote.app.get("/datasets/gov/example/Country")
    assert listdata(resp, "code", "name") == [("ee", "Estija"), ("lt", "Lietuva"), ("lv", "Latvija")]

    codes = dict(listdata(resp, "_id", "code"))
    remote.app.authmodel("datasets/gov/example/City", ["getall"])
    resp = remote.app.get("/datasets/gov/example/City")
    data = listdata(resp, "country._id", "name")
    data = [(codes.get(country), city) for country, city in data]
    assert sorted(data) == [
        ("ee", "Talinas"),
        ("lt", "Vilnius"),
        ("lv", "Ryga"),
    ]

    # Add new data to local server
    geodb.write(
        "miestas",
        [
            {"salis": "lt", "pavadinimas": "Kaunas"},
        ],
    )

    # Push data from local to remote.
    cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "datasets/gov/example",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
        ],
    )

    resp = remote.app.get("/datasets/gov/example/Country")
    assert listdata(resp, "code", "name") == [("ee", "Estija"), ("lt", "Lietuva"), ("lv", "Latvija")]

    resp = remote.app.get("/datasets/gov/example/City")
    data = listdata(resp, "country._id", "name")
    data = [(codes.get(country), city) for country, city in data]
    assert sorted(data) == [
        ("ee", "Talinas"),
        ("lt", "Kaunas"),
        ("lt", "Vilnius"),
        ("lv", "Ryga"),
    ]


def test_push_dry_run(context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property| type   | ref     | source       | access
    datasets/gov/example    |        |         |              |
      | data                | sql    |         |              |
      |   |                 |        |         |              |
      |   |   | Country     |        | code    | salis        |
      |   |   |   | code    | string |         | kodas        | open
      |   |   |   | name    | string |         | pavadinimas  | open
      |   |                 |        |         |              |
      |   |   | City        |        | name    | miestas      |
      |   |   |   | name    | string |         | pavadinimas  | open
      |   |   |   | country | ref    | Country | salis        | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "datasets/gov/example",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--dry-run",
        ],
    )
    assert result.exit_code == 0

    remote.app.authmodel("datasets/gov/example/Country", ["getall"])
    resp = remote.app.get("/datasets/gov/example/Country")
    assert listdata(resp, "code", "name") == []


def test_no_primary_key(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | source      | type   | ref | access
    datasets/gov/example     |             |        |     |
      | data                 |             | sql    |     |
      |   |                  |             |        |     |
      |   |   | Country      | salis       |        |     |
      |   |   |   | code     | kodas       | string |     | open
      |   |   |   | name     | pavadinimas | string |     | open
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Country")
    codes = dict(listdata(resp, "_id", "code"))
    data = listdata(resp, "_id", "code", "name", sort="code")
    data = [(codes.get(_id), code, name) for _id, code, name in data]
    assert data == [
        ("ee", "ee", "Estija"),
        ("lt", "lt", "Lietuva"),
        ("lv", "lv", "Latvija"),
    ]


def test_count(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | source      | type   | ref | access
    datasets/gov/example     |             |        |     |
      | data                 |             | sql    |     |
      |   |                  |             |        |     |
      |   |   | Country      | salis       |        |     |
      |   |   |   | code     | kodas       | string |     | open
      |   |   |   | name     | pavadinimas | string |     | open
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    # Backwards compatibility support
    resp = app.get("/datasets/gov/example/Country?count()")
    assert listdata(resp) == [3]

    resp = app.get("/datasets/gov/example/Country?select(count())")
    assert listdata(resp) == [3]


def test_push_chunks(
    context,
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | source      | type   | ref     | access
    datasets/gov/example     |             |        |         |
      | data                 |             | sql    |         |
      |   |                  |             |        |         |
      |   |   | Country      | salis       |        | code    |
      |   |   |   | code     | kodas       | string |         | open
      |   |   |   | name     | pavadinimas | string |         | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "datasets/gov/example",
            "-o",
            "spinta+" + remote.url,
            "--credentials",
            remote.credsfile,
            "--chunk-size=1",
        ],
    )

    remote.app.authmodel("datasets/gov/example/Country", ["getall"])
    resp = remote.app.get("/datasets/gov/example/Country")
    assert listdata(resp, "code", "name") == [("ee", "Estija"), ("lt", "Lietuva"), ("lv", "Latvija")]


def test_push_state(context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | source      | type   | ref     | access
    datasets/gov/example     |             |        |         |
      | data                 |             | sql    |         |
      |   |                  |             |        |         |
      |   |   | Country      | salis       |        | code    |
      |   |   |   | code     | kodas       | string |         | open
      |   |   |   | name     | pavadinimas | string |         | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push one row, save state and stop.
    cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "datasets/gov/example",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--chunk-size",
            "1k",
            "--stop-time",
            "1h",
            "--stop-row",
            "1",
            "--state",
            tmp_path / "state.db",
        ],
    )

    remote.app.authmodel("datasets/gov/example/Country", ["getall"])
    resp = remote.app.get("/datasets/gov/example/Country")
    assert len(listdata(resp)) == 1

    cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "datasets/gov/example",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--stop-row",
            "1",
            "--state",
            tmp_path / "state.db",
        ],
    )

    resp = remote.app.get("/datasets/gov/example/Country")
    assert len(listdata(resp)) == 2


def test_prepared_property(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property  | type   | ref  | source      | prepare | access
    datasets/gov/example      |        |      |             |         |
      | data                  | sql    |      |             |         |
      |   |   | Country       |        | code | salis       |         | open
      |   |   |   | code      | string |      | kodas       |         |
      |   |   |   | name      | string |      | pavadinimas |         |
      |   |   |   | continent | string |      |             | 'EU'    |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Country")
    assert listdata(resp, "continent", "code", "name") == [
        ("EU", "ee", "Estija"),
        ("EU", "lt", "Lietuva"),
        ("EU", "lv", "Latvija"),
    ]


def test_composite_keys(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property     | type   | ref             | source    | prepare                 | access
    datasets/ds              |        |                 |           |                         |
      | rs                   | sql    |                 |           |                         |
      |   | Country          |        | code, continent | COUNTRY   |                         | open
      |   |   | name         | string |                 | NAME      |                         |
      |   |   | code         | string |                 | CODE      |                         |
      |   |   | continent    | string |                 | CONTINENT |                         |
      |   | City             |        | name, country   | CITY      |                         | open
      |   |   | name         | string |                 | NAME      |                         |
      |   |   | country_code | string |                 | COUNTRY   |                         |
      |   |   | continent    | string |                 | CONTINENT |                         |
      |   |   | country      | ref    | Country         |           | country_code, continent |
    """),
    )

    app = create_client(rc, tmp_path, sqlite)

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
                sa.Column("CODE", sa.Text),
                sa.Column("CONTINENT", sa.Text),
            ],
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY", sa.Text),
                sa.Column("CONTINENT", sa.Text),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"CONTINENT": "eu", "CODE": "lt", "NAME": "Lithuania"},
            {"CONTINENT": "eu", "CODE": "lv", "NAME": "Latvia"},
            {"CONTINENT": "eu", "CODE": "ee", "NAME": "Estonia"},
        ],
    )
    sqlite.write(
        "CITY",
        [
            {"CONTINENT": "eu", "COUNTRY": "lt", "NAME": "Vilnius"},
            {"CONTINENT": "eu", "COUNTRY": "lt", "NAME": "Kaunas"},
            {"CONTINENT": "eu", "COUNTRY": "lv", "NAME": "Riga"},
            {"CONTINENT": "eu", "COUNTRY": "ee", "NAME": "Tallinn"},
        ],
    )

    resp = app.get("/datasets/ds/Country")
    data = listdata(resp, "_id", "continent", "code", "name", sort="name")
    country_key_map = {_id: (continent, code) for _id, continent, code, name in data}
    data = [(country_key_map.get(_id), continent, code, name) for _id, continent, code, name in data]
    assert data == [
        (("eu", "ee"), "eu", "ee", "Estonia"),
        (("eu", "lv"), "eu", "lv", "Latvia"),
        (("eu", "lt"), "eu", "lt", "Lithuania"),
    ]

    resp = app.get("/datasets/ds/City")
    data = listdata(resp, "country", "name", sort="name")
    data = [
        (
            {
                **country,
                "_id": country_key_map.get(country.get("_id")),
            },
            name,
        )
        for country, name in data
    ]
    assert data == [
        ({"_id": ("eu", "lt")}, "Kaunas"),
        ({"_id": ("eu", "lv")}, "Riga"),
        ({"_id": ("eu", "ee")}, "Tallinn"),
        ({"_id": ("eu", "lt")}, "Vilnius"),
    ]


def test_composite_ref_keys(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property     | type    | ref                     | source       | prepare                 | access
    datasets/ds              |         |                         |              |                         |
      | rs                   | sql     |                         |              |                         |
      |   | Continent        |         | id                      | CONTINENT    |                         | open
      |   |   | id           | integer |                         | ID           |                         |
      |   |   | name         | string  |                         | NAME         |                         |
      |   | Country          |         | id                      | COUNTRY      |                         | open
      |   |   | id           | integer |                         | ID           |                         |
      |   |   | name         | string  |                         | NAME         |                         |
      |   |   | code         | string  |                         | CODE         |                         |
      |   |   | continent    | ref     | Continent               | CONTINENT_ID |                         |
      |   | City             |         | id                      | CITY         |                         | open
      |   |   | id           | integer |                         | ID           |                         |
      |   |   | name         | string  |                         | NAME         |                         |
      |   |   | continent    | ref     | Continent               | CONTINENT_ID |                         |
      |   |   | country_code | string  |                         | COUNTRY_CODE |                         |
      |   |   | country      | ref     | Country[continent,code] |              | continent, country_code |
    """),
    )

    app = create_client(rc, tmp_path, sqlite)

    sqlite.init(
        {
            "CONTINENT": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
            ],
            "COUNTRY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("CODE", sa.Text),
                sa.Column("CONTINENT_ID", sa.Integer, sa.ForeignKey("CONTINENT.ID")),
                sa.Column("NAME", sa.Text),
            ],
            "CITY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("CONTINENT_ID", sa.Integer, sa.ForeignKey("CONTINENT.ID")),
                sa.Column("COUNTRY_CODE", sa.Text),
                sa.Column("NAME", sa.Text),
            ],
        }
    )

    sqlite.write(
        "CONTINENT",
        [
            {"ID": 1, "NAME": "Europe"},
            {"ID": 2, "NAME": "Africa"},
        ],
    )
    sqlite.write(
        "COUNTRY",
        [
            {"ID": 1, "CONTINENT_ID": 1, "CODE": "lt", "NAME": "Lithuania"},
            {"ID": 2, "CONTINENT_ID": 1, "CODE": "lv", "NAME": "Latvia"},
            {"ID": 3, "CONTINENT_ID": 1, "CODE": "ee", "NAME": "Estonia"},
        ],
    )
    sqlite.write(
        "CITY",
        [
            {"ID": 1, "CONTINENT_ID": 1, "COUNTRY_CODE": "lt", "NAME": "Vilnius"},
            {"ID": 2, "CONTINENT_ID": 1, "COUNTRY_CODE": "lt", "NAME": "Kaunas"},
            {"ID": 3, "CONTINENT_ID": 1, "COUNTRY_CODE": "lv", "NAME": "Riga"},
            {"ID": 4, "CONTINENT_ID": 1, "COUNTRY_CODE": "ee", "NAME": "Tallinn"},
        ],
    )

    resp = app.get("/datasets/ds/Continent")
    continents = dict(listdata(resp, "_id", "name"))
    assert sorted(continents.values()) == [
        "Africa",
        "Europe",
    ]

    resp = app.get("/datasets/ds/Country")
    countries = dict(listdata(resp, "_id", "name"))
    assert sorted(countries.values()) == [
        "Estonia",
        "Latvia",
        "Lithuania",
    ]

    resp = app.get("/datasets/ds/City")
    cities = listdata(resp, "name", "country", "continent", sort="name")
    cities = [
        (
            name,
            countries[country["_id"]],
            continents[continent["_id"]],
        )
        for name, country, continent in cities
    ]
    assert cities == [
        ("Kaunas", "Lithuania", "Europe"),
        ("Riga", "Latvia", "Europe"),
        ("Tallinn", "Estonia", "Europe"),
        ("Vilnius", "Lithuania", "Europe"),
    ]


def test_composite_non_pk_keys(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property     | type   | ref                     | source    | prepare                 | access
    datasets/ds              |        |                         |           |                         |
      | rs                   | sql    |                         |           |                         |
      |   | Country          |        | code                    | COUNTRY   |                         | open
      |   |   | name         | string |                         | NAME      |                         |
      |   |   | code         | string |                         | CODE      |                         |
      |   |   | continent    | string |                         | CONTINENT |                         |
      |   | City             |        | name, country           | CITY      |                         | open
      |   |   | name         | string |                         | NAME      |                         |
      |   |   | country_code | string |                         | COUNTRY   |                         |
      |   |   | continent    | string |                         | CONTINENT |                         |
      |   |   | country      | ref    | Country[continent,code] |           | continent, country_code |
    """),
    )

    app = create_client(rc, tmp_path, sqlite)

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
                sa.Column("CODE", sa.Text),
                sa.Column("CONTINENT", sa.Text),
            ],
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY", sa.Text),
                sa.Column("CONTINENT", sa.Text),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"CONTINENT": "eu", "CODE": "lt", "NAME": "Lithuania"},
            {"CONTINENT": "eu", "CODE": "lv", "NAME": "Latvia"},
            {"CONTINENT": "eu", "CODE": "ee", "NAME": "Estonia"},
        ],
    )
    sqlite.write(
        "CITY",
        [
            {"CONTINENT": "eu", "COUNTRY": "lt", "NAME": "Vilnius"},
            {"CONTINENT": "eu", "COUNTRY": "lt", "NAME": "Kaunas"},
            {"CONTINENT": "eu", "COUNTRY": "lv", "NAME": "Riga"},
            {"CONTINENT": "eu", "COUNTRY": "ee", "NAME": "Tallinn"},
        ],
    )

    resp = app.get("/datasets/ds/Country")
    data = listdata(resp, "_id", "continent", "code", "name", sort="name")
    country_key_map = {_id: (continent, code) for _id, continent, code, name in data}
    data = [(country_key_map.get(_id), continent, code, name) for _id, continent, code, name in data]
    assert data == [
        (("eu", "ee"), "eu", "ee", "Estonia"),
        (("eu", "lv"), "eu", "lv", "Latvia"),
        (("eu", "lt"), "eu", "lt", "Lithuania"),
    ]

    resp = app.get("/datasets/ds/City")
    data = listdata(resp, "country", "name", sort="name")
    data = [
        (
            {
                **country,
                "_id": country_key_map.get(country.get("_id")),
            },
            name,
        )
        for country, name in data
    ]
    assert data == [
        ({"_id": ("eu", "lt")}, "Kaunas"),
        ({"_id": ("eu", "lv")}, "Riga"),
        ({"_id": ("eu", "ee")}, "Tallinn"),
        ({"_id": ("eu", "lt")}, "Vilnius"),
    ]


def test_composite_non_pk_keys_with_filter(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property     | type   | ref                     | source    | prepare                 | access
    datasets/ds              |        |                         |           |                         |
      | rs                   | sql    |                         |           |                         |
      |   | Country          |        | code                    | COUNTRY   |                         | open
      |   |   | name         | string |                         | NAME      |                         |
      |   |   | code         | string |                         | CODE      |                         |
      |   |   | continent    | string |                         | CONTINENT |                         |
      |   | City             |        | name, country           | CITY      | country.code='lt'       | open
      |   |   | name         | string |                         | NAME      |                         |
      |   |   | country_code | string |                         | COUNTRY   |                         |
      |   |   | continent    | string |                         | CONTINENT |                         |
      |   |   | country      | ref    | Country[continent,code] |           | continent, country_code |
    """),
    )

    app = create_client(rc, tmp_path, sqlite)

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
                sa.Column("CODE", sa.Text),
                sa.Column("CONTINENT", sa.Text),
            ],
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY", sa.Text),
                sa.Column("CONTINENT", sa.Text),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"CONTINENT": "eu", "CODE": "lt", "NAME": "Lithuania"},
            {"CONTINENT": "eu", "CODE": "lv", "NAME": "Latvia"},
            {"CONTINENT": "eu", "CODE": "ee", "NAME": "Estonia"},
        ],
    )
    sqlite.write(
        "CITY",
        [
            {"CONTINENT": "eu", "COUNTRY": "lt", "NAME": "Vilnius"},
            {"CONTINENT": "eu", "COUNTRY": "lt", "NAME": "Kaunas"},
            {"CONTINENT": "eu", "COUNTRY": "lv", "NAME": "Riga"},
            {"CONTINENT": "eu", "COUNTRY": "ee", "NAME": "Tallinn"},
        ],
    )

    resp = app.get("/datasets/ds/Country")
    data = listdata(resp, "_id", "continent", "code", "name", sort="name")
    country_key_map = {_id: (continent, code) for _id, continent, code, name in data}
    data = [(country_key_map.get(_id), continent, code, name) for _id, continent, code, name in data]
    assert data == [
        (("eu", "ee"), "eu", "ee", "Estonia"),
        (("eu", "lv"), "eu", "lv", "Latvia"),
        (("eu", "lt"), "eu", "lt", "Lithuania"),
    ]

    resp = app.get("/datasets/ds/City")
    data = listdata(resp, "country", "name", sort="name")
    data = [
        (
            {
                **country,
                "_id": country_key_map.get(country.get("_id")),
            },
            name,
        )
        for country, name in data
    ]
    assert data == [
        ({"_id": ("eu", "lt")}, "Kaunas"),
        ({"_id": ("eu", "lt")}, "Vilnius"),
    ]


def test_access_private_primary_key(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property     | type   | ref     | source  | access
    datasets/ds              |        |         |         |
      | rs                   | sql    |         |         |
      |   | Country          |        | code    | COUNTRY |
      |   |   | name         | string |         | NAME    | open
      |   |   | code         | string |         | CODE    | private
      |   | City             |        | country | CITY    |
      |   |   | name         | string |         | NAME    | open
      |   |   | country      | ref    | Country | COUNTRY | open
    """),
    )

    app = create_client(rc, tmp_path, sqlite)

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
                sa.Column("CODE", sa.Text),
            ],
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY", sa.Text),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"CODE": "lt", "NAME": "Lithuania"},
            {"CODE": "lv", "NAME": "Latvia"},
            {"CODE": "ee", "NAME": "Estonia"},
        ],
    )
    sqlite.write(
        "CITY",
        [
            {"COUNTRY": "lt", "NAME": "Vilnius"},
            {"COUNTRY": "lt", "NAME": "Kaunas"},
            {"COUNTRY": "lv", "NAME": "Riga"},
            {"COUNTRY": "ee", "NAME": "Tallinn"},
        ],
    )

    resp = app.get("/datasets/ds/Country")
    data = listdata(resp, "_id", "code", "name", sort="name")
    country_key_map = {_id: name for _id, code, name in data}
    data = [(country_key_map.get(_id), code, name) for _id, code, name in data]
    assert data == [
        ("Estonia", NA, "Estonia"),
        ("Latvia", NA, "Latvia"),
        ("Lithuania", NA, "Lithuania"),
    ]

    resp = app.get("/datasets/ds/City")
    data = listdata(resp, "country", "name", sort="name")
    data = [
        (
            {
                **country,
                "_id": country_key_map.get(country.get("_id")),
            },
            name,
        )
        for country, name in data
    ]
    assert data == [
        ({"_id": "Lithuania"}, "Kaunas"),
        ({"_id": "Latvia"}, "Riga"),
        ({"_id": "Estonia"}, "Tallinn"),
        ({"_id": "Lithuania"}, "Vilnius"),
    ]


def test_enum(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property     | type   | ref     | source  | prepare | access
    datasets/gov/example     |        |         |         |         |
      | resource             | sql    |         |         |         |
      |   | Country          |        | name    | COUNTRY |         |
      |   |   | name         | string |         | NAME    |         | open
      |   |   | driving      | string |         | DRIVING |         | open
                             | enum   |         | l       | 'left'  | open
                             |        |         | r       | 'right' | open
    """),
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
                sa.Column("DRIVING", sa.Text),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"DRIVING": "r", "NAME": "Lithuania"},
            {"DRIVING": "r", "NAME": "Latvia"},
            {"DRIVING": "l", "NAME": "India"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get("/datasets/gov/example/Country")
    assert listdata(resp) == [
        ("left", "India"),
        ("right", "Latvia"),
        ("right", "Lithuania"),
    ]


def test_enum_ref(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property | type   | ref  | source  | prepare | access
                         | enum   | side | l       | 'left'  | open
                         |        |      | r       | 'right' | open
                         |        |      |         |         |
    datasets/gov/example |        |      |         |         |
      | resource         | sql    |      |         |         |
      |   | Country      |        | name | COUNTRY |         |
      |   |   | name     | string |      | NAME    |         | open
      |   |   | driving  | string | side | DRIVING |         | open
    """),
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
                sa.Column("DRIVING", sa.Text),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"DRIVING": "r", "NAME": "Lithuania"},
            {"DRIVING": "r", "NAME": "Latvia"},
            {"DRIVING": "l", "NAME": "India"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get("/datasets/gov/example/Country")
    assert listdata(resp) == [
        ("left", "India"),
        ("right", "Latvia"),
        ("right", "Lithuania"),
    ]


def test_enum_no_prepare(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property     | type   | ref     | source  | prepare | access
    datasets/gov/example     |        |         |         |         |
      | resource             | sql    |         |         |         |
      |   | Country          |        | name    | COUNTRY |         |
      |   |   | name         | string |         | NAME    |         | open
      |   |   | driving      | string |         | DRIVING |         | open
                             | enum   |         | l       |         | open
                             |        |         | r       |         | open
    """),
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
                sa.Column("DRIVING", sa.Text),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"DRIVING": "r", "NAME": "Lithuania"},
            {"DRIVING": "r", "NAME": "Latvia"},
            {"DRIVING": "l", "NAME": "India"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get("/datasets/gov/example/Country")
    assert listdata(resp) == [
        ("l", "India"),
        ("r", "Latvia"),
        ("r", "Lithuania"),
    ]


def test_enum_empty_source(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property     | type   | ref     | source  | prepare       | access
    datasets/gov/example     |        |         |         |               |
      | resource             | sql    |         |         |               |
      |   | Country          |        | name    | COUNTRY |               |
      |   |   | name         | string |         | NAME    |               | open
      |   |   | driving      | string |         | DRIVING | swap('', '-') | open
                             | enum   |         | l       |               | open
                             |        |         | r       |               | open
                             |        |         | -       | null          | open
    """),
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
                sa.Column("DRIVING", sa.Text),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"DRIVING": "r", "NAME": "Lithuania"},
            {"DRIVING": "r", "NAME": "Latvia"},
            {"DRIVING": "", "NAME": "Antarctica"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get("/datasets/gov/example/Country")
    assert listdata(resp) == [
        ("r", "Latvia"),
        ("r", "Lithuania"),
        (None, "Antarctica"),
    ]


def test_enum_ref_empty_source(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property     | type   | ref     | source  | prepare       | access
                             | enum   | side    | l       |               | open
                             |        |         | r       |               | open
                             |        |         | -       | null          | open
    datasets/gov/example     |        |         |         |               |
      | resource             | sql    |         |         |               |
      |   | Country          |        | name    | COUNTRY |               |
      |   |   | name         | string |         | NAME    |               | open
      |   |   | driving      | string | side    | DRIVING | swap('', '-') | open
    """),
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
                sa.Column("DRIVING", sa.Text),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"DRIVING": "r", "NAME": "Lithuania"},
            {"DRIVING": "r", "NAME": "Latvia"},
            {"DRIVING": "", "NAME": "Antarctica"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get("/datasets/gov/example/Country")
    assert listdata(resp) == [
        ("r", "Latvia"),
        ("r", "Lithuania"),
        (None, "Antarctica"),
    ]


def test_enum_empty_integer_source(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property     | type   | ref     | source  | prepare | access
    datasets/gov/example     |        |         |         |         |
      | resource             | sql    |         |         |         |
      |   | Country          |        | name    | COUNTRY |         |
      |   |   | name         | string |         | NAME    |         | open
      |   |   | driving      | string |         | DRIVING |         | open
                             | enum   |         | 0       | 'l'     | open
                             |        |         | 1       | 'r'     | open
    """),
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
                sa.Column("DRIVING", sa.Integer),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"DRIVING": 0, "NAME": "India"},
            {"DRIVING": 1, "NAME": "Lithuania"},
            {"DRIVING": 1, "NAME": "Latvia"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get("/datasets/gov/example/Country")
    assert listdata(resp) == [
        ("l", "India"),
        ("r", "Latvia"),
        ("r", "Lithuania"),
    ]


def test_filter_by_enum_access(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property     | type   | ref     | source  | prepare | access
    datasets/gov/example     |        |         |         |         |
      | resource             | sql    |         |         |         |
      |   | Country          |        | name    | COUNTRY |         |
      |   |   | name         | string |         | NAME    |         | open
      |   |   | driving      | string |         | DRIVING |         | open
                             | enum   |         | 0       | 'l'     | private
                             |        |         | 1       | 'r'     | open
    """),
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
                sa.Column("DRIVING", sa.Integer),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"DRIVING": 0, "NAME": "India"},
            {"DRIVING": 1, "NAME": "Lithuania"},
            {"DRIVING": 1, "NAME": "Latvia"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get("/datasets/gov/example/Country")
    assert listdata(resp) == [
        ("r", "Latvia"),
        ("r", "Lithuania"),
    ]


def test_filter_by_ref_enum_access(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property     | type   | ref     | source  | prepare | access
                             | enum   | side    | 0       | 'l'     | private
                             |        |         | 1       | 'r'     | open
    datasets/gov/example     |        |         |         |         |
      | resource             | sql    |         |         |         |
      |   | Country          |        | name    | COUNTRY |         |
      |   |   | name         | string |         | NAME    |         | open
      |   |   | driving      | string | side    | DRIVING |         | open
    """),
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
                sa.Column("DRIVING", sa.Integer),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"DRIVING": 0, "NAME": "India"},
            {"DRIVING": 1, "NAME": "Lithuania"},
            {"DRIVING": 1, "NAME": "Latvia"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get("/datasets/gov/example/Country")
    assert listdata(resp) == [
        ("r", "Latvia"),
        ("r", "Lithuania"),
    ]


def test_filter_by_enum(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property     | type   | ref     | source  | prepare | access
    datasets/gov/example     |        |         |         |         |
      | resource             | sql    |         |         |         |
      |   | Country          |        | name    | COUNTRY |         |
      |   |   | name         | string |         | NAME    |         | open
      |   |   | driving      | string |         | DRIVING |         | open
                             | enum   |         | 0       | 'l'     | private
                             |        |         | 1       | 'r'     | open
    """),
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
                sa.Column("DRIVING", sa.Integer),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"DRIVING": 0, "NAME": "India"},
            {"DRIVING": 1, "NAME": "Lithuania"},
            {"DRIVING": 1, "NAME": "Latvia"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get('/datasets/gov/example/Country?driving="r"')
    assert listdata(resp) == [
        ("r", "Latvia"),
        ("r", "Lithuania"),
    ]


def test_filter_by_ref_enum(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property     | type   | ref     | source  | prepare | access
                             | enum   | side    | 0       | 'l'     | private
                             |        |         | 1       | 'r'     | open
    datasets/gov/example     |        |         |         |         |
      | resource             | sql    |         |         |         |
      |   | Country          |        | name    | COUNTRY |         |
      |   |   | name         | string |         | NAME    |         | open
      |   |   | driving      | string | side    | DRIVING |         | open
    """),
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
                sa.Column("DRIVING", sa.Integer),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"DRIVING": 0, "NAME": "India"},
            {"DRIVING": 1, "NAME": "Lithuania"},
            {"DRIVING": 1, "NAME": "Latvia"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get('/datasets/gov/example/Country?driving="r"')
    assert listdata(resp) == [
        ("r", "Latvia"),
        ("r", "Lithuania"),
    ]


def test_filter_by_enum_multi_value(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property     | type   | ref     | source  | prepare | access
    datasets/gov/example     |        |         |         |         |
      | resource             | sql    |         |         |         |
      |   | Country          |        | name    | COUNTRY |         |
      |   |   | name         | string |         | NAME    |         | open
      |   |   | driving      | string |         | DRIVING |         | open
                             | enum   |         | 0       | 'l'     |
                             |        |         | 1       | 'r'     |
                             |        |         | 2       | 'r'     |
    """),
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
                sa.Column("DRIVING", sa.Integer),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"DRIVING": 0, "NAME": "India"},
            {"DRIVING": 1, "NAME": "Lithuania"},
            {"DRIVING": 2, "NAME": "Latvia"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get('/datasets/gov/example/Country?driving="r"')
    assert listdata(resp) == [
        ("r", "Latvia"),
        ("r", "Lithuania"),
    ]


def test_filter_by_enum_list_value(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property     | type   | ref     | source  | prepare | access
    datasets/gov/example     |        |         |         |         |
      | resource             | sql    |         |         |         |
      |   | Country          |        | name    | COUNTRY |         |
      |   |   | name         | string |         | NAME    |         | open
      |   |   | driving      | string |         | DRIVING |         | open
                             | enum   |         | 0       | 'l'     |
                             |        |         | 1       | 'r'     |
    """),
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
                sa.Column("DRIVING", sa.Integer),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"DRIVING": 0, "NAME": "India"},
            {"DRIVING": 1, "NAME": "Lithuania"},
            {"DRIVING": 1, "NAME": "Latvia"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get('/datasets/gov/example/Country?driving=["l","r"]')
    assert listdata(resp) == [
        ("l", "India"),
        ("r", "Latvia"),
        ("r", "Lithuania"),
    ]


def test_implicit_filter(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property     | type   | ref     | source    | prepare          | access
    datasets/gov/example     |        |         |           |                  |
      | resource             | sql    |         |           |                  |
      |   | Country          |        | code    | COUNTRY   | continent = 'eu' |
      |   |   | code         | string |         | CODE      |                  | open
      |   |   | continent    | string |         | CONTINENT |                  | open
      |   |   | name         | string |         | NAME      |                  | open
                             |        |         |           |                  |
      |   | City             |        | name    | CITY      |                  |
      |   |   | name         | string |         | NAME      |                  | open
      |   |   | country      | ref    | Country | COUNTRY   |                  | open
    """),
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("CODE", sa.String),
                sa.Column("CONTINENT", sa.String),
                sa.Column("NAME", sa.Text),
            ],
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY", sa.String),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"CODE": "in", "CONTINENT": "as", "NAME": "India"},
            {"CODE": "lt", "CONTINENT": "eu", "NAME": "Lithuania"},
            {"CODE": "lv", "CONTINENT": "eu", "NAME": "Latvia"},
        ],
    )

    sqlite.write(
        "CITY",
        [
            {"COUNTRY": "in", "NAME": "Mumbai"},
            {"COUNTRY": "in", "NAME": "Delhi"},
            {"COUNTRY": "lt", "NAME": "Vilnius"},
            {"COUNTRY": "lv", "NAME": "Ryga"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get("/datasets/gov/example/City")
    assert listdata(resp, "name") == [
        "Ryga",
        "Vilnius",
    ]


def test_implicit_filter_no_external_source(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property     | type   | ref     | source    | prepare     | access
    datasets/gov/example     |        |         |           |             |
      | resource             | sql    |         |           |             |
      |   | Country          |        | code    | COUNTRY   | code = 'lt' |
      |   |   | code         | string |         | CODE      |             | open
      |   |   | name         | string |         | NAME      |             | open
                             |        |         |           |             |
      |   | City             |        | name    | CITY      |             |
      |   |   | name         | string |         | NAME      |             | open
      |   |   | country      | ref    | Country |           |             | open
    """),
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("CODE", sa.String),
                sa.Column("NAME", sa.Text),
            ],
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY", sa.String),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"CODE": "lt", "NAME": "Lithuania"},
            {"CODE": "lv", "NAME": "Latvia"},
        ],
    )

    sqlite.write(
        "CITY",
        [
            {"COUNTRY": "lt", "NAME": "Vilnius"},
            {"COUNTRY": "lv", "NAME": "Ryga"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get("/datasets/gov/example/City")
    assert listdata(resp, "name") == [
        "Ryga",
        "Vilnius",
    ]


def test_implicit_filter_two_refs(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property     | type    | ref                | source             | prepare | access
    example/standards            |         |                    |                    |         |
      | sql                      | sql     |                    | sqlite://          |         |
                                 |         |                    |                    |         |
      |   |   | StandardDocument |         | standard, document | STANDARD_DOCUMENTS |         |
      |   |   |   | standard     | ref     | Standard           | STANDARD_ID        |         | open
      |   |   |   | document     | ref     | Document           | DOCUMENT_ID        |         | open
                                 |         |                    |                    |         |
      |   |   | Standard         |         | id                 | STANDARD           |         |
      |   |   |   | id           | integer |                    | STANDARD_ID        |         | private
      |   |   |   | name         | string  |                    | STANDARD_NAME      |         | open
                                 |         |                    |                    |         |
      |   |   | Document         |         | id                 | DOCUMENT           | id=2    |
      |   |   |   | id           | integer |                    | DOCUMENT_ID        |         | private
      |   |   |   | name         | string  |                    | NAME               |         | open
    """),
    )

    sqlite.init(
        {
            "STANDARD": [
                sa.Column("STANDARD_ID", sa.Integer),
                sa.Column("STANDARD_NAME", sa.Text),
            ],
            "DOCUMENT": [
                sa.Column("DOCUMENT_ID", sa.Integer),
                sa.Column("NAME", sa.Text),
            ],
            "STANDARD_DOCUMENTS": [
                sa.Column("STANDARD_ID", sa.Integer, sa.ForeignKey("STANDARD.STANDARD_ID")),
                sa.Column("DOCUMENT_ID", sa.Integer, sa.ForeignKey("DOCUMENT.DOCUMENT_ID")),
            ],
        }
    )

    sqlite.write(
        "STANDARD",
        [
            {"STANDARD_ID": 1, "STANDARD_NAME": "S1"},
            {"STANDARD_ID": 2, "STANDARD_NAME": "S2"},
        ],
    )

    sqlite.write(
        "DOCUMENT",
        [
            {"DOCUMENT_ID": 1, "NAME": "DOC1"},
            {"DOCUMENT_ID": 2, "NAME": "DOC2"},
        ],
    )

    sqlite.write(
        "STANDARD_DOCUMENTS",
        [
            {"STANDARD_ID": 1, "DOCUMENT_ID": 1},
            {"STANDARD_ID": 2, "DOCUMENT_ID": 2},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)

    resp = app.get("/example/standards/StandardDocument?select(standard.name, document.name)")
    assert listdata(resp, "standard.name", "document.name") == [
        ("S2", "DOC2"),
    ]


def test_implicit_filter_by_enum(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property     | type   | ref     | source  | prepare | access
    datasets/gov/example     |        |         |         |         |
      | resource             | sql    |         |         |         |
      |   | Country          |        | id      | COUNTRY |         |
      |   |   | id           | string |         | ID      |         | private
      |   |   | name         | string |         | NAME    |         | open
      |   |   | driving      | string |         | DRIVING |         | open
                             | enum   |         | 0       | 'l'     | protected
                             |        |         | 1       | 'r'     |
                             |        |         |         |         |
      |   | City             |        | name    | CITY    |         |
      |   |   | name         | string |         | NAME    |         | open
      |   |   | country      | ref    | Country | COUNTRY |         | open
    """),
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("ID", sa.Integer),
                sa.Column("NAME", sa.Text),
                sa.Column("DRIVING", sa.Integer),
            ],
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY", sa.Integer),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"ID": 1, "DRIVING": 0, "NAME": "India"},
            {"ID": 2, "DRIVING": 1, "NAME": "Lithuania"},
            {"ID": 3, "DRIVING": 1, "NAME": "Latvia"},
        ],
    )

    sqlite.write(
        "CITY",
        [
            {"COUNTRY": 1, "NAME": "Mumbai"},
            {"COUNTRY": 1, "NAME": "Delhi"},
            {"COUNTRY": 2, "NAME": "Vilnius"},
            {"COUNTRY": 3, "NAME": "Ryga"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get("/datasets/gov/example/City")
    assert listdata(resp, "name") == [
        "Ryga",
        "Vilnius",
    ]


def test_implicit_filter_by_enum_empty_access(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property     | type   | ref     | source    | prepare          | access
    datasets/gov/example     |        |         |           |                  |
                             | enum   | Side    | 0         | 'l'              |
                             |        |         | 1         | 'r'              |
                             |        |         |           |                  |
      | resource             | sql    |         |           |                  |
      |   | Country          |        | id      | COUNTRY   | continent = "eu" |
      |   |   | id           | string |         | ID        |                  | private
      |   |   | name         | string |         | NAME      |                  | open
      |   |   | continent    | string |         | CONTINENT |                  | open
      |   |   | driving      | string | Side    | DRIVING   |                  | open
                             |        |         |           |                  |
      |   | City             |        | name    | CITY      |                  |
      |   |   | name         | string |         | NAME      |                  | open
      |   |   | country      | ref    | Country | COUNTRY   |                  | open
    """),
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("ID", sa.Integer),
                sa.Column("NAME", sa.Text),
                sa.Column("CONTINENT", sa.Text),
                sa.Column("DRIVING", sa.Integer),
            ],
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY", sa.Integer),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"ID": 1, "CONTINENT": "az", "DRIVING": 0, "NAME": "India"},
            {"ID": 2, "CONTINENT": "eu", "DRIVING": 1, "NAME": "Lithuania"},
            {"ID": 3, "CONTINENT": "eu", "DRIVING": 1, "NAME": "Latvia"},
        ],
    )

    sqlite.write(
        "CITY",
        [
            {"COUNTRY": 1, "NAME": "Mumbai"},
            {"COUNTRY": 1, "NAME": "Delhi"},
            {"COUNTRY": 2, "NAME": "Vilnius"},
            {"COUNTRY": 3, "NAME": "Ryga"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get("/datasets/gov/example/City")
    assert listdata(resp, "name") == [
        "Ryga",
        "Vilnius",
    ]


def test_file(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        """
    d | r | m | property  | type   | ref | source    | prepare                                   | access
    datasets/gov/example  |        |     |           |                                           |
      | resource          | sql    |     |           |                                           |
      |   | Country       |        | id  | COUNTRY   |                                           |
      |   |   | id        | string |     | ID        |                                           | private
      |   |   | name      | string |     | NAME      |                                           | open
      |   |   | flag_name | string |     | FLAG_FILE |                                           | private
      |   |   | flag_data | binary |     | FLAG_DATA |                                           | private
      |   |   | flag      | file   |     |           | file(name: flag_name, content: flag_data) | open
    """,
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("ID", sa.Integer),
                sa.Column("NAME", sa.Text),
                sa.Column("FLAG_FILE", sa.Text),
                sa.Column("FLAG_DATA", sa.LargeBinary),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {
                "ID": 2,
                "NAME": "Lithuania",
                "FLAG_FILE": "lt.png",
                "FLAG_DATA": b"DATA",
            },
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get("/datasets/gov/example/Country")
    assert listdata(resp, full=True) == [
        {
            "name": "Lithuania",
            "flag._id": "lt.png",
            "flag._content_type": None,  # FIXME: Should be 'image/png'.
        },
    ]


def test_push_file(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    sqlite: Sqlite,
    request,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest_push.csv",
        """
    d | r | m | property   | type   | ref | source    | prepare                                   | access
    datasets/gov/push/file |        |     |           |                                           |
      | resource           | sql    | sql |           |                                           |
      |   | Country        |        | id  | COUNTRY   |                                           |
      |   |   | id         | string |     | ID        |                                           | private
      |   |   | name       | string |     | NAME      |                                           | open
      |   |   | flag_name  | string |     | FLAG_FILE |                                           | private
      |   |   | flag_data  | binary |     | FLAG_DATA |                                           | private
      |   |   | flag       | file   |     |           | file(name: flag_name, content: flag_data) | open
    """,
    )

    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        """
    d | r | m | property   | type   | ref | source | prepare | access
    datasets/gov/push/file |        |     |        |         |
      |   | Country        |        | id  |        |         |
      |   |   | id         | string |     |        |         | private
      |   |   | name       | string |     |        |         | open
      |   |   | flag_name  | string |     |        |         | private
      |   |   | flag_data  | binary |     |        |         | private
      |   |   | flag       | file   |     |        |         | open
    """,
    )

    # Configure local server with SQL backend
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("ID", sa.Integer),
                sa.Column("NAME", sa.Text),
                sa.Column("FLAG_FILE", sa.Text),
                sa.Column("FLAG_DATA", sa.LargeBinary),
            ],
        }
    )
    sqlite.write(
        "COUNTRY",
        [
            {
                "ID": 2,
                "NAME": "Lithuania",
                "FLAG_FILE": "lt.png",
                "FLAG_DATA": b"DATA",
            },
        ],
    )
    local_rc = create_rc(rc, tmp_path, sqlite)

    # Configure remote server
    remote = configure_remote_server(cli, local_rc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data to the remote server
    cli.invoke(
        local_rc,
        [
            "push",
            str(tmp_path / "manifest_push.csv"),
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
        ],
    )

    remote.app.authmodel("datasets/gov/push/file/Country", ["getall", "getone"])
    resp = remote.app.get("/datasets/gov/push/file/Country")
    assert listdata(resp, full=True) == [
        {
            "name": "Lithuania",
            "flag._id": "lt.png",
            "flag._content_type": None,  # FIXME: Should be 'image/png'.
        },
    ]
    _id = resp.json()["_data"][0]["_id"]
    resp = remote.app.get(f"/datasets/gov/push/file/Country/{_id}/flag")
    assert resp.status_code == 200
    assert resp.content == b"DATA"


def test_image(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        """
    d | r | m | property  | type   | ref | source    | prepare                                   | access
    datasets/gov/example  |        |     |           |                                           |
      | resource          | sql    |     |           |                                           |
      |   | Country       |        | id  | COUNTRY   |                                           |
      |   |   | id        | string |     | ID        |                                           | private
      |   |   | name      | string |     | NAME      |                                           | open
      |   |   | flag_name | string |     | FLAG_FILE |                                           | private
      |   |   | flag_data | binary |     | FLAG_DATA |                                           | private
      |   |   | flag      | image  |     |           | file(name: flag_name, content: flag_data) | open
    """,
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("ID", sa.Integer),
                sa.Column("NAME", sa.Text),
                sa.Column("FLAG_FILE", sa.Text),
                sa.Column("FLAG_DATA", sa.LargeBinary),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {
                "ID": 2,
                "NAME": "Lithuania",
                "FLAG_FILE": "lt.png",
                "FLAG_DATA": b"DATA",
            },
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get("/datasets/gov/example/Country")
    assert listdata(resp, full=True) == [
        {
            "name": "Lithuania",
            "flag._id": "lt.png",
            "flag._content_type": None,  # FIXME: Should be 'image/png'.
        },
    ]


def test_image_file(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    sqlite: Sqlite,
    request,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest_push.csv",
        """
    d | r | m | property   | type   | ref | source    | prepare                                   | access
    datasets/gov/push/file |        |     |           |                                           |
      | resource           | sql    | sql |           |                                           |
      |   | Country        |        | id  | COUNTRY   |                                           |
      |   |   | id         | string |     | ID        |                                           | private
      |   |   | name       | string |     | NAME      |                                           | open
      |   |   | flag_name  | string |     | FLAG_FILE |                                           | private
      |   |   | flag_data  | binary |     | FLAG_DATA |                                           | private
      |   |   | flag       | image  |     |           | file(name: flag_name, content: flag_data) | open
    """,
    )

    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        """
    d | r | m | property   | type   | ref | source | prepare | access
    datasets/gov/push/file |        |     |        |         |
      |   | Country        |        | id  |        |         |
      |   |   | id         | string |     |        |         | private
      |   |   | name       | string |     |        |         | open
      |   |   | flag_name  | string |     |        |         | private
      |   |   | flag_data  | binary |     |        |         | private
      |   |   | flag       | image  |     |        |         | open
    """,
    )

    # Configure local server with SQL backend
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("ID", sa.Integer),
                sa.Column("NAME", sa.Text),
                sa.Column("FLAG_FILE", sa.Text),
                sa.Column("FLAG_DATA", sa.LargeBinary),
            ],
        }
    )
    sqlite.write(
        "COUNTRY",
        [
            {
                "ID": 2,
                "NAME": "Lithuania",
                "FLAG_FILE": "lt.png",
                "FLAG_DATA": b"DATA",
            },
        ],
    )
    local_rc = create_rc(rc, tmp_path, sqlite)

    # Configure remote server
    remote = configure_remote_server(cli, local_rc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data to the remote server
    cli.invoke(
        local_rc,
        [
            "push",
            str(tmp_path / "manifest_push.csv"),
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
        ],
    )

    remote.app.authmodel("datasets/gov/push/file/Country", ["getall", "getone"])
    resp = remote.app.get("/datasets/gov/push/file/Country")
    assert listdata(resp, full=True) == [
        {
            "name": "Lithuania",
            "flag._id": "lt.png",
            "flag._content_type": None,  # FIXME: Should be 'image/png'.
        },
    ]
    _id = resp.json()["_data"][0]["_id"]
    resp = remote.app.get(f"/datasets/gov/push/file/Country/{_id}/flag")
    assert resp.status_code == 200
    assert resp.content == b"DATA"


def test_push_null_foreign_key(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    sqlite: Sqlite,
    request,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        """
    d | r | b | m | property      | type     | ref          | source        | access
    example/null/fk               |          |              |               |
      | resource                  | sql      | sql          |               |
      |   |   | Country           |          | id           | COUNTRY       |
      |   |   |   | id            | integer  |              | ID            | private
      |   |   |   | name          | string   |              | NAME          | open
      |   |   | City              |          | id           | CITY          |
      |   |   |   | id            | integer  |              | ID            | private
      |   |   |   | name          | string   |              | NAME          | open
      |   |   |   | country       | ref      | Country      | COUNTRY       | open
      |   |   |   | embassy       | ref      | Country      | EMBASSY       | open
    """,
    )

    # Configure local server with SQL backend
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("ID", sa.Integer),
                sa.Column("NAME", sa.String),
            ],
            "CITY": [
                sa.Column("ID", sa.Integer),
                sa.Column("NAME", sa.String),
                sa.Column("COUNTRY", sa.Integer, sa.ForeignKey("COUNTRY.ID")),
                sa.Column("EMBASSY", sa.Integer, sa.ForeignKey("COUNTRY.ID")),
            ],
        }
    )
    sqlite.write(
        "COUNTRY",
        [
            {
                "ID": 1,
                "NAME": "Latvia",
            },
            {
                "ID": 2,
                "NAME": "Lithuania",
            },
        ],
    )
    sqlite.write(
        "CITY",
        [
            {
                "ID": 1,
                "NAME": "Ryga",
                "COUNTRY": 1,
                "EMBASSY": None,
            },
            {
                "ID": 2,
                "NAME": "Vilnius",
                "COUNTRY": 2,
                "EMBASSY": 1,
            },
            {
                "ID": 3,
                "NAME": "Winterfell",
                "COUNTRY": None,
                "EMBASSY": None,
            },
        ],
    )
    local_rc = create_rc(rc, tmp_path, sqlite)

    # Configure remote server
    remote = configure_remote_server(cli, local_rc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    cli.invoke(
        local_rc,
        [
            "push",
            "-o",
            "spinta+" + remote.url,
            "--credentials",
            remote.credsfile,
        ],
    )

    remote.app.authmodel("example/null/fk", ["getall"])

    resp = remote.app.get("/example/null/fk/Country")
    countries = dict(listdata(resp, "name", "_id"))

    resp = remote.app.get("/example/null/fk/City")
    assert listdata(resp, full=True, sort="name") == [
        {
            "name": "Ryga",
            "country._id": countries["Latvia"],
            "embassy": None,
        },
        {
            "name": "Vilnius",
            "country._id": countries["Lithuania"],
            "embassy._id": countries["Latvia"],
        },
        {
            "name": "Winterfell",
            "country": None,
            "embassy": None,
        },
    ]


def test_push_self_ref(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    sqlite: Sqlite,
    request,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        """
    d | r | b | m | property      | type     | ref          | source        | access
    example/self/ref              |          |              |               |
      | resource                  | sql      | sql          |               |
      |   |   | City              |          | id           | CITY          |
      |   |   |   | id            | integer  |              | ID            | private
      |   |   |   | name          | string   |              | NAME          | open
      |   |   |   | governance    | ref      | City         | GOVERNANCE    | open
    """,
    )

    # Configure local server with SQL backend
    sqlite.init(
        {
            "CITY": [
                sa.Column("ID", sa.Integer),
                sa.Column("NAME", sa.String),
                sa.Column("GOVERNANCE", sa.Integer, sa.ForeignKey("CITY.ID")),
            ],
        }
    )
    sqlite.write(
        "CITY",
        [
            {
                "ID": 1,
                "NAME": "Vilnius",
                "GOVERNANCE": None,
            },
            {
                "ID": 2,
                "NAME": "Trakai",
                "GOVERNANCE": 1,
            },
        ],
    )
    local_rc = create_rc(rc, tmp_path, sqlite)

    # Configure remote server
    remote = configure_remote_server(cli, local_rc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    cli.invoke(
        local_rc,
        [
            "push",
            "-o",
            "spinta+" + remote.url,
            "--credentials",
            remote.credsfile,
        ],
    )

    remote.app.authmodel("example/self/ref", ["getall"])

    resp = remote.app.get("/example/self/ref/City")
    cities = dict(listdata(resp, "name", "_id"))
    assert listdata(resp, full=True) == [
        {
            "name": "Vilnius",
            "governance": None,
        },
        {
            "name": "Trakai",
            "governance._id": cities["Vilnius"],
        },
    ]


def _prep_error_handling(
    context,
    tmp_path: pathlib.Path,
    sqlite: Sqlite,
    rc: RawConfig,
    responses: RequestsMock,
    *,
    response: Tuple[int, Dict[str, str], str] = None,
    exception: Exception = None,
) -> RawConfig:
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        """
    d | r | b | m | property      | type     | ref          | source        | access
    example/errors                |          |              |               |
      | resource                  | sql      | sql          |               |
      |   |   | City              |          | id           | CITY          |
      |   |   |   | id            | integer  |              | ID            | private
      |   |   |   | name          | string   |              | NAME          | open
    """,
    )

    # Configure local server with SQL backend
    sqlite.init(
        {
            "CITY": [
                sa.Column("ID", sa.Integer),
                sa.Column("NAME", sa.String),
            ],
        }
    )
    sqlite.write(
        "CITY",
        [
            {"ID": 1, "NAME": "Vilnius"},
            {"ID": 2, "NAME": "Kaunas"},
        ],
    )
    rc = create_rc(rc, tmp_path, sqlite)

    # Configure remote server
    def handler(request: PreparedRequest):
        if request.url.endswith("/auth/token"):
            return (
                200,
                {"content-type": "application/json"},
                '{"access_token":"TOKEN"}',
            )
        elif exception:
            raise exception
        else:
            return response

    responses.add_callback(
        POST,
        re.compile(r"https://example.com/.*"),
        callback=handler,
        content_type="application/json",
    )

    add_client_credentials(
        tmp_path / "credentials.cfg",
        "https://example.com",
        client="test",
        secret="secret",
        scopes=["spinta_insert"],
    )

    return rc


def test_error_handling_server_error(
    context,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses: RequestsMock,
    tmp_path: pathlib.Path,
    sqlite: Sqlite,
    caplog: LogCaptureFixture,
):
    rc = _prep_error_handling(
        context,
        tmp_path,
        sqlite,
        rc,
        responses,
        response=(
            400,
            {"content-type": "application/json"},
            '{"errors":[{"type": "system", "message": "ERROR"}]}',
        ),
    )

    # Push data from local to remote.
    with caplog.at_level(logging.ERROR):
        cli.invoke(
            rc,
            [
                "push",
                "-o",
                "spinta+https://example.com",
                "--credentials",
                tmp_path / "credentials.cfg",
            ],
            fail=False,
        )

    message = (
        "Error when sending and receiving data. Model example/errors/City, items in chunk: 2, first item in chunk:"
    )
    assert message in caplog.text
    assert "Server response (status=400):" in caplog.text


def test_error_handling_io_error(
    context,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses: RequestsMock,
    tmp_path,
    sqlite: Sqlite,
    caplog: LogCaptureFixture,
):
    rc = _prep_error_handling(
        context,
        tmp_path,
        sqlite,
        rc,
        responses,
        exception=IOError("I/O error."),
    )

    # Push data from local to remote.
    with caplog.at_level(logging.ERROR):
        cli.invoke(
            rc,
            [
                "push",
                "-o",
                "spinta+https://example.com",
                "--credentials",
                tmp_path / "credentials.cfg",
            ],
            fail=False,
        )

    message = (
        "Error when sending and receiving data. Model example/errors/City, items in chunk: 2, first item in chunk:"
    )
    assert message in caplog.text
    assert "Error: I/O error." in caplog.text


def test_sql_views(context, rc: RawConfig, tmp_path: pathlib.Path, sqlite: Sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        """
    d | r | b | m | property      | type     | ref          | source        | access
    example/views                 |          |              |               |
      | resource                  | sql      | sql          |               |
      |   |   | City              |          | id           | CITY_VIEW     |
      |   |   |   | id            | integer  |              | ID            | private
      |   |   |   | name          | string   |              | NAME          | open
    """,
    )

    # Configure local server with SQL backend
    sqlite.init(
        {
            "CITY": [
                sa.Column("ID", sa.Integer),
                sa.Column("NAME", sa.String),
            ],
        }
    )
    sqlite.write(
        "CITY",
        [
            {"ID": 1, "NAME": "Vilnius"},
            {"ID": 2, "NAME": "Kaunas"},
        ],
    )
    sqlite.engine.execute("CREATE VIEW CITY_VIEW AS SELECT * FROM CITY")

    app = create_client(rc, tmp_path, sqlite)

    resp = app.get("/example/views/City")
    assert listdata(resp) == ["Kaunas", "Vilnius"]


@pytest.mark.skip("TODO")
def test_params(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    sqlite: Sqlite,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        """
    d | r | b | m | property | type    | ref      | source   | prepare
    example/self/ref/param   |         |          |          |
      | resource             | sql     | sql      |          |
      |   |   | Category     |         | id       | CATEGORY | parent = param(parent)
      |   |   |   |          | param   | parent   |          | null
      |   |   |   |          |         |          | Category | select(id).id
      |   |   |   | id       | integer |          | ID       |
      |   |   |   | name     | string  |          | NAME     |
      |   |   |   | parent   | ref     | Category | PARENT   |
    """,
    )

    # Configure local server with SQL backend
    sqlite.init(
        {
            "CATEGORY": [
                sa.Column("ID", sa.Integer),
                sa.Column("NAME", sa.String),
                sa.Column("PARENT", sa.Integer, sa.ForeignKey("CATEGORY.ID")),
            ],
        }
    )
    sqlite.write(
        "CATEGORY",
        [
            {
                "ID": 1,
                "NAME": "Cat 1",
                "PARENT": None,
            },
            {
                "ID": 2,
                "NAME": "Cat 1.1",
                "PARENT": 1,
            },
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel("example/self/ref/param", ["search"])

    resp = app.get("/example/self/ref/param/Category?select(name)")
    assert listdata(resp, sort=False) == ["Cat 1", "Cat 1.1"]


def test_cast_string(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    sqlite: Sqlite,
):
    dataset = "example/func/cast/string"
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        f"""
    d | r | b | m | property  | type    | ref      | source   | prepare
    {dataset}                 |         |          |          |
      | resource              | sql     | sql      |          |
      |   |   | Data          |         | id       | DATA     |
      |   |   |   | id        | string  |          | ID       | cast()
    """,
    )

    # Configure local server with SQL backend
    sqlite.init(
        {
            "DATA": [
                sa.Column("ID", sa.Integer),
            ],
        }
    )
    sqlite.write("DATA", [{"ID": 1}])

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel(dataset, ["getall"])

    resp = app.get(f"/{dataset}/Data")
    assert listdata(resp) == ["1"]


@pytest.mark.skip("todo")
def test_type_text_push(context, postgresql, rc, cli: SpintaCliRunner, responses, tmpdir, geodb, request):
    create_tabular_manifest(
        context,
        tmpdir / "manifest.csv",
        striptable("""
        d | r | b | m | property| type   | ref     | source       | access
        datasets/gov/example/text_push    |        |         |              |
          | data                | sql    |         |              |
          |   |                 |        |         |              |
          |   |   | Country     |        | code    | salis        |
          |   |   |   | code    | string |         | kodas        | open
          |   |   |   | name@lt | string |         | pavadinimas  | open
          |   |                 |        |         |              |
          |   |   | City        |        | name    | miestas      |
          |   |   |   | name    | string |         | pavadinimas  | open
          |   |   |   | country | ref    | Country | salis        | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmpdir, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmpdir, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "datasets/gov/example/text_push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--dry-run",
        ],
    )
    assert result.exit_code == 0

    remote.app.authmodel("datasets/gov/example/text_push/Country", ["getall"])
    resp = remote.app.get("/datasets/gov/example/text_push/Country")
    assert listdata(resp, "code", "name") == []


def test_text_type_push_chunks(
    context,
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | source      | type   | ref     | access
    datasets/gov/example/text_chunks     |             |        |         |
      | data                 |             | sql    |         |
      |   |                  |             |        |         |
      |   |   | Country      | salis       |        | code    |
      |   |   |   | code     | kodas       | string |         | open
      |   |   |   | name@lt  | pavadinimas | string |         | open
      |   |   |   | name@en  | pavadinimas | string |         | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "datasets/gov/example/text_chunks",
            "-o",
            "spinta+" + remote.url,
            "--credentials",
            remote.credsfile,
            "--chunk-size=1",
        ],
    )

    cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "datasets/gov/example/text_chunks",
            "-o",
            "spinta+" + remote.url,
            "--credentials",
            remote.credsfile,
            "--chunk-size=1",
        ],
    )

    remote.app.authmodel("datasets/gov/example/text_chunks/Country", ["getall"])
    resp = remote.app.get("/datasets/gov/example/text_chunks/Country")
    assert listdata(resp, "code", "name") == [("ee", "Estija"), ("lt", "Lietuva"), ("lv", "Latvija")]


def test_text_type_push_state(context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | source      | type   | ref     | access
    datasets/gov/example/text     |             |        |         |
      | data                 |             | sql    |         |
      |   |                  |             |        |         |
      |   |   | Country      | salis       |        | code    |
      |   |   |   | code     | kodas       | string |         | open
      |   |   |   | name@lt  | pavadinimas | string |         | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push one row, save state and stop.
    cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "datasets/gov/example/text",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--chunk-size",
            "1k",
            "--stop-time",
            "1h",
            "--stop-row",
            "1",
            "--state",
            tmp_path / "state.db",
        ],
    )

    remote.app.authmodel("/datasets/gov/example/text/Country", ["getall"])
    resp = remote.app.get("/datasets/gov/example/text/Country")
    assert len(listdata(resp)) == 1

    cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "datasets/gov/example/text",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--stop-row",
            "1",
            "--state",
            tmp_path / "state.db",
        ],
    )

    resp = remote.app.get("/datasets/gov/example/text/Country")
    assert len(listdata(resp)) == 2


def test_cast_integer(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    sqlite: Sqlite,
):
    dataset = "example/func/cast/integer"
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        f"""
    d | r | b | m | property  | type    | ref      | source   | prepare
    {dataset}                 |         |          |          |
      | resource              | sql     | sql      |          |
      |   |   | Data          |         | id       | DATA     |
      |   |   |   | id        | integer |          | ID       | cast()
    """,
    )

    # Configure local server with SQL backend
    sqlite.init(
        {
            "DATA": [
                sa.Column("ID", sa.Float),
            ],
        }
    )
    sqlite.write("DATA", [{"ID": 1.0}])

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel(dataset, ["getall"])

    resp = app.get(f"/{dataset}/Data")
    assert listdata(resp) == [1]


def test_cast_integer_error(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    sqlite: Sqlite,
):
    dataset = "example/func/cast/integer/error"
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        f"""
    d | r | b | m | property  | type    | ref      | source   | prepare
    {dataset}                 |         |          |          |
      | resource              | sql     | sql      |          |
      |   |   | Data          |         | id       | DATA     |
      |   |   |   | id        | integer |          | ID       | cast()
    """,
    )

    # Configure local server with SQL backend
    sqlite.init(
        {
            "DATA": [
                sa.Column("ID", sa.Float),
            ],
        }
    )
    sqlite.write("DATA", [{"ID": 1.1}])

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel(dataset, ["getall"])

    resp = app.get(f"/{dataset}/Data")
    assert error(resp) == "UnableToCast"


def test_point(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    sqlite: Sqlite,
):
    dataset = "example/func/point"
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        f"""
    d | r | b | m | property | type     | ref | source | prepare     | access
    {dataset}                |          |     |        |             |
      | resource             | sql      | sql |        |             |
      |   |   | Data         |          | id  | data   |             |
      |   |   |   | id       | integer  |     | id     |             | open
      |   |   |   | x        | number   |     | x      |             | private
      |   |   |   | y        | number   |     | y      |             | private
      |   |   |   | point    | geometry |     |        | point(x, y) | open
    """,
    )

    # Configure local server with SQL backend
    sqlite.init(
        {
            "data": [
                sa.Column("id", sa.Integer),
                sa.Column("x", sa.Float),
                sa.Column("y", sa.Float),
            ],
        }
    )
    sqlite.write(
        "data",
        [
            {
                "id": 1,
                "x": 4.5,
                "y": 2.5,
            }
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel(dataset, ["getall"])

    resp = app.get(f"/{dataset}/Data")
    assert listdata(resp) == [(1, "POINT (4.5 2.5)")]


def test_swap_single(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | source      | type   | ref     | access | prepare
       | datasets/gov/example     |             |        |         |        |
       |   | data                 |             | sql    |         |        |
       |   |   |                  |             |        |         |        |
       |   |   |   | Country      | salis       |        | code    | open   |
       |   |   |   |   | code     | kodas       | string |         |        | swap('lt', 'LT')
       |   |   |   |   | name     | pavadinimas | string |         |        |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Country")
    assert listdata(resp, "code", "name") == [("LT", "Lietuva"), ("ee", "Estija"), ("lv", "Latvija")]


def test_swap_multi_with_dot(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | source      | type   | ref     | access | prepare
       | datasets/gov/example     |             |        |         |        |
       |   | data                 |             | sql    |         |        |
       |   |   |                  |             |        |         |        |
       |   |   |   | Country      | salis       |        | code    | open   |
       |   |   |   |   | code     | kodas       | string |         |        | swap('lt', 'LT').swap('lv', 'LV')
       |   |   |   |   | name     | pavadinimas | string |         |        |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Country")
    assert listdata(resp, "code", "name") == [("LT", "Lietuva"), ("LV", "Latvija"), ("ee", "Estija")]


def test_swap_multi_with_multi_lines(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | source      | type   | ref     | access | prepare
       | datasets/gov/example     |             |        |         |        |
       |   | data                 |             | sql    |         |        |
       |   |   |                  |             |        |         |        |
       |   |   |   | Country      | salis       |        | code    | open   |
       |   |   |   |   | code     | kodas       | string |         |        | swap('lt', 'LT')
       |   |   |   |   |          | lv        |        |         |        | swap('LV')
       |   |   |   |   | name     | pavadinimas | string |         |        |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Country")
    assert listdata(resp, "code", "name") == [("LT", "Lietuva"), ("LV", "Latvija"), ("ee", "Estija")]


def test_swap_multi_with_multi_lines_all_to_same(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | source      | type   | ref     | access | prepare
       | datasets/gov/example     |             |        |         |        |
       |   | data                 |             | sql    |         |        |
       |   |   |                  |             |        |         |        |
       |   |   |   | Country      | salis       |        | code    | open   |
       |   |   |   |   | code     | kodas       | string |         |        | swap('lt', 'CODE')
       |   |   |   |   |          | lv          |        |         |        | swap('CODE')
       |   |   |   |   |          |             |        |         |        | swap('ee', 'CODE')
       |   |   |   |   | name     | pavadinimas | string |         |        |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Country")
    assert listdata(resp, "code", "name") == [("CODE", "Estija"), ("CODE", "Latvija"), ("CODE", "Lietuva")]


def test_swap_multi_escape_source(context, rc, tmp_path, geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property | source          | type    | ref     | access | prepare
       | datasets/gov/example     |                 |         |         |        |
       |   | data                 |                 | sql     |         |        |
       |   |   |                  |                 |         |         |        |
       |   |   |   | Test         | test            |         | id      | open   |
       |   |   |   |   | id       | id              | integer |         |        |
       |   |   |   |   | text     | text            | string  |         |        | swap("\\"TEST\\"", "NORMAL SWAPPED PREPARE")
       |   |   |   |   |          | 'TEST'          |         |         |        | swap("TESTAS")
       |   |   |   |   |          | test 'TEST'     |         |         |        | swap("TEST 'test'")
       |   |   |   |   |          |                 |         |         |        | swap("test \\"TEST\\"", "TEST \\"test\\"")
       |   |   |   |   |          | "TEST" 'TEST'   |         |         |        | swap("'TEST' \\"TEST\\"")
       |   |   |   |   | old      | text            | string  |         |        |
    """),
    )

    app = create_client(rc, tmp_path, geodb)

    resp = app.get("/datasets/gov/example/Test")
    assert listdata(resp, "old", "text") == [
        ("'TEST'", "TESTAS"),
        ("test 'TEST'", "TEST 'test'"),
        ("\"TEST\" 'TEST'", "'TEST' \"TEST\""),
        ('"TEST"', "NORMAL SWAPPED PREPARE"),
        ('test "TEST"', 'TEST "test"'),
    ]


def test_advanced_denorm(context, rc, tmp_path, geodb_denorm):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
        d | r | m | property            | type    | ref     | source       | prepare | access
    datasets/denorm                 |         |         |              |         |
      | rs                          | sql     |         |              |         |
      |   | Planet                  |         | code    | PLANET       |         | open
      |   |   | code                | string  |         | code         |         |
      |   |   | name                | string  |         | name         |         |
      |   | Country                 |         | code    | COUNTRY      |         | open
      |   |   | code                | string  |         | code         |         |
      |   |   | name                | string  |         | name         |         |
      |   |   | planet              | ref     | Planet  | planet       |         |
      |   |   | planet.name         |         |         |              |         |
      |   | City                    |         | code    | CITY         |         | open
      |   |   | code                | string  |         | code         |         |
      |   |   | name                | string  |         | name         |         |
      |   |   | country             | ref     | Country | country      |         |
      |   |   | country.code        |         |         |              |         |
      |   |   | country.name        | string  |         | countryName  |         |
      |   |   | country.year        | integer |         | countryYear  |         |
      |   |   | country.planet.name |         |         |              |         |
      |   |   | country.planet.code | string  |         |              | 'ER'    |
      """),
    )

    app = create_client(rc, tmp_path, geodb_denorm)

    resp = app.get("/datasets/denorm/Planet")
    assert listdata(resp, sort="code", full=True) == [
        {"code": "ER", "name": "Earth"},
        {"code": "JP", "name": "Jupyter"},
        {"code": "MR", "name": "Mars"},
    ]

    resp = app.get("/datasets/denorm/Country")
    assert listdata(resp, "code", "name", "planet.name", sort="code", full=True) == [
        {"code": "EE", "name": "Estonia", "planet.name": "Jupyter"},
        {"code": "LT", "name": "Lithuania", "planet.name": "Earth"},
        {"code": "LV", "name": "Latvia", "planet.name": "Mars"},
    ]

    resp = app.get("/datasets/denorm/City")
    assert listdata(
        resp,
        "code",
        "name",
        "country.name",
        "country.code",
        "country.year",
        "country.planet.name",
        "country.planet.code",
        sort="code",
        full=True,
    ) == [
        {
            "code": "RYG",
            "name": "Ryga",
            "country.name": "Latvia",
            "country.code": "LV",
            "country.year": 1408,
            "country.planet.name": "Earth",
            "country.planet.code": "ER",
        },
        {
            "code": "TLN",
            "name": "Talin",
            "country.name": "Estija",
            "country.code": "EE",
            "country.year": 1784,
            "country.planet.name": "Earth",
            "country.planet.code": "ER",
        },
        {
            "code": "VLN",
            "name": "Vilnius",
            "country.name": "Lietuva",
            "country.code": "LT",
            "country.year": 1204,
            "country.planet.name": "Earth",
            "country.planet.code": "ER",
        },
    ]


def test_advanced_denorm_lvl_3(context, rc, tmp_path, geodb_denorm):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property            | type    | ref     | source       | prepare | access | level
    datasets/denorm/lvl3            |         |         |              |         |        |
      | rs                          | sql     |         |              |         |        |
      |   | Planet                  |         | code    | PLANET       |         | open   |
      |   |   | code                | string  |         | code         |         |        |
      |   |   | name                | string  |         | name         |         |        |
      |   | Country                 |         | code    | COUNTRY      |         | open   |
      |   |   | code                | string  |         | code         |         |        |
      |   |   | name                | string  |         | name         |         |        |
      |   |   | planet              | ref     | Planet  | planet       |         |        | 3
      |   |   | planet.name         |         |         |              |         |        |
      |   | City                    |         | code    | CITY         |         | open   |
      |   |   | code                | string  |         | code         |         |        |
      |   |   | name                | string  |         | name         |         |        |
      |   |   | country             | ref     | Country | country      |         |        | 3
      |   |   | country.name        |         |         | countryName  |         |        |
      |   |   | country.year        | integer |         | countryYear  |         |        |
      |   |   | country.planet.name | string  |         | planetName   |         |        |
    """),
    )

    app = create_client(rc, tmp_path, geodb_denorm)

    resp = app.get("/datasets/denorm/lvl3/Planet")
    assert listdata(resp, sort="code", full=True) == [
        {"code": "ER", "name": "Earth"},
        {"code": "JP", "name": "Jupyter"},
        {"code": "MR", "name": "Mars"},
    ]

    resp = app.get("/datasets/denorm/lvl3/Country")
    assert listdata(resp, "code", "name", "planet.name", sort="code", full=True) == [
        {"code": "EE", "name": "Estonia", "planet.name": "Jupyter"},
        {"code": "LT", "name": "Lithuania", "planet.name": "Earth"},
        {"code": "LV", "name": "Latvia", "planet.name": "Mars"},
    ]

    resp = app.get("/datasets/denorm/lvl3/City")
    assert listdata(
        resp,
        "code",
        "name",
        "country.name",
        "country.code",
        "country.year",
        "country.planet.name",
        "country.planet.code",
        sort="code",
        full=True,
    ) == [
        {
            "code": "RYG",
            "name": "Ryga",
            "country.name": "Latvia",
            "country.code": "LV",
            "country.year": 1408,
            "country.planet.name": "Marsas",
        },
        {
            "code": "TLN",
            "name": "Talin",
            "country.name": "Estonia",
            "country.code": "EE",
            "country.year": 1784,
            "country.planet.name": "Jupiteris",
        },
        {
            "code": "VLN",
            "name": "Vilnius",
            "country.name": "Lithuania",
            "country.code": "LT",
            "country.year": 1204,
            "country.planet.name": "Zeme",
        },
    ]


def test_advanced_denorm_lvl_3_multi(context, rc, tmp_path, geodb_denorm):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property            | type    | ref        | source      | prepare        | access | level
    datasets/denorm/lvl3            |         |            |             |                |        |
      | rs                          | sql     |            |             |                |        |
      |   | Planet                  |         | id, code   | PLANET      |                | open   |
      |   |   | id                  | integer |            | id          |                |        |
      |   |   | code                | string  |            | code        |                |        |
      |   |   | name                | string  |            | name        |                |        |
      |   | Country                 |         | code       | COUNTRY     |                | open   |
      |   |   | code                | string  |            | code        |                |        |
      |   |   | name                | string  |            | name        |                |        |
      |   |   | planet_code         | string  |            | planet      |                |        |
      |   |   | planet              | ref     | Planet     |             | 0, planet_code |        | 3
      |   | City                    |         | code       | CITY        |                | open   |
      |   |   | code                | string  |            | code        |                |        |
      |   |   | name                | string  |            | name        |                |        |
      |   |   | country             | ref     | Country    | country     |                |        | 3
      |   |   | country.name        |         |            | countryName |                |        |
      |   |   | country.year        | integer |            | countryYear |                |        |
      |   |   | country.planet.name |         |            |             |                |        |
      |   |   | country.planet.code | string  |            |             | 'ER'           |        |
    """),
    )

    app = create_client(rc, tmp_path, geodb_denorm)

    resp = app.get("/datasets/denorm/lvl3/Planet")
    assert listdata(resp, sort="code", full=True) == [
        {"id": 0, "code": "ER", "name": "Earth"},
        {"id": 0, "code": "JP", "name": "Jupyter"},
        {"id": 0, "code": "MR", "name": "Mars"},
    ]

    resp = app.get("/datasets/denorm/lvl3/Country")
    assert listdata(resp, "code", "name", sort="code", full=True) == [
        {
            "code": "EE",
            "name": "Estonia",
        },
        {
            "code": "LT",
            "name": "Lithuania",
        },
        {
            "code": "LV",
            "name": "Latvia",
        },
    ]

    resp = app.get("/datasets/denorm/lvl3/City")
    assert listdata(
        resp,
        "code",
        "name",
        "country.name",
        "country.code",
        "country.year",
        "country.planet.name",
        "country.planet.code",
        sort="code",
        full=True,
    ) == [
        {
            "code": "RYG",
            "name": "Ryga",
            "country.name": "Latvia",
            "country.code": "LV",
            "country.year": 1408,
            "country.planet.name": "Earth",
            "country.planet.code": "ER",
        },
        {
            "code": "TLN",
            "name": "Talin",
            "country.name": "Estonia",
            "country.code": "EE",
            "country.year": 1784,
            "country.planet.name": "Earth",
            "country.planet.code": "ER",
        },
        {
            "code": "VLN",
            "name": "Vilnius",
            "country.name": "Lithuania",
            "country.code": "LT",
            "country.year": 1204,
            "country.planet.name": "Earth",
            "country.planet.code": "ER",
        },
    ]


def test_denorm_lvl_3_multi(context, rc, tmp_path, geodb_denorm):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property            | type    | ref        | source      | prepare        | access | level
    datasets/denorm/lvl3            |         |            |             |                |        |
      | rs                          | sql     |            |             |                |        |
      |   | Planet                  |         | id, code   | PLANET      |                | open   |
      |   |   | id                  | integer |            | id          |                |        |
      |   |   | code                | string  |            | code        |                |        |
      |   |   | name                | string  |            | name        |                |        |
      |   | Country                 |         | code       | COUNTRY     |                | open   |
      |   |   | code                | string  |            | code        |                |        |
      |   |   | name                | string  |            | name        |                |        |
      |   |   | planet_code         | string  |            | planet      |                |        |
      |   |   | planet              | ref     | Planet     |             | 0, planet_code |        | 3
      |   |   | planet.name         |         |            |             |                |        |
    """),
    )

    app = create_client(rc, tmp_path, geodb_denorm)

    resp = app.get("/datasets/denorm/lvl3/Planet")
    assert listdata(resp, sort="code", full=True) == [
        {"id": 0, "code": "ER", "name": "Earth"},
        {"id": 0, "code": "JP", "name": "Jupyter"},
        {"id": 0, "code": "MR", "name": "Mars"},
    ]

    resp = app.get("/datasets/denorm/lvl3/Country")
    assert listdata(resp, "code", "name", "planet", sort="code", full=True) == [
        {"code": "EE", "name": "Estonia", "planet": {"code": "JP", "id": 0, "name": "Jupyter"}},
        {"code": "LT", "name": "Lithuania", "planet": {"code": "ER", "id": 0, "name": "Earth"}},
        {"code": "LV", "name": "Latvia", "planet": {"code": "MR", "id": 0, "name": "Mars"}},
    ]


def test_denorm_lvl_4_multi(context, rc, tmp_path, geodb_denorm):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property            | type    | ref        | source      | prepare        | access | level
    datasets/denorm/lvl4            |         |            |             |                |        |
      | rs                          | sql     |            |             |                |        |
      |   | Planet                  |         | id, code   | PLANET      |                | open   |
      |   |   | id                  | integer |            | id          |                |        |
      |   |   | code                | string  |            | code        |                |        |
      |   |   | name                | string  |            | name        |                |        |
      |   | Country                 |         | code       | COUNTRY     |                | open   |
      |   |   | code                | string  |            | code        |                |        |
      |   |   | name                | string  |            | name        |                |        |
      |   |   | planet_code         | string  |            | planet      |                |        |
      |   |   | planet              | ref     | Planet     |             | 0, planet_code |        | 4
      |   |   | planet.name         |         |            |             |                |        |
    """),
    )

    app = create_client(rc, tmp_path, geodb_denorm)

    resp = app.get("/datasets/denorm/lvl4/Planet")
    assert listdata(resp, sort="code", full=True) == [
        {"id": 0, "code": "ER", "name": "Earth"},
        {"id": 0, "code": "JP", "name": "Jupyter"},
        {"id": 0, "code": "MR", "name": "Mars"},
    ]
    ids = {values["name"]: values["_id"] for values in resp.json()["_data"]}
    ids_list = list(ids.values())
    assert ids_list.count(ids_list[0]) != len(ids_list)

    resp = app.get("/datasets/denorm/lvl4/Country")
    assert listdata(resp, "code", "name", "planet", sort="code", full=True) == [
        {"code": "EE", "name": "Estonia", "planet": {"_id": ids["Jupyter"], "name": "Jupyter"}},
        {"code": "LT", "name": "Lithuania", "planet": {"_id": ids["Earth"], "name": "Earth"}},
        {"code": "LV", "name": "Latvia", "planet": {"_id": ids["Mars"], "name": "Mars"}},
    ]


def test_keymap_ref_keys_valid_order(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""                   
        d | r | m | property        | type    | ref                | source       | prepare             | access
    datasets/keymap                 |         |                    |              |                     |
      | rs                          | sql     |                    |              |                     |
      |   | Planet                  |         | code               | PLANET       |                     | open
      |   |   | code                | string  |                    | CODE         |                     |
      |   |   | name                | string  |                    | NAME         |                     |
      |   | Country                 |         | code               | COUNTRY      |                     | open
      |   |   | code                | string  |                    | CODE         |                     |
      |   |   | name                | string  |                    | NAME         |                     |
      |   |   | planet              | ref     | Planet             | PLANET_CODE  |                     |
      |   |   | planet_name         | ref     | Planet[name]       | PLANET_NAME  |                     |
      |   |   | planet_combine      | ref     | Planet[code, name] |              | planet, planet_name |
      """),
    )

    sqlite.init(
        {
            "PLANET": [
                sa.Column("CODE", sa.Text, primary_key=True),
                sa.Column("NAME", sa.Text),
            ],
            "COUNTRY": [
                sa.Column("CODE", sa.Text, primary_key=True),
                sa.Column("NAME", sa.Text),
                sa.Column("PLANET_CODE", sa.Text),
                sa.Column("PLANET_NAME", sa.Text),
            ],
        }
    )

    sqlite.write(
        "PLANET",
        [
            {"CODE": "ER", "NAME": "Earth"},
            {"CODE": "MS", "NAME": "Mars"},
        ],
    )
    sqlite.write(
        "COUNTRY",
        [
            {"CODE": "LT", "NAME": "Lithuania", "PLANET_CODE": "ER", "PLANET_NAME": "Earth"},
            {"CODE": "LV", "NAME": "Latvia", "PLANET_CODE": "ER", "PLANET_NAME": "Earth"},
            {"CODE": "S5", "NAME": "s58467", "PLANET_CODE": "MS", "PLANET_NAME": "Mars"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)

    resp = app.get("/datasets/keymap/Planet")
    id_mapping = {data["code"]: data["_id"] for data in resp.json()["_data"]}
    assert listdata(resp, "_id", "code", "name", sort="code", full=True) == [
        {"_id": id_mapping["ER"], "code": "ER", "name": "Earth"},
        {"_id": id_mapping["MS"], "code": "MS", "name": "Mars"},
    ]

    resp = app.get("/datasets/keymap/Country")
    assert listdata(
        resp, "code", "name", "planet._id", "planet_name._id", "planet_combine._id", sort="code", full=True
    ) == [
        {
            "code": "LT",
            "name": "Lithuania",
            "planet._id": id_mapping["ER"],
            "planet_name._id": id_mapping["ER"],
            "planet_combine._id": id_mapping["ER"],
        },
        {
            "code": "LV",
            "name": "Latvia",
            "planet._id": id_mapping["ER"],
            "planet_name._id": id_mapping["ER"],
            "planet_combine._id": id_mapping["ER"],
        },
        {
            "code": "S5",
            "name": "s58467",
            "planet._id": id_mapping["MS"],
            "planet_name._id": id_mapping["MS"],
            "planet_combine._id": id_mapping["MS"],
        },
    ]


def test_keymap_ref_keys_invalid_order(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""                   
        d | r | m | property        | type    | ref                | source       | prepare             | access
    datasets/keymap                 |         |                    |              |                     |
      | rs                          | sql     |                    |              |                     |
      |   | Planet                  |         | code               | PLANET       |                     | open
      |   |   | code                | string  |                    | CODE         |                     |
      |   |   | name                | string  |                    | NAME         |                     |
      |   | Country                 |         | code               | COUNTRY      |                     | open
      |   |   | code                | string  |                    | CODE         |                     |
      |   |   | name                | string  |                    | NAME         |                     |
      |   |   | planet              | ref     | Planet             | PLANET_CODE  |                     |
      |   |   | planet_name         | ref     | Planet[name]       | PLANET_NAME  |                     |
      |   |   | planet_combine      | ref     | Planet[code, name] |              | planet, planet_name |
      """),
    )

    sqlite.init(
        {
            "PLANET": [
                sa.Column("CODE", sa.Text, primary_key=True),
                sa.Column("NAME", sa.Text),
            ],
            "COUNTRY": [
                sa.Column("CODE", sa.Text, primary_key=True),
                sa.Column("NAME", sa.Text),
                sa.Column("PLANET_CODE", sa.Text),
                sa.Column("PLANET_NAME", sa.Text),
            ],
        }
    )

    sqlite.write(
        "PLANET",
        [
            {"CODE": "ER", "NAME": "Earth"},
            {"CODE": "MS", "NAME": "Mars"},
        ],
    )
    sqlite.write(
        "COUNTRY",
        [
            {"CODE": "LT", "NAME": "Lithuania", "PLANET_CODE": "ER", "PLANET_NAME": "Earth"},
            {"CODE": "LV", "NAME": "Latvia", "PLANET_CODE": "ER", "PLANET_NAME": "Earth"},
            {"CODE": "S5", "NAME": "s58467", "PLANET_CODE": "MS", "PLANET_NAME": "Mars"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)

    resp = app.get("/datasets/keymap/Country")
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
            "planet_combine._id": id_mapping["ER"],
        },
        {
            "code": "LV",
            "name": "Latvia",
            "planet._id": id_mapping["ER"],
            "planet_name._id": id_mapping["ER"],
            "planet_combine._id": id_mapping["ER"],
        },
        {
            "code": "S5",
            "name": "s58467",
            "planet._id": id_mapping["MS"],
            "planet_name._id": id_mapping["MS"],
            "planet_combine._id": id_mapping["MS"],
        },
    ]


def test_composite_non_pk_ref_with_literal(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
d | r | b | m | property | type    | ref                       | source       | prepare  | access
example                  |         |                           |              |          |
  |   |   | Translation  |         | id                        | translations |          | open
  |   |   |   | id       | integer |                           | id           |          |
  |   |   |   | lang     | string  |                           | lang         |          |
  |   |   |   | name     | string  |                           | name         |          |
  |   |   |   | city_id  | integer |                           | city_id      |          |
  |   |   |   |          |         |                           |              |          |
  |   |   | City         |         | id                        | cities       |          | open
  |   |   |   | id       | integer |                           | id           |          |
  |   |   |   | en       | ref     | Translation[city_id,lang] |              | id, "en" |
  |   |   |   | name_en  | string  |                           |              | en.name  |
  |   |   |   | lt       | ref     | Translation[city_id,lang] |              | id, "lt" |
  |   |   |   | name_lt  | string  |                           |              | lt.name  |
    """),
    )

    sqlite.init(
        {
            "cities": [
                sa.Column("id", sa.Integer, primary_key=True),
            ],
            "translations": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("lang", sa.Text),
                sa.Column("name", sa.Text),
                sa.Column("city_id", sa.Integer),
            ],
        }
    )

    sqlite.write(
        "translations",
        [
            {"id": 0, "lang": "lt", "name": "Vilniaus miestas", "city_id": 0},
            {"id": 1, "lang": "en", "name": "City of Vilnius", "city_id": 0},
            {"id": 2, "lang": "lt", "name": "Kauno miestas", "city_id": 1},
            {"id": 3, "lang": "en", "name": "City of Kaunas", "city_id": 1},
        ],
    )
    sqlite.write(
        "cities",
        [
            {"id": 0},
            {"id": 1},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)

    resp = app.get("/example/Translation")
    mapper = {}
    for item in resp.json()["_data"]:
        mapper[(item["city_id"], item["lang"])] = item["_id"]

    resp = app.get("/example/City")
    assert listdata(resp, sort="id", full=True) == [
        {
            "id": 0,
            "en._id": mapper[(0, "en")],
            "name_en": "City of Vilnius",
            "lt._id": mapper[(0, "lt")],
            "name_lt": "Vilniaus miestas",
        },
        {
            "id": 1,
            "en._id": mapper[(1, "en")],
            "name_en": "City of Kaunas",
            "lt._id": mapper[(1, "lt")],
            "name_lt": "Kauno miestas",
        },
    ]


def test_non_pk_ref_only_literal(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
d | r | b | m | property | type    | ref                       | source       | prepare  | access
example                  |         |                           |              |          |
  |   |   | Translation  |         | id                        | translations |          | open
  |   |   |   | id       | integer |                           | id           |          |
  |   |   |   | lang     | string  |                           | lang         |          |
  |   |   |   | name     | string  |                           | name         |          |
  |   |   |   | city_id  | integer |                           | city_id      |          |
  |   |   |   |          |         |                           |              |          |
  |   |   | City         |         | id                        | cities       |          | open
  |   |   |   | id       | integer |                           | id           |          |
  |   |   |   | en       | ref     | Translation[lang]         |              | "en"     |
  |   |   |   | name_en  | string  |                           |              | en.name  |
  |   |   |   | lt       | ref     | Translation[lang]         |              | "lt"     |
  |   |   |   | name_lt  | string  |                           |              | lt.name  |
    """),
    )

    sqlite.init(
        {
            "cities": [
                sa.Column("id", sa.Integer, primary_key=True),
            ],
            "translations": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("lang", sa.Text),
                sa.Column("name", sa.Text),
                sa.Column("city_id", sa.Integer),
            ],
        }
    )

    sqlite.write(
        "translations",
        [
            {"id": 0, "lang": "lt", "name": "Vilniaus miestas", "city_id": 0},
            {"id": 1, "lang": "en", "name": "City of Vilnius", "city_id": 0},
        ],
    )
    sqlite.write(
        "cities",
        [
            {"id": 0},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)

    resp = app.get("/example/Translation")
    mapper = {}
    for item in resp.json()["_data"]:
        mapper[(item["city_id"], item["lang"])] = item["_id"]

    resp = app.get("/example/City")
    assert listdata(resp, sort="id", full=True) == [
        {
            "id": 0,
            "en._id": mapper[(0, "en")],
            "name_en": "City of Vilnius",
            "lt._id": mapper[(0, "lt")],
            "name_lt": "Vilniaus miestas",
        }
    ]


def test_non_pk_external_ref_with_literal(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
d | r | b | m | property | type    | ref                       | source       | prepare  | access | level
example                  |         |                           |              |          |        |
  |   |   | Translation  |         | id                        | translations |          | open   |
  |   |   |   | id       | integer |                           | id           |          |        |
  |   |   |   | lang     | string  |                           | lang         |          |        |
  |   |   |   | name     | string  |                           | name         |          |        |
  |   |   |   | city_id  | integer |                           | city_id      |          |        |
  |   |   |   |          |         |                           |              |          |        |
  |   |   | City         |         | id                        | cities       |          | open   |
  |   |   |   | id       | integer |                           | id           |          |        |
  |   |   |   | en       | ref     | Translation[city_id,lang] |              | id, "en" |        | 3
  |   |   |   | name_en  | string  |                           |              | en.name  |        |
  |   |   |   | lt       | ref     | Translation[city_id,lang] |              | id, "lt" |        | 3
  |   |   |   | name_lt  | string  |                           |              | lt.name  |        |
    """),
    )

    sqlite.init(
        {
            "cities": [
                sa.Column("id", sa.Integer, primary_key=True),
            ],
            "translations": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("lang", sa.Text),
                sa.Column("name", sa.Text),
                sa.Column("city_id", sa.Integer),
            ],
        }
    )

    sqlite.write(
        "translations",
        [
            {"id": 0, "lang": "lt", "name": "Vilniaus miestas", "city_id": 0},
            {"id": 1, "lang": "en", "name": "City of Vilnius", "city_id": 0},
            {"id": 2, "lang": "lt", "name": "Kauno miestas", "city_id": 1},
            {"id": 3, "lang": "en", "name": "City of Kaunas", "city_id": 1},
        ],
    )
    sqlite.write(
        "cities",
        [
            {"id": 0},
            {"id": 1},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)

    resp = app.get("/example/City")
    assert listdata(resp, sort="id", full=True) == [
        {
            "id": 0,
            "en.city_id": 0,
            "en.lang": "en",
            "name_en": "City of Vilnius",
            "lt.city_id": 0,
            "lt.lang": "lt",
            "name_lt": "Vilniaus miestas",
        },
        {
            "id": 1,
            "en.city_id": 1,
            "en.lang": "en",
            "name_en": "City of Kaunas",
            "lt.city_id": 1,
            "lt.lang": "lt",
            "name_lt": "Kauno miestas",
        },
    ]


def test_non_pk_external_ref_only_literal(context, rc, tmp_path, sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
d | r | b | m | property | type    | ref                       | source       | prepare  | access | level
example                  |         |                           |              |          |        |
  |   |   | Translation  |         | id                        | translations |          | open   |
  |   |   |   | id       | integer |                           | id           |          |        |
  |   |   |   | lang     | string  |                           | lang         |          |        |
  |   |   |   | name     | string  |                           | name         |          |        |
  |   |   |   | city_id  | integer |                           | city_id      |          |        |
  |   |   |   |          |         |                           |              |          |        |
  |   |   | City         |         | id                        | cities       |          | open   |
  |   |   |   | id       | integer |                           | id           |          |        |
  |   |   |   | en       | ref     | Translation[lang]         |              | "en"     |        | 3
  |   |   |   | name_en  | string  |                           |              | en.name  |        |
  |   |   |   | lt       | ref     | Translation[lang]         |              | "lt"     |        | 3
  |   |   |   | name_lt  | string  |                           |              | lt.name  |        |
    """),
    )

    sqlite.init(
        {
            "cities": [
                sa.Column("id", sa.Integer, primary_key=True),
            ],
            "translations": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("lang", sa.Text),
                sa.Column("name", sa.Text),
                sa.Column("city_id", sa.Integer),
            ],
        }
    )

    sqlite.write(
        "translations",
        [
            {"id": 0, "lang": "lt", "name": "Vilniaus miestas", "city_id": 0},
            {"id": 1, "lang": "en", "name": "City of Vilnius", "city_id": 0},
        ],
    )
    sqlite.write(
        "cities",
        [
            {"id": 0},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)

    resp = app.get("/example/City")
    assert listdata(resp, sort="id", full=True) == [
        {"id": 0, "en.lang": "en", "name_en": "City of Vilnius", "lt.lang": "lt", "name_lt": "Vilniaus miestas"}
    ]


def test_object(context, rc, tmp_path, geodb_denorm):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property            | type    | ref     | source       | prepare | access | level
    datasets/object                 |         |         |              |         |        |
      | rs                          | sql     |         |              |         |        |
      |   | City                    |         | code    | CITY         |         | open   |
      |   |   | code                | string  |         | code         |         |        |
      |   |   | name                | string  |         | name         |         |        |
      |   |   | country             | object  |         |              |         |        |
      |   |   | country.name        | string  |         | countryName  |         |        |
      |   |   | country.year        | integer |         | countryYear  |         |        |
    """),
    )

    app = create_client(rc, tmp_path, geodb_denorm)

    resp = app.get("/datasets/object/City")
    assert listdata(resp, sort="code", full=True) == [
        {
            "code": "RYG",
            "name": "Ryga",
            "country.name": "Latvia",
            "country.year": 1408,
        },
        {
            "code": "TLN",
            "name": "Talin",
            "country.name": "Estija",
            "country.year": 1784,
        },
        {
            "code": "VLN",
            "name": "Vilnius",
            "country.name": "Lietuva",
            "country.year": 1204,
        },
    ]


def test_nested_object(context, rc, tmp_path, geodb_denorm):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property            | type    | ref     | source       | prepare | access | level
    datasets/object/nested          |         |         |              |         |        |
      | rs                          | sql     |         |              |         |        |
      |   | City                    |         | code    | CITY         |         | open   |
      |   |   | code                | string  |         | code         |         |        |
      |   |   | name                | string  |         | name         |         |        |
      |   |   | c                   | object  |         |              |         |        |
      |   |   | c.country           | object  |         |              |         |        |
      |   |   | c.country.name      | string  |         | countryName  |         |        |
      |   |   | c.country.year      | integer |         | countryYear  |         |        |
    """),
    )

    app = create_client(rc, tmp_path, geodb_denorm)

    resp = app.get("/datasets/object/nested/City")
    assert listdata(resp, sort="code", full=True) == [
        {
            "code": "RYG",
            "name": "Ryga",
            "c.country.name": "Latvia",
            "c.country.year": 1408,
        },
        {
            "code": "TLN",
            "name": "Talin",
            "c.country.name": "Estija",
            "c.country.year": 1784,
        },
        {
            "code": "VLN",
            "name": "Vilnius",
            "c.country.name": "Lietuva",
            "c.country.year": 1204,
        },
    ]


def test_ref_object(context, rc, tmp_path, geodb_denorm):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property            | type    | ref     | source       | prepare | access | level
    datasets/object/ref             |         |         |              |         |        |
      | rs                          | sql     |         |              |         |        |
      |   | Country                 |         | c       | COUNTRY      |         | open   |
      |   |   | c                   | string  |         | code         |         |        |
      |   |   | name                | string  |         | name         |         |        |
      |   |   | code                | string  |         | code         |         |        |
      |   | City                    |         | code    | CITY         |         | open   |
      |   |   | code                | string  |         | code         |         |        |
      |   |   | name                | string  |         | name         |         |        |
      |   |   | country             | ref     | Country | country      |         |        | 3
      |   |   | country.code        |         |         |              |         |        |
      |   |   | country.meta        | object  |         |              |         |        |
      |   |   | country.meta.name   | string  |         | countryName  |         |        |
      |   |   | country.meta.year   | string  |         | countryYear  |         |        |
    """),
    )

    app = create_client(rc, tmp_path, geodb_denorm)

    resp = app.get("/datasets/object/ref/City")
    assert listdata(resp, sort="code", full=True) == [
        {
            "code": "RYG",
            "name": "Ryga",
            "country.c": "LV",
            "country.code": "LV",
            "country.meta.name": "Latvia",
            "country.meta.year": 1408,
        },
        {
            "code": "TLN",
            "name": "Talin",
            "country.c": "EE",
            "country.code": "EE",
            "country.meta.name": "Estija",
            "country.meta.year": 1784,
        },
        {
            "code": "VLN",
            "name": "Vilnius",
            "country.c": "LT",
            "country.code": "LT",
            "country.meta.name": "Lietuva",
            "country.meta.year": 1204,
        },
    ]


def test_object_filter(context, rc, tmp_path, geodb_denorm):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property            | type    | ref     | source       | prepare | access | level
    datasets/object                 |         |         |              |         |        |
      | rs                          | sql     |         |              |         |        |
      |   | City                    |         | code    | CITY         |         | open   |
      |   |   | code                | string  |         | code         |         |        |
      |   |   | name                | string  |         | name         |         |        |
      |   |   | country             | object  |         |              |         |        |
      |   |   | country.name        | string  |         | countryName  |         |        |
      |   |   | country.year        | integer |         | countryYear  |         |        |
    """),
    )

    app = create_client(rc, tmp_path, geodb_denorm)

    resp = app.get("/datasets/object/City?country.year>1300")
    assert listdata(resp, sort="code", full=True) == [
        {
            "code": "RYG",
            "name": "Ryga",
            "country.name": "Latvia",
            "country.year": 1408,
        },
        {
            "code": "TLN",
            "name": "Talin",
            "country.name": "Estija",
            "country.year": 1784,
        },
    ]

    resp = app.get("/datasets/object/City?country.year=1408")
    assert listdata(resp, sort="code", full=True) == [
        {
            "code": "RYG",
            "name": "Ryga",
            "country.name": "Latvia",
            "country.year": 1408,
        }
    ]


def test_object_filter_nested(context, rc, tmp_path, geodb_denorm):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property            | type    | ref     | source       | prepare | access | level
    datasets/object/nested          |         |         |              |         |        |
      | rs                          | sql     |         |              |         |        |
      |   | City                    |         | code    | CITY         |         | open   |
      |   |   | code                | string  |         | code         |         |        |
      |   |   | name                | string  |         | name         |         |        |
      |   |   | c                   | object  |         |              |         |        |
      |   |   | c.country           | object  |         |              |         |        |
      |   |   | c.country.name      | string  |         | countryName  |         |        |
      |   |   | c.country.year      | integer |         | countryYear  |         |        |
    """),
    )

    app = create_client(rc, tmp_path, geodb_denorm)

    resp = app.get("/datasets/object/nested/City?c.country.year>1300")
    assert listdata(resp, sort="code", full=True) == [
        {
            "code": "RYG",
            "name": "Ryga",
            "c.country.name": "Latvia",
            "c.country.year": 1408,
        },
        {
            "code": "TLN",
            "name": "Talin",
            "c.country.name": "Estija",
            "c.country.year": 1784,
        },
    ]

    resp = app.get("/datasets/object/nested/City?c.country.year=1408")
    assert listdata(resp, sort="code", full=True) == [
        {
            "code": "RYG",
            "name": "Ryga",
            "c.country.name": "Latvia",
            "c.country.year": 1408,
        }
    ]


@pytest.mark.parametrize("ref_level", [3, 4])
def test_ref_prepare_key_count_missmatch(ref_level, context, rc, tmp_path, geodb_denorm):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(f"""
    d | r | m | property    | type    | ref    | source      | prepare        | access | level
    datasets/ref/err        |         |        |             |                |        |
      | rs                  | sql     |        |             |                |        |
      |   | Planet          |         | id     | PLANET      |                | open   |
      |   |   | id          | integer |        | id          |                |        |
      |   |   | code        | string  |        | code        |                |        |
      |   |   | name        | string  |        | name        |                |        |
      |   | Country         |         | code   | COUNTRY     |                | open   |
      |   |   | code        | string  |        | code        |                |        |
      |   |   | name        | string  |        | name        |                |        |
      |   |   | planet_code | string  |        | planet      |                |        |
      |   |   | planet      | ref     | Planet |             | 0, planet_code |        | {ref_level}
      |   |   | planet.name |         |        |             |                |        |
    """),
    )

    app = create_client(rc, tmp_path, geodb_denorm)

    resp = app.get("/datasets/ref/err/Planet")
    assert listdata(resp, sort="code", full=True) == [
        {"id": 0, "code": "ER", "name": "Earth"},
        {"id": 0, "code": "JP", "name": "Jupyter"},
        {"id": 0, "code": "MR", "name": "Mars"},
    ]

    resp = app.get("/datasets/ref/err/Country")
    assert get_error_codes(resp.json()) == ["GivenValueCountMissmatch"]


@pytest.mark.parametrize("ref_level", [3, 4])
def test_ref_source_key_count_missmatch(ref_level, context, rc, tmp_path, geodb_denorm, reset_keymap):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(f"""
    d | r | m | property    | type    | ref      | source  | prepare | access | level
    datasets/ref            |         |          |         |         |        |
      | rs                  | sql     |          |         |         |        |
      |   | Planet          |         | id, code | PLANET  |         | open   |
      |   |   | id          | integer |          | id      |         |        |
      |   |   | code        | string  |          | code    |         |        |
      |   |   | name        | string  |          | name    |         |        |
      |   | Country         |         | code     | COUNTRY |         | open   |
      |   |   | code        | string  |          | code    |         |        |
      |   |   | name        | string  |          | name    |         |        |
      |   |   | planet      | ref     | Planet   | planet  |         |        | {ref_level}
      |   |   | planet.name |         |          |         |         |        |
    """),
    )

    app = create_client(rc, tmp_path, geodb_denorm)

    resp = app.get("/datasets/ref/Planet")
    assert listdata(resp, sort="code", full=True) == [
        {"id": 0, "code": "ER", "name": "Earth"},
        {"id": 0, "code": "JP", "name": "Jupyter"},
        {"id": 0, "code": "MR", "name": "Mars"},
    ]

    resp = app.get("/datasets/ref/Country")
    assert get_error_codes(resp.json()) == ["GivenValueCountMissmatch"]


def test_keymap_value_not_found_internal_model(context, rc, tmp_path, geodb_denorm, reset_keymap):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | m | property    | type    | ref          | source      | prepare | access | level
    datasets/ref            |         |              |             |         |        |
      | rs                  |         | sql          |             |         |        |
      |   | Planet          |         | id, code     |             |         | open   |
      |   |   | id          | integer |              |             |         |        |
      |   |   | code        | string  |              |             |         |        |
      |   |   | name        | string  |              |             |         |        |
      |   | Country         |         | code         | COUNTRY     |         | open   |
      |   |   | code        | string  |              | code        |         |        |
      |   |   | name        | string  |              | name        |         |        |
      |   |   | planet      | ref     | Planet[code] | planet      |         |        |
    """),
    )

    app = create_client(rc, tmp_path, geodb_denorm, mode="external")
    resp = app.get("/datasets/ref/Country")
    assert get_error_codes(resp.json()) == ["KeymapValueNotFound"]


def test_keymap_internal_model_after_sync(
    context, rc, tmp_path, geodb_denorm, postgresql, cli: SpintaCliRunner, responses, request, reset_keymap
):
    table = """
    d | r | m | property    | type    | ref          | source      | prepare | access | level
    datasets/ref            |         |              |             |         |        |
      | rs                  |         | sql          |             |         |        |
      |   | Planet          |         | id, code     |             |         | open   |
      |   |   | id          | integer |              |             |         |        |
      |   |   | code        | string  |              |             |         |        |
      |   |   | name        | string  |              |             |         |        |
      |   | Country         |         | code         | COUNTRY     |         | open   |
      |   |   | code        | string  |              | code        |         |        |
      |   |   | name        | string  |              | name        |         |        |
      |   |   | planet      | ref     | Planet[code] | planet      |         |        |
    """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    localrc = create_rc(rc, tmp_path, geodb_denorm)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    remote.app.authmodel("datasets/ref/Planet", ["insert", "wipe"])
    er_id = remote.app.post("/datasets/ref/Planet", json={"id": 0, "code": "ER"}).json()["_id"]
    mr_id = remote.app.post("/datasets/ref/Planet", json={"id": 1, "code": "MR"}).json()["_id"]
    jp_id = remote.app.post("/datasets/ref/Planet", json={"id": 2, "code": "JP"}).json()["_id"]

    app = create_client(rc, tmp_path, geodb_denorm, mode="external")
    resp = app.get("/datasets/ref/Country")
    assert get_error_codes(resp.json()) == ["KeymapValueNotFound"]

    cli.invoke(
        localrc, ["keymap", "sync", tmp_path / "manifest.csv", "-i", remote.url, "--credentials", remote.credsfile]
    )

    resp = app.get("/datasets/ref/Country")
    assert listdata(resp, "code", "planet", full=True) == [
        {"code": "EE", "planet": {"_id": jp_id}},
        {"code": "LT", "planet": {"_id": er_id}},
        {"code": "LV", "planet": {"_id": mr_id}},
    ]
