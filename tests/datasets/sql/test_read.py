import pytest
import sqlalchemy as sa

from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.exceptions import TooShortPageSize
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.client import create_client
from spinta.testing.data import listdata
from spinta.testing.datasets import create_sqlite_db
from spinta.testing.manifest import load_manifest_and_context
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.utils import create_empty_backend, get_error_codes
from spinta.ufuncs.querybuilder.components import Selected
from spinta.ufuncs.resultbuilder.helpers import get_row_value

_DEFAULT_WITH_SQLITE_BACKENDS = ["sql", "sqlite"]


@pytest.fixture(scope="module")
def geodb_null_check():
    with create_sqlite_db(
        {
            "cities": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("name", sa.Text),
            ]
        }
    ) as db:
        db.write(
            "cities",
            [
                {"id": 0, "name": "Vilnius"},
            ],
        )
        yield db


@pytest.fixture(scope="module")
def geodb_with_nulls():
    with create_sqlite_db(
        {
            "cities": [
                sa.Column("id", sa.Integer),
                sa.Column("name", sa.Text),
                sa.Column("code", sa.Text),
                sa.Column("unique", sa.Integer),
            ]
        }
    ) as db:
        db.write(
            "cities",
            [
                {"id": 0, "name": "Vilnius", "code": "VLN", "unique": 0},
                {"id": 0, "name": "Vilnius", "code": "vln", "unique": 1},
                {"id": 0, "name": "Vilnius", "code": "V", "unique": 2},
                {"id": 0, "name": None, "code": None, "unique": 3},
                {"id": 0, "name": "Vilnius", "code": None, "unique": 4},
                {"id": 1, "name": "Test", "code": None, "unique": 5},
                {"id": 2, "name": None, "code": None, "unique": None},
                {"id": 2, "name": "Ryga", "code": "RG", "unique": 6},
                {"id": None, "name": "EMPTY", "code": None, "unique": 7},
                {"id": None, "name": "EMPTY", "code": None, "unique": 8},
                {"id": None, "name": "EMPTY", "code": None, "unique": 9},
                {"id": None, "name": "EMPTY", "code": "ERROR", "unique": 10},
            ],
        )
        yield db


@pytest.fixture(scope="module")
def geodb_geometry():
    with create_sqlite_db(
        {
            "cities": [
                sa.Column("name", sa.Text),
                sa.Column("id", sa.Integer),
                sa.Column("geo_id", sa.Integer),
                sa.Column("poly", sa.Text),
                sa.Column("geo_lt", sa.Text),
            ],
            "geodata": [sa.Column("id", sa.Integer), sa.Column("geo_poly", sa.Text), sa.Column("geo_point", sa.Text)],
        }
    ) as db:
        db.write(
            "cities",
            [
                {
                    "name": "Vilnius",
                    "id": 0,
                    "poly": "POLYGON ((80 50, 50 50, 50 80, 80 80, 80 50))",
                    "geo_lt": "POINT (5980000 200000)",
                    "geo_id": 0,
                },
            ],
        )
        db.write(
            "geodata",
            [
                {
                    "id": 0,
                    "geo_poly": "POLYGON ((80 50, 50 50, 50 80, 80 80, 80 50))",
                    "geo_point": "POINT (5980000 200000)",
                },
            ],
        )
        yield db


@pytest.fixture(scope="module")
def geodb_array():
    with create_sqlite_db(
        {
            "language": [
                sa.Column("name", sa.Text),
                sa.Column("id", sa.Integer),
            ],
            "country": [sa.Column("name", sa.Text), sa.Column("id", sa.Integer), sa.Column("languages", sa.Text)],
            "country_language": [
                sa.Column("country_id", sa.Integer),
                sa.Column("country_name", sa.Text),
                sa.Column("language_id", sa.Integer),
                sa.Column("language_name", sa.Text),
            ],
        }
    ) as db:
        db.write(
            "language", [{"id": 0, "name": "English"}, {"id": 1, "name": "Lithuanian"}, {"id": 2, "name": "Polish"}]
        )
        db.write(
            "country",
            [
                {"id": 0, "name": "United Kingdoms", "languages": "English"},
                {"id": 1, "name": "Lithuania", "languages": "English,Lithuanian"},
                {"id": 2, "name": "Poland", "languages": "English,Polish"},
            ],
        )
        db.write(
            "country_language",
            [
                {"country_id": 0, "country_name": "United Kingdoms", "language_id": 0, "language_name": "English"},
                {"country_id": 1, "country_name": "Lithuania", "language_id": 0, "language_name": "English"},
                {"country_id": 1, "country_name": "Lithuania", "language_id": 1, "language_name": "Lithuanian"},
                {"country_id": 2, "country_name": "Poland", "language_id": 0, "language_name": "English"},
                {"country_id": 2, "country_name": "Poland", "language_id": 2, "language_name": "Polish"},
            ],
        )
        yield db


def test__get_row_value_null(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property | type    | ref | source | prepare | access
    example                  |         |     |        |         |
      |   |   | City         |         |     |        |         |
      |   |   |   | name     | string  |     |        |         | open
      |   |   |   | rating   | integer |     |        |         | open
      |   |   |   |          | enum    |     | 1      | 1       |
      |   |   |   |          |         |     | 2      | 2       |
    """,
    )
    row = ["Vilnius", None]
    model = commands.get_model(context, manifest, "example/City")
    sel = Selected(1, model.properties["rating"])
    backend = create_empty_backend(context, "sql")
    assert get_row_value(context, backend, row, sel) is None


@pytest.mark.parametrize("db_dialect", _DEFAULT_WITH_SQLITE_BACKENDS)
def test_getall_paginate_invalid_type(db_dialect: str, context: Context, rc: RawConfig, tmp_path, geodb_null_check):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(f"""
    id | d | r | b | m | property | source | type    | ref          | access | prepare
       | external/paginate        |        |         |              |        |
       |   | data                 |        |         | {db_dialect} |        |
       |   |   |                  |        |         |              |        |
       |   |   |   | City         | cities |         | id, test     | open   |
       |   |   |   |   | id       | id     | integer |              |        |
       |   |   |   |   | name     | name   | string  |              |        | 
       |   |   |   |   | test     | name   | object  |              |        | 
    """),
    )

    app = create_client(rc, tmp_path, geodb_null_check, mode="external")

    resp = app.get("/external/paginate/City")
    assert listdata(resp, "id", "name") == [
        (0, "Vilnius"),
    ]
    assert "_page" not in resp.json()


@pytest.mark.parametrize("db_dialect", _DEFAULT_WITH_SQLITE_BACKENDS)
def test_getall_paginate_null_check_value(db_dialect: str, context: Context, rc: RawConfig, tmp_path, geodb_null_check):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(f"""
    id | d | r | b | m | property | source | type    | ref          | access | prepare
       | external/paginate        |        |         |              |        |
       |   | data                 |        |         | {db_dialect} |        |
       |   |   |                  |        |         |              |        |
       |   |   |   | City         | cities |         | id           | open   |
       |   |   |   |   | id       | id     | integer |              |        |
       |   |   |   |   | name     | name   | string  |              |        | 
    """),
    )

    app = create_client(rc, tmp_path, geodb_null_check, mode="external")

    resp = app.get("/external/paginate/City")
    assert listdata(resp, "id", "name") == [
        (0, "Vilnius"),
    ]


@pytest.mark.parametrize("db_dialect", _DEFAULT_WITH_SQLITE_BACKENDS)
def test_getall_paginate_with_nulls_page_too_small(
    db_dialect: str, context: Context, rc: RawConfig, tmp_path, geodb_with_nulls
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(f"""
    id | d | r | b | m | property | source | type    | ref          | access | prepare
       | external/paginate        |        |         |              |        |
       |   | data                 |        |         | {db_dialect} |        |
       |   |   |                  |        |         |              |        |
       |   |   |   | City         | cities |         | id           | open   |
       |   |   |   |   | id       | id     | integer |              |        |
       |   |   |   |   | name     | name   | string  |              |        | 
       |   |   |   |   | code     | code   | string  |              |        | 
    """),
    )

    app = create_client(rc, tmp_path, geodb_with_nulls, mode="external")

    with pytest.raises(BaseException) as e:
        app.get("/external/paginate/City?page(size:2)")
        exceptions = e.exceptions
        assert len(exceptions) == 1
        assert isinstance(exceptions[0], TooShortPageSize)


@pytest.mark.parametrize("db_dialect", _DEFAULT_WITH_SQLITE_BACKENDS)
def test_getall_paginate_with_nulls(db_dialect: str, context: Context, rc: RawConfig, tmp_path, geodb_with_nulls):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(f"""
    id | d | r | b | m | property | source | type    | ref          | access | prepare
       | external/paginate/null0  |        |         |              |        |
       |   | data                 |        |         | {db_dialect} |        |
       |   |   |                  |        |         |              |        |
       |   |   |   | City         | cities |         | id           | open   |
       |   |   |   |   | id       | id     | integer |              |        |
       |   |   |   |   | name     | name   | string  |              |        | 
       |   |   |   |   | code     | code   | string  |              |        | 
    """),
    )

    app = create_client(rc, tmp_path, geodb_with_nulls, mode="external")
    resp = app.get("/external/paginate/null0/City?page(size:6)")
    assert listdata(resp, "id", "name", "code") == [
        (0, "Vilnius", "V"),
        (0, "Vilnius", "VLN"),
        (0, "Vilnius", "vln"),
        (0, "Vilnius", None),
        (0, None, None),
        (1, "Test", None),
        (2, "Ryga", "RG"),
        (2, None, None),
        (None, "EMPTY", "ERROR"),
        (None, "EMPTY", None),
        (None, "EMPTY", None),
        (None, "EMPTY", None),
    ]


@pytest.mark.parametrize("db_dialect", _DEFAULT_WITH_SQLITE_BACKENDS)
def test_getall_paginate_with_nulls_multi_key(
    db_dialect: str, context: Context, rc: RawConfig, tmp_path, geodb_with_nulls
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(f"""
    id | d | r | b | m | property | source | type    | ref          | access | prepare
       | external/paginate/null1  |        |         |              |        |
       |   | data                 |        |         | {db_dialect} |        |
       |   |   |                  |        |         |              |        |
       |   |   |   | City         | cities |         | id, code     | open   |
       |   |   |   |   | id       | id     | integer |              |        |
       |   |   |   |   | name     | name   | string  |              |        | 
       |   |   |   |   | code     | code   | string  |              |        | 
    """),
    )

    app = create_client(rc, tmp_path, geodb_with_nulls, mode="external")
    resp = app.get("/external/paginate/null1/City?page(size:6)")
    assert listdata(resp, "id", "name", "code") == [
        (0, "Vilnius", "V"),
        (0, "Vilnius", "VLN"),
        (0, "Vilnius", "vln"),
        (0, "Vilnius", None),
        (0, None, None),
        (1, "Test", None),
        (2, "Ryga", "RG"),
        (2, None, None),
        (None, "EMPTY", "ERROR"),
        (None, "EMPTY", None),
        (None, "EMPTY", None),
        (None, "EMPTY", None),
    ]


@pytest.mark.parametrize("db_dialect", _DEFAULT_WITH_SQLITE_BACKENDS)
def test_getall_paginate_with_nulls_all_keys(
    db_dialect: str, context: Context, rc: RawConfig, tmp_path, geodb_with_nulls
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(f"""
    id | d | r | b | m | property | source | type    | ref            | access | prepare
       | external/paginate/null1  |        |         |                |        |
       |   | data                 |        |         | {db_dialect}   |        |
       |   |   |                  |        |         |                |        |
       |   |   |   | City         | cities |         | id, name, code | open   |
       |   |   |   |   | id       | id     | integer |                |        |
       |   |   |   |   | name     | name   | string  |                |        | 
       |   |   |   |   | code     | code   | string  |                |        | 
    """),
    )

    app = create_client(rc, tmp_path, geodb_with_nulls, mode="external")
    resp = app.get("/external/paginate/null1/City?page(size:3)")
    assert listdata(resp, "id", "name", "code") == [
        (0, "Vilnius", "V"),
        (0, "Vilnius", "VLN"),
        (0, "Vilnius", "vln"),
        (0, "Vilnius", None),
        (0, None, None),
        (1, "Test", None),
        (2, "Ryga", "RG"),
        (2, None, None),
        (None, "EMPTY", "ERROR"),
        (None, "EMPTY", None),
        (None, "EMPTY", None),
        (None, "EMPTY", None),
    ]


@pytest.mark.parametrize("db_dialect", _DEFAULT_WITH_SQLITE_BACKENDS)
def test_getall_paginate_with_nulls_and_sort(
    db_dialect: str, context: Context, rc: RawConfig, tmp_path, geodb_with_nulls
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(f"""
    id | d | r | b | m | property | source | type    | ref          | access | prepare
       | external/paginate/null2  |        |         |              |        |
       |   | data                 |        |         | {db_dialect} |        |
       |   |   |                  |        |         |              |        |
       |   |   |   | City         | cities |         | id           | open   |
       |   |   |   |   | id       | id     | integer |              |        |
       |   |   |   |   | name     | name   | string  |              |        | 
       |   |   |   |   | code     | code   | string  |              |        | 
    """),
    )

    app = create_client(rc, tmp_path, geodb_with_nulls, mode="external")
    resp = app.get("/external/paginate/null2/City?sort(name)&page(size:6)")
    assert listdata(resp, "id", "name", "code") == [
        (0, "Vilnius", "V"),
        (0, "Vilnius", "VLN"),
        (0, "Vilnius", "vln"),
        (0, "Vilnius", None),
        (0, None, None),
        (1, "Test", None),
        (2, "Ryga", "RG"),
        (2, None, None),
        (None, "EMPTY", "ERROR"),
        (None, "EMPTY", None),
        (None, "EMPTY", None),
        (None, "EMPTY", None),
    ]


@pytest.mark.parametrize("db_dialect", _DEFAULT_WITH_SQLITE_BACKENDS)
def test_getall_paginate_with_nulls_unique(
    db_dialect: str, context: Context, rc: RawConfig, tmp_path, geodb_with_nulls
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(f"""
    id | d | r | b | m | property | source | type    | ref          | access | prepare
       | external/paginate/null3  |        |         |              |        |
       |   | data                 |        |         | {db_dialect} |        |
       |   |   |                  |        |         |              |        |
       |   |   |   | City         | cities |         | name, unique | open   |
       |   |   |   |   | id       | id     | integer |              |        |
       |   |   |   |   | name     | name   | string  |              |        | 
       |   |   |   |   | code     | code   | string  |              |        | 
       |   |   |   |   | unique   | unique | integer |              |        | 
    """),
    )

    app = create_client(rc, tmp_path, geodb_with_nulls, mode="external")
    resp = app.get("/external/paginate/null3/City?sort(name, -unique)&page(size:1)")
    assert listdata(resp, "name", "unique", "id", "code") == [
        ("EMPTY", 10, None, "ERROR"),
        ("EMPTY", 7, None, None),
        ("EMPTY", 8, None, None),
        ("EMPTY", 9, None, None),
        ("Ryga", 6, 2, "RG"),
        ("Test", 5, 1, None),
        ("Vilnius", 0, 0, "VLN"),
        ("Vilnius", 1, 0, "vln"),
        ("Vilnius", 2, 0, "V"),
        ("Vilnius", 4, 0, None),
        (None, 3, 0, None),
        (None, None, 2, None),
    ]

    resp = app.get("/external/paginate/null3/City?sort(name, unique)&page(size:1)")
    assert listdata(resp, "name", "unique", "id", "code") == [
        ("EMPTY", 10, None, "ERROR"),
        ("EMPTY", 7, None, None),
        ("EMPTY", 8, None, None),
        ("EMPTY", 9, None, None),
        ("Ryga", 6, 2, "RG"),
        ("Test", 5, 1, None),
        ("Vilnius", 0, 0, "VLN"),
        ("Vilnius", 1, 0, "vln"),
        ("Vilnius", 2, 0, "V"),
        ("Vilnius", 4, 0, None),
        (None, 3, 0, None),
        (None, None, 2, None),
    ]


def test_getall_distinct(context, rc, tmp_path):
    with create_sqlite_db(
        {"cities": [sa.Column("name", sa.Text), sa.Column("country", sa.Text), sa.Column("id", sa.Integer)]}
    ) as db:
        db.write(
            "cities",
            [
                {"name": "Vilnius", "country": "Lietuva", "id": 0},
                {"name": "Kaunas", "country": "Lietuva", "id": 0},
                {"name": "Siauliai", "country": "Lietuva", "id": 1},
            ],
        )
        create_tabular_manifest(
            context,
            tmp_path / "manifest.csv",
            striptable("""
        id | d | r | b | m | property         | source  | type    | ref      | access | prepare    | level
           | external/distinct                |         |         |          |        |            |
           |   |   |                          |         |         |          |        |            |
           |   |   |   | City                 | cities  |         | name     | open   |            |
           |   |   |   |   | name             | name    | string  |          |        |            |
           |   |   |   |   | country          | country | ref     | Country  |        |            | 3
           |   |   |   | Country              | cities  |         | name     | open   |            |
           |   |   |   |   | name             | country | string  |          |        |            |
           |   |   |   |   | id               | id      | integer |          |        |            |     
           |   |   |   | CountryDistinct      | cities  |         | name     | open   | distinct() |
           |   |   |   |   | name             | country | string  |          |        |            |
           |   |   |   |   | id               | id      | integer |          |        |            |        
           |   |   |   | CountryMultiDistinct | cities  |         | name, id | open   | distinct() |
           |   |   |   |   | name             | country | string  |          |        |            |
           |   |   |   |   | id               | id      | integer |          |        |            |       
           |   |   |   | CountryAllDistinct   | cities  |         |          | open   | distinct() |
           |   |   |   |   | name             | country | string  |          |        |            |
           |   |   |   |   | id               | id      | integer |          |        |            |   
        """),
        )

        app = create_client(rc, tmp_path, db)
        resp = app.get("/external/distinct/City")
        assert listdata(resp, "name", "country") == [
            ("Kaunas", {"name": "Lietuva"}),
            ("Siauliai", {"name": "Lietuva"}),
            ("Vilnius", {"name": "Lietuva"}),
        ]

        resp = app.get("/external/distinct/Country")
        assert listdata(resp, "name", "id") == [("Lietuva", 0), ("Lietuva", 0), ("Lietuva", 1)]

        resp = app.get("/external/distinct/CountryDistinct")
        assert listdata(resp, "name", "id") == [
            ("Lietuva", 0),
        ]

        resp = app.get("/external/distinct/CountryMultiDistinct")
        assert listdata(resp, "name", "id") == [("Lietuva", 0), ("Lietuva", 1)]

        resp = app.get("/external/distinct/CountryAllDistinct")
        assert listdata(resp, "name", "id") == [("Lietuva", 0), ("Lietuva", 1)]


def test_get_one(context, rc, tmp_path):
    with create_sqlite_db({"cities": [sa.Column("name", sa.Text), sa.Column("id", sa.Integer)]}) as db:
        db.write(
            "cities",
            [
                {"name": "Vilnius", "id": 0},
                {"name": "Kaunas", "id": 1},
            ],
        )
        create_tabular_manifest(
            context,
            tmp_path / "manifest.csv",
            striptable("""
        id | d | r | b | m | property     | type    | ref | level | source  | access
           | example                      |         |     |       |         |
           |   |   |   | City             |         | id  |       | cities  |
           |   |   |   |   | id           | integer |     | 4     | id      | open
           |   |   |   |   | name         | string  |     | 4     | name    | open
        """),
        )
        app = create_client(rc, tmp_path, db)
        response = app.get("/example/City")
        response_json = response.json()

        _id = response_json["_data"][0]["_id"]
        getone_response = app.get(f"/example/City/{_id}")
        result = getone_response.json()
        assert result == {"_id": _id, "_type": "example/City", "id": 0, "name": "Vilnius"}


def test_get_one_compound_pk(context, rc, tmp_path):
    with create_sqlite_db(
        {"cities": [sa.Column("name", sa.Text), sa.Column("id", sa.Integer), sa.Column("code", sa.Integer)]}
    ) as db:
        db.write(
            "cities",
            [
                {"name": "Vilnius", "id": 4, "code": "city"},
                {"name": "Kaunas", "id": 2, "code": "city"},
            ],
        )
        create_tabular_manifest(
            context,
            tmp_path / "manifest.csv",
            striptable("""
        id | d | r | b | m | property     | type    | ref       | level | source  | access
           | example                      |         |           |       |         |
           |   |   |   | City             |         | id, code  |       | cities  |
           |   |   |   |   | id           | integer |           | 4     | id      | open
           |   |   |   |   | name         | string  |           | 4     | name    | open
           |   |   |   |   | code         | string  |           | 4     | code    | open
        """),
        )
        app = create_client(rc, tmp_path, db)
        response = app.get("/example/City")
        response_json = response.json()
        _id = response_json["_data"][0]["_id"]
        getone_response = app.get(f"/example/City/{_id}")
        result = getone_response.json()
        assert result == {"_id": _id, "_type": "example/City", "code": "city", "id": 2, "name": "Kaunas"}


def test_getall_geometry_manifest_flip_select(context, rc, tmp_path, geodb_geometry):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property     | type              | ref | level | source  | access | prepare
       | example                      |                   |     |       |         |        |
       |   |   |   | City             |                   | id  |       | cities  |        |
       |   |   |   |   | id           | integer           |     | 4     | id      | open   |
       |   |   |   |   | name         | string            |     | 4     | name    | open   |
       |   |   |   |   | poly         | geometry(polygon) |     | 4     | poly    | open   | flip()
       |   |   |   |   | geo_lt       | geometry(3346)    |     | 4     | geo_lt  | open   | flip()
    """),
    )
    app = create_client(rc, tmp_path, geodb_geometry)
    resp = app.get("/example/City?select(id, name, poly, geo_lt)")
    assert resp.status_code == 200
    assert listdata(resp, "id", "name", "poly", "geo_lt", full=True) == [
        {
            "id": 0,
            "name": "Vilnius",
            "poly": "POLYGON ((50 80, 50 50, 80 50, 80 80, 50 80))",
            "geo_lt": "POINT (200000 5980000)",
        },
    ]


def test_getall_geometry_manifest_flip(context, rc, tmp_path, geodb_geometry):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property     | type              | ref | level | source  | access | prepare
       | example                      |                   |     |       |         |        |
       |   |   |   | City             |                   | id  |       | cities  |        |
       |   |   |   |   | id           | integer           |     | 4     | id      | open   |
       |   |   |   |   | name         | string            |     | 4     | name    | open   |
       |   |   |   |   | poly         | geometry(polygon) |     | 4     | poly    | open   | flip()
       |   |   |   |   | geo_lt       | geometry(3346)    |     | 4     | geo_lt  | open   | flip()
    """),
    )
    app = create_client(rc, tmp_path, geodb_geometry)
    resp = app.get("/example/City")
    assert resp.status_code == 200
    assert listdata(resp, "id", "name", "poly", "geo_lt", full=True) == [
        {
            "id": 0,
            "name": "Vilnius",
            "poly": "POLYGON ((50 80, 50 50, 80 50, 80 80, 50 80))",
            "geo_lt": "POINT (200000 5980000)",
        },
    ]


def test_getall_geometry_select_flip(context, rc, tmp_path, geodb_geometry):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property     | type              | ref | level | source  | access | prepare
       | example                      |                   |     |       |         |        |
       |   |   |   | City             |                   | id  |       | cities  |        |
       |   |   |   |   | id           | integer           |     | 4     | id      | open   |
       |   |   |   |   | name         | string            |     | 4     | name    | open   |
       |   |   |   |   | poly         | geometry(polygon) |     | 4     | poly    | open   |
       |   |   |   |   | geo_lt       | geometry(3346)    |     | 4     | geo_lt  | open   |
    """),
    )
    app = create_client(rc, tmp_path, geodb_geometry)
    resp = app.get("/example/City?select(id,name,flip(poly),flip(geo_lt))")
    assert resp.status_code == 200
    assert listdata(resp, "id", "name", "flip(poly)", "flip(geo_lt)", full=True) == [
        {
            "id": 0,
            "name": "Vilnius",
            "flip(poly)": "POLYGON ((50 80, 50 50, 80 50, 80 80, 50 80))",
            "flip(geo_lt)": "POINT (200000 5980000)",
        },
    ]


def test_getall_geometry_select_and_manifest_flip(context, rc, tmp_path, geodb_geometry):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property     | type              | ref | level | source  | access | prepare
       | example                      |                   |     |       |         |        |
       |   |   |   | City             |                   | id  |       | cities  |        |
       |   |   |   |   | id           | integer           |     | 4     | id      | open   |
       |   |   |   |   | name         | string            |     | 4     | name    | open   |
       |   |   |   |   | poly         | geometry(polygon) |     | 4     | poly    | open   | flip()
       |   |   |   |   | geo_lt       | geometry(3346)    |     | 4     | geo_lt  | open   | flip()
    """),
    )
    app = create_client(rc, tmp_path, geodb_geometry)
    resp = app.get("/example/City?select(poly,flip(poly),flip(flip(poly)),geo_lt,flip(geo_lt),flip(flip(geo_lt)))")
    assert resp.status_code == 200
    assert listdata(resp, full=True) == [
        {
            "poly": "POLYGON ((50 80, 50 50, 80 50, 80 80, 50 80))",
            "flip(poly)": "POLYGON ((80 50, 50 50, 50 80, 80 80, 80 50))",
            "flip(flip(poly))": "POLYGON ((50 80, 50 50, 80 50, 80 80, 50 80))",
            "geo_lt": "POINT (200000 5980000)",
            "flip(geo_lt)": "POINT (5980000 200000)",
            "flip(flip(geo_lt))": "POINT (200000 5980000)",
        },
    ]


def test_getall_ref_geometry_manifest_flip(context, rc, tmp_path, geodb_geometry):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property         | type              | ref      | level | source    | access | prepare
       | example                          |                   |          |       |           |        |
       |   |   |   | CityMeta             |                   | id       |       | geodata   |        |
       |   |   |   |   | id               | integer           |          | 4     | id        | open   |
       |   |   |   |   | geo              | geometry(3346)    |          | 4     | geo_point | open   |
       |   |   |   |   | geo_flipped      | geometry(3346)    |          | 4     | geo_point | open   | flip()
       |   |   |   | City                 |                   | id       |       | cities    |        |
       |   |   |   |   | id               | integer           |          | 4     | id        | open   |
       |   |   |   |   | name             | string            |          | 4     | name      | open   |
       |   |   |   |   | meta             | ref               | CityMeta | 4     | geo_id    | open   |
       |   |   |   |   | meta.poly        | geometry(polygon) |          | 4     | poly      | open   |
       |   |   |   |   | meta.geo_flipped |                   |          | 4     |           | open   |
    """),
    )
    app = create_client(rc, tmp_path, geodb_geometry)
    resp = app.get("/example/City")
    assert resp.status_code == 200
    assert listdata(resp, "id", "name", "meta.poly", "meta.geo_flipped", full=True) == [
        {
            "id": 0,
            "name": "Vilnius",
            "meta.poly": "POLYGON ((80 50, 50 50, 50 80, 80 80, 80 50))",
            "meta.geo_flipped": "POINT (200000 5980000)",
        },
    ]


def test_getall_ref_geometry_select_and_manifest_flip(context, rc, tmp_path, geodb_geometry):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    id | d | r | b | m | property         | type              | ref      | level | source    | access | prepare
       | example                          |                   |          |       |           |        |
       |   |   |   | CityMeta             |                   | id       |       | geodata   |        |
       |   |   |   |   | id               | integer           |          | 4     | id        | open   |
       |   |   |   |   | geo              | geometry(3346)    |          | 4     | geo_point | open   |
       |   |   |   |   | geo_flipped      | geometry(3346)    |          | 4     | geo_point | open   | flip()
       |   |   |   | City                 |                   | id       |       | cities    |        |
       |   |   |   |   | id               | integer           |          | 4     | id        | open   |
       |   |   |   |   | name             | string            |          | 4     | name      | open   |
       |   |   |   |   | meta             | ref               | CityMeta | 4     | geo_id    | open   |
       |   |   |   |   | meta.poly        | geometry(polygon) |          | 4     | poly      | open   |
       |   |   |   |   | meta.geo_flipped |                   |          | 4     |           | open   |
    """),
    )
    app = create_client(rc, tmp_path, geodb_geometry)
    resp = app.get(
        "/example/City?select("
        "meta.poly,"
        "flip(meta.poly),"
        "flip(flip(meta.poly)),"
        "meta.geo_flipped,"
        "flip(meta.geo_flipped),"
        "flip(flip(meta.geo_flipped)),"
        "meta.geo,"
        "flip(meta.geo),"
        "flip(flip(meta.geo))"
        ")"
    )
    assert resp.status_code == 200
    assert listdata(resp, full=True) == [
        {
            "meta.poly": "POLYGON ((80 50, 50 50, 50 80, 80 80, 80 50))",
            "flip(meta.poly)": "POLYGON ((50 80, 50 50, 80 50, 80 80, 50 80))",
            "flip(flip(meta.poly))": "POLYGON ((80 50, 50 50, 50 80, 80 80, 80 50))",
            "meta.geo_flipped": "POINT (200000 5980000)",
            "flip(meta.geo_flipped)": "POINT (5980000 200000)",
            "flip(flip(meta.geo_flipped))": "POINT (200000 5980000)",
            "meta.geo": "POINT (5980000 200000)",
            "flip(meta.geo)": "POINT (200000 5980000)",
            "flip(flip(meta.geo))": "POINT (5980000 200000)",
        },
    ]


def test_getall_array_intermediate_single_pkey_sqlite(context, rc, tmp_path, geodb_array):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property    | type       | ref             | source           | level   | access
    example                     |            |                 |                  |         |        
      | db                      |            | sqlite          |                  |         |        
      |                         |            |                 |                  |         |
      |   |   | Language        |            | id              | language         |         |
      |   |   |   | id          | string     |                 | id               |         | open
      |   |   |   | name        | string     |                 | name             |         | open
      |   |                     |            |                 |                  |         |
      |   |   | Country         |            | id              | country          |         |
      |   |   |   | id          | string     |                 | id               |         | open
      |   |   |   | name        | string     |                 | name             |         | open
      |   |   |   | languages   | array      | CountryLanguage |                  |         | open
      |   |   |   | languages[] | ref        | Language        |                  |         | open
      |   |                     |            |                 |                  |         |
      |   |   | CountryLanguage |            |                 | country_language |         |
      |   |   |   | country     | ref        | Country         | country_id       |         | open
      |   |   |   | language    | ref        | Language        | language_id      |         | open
    """),
    )
    app = create_client(rc, tmp_path, geodb_array, mode="external")
    resp = app.get("/example/Language")
    lang_data = resp.json()["_data"]
    lang_mapping = {lang["id"]: lang for lang in lang_data}
    resp = app.get("/example/Country")

    assert resp.status_code == 200
    assert listdata(resp, "id", "name", "languages", full=True) == [
        {"id": 0, "name": "United Kingdoms", "languages": [{"_id": lang_mapping[0]["_id"]}]},
        {"id": 1, "name": "Lithuania", "languages": [{"_id": lang_mapping[0]["_id"]}, {"_id": lang_mapping[1]["_id"]}]},
        {"id": 2, "name": "Poland", "languages": [{"_id": lang_mapping[0]["_id"]}, {"_id": lang_mapping[2]["_id"]}]},
    ]


def test_getall_array_intermediate_multi_pkey_sqlite(context, rc, tmp_path, geodb_array):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property     | type       | ref             | source           | level   | access | prepare  
    example                      |            |                 |                  |         |        |        
      | db                       |            | sqlite          |                  |         |        |        
      |                          |            |                 |                  |         |        |
      |   |   | Language         |            | id              | language         |         |        |
      |   |   |   | id           | integer    |                 | id               |         | open   |    
      |   |   |   | name         | string     |                 | name             |         | open   |    
      |   |                      |            |                 |                  |         |        |
      |   |   | Country          |            | id, name        | country          |         |        |
      |   |   |   | id           | integer    |                 | id               |         | open   |    
      |   |   |   | name         | string     |                 | name             |         | open   |    
      |   |   |   | languages    | array      | CountryLanguage |                  |         | open   |    
      |   |   |   | languages[]  | ref        | Language        |                  |         | open   |    
      |   |                      |            |                 |                  |         |        |
      |   |   | CountryLanguage  |            |                 | country_language |         |        |
      |   |   |   | country_id   | integer    |                 | country_id       |         | open   |    
      |   |   |   | country_name | string     |                 | country_name     |         | open   |    
      |   |   |   | country      | ref        | Country         |                  |         | open   | country_id, country_name   
      |   |   |   | language     | ref        | Language        | language_id      |         | open   |    
    """),
    )
    app = create_client(rc, tmp_path, geodb_array, mode="external")
    resp = app.get("/example/Language")
    lang_data = resp.json()["_data"]
    lang_mapping = {lang["id"]: lang for lang in lang_data}
    resp = app.get("/example/Country")

    assert resp.status_code == 200
    assert listdata(resp, "id", "name", "languages", full=True) == [
        {"id": 0, "name": "United Kingdoms", "languages": [{"_id": lang_mapping[0]["_id"]}]},
        {"id": 1, "name": "Lithuania", "languages": [{"_id": lang_mapping[0]["_id"]}, {"_id": lang_mapping[1]["_id"]}]},
        {"id": 2, "name": "Poland", "languages": [{"_id": lang_mapping[0]["_id"]}, {"_id": lang_mapping[2]["_id"]}]},
    ]


def test_getall_array_intermediate_ref_single_pkey_sqlite(context, rc, tmp_path, geodb_array):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property      | type       | ref             | source           | level   | access | prepare  
    example                       |            |                 |                  |         |        |        
      | db                        |            | sqlite          |                  |         |        |        
      |                           |            |                 |                  |         |        |
      |   |   | Language          |            | id              | language         |         |        |
      |   |   |   | id            | integer    |                 | id               |         | open   |    
      |   |   |   | name          | string     |                 | name             |         | open   |    
      |   |                       |            |                 |                  |         |        |
      |   |   | Country           |            | id              | country          |         |        |
      |   |   |   | id            | integer    |                 | id               |         | open   |    
      |   |   |   | name          | string     |                 | name             |         | open   |    
      |   |   |   | languages     | array      | CountryLanguage |                  |         | open   |    
      |   |   |   | languages[]   | ref        | Language        |                  |         | open   |    
      |   |                       |            |                 |                  |         |        |
      |   |   | CountryLanguage   |            |                 | country_language |         |        |  
      |   |   |   | country       | ref        | Country         | country_id       |         | open   |
      |   |   |   | language_id   | integer    |                 | language_id      |         | open   |    
      |   |   |   | language      | ref        | Language        |                  |         | open   | language_id   
    """),
    )
    app = create_client(rc, tmp_path, geodb_array, mode="external")
    resp = app.get("/example/Language")
    lang_data = resp.json()["_data"]
    lang_mapping = {lang["id"]: lang for lang in lang_data}
    resp = app.get("/example/Country")

    assert resp.status_code == 200
    assert listdata(resp, "id", "name", "languages", full=True) == [
        {"id": 0, "name": "United Kingdoms", "languages": [{"_id": lang_mapping[0]["_id"]}]},
        {"id": 1, "name": "Lithuania", "languages": [{"_id": lang_mapping[0]["_id"]}, {"_id": lang_mapping[1]["_id"]}]},
        {"id": 2, "name": "Poland", "languages": [{"_id": lang_mapping[0]["_id"]}, {"_id": lang_mapping[2]["_id"]}]},
    ]


def test_getall_array_intermediate_ref_multi_pkey_sqlite(context, rc, tmp_path, geodb_array):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property      | type       | ref             | source           | level   | access | prepare  
    example                       |            |                 |                  |         |        |        
      | db                        |            | sqlite          |                  |         |        |        
      |                           |            |                 |                  |         |        |
      |   |   | Language          |            | id, name        | language         |         |        |
      |   |   |   | id            | integer    |                 | id               |         | open   |    
      |   |   |   | name          | string     |                 | name             |         | open   |    
      |   |                       |            |                 |                  |         |        |
      |   |   | Country           |            | id              | country          |         |        |
      |   |   |   | id            | integer    |                 | id               |         | open   |    
      |   |   |   | name          | string     |                 | name             |         | open   |    
      |   |   |   | languages     | array      | CountryLanguage |                  |         | open   |    
      |   |   |   | languages[]   | ref        | Language        |                  |         | open   |    
      |   |                       |            |                 |                  |         |        |
      |   |   | CountryLanguage   |            |                 | country_language |         |        |  
      |   |   |   | country       | ref        | Country         | country_id       |         | open   |
      |   |   |   | language_id   | integer    |                 | language_id      |         | open   |    
      |   |   |   | language_name | string     |                 | language_name    |         | open   |  
      |   |   |   | language      | ref        | Language        |                  |         | open   | language_id, language_name   
    """),
    )
    app = create_client(rc, tmp_path, geodb_array, mode="external")
    resp = app.get("/example/Language")
    lang_data = resp.json()["_data"]
    lang_mapping = {lang["id"]: lang for lang in lang_data}
    resp = app.get("/example/Country")

    assert resp.status_code == 200
    assert listdata(resp, "id", "name", "languages", full=True) == [
        {"id": 0, "name": "United Kingdoms", "languages": [{"_id": lang_mapping[0]["_id"]}]},
        {"id": 1, "name": "Lithuania", "languages": [{"_id": lang_mapping[0]["_id"]}, {"_id": lang_mapping[1]["_id"]}]},
        {"id": 2, "name": "Poland", "languages": [{"_id": lang_mapping[0]["_id"]}, {"_id": lang_mapping[2]["_id"]}]},
    ]


def test_getall_array_intermediate_ref_level_3_sqlite(context, rc, tmp_path, geodb_array):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property      | type       | ref             | source           | level   | access | prepare  
    example                       |            |                 |                  |         |        |        
      | db                        |            | sqlite          |                  |         |        |        
      |                           |            |                 |                  |         |        |
      |   |   | Language          |            | id, name        | language         |         |        |
      |   |   |   | id            | integer    |                 | id               |         | open   |    
      |   |   |   | name          | string     |                 | name             |         | open   |    
      |   |                       |            |                 |                  |         |        |
      |   |   | Country           |            | id              | country          |         |        |
      |   |   |   | id            | integer    |                 | id               |         | open   |    
      |   |   |   | name          | string     |                 | name             |         | open   |    
      |   |   |   | languages     | array      | CountryLanguage |                  |         | open   |    
      |   |   |   | languages[]   | ref        | Language        |                  | 3       | open   |    
      |   |                       |            |                 |                  |         |        |
      |   |   | CountryLanguage   |            |                 | country_language |         |        |  
      |   |   |   | country       | ref        | Country         | country_id       |         | open   |
      |   |   |   | language_id   | integer    |                 | language_id      |         | open   |    
      |   |   |   | language_name | string     |                 | language_name    |         | open   |  
      |   |   |   | language      | ref        | Language        |                  |         | open   | language_id, language_name   
    """),
    )
    app = create_client(rc, tmp_path, geodb_array, mode="external")
    resp = app.get("/example/Country")

    assert resp.status_code == 200
    assert listdata(resp, "id", "name", "languages", full=True) == [
        {"id": 0, "name": "United Kingdoms", "languages": [{"id": 0, "name": "English"}]},
        {"id": 1, "name": "Lithuania", "languages": [{"id": 0, "name": "English"}, {"id": 1, "name": "Lithuanian"}]},
        {"id": 2, "name": "Poland", "languages": [{"id": 0, "name": "English"}, {"id": 2, "name": "Polish"}]},
    ]


def test_getall_array_intermediate_ref_refprop_sqlite(context, rc, tmp_path, geodb_array):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property      | type       | ref             | source           | level   | access | prepare  
    example                       |            |                 |                  |         |        |        
      | db                        |            | sqlite          |                  |         |        |        
      |                           |            |                 |                  |         |        |
      |   |   | Language          |            | id, name        | language         |         |        |
      |   |   |   | id            | integer    |                 | id               |         | open   |    
      |   |   |   | name          | string     |                 | name             |         | open   |    
      |   |                       |            |                 |                  |         |        |
      |   |   | Country           |            | id              | country          |         |        |
      |   |   |   | id            | integer    |                 | id               |         | open   |    
      |   |   |   | name          | string     |                 | name             |         | open   |    
      |   |   |   | languages     | array      | CountryLanguage |                  |         | open   |    
      |   |   |   | languages[]   | ref        | Language[id]    |                  |         | open   |    
      |   |                       |            |                 |                  |         |        |
      |   |   | CountryLanguage   |            |                 | country_language |         |        |  
      |   |   |   | country       | ref        | Country         | country_id       |         | open   |
      |   |   |   | language_id   | integer    |                 | language_id      |         | open   |    
      |   |   |   | language      | ref        | Language[id]    |                  |         | open   | language_id   
    """),
    )
    app = create_client(rc, tmp_path, geodb_array, mode="external")
    resp = app.get("/example/Language")
    lang_data = resp.json()["_data"]
    lang_mapping = {lang["id"]: lang for lang in lang_data}
    resp = app.get("/example/Country")

    assert resp.status_code == 200
    assert listdata(resp, "id", "name", "languages", full=True) == [
        {"id": 0, "name": "United Kingdoms", "languages": [{"_id": lang_mapping[0]["_id"]}]},
        {"id": 1, "name": "Lithuania", "languages": [{"_id": lang_mapping[0]["_id"]}, {"_id": lang_mapping[1]["_id"]}]},
        {"id": 2, "name": "Poland", "languages": [{"_id": lang_mapping[0]["_id"]}, {"_id": lang_mapping[2]["_id"]}]},
    ]


def test_getall_array_prepare_split(context, rc, tmp_path, geodb_array):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property      | type       | ref             | source           | level   | access | prepare  
    example                       |            |                 |                  |         |        |        
      | db                        |            | sqlite          |                  |         |        |        
      |                           |            |                 |                  |         |        |
      |   |   | Language          |            | id              | language         |         |        |
      |   |   |   | id            | integer    |                 | id               |         | open   |    
      |   |   |   | name          | string     |                 | name             |         | open   |    
      |   |                       |            |                 |                  |         |        |
      |   |   | Country           |            | id              | country          |         |        |
      |   |   |   | id            | integer    |                 | id               |         | open   |    
      |   |   |   | name          | string     |                 | name             |         | open   |        
      |   |   |   | languages     | array      |                 | languages        |         | open   | split(',')    
      |   |   |   | languages[]   | ref        | Language[name]  |                  |         | open   |    
    """),
    )
    app = create_client(rc, tmp_path, geodb_array, mode="external")
    resp = app.get("/example/Language")
    lang_data = resp.json()["_data"]
    lang_mapping = {lang["id"]: lang for lang in lang_data}
    resp = app.get("/example/Country")

    assert resp.status_code == 200
    assert listdata(resp, "id", "name", "languages", full=True) == [
        {"id": 0, "name": "United Kingdoms", "languages": [{"_id": lang_mapping[0]["_id"]}]},
        {"id": 1, "name": "Lithuania", "languages": [{"_id": lang_mapping[0]["_id"]}, {"_id": lang_mapping[1]["_id"]}]},
        {"id": 2, "name": "Poland", "languages": [{"_id": lang_mapping[0]["_id"]}, {"_id": lang_mapping[2]["_id"]}]},
    ]


@pytest.mark.parametrize("ref_level", [3, 4])
def test_array_ref_key_count_missmatch(ref_level, context, rc, tmp_path, geodb_array):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(f"""
    d | r | b | m | property      | type       | ref      | source    | level       | access | prepare  
    example                       |            |          |           |             |        |        
      | db                        |            | sqlite   |           |             |        |        
      |                           |            |          |           |             |        |
      |   |   | Language          |            | id, name | language  |             |        |
      |   |   |   | id            | integer    |          | id        |             | open   |    
      |   |   |   | name          | string     |          | name      |             | open   |    
      |   |                       |            |          |           |             |        |
      |   |   | Country           |            | id       | country   |             |        |
      |   |   |   | id            | integer    |          | id        |             | open   |    
      |   |   |   | name          | string     |          | name      |             | open   |        
      |   |   |   | languages     | array      |          | languages |             | open   | split(',')    
      |   |   |   | languages[]   | ref        | Language |           | {ref_level} | open   |    
    """),
    )
    app = create_client(rc, tmp_path, geodb_array, mode="external")
    app.get("/example/Language")
    resp = app.get("/example/Country")
    assert get_error_codes(resp.json()) == ["GivenValueCountMissmatch"]
