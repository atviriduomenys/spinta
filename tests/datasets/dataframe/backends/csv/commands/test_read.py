import pytest
from fsspec import AbstractFileSystem
from fsspec.implementations.memory import MemoryFileSystem

from spinta.core.config import RawConfig
from spinta.core.enums import Mode
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
        d | r | b | m | property | type     | ref  | source              | access
        example/csv              |          |      |                     |
          | csv                  | dask/csv |      | memory://cities.csv |
          |   |   | City         |          | name |                     |
          |   |   |   | name     | string   |      | miestas             | open
          |   |   |   | country  | string   |      | šalis               | open
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
        d | r | b | m | property | type     | source              | access
        example/csv              |          |                     |
          | csv                  | dask/csv | memory://cities.csv |
          |   |   | City         |          |                     |
          |   |   |   | name     | string   | miestas             | open
          |   |   |   | country  | string   | salis               | open
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
        d | r | b | m | property | type     | ref                            | source                 | access
        example/csv/countries    |          |                                |                        |
          | csv                  | dask/csv |                                | memory://countries.csv |
          |   |   | Country      |          | code                           |                        |
          |   |   |   | code     | string   |                                | kodas                  | open
          |   |   |   | name     | string   |                                | pavadinimas            | open
        example/csv/cities       |          |                                |                        |
          | csv                  | dask/csv |                                | memory://cities.csv    |
          |   |   | City         |          | name                           |                        |
          |   |   |   | name     | string   |                                | miestas                | open
          |   |   |   | country  | ref      | /example/csv/countries/Country | šalis                  | open
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
        d | r | b | m | property | type     | ref     | source                 | access
        example/csv/countries    |          |         |                        |
          | csv0                 | dask/csv |         | memory://countries.csv |
          |   |   | Country      |          | code    |                        |
          |   |   |   | code     | string   |         | kodas                  | open
          |   |   |   | name     | string   |         | pavadinimas            | open
          |   |   |   | id       | integer  |         | id                     | open
                                 |          |         |                        |
          | csv1                 | dask/csv |         | memory://cities.csv    |
          |   |   | City         |          | miestas |                        |
          |   |   |   | miestas  | string   |         | miestas                | open
          |   |   |   | kodas    | string   |         | šalis                  | open
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


def test_text_read_from_external_source(rc: RawConfig, fs: AbstractFileSystem):
    fs.pipe("countries.csv", ("šalislt\nlietuva\n").encode("utf-8"))

    context, manifest = prepare_manifest(
        rc,
        """
        d | r | b | m | property    | type     | ref  | source                 | access
        example/countries           |          |      |                        |
          | csv                     | dask/csv |      | memory://countries.csv |
          |   |   | Country         |          | name |                        |
          |   |   |   | name@lt     | string   |      | šalislt                | open
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/countries", ["getall"])

    resp = app.get("example/countries/Country")
    assert listdata(resp, sort=False) == ["lietuva"]
