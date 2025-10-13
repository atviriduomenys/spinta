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
