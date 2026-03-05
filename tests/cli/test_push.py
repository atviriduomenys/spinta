import datetime
import logging
import os
import re

import pytest
import sqlalchemy as sa
import sqlalchemy_utils as su
from requests.exceptions import ReadTimeout, ConnectTimeout

from spinta.core.config import RawConfig
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.client import create_client, create_rc, configure_remote_server
from spinta.testing.data import listdata
from spinta.testing.datasets import create_sqlite_db, Sqlite
from spinta.testing.push import compare_push_state_rows
from spinta.testing.tabular import create_tabular_manifest


@pytest.fixture(scope="function")
def push_state_geodb():
    with create_sqlite_db(
        {
            "COUNTRY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("CODE", sa.Text),
                sa.Column("NAME", sa.Text),
            ],
        }
    ) as db:
        db.write(
            "COUNTRY",
            [
                {"ID": 0, "CODE": "LT", "NAME": "LITHUANIA"},
                {"ID": 1, "CODE": "LV", "NAME": "LATVIA"},
                {"ID": 2, "CODE": "PL", "NAME": "POLAND"},
            ],
        )
        yield db


@pytest.fixture(scope="function")
def multi_type_geodb():
    with create_sqlite_db(
        {
            "TEST": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
                sa.Column("NUMBER", sa.Float),
                sa.Column("URL", sa.Text),
                sa.Column("DATE", sa.Date),
                sa.Column("TIME", sa.Time),
                sa.Column("DATETIME", sa.DateTime),
            ],
        }
    ) as db:
        db.write(
            "TEST",
            [
                {
                    "ID": 0,
                    "NAME": "LT",
                    "NUMBER": 0.1,
                    "URL": "https://www.example.com/LT",
                    "DATE": datetime.date(2024, 2, 1),
                    "TIME": datetime.time(12, 10, 20),
                    "DATETIME": datetime.datetime(2024, 2, 1, 12, 10, 20),
                },
                {
                    "ID": 1,
                    "NAME": "LV",
                    "NUMBER": 1.2,
                    "URL": "https://www.example.com/LV",
                    "DATE": datetime.date(2024, 2, 2),
                    "TIME": datetime.time(12, 20, 20),
                    "DATETIME": datetime.datetime(2024, 2, 2, 12, 20, 20),
                },
                {
                    "ID": 2,
                    "NAME": "PL",
                    "NUMBER": 2.3,
                    "URL": "https://www.example.com/PL",
                    "DATE": datetime.date(2024, 2, 3),
                    "TIME": datetime.time(12, 30, 20),
                    "DATETIME": datetime.datetime(2024, 2, 3, 12, 30, 20),
                },
            ],
        )
        yield db


@pytest.fixture(scope="function")
def base_geodb():
    with create_sqlite_db(
        {
            "city": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("code", sa.Text),
                sa.Column("name", sa.Text),
                sa.Column("location", sa.Text),
            ],
            "location": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("name", sa.Text),
                sa.Column("code", sa.Text),
            ],
        }
    ) as db:
        db.write(
            "location",
            [
                {"code": "lt", "name": "Vilnius", "id": 1},
                {"code": "lv", "name": "Ryga", "id": 2},
                {"code": "ee", "name": "Talin", "id": 3},
            ],
        )
        db.write(
            "city",
            [
                {"id": 2, "name": "Ryga", "code": "lv", "location": "Latvia"},
            ],
        )
        yield db


@pytest.fixture(scope="function")
def text_geodb():
    with create_sqlite_db(
        {
            "city": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("name", sa.Text),
                sa.Column("name_lt", sa.Text),
                sa.Column("name_pl", sa.Text),
                sa.Column("country", sa.Integer),
            ],
            "country": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("name_lt", sa.Text),
                sa.Column("name_en", sa.Text),
            ],
        }
    ) as db:
        db.write(
            "country",
            [
                {"name_lt": "Lietuva", "name_en": None, "id": 1},
                {"name_lt": None, "name_en": "Latvia", "id": 2},
                {"name_lt": "Lenkija", "name_en": "Poland", "id": 3},
            ],
        )
        db.write(
            "city",
            [
                {"id": 1, "name": "VLN", "name_lt": "Vilnius", "name_pl": "Vilna", "country": 1},
            ],
        )
        yield db


@pytest.fixture(scope="function")
def array_geodb():
    with create_sqlite_db(
        {
            "country": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("name", sa.Text),
                sa.Column("languages", sa.Text),
            ],
            "language": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("code", sa.Text),
                sa.Column("name", sa.Text),
            ],
            "countrylanguage": [
                sa.Column("country_id", sa.Integer),
                sa.Column("language_id", sa.Integer),
            ],
        }
    ) as db:
        db.write(
            "language",
            [
                {"id": 0, "code": "lt", "name": "Lithuanian"},
                {"id": 1, "code": "en", "name": "English"},
                {"id": 2, "code": "pl", "name": "Polish"},
            ],
        )
        db.write(
            "country",
            [
                {"id": 0, "name": "Lithuania", "languages": "lt,en"},
                {"id": 1, "name": "England", "languages": "en"},
                {"id": 2, "name": "Poland", "languages": "en,pl"},
            ],
        )
        db.write(
            "countrylanguage",
            [
                {"country_id": 0, "language_id": 0},
                {"country_id": 0, "language_id": 1},
                {"country_id": 1, "language_id": 1},
                {"country_id": 2, "language_id": 1},
                {"country_id": 2, "language_id": 2},
            ],
        )
        yield db


@pytest.fixture(scope="function")
def geodb():
    with create_sqlite_db(
        {
            "salis": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("kodas", sa.Text),
                sa.Column("pavadinimas", sa.Text),
            ],
            "cities": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("name", sa.Text),
                sa.Column("country", sa.Integer),
            ],
            "nullable": [sa.Column("id", sa.Integer), sa.Column("name", sa.Text), sa.Column("code", sa.Text)],
        }
    ) as db:
        db.write(
            "salis",
            [
                {"kodas": "lt", "pavadinimas": "Lietuva", "id": 1},
                {"kodas": "lv", "pavadinimas": "Latvija", "id": 2},
                {"kodas": "ee", "pavadinimas": "Estija", "id": 3},
            ],
        )
        db.write(
            "cities",
            [
                {"name": "Vilnius", "country": 2},
            ],
        )
        db.write(
            "nullable",
            [
                {"id": 0, "name": "Test", "code": "0"},
                {"id": 0, "name": "Test", "code": "1"},
                {"id": 0, "name": "Test0", "code": None},
                {"id": 0, "name": None, "code": "0"},
                {"id": 0, "name": None, "code": None},
                {"id": 1, "name": "Test", "code": None},
                {"id": 1, "name": None, "code": None},
                {"id": None, "name": "Test", "code": None},
                {"id": None, "name": "Test", "code": "0"},
            ],
        )
        yield db


@pytest.fixture(scope="module")
def errordb():
    with create_sqlite_db(
        {
            "salis": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("kodas", sa.Text),
                sa.Column("pavadinimas", sa.Text),
            ]
        }
    ) as db:
        db.write(
            "salis",
            [
                {"kodas": "lt", "pavadinimas": "Lietuva"},
                {"kodas": "lt", "pavadinimas": "Latvija"},
                {"kodas": "lt", "pavadinimas": "Estija"},
            ],
        )
        yield db


def test_push_with_progress_bar(context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request):
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
    assert re.search(r"Count rows:\s*0%", result.stderr)
    assert re.search(r"PUSH:\s*0%.*0/3", result.stderr)
    assert re.search(r"PUSH:\s*100%.*3/3", result.stderr)


def test_push_without_progress_bar(context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request):
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
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    assert result.stderr == ""


def test_push_error_exit_code(context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, errordb, request):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property| type    | ref     | source       | access
    datasets/gov/example    |         |         |              |
      | data                | sql     |         |              |
      |   |                 |         |         |              |
      |   |   | Country     |         | code    | salis        |
      |   |   |   | code    | string unique|         | kodas        | open
      |   |   |   | name    | string  |         | pavadinimas  | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, errordb)

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
        fail=False,
    )
    assert result.exit_code == 1


def test_push_error_exit_code_with_bad_resource(
    context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, request
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(f"""
    d | r | b | m | property| type    | ref     | source       | access
    datasets/gov/example    |         |         |              |
      | data                | sql     |         | sqlite:///{tmp_path}/bad.db |
      |   |                 |         |         |              |
      |   |   | Country     |         | code    | salis        |
      |   |   |   | code    | string  |         | kodas        | open
      |   |   |   | name    | string  |         | pavadinimas  | open
    """),
    )

    localrc = rc.fork(
        {
            "manifests": {
                "default": {
                    "type": "tabular",
                    "path": str(tmp_path / "manifest.csv"),
                    "backend": "sql",
                    "keymap": "default",
                },
            },
            "backends": {
                "sql": {
                    "type": "sql",
                    "dsn": f"sqlite:///{tmp_path}/bad.db",
                },
            },
            # tests/config/clients/3388ea36-4a4f-4821-900a-b574c8829d52.yml
            "default_auth_client": "3388ea36-4a4f-4821-900a-b574c8829d52",
        }
    )

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
        fail=False,
    )
    assert result.exit_code == 1


def test_push_ref_with_level_3(context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type     | ref      | source      | level | access
    level3dataset            |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   | City         |          | id       | cities      | 4     |
      |   |   |   | id       | integer  |          | id          | 4     | open
      |   |   |   | name     | string   |          | name        | 4     | open
      |   |   |   | country  | ref      | Country  | country     | 3     | open
      |   |   |   |          |          |          |             |       |
      |   |   | Country      |          | id       | salis       | 4     |
      |   |   |   | code     | string   |          | kodas       | 4     | open
      |   |   |   | name     | string   |          | pavadinimas | 4     | open
      |   |   |   | id       | integer  |          | id          | 4     | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "level3dataset",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0

    remote.app.authmodel("level3dataset/City", ["getall", "search"])
    resp_city = remote.app.get("level3dataset/City")

    assert resp_city.status_code == 200
    assert listdata(resp_city, "name") == ["Vilnius"]
    assert listdata(resp_city, "id", "name", "country")[0] == (1, "Vilnius", {"id": 2})
    assert "id" in listdata(resp_city, "country")[0].keys()


def test_push_ref_with_level_4(context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type     | ref      | source      | level | access
    level4dataset            |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   | City         |          | id       | cities      | 4     |
      |   |   |   | id       | integer  |          | id          | 4     | open
      |   |   |   | name     | string   |          | name        | 4     | open
      |   |   |   | country  | ref      | Country  | country     | 4     | open
      |   |   |   |          |          |          |             |       |
      |   |   | Country      |          | id       | salis       | 4     |
      |   |   |   | code     | string   |          | kodas       | 4     | open
      |   |   |   | name     | string   |          | pavadinimas | 4     | open
      |   |   |   | id       | integer  |          | id          | 4     | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "level4dataset",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0

    remote.app.authmodel("level4dataset/City", ["getall", "search"])
    resp_city = remote.app.get("level4dataset/City")

    assert resp_city.status_code == 200
    assert listdata(resp_city, "name") == ["Vilnius"]
    assert len(listdata(resp_city, "id", "name", "country")) == 1
    assert "_id" in listdata(resp_city, "country")[0].keys()


def test_push_with_resource_check(context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property    | type   | ref     | source       | access
    datasets/gov/example_res    |        |         |              |
      | data                    | sql    |         |              |
      |   |   | CountryRes      |        | code    | salis        |
      |   |   |   | code        | string |         | kodas        | open
      |   |   |   | name        | string |         | pavadinimas  | open
      |   |                     |        |         |              |
    datasets/gov/example_no_res |        |         |              |
      |   |   | CountryNoRes    |        |         |              |
      |   |   |   | code        | string |         |              | open
      |   |   |   | name        | string |         |              | open
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
            "datasets/gov/example_res",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0

    result = cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "datasets/gov/example_no_res",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0

    remote.app.authmodel("datasets/gov/example_res/CountryRes", ["getall"])
    resp_res = remote.app.get("/datasets/gov/example_res/CountryRes")
    assert len(listdata(resp_res)) == 3

    remote.app.authmodel("datasets/gov/example_no_res/CountryNoRes", ["getall"])
    resp_no_res = remote.app.get("/datasets/gov/example_no_res/CountryNoRes")
    assert len(listdata(resp_no_res)) == 0


def test_push_ref_with_level_no_source(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request
):
    table = """
    d | r | b | m | property | type    | ref                             | source         | level | access
    leveldataset             |         |                                 |                |       |
      | db                   | sql     |                                 |                |       |
      |   |   | City         |         | id                              | cities         | 4     |
      |   |   |   | id       | integer |                                 | id             | 4     | open
      |   |   |   | name     | string  |                                 | name           | 2     | open
      |   |   |   | country  | ref     | /leveldataset/countries/Country | country        | 3     | open
      |   |   |   |          |         |                                 |                |       |
    leveldataset/countries   |         |                                 |                |       |
      |   |   | Country      |         | code                            |                | 4     |
      |   |   |   | code     | string  |                                 |                | 4     | open
      |   |   |   | name     | string  |                                 |                | 2     | open
    """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))

    app = create_client(rc, tmp_path, geodb)
    app.authmodel("leveldataset", ["getall"])
    resp = app.get("leveldataset/City")
    assert listdata(resp, "id", "name", "country")[0] == (1, "Vilnius", {"code": 2})

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    remote.app.authmodel("leveldataset/City", ["getall", "search"])
    resp_city = remote.app.get("leveldataset/City")

    assert resp_city.status_code == 200
    assert listdata(resp_city, "name") == ["Vilnius"]
    assert listdata(resp_city, "id", "name", "country")[0] == (1, "Vilnius", {"code": "2"})


def test_push_ref_with_level_no_source_status_code_400_check(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request
):
    table = """
    d | r | b | m | property | type    | ref                             | source         | level | access
    leveldataset             |         |                                 |                |       |
      | db                   | sql     |                                 |                |       |
      |   |   | City         |         | id                              | cities         | 4     |
      |   |   |   | id       | integer |                                 | id             | 4     | open
      |   |   |   | name     | string  |                                 | name           | 2     | open
      |   |   |   | country  | ref     | /leveldataset/countries/Country | country        | 3     | open
      |   |   |   |          |         |                                 |                |       |
    leveldataset/countries   |         |                                 |                |       |
      |   |   | Country      |         | code                            |                | 4     |
      |   |   |   | code     | string  |                                 |                | 4     | open
      |   |   |   | name     | string  |                                 |                | 2     | open
    """

    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))

    app = create_client(rc, tmp_path, geodb)
    app.authmodel("leveldataset", ["getall"])
    resp = app.get("leveldataset/City")
    assert listdata(resp, "id", "name", "country")[0] == (1, "Vilnius", {"code": 2})

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb, "external")

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    remote.app.authmodel("leveldataset/countries/Country", ["getall", "search"])
    resp_city = remote.app.get("leveldataset/countries/Country")

    assert resp_city.status_code == 400


def test_push_pagination_incremental(
    context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type     | ref      | source      | level | access
    paginated             |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   |   |          |          |          |             |       |
      |   |   | Country      |          | id       | salis       | 4     |
      |   |   |   | code     | string   |          | kodas       | 4     | open
      |   |   |   | name     | string   |          | pavadinimas | 4     | open
      |   |   |   | id       | integer  |          | id          | 4     | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(localrc, ["push", "-d", "paginated", "-o", remote.url, "--credentials", remote.credsfile])
    assert result.exit_code == 0
    assert re.search(r"PUSH:\s*100%.*3/3", result.stderr)

    geodb.write(
        "salis",
        [
            {"kodas": "test", "pavadinimas": "Test", "id": 10},
        ],
    )

    result = cli.invoke(
        localrc, ["push", "-d", "paginated", "-o", remote.url, "--credentials", remote.credsfile, "--incremental"]
    )
    assert result.exit_code == 0
    assert re.search(r"PUSH:\s*100%.*1/1", result.stderr)


def test_push_pagination_without_incremental(
    context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type     | ref      | source      | level | access
    paginated/without             |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   |   |          |          |          |             |       |
      |   |   | Country      |          | id       | salis       | 4     |
      |   |   |   | code     | string   |          | kodas       | 4     | open
      |   |   |   | name     | string   |          | pavadinimas | 4     | open
      |   |   |   | id       | integer  |          | id          | 4     | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(
        localrc, ["push", "-d", "paginated/without", "-o", remote.url, "--credentials", remote.credsfile]
    )
    assert result.exit_code == 0
    assert re.search(r"PUSH:\s*100%.*3/3", result.stderr)

    geodb.write(
        "salis",
        [
            {"kodas": "test", "pavadinimas": "Test", "id": 10},
        ],
    )

    result = cli.invoke(
        localrc, ["push", "-d", "paginated/without", "-o", remote.url, "--credentials", remote.credsfile]
    )
    assert result.exit_code == 0
    assert re.search(r"PUSH:\s*100%.*4/4", result.stderr)


def test_push_pagination_incremental_with_page_valid(
    context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type     | ref      | source      | level | access
    paginated/valid             |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   |   |          |          |          |             |       |
      |   |   | Country      |          | id       | salis       | 4     |
      |   |   |   | code     | string   |          | kodas       | 4     | open
      |   |   |   | name     | string   |          | pavadinimas | 4     | open
      |   |   |   | id       | integer  |          | id          | 4     | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "paginated/valid",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--incremental",
            "--model",
            "paginated/valid/Country",
            "--page",
            "2",
        ],
    )
    assert result.exit_code == 0
    assert re.search(r"PUSH:\s*100%.*1/1", result.stderr)

    geodb.write(
        "salis",
        [
            {"kodas": "test", "pavadinimas": "Test", "id": 10},
        ],
    )

    result = cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "paginated/valid",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--incremental",
            "--model",
            "paginated/valid/Country",
            "--page",
            "2",
        ],
    )
    assert result.exit_code == 0
    assert re.search(r"PUSH:\s*100%.*2/2", result.stderr)


def test_push_pagination_incremental_with_page_invalid(
    context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type     | ref      | source      | level | access
    paginated/invalid             |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   |   |          |          |          |             |       |
      |   |   | Country      |          | id       | salis       | 4     |
      |   |   |   | code     | string   |          | kodas       | 4     | open
      |   |   |   | name     | string   |          | pavadinimas | 4     | open
      |   |   |   | id       | integer  |          | id          | 4     | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "paginated/invalid",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--incremental",
            "--model",
            "paginated/invalid/Country",
            "--page",
            "2",
            "--page",
            "test",
        ],
        fail=False,
    )
    assert result.exit_code == 1


def test_push_with_base(context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, request, base_geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type     | ref      | source      | level | access
    level4basedataset            |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   | Location     |          | id       | location    | 4     |
      |   |   |   | id       | integer  |          | id          | 4     | open
      |   |   |   | name     | string   |          | name        | 4     | open
      |   |   |   | code     | string   |          | code        | 4     | open
      |   |   |   |          |          |          |             |       |
      |   | Location |           |          |          |          |             |       |
      |   |   | City         |          | id       | city        | 4     |
      |   |   |   | code     |    |          | code        | 4     | open
      |   |   |   | name     |    |          | name        | 4     | open
      |   |   |   | id       | integer  |          | id          | 4     | open
      |   |   |   | location | string   |          | location    | 4     | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, base_geodb)
    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "level4basedataset",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )

    assert result.exit_code == 0
    remote.app.authmodel("level4basedataset/Location", ["getall", "search"])
    resp_location = remote.app.get("level4basedataset/Location")

    locations = listdata(resp_location, "_id", "id")
    ryga_id = None
    for _id, id in locations:
        if id == 2:
            ryga_id = _id
            break

    remote.app.authmodel("level4basedataset/City", ["getall", "search"])
    resp_city = remote.app.get("level4basedataset/City")
    assert resp_city.status_code == 200
    assert listdata(resp_city, "_id", "name") == [(ryga_id, "Ryga")]
    assert len(listdata(resp_city, "id", "name", "location", "code")) == 1


def test_push_with_base_different_ref(
    context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, request, base_geodb
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type     | ref      | source      | level | access
    level4basedatasetref           |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   | Location     |          | id       | location    | 4     |
      |   |   |   | id       | integer  |          | id          | 4     | open
      |   |   |   | name     | string   |          | name        | 4     | open
      |   |   |   | code     | string   |          | code        | 4     | open
      |   |   |   |          |          |          |             |       |
      |   | Location |           |          |          | name     |             | 4     |
      |   |   | City         |          | id       | city        | 4     |
      |   |   |   | code     |    |          | code        | 4     | open
      |   |   |   | name     |    |          | name        | 4     | open
      |   |   |   | id       | integer  |          | id          | 4     | open
      |   |   |   | location | string   |          | location    | 4     | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, base_geodb)
    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "level4basedatasetref",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )

    assert result.exit_code == 0
    remote.app.authmodel("level4basedatasetref/Location", ["getall", "search"])
    resp_location = remote.app.get("level4basedatasetref/Location")

    locations = listdata(resp_location, "_id", "name")
    ryga_id = None
    for _id, name in locations:
        if name == "Ryga":
            ryga_id = _id
            break

    remote.app.authmodel("level4basedatasetref/City", ["getall", "search"])
    resp_city = remote.app.get("level4basedatasetref/City")
    assert resp_city.status_code == 200
    assert listdata(resp_city, "_id", "name") == [(ryga_id, "Ryga")]
    assert len(listdata(resp_city, "id", "name", "location", "code")) == 1


def test_push_with_base_level_3(
    context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, request, base_geodb
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | base     | m | property | type     | ref      | source      | level | access
    level3basedataset               |          |          |             |       |
      | db           |   |          | sql      |          |             |       |
      |   |          | Location     |          | id       | location    | 4     |
      |   |          |   | id       | integer  |          | id          | 4     | open
      |   |          |   | name     | string   |          | name        | 4     | open
      |   |          |   | code     | string   |          | code        | 4     | open
      |   |          |   |          |          |          |             |       |
      |   | Location |   |          |          | name     |             | 3     |
      |   |          | City         |          | id       | city        | 4     |
      |   |          |   | code     |          |          | code        | 4     | open
      |   |          |   | name     | string   |          | name        | 4     | open
      |   |          |   | id       | integer  |          | id          | 4     | open
      |   |          |   | location | string   |          | location    | 4     | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, base_geodb)
    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "level3basedataset",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )

    assert result.exit_code == 0
    remote.app.authmodel("level3basedataset/Location", ["getall", "search"])
    resp_location = remote.app.get("level3basedataset/Location")

    locations = listdata(resp_location, "_id", "name")
    ryga_id = None
    for _id, name in locations:
        if name == "Ryga":
            ryga_id = _id
            break

    remote.app.authmodel("level3basedataset/City", ["getall", "search"])
    resp_city = remote.app.get("level3basedataset/City")
    assert resp_city.status_code == 200
    assert listdata(resp_city, "name") == ["Ryga"]
    assert listdata(resp_city, "_id") != [ryga_id]
    assert len(listdata(resp_city, "id", "name", "location", "code")) == 1


def test_push_sync_keymap(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request
):
    table = """
        d | r | b | m | property | type    | ref                             | source         | level | access
        syncdataset             |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | City         |         | id                              | cities         | 4     |
          |   |   |   | id       | integer |                                 | id             | 4     | open
          |   |   |   | name     | string  |                                 | name           | 2     | open
          |   |   |   | country  | ref     | /syncdataset/countries/Country | country        | 4     | open
          |   |   |   |          |         |                                 |                |       |
        syncdataset/countries   |         |                                 |                |       |
          |   |   | Country      |         | code                            |                | 4     |
          |   |   |   | code     | integer |                                 |                | 4     | open
          |   |   |   | name     | string  |                                 |                | 2     | open
        """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    remote.app.authmodel("syncdataset/countries/Country", ["insert", "wipe"])
    resp = remote.app.post("https://example.com/syncdataset/countries/Country", json={"code": 2})
    country_id = resp.json()["_id"]
    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--sync",
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    remote.app.authmodel("syncdataset/City", ["getall", "search", "wipe"])
    resp_city = remote.app.get("syncdataset/City")
    city_id = listdata(resp_city, "_id")[0]

    assert resp_city.status_code == 200
    assert listdata(resp_city, "name") == ["Vilnius"]
    assert listdata(resp_city, "_id", "id", "name", "country")[0] == (city_id, 1, "Vilnius", {"_id": country_id})

    # Reset data
    remote.app.delete("https://example.com/syncdataset/countries/City/:wipe")
    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    resp_city = remote.app.get("syncdataset/City")

    assert resp_city.status_code == 200
    assert listdata(resp_city, "name") == ["Vilnius"]
    assert listdata(resp_city, "_id", "id", "name", "country")[0] == (city_id, 1, "Vilnius", {"_id": country_id})


def test_push_sync_keymap_private_no_error(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request
):
    table = """
        d | r | b | m | property | type    | ref                             | source         | level | access
        syncdataset              |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | City         |         | id                              | cities         | 4     |
          |   |   |   | id       | integer |                                 | id             | 4     | open
          |   |   |   | name     | string  |                                 | name           | 2     | open
          |   |   |   | country  | ref     | /syncdataset/countries/Country  | country        | 4     | private
          |   |   |   |          |         |                                 |                |       |
        syncdataset/countries    |         |                                 |                |       |
          |   |   | Country      |         | code                            |                | 4     |
          |   |   |   | code     | integer |                                 |                | 4     | private
          |   |   |   | name     | string  |                                 |                | 2     | open
        """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--sync",
            "--no-progress-bar",
        ],
        fail=False,
    )
    assert result.exception is None


def test_push_with_text(context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, request, text_geodb):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type     | ref      | source      | level | access
    textnormal               |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   | Country      |          | id       | country     | 4     |
      |   |   |   | id       | integer  |          | id          | 4     | open
      |   |   |   | name@lt  | string   |          | name_lt     | 4     | open
      |   |   |   | name@en  | string   |          | name_en     | 4     | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, text_geodb)
    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "textnormal",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )

    assert result.exit_code == 0
    remote.app.authmodel("textnormal/Country", ["getall", "search"])
    countries = remote.app.get("textnormal/Country?select(id,name@lt,name@en)")
    assert countries.status_code == 200
    assert listdata(countries, "id", "name", sort=True) == [
        (1, {"en": None, "lt": "Lietuva"}),
        (2, {"en": "Latvia", "lt": None}),
        (3, {"en": "Poland", "lt": "Lenkija"}),
    ]


def test_push_with_text_unknown(
    context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, request, text_geodb
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type     | ref      | source      | level | access
    textunknown              |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   | City         |          | id       | city        | 4     |
      |   |   |   | id       | integer  |          | id          | 4     | open
      |   |   |   | name@lt  | string   |          | name_lt     | 4     | open
      |   |   |   | name@pl  | string   |          | name_pl     | 4     | open
      |   |   |   | name     | text     |          | name        | 2     | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, text_geodb)
    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "textunknown",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )

    assert result.exit_code == 0
    remote.app.authmodel("textunknown/City", ["getall", "search"])
    countries = remote.app.get("textunknown/City?select(id,name@lt,name@pl,name@C)")
    assert countries.status_code == 200
    assert listdata(countries, "id", "name", sort=True) == [
        (1, {"": "VLN", "lt": "Vilnius", "pl": "Vilna"}),
    ]


def test_push_postgresql(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    request,
    geodb,
):
    db = f"{postgresql}/push_db"
    if su.database_exists(db):
        su.drop_database(db)
    su.create_database(db)
    engine = sa.create_engine(db)
    with engine.connect() as conn:
        meta = sa.MetaData(conn)
        table = sa.Table("cities", meta, sa.Column("id", sa.Integer), sa.Column("name", sa.Text))
        meta.create_all()
        conn.execute(table.insert((0, "Test")))
        conn.execute(table.insert((1, "Test1")))
    table = f"""
        d | r | b | m | property | type    | ref                             | source         | level | access
        postgrespush              |         |                                |                |       |
          | db                   | sql     |                                 | {db}           |       |
          |   |   | City         |         | id                              | cities         | 4     |
          |   |   |   | id       | integer |                                 | id             | 4     | open
          |   |   |   | name     | string  |                                 | name           | 2     | open
        """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))

    # Configure local server with SQL backend
    tmp = Sqlite(db)
    localrc = create_rc(rc, tmp_path, tmp)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(localrc, ["push", "-o", remote.url, "--credentials", remote.credsfile], fail=False)
    assert result.exit_code == 0
    assert re.search(r"PUSH:\s*100%.*2/2", result.stderr)

    result = cli.invoke(localrc, ["push", "-o", remote.url, "--credentials", remote.credsfile, "-i"], fail=False)
    assert result.exit_code == 0
    assert not re.search(r"PUSH:\s*100%.*2/2", result.stderr)
    su.drop_database(db)


def test_push_postgresql_big_datastream(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    request,
    geodb,
):
    db = f"{postgresql}/push_db"
    if su.database_exists(db):
        su.drop_database(db)
    su.create_database(db)
    engine = sa.create_engine(db)
    with engine.connect() as conn:
        meta = sa.MetaData(conn)
        table = sa.Table("cities", meta, sa.Column("id", sa.Integer), sa.Column("name", sa.Text))
        meta.create_all()
        conn.execute(table.insert((sa.func.generate_series(1, 2000), "Test")))
    table = f"""
        d | r | b | m | property | type    | ref                             | source         | level | access
        postgrespush             |         |                                |                |       |
          | db                   | sql     |                                 | {db}           |       |
          |   |   | City         |         | id                              | cities         | 4     |
          |   |   |   | id       | integer |                                 | id             | 4     | open
          |   |   |   | name     | string  |                                 | name           | 2     | open
        """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))

    # Configure local server with SQL backend
    tmp = Sqlite(db)
    rc = rc.fork({"default_page_size": 100})
    localrc = create_rc(rc, tmp_path, tmp)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(localrc, ["push", "-o", remote.url, "--credentials", remote.credsfile], fail=False)
    assert result.exit_code == 0
    assert "PUSH: 100%" in result.stderr
    assert "2000/2000" in result.stderr

    result = cli.invoke(localrc, ["push", "-o", remote.url, "--credentials", remote.credsfile], fail=False)
    assert result.exit_code == 0
    assert "PUSH: 100%" in result.stderr
    assert "2000/2000" in result.stderr
    su.drop_database(db)


def test_push_with_nulls(context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type     | ref            | source      | level | access
    nullpush                 |          |                |             |       |
      | db                   | sql      |                |             |       |
      |   |   | Nullable     |          | id, name, code | nullable    | 4     |
      |   |   |   | id       | integer  |                | id          | 4     | open
      |   |   |   | name     | string   |                | name        | 4     | open
      |   |   |   | code     | string   |                | code        | 4     | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "nullpush",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0

    remote.app.authmodel("nullpush/Nullable", ["getall", "search"])
    resp_city = remote.app.get("nullpush/Nullable")

    assert resp_city.status_code == 200
    assert listdata(resp_city, "id", "name", "code") == [
        (0, "Test", "0"),
        (0, "Test", "1"),
        (0, "Test0", None),
        (0, None, "0"),
        (0, None, None),
        (1, "Test", None),
        (1, None, None),
        (None, "Test", "0"),
        (None, "Test", None),
    ]
    assert len(listdata(resp_city, "id", "name", "country")) == 9


def test_push_with_errors_rollback(
    context,
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    request,
    sqlite: Sqlite,
):
    sqlite.init(
        {
            "countries": [sa.Column("id", sa.Integer), sa.Column("name", sa.String), sa.Column("code", sa.String)],
            "cities": [sa.Column("id", sa.Integer), sa.Column("name", sa.String), sa.Column("country", sa.Integer)],
        }
    )
    sqlite.write("countries", [{"id": i, "name": f"test{i}", "code": "test"} for i in range(10)])
    sqlite.write("cities", [{"id": i, "name": f"test{i}", "country": i} for i in range(12)])
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type     | ref      | source      | level | access
    errordataset             |          |          |             |       |
      | db                   | sql      |          |             |       |
      |   |   | City         |          | id       | cities      | 4     |
      |   |   |   | id       | integer  |          | id          | 4     | open
      |   |   |   | name     | string   |          | name        | 4     | open
      |   |   |   | country  | ref      | Country  | country     | 4     | open
      |   |   |   |          |          |          |             |       |
      |   |   | Country      |          | id       | countries   | 4     |
      |   |   |   | code     | string   |          | code        | 4     | open
      |   |   |   | name     | string   |          | name        | 4     | open
      |   |   |   | id       | integer  |          | id          | 4     | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, sqlite)
    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    result = cli.invoke(
        localrc,
        [
            "push",
            "-d",
            "errordataset",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
        fail=False,
    )
    assert result.exit_code != 0
    remote.app.authmodel("errordataset/City", ["getall", "search"])
    cities = remote.app.get("errordataset/City")
    assert cities.status_code == 200
    assert listdata(cities, "id", "name", sort=True) == []


@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_set_meta_fields", "spinta_patch", "spinta_update"],
        ["uapi:/:set_meta_fields", "uapi:/:patch", "uapi:/:update"],
    ],
)
def test_push_sync_state_insert(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    push_state_geodb,
    request,
    scope: list,
):
    state_db = os.path.join(tmp_path, "sync.sqlite")
    table = """
        d | r | b | m | property | type    | ref                             | source         | level | access
        datasets/push/state      |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | Country      |         | id, code                        | COUNTRY        | 4     |
          |   |   |   | id       | integer |                                 | ID             | 4     | open
          |   |   |   | code     | string  |                                 | CODE           | 2     | open
          |   |   |   | name     | string  |                                 | NAME           | 2     | open
        """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, push_state_geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    remote.app.authorize(scope)
    remote.app.authmodel("datasets/push/state/Country", ["insert", "getall", "search", "wipe"])

    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--state",
            state_db,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0

    engine = sa.engine.create_engine("sqlite:///" + state_db)

    data_mapping = {}
    result = remote.app.get("https://example.com/datasets/push/state/Country")
    for row in listdata(result, "_id", "_revision", "code", full=True):
        data_mapping[row["code"]] = {"id": row["_id"], "revision": row["_revision"]}

    # Check if pushed values match
    compare_push_state_rows(
        engine,
        "datasets/push/state/Country",
        [
            {
                "checksum": "a8d8a04ebb10f4f0027721e4f90babba9de12fcd",
                "error": False,
                "data": None,
                "page.id": 0,
                "page.code": "LT",
            },
            {
                "checksum": "bef6bdc50bbcf1925ac2fcb1c3cd434474eec9f6",
                "error": False,
                "data": None,
                "page.id": 1,
                "page.code": "LV",
            },
            {
                "checksum": "9b5f08e06bb141eac5e65ebabfb76104323eec5f",
                "error": False,
                "data": None,
                "page.id": 2,
                "page.code": "PL",
            },
        ],
        ['"page.id"'],
    )

    de_id = "d6765254-37da-4915-9e86-0b1908e9b32a"
    # Emulate new row
    remote.app.post(
        "https://example.com/datasets/push/state/Country",
        json={"_id": de_id, "id": 3, "code": "DE", "name": "GERMANY"},
    )

    # Run sync again and check if checksum and page values got updated
    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--state",
            state_db,
            "--sync",
            "--dry-run",
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    compare_push_state_rows(
        engine,
        "datasets/push/state/Country",
        [
            {
                "id": data_mapping["LT"]["id"],
                "checksum": "a8d8a04ebb10f4f0027721e4f90babba9de12fcd",
                "error": False,
                "data": None,
                "page.id": 0,
                "page.code": "LT",
            },
            {
                "id": data_mapping["LV"]["id"],
                "checksum": "bef6bdc50bbcf1925ac2fcb1c3cd434474eec9f6",
                "error": False,
                "data": None,
                "page.id": 1,
                "page.code": "LV",
            },
            {
                "id": data_mapping["PL"]["id"],
                "checksum": "9b5f08e06bb141eac5e65ebabfb76104323eec5f",
                "error": False,
                "data": None,
                "page.id": 2,
                "page.code": "PL",
            },
            {
                "id": de_id,
                "checksum": "ad27b99d9139eaa3a0df159afda9cacc2b26b0b9",
                "error": False,
                "data": None,
                "page.id": 3,
                "page.code": "DE",
            },
        ],
        ['"page.id"'],
    )

    # Reset data
    remote.app.delete("https://example.com/syncdataset/countries/City/:wipe")


@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_set_meta_fields", "spinta_patch", "spinta_update"],
        ["uapi:/:set_meta_fields", "uapi:/:patch", "uapi:/:update"],
    ],
)
def test_push_sync_state_delete(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    push_state_geodb,
    request,
    scope: list,
):
    state_db = os.path.join(tmp_path, "sync.sqlite")
    table = """
        d | r | b | m | property | type    | ref                             | source         | level | access
        datasets/push/state      |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | Country      |         | id, code                        | COUNTRY        | 4     |
          |   |   |   | id       | integer |                                 | ID             | 4     | open
          |   |   |   | code     | string  |                                 | CODE           | 2     | open
          |   |   |   | name     | string  |                                 | NAME           | 2     | open
        """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, push_state_geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    remote.app.authorize(scope)
    remote.app.authmodel("datasets/push/state/Country", ["insert", "getall", "search", "wipe", "delete"])

    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--state",
            state_db,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0

    engine = sa.engine.create_engine("sqlite:///" + state_db)

    data_mapping = {}
    result = remote.app.get("https://example.com/datasets/push/state/Country")
    for row in listdata(result, "_id", "_revision", "code", full=True):
        data_mapping[row["code"]] = {"id": row["_id"], "revision": row["_revision"]}

    # Check if pushed values match
    compare_push_state_rows(
        engine,
        "datasets/push/state/Country",
        [
            {
                "checksum": "a8d8a04ebb10f4f0027721e4f90babba9de12fcd",
                "error": False,
                "data": None,
                "page.id": 0,
                "page.code": "LT",
            },
            {
                "checksum": "bef6bdc50bbcf1925ac2fcb1c3cd434474eec9f6",
                "error": False,
                "data": None,
                "page.id": 1,
                "page.code": "LV",
            },
            {
                "checksum": "9b5f08e06bb141eac5e65ebabfb76104323eec5f",
                "error": False,
                "data": None,
                "page.id": 2,
                "page.code": "PL",
            },
        ],
        ['"page.id"'],
    )

    # Emulate row deletion
    remote.app.delete(f"https://example.com/datasets/push/state/Country/{data_mapping['LT']['id']}")

    # Run sync again and check if row was removed
    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--state",
            state_db,
            "--sync",
            "--dry-run",
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    compare_push_state_rows(
        engine,
        "datasets/push/state/Country",
        [
            {
                "id": data_mapping["LV"]["id"],
                "checksum": "bef6bdc50bbcf1925ac2fcb1c3cd434474eec9f6",
                "error": False,
                "data": None,
                "page.id": 1,
                "page.code": "LV",
            },
            {
                "id": data_mapping["PL"]["id"],
                "checksum": "9b5f08e06bb141eac5e65ebabfb76104323eec5f",
                "error": False,
                "data": None,
                "page.id": 2,
                "page.code": "PL",
            },
        ],
        ['"page.id"'],
    )

    # Reset data
    remote.app.delete("https://example.com/syncdataset/countries/City/:wipe")


@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_set_meta_fields", "spinta_delete", "spinta_update", "spinta_patch"],
        ["uapi:/:set_meta_fields", "uapi:/:delete", "uapi:/:update", "uapi:/:patch"],
    ],
)
def test_push_sync_state_update(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    push_state_geodb,
    request,
    scope: list,
):
    state_db = os.path.join(tmp_path, "sync.sqlite")
    table = """
        d | r | b | m | property | type    | ref                             | source         | level | access
        datasets/push/state      |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | Country      |         | id, code                        | COUNTRY        | 4     |
          |   |   |   | id       | integer |                                 | ID             | 4     | open
          |   |   |   | code     | string  |                                 | CODE           | 2     | open
          |   |   |   | name     | string  |                                 | NAME           | 2     | open
        """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, push_state_geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    remote.app.authorize(scope)
    remote.app.authmodel("datasets/push/state/Country", ["insert", "getall", "search", "wipe"])

    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--state",
            state_db,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0

    engine = sa.engine.create_engine("sqlite:///" + state_db)

    data_mapping = {}
    result = remote.app.get("https://example.com/datasets/push/state/Country")
    for row in listdata(result, "_id", "_revision", "code", full=True):
        data_mapping[row["code"]] = {"id": row["_id"], "revision": row["_revision"]}

    # Check if pushed values match
    compare_push_state_rows(
        engine,
        "datasets/push/state/Country",
        [
            {
                "checksum": "a8d8a04ebb10f4f0027721e4f90babba9de12fcd",
                "error": False,
                "data": None,
                "page.id": 0,
                "page.code": "LT",
            },
            {
                "checksum": "bef6bdc50bbcf1925ac2fcb1c3cd434474eec9f6",
                "error": False,
                "data": None,
                "page.id": 1,
                "page.code": "LV",
            },
            {
                "checksum": "9b5f08e06bb141eac5e65ebabfb76104323eec5f",
                "error": False,
                "data": None,
                "page.id": 2,
                "page.code": "PL",
            },
        ],
        ['"page.id"'],
    )

    # Emulate changed row
    remote.app.patch(
        f"https://example.com/datasets/push/state/Country/{data_mapping['LT']['id']}",
        json={"_revision": data_mapping["LT"]["revision"], "code": "lt", "name": "lietuva"},
    )

    # Run sync again and check if checksum and page values got updated
    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--state",
            state_db,
            "--sync",
            "--dry-run",
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    compare_push_state_rows(
        engine,
        "datasets/push/state/Country",
        [
            {
                "checksum": "ab9c32680c7114df96309609ce926a85c67e3ab4",
                "error": False,
                "data": None,
                "page.id": 0,
                "page.code": "lt",
            },
            {
                "checksum": "bef6bdc50bbcf1925ac2fcb1c3cd434474eec9f6",
                "error": False,
                "data": None,
                "page.id": 1,
                "page.code": "LV",
            },
            {
                "checksum": "9b5f08e06bb141eac5e65ebabfb76104323eec5f",
                "error": False,
                "data": None,
                "page.id": 2,
                "page.code": "PL",
            },
        ],
        ['"page.id"'],
    )

    # Reset data
    remote.app.delete("https://example.com/syncdataset/countries/City/:wipe")


@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_set_meta_fields", "spinta_delete", "spinta_update", "spinta_patch"],
        ["uapi:/:set_meta_fields", "uapi:/:delete", "uapi:/:update", "uapi:/:patch"],
    ],
)
def test_push_sync_state_update_revision(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    push_state_geodb,
    request,
    scope: list,
):
    state_db = os.path.join(tmp_path, "sync.sqlite")
    table = """
        d | r | b | m | property | type    | ref                             | source         | level | access
        datasets/push/state      |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | Country      |         | id, code                        | COUNTRY        | 4     |
          |   |   |   | id       | integer |                                 | ID             | 4     | open
          |   |   |   | code     | string  |                                 | CODE           | 2     | open
          |   |   |   | name     | string  |                                 | NAME           | 2     | open
        """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, push_state_geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    remote.app.authorize(scope)
    remote.app.authmodel("datasets/push/state/Country", ["insert", "getall", "search", "wipe"])

    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--state",
            state_db,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0

    engine = sa.engine.create_engine("sqlite:///" + state_db)

    data_mapping = {}
    result = remote.app.get("https://example.com/datasets/push/state/Country")
    for row in listdata(result, "_id", "_revision", "code", full=True):
        data_mapping[row["code"]] = {"id": row["_id"], "revision": row["_revision"]}

    # Check if pushed values match
    compare_push_state_rows(
        engine,
        "datasets/push/state/Country",
        [
            {
                "revision": data_mapping["LT"]["revision"],
                "checksum": "a8d8a04ebb10f4f0027721e4f90babba9de12fcd",
                "error": False,
                "data": None,
                "page.id": 0,
                "page.code": "LT",
            },
            {
                "revision": data_mapping["LV"]["revision"],
                "checksum": "bef6bdc50bbcf1925ac2fcb1c3cd434474eec9f6",
                "error": False,
                "data": None,
                "page.id": 1,
                "page.code": "LV",
            },
            {
                "revision": data_mapping["PL"]["revision"],
                "checksum": "9b5f08e06bb141eac5e65ebabfb76104323eec5f",
                "error": False,
                "data": None,
                "page.id": 2,
                "page.code": "PL",
            },
        ],
        ['"page.id"'],
    )

    # Emulate changed revision
    result = remote.app.patch(
        f"https://example.com/datasets/push/state/Country/{data_mapping['LT']['id']}",
        json={"_revision": data_mapping["LT"]["revision"], "code": "lt", "name": "lietuva"},
    )
    rev = result.json()["_revision"]
    result = remote.app.patch(
        f"https://example.com/datasets/push/state/Country/{data_mapping['LT']['id']}",
        json={"_revision": rev, "code": "LT", "name": "LITHUANIA"},
    )
    rev = result.json()["_revision"]

    assert data_mapping["LT"]["revision"] != rev

    # Run sync again and check if checksum and page values got updated
    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--state",
            state_db,
            "--sync",
            "--dry-run",
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    compare_push_state_rows(
        engine,
        "datasets/push/state/Country",
        [
            {
                "revision": rev,
                "checksum": "a8d8a04ebb10f4f0027721e4f90babba9de12fcd",
                "error": False,
                "data": None,
                "page.id": 0,
                "page.code": "LT",
            },
            {
                "revision": data_mapping["LV"]["revision"],
                "checksum": "bef6bdc50bbcf1925ac2fcb1c3cd434474eec9f6",
                "error": False,
                "data": None,
                "page.id": 1,
                "page.code": "LV",
            },
            {
                "revision": data_mapping["PL"]["revision"],
                "checksum": "9b5f08e06bb141eac5e65ebabfb76104323eec5f",
                "error": False,
                "data": None,
                "page.id": 2,
                "page.code": "PL",
            },
        ],
        ['"page.id"'],
    )

    # Reset data
    remote.app.delete("https://example.com/syncdataset/countries/City/:wipe")


@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_set_meta_fields", "spinta_delete", "spinta_update", "spinta_patch"],
        ["uapi:/:set_meta_fields", "uapi:/:delete", "uapi:/:update", "uapi:/:patch"],
    ],
)
def test_push_sync_state_combined(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    push_state_geodb,
    request,
    scope: list,
):
    state_db = os.path.join(tmp_path, "sync.sqlite")
    table = """
        d | r | b | m | property | type    | ref                             | source         | level | access
        datasets/push/state      |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | Country      |         | id, code                        | COUNTRY        | 4     |
          |   |   |   | id       | integer |                                 | ID             | 4     | open
          |   |   |   | code     | string  |                                 | CODE           | 2     | open
          |   |   |   | name     | string  |                                 | NAME           | 2     | open
        """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, push_state_geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    remote.app.authorize(scope)
    remote.app.authmodel("datasets/push/state/Country", ["insert", "getall", "search", "wipe"])

    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--state",
            state_db,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0

    engine = sa.engine.create_engine("sqlite:///" + state_db)

    data_mapping = {}
    result = remote.app.get("https://example.com/datasets/push/state/Country")
    for row in listdata(result, "_id", "_revision", "code", full=True):
        data_mapping[row["code"]] = {"id": row["_id"], "revision": row["_revision"]}

    # Check if pushed values match
    compare_push_state_rows(
        engine,
        "datasets/push/state/Country",
        [
            {
                "checksum": "a8d8a04ebb10f4f0027721e4f90babba9de12fcd",
                "error": False,
                "data": None,
                "page.id": 0,
                "page.code": "LT",
            },
            {
                "checksum": "bef6bdc50bbcf1925ac2fcb1c3cd434474eec9f6",
                "error": False,
                "data": None,
                "page.id": 1,
                "page.code": "LV",
            },
            {
                "checksum": "9b5f08e06bb141eac5e65ebabfb76104323eec5f",
                "error": False,
                "data": None,
                "page.id": 2,
                "page.code": "PL",
            },
        ],
        ['"page.id"'],
    )
    de_id = "d6765254-37da-4915-9e86-0b1908e9b32a"

    remote.app.patch(
        f"https://example.com/datasets/push/state/Country/{data_mapping['LT']['id']}",
        json={"_revision": data_mapping["LT"]["revision"], "code": "lt", "name": "lietuva"},
    )
    remote.app.delete(f"https://example.com/datasets/push/state/Country/{data_mapping['LV']['id']}")
    remote.app.post(
        "https://example.com/datasets/push/state/Country",
        json={"_id": de_id, "id": 1, "code": "DE", "name": "GERMANY"},
    )

    # Run sync again and check if checksum and page values got updated
    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--state",
            state_db,
            "--sync",
            "--dry-run",
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    compare_push_state_rows(
        engine,
        "datasets/push/state/Country",
        [
            {
                "id": data_mapping["LT"]["id"],
                "checksum": "ab9c32680c7114df96309609ce926a85c67e3ab4",
                "error": False,
                "data": None,
                "page.id": 0,
                "page.code": "lt",
            },
            {
                "id": de_id,
                "checksum": "2de7ef4d30147ba45917a94cd0b4113793b76cfa",
                "error": False,
                "data": None,
                "page.id": 1,
                "page.code": "DE",
            },
            {
                "id": data_mapping["PL"]["id"],
                "checksum": "9b5f08e06bb141eac5e65ebabfb76104323eec5f",
                "error": False,
                "data": None,
                "page.id": 2,
                "page.code": "PL",
            },
        ],
        ['"page.id"'],
    )

    # Reset data
    remote.app.delete("https://example.com/syncdataset/countries/City/:wipe")


@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_set_meta_fields", "spinta_patch", "spinta_update"],
        ["uapi:/:set_meta_fields", "uapi:/:patch", "uapi:/:update"],
    ],
)
def test_push_sync_state_migrate_page_values(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    push_state_geodb,
    request,
    scope: list,
):
    state_db = os.path.join(tmp_path, "sync.sqlite")
    table = """
        d | r | b | m | property | type    | ref                             | source         | level | access
        datasets/push/state      |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | Country      |         | id, code                        | COUNTRY        | 4     |
          |   |   |   | id       | integer |                                 | ID             | 4     | open
          |   |   |   | code     | string  |                                 | CODE           | 2     | open
          |   |   |   | name     | string  |                                 | NAME           | 2     | open
        """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, push_state_geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    remote.app.authorize(scope)
    remote.app.authmodel("datasets/push/state/Country", ["insert", "getall", "search", "wipe"])

    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--state",
            state_db,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0

    engine = sa.engine.create_engine("sqlite:///" + state_db)

    data_mapping = {}
    result = remote.app.get("https://example.com/datasets/push/state/Country")
    for row in listdata(result, "_id", "_revision", "code", full=True):
        data_mapping[row["code"]] = {"id": row["_id"], "revision": row["_revision"]}

    # Emulate empty page columns
    with engine.connect() as conn:
        conn.execute(sa.text('UPDATE "datasets/push/state/Country" SET "page.id" = NULL, "page.code" = NULL'))

    compare_push_state_rows(
        engine,
        "datasets/push/state/Country",
        [
            {
                "checksum": "a8d8a04ebb10f4f0027721e4f90babba9de12fcd",
                "error": False,
                "data": None,
                "page.id": None,
                "page.code": None,
            },
            {
                "checksum": "bef6bdc50bbcf1925ac2fcb1c3cd434474eec9f6",
                "error": False,
                "data": None,
                "page.id": None,
                "page.code": None,
            },
            {
                "checksum": "9b5f08e06bb141eac5e65ebabfb76104323eec5f",
                "error": False,
                "data": None,
                "page.id": None,
                "page.code": None,
            },
        ],
        ['"page.id"'],
    )

    # Run sync again and check if page values were updated
    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--state",
            state_db,
            "--sync",
            "--dry-run",
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    compare_push_state_rows(
        engine,
        "datasets/push/state/Country",
        [
            {
                "checksum": "a8d8a04ebb10f4f0027721e4f90babba9de12fcd",
                "error": False,
                "data": None,
                "page.id": 0,
                "page.code": "LT",
            },
            {
                "checksum": "bef6bdc50bbcf1925ac2fcb1c3cd434474eec9f6",
                "error": False,
                "data": None,
                "page.id": 1,
                "page.code": "LV",
            },
            {
                "checksum": "9b5f08e06bb141eac5e65ebabfb76104323eec5f",
                "error": False,
                "data": None,
                "page.id": 2,
                "page.code": "PL",
            },
        ],
        ['"page.id"'],
    )

    # Reset data
    remote.app.delete("https://example.com/syncdataset/countries/City/:wipe")


@pytest.mark.parametrize(
    "scope",
    [
        [
            "spinta_set_meta_fields",
            "spinta_patch",
            "spinta_update",
            "spinta_insert",
            "spinta_getall",
            "spinta_search",
            "spinta_wipe",
        ],
        [
            "uapi:/:set_meta_fields",
            "uapi:/:patch",
            "uapi:/:update",
            "uapi:/:create",
            "uapi:/:getall",
            "uapi:/:search",
            "uapi:/:wipe",
        ],
    ],
)
def test_push_sync_state_skip_no_auth(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    push_state_geodb,
    request,
    scope: list,
):
    state_db = os.path.join(tmp_path, "sync.sqlite")
    table = """
        d | r | b | m | property | type    | ref                             | source         | level | access
        datasets/push/state      |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | Country      |         | id, code                        | COUNTRY        | 4     |
          |   |   |   | id       | integer |                                 | ID             | 4     | protected
          |   |   |   | code     | string  |                                 | CODE           | 2     | open
          |   |   |   | name     | string  |                                 | NAME           | 2     | open
          |   |   | CountryOpen  |         | id, code                        | COUNTRY        | 4     |
          |   |   |   | id       | integer |                                 | ID             | 4     | open
          |   |   |   | code     | string  |                                 | CODE           | 2     | open
          |   |   |   | name     | string  |                                 | NAME           | 2     | open
        """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, push_state_geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    remote.app.authorize(scope)

    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--state",
            state_db,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0

    engine = sa.engine.create_engine("sqlite:///" + state_db)

    data_mapping = {}
    result = remote.app.get("https://example.com/datasets/push/state/CountryOpen")
    for row in listdata(result, "_id", "_revision", "code", full=True):
        data_mapping[row["code"]] = {"id": row["_id"], "revision": row["_revision"]}

    # Check if pushed values match
    # Will have different checksums, because not all data is pushed
    compare_push_state_rows(
        engine,
        "datasets/push/state/Country",
        [
            {
                "checksum": "2e9798a712eada11b26e1a7947fe05922838f8d3",
                "error": False,
                "data": None,
                "page.id": 0,
                "page.code": "LT",
            },
            {
                "checksum": "df9b36c5237fe25960c265df63f004d6a839f104",
                "error": False,
                "data": None,
                "page.id": 1,
                "page.code": "LV",
            },
            {
                "checksum": "72e48e32afdba7c4e3db5a33852ee35f1a8abe2c",
                "error": False,
                "data": None,
                "page.id": 2,
                "page.code": "PL",
            },
        ],
        ['"page.id"'],
    )
    compare_push_state_rows(
        engine,
        "datasets/push/state/CountryOpen",
        [
            {
                "checksum": "a8d8a04ebb10f4f0027721e4f90babba9de12fcd",
                "error": False,
                "data": None,
                "page.id": 0,
                "page.code": "LT",
            },
            {
                "checksum": "bef6bdc50bbcf1925ac2fcb1c3cd434474eec9f6",
                "error": False,
                "data": None,
                "page.id": 1,
                "page.code": "LV",
            },
            {
                "checksum": "9b5f08e06bb141eac5e65ebabfb76104323eec5f",
                "error": False,
                "data": None,
                "page.id": 2,
                "page.code": "PL",
            },
        ],
        ['"page.id"'],
    )

    de_id = "d6765254-37da-4915-9e86-0b1908e9b32a"
    # Emulate new row
    remote.app.post(
        "https://example.com/datasets/push/state/Country", json={"_id": de_id, "code": "DE", "name": "GERMANY"}
    )
    remote.app.post(
        "https://example.com/datasets/push/state/CountryOpen",
        json={"_id": de_id, "id": 3, "code": "DE", "name": "GERMANY"},
    )

    # Run sync again and check if checksum and page values got updated
    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--state",
            state_db,
            "--sync",
            "--dry-run",
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    assert "SKIPPED PUSH STATE 'datasets/push/state/Country' MODEL SYNC, NO PERMISSION." in result.stdout

    compare_push_state_rows(
        engine,
        "datasets/push/state/Country",
        [
            {
                "checksum": "2e9798a712eada11b26e1a7947fe05922838f8d3",
                "error": False,
                "data": None,
                "page.id": 0,
                "page.code": "LT",
            },
            {
                "checksum": "df9b36c5237fe25960c265df63f004d6a839f104",
                "error": False,
                "data": None,
                "page.id": 1,
                "page.code": "LV",
            },
            {
                "checksum": "72e48e32afdba7c4e3db5a33852ee35f1a8abe2c",
                "error": False,
                "data": None,
                "page.id": 2,
                "page.code": "PL",
            },
        ],
        ['"page.id"'],
    )
    compare_push_state_rows(
        engine,
        "datasets/push/state/CountryOpen",
        [
            {
                "id": data_mapping["LT"]["id"],
                "checksum": "a8d8a04ebb10f4f0027721e4f90babba9de12fcd",
                "error": False,
                "data": None,
                "page.id": 0,
                "page.code": "LT",
            },
            {
                "id": data_mapping["LV"]["id"],
                "checksum": "bef6bdc50bbcf1925ac2fcb1c3cd434474eec9f6",
                "error": False,
                "data": None,
                "page.id": 1,
                "page.code": "LV",
            },
            {
                "id": data_mapping["PL"]["id"],
                "checksum": "9b5f08e06bb141eac5e65ebabfb76104323eec5f",
                "error": False,
                "data": None,
                "page.id": 2,
                "page.code": "PL",
            },
            {
                "id": de_id,
                "checksum": "ad27b99d9139eaa3a0df159afda9cacc2b26b0b9",
                "error": False,
                "data": None,
                "page.id": 3,
                "page.code": "DE",
            },
        ],
        ['"page.id"'],
    )

    # Reset data
    remote.app.delete("https://example.com/syncdataset/countries/City/:wipe")


@pytest.mark.parametrize(
    "scope",
    [
        [
            "spinta_set_meta_fields",
            "spinta_patch",
            "spinta_update",
            "spinta_insert",
            "spinta_getall",
            "spinta_search",
            "spinta_wipe",
        ],
        [
            "uapi:/:set_meta_fields",
            "uapi:/:patch",
            "uapi:/:update",
            "uapi:/:create",
            "uapi:/:getall",
            "uapi:/:search",
            "uapi:/:wipe",
        ],
    ],
)
def test_push_page_multiple_keys(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    multi_type_geodb,
    request,
    scope: list,
):
    state_db = os.path.join(tmp_path, "sync.sqlite")
    table = """
        d | r | b | m | property | type     | ref                             | source         | level | access
        datasets/push/page       |          |                                 |                |       |
          | db                   | sql      |                                 |                |       |
          |   |   | Test         |          | id, name, number, url, date, time, datetime | TEST | 4     |
          |   |   |   | id       | integer  |                                 | ID             | 4     | open
          |   |   |   | name     | string   |                                 | NAME           | 2     | open
          |   |   |   | number   | number   |                                 | NUMBER         | 2     | open
          |   |   |   | url      | url      |                                 | URL            | 2     | open
          |   |   |   | date     | date     |                                 | DATE           | 2     | open
          |   |   |   | time     | time     |                                 | TIME           | 2     | open
          |   |   |   | datetime | datetime |                                 | DATETIME       | 2     | open
        """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    # Configure local server with SQL backend
    rc = rc.fork({"default_page_size": 2})
    localrc = create_rc(rc, tmp_path, multi_type_geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    remote.app.authorize(scope)

    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--state",
            state_db,
        ],
    )
    assert result.exit_code == 0

    engine = sa.engine.create_engine("sqlite:///" + state_db)

    compare_push_state_rows(
        engine,
        "datasets/push/page/Test",
        [
            {
                "checksum": "d1b91f5bda845db8a0c0dca0e76d6d5354327845",
                "error": False,
                "data": None,
                "page.id": 0,
                "page.name": "LT",
                "page.number": 0.1,
                "page.date": "2024-02-01",
                "page.time": "12:10:20.000000",
                "page.datetime": "2024-02-01 12:10:20.000000",
            },
            {
                "checksum": "ccad01770f5f86c776fa8faad652f46a6f697b09",
                "error": False,
                "data": None,
                "page.id": 1,
                "page.name": "LV",
                "page.number": 1.2,
                "page.date": "2024-02-02",
                "page.time": "12:20:20.000000",
                "page.datetime": "2024-02-02 12:20:20.000000",
            },
            {
                "checksum": "933c0e0216d721d1c7974672b1b5733cdeff7a3c",
                "error": False,
                "data": None,
                "page.id": 2,
                "page.name": "PL",
                "page.number": 2.3,
                "page.date": "2024-02-03",
                "page.time": "12:30:20.000000",
                "page.datetime": "2024-02-03 12:30:20.000000",
            },
        ],
        ['"page.id"'],
    )

    # Run sync again with empty db, to see if everything gets synced
    state_db = os.path.join(tmp_path, "sync_page.sqlite")
    engine = sa.engine.create_engine("sqlite:///" + state_db)

    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--state",
            state_db,
            "--sync",
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0

    compare_push_state_rows(
        engine,
        "datasets/push/page/Test",
        [
            {
                "checksum": "d1b91f5bda845db8a0c0dca0e76d6d5354327845",
                "error": False,
                "data": None,
                "page.id": 0,
                "page.name": "LT",
                "page.number": 0.1,
                "page.date": "2024-02-01",
                "page.time": "12:10:20.000000",
                "page.datetime": "2024-02-01 12:10:20.000000",
            },
            {
                "checksum": "ccad01770f5f86c776fa8faad652f46a6f697b09",
                "error": False,
                "data": None,
                "page.id": 1,
                "page.name": "LV",
                "page.number": 1.2,
                "page.date": "2024-02-02",
                "page.time": "12:20:20.000000",
                "page.datetime": "2024-02-02 12:20:20.000000",
            },
            {
                "checksum": "933c0e0216d721d1c7974672b1b5733cdeff7a3c",
                "error": False,
                "data": None,
                "page.id": 2,
                "page.name": "PL",
                "page.number": 2.3,
                "page.date": "2024-02-03",
                "page.time": "12:30:20.000000",
                "page.datetime": "2024-02-03 12:30:20.000000",
            },
        ],
        ['"page.id"'],
    )
    # Reset data
    remote.app.delete("https://example.com/syncdataset/countries/City/:wipe")


@pytest.mark.parametrize(
    "scope",
    [
        [
            "spinta_set_meta_fields",
            "spinta_patch",
            "spinta_update",
            "spinta_insert",
            "spinta_getall",
            "spinta_search",
            "spinta_wipe",
        ],
        [
            "uapi:/:set_meta_fields",
            "uapi:/:patch",
            "uapi:/:update",
            "uapi:/:create",
            "uapi:/:getall",
            "uapi:/:search",
            "uapi:/:wipe",
        ],
    ],
)
def test_push_with_geometry(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    request,
    sqlite,
    scope: list,
):
    sqlite.init(
        {
            "TEST": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("GEO", sa.Text),
            ],
        }
    )

    sqlite.write(
        "TEST",
        [
            {"ID": 0, "GEO": "POINT(0 0)"},
            {"ID": 1, "GEO": "POINT(10 10)"},
            {"ID": 2, "GEO": "POINT(-10 -10)"},
        ],
    )

    table = """
        d | r | b | m | property | type            | ref | source         | level | access
        datasets/push/geo        |                 |     |                |       |
          | db                   | sql             |     |                |       |
          |   |   | Test         |                 | id  | TEST           | 4     |
          |   |   |   | id       | integer         |     | ID             | 4     | open
          |   |   |   | geo      | geometry(point) |     | GEO            | 2     | open
        """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    # Configure local server with SQL backend
    rc = rc.fork({"default_page_size": 2})
    localrc = create_rc(rc, tmp_path, sqlite)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    remote.app.authorize(scope)

    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
        ],
    )
    assert result.exit_code == 0

    result = remote.app.get("datasets/push/geo/Test/:format/html")
    assert result.status_code == 200
    assert listdata(result, "id", "geo", sort=True) == [
        (0, "POINT (0 0)"),
        (1, "POINT (10 10)"),
        (2, "POINT (-10 -10)"),
    ]
    remote.app.delete("https://example.com/datasets/push/geo/Test/:wipe")


def test_push_default_timeout(
    context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request, caplog
):
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
     """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)
    assert remote.url == "https://example.com/"

    responses.add(
        responses.POST,
        remote.url,
        body=ReadTimeout(),
    )
    responses.add(
        responses.GET,
        re.compile(r"https://example.com/datasets/gov/example/Country/.*"),
        body=ReadTimeout(),
    )
    with caplog.at_level(logging.ERROR):
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
                "--sync",
                "--no-progress-bar",
            ],
            fail=False,
        )

    assert result.exit_code == 1
    assert any(
        "Read timeout occurred. Consider using a smaller --chunk-size to avoid timeouts. Current timeout settings are (connect: 5.0s, read: 300.0s)."
        in message
        for message in caplog.messages
    )


def test_push_read_timeout(context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request, caplog):
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
     """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)
    assert remote.url == "https://example.com/"

    responses.add(
        responses.POST,
        remote.url,
        body=ReadTimeout(),
    )
    responses.add(
        responses.GET,
        re.compile(r"https://example.com/datasets/gov/example/Country/.*"),
        body=ReadTimeout(),
    )
    with caplog.at_level(logging.ERROR):
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
                "--sync",
                "--read-timeout",
                "0.1",
                "--no-progress-bar",
            ],
            fail=False,
        )

    assert result.exit_code == 1
    assert any(
        "Read timeout occurred. Consider using a smaller --chunk-size to avoid timeouts. Current timeout settings are (connect: 5.0s, read: 0.1s)."
        in message
        for message in caplog.messages
    )


def test_push_connect_timeout(
    context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request, caplog
):
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
     """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)
    assert remote.url == "https://example.com/"

    responses.add(
        responses.POST,
        remote.url,
        body=ConnectTimeout(),
    )
    responses.add(
        responses.GET,
        re.compile(r"https://example.com/datasets/gov/example/Country/.*"),
        body=ConnectTimeout(),
    )
    with caplog.at_level(logging.ERROR):
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
                "--sync",
                "--connect-timeout",
                "0.1",
                "--no-progress-bar",
            ],
            fail=False,
        )

    assert result.exit_code == 1
    assert any(
        "Connect timeout occurred. Current timeout settings are (connect: 0.1s, read: 300.0s)." in message
        for message in caplog.messages
    )


def test_push_connect_and_read_timeout(
    context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request, caplog
):
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
     """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)
    assert remote.url == "https://example.com/"

    responses.add(
        responses.POST,
        remote.url,
        body=ReadTimeout(),
    )
    responses.add(
        responses.GET,
        re.compile(r"https://example.com/datasets/gov/example/Country/.*"),
        body=ReadTimeout(),
    )

    with caplog.at_level(logging.ERROR):
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
                "--sync",
                "--connect-timeout",
                "0.1",
                "--read-timeout",
                "0.1",
                "--no-progress-bar",
            ],
            fail=False,
        )

    assert result.exit_code == 1
    assert any(
        "Read timeout occurred. Consider using a smaller --chunk-size to avoid timeouts. Current timeout settings are (connect: 0.1s, read: 0.1s)."
        in message
        for message in caplog.messages
    )


def test_push_timeout_with_retries(
    context, postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request, caplog
):
    rc = rc.fork({"sync_retry_count": 6, "sync_retry_delay_range": [0.1, 0.2, 0.3]})
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
     """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)
    assert remote.url == "https://example.com/"

    responses.add(
        responses.POST,
        remote.url,
        body=ReadTimeout(),
    )
    responses.add(
        responses.GET,
        re.compile(r"https://example.com/datasets/gov/example/Country/.*"),
        body=ReadTimeout(),
    )
    with caplog.at_level(logging.ERROR):
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
                "--sync",
                "--no-progress-bar",
            ],
            fail=False,
        )

    assert result.exit_code == 1
    assert any(
        "Read timeout occurred. Consider using a smaller --chunk-size to avoid timeouts. Current timeout settings are (connect: 5.0s, read: 300.0s)."
        in message
        for message in caplog.messages
    )
    assert "Retrying (1/6) in 0.1 seconds..." in result.output
    assert "Retrying (2/6) in 0.2 seconds..." in result.output
    assert "Retrying (3/6) in 0.3 seconds..." in result.output
    assert "Retrying (4/6) in 0.3 seconds..." in result.output
    assert "Retrying (5/6) in 0.3 seconds..." in result.output
    assert "Retrying (6/6) in 0.3 seconds..." in result.output


@pytest.mark.parametrize(
    "scope",
    [
        [
            "spinta_set_meta_fields",
            "spinta_patch",
            "spinta_update",
            "spinta_insert",
            "spinta_getall",
            "spinta_search",
            "spinta_wipe",
        ],
        [
            "uapi:/:set_meta_fields",
            "uapi:/:patch",
            "uapi:/:update",
            "uapi:/:create",
            "uapi:/:getall",
            "uapi:/:search",
            "uapi:/:wipe",
        ],
    ],
)
def test_push_with_geometry_flip_both(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    request,
    sqlite,
    scope: list,
):
    sqlite.init(
        {
            "TEST": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("GEO", sa.Text),
            ],
        }
    )

    sqlite.write(
        "TEST",
        [
            {"ID": 0, "GEO": "POINT(200000 5980000)"},
            {"ID": 1, "GEO": "POINT(210000 5985000)"},
            {"ID": 2, "GEO": "POINT(220000 5990000)"},
        ],
    )

    table = """
        d | r | b | m | property | type                  | ref | source | level | access | prepare
        datasets/push/geo/flip   |                       |     |        |       |        |
          | db                   | sql                   |     |        |       |        |
          |   |   | Test         |                       | id  | TEST   | 4     |        |
          |   |   |   | id       | integer               |     | ID     | 4     | open   |    
          |   |   |   | geo      | geometry(point, 3346) |     | GEO    | 2     | open   | flip()
        """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, sqlite)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    remote.app.authorize(scope)

    result = cli.invoke(
        localrc,
        [
            "push",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
        ],
    )
    assert result.exit_code == 0

    result = remote.app.get("datasets/push/geo/flip/Test")
    assert result.status_code == 200
    assert listdata(result, "id", "geo", sort=True) == [
        (0, "POINT (200000 5980000)"),
        (1, "POINT (210000 5985000)"),
        (2, "POINT (220000 5990000)"),
    ]
    remote.app.delete("https://example.com/datasets/push/geo/flip/Test/:wipe")


@pytest.mark.parametrize(
    "scope",
    [
        [
            "spinta_set_meta_fields",
            "spinta_patch",
            "spinta_update",
            "spinta_insert",
            "spinta_getall",
            "spinta_search",
            "spinta_wipe",
        ],
        [
            "uapi:/:set_meta_fields",
            "uapi:/:patch",
            "uapi:/:update",
            "uapi:/:create",
            "uapi:/:getall",
            "uapi:/:search",
            "uapi:/:wipe",
        ],
    ],
)
def test_push_with_geometry_flip_source(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    request,
    sqlite,
    scope: list,
):
    sqlite.init(
        {
            "TEST": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("GEO", sa.Text),
            ],
        }
    )

    sqlite.write(
        "TEST",
        [
            {"ID": 0, "GEO": "POINT(200000 5980000)"},
            {"ID": 1, "GEO": "POINT(210000 5985000)"},
            {"ID": 2, "GEO": "POINT(220000 5990000)"},
        ],
    )

    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        """
        d | r | b | m | property | type                  | ref | source | level | access | prepare
        datasets/push/geo/flip   |                       |     |        |       |        |
          |   |   | Test         |                       | id  |        | 4     |        |
          |   |   |   | id       | integer               |     |        | 4     | open   |    
          |   |   |   | geo      | geometry(point, 3346) |     |        | 2     | open   |
        """,
    )

    create_tabular_manifest(
        context,
        tmp_path / "manifest_push.csv",
        """
        d | r | b | m | property | type                  | ref | source | level | access | prepare
        datasets/push/geo/flip   |                       |     |        |       |        |
          | db                   | sql                   | sql |        |       |        |
          |   |   | Test         |                       | id  | TEST   | 4     |        |
          |   |   |   | id       | integer               |     | ID     | 4     | open   |    
          |   |   |   | geo      | geometry(point, 3346) |     | GEO    | 2     | open   | flip()
        """,
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, sqlite)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    remote.app.authorize(scope)

    result = cli.invoke(
        localrc,
        [
            "push",
            tmp_path / "manifest_push.csv",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
        ],
    )
    assert result.exit_code == 0

    result = remote.app.get("datasets/push/geo/flip/Test")
    assert result.status_code == 200
    assert listdata(result, "id", "geo", sort=True) == [
        (0, "POINT (5980000 200000)"),
        (1, "POINT (5985000 210000)"),
        (2, "POINT (5990000 220000)"),
    ]
    remote.app.delete("https://example.com/datasets/push/geo/flip/Test/:wipe")


@pytest.mark.parametrize(
    "scope",
    [
        [
            "spinta_set_meta_fields",
            "spinta_patch",
            "spinta_update",
            "spinta_insert",
            "spinta_getall",
            "spinta_search",
            "spinta_wipe",
        ],
        [
            "uapi:/:set_meta_fields",
            "uapi:/:patch",
            "uapi:/:update",
            "uapi:/:create",
            "uapi:/:getall",
            "uapi:/:search",
            "uapi:/:wipe",
        ],
    ],
)
def test_push_with_geometry_flip_invalid_bounding_box(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    request,
    sqlite,
    scope: list,
):
    sqlite.init(
        {
            "TEST": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("GEO", sa.Text),
            ],
        }
    )

    sqlite.write(
        "TEST",
        [
            {"ID": 0, "GEO": "POINT(5980000 200000)"},
            {"ID": 1, "GEO": "POINT(5985000 210000)"},
            {"ID": 2, "GEO": "POINT(5990000 220000)"},
        ],
    )

    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        """
        d | r | b | m | property | type                  | ref | source | level | access | prepare
        datasets/push/geo/flip   |                       |     |        |       |        |
          |   |   | Test         |                       | id  |        | 4     |        |
          |   |   |   | id       | integer               |     |        | 4     | open   |    
          |   |   |   | geo      | geometry(point, 3346) |     |        | 2     | open   |
        """,
    )

    create_tabular_manifest(
        context,
        tmp_path / "manifest_push.csv",
        """
        d | r | b | m | property | type                  | ref | source | level | access | prepare
        datasets/push/geo/flip   |                       |     |        |       |        |
          | db                   | sql                   | sql |        |       |        |
          |   |   | Test         |                       | id  | TEST   | 4     |        |
          |   |   |   | id       | integer               |     | ID     | 4     | open   |    
          |   |   |   | geo      | geometry(point, 3346) |     | GEO    | 2     | open   | flip()
        """,
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, sqlite)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    remote.app.authorize(scope)

    result = cli.invoke(
        localrc,
        [
            "push",
            tmp_path / "manifest_push.csv",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
        ],
        fail=False,
    )
    assert result.exit_code == 1

    result = remote.app.get("datasets/push/geo/flip/Test")
    assert result.status_code == 200
    assert listdata(result, "id", "geo", sort=True) == []
    remote.app.delete("https://example.com/datasets/push/geo/flip/Test/:wipe")


@pytest.mark.parametrize(
    "scope",
    [
        [
            "spinta_set_meta_fields",
            "spinta_patch",
            "spinta_update",
            "spinta_insert",
            "spinta_getall",
            "spinta_search",
            "spinta_wipe",
        ],
        [
            "uapi:/:set_meta_fields",
            "uapi:/:patch",
            "uapi:/:update",
            "uapi:/:create",
            "uapi:/:getall",
            "uapi:/:search",
            "uapi:/:wipe",
        ],
    ],
)
def test_push_with_array_intermediate_table(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    request,
    array_geodb,
    scope: list,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        """
        d | r | b | m | property    | type    | ref      | source | level | access | prepare
        datasets/push/array/int     |         |          |        |       |        |
          |   |   | Country         |         | id       |        |       | open   |
          |   |   |   | id          | integer |          |        |       |        |    
          |   |   |   | name        | string  |          |        |       |        | 
          |   |   |   | languages[] | ref     | Language |        |       |        | 
          |   |   | Language        |         | id       |        |       | open   |
          |   |   |   | id          | integer |          |        |       |        |    
          |   |   |   | code        | string  |          |        |       |        | 
          |   |   |   | name        | string  |          |        |       |        |
        """,
    )

    create_tabular_manifest(
        context,
        tmp_path / "manifest_push.csv",
        """
        d | r | b | m | property    | type    | ref             | source          | level | access  | prepare
        datasets/push/array/int     |         |                 |                 |       |         |
          | db                      |         | sqlite          |                 |       |         |
          |   |   | Country         |         | id              | country         |       |         |
          |   |   |   | id          | integer |                 | id              |       | open    |    
          |   |   |   | name        | string  |                 | name            |       | open    | 
          |   |   |   | languages   | array   | CountryLanguage |                 |       | open    | 
          |   |   |   | languages[] | ref     | Language        |                 |       | open    | 
          |   |   | Language        |         | id              | language        |       |         |
          |   |   |   | id          | integer |                 | id              |       | open    |    
          |   |   |   | code        | string  |                 | code            |       | open    | 
          |   |   |   | name        | string  |                 | name            |       | open    |
          |   |   | CountryLanguage |         |                 | countrylanguage |       | private |
          |   |   |   | country     | ref     | Country         | country_id      |       |         |    
          |   |   |   | language    | ref     | Language        | language_id     |       |         |
        """,
    )

    # Configure local server with SQL backend
    rc = rc.fork({"default_page_size": 2})
    localrc = create_rc(rc, tmp_path, array_geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    remote.app.authorize(scope)
    result = cli.invoke(
        localrc,
        [
            "push",
            tmp_path / "manifest_push.csv",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
        ],
    )
    assert result.exit_code == 0

    resp = remote.app.get("datasets/push/array/int/Language")
    lang_data = resp.json()["_data"]
    lang_mapping = {lang["id"]: lang for lang in lang_data}
    result = remote.app.get("datasets/push/array/int/Country?expand(languages)")
    assert result.status_code == 200
    assert listdata(result, "id", "name", "languages", sort=True) == [
        (0, "Lithuania", [{"_id": lang_mapping[0]["_id"]}, {"_id": lang_mapping[1]["_id"]}]),
        (
            1,
            "England",
            [
                {"_id": lang_mapping[1]["_id"]},
            ],
        ),
        (2, "Poland", [{"_id": lang_mapping[1]["_id"]}, {"_id": lang_mapping[2]["_id"]}]),
    ]


@pytest.mark.parametrize(
    "scope",
    [
        [
            "spinta_set_meta_fields",
            "spinta_patch",
            "spinta_update",
            "spinta_insert",
            "spinta_getall",
            "spinta_search",
            "spinta_wipe",
        ],
        [
            "uapi:/:set_meta_fields",
            "uapi:/:patch",
            "uapi:/:update",
            "uapi:/:create",
            "uapi:/:getall",
            "uapi:/:search",
            "uapi:/:wipe",
        ],
    ],
)
def test_push_with_array_split(
    context,
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    request,
    array_geodb,
    scope: list,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        """
        d | r | b | m | property    | type    | ref            | source | level | access | prepare
        datasets/push/array/int     |         |                |        |       |        |
          |   |   | Country         |         | id             |        |       | open   |
          |   |   |   | id          | integer |                |        |       |        |    
          |   |   |   | name        | string  |                |        |       |        | 
          |   |   |   | languages[] | ref     | Language[code] |        |       |        | 
          |   |   | Language        |         | id             |        |       | open   |
          |   |   |   | id          | integer |                |        |       |        |    
          |   |   |   | code        | string  |                |        |       |        | 
          |   |   |   | name        | string  |                |        |       |        |
        """,
    )

    create_tabular_manifest(
        context,
        tmp_path / "manifest_push.csv",
        """
        d | r | b | m | property    | type    | ref             | source          | level | access  | prepare
        datasets/push/array/int     |         |                 |                 |       |         |
          | db                      |         | sqlite          |                 |       |         |
          |   |   | Country         |         | id              | country         |       |         |
          |   |   |   | id          | integer |                 | id              |       | open    |    
          |   |   |   | name        | string  |                 | name            |       | open    | 
          |   |   |   | languages   | array   |                 | languages       |       | open    | split(',') 
          |   |   |   | languages[] | ref     | Language[code]  |                 |       | open    | 
          |   |   | Language        |         | id              | language        |       |         |
          |   |   |   | id          | integer |                 | id              |       | open    |    
          |   |   |   | code        | string  |                 | code            |       | open    | 
          |   |   |   | name        | string  |                 | name            |       | open    |
        """,
    )

    # Configure local server with SQL backend
    rc = rc.fork({"default_page_size": 2})
    localrc = create_rc(rc, tmp_path, array_geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == "https://example.com/"
    remote.app.authorize(scope)
    result = cli.invoke(
        localrc,
        [
            "push",
            tmp_path / "manifest_push.csv",
            "-o",
            remote.url,
            "--credentials",
            remote.credsfile,
        ],
    )
    assert result.exit_code == 0

    resp = remote.app.get("datasets/push/array/int/Language")
    lang_data = resp.json()["_data"]
    lang_mapping = {lang["id"]: lang for lang in lang_data}
    result = remote.app.get("datasets/push/array/int/Country?expand(languages)")
    assert result.status_code == 200
    assert listdata(result, "id", "name", "languages", sort=True) == [
        (0, "Lithuania", [{"_id": lang_mapping[0]["_id"]}, {"_id": lang_mapping[1]["_id"]}]),
        (
            1,
            "England",
            [
                {"_id": lang_mapping[1]["_id"]},
            ],
        ),
        (2, "Poland", [{"_id": lang_mapping[1]["_id"]}, {"_id": lang_mapping[2]["_id"]}]),
    ]
