from __future__ import annotations

import csv
import datetime
import pathlib
from typing import Union

import pytest
import sqlalchemy as sa

from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.client import create_rc, configure_remote_server
from spinta.testing.datasets import create_sqlite_db, Sqlite
from spinta.testing.tabular import create_tabular_manifest


@pytest.fixture(scope="function")
def export_db():
    with create_sqlite_db(
        {
            "COUNTRY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("CODE", sa.Text),
                sa.Column("NAME", sa.Text),
                sa.Column("CREATED", sa.DateTime),
            ],
            "CITY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("CODE", sa.Text),
                sa.Column("NAME_LT", sa.Text),
                sa.Column("NAME_EN", sa.Text),
                sa.Column("COUNTRY", sa.Integer),
                sa.Column("EMPTY", sa.Integer),
            ],
        }
    ) as db:
        db.write(
            "COUNTRY",
            [
                {"ID": 0, "CODE": "LT", "NAME": "LITHUANIA", "CREATED": datetime.datetime(2020, 1, 1)},
                {"ID": 1, "CODE": "LV", "NAME": "LATVIA", "CREATED": datetime.datetime(2020, 1, 1)},
                {"ID": 2, "CODE": "PL", "NAME": "POLAND", "CREATED": datetime.datetime(2020, 1, 1)},
            ],
        )
        db.write(
            "CITY",
            [
                {"ID": 0, "CODE": "VLN", "NAME_LT": "VILNIUS", "NAME_EN": "VLN", "COUNTRY": 0},
                {"ID": 1, "CODE": "RYG", "NAME_LT": "RYGA", "NAME_EN": "RIGA", "COUNTRY": 1},
                {"ID": 2, "CODE": "WAR", "NAME_LT": "VARSUVA", "NAME_EN": "WARSAW", "COUNTRY": 2},
            ],
        )
        yield db


def _assert_files_exist(dir_path: pathlib.Path, files: list):
    for file in files:
        assert (dir_path / file).exists()


def _assert_files_not_exist(dir_path: pathlib.Path, files: list):
    for file in files:
        assert not (dir_path / file).exists()


def _assert_data(source_data: dict, target_data: dict, skip_columns: list = None):
    if skip_columns is None:
        skip_columns = []

    source_data = {key: value for key, value in source_data.items() if key not in skip_columns}
    target_data = {key: value for key, value in target_data.items() if key not in skip_columns}

    assert set(target_data.keys()).issubset(source_data.keys())

    for key, value in source_data.items():
        assert target_data.get(key) == value


def _assert_meta_keys_exists(data: list[dict], keys: list, nullable: list = None):
    if nullable is None:
        nullable = []

    assert all(all(key in row for key in keys) for row in data)
    assert all(all(value or key in nullable for key, value in row.items() if key in keys) for row in data)


def _extract_postgresql_data_from_file(dir_path: pathlib.Path, file_path: Union[pathlib.Path, str]) -> list[dict]:
    with (dir_path / file_path).open("r") as f:
        csv_reader = csv.DictReader(f)
        data = [row for row in csv_reader]
    return data


def test_export_no_format(context, rc, cli: SpintaCliRunner, tmp_path: pathlib.Path, export_db: Sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property| type     | ref     | source       | access
    datasets/gov/example    |          |         |              |
      | data                | sql      | sql     |              |
      |   |                 |          |         |              |
      |   |   | Country     |          | id      | COUNTRY      |
      |   |   |   | id      | integer  |         | ID           | open
      |   |   |   | code    | string   |         | CODE         | open
      |   |   |   | name    | string   |         | NAME         | open
      |   |   |   | created | datetime |         | CREATED      | open
      |   |   | City        |          | id      | CITY         |
      |   |   |   | id      | integer  |         | ID           | open
      |   |   |   | code    | string   |         | CODE         | open
      |   |   |   | name    | string   |         | NAME_LT      | open
      |   |   |   | country | ref      | Country | COUNTRY      | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, export_db, mode="external")
    result = cli.invoke(
        localrc,
        [
            "export",
        ],
        fail=False,
    )
    assert result.exit_code == 1
    assert "Export must be given an output format" in result.stderr


def test_export_both_formats(context, rc, cli: SpintaCliRunner, tmp_path: pathlib.Path, export_db: Sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property| type     | ref     | source       | access
    datasets/gov/example    |          |         |              |
      | data                | sql      | sql     |              |
      |   |                 |          |         |              |
      |   |   | Country     |          | id      | COUNTRY      |
      |   |   |   | id      | integer  |         | ID           | open
      |   |   |   | code    | string   |         | CODE         | open
      |   |   |   | name    | string   |         | NAME         | open
      |   |   |   | created | datetime |         | CREATED      | open
      |   |   | City        |          | id      | CITY         |
      |   |   |   | id      | integer  |         | ID           | open
      |   |   |   | code    | string   |         | CODE         | open
      |   |   |   | name    | string   |         | NAME_LT      | open
      |   |   |   | country | ref      | Country | COUNTRY      | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, export_db, mode="external")
    result = cli.invoke(localrc, ["export", "-f", "csv", "-b", "postgresql"], fail=False)
    assert result.exit_code == 1
    assert "Export can only output to one type" in result.stderr


def test_export_unsupported_format(context, rc, cli: SpintaCliRunner, tmp_path: pathlib.Path, export_db: Sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property| type     | ref     | source       | access
    datasets/gov/example    |          |         |              |
      | data                | sql      | sql     |              |
      |   |                 |          |         |              |
      |   |   | Country     |          | id      | COUNTRY      |
      |   |   |   | id      | integer  |         | ID           | open
      |   |   |   | code    | string   |         | CODE         | open
      |   |   |   | name    | string   |         | NAME         | open
      |   |   |   | created | datetime |         | CREATED      | open
      |   |   | City        |          | id      | CITY         |
      |   |   |   | id      | integer  |         | ID           | open
      |   |   |   | code    | string   |         | CODE         | open
      |   |   |   | name    | string   |         | NAME_LT      | open
      |   |   |   | country | ref      | Country | COUNTRY      | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, export_db, mode="external")
    result = cli.invoke(
        localrc,
        [
            "export",
            "-f",
            "not supported format",
        ],
        fail=False,
    )
    assert result.exit_code == 1
    assert "Unknown output format 'not supported format'" in result.stderr


def test_export_unsupported_backend(context, rc, cli: SpintaCliRunner, tmp_path: pathlib.Path, export_db: Sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property| type     | ref     | source       | access
    datasets/gov/example    |          |         |              |
      | data                | sql      | sql     |              |
      |   |                 |          |         |              |
      |   |   | Country     |          | id      | COUNTRY      |
      |   |   |   | id      | integer  |         | ID           | open
      |   |   |   | code    | string   |         | CODE         | open
      |   |   |   | name    | string   |         | NAME         | open
      |   |   |   | created | datetime |         | CREATED      | open
      |   |   | City        |          | id      | CITY         |
      |   |   |   | id      | integer  |         | ID           | open
      |   |   |   | code    | string   |         | CODE         | open
      |   |   |   | name    | string   |         | NAME_LT      | open
      |   |   |   | country | ref      | Country | COUNTRY      | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, export_db, mode="external")
    result = cli.invoke(
        localrc,
        [
            "export",
            "-b",
            "not supported format",
        ],
        fail=False,
    )
    assert result.exit_code == 1
    assert "Unknown output backend 'not supported format'" in result.stderr


def test_export_unknown_dataset(context, rc, cli: SpintaCliRunner, tmp_path: pathlib.Path, export_db: Sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property| type     | ref     | source       | access
    datasets/gov/example    |          |         |              |
      | data                | sql      | sql     |              |
      |   |                 |          |         |              |
      |   |   | Country     |          | id      | COUNTRY      |
      |   |   |   | id      | integer  |         | ID           | open
      |   |   |   | code    | string   |         | CODE         | open
      |   |   |   | name    | string   |         | NAME         | open
      |   |   |   | created | datetime |         | CREATED      | open
      |   |   | City        |          | id      | CITY         |
      |   |   |   | id      | integer  |         | ID           | open
      |   |   |   | code    | string   |         | CODE         | open
      |   |   |   | name    | string   |         | NAME_LT      | open
      |   |   |   | country | ref      | Country | COUNTRY      | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, export_db, mode="external")
    result = cli.invoke(localrc, ["export", "-b", "postgresql", "-d", "unknown"], fail=False)
    assert result.exit_code == 1
    assert "Node 'unknown' of type 'dataset' not found." in result.stderr


def test_export_no_output(context, rc, cli: SpintaCliRunner, tmp_path: pathlib.Path, export_db: Sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property| type     | ref     | source       | access
    datasets/gov/example    |          |         |              |
      | data                | sql      | sql     |              |
      |   |                 |          |         |              |
      |   |   | Country     |          | id      | COUNTRY      |
      |   |   |   | id      | integer  |         | ID           | open
      |   |   |   | code    | string   |         | CODE         | open
      |   |   |   | name    | string   |         | NAME         | open
      |   |   |   | created | datetime |         | CREATED      | open
      |   |   | City        |          | id      | CITY         |
      |   |   |   | id      | integer  |         | ID           | open
      |   |   |   | code    | string   |         | CODE         | open
      |   |   |   | name    | string   |         | NAME_LT      | open
      |   |   |   | country | ref      | Country | COUNTRY      | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, export_db, mode="external")
    result = cli.invoke(
        localrc,
        [
            "export",
            "-b",
            "postgresql",
        ],
        fail=False,
    )
    assert result.exit_code == 1
    assert "Output argument is required" in result.stderr


def test_export_postgresql(
    context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path,
    export_db: Sqlite,
    responses,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property| type     | ref     | source       | access
    datasets/gov/example    |          |         |              |
      | data                | sql      | sql     |              |
      |   |                 |          |         |              |
      |   |   | Country     |          | id      | COUNTRY      |
      |   |   |   | id      | integer  |         | ID           | open
      |   |   |   | code    | string   |         | CODE         | open
      |   |   |   | name    | string   |         | NAME         | open
      |   |   |   | created | datetime |         | CREATED      | open
      |   |   | City        |          | id      | CITY         |
      |   |   |   | id      | integer  |         | ID           | open
      |   |   |   | code    | string   |         | CODE         | open
      |   |   |   | name    | string   |         | NAME_LT      | open
      |   |   |   | country | ref      | Country | COUNTRY      | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, export_db, mode="internal")
    dir_path = tmp_path / "data"
    dir_path.mkdir()
    cli.invoke(
        localrc,
        [
            "export",
            "-b",
            "postgresql",
            "-o",
            dir_path,
        ],
    )
    _assert_files_exist(
        dir_path,
        [
            "datasets/gov/example/Country.csv",
            "datasets/gov/example/Country.changes.csv",
            "datasets/gov/example/City.csv",
            "datasets/gov/example/City.changes.csv",
        ],
    )
    data_meta_keys = ["_id", "_revision", "_txn", "_created", "_updated"]
    nullable_keys = ["_updated"]
    changes_meta_keys = ["_id", "_revision", "_txn", "_rid", "datetime", "action"]

    country_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/Country.csv")
    assert len(country_data) == 3
    lt_data = country_data[0]
    lv_data = country_data[1]
    pl_data = country_data[2]

    country_changes_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/Country.changes.csv")
    assert len(country_changes_data) == 3
    lt_change_data = country_changes_data[0]
    lv_change_data = country_changes_data[1]
    pl_change_data = country_changes_data[2]

    city_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/City.csv")
    assert len(city_data) == 3
    vln_data = city_data[0]
    ryg_data = city_data[1]
    war_data = city_data[2]

    city_changes_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/City.changes.csv")
    assert len(city_changes_data) == 3
    vln_change_data = city_changes_data[0]
    ryg_change_data = city_changes_data[1]
    war_change_data = city_changes_data[2]

    txn = lt_data["_txn"]
    # Check `Country` data
    _assert_meta_keys_exists(country_data, data_meta_keys, nullable_keys)
    _assert_data(
        lt_data,
        {"_txn": txn, "_updated": "", "id": "0", "code": "LT", "name": "LITHUANIA", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        lv_data,
        {"_txn": txn, "_updated": "", "id": "1", "code": "LV", "name": "LATVIA", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        pl_data,
        {"_txn": txn, "_updated": "", "id": "2", "code": "PL", "name": "POLAND", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )

    # Check `Country` changelog data
    _assert_meta_keys_exists(country_changes_data, changes_meta_keys)
    _assert_data(
        lt_change_data,
        {
            "_id": "1",
            "_revision": lt_data["_revision"],
            "_txn": txn,
            "_rid": lt_data["_id"],
            "action": "insert",
            "data": '{"id": 0, "code": "LT", "name": "LITHUANIA", "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        lv_change_data,
        {
            "_id": "2",
            "_revision": lv_data["_revision"],
            "_txn": txn,
            "_rid": lv_data["_id"],
            "action": "insert",
            "data": '{"id": 1, "code": "LV", "name": "LATVIA", "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        pl_change_data,
        {
            "_id": "3",
            "_revision": pl_data["_revision"],
            "_txn": txn,
            "_rid": pl_data["_id"],
            "action": "insert",
            "data": '{"id": 2, "code": "PL", "name": "POLAND", "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )

    # Check `City` data
    _assert_meta_keys_exists(city_data, data_meta_keys, nullable_keys)
    _assert_data(
        vln_data,
        {"_txn": txn, "_updated": "", "id": "0", "code": "VLN", "name": "VILNIUS", "country._id": lt_data["_id"]},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        ryg_data,
        {"_txn": txn, "_updated": "", "id": "1", "code": "RYG", "name": "RYGA", "country._id": lv_data["_id"]},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        war_data,
        {"_txn": txn, "_updated": "", "id": "2", "code": "WAR", "name": "VARSUVA", "country._id": pl_data["_id"]},
        skip_columns=["_id", "_revision", "_created"],
    )

    # Check `Country` changelog data
    _assert_meta_keys_exists(city_changes_data, changes_meta_keys)
    _assert_data(
        vln_change_data,
        {
            "_id": "1",
            "_revision": vln_data["_revision"],
            "_txn": txn,
            "_rid": vln_data["_id"],
            "action": "insert",
            "data": f'{{"id": 0, "code": "VLN", "name": "VILNIUS", "country": {{"_id": "{lt_data["_id"]}"}}}}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        ryg_change_data,
        {
            "_id": "2",
            "_revision": ryg_data["_revision"],
            "_txn": txn,
            "_rid": ryg_data["_id"],
            "action": "insert",
            "data": f'{{"id": 1, "code": "RYG", "name": "RYGA", "country": {{"_id": "{lv_data["_id"]}"}}}}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        war_change_data,
        {
            "_id": "3",
            "_revision": war_data["_revision"],
            "_txn": txn,
            "_rid": war_data["_id"],
            "action": "insert",
            "data": f'{{"id": 2, "code": "WAR", "name": "VARSUVA", "country": {{"_id": "{pl_data["_id"]}"}}}}',
        },
        skip_columns=["datetime"],
    )


def test_export_postgresql_empty_ref(
    context, rc, cli: SpintaCliRunner, tmp_path: pathlib.Path, export_db: Sqlite, responses
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property| type     | ref     | source       | access
    datasets/gov/example    |          |         |              |
      | data                | sql      | sql     |              |
      |   |                 |          |         |              |
      |   |   | Country     |          | id      | COUNTRY      |
      |   |   |   | id      | integer  |         | ID           | open
      |   |   | City        |          | id      | CITY         |
      |   |   |   | id      | integer  |         | ID           | open
      |   |   |   | code    | string   |         | CODE         | open
      |   |   |   | country | ref      | Country | EMPTY        | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, export_db, mode="external")
    dir_path = tmp_path / "data"
    dir_path.mkdir()
    cli.invoke(
        localrc,
        [
            "export",
            "-b",
            "postgresql",
            "-o",
            dir_path,
        ],
    )
    _assert_files_exist(
        dir_path,
        [
            "datasets/gov/example/Country.csv",
            "datasets/gov/example/Country.changes.csv",
            "datasets/gov/example/City.csv",
            "datasets/gov/example/City.changes.csv",
        ],
    )
    data_meta_keys = ["_id", "_revision", "_txn", "_created", "_updated"]
    nullable_keys = ["_updated"]
    changes_meta_keys = ["_id", "_revision", "_txn", "_rid", "datetime", "action"]

    country_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/Country.csv")
    assert len(country_data) == 3
    lt_data = country_data[0]
    lv_data = country_data[1]
    pl_data = country_data[2]

    country_changes_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/Country.changes.csv")
    assert len(country_changes_data) == 3
    lt_change_data = country_changes_data[0]
    lv_change_data = country_changes_data[1]
    pl_change_data = country_changes_data[2]

    city_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/City.csv")
    assert len(city_data) == 3
    vln_data = city_data[0]
    ryg_data = city_data[1]
    war_data = city_data[2]

    city_changes_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/City.changes.csv")
    assert len(city_changes_data) == 3
    vln_change_data = city_changes_data[0]
    ryg_change_data = city_changes_data[1]
    war_change_data = city_changes_data[2]

    txn_country = lt_data["_txn"]
    # Check `Country` data
    _assert_meta_keys_exists(country_data, data_meta_keys, nullable_keys)
    _assert_data(
        lt_data, {"_txn": txn_country, "_updated": "", "id": "0"}, skip_columns=["_id", "_revision", "_created"]
    )
    _assert_data(
        lv_data, {"_txn": txn_country, "_updated": "", "id": "1"}, skip_columns=["_id", "_revision", "_created"]
    )
    _assert_data(
        pl_data, {"_txn": txn_country, "_updated": "", "id": "2"}, skip_columns=["_id", "_revision", "_created"]
    )

    # Check `Country` changelog data
    _assert_meta_keys_exists(country_changes_data, changes_meta_keys)
    _assert_data(
        lt_change_data,
        {
            "_id": "1",
            "_revision": lt_data["_revision"],
            "_txn": txn_country,
            "_rid": lt_data["_id"],
            "action": "insert",
            "data": '{"id": 0}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        lv_change_data,
        {
            "_id": "2",
            "_revision": lv_data["_revision"],
            "_txn": txn_country,
            "_rid": lv_data["_id"],
            "action": "insert",
            "data": '{"id": 1}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        pl_change_data,
        {
            "_id": "3",
            "_revision": pl_data["_revision"],
            "_txn": txn_country,
            "_rid": pl_data["_id"],
            "action": "insert",
            "data": '{"id": 2}',
        },
        skip_columns=["datetime"],
    )

    txn_city = vln_data["_txn"]
    # Check `City` data
    _assert_meta_keys_exists(city_data, data_meta_keys, nullable_keys)
    _assert_data(
        vln_data,
        {"_txn": txn_city, "_updated": "", "id": "0", "code": "VLN", "country._id": ""},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        ryg_data,
        {"_txn": txn_city, "_updated": "", "id": "1", "code": "RYG", "country._id": ""},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        war_data,
        {"_txn": txn_city, "_updated": "", "id": "2", "code": "WAR", "country._id": ""},
        skip_columns=["_id", "_revision", "_created"],
    )

    # Check `Country` changelog data
    _assert_meta_keys_exists(city_changes_data, changes_meta_keys)
    _assert_data(
        vln_change_data,
        {
            "_id": "1",
            "_revision": vln_data["_revision"],
            "_txn": txn_city,
            "_rid": vln_data["_id"],
            "action": "insert",
            "data": '{"id": 0, "code": "VLN", "country": null}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        ryg_change_data,
        {
            "_id": "2",
            "_revision": ryg_data["_revision"],
            "_txn": txn_city,
            "_rid": ryg_data["_id"],
            "action": "insert",
            "data": '{"id": 1, "code": "RYG", "country": null}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        war_change_data,
        {
            "_id": "3",
            "_revision": war_data["_revision"],
            "_txn": txn_city,
            "_rid": war_data["_id"],
            "action": "insert",
            "data": '{"id": 2, "code": "WAR", "country": null}',
        },
        skip_columns=["datetime"],
    )


def test_export_postgresql_denorm(
    context, rc, cli: SpintaCliRunner, tmp_path: pathlib.Path, export_db: Sqlite, responses
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property     | type     | ref     | source       | access
    datasets/gov/example         |          |         |              |
      | data                     | sql      | sql     |              |
      |   |                      |          |         |              |
      |   |   | Country          |          | id      | COUNTRY      |
      |   |   |   | id           | integer  |         | ID           | open
      |   |   | City             |          | id      | CITY         |
      |   |   |   | id           | integer  |         | ID           | open
      |   |   |   | country      | ref      | Country | COUNTRY      | open
      |   |   |   | country.name | string   |         | NAME_LT      | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, export_db, mode="external")
    dir_path = tmp_path / "data"
    dir_path.mkdir()
    cli.invoke(
        localrc,
        [
            "export",
            "-b",
            "postgresql",
            "-o",
            dir_path,
        ],
    )
    _assert_files_exist(
        dir_path,
        [
            "datasets/gov/example/Country.csv",
            "datasets/gov/example/Country.changes.csv",
            "datasets/gov/example/City.csv",
            "datasets/gov/example/City.changes.csv",
        ],
    )
    data_meta_keys = ["_id", "_revision", "_txn", "_created", "_updated"]
    nullable_keys = ["_updated"]
    changes_meta_keys = ["_id", "_revision", "_txn", "_rid", "datetime", "action"]

    country_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/Country.csv")
    assert len(country_data) == 3
    lt_data = country_data[0]
    lv_data = country_data[1]
    pl_data = country_data[2]

    city_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/City.csv")
    assert len(city_data) == 3
    vln_data = city_data[0]
    ryg_data = city_data[1]
    war_data = city_data[2]

    city_changes_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/City.changes.csv")
    assert len(city_changes_data) == 3
    vln_change_data = city_changes_data[0]
    ryg_change_data = city_changes_data[1]
    war_change_data = city_changes_data[2]

    txn_city = vln_data["_txn"]
    # Check `City` data
    _assert_meta_keys_exists(city_data, data_meta_keys, nullable_keys)
    _assert_data(
        vln_data,
        {"_txn": txn_city, "_updated": "", "id": "0", "country.name": "VILNIUS", "country._id": lt_data["_id"]},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        ryg_data,
        {"_txn": txn_city, "_updated": "", "id": "1", "country.name": "RYGA", "country._id": lv_data["_id"]},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        war_data,
        {"_txn": txn_city, "_updated": "", "id": "2", "country.name": "VARSUVA", "country._id": pl_data["_id"]},
        skip_columns=["_id", "_revision", "_created"],
    )

    # Check `Country` changelog data
    _assert_meta_keys_exists(city_changes_data, changes_meta_keys)
    _assert_data(
        vln_change_data,
        {
            "_id": "1",
            "_revision": vln_data["_revision"],
            "_txn": txn_city,
            "_rid": vln_data["_id"],
            "action": "insert",
            "data": f'{{"id": 0, "country": {{"_id": "{lt_data["_id"]}", "name": "VILNIUS"}}}}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        ryg_change_data,
        {
            "_id": "2",
            "_revision": ryg_data["_revision"],
            "_txn": txn_city,
            "_rid": ryg_data["_id"],
            "action": "insert",
            "data": f'{{"id": 1, "country": {{"_id": "{lv_data["_id"]}", "name": "RYGA"}}}}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        war_change_data,
        {
            "_id": "3",
            "_revision": war_data["_revision"],
            "_txn": txn_city,
            "_rid": war_data["_id"],
            "action": "insert",
            "data": f'{{"id": 2, "country": {{"_id": "{pl_data["_id"]}", "name": "VARSUVA"}}}}',
        },
        skip_columns=["datetime"],
    )


def test_export_postgresql_text(
    context, rc, cli: SpintaCliRunner, tmp_path: pathlib.Path, export_db: Sqlite, responses
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property     | type     | ref     | source       | access | level
    datasets/gov/example         |          |         |              |        |
      | data                     | sql      | sql     |              |        |
      |   |                      |          |         |              |        |
      |   |   | City             |          | id      | CITY         |        |
      |   |   |   | id           | integer  |         | ID           | open   |
      |   |   |   | name         | text     |         |              | open   | 3
      |   |   |   | name@lt      | string   |         | NAME_LT      | open   |
      |   |   |   | name@en      | string   |         | NAME_EN      | open   |
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, export_db, mode="external")
    dir_path = tmp_path / "data"
    dir_path.mkdir()
    cli.invoke(
        localrc,
        [
            "export",
            "-b",
            "postgresql",
            "-o",
            dir_path,
        ],
    )
    _assert_files_exist(
        dir_path,
        [
            "datasets/gov/example/City.csv",
            "datasets/gov/example/City.changes.csv",
        ],
    )
    data_meta_keys = ["_id", "_revision", "_txn", "_created", "_updated"]
    nullable_keys = ["_updated"]
    changes_meta_keys = ["_id", "_revision", "_txn", "_rid", "datetime", "action"]

    city_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/City.csv")
    assert len(city_data) == 3
    vln_data = city_data[0]
    ryg_data = city_data[1]
    war_data = city_data[2]

    city_changes_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/City.changes.csv")
    assert len(city_changes_data) == 3
    vln_change_data = city_changes_data[0]
    ryg_change_data = city_changes_data[1]
    war_change_data = city_changes_data[2]

    txn_city = vln_data["_txn"]
    # Check `City` data
    _assert_meta_keys_exists(city_data, data_meta_keys, nullable_keys)
    _assert_data(
        vln_data,
        {"_txn": txn_city, "_updated": "", "id": "0", "name": f"{{'C': {None}, 'lt': 'VILNIUS', 'en': 'VLN'}}"},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        ryg_data,
        {"_txn": txn_city, "_updated": "", "id": "1", "name": f"{{'C': {None}, 'lt': 'RYGA', 'en': 'RIGA'}}"},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        war_data,
        {"_txn": txn_city, "_updated": "", "id": "2", "name": f"{{'C': {None}, 'lt': 'VARSUVA', 'en': 'WARSAW'}}"},
        skip_columns=["_id", "_revision", "_created"],
    )

    # Check `Country` changelog data
    _assert_meta_keys_exists(city_changes_data, changes_meta_keys)
    _assert_data(
        vln_change_data,
        {
            "_id": "1",
            "_revision": vln_data["_revision"],
            "_txn": txn_city,
            "_rid": vln_data["_id"],
            "action": "insert",
            "data": '{"id": 0, "name": {"C": null, "lt": "VILNIUS", "en": "VLN"}}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        ryg_change_data,
        {
            "_id": "2",
            "_revision": ryg_data["_revision"],
            "_txn": txn_city,
            "_rid": ryg_data["_id"],
            "action": "insert",
            "data": '{"id": 1, "name": {"C": null, "lt": "RYGA", "en": "RIGA"}}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        war_change_data,
        {
            "_id": "3",
            "_revision": war_data["_revision"],
            "_txn": txn_city,
            "_rid": war_data["_id"],
            "action": "insert",
            "data": '{"id": 2, "name": {"C": null, "lt": "VARSUVA", "en": "WARSAW"}}',
        },
        skip_columns=["datetime"],
    )


def test_export_postgresql_without_progress_bar(
    context, rc, cli: SpintaCliRunner, tmp_path: pathlib.Path, export_db: Sqlite, responses
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
        d | r | b | m | property     | type     | ref     | source       | access | level
        datasets/gov/example         |          |         |              |        |
          | data                     | sql      | sql     |              |        |
          |   |                      |          |         |              |        |
          |   |   | City             |          | id      | CITY         |        |
          |   |   |   | id           | integer  |         | ID           | open   |
          |   |   |   | name         | text     |         |              | open   | 3
          |   |   |   | name@lt      | string   |         | NAME_LT      | open   |
          |   |   |   | name@en      | string   |         | NAME_EN      | open   |
        """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, export_db, mode="external")
    dir_path = tmp_path / "data"
    dir_path.mkdir()
    result = cli.invoke(localrc, ["export", "-b", "postgresql", "-o", dir_path, "--no-progress-bar"])
    assert result.exit_code == 0
    assert result.stderr == ""


def test_export_postgresql_sync_data(
    context, rc, cli: SpintaCliRunner, tmp_path: pathlib.Path, export_db: Sqlite, responses, request
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property     | type     | ref     | source       | access
    datasets/gov/export          |          |         |              |
      | data                     | sql      | sql     |              |
      |   |                      |          |         |              |
      |   |   | Country          |          | id      |              |
      |   |   |   | id           | integer  |         |              | open
      |   |   | City             |          | id      | CITY         |
      |   |   |   | id           | integer  |         | ID           | open
      |   |   |   | country      | ref      | Country | COUNTRY      | open
    """),
    )

    internal_path = tmp_path / "internal"
    internal_path.mkdir(exist_ok=True)
    create_tabular_manifest(
        context,
        internal_path / "manifest.csv",
        striptable("""
    d | r | b | m | property     | type     | ref     | source       | access
    datasets/gov/export          |          |         |              |
      | data                     | sql      |         |              |
      |   |                      |          |         |              |
      |   |   | Country          |          | id      |              |
      |   |   |   | id           | integer  |         |              | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, export_db, mode="external")
    dir_path = tmp_path / "data"
    dir_path.mkdir()

    remoterc = create_rc(rc, internal_path, export_db, mode="internal")
    remote = configure_remote_server(cli, remoterc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    remote.app.authmodel("datasets/gov/export/Country", ["insert", "wipe"])
    resp = remote.app.post(
        "datasets/gov/export/Country",
        json={
            "id": 0,
        },
    )
    lt_id = resp.json()["_id"]
    resp = remote.app.post(
        "datasets/gov/export/Country",
        json={
            "id": 1,
        },
    )
    lv_id = resp.json()["_id"]
    resp = remote.app.post(
        "datasets/gov/export/Country",
        json={
            "id": 2,
        },
    )
    pl_id = resp.json()["_id"]

    cli.invoke(
        localrc,
        [
            "export",
            "-b",
            "postgresql",
            "-o",
            dir_path,
            "--credentials",
            remote.credsfile,
            "-i",
            remote.url,
        ],
    )
    _assert_files_exist(
        dir_path,
        [
            "datasets/gov/export/City.csv",
            "datasets/gov/export/City.changes.csv",
        ],
    )
    data_meta_keys = ["_id", "_revision", "_txn", "_created", "_updated"]
    nullable_keys = ["_updated"]
    changes_meta_keys = ["_id", "_revision", "_txn", "_rid", "datetime", "action"]

    city_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/export/City.csv")
    assert len(city_data) == 3
    vln_data = city_data[0]
    ryg_data = city_data[1]
    war_data = city_data[2]

    city_changes_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/export/City.changes.csv")
    assert len(city_changes_data) == 3
    vln_change_data = city_changes_data[0]
    ryg_change_data = city_changes_data[1]
    war_change_data = city_changes_data[2]

    txn_city = vln_data["_txn"]
    # Check `City` data
    _assert_meta_keys_exists(city_data, data_meta_keys, nullable_keys)
    _assert_data(
        vln_data,
        {"_txn": txn_city, "_updated": "", "id": "0", "country._id": lt_id},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        ryg_data,
        {"_txn": txn_city, "_updated": "", "id": "1", "country._id": lv_id},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        war_data,
        {"_txn": txn_city, "_updated": "", "id": "2", "country._id": pl_id},
        skip_columns=["_id", "_revision", "_created"],
    )

    # Check `Country` changelog data
    _assert_meta_keys_exists(city_changes_data, changes_meta_keys)
    _assert_data(
        vln_change_data,
        {
            "_id": "1",
            "_revision": vln_data["_revision"],
            "_txn": txn_city,
            "_rid": vln_data["_id"],
            "action": "insert",
            "data": f'{{"id": 0, "country": {{"_id": "{lt_id}"}}}}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        ryg_change_data,
        {
            "_id": "2",
            "_revision": ryg_data["_revision"],
            "_txn": txn_city,
            "_rid": ryg_data["_id"],
            "action": "insert",
            "data": f'{{"id": 1, "country": {{"_id": "{lv_id}"}}}}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        war_change_data,
        {
            "_id": "3",
            "_revision": war_data["_revision"],
            "_txn": txn_city,
            "_rid": war_data["_id"],
            "action": "insert",
            "data": f'{{"id": 2, "country": {{"_id": "{pl_id}"}}}}',
        },
        skip_columns=["datetime"],
    )


def test_export_postgresql_required_sync(
    context, rc, cli: SpintaCliRunner, tmp_path: pathlib.Path, export_db: Sqlite, responses, request
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property     | type     | ref     | source       | access
    datasets/gov/export/nsync    |          |         |              |
      | data                     | sql      | sql     |              |
      |   |                      |          |         |              |
      |   |   | Country          |          | id      |              |
      |   |   |   | id           | integer  |         |              | open
      |   |   | City             |          | id      | CITY         |
      |   |   |   | id           | integer  |         | ID           | open
      |   |   |   | country      | ref      | Country | COUNTRY      | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, export_db, mode="external")
    dir_path = tmp_path / "data"
    dir_path.mkdir()
    result = cli.invoke(
        localrc,
        [
            "export",
            "-b",
            "postgresql",
            "-o",
            dir_path,
        ],
        fail=False,
    )
    assert result.exit_code == 1
    assert "Detected some models, that might require synchronization step" in result.stderr


def test_export_postgresql_invalid_output(
    context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path,
    export_db: Sqlite,
    responses,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property     | type     | ref     | source       | access
    datasets/gov/export/nsync    |          |         |              |
      | data                     | sql      | sql     |              |
      |   |                      |          |         |              |
      |   |   | City             |          | id      | CITY         |
      |   |   |   | id           | integer  |         | ID           | open
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, export_db, mode="external")
    dir_path = tmp_path / "data"
    result = cli.invoke(
        localrc,
        [
            "export",
            "-b",
            "postgresql",
            "-o",
            dir_path,
        ],
        fail=False,
    )
    assert result.exit_code == 1
    assert "directory does not exist" in result.stderr


def test_export_postgresql_access_private(
    context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path,
    export_db: Sqlite,
    responses,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property| type     | ref     | source       | access
    datasets/gov/example    |          |         |              |
      | data                | sql      | sql     |              |
      |   |                 |          |         |              |
      |   |   | Country     |          | id      | COUNTRY      | open
      |   |   |   | id      | integer  |         | ID           | 
      |   |   |   | code    | string   |         | CODE         | protected
      |   |   |   | name    | string   |         | NAME         | private
      |   |   |   | created | datetime |         | CREATED      | 
      |   |   | City        |          | id      | CITY         | open
      |   |   |   | id      | integer  |         | ID           | 
      |   |   |   | code    | string   |         | CODE         | protected
      |   |   |   | name    | string   |         | NAME_LT      | 
      |   |   |   | country | ref      | Country | COUNTRY      | public
      |   |   | CountryPr   |          | id      | COUNTRY      | private
      |   |   |   | id      | integer  |         | ID           | 
      |   |   |   | code    | string   |         | CODE         | 
      |   |   |   | name    | string   |         | NAME         | 
      |   |   |   | created | datetime |         | CREATED      | 
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, export_db, mode="internal")
    dir_path = tmp_path / "data"
    dir_path.mkdir()
    cli.invoke(localrc, ["export", "-b", "postgresql", "-o", dir_path, "--access", "private"])
    _assert_files_exist(
        dir_path,
        [
            "datasets/gov/example/Country.csv",
            "datasets/gov/example/Country.changes.csv",
            "datasets/gov/example/City.csv",
            "datasets/gov/example/City.changes.csv",
            "datasets/gov/example/CountryPr.csv",
            "datasets/gov/example/CountryPr.changes.csv",
        ],
    )
    data_meta_keys = ["_id", "_revision", "_txn", "_created", "_updated"]
    nullable_keys = ["_updated"]
    changes_meta_keys = ["_id", "_revision", "_txn", "_rid", "datetime", "action"]

    country_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/Country.csv")
    assert len(country_data) == 3
    lt_data = country_data[0]
    lv_data = country_data[1]
    pl_data = country_data[2]

    country_changes_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/Country.changes.csv")
    assert len(country_changes_data) == 3
    lt_change_data = country_changes_data[0]
    lv_change_data = country_changes_data[1]
    pl_change_data = country_changes_data[2]

    country_pr_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/CountryPr.csv")
    assert len(country_pr_data) == 3
    lt_pr_data = country_pr_data[0]
    lv_pr_data = country_pr_data[1]
    pl_pr_data = country_pr_data[2]

    country_pr_changes_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/CountryPr.changes.csv")
    assert len(country_pr_changes_data) == 3
    lt_pr_change_data = country_pr_changes_data[0]
    lv_pr_change_data = country_pr_changes_data[1]
    pl_pr_change_data = country_pr_changes_data[2]

    city_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/City.csv")
    assert len(city_data) == 3
    vln_data = city_data[0]
    ryg_data = city_data[1]
    war_data = city_data[2]

    city_changes_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/City.changes.csv")
    assert len(city_changes_data) == 3
    vln_change_data = city_changes_data[0]
    ryg_change_data = city_changes_data[1]
    war_change_data = city_changes_data[2]

    txn = lt_data["_txn"]
    # Check `Country` data
    _assert_meta_keys_exists(country_data, data_meta_keys, nullable_keys)
    _assert_data(
        lt_data,
        {"_txn": txn, "_updated": "", "id": "0", "code": "LT", "name": "LITHUANIA", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        lv_data,
        {"_txn": txn, "_updated": "", "id": "1", "code": "LV", "name": "LATVIA", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        pl_data,
        {"_txn": txn, "_updated": "", "id": "2", "code": "PL", "name": "POLAND", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )

    # Check `Country` changelog data
    _assert_meta_keys_exists(country_changes_data, changes_meta_keys)
    _assert_data(
        lt_change_data,
        {
            "_id": "1",
            "_revision": lt_data["_revision"],
            "_txn": txn,
            "_rid": lt_data["_id"],
            "action": "insert",
            "data": '{"id": 0, "code": "LT", "name": "LITHUANIA", "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        lv_change_data,
        {
            "_id": "2",
            "_revision": lv_data["_revision"],
            "_txn": txn,
            "_rid": lv_data["_id"],
            "action": "insert",
            "data": '{"id": 1, "code": "LV", "name": "LATVIA", "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        pl_change_data,
        {
            "_id": "3",
            "_revision": pl_data["_revision"],
            "_txn": txn,
            "_rid": pl_data["_id"],
            "action": "insert",
            "data": '{"id": 2, "code": "PL", "name": "POLAND", "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )

    # Check `CountryPr` data
    _assert_meta_keys_exists(country_pr_data, data_meta_keys, nullable_keys)
    _assert_data(
        lt_pr_data,
        {"_txn": txn, "_updated": "", "id": "0", "code": "LT", "name": "LITHUANIA", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        lv_pr_data,
        {"_txn": txn, "_updated": "", "id": "1", "code": "LV", "name": "LATVIA", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        pl_pr_data,
        {"_txn": txn, "_updated": "", "id": "2", "code": "PL", "name": "POLAND", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )

    # Check `CountryPr` changelog data
    _assert_meta_keys_exists(country_pr_changes_data, changes_meta_keys)
    _assert_data(
        lt_pr_change_data,
        {
            "_id": "1",
            "_revision": lt_pr_data["_revision"],
            "_txn": txn,
            "_rid": lt_pr_data["_id"],
            "action": "insert",
            "data": '{"id": 0, "code": "LT", "name": "LITHUANIA", "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        lv_pr_change_data,
        {
            "_id": "2",
            "_revision": lv_pr_data["_revision"],
            "_txn": txn,
            "_rid": lv_pr_data["_id"],
            "action": "insert",
            "data": '{"id": 1, "code": "LV", "name": "LATVIA", "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        pl_pr_change_data,
        {
            "_id": "3",
            "_revision": pl_pr_data["_revision"],
            "_txn": txn,
            "_rid": pl_pr_data["_id"],
            "action": "insert",
            "data": '{"id": 2, "code": "PL", "name": "POLAND", "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )

    # Check `City` data
    _assert_meta_keys_exists(city_data, data_meta_keys, nullable_keys)
    _assert_data(
        vln_data,
        {"_txn": txn, "_updated": "", "id": "0", "code": "VLN", "name": "VILNIUS", "country._id": lt_data["_id"]},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        ryg_data,
        {"_txn": txn, "_updated": "", "id": "1", "code": "RYG", "name": "RYGA", "country._id": lv_data["_id"]},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        war_data,
        {"_txn": txn, "_updated": "", "id": "2", "code": "WAR", "name": "VARSUVA", "country._id": pl_data["_id"]},
        skip_columns=["_id", "_revision", "_created"],
    )

    # Check `Country` changelog data
    _assert_meta_keys_exists(city_changes_data, changes_meta_keys)
    _assert_data(
        vln_change_data,
        {
            "_id": "1",
            "_revision": vln_data["_revision"],
            "_txn": txn,
            "_rid": vln_data["_id"],
            "action": "insert",
            "data": f'{{"id": 0, "code": "VLN", "name": "VILNIUS", "country": {{"_id": "{lt_data["_id"]}"}}}}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        ryg_change_data,
        {
            "_id": "2",
            "_revision": ryg_data["_revision"],
            "_txn": txn,
            "_rid": ryg_data["_id"],
            "action": "insert",
            "data": f'{{"id": 1, "code": "RYG", "name": "RYGA", "country": {{"_id": "{lv_data["_id"]}"}}}}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        war_change_data,
        {
            "_id": "3",
            "_revision": war_data["_revision"],
            "_txn": txn,
            "_rid": war_data["_id"],
            "action": "insert",
            "data": f'{{"id": 2, "code": "WAR", "name": "VARSUVA", "country": {{"_id": "{pl_data["_id"]}"}}}}',
        },
        skip_columns=["datetime"],
    )


def test_export_postgresql_access_protected(
    context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path,
    export_db: Sqlite,
    responses,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property| type     | ref     | source       | access
    datasets/gov/example    |          |         |              |
      | data                | sql      | sql     |              |
      |   |                 |          |         |              |
      |   |   | Country     |          | id      | COUNTRY      | open
      |   |   |   | id      | integer  |         | ID           | 
      |   |   |   | code    | string   |         | CODE         | protected
      |   |   |   | name    | string   |         | NAME         | private
      |   |   |   | created | datetime |         | CREATED      | 
      |   |   | City        |          | id      | CITY         | open
      |   |   |   | id      | integer  |         | ID           | 
      |   |   |   | code    | string   |         | CODE         | protected
      |   |   |   | name    | string   |         | NAME_LT      | 
      |   |   |   | country | ref      | Country | COUNTRY      | public
      |   |   | CountryPr   |          | id      | COUNTRY      | private
      |   |   |   | id      | integer  |         | ID           | 
      |   |   |   | code    | string   |         | CODE         | 
      |   |   |   | name    | string   |         | NAME         | 
      |   |   |   | created | datetime |         | CREATED      | 
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, export_db, mode="internal")
    dir_path = tmp_path / "data"
    dir_path.mkdir()
    cli.invoke(localrc, ["export", "-b", "postgresql", "-o", dir_path, "--access", "protected"])
    _assert_files_exist(
        dir_path,
        [
            "datasets/gov/example/Country.csv",
            "datasets/gov/example/Country.changes.csv",
            "datasets/gov/example/City.csv",
            "datasets/gov/example/City.changes.csv",
        ],
    )

    _assert_files_not_exist(
        dir_path,
        [
            "datasets/gov/example/CountryPr.csv",
            "datasets/gov/example/CountryPr.changes.csv",
        ],
    )
    data_meta_keys = ["_id", "_revision", "_txn", "_created", "_updated"]
    nullable_keys = ["_updated"]
    changes_meta_keys = ["_id", "_revision", "_txn", "_rid", "datetime", "action"]

    country_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/Country.csv")
    assert len(country_data) == 3
    lt_data = country_data[0]
    lv_data = country_data[1]
    pl_data = country_data[2]

    country_changes_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/Country.changes.csv")
    assert len(country_changes_data) == 3
    lt_change_data = country_changes_data[0]
    lv_change_data = country_changes_data[1]
    pl_change_data = country_changes_data[2]

    city_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/City.csv")
    assert len(city_data) == 3
    vln_data = city_data[0]
    ryg_data = city_data[1]
    war_data = city_data[2]

    city_changes_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/City.changes.csv")
    assert len(city_changes_data) == 3
    vln_change_data = city_changes_data[0]
    ryg_change_data = city_changes_data[1]
    war_change_data = city_changes_data[2]

    txn = lt_data["_txn"]
    # Check `Country` data
    _assert_meta_keys_exists(country_data, data_meta_keys, nullable_keys)
    _assert_data(
        lt_data,
        {"_txn": txn, "_updated": "", "id": "0", "code": "LT", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        lv_data,
        {"_txn": txn, "_updated": "", "id": "1", "code": "LV", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        pl_data,
        {"_txn": txn, "_updated": "", "id": "2", "code": "PL", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )

    # Check `Country` changelog data
    _assert_meta_keys_exists(country_changes_data, changes_meta_keys)
    _assert_data(
        lt_change_data,
        {
            "_id": "1",
            "_revision": lt_data["_revision"],
            "_txn": txn,
            "_rid": lt_data["_id"],
            "action": "insert",
            "data": '{"id": 0, "code": "LT", "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        lv_change_data,
        {
            "_id": "2",
            "_revision": lv_data["_revision"],
            "_txn": txn,
            "_rid": lv_data["_id"],
            "action": "insert",
            "data": '{"id": 1, "code": "LV", "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        pl_change_data,
        {
            "_id": "3",
            "_revision": pl_data["_revision"],
            "_txn": txn,
            "_rid": pl_data["_id"],
            "action": "insert",
            "data": '{"id": 2, "code": "PL", "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )

    # Check `City` data
    _assert_meta_keys_exists(city_data, data_meta_keys, nullable_keys)
    _assert_data(
        vln_data,
        {"_txn": txn, "_updated": "", "id": "0", "code": "VLN", "name": "VILNIUS", "country._id": lt_data["_id"]},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        ryg_data,
        {"_txn": txn, "_updated": "", "id": "1", "code": "RYG", "name": "RYGA", "country._id": lv_data["_id"]},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        war_data,
        {"_txn": txn, "_updated": "", "id": "2", "code": "WAR", "name": "VARSUVA", "country._id": pl_data["_id"]},
        skip_columns=["_id", "_revision", "_created"],
    )

    # Check `Country` changelog data
    _assert_meta_keys_exists(city_changes_data, changes_meta_keys)
    _assert_data(
        vln_change_data,
        {
            "_id": "1",
            "_revision": vln_data["_revision"],
            "_txn": txn,
            "_rid": vln_data["_id"],
            "action": "insert",
            "data": f'{{"id": 0, "code": "VLN", "name": "VILNIUS", "country": {{"_id": "{lt_data["_id"]}"}}}}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        ryg_change_data,
        {
            "_id": "2",
            "_revision": ryg_data["_revision"],
            "_txn": txn,
            "_rid": ryg_data["_id"],
            "action": "insert",
            "data": f'{{"id": 1, "code": "RYG", "name": "RYGA", "country": {{"_id": "{lv_data["_id"]}"}}}}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        war_change_data,
        {
            "_id": "3",
            "_revision": war_data["_revision"],
            "_txn": txn,
            "_rid": war_data["_id"],
            "action": "insert",
            "data": f'{{"id": 2, "code": "WAR", "name": "VARSUVA", "country": {{"_id": "{pl_data["_id"]}"}}}}',
        },
        skip_columns=["datetime"],
    )


def test_export_postgresql_access_public(
    context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path,
    export_db: Sqlite,
    responses,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property| type     | ref     | source       | access
    datasets/gov/example    |          |         |              |
      | data                | sql      | sql     |              |
      |   |                 |          |         |              |
      |   |   | Country     |          | id      | COUNTRY      | open
      |   |   |   | id      | integer  |         | ID           | 
      |   |   |   | code    | string   |         | CODE         | protected
      |   |   |   | name    | string   |         | NAME         | private
      |   |   |   | created | datetime |         | CREATED      | 
      |   |   | City        |          | id      | CITY         | open
      |   |   |   | id      | integer  |         | ID           | 
      |   |   |   | code    | string   |         | CODE         | protected
      |   |   |   | name    | string   |         | NAME_LT      | 
      |   |   |   | country | ref      | Country | COUNTRY      | public
      |   |   | CountryPr   |          | id      | COUNTRY      | private
      |   |   |   | id      | integer  |         | ID           | 
      |   |   |   | code    | string   |         | CODE         | 
      |   |   |   | name    | string   |         | NAME         | 
      |   |   |   | created | datetime |         | CREATED      | 
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, export_db, mode="internal")
    dir_path = tmp_path / "data"
    dir_path.mkdir()
    cli.invoke(localrc, ["export", "-b", "postgresql", "-o", dir_path, "--access", "public"])
    _assert_files_exist(
        dir_path,
        [
            "datasets/gov/example/Country.csv",
            "datasets/gov/example/Country.changes.csv",
            "datasets/gov/example/City.csv",
            "datasets/gov/example/City.changes.csv",
        ],
    )

    _assert_files_not_exist(
        dir_path,
        [
            "datasets/gov/example/CountryPr.csv",
            "datasets/gov/example/CountryPr.changes.csv",
        ],
    )
    data_meta_keys = ["_id", "_revision", "_txn", "_created", "_updated"]
    nullable_keys = ["_updated"]
    changes_meta_keys = ["_id", "_revision", "_txn", "_rid", "datetime", "action"]

    country_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/Country.csv")
    assert len(country_data) == 3
    lt_data = country_data[0]
    lv_data = country_data[1]
    pl_data = country_data[2]

    country_changes_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/Country.changes.csv")
    assert len(country_changes_data) == 3
    lt_change_data = country_changes_data[0]
    lv_change_data = country_changes_data[1]
    pl_change_data = country_changes_data[2]

    city_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/City.csv")
    assert len(city_data) == 3
    vln_data = city_data[0]
    ryg_data = city_data[1]
    war_data = city_data[2]

    city_changes_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/City.changes.csv")
    assert len(city_changes_data) == 3
    vln_change_data = city_changes_data[0]
    ryg_change_data = city_changes_data[1]
    war_change_data = city_changes_data[2]

    txn = lt_data["_txn"]
    # Check `Country` data
    _assert_meta_keys_exists(country_data, data_meta_keys, nullable_keys)
    _assert_data(
        lt_data,
        {"_txn": txn, "_updated": "", "id": "0", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        lv_data,
        {"_txn": txn, "_updated": "", "id": "1", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        pl_data,
        {"_txn": txn, "_updated": "", "id": "2", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )

    # Check `Country` changelog data
    _assert_meta_keys_exists(country_changes_data, changes_meta_keys)
    _assert_data(
        lt_change_data,
        {
            "_id": "1",
            "_revision": lt_data["_revision"],
            "_txn": txn,
            "_rid": lt_data["_id"],
            "action": "insert",
            "data": '{"id": 0, "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        lv_change_data,
        {
            "_id": "2",
            "_revision": lv_data["_revision"],
            "_txn": txn,
            "_rid": lv_data["_id"],
            "action": "insert",
            "data": '{"id": 1, "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        pl_change_data,
        {
            "_id": "3",
            "_revision": pl_data["_revision"],
            "_txn": txn,
            "_rid": pl_data["_id"],
            "action": "insert",
            "data": '{"id": 2, "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )

    # Check `City` data
    _assert_meta_keys_exists(city_data, data_meta_keys, nullable_keys)
    _assert_data(
        vln_data,
        {"_txn": txn, "_updated": "", "id": "0", "name": "VILNIUS", "country._id": lt_data["_id"]},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        ryg_data,
        {"_txn": txn, "_updated": "", "id": "1", "name": "RYGA", "country._id": lv_data["_id"]},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        war_data,
        {"_txn": txn, "_updated": "", "id": "2", "name": "VARSUVA", "country._id": pl_data["_id"]},
        skip_columns=["_id", "_revision", "_created"],
    )

    # Check `Country` changelog data
    _assert_meta_keys_exists(city_changes_data, changes_meta_keys)
    _assert_data(
        vln_change_data,
        {
            "_id": "1",
            "_revision": vln_data["_revision"],
            "_txn": txn,
            "_rid": vln_data["_id"],
            "action": "insert",
            "data": f'{{"id": 0, "name": "VILNIUS", "country": {{"_id": "{lt_data["_id"]}"}}}}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        ryg_change_data,
        {
            "_id": "2",
            "_revision": ryg_data["_revision"],
            "_txn": txn,
            "_rid": ryg_data["_id"],
            "action": "insert",
            "data": f'{{"id": 1, "name": "RYGA", "country": {{"_id": "{lv_data["_id"]}"}}}}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        war_change_data,
        {
            "_id": "3",
            "_revision": war_data["_revision"],
            "_txn": txn,
            "_rid": war_data["_id"],
            "action": "insert",
            "data": f'{{"id": 2, "name": "VARSUVA", "country": {{"_id": "{pl_data["_id"]}"}}}}',
        },
        skip_columns=["datetime"],
    )


def test_export_postgresql_access_open(
    context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path,
    export_db: Sqlite,
    responses,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property| type     | ref     | source       | access
    datasets/gov/example    |          |         |              |
      | data                | sql      | sql     |              |
      |   |                 |          |         |              |
      |   |   | Country     |          | id      | COUNTRY      | open
      |   |   |   | id      | integer  |         | ID           | 
      |   |   |   | code    | string   |         | CODE         | protected
      |   |   |   | name    | string   |         | NAME         | private
      |   |   |   | created | datetime |         | CREATED      | 
      |   |   | City        |          | id      | CITY         | open
      |   |   |   | id      | integer  |         | ID           | 
      |   |   |   | code    | string   |         | CODE         | protected
      |   |   |   | name    | string   |         | NAME_LT      | 
      |   |   |   | country | ref      | Country | COUNTRY      | public
      |   |   | CountryPr   |          | id      | COUNTRY      | private
      |   |   |   | id      | integer  |         | ID           | 
      |   |   |   | code    | string   |         | CODE         | 
      |   |   |   | name    | string   |         | NAME         | 
      |   |   |   | created | datetime |         | CREATED      | 
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, export_db, mode="internal")
    dir_path = tmp_path / "data"
    dir_path.mkdir()
    cli.invoke(localrc, ["export", "-b", "postgresql", "-o", dir_path, "--access", "open"])
    _assert_files_exist(
        dir_path,
        [
            "datasets/gov/example/Country.csv",
            "datasets/gov/example/Country.changes.csv",
            "datasets/gov/example/City.csv",
            "datasets/gov/example/City.changes.csv",
        ],
    )

    _assert_files_not_exist(
        dir_path,
        [
            "datasets/gov/example/CountryPr.csv",
            "datasets/gov/example/CountryPr.changes.csv",
        ],
    )
    data_meta_keys = ["_id", "_revision", "_txn", "_created", "_updated"]
    nullable_keys = ["_updated"]
    changes_meta_keys = ["_id", "_revision", "_txn", "_rid", "datetime", "action"]

    country_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/Country.csv")
    assert len(country_data) == 3
    lt_data = country_data[0]
    lv_data = country_data[1]
    pl_data = country_data[2]

    country_changes_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/Country.changes.csv")
    assert len(country_changes_data) == 3
    lt_change_data = country_changes_data[0]
    lv_change_data = country_changes_data[1]
    pl_change_data = country_changes_data[2]

    city_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/City.csv")
    assert len(city_data) == 3
    vln_data = city_data[0]
    ryg_data = city_data[1]
    war_data = city_data[2]

    city_changes_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/City.changes.csv")
    assert len(city_changes_data) == 3
    vln_change_data = city_changes_data[0]
    ryg_change_data = city_changes_data[1]
    war_change_data = city_changes_data[2]

    txn = lt_data["_txn"]
    # Check `Country` data
    _assert_meta_keys_exists(country_data, data_meta_keys, nullable_keys)
    _assert_data(
        lt_data,
        {"_txn": txn, "_updated": "", "id": "0", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        lv_data,
        {"_txn": txn, "_updated": "", "id": "1", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        pl_data,
        {"_txn": txn, "_updated": "", "id": "2", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )

    # Check `Country` changelog data
    _assert_meta_keys_exists(country_changes_data, changes_meta_keys)
    _assert_data(
        lt_change_data,
        {
            "_id": "1",
            "_revision": lt_data["_revision"],
            "_txn": txn,
            "_rid": lt_data["_id"],
            "action": "insert",
            "data": '{"id": 0, "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        lv_change_data,
        {
            "_id": "2",
            "_revision": lv_data["_revision"],
            "_txn": txn,
            "_rid": lv_data["_id"],
            "action": "insert",
            "data": '{"id": 1, "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        pl_change_data,
        {
            "_id": "3",
            "_revision": pl_data["_revision"],
            "_txn": txn,
            "_rid": pl_data["_id"],
            "action": "insert",
            "data": '{"id": 2, "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )

    # Check `City` data
    _assert_meta_keys_exists(city_data, data_meta_keys, nullable_keys)
    _assert_data(
        vln_data,
        {"_txn": txn, "_updated": "", "id": "0", "name": "VILNIUS"},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        ryg_data,
        {"_txn": txn, "_updated": "", "id": "1", "name": "RYGA"},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        war_data,
        {"_txn": txn, "_updated": "", "id": "2", "name": "VARSUVA"},
        skip_columns=["_id", "_revision", "_created"],
    )

    # Check `Country` changelog data
    _assert_meta_keys_exists(city_changes_data, changes_meta_keys)
    _assert_data(
        vln_change_data,
        {
            "_id": "1",
            "_revision": vln_data["_revision"],
            "_txn": txn,
            "_rid": vln_data["_id"],
            "action": "insert",
            "data": '{"id": 0, "name": "VILNIUS"}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        ryg_change_data,
        {
            "_id": "2",
            "_revision": ryg_data["_revision"],
            "_txn": txn,
            "_rid": ryg_data["_id"],
            "action": "insert",
            "data": '{"id": 1, "name": "RYGA"}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        war_change_data,
        {
            "_id": "3",
            "_revision": war_data["_revision"],
            "_txn": txn,
            "_rid": war_data["_id"],
            "action": "insert",
            "data": '{"id": 2, "name": "VARSUVA"}',
        },
        skip_columns=["datetime"],
    )


def test_export_postgresql_access_nested(
    context,
    rc,
    cli: SpintaCliRunner,
    tmp_path: pathlib.Path,
    export_db: Sqlite,
    responses,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property            | type     | ref     | source  | access
    datasets/gov/example                |          |         |         |
      | data                            | sql      | sql     |         |
      |   |                             |          |         |         |
      |   |   | Country                 |          | id      | COUNTRY | open
      |   |   |   | id                  | integer  |         | ID      | 
      |   |   |   | code                | string   |         | CODE    | 
      |   |   |   | name                | string   |         | NAME    | private
      |   |   |   | created             | datetime |         | CREATED | 
      |   |   | City                    |          | id      | CITY    | open
      |   |   |   | id                  | integer  |         | ID      | 
      |   |   |   | code                | string   |         | CODE    | 
      |   |   |   | name                | string   |         | NAME_LT | 
      |   |   |   | country             | ref      | Country | COUNTRY | 
      |   |   |   | country.test        | integer  |         | ID      | 
      |   |   |   | country.private     | integer  |         | ID      | private 
      |   |   |   | country.obj         | object   |         |         | 
      |   |   |   | country.obj.test    | integer  |         | ID      | 
      |   |   |   | country.obj.private | integer  |         | ID      | private 
    """),
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, export_db, mode="internal")
    dir_path = tmp_path / "data"
    dir_path.mkdir()
    cli.invoke(localrc, ["export", "-b", "postgresql", "-o", dir_path, "--access", "open"])
    _assert_files_exist(
        dir_path,
        [
            "datasets/gov/example/Country.csv",
            "datasets/gov/example/Country.changes.csv",
            "datasets/gov/example/City.csv",
            "datasets/gov/example/City.changes.csv",
        ],
    )
    data_meta_keys = ["_id", "_revision", "_txn", "_created", "_updated"]
    nullable_keys = ["_updated"]
    changes_meta_keys = ["_id", "_revision", "_txn", "_rid", "datetime", "action"]

    country_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/Country.csv")
    assert len(country_data) == 3
    lt_data = country_data[0]
    lv_data = country_data[1]
    pl_data = country_data[2]

    country_changes_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/Country.changes.csv")
    assert len(country_changes_data) == 3
    lt_change_data = country_changes_data[0]
    lv_change_data = country_changes_data[1]
    pl_change_data = country_changes_data[2]

    city_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/City.csv")
    assert len(city_data) == 3
    vln_data = city_data[0]
    ryg_data = city_data[1]
    war_data = city_data[2]

    city_changes_data = _extract_postgresql_data_from_file(dir_path, "datasets/gov/example/City.changes.csv")
    assert len(city_changes_data) == 3
    vln_change_data = city_changes_data[0]
    ryg_change_data = city_changes_data[1]
    war_change_data = city_changes_data[2]

    txn = lt_data["_txn"]
    # Check `Country` data
    _assert_meta_keys_exists(country_data, data_meta_keys, nullable_keys)
    _assert_data(
        lt_data,
        {"_txn": txn, "_updated": "", "id": "0", "code": "LT", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        lv_data,
        {"_txn": txn, "_updated": "", "id": "1", "code": "LV", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        pl_data,
        {"_txn": txn, "_updated": "", "id": "2", "code": "PL", "created": "2020-01-01 00:00:00"},
        skip_columns=["_id", "_revision", "_created"],
    )

    # Check `Country` changelog data
    _assert_meta_keys_exists(country_changes_data, changes_meta_keys)
    _assert_data(
        lt_change_data,
        {
            "_id": "1",
            "_revision": lt_data["_revision"],
            "_txn": txn,
            "_rid": lt_data["_id"],
            "action": "insert",
            "data": '{"id": 0, "code": "LT", "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        lv_change_data,
        {
            "_id": "2",
            "_revision": lv_data["_revision"],
            "_txn": txn,
            "_rid": lv_data["_id"],
            "action": "insert",
            "data": '{"id": 1, "code": "LV", "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        pl_change_data,
        {
            "_id": "3",
            "_revision": pl_data["_revision"],
            "_txn": txn,
            "_rid": pl_data["_id"],
            "action": "insert",
            "data": '{"id": 2, "code": "PL", "created": "2020-01-01T00:00:00"}',
        },
        skip_columns=["datetime"],
    )

    # Check `City` data
    _assert_meta_keys_exists(city_data, data_meta_keys, nullable_keys)
    _assert_data(
        vln_data,
        {
            "_txn": txn,
            "_updated": "",
            "id": "0",
            "code": "VLN",
            "name": "VILNIUS",
            "country._id": lt_data["_id"],
            "country.test": "0",
            "country.obj.test": "0",
        },
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        ryg_data,
        {
            "_txn": txn,
            "_updated": "",
            "id": "1",
            "code": "RYG",
            "name": "RYGA",
            "country._id": lv_data["_id"],
            "country.test": "1",
            "country.obj.test": "1",
        },
        skip_columns=["_id", "_revision", "_created"],
    )
    _assert_data(
        war_data,
        {
            "_txn": txn,
            "_updated": "",
            "id": "2",
            "code": "WAR",
            "name": "VARSUVA",
            "country._id": pl_data["_id"],
            "country.test": "2",
            "country.obj.test": "2",
        },
        skip_columns=["_id", "_revision", "_created"],
    )

    # Check `Country` changelog data
    _assert_meta_keys_exists(city_changes_data, changes_meta_keys)
    _assert_data(
        vln_change_data,
        {
            "_id": "1",
            "_revision": vln_data["_revision"],
            "_txn": txn,
            "_rid": vln_data["_id"],
            "action": "insert",
            "data": f'{{"id": 0, "code": "VLN", "name": "VILNIUS", "country": {{"_id": "{lt_data["_id"]}", "test": 0, "obj": {{"test": 0}}}}}}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        ryg_change_data,
        {
            "_id": "2",
            "_revision": ryg_data["_revision"],
            "_txn": txn,
            "_rid": ryg_data["_id"],
            "action": "insert",
            "data": f'{{"id": 1, "code": "RYG", "name": "RYGA", "country": {{"_id": "{lv_data["_id"]}", "test": 1, "obj": {{"test": 1}}}}}}',
        },
        skip_columns=["datetime"],
    )
    _assert_data(
        war_change_data,
        {
            "_id": "3",
            "_revision": war_data["_revision"],
            "_txn": txn,
            "_rid": war_data["_id"],
            "action": "insert",
            "data": f'{{"id": 2, "code": "WAR", "name": "VARSUVA", "country": {{"_id": "{pl_data["_id"]}", "test": 2, "obj": {{"test": 2}}}}}}',
        },
        skip_columns=["datetime"],
    )
