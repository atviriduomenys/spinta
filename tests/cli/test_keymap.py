from __future__ import annotations

import dataclasses
import datetime
import json
import re
import uuid

import pytest
import sqlalchemy as sa

from spinta.backends.helpers import get_table_identifier
from spinta.backends.postgresql.helpers.name import get_pg_constraint_name
from spinta.components import Context
from spinta.core.enums import Action
from spinta.exceptions import KeymapDuplicateMapping
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.client import create_rc, configure_remote_server
from spinta.testing.config import RawConfig
from spinta.testing.data import send
from spinta.testing.datasets import create_sqlite_db
from spinta.testing.tabular import create_tabular_manifest


@dataclasses.dataclass
class KeymapData:
    key: str
    identifier: str
    value: object
    redirect: object | None = dataclasses.field(default=None)
    modified_at: datetime.datetime | None = dataclasses.field(default=None)


@pytest.fixture(scope="function")
def geodb():
    with create_sqlite_db(
        {
            "country": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("code", sa.Text),
                sa.Column("name", sa.Text),
            ],
            "cities": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("name", sa.Text),
                sa.Column("country", sa.Integer),
            ],
        }
    ) as db:
        db.write(
            "country",
            [
                {"code": "lt", "name": "Lietuva", "id": 1},
                {"code": "lv", "name": "Latvija", "id": 2},
                {"code": "ee", "name": "Estija", "id": 3},
            ],
        )
        db.write(
            "cities",
            [
                {"name": "Vilnius", "country": 1},
            ],
        )
        yield db


def check_keymap_state(context: Context, table_name: str) -> list[KeymapData]:
    keymap = context.get("store").keymaps["default"]
    values = []
    with keymap.engine.connect() as conn:
        table = keymap.get_table(table_name)
        query = sa.select([table])
        for row in conn.execute(query):
            values.append(
                KeymapData(
                    key=table_name,
                    identifier=row["key"],
                    value=json.loads(row["value"]),
                    redirect=row["redirect"],
                    modified_at=row["modified_at"],
                )
            )
        return values


def test_keymap_sync_dry_run(
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
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    assert remote.url == "https://example.com/"
    remote.app.authmodel("syncdataset/countries/Country", ["insert", "wipe"])
    remote.app.post(
        "https://example.com/syncdataset/countries/Country",
        json={
            "code": 2,
        },
    )

    manifest = tmp_path / "manifest.csv"

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, "syncdataset/countries/Country")
    assert len(keymap_before_sync) == 0

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--dry-run",
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    responses.remove("POST", re.compile(r"https://example\.com/.*"))

    # Check keymap state after sync for Country
    keymap_after_sync = check_keymap_state(context, "syncdataset/countries/Country")
    assert len(keymap_after_sync) == 0


def test_keymap_sync(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request, reset_keymap
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
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    assert remote.url == "https://example.com/"
    remote.app.authmodel("syncdataset/countries/Country", ["insert", "wipe"])
    resp = remote.app.post(
        "https://example.com/syncdataset/countries/Country",
        json={
            "code": 2,
        },
    )
    country_id = resp.json()["_id"]

    manifest = tmp_path / "manifest.csv"

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, "syncdataset/countries/Country")
    assert len(keymap_before_sync) == 0

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0

    # Check keymap state after sync for Country
    keymap_after_sync = check_keymap_state(context, "syncdataset/countries/Country")
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0].identifier == country_id
    assert keymap_after_sync[0].value == 2
    assert keymap_after_sync[0].redirect is None


def test_keymap_sync_more_entries(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request, reset_keymap
):
    table = """
            d | r | b | m | property | type    | ref                             | source         | level | access
            largedataset             |         |                                 |                |       |
              | db                   | sql     |                                 |                |       |
              |   |   | City         |         | id                              | cities         | 4     |
              |   |   |   | id       | integer |                                 | id             | 4     | open
              |   |   |   | name     | string  |                                 | name           | 2     | open
              |   |   |   | country  | ref     | /largedataset/countries/Country | country        | 4     | open
              |   |   |   |          |         |                                 |                |       |
            largedataset/countries   |         |                                 |                |       |
              |   |   | Country      |         | code                            |                | 4     |
              |   |   |   | code     | integer |                                 |                | 4     | open
              |   |   |   | name     | string  |                                 |                | 2     | open
            """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    assert remote.url == "https://example.com/"
    remote.app.authmodel("largedataset/countries/Country", ["insert", "wipe"])

    entry_ids = [
        remote.app.post("https://example.com/largedataset/countries/Country", json={"code": i}).json()["_id"]
        for i in range(10)
    ]

    keymap_before_sync = check_keymap_state(context, "largedataset/countries/Country")
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / "manifest.csv"

    result = cli.invoke(localrc, ["keymap", "sync", manifest, "-i", remote.url, "--credentials", remote.credsfile])
    assert result.exit_code == 0

    keymap_after_sync = check_keymap_state(context, "largedataset/countries/Country")
    assert len(keymap_after_sync) == 10
    keymap_keys = [entry.identifier for entry in keymap_after_sync]
    assert all(key in entry_ids for key in keymap_keys)


def test_keymap_sync_dataset(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request, reset_keymap
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
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    assert remote.url == "https://example.com/"
    remote.app.authmodel("syncdataset/countries/Country", ["insert", "wipe"])
    resp = remote.app.post(
        "https://example.com/syncdataset/countries/Country",
        json={
            "code": 2,
        },
    )
    country_id = resp.json()["_id"]

    manifest = tmp_path / "manifest.csv"

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, "syncdataset/countries/Country")
    assert len(keymap_before_sync) == 0

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "-d",
            "syncdataset",
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0

    # Check keymap state before sync for Country
    keymap_after_sync = check_keymap_state(context, "syncdataset/countries/Country")
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0].identifier == country_id
    assert keymap_after_sync[0].value == 2
    assert keymap_after_sync[0].redirect is None


def test_keymap_sync_no_changes(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request, reset_keymap
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
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    assert remote.url == "https://example.com/"
    remote.app.authmodel("syncdataset/countries/Country", ["insert", "wipe"])
    resp = remote.app.post(
        "https://example.com/syncdataset/countries/Country",
        json={
            "code": 2,
        },
    )
    country_id = resp.json()["_id"]

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, "syncdataset/countries/Country")
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / "manifest.csv"

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0

    # Run sync again with no changes
    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0

    keymap_after_sync = check_keymap_state(context, "syncdataset/countries/Country")
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0].identifier == country_id
    assert keymap_after_sync[0].value == 2
    assert keymap_after_sync[0].redirect is None


def test_keymap_sync_consequitive_changes(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request, reset_keymap
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
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    assert remote.url == "https://example.com/"
    remote.app.authmodel("syncdataset/countries/Country", ["insert", "wipe"])
    resp = remote.app.post(
        "https://example.com/syncdataset/countries/Country",
        json={
            "code": 2,
        },
    )
    country_id_1 = resp.json()["_id"]

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, "syncdataset/countries/Country")
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / "manifest.csv"

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, "syncdataset/countries/Country")
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0].identifier == country_id_1
    assert keymap_after_sync[0].value == 2
    assert keymap_after_sync[0].redirect is None

    remote.app.authmodel("syncdataset/countries/Country", ["insert", "wipe"])
    resp = remote.app.post(
        "https://example.com/syncdataset/countries/Country",
        json={
            "code": 3,
        },
    )
    country_id_2 = resp.json()["_id"]
    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0

    keymap_after_sync = check_keymap_state(context, "syncdataset/countries/Country")
    assert len(keymap_after_sync) == 2
    assert keymap_after_sync[0].identifier == country_id_1
    assert keymap_after_sync[0].value == 2
    assert keymap_after_sync[0].redirect is None
    assert keymap_after_sync[1].identifier == country_id_2
    assert keymap_after_sync[1].value == 3
    assert keymap_after_sync[1].redirect is None


def test_keymap_sync_missing_input(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb
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
    localrc = create_rc(rc, tmp_path, geodb)
    result = cli.invoke(localrc, ["keymap", "sync", str(tmp_path / "manifest.csv")], fail=False)
    assert result.exit_code == 1
    assert "Input source is required." in result.stderr


def test_keymap_sync_invalid_credentials(
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
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    manifest = tmp_path / "manifest.csv"

    responses.remove("POST", re.compile(r"https://example\.com/.*"))

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            "invalid_credentials",
            "--no-progress-bar",
        ],
        fail=False,
    )
    assert result.exit_code == 1


def test_keymap_sync_no_credentials(
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
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    manifest = tmp_path / "manifest.csv"

    responses.remove("POST", re.compile(r"https://example\.com/.*"))

    # Credentials not present in credentials.cfg (Remote client credentials not found for 'https://example.com/')
    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--no-progress-bar",
        ],
        fail=False,
    )
    assert result.exit_code == 1


def test_keymap_sync_non_existent_dataset(
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
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    manifest = tmp_path / "manifest.csv"

    responses.remove("POST", re.compile(r"https://example\.com/.*"))

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--dataset",
            "non_existent_dataset",
            "--no-progress-bar",
        ],
        fail=False,
    )

    assert result.exit_code == 1
    assert "'dataset' not found" in result.stderr


def test_keymap_sync_with_pages(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request, reset_keymap
):
    table = """
            d | r | b | m | property | type    | ref                             | source         | level | access
            largedataset             |         |                                 |                |       |
              | db                   | sql     |                                 |                |       |
              |   |   | City         |         | id                              | cities         | 4     |
              |   |   |   | id       | integer |                                 | id             | 4     | open
              |   |   |   | name     | string  |                                 | name           | 2     | open
              |   |   |   | country  | ref     | /largedataset/countries/Country | country        | 4     | open
              |   |   |   |          |         |                                 |                |       |
            largedataset/countries   |         |                                 |                |       |
              |   |   | Country      |         | code                            |                | 4     |
              |   |   |   | code     | integer |                                 |                | 4     | open
              |   |   |   | name     | string  |                                 |                | 2     | open
            """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    localrc = localrc.fork({"sync_page_size": 3})
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    assert remote.url == "https://example.com/"
    remote.app.authmodel("largedataset/countries/Country", ["insert", "wipe"])

    entry_ids = [
        remote.app.post("https://example.com/largedataset/countries/Country", json={"code": i}).json()["_id"]
        for i in range(10)
    ]

    keymap_before_sync = check_keymap_state(context, "largedataset/countries/Country")
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / "manifest.csv"

    result = cli.invoke(localrc, ["keymap", "sync", manifest, "-i", remote.url, "--credentials", remote.credsfile])
    assert result.exit_code == 0

    keymap_after_sync = check_keymap_state(context, "largedataset/countries/Country")
    assert len(keymap_after_sync) == 10
    keymap_keys = [entry.identifier for entry in keymap_after_sync]
    assert all(key in entry_ids for key in keymap_keys)


def test_keymap_sync_with_transaction_batches(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request, reset_keymap
):
    table = """
            d | r | b | m | property | type    | ref                             | source         | level | access
            largedataset             |         |                                 |                |       |
              | db                   | sql     |                                 |                |       |
              |   |   | City         |         | id                              | cities         | 4     |
              |   |   |   | id       | integer |                                 | id             | 4     | open
              |   |   |   | name     | string  |                                 | name           | 2     | open
              |   |   |   | country  | ref     | /largedataset/countries/Country | country        | 4     | open
              |   |   |   |          |         |                                 |                |       |
            largedataset/countries   |         |                                 |                |       |
              |   |   | Country      |         | code                            |                | 4     |
              |   |   |   | code     | integer |                                 |                | 4     | open
              |   |   |   | name     | string  |                                 |                | 2     | open
            """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    localrc = localrc.fork(
        {"sync_page_size": 3, "keymaps": {"default": {"type": "sqlalchemy", "sync_transaction_size": 4}}}
    )
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    assert remote.url == "https://example.com/"
    remote.app.authmodel("largedataset/countries/Country", ["insert", "wipe"])

    entry_ids = [
        remote.app.post("https://example.com/largedataset/countries/Country", json={"code": i}).json()["_id"]
        for i in range(10)
    ]

    keymap_before_sync = check_keymap_state(context, "largedataset/countries/Country")
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / "manifest.csv"

    result = cli.invoke(localrc, ["keymap", "sync", manifest, "-i", remote.url, "--credentials", remote.credsfile])
    assert result.exit_code == 0

    keymap_after_sync = check_keymap_state(context, "largedataset/countries/Country")
    assert len(keymap_after_sync) == 10
    keymap_keys = [entry.identifier for entry in keymap_after_sync]
    assert all(key in entry_ids for key in keymap_keys)


def test_keymap_sync_insert(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request, reset_keymap
):
    table = """
        d | r | b | m | property | type    | ref                             | source         | level | access
        syncdataset              |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | City         |         | id                              | cities         | 4     |
          |   |   |   | id       | integer |                                 | id             | 4     | open
          |   |   |   | name     | string  |                                 | name           | 2     | open
          |   |   |   | country  | ref     | /syncdataset/countries/Country  | country        | 4     | open
          |   |   |   |          |         |                                 |                |       |
        syncdataset/countries    |         |                                 |                |       |
          |   |   | Country      |         | code                            |                | 4     |
          |   |   |   | code     | integer |                                 |                | 4     | open
          |   |   |   | name     | string  |                                 |                | 2     | open
    """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    store = remote.app.context.get("store")
    manifest = store.manifest
    keymap = manifest.keymap
    request.addfinalizer(remote.app.context.wipe_all)

    model = "syncdataset/countries/Country"
    remote.app.authmodel(model, ["insert", "wipe", "changes"])
    obj = remote.app.post(model, json={"code": 1})
    country_id_1 = obj.json()["_id"]
    assert send(remote.app, model, ":changes/-1?limit(1)", select=["_cid", "_op", "_id", "code"]) == [
        {"_cid": 1, "_op": "insert", "_id": country_id_1, "code": 1},
    ]

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, model)
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / "manifest.csv"

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0].identifier == country_id_1
    assert keymap_after_sync[0].value == 1
    assert keymap_after_sync[0].redirect is None
    with keymap as km:
        assert km.decode(model, country_id_1) == 1


def test_keymap_sync_update(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request, reset_keymap
):
    table = """
        d | r | b | m | property | type    | ref                             | source         | level | access
        syncdataset              |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | City         |         | id                              | cities         | 4     |
          |   |   |   | id       | integer |                                 | id             | 4     | open
          |   |   |   | name     | string  |                                 | name           | 2     | open
          |   |   |   | country  | ref     | /syncdataset/countries/Country  | country        | 4     | open
          |   |   |   |          |         |                                 |                |       |
        syncdataset/countries    |         |                                 |                |       |
          |   |   | Country      |         | code                            |                | 4     |
          |   |   |   | code     | integer |                                 |                | 4     | open
          |   |   |   | name     | string  |                                 |                | 2     | open
    """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    store = remote.app.context.get("store")
    manifest = store.manifest
    keymap = manifest.keymap
    request.addfinalizer(remote.app.context.wipe_all)

    model = "syncdataset/countries/Country"
    remote.app.authmodel(model, ["insert", "wipe", "changes", "update"])
    obj = remote.app.post(model, json={"code": 1, "name": "a"}).json()
    country_id_1 = obj["_id"]

    assert send(remote.app, model, ":changes", select=["_cid", "_op", "_id", "code"]) == [
        {"_cid": 1, "_op": "insert", "_id": country_id_1, "code": 1},
    ]

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, model)
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / "manifest.csv"

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0].identifier == country_id_1
    assert keymap_after_sync[0].value == 1
    assert keymap_after_sync[0].redirect is None
    with keymap as km:
        assert km.decode(model, country_id_1) == 1

    obj = remote.app.post(
        f"{model}/{country_id_1}", json={"_op": Action.UPDATE.value, "_revision": obj["_revision"], "code": 10}
    ).json()
    country_id_2 = obj["_id"]
    assert country_id_1 == country_id_2

    assert send(remote.app, model, ":changes", select=["_cid", "_op", "_id", "code"]) == [
        {"_cid": 1, "_op": "insert", "_id": country_id_1, "code": 1},
        {"_cid": 2, "_op": "update", "_id": country_id_2, "code": 10},
    ]

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0].identifier == country_id_2
    assert keymap_after_sync[0].value == 10
    assert keymap_after_sync[0].redirect is None
    with keymap as km:
        assert km.decode(model, country_id_2) == 10


def test_keymap_sync_patch(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request, reset_keymap
):
    table = """
        d | r | b | m | property | type    | ref                             | source         | level | access
        syncdataset              |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | City         |         | id                              | cities         | 4     |
          |   |   |   | id       | integer |                                 | id             | 4     | open
          |   |   |   | name     | string  |                                 | name           | 2     | open
          |   |   |   | country  | ref     | /syncdataset/countries/Country  | country        | 4     | open
          |   |   |   |          |         |                                 |                |       |
        syncdataset/countries    |         |                                 |                |       |
          |   |   | Country      |         | code                            |                | 4     |
          |   |   |   | code     | integer |                                 |                | 4     | open
          |   |   |   | name     | string  |                                 |                | 2     | open
    """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    store = remote.app.context.get("store")
    manifest = store.manifest
    keymap = manifest.keymap
    request.addfinalizer(remote.app.context.wipe_all)

    model = "syncdataset/countries/Country"
    remote.app.authmodel(model, ["insert", "wipe", "changes", "patch"])
    obj = remote.app.post(model, json={"code": 1, "name": "a"}).json()
    country_id_1 = obj["_id"]

    assert send(remote.app, model, ":changes", select=["_cid", "_op", "_id", "code"]) == [
        {"_cid": 1, "_op": "insert", "_id": country_id_1, "code": 1},
    ]

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, model)
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / "manifest.csv"

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0].identifier == country_id_1
    assert keymap_after_sync[0].value == 1
    assert keymap_after_sync[0].redirect is None
    with keymap as km:
        assert km.decode(model, country_id_1) == 1

    obj = remote.app.post(
        f"{model}/{country_id_1}", json={"_op": Action.PATCH.value, "_revision": obj["_revision"], "code": 10}
    ).json()
    country_id_2 = obj["_id"]
    assert country_id_1 == country_id_2

    assert send(remote.app, model, ":changes", select=["_cid", "_op", "_id", "code"]) == [
        {"_cid": 1, "_op": "insert", "_id": country_id_1, "code": 1},
        {"_cid": 2, "_op": "patch", "_id": country_id_2, "code": 10},
    ]

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0].identifier == country_id_2
    assert keymap_after_sync[0].value == 10
    assert keymap_after_sync[0].redirect is None
    with keymap as km:
        assert km.decode(model, country_id_2) == 10


def test_keymap_sync_upsert_insert(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request, reset_keymap
):
    table = """
        d | r | b | m | property | type    | ref                             | source         | level | access
        syncdataset              |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | City         |         | id                              | cities         | 4     |
          |   |   |   | id       | integer |                                 | id             | 4     | open
          |   |   |   | name     | string  |                                 | name           | 2     | open
          |   |   |   | country  | ref     | /syncdataset/countries/Country  | country        | 4     | open
          |   |   |   |          |         |                                 |                |       |
        syncdataset/countries    |         |                                 |                |       |
          |   |   | Country      |         | code                            |                | 4     |
          |   |   |   | code     | integer |                                 |                | 4     | open
          |   |   |   | name     | string  |                                 |                | 2     | open
    """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    store = remote.app.context.get("store")
    manifest = store.manifest
    keymap = manifest.keymap
    request.addfinalizer(remote.app.context.wipe_all)

    model = "syncdataset/countries/Country"
    remote.app.authmodel(model, ["insert", "wipe", "changes", "upsert"])
    obj = remote.app.post(model, json={"code": 1, "name": "a"}).json()
    country_id_1 = obj["_id"]

    assert send(remote.app, model, ":changes", select=["_cid", "_op", "_id", "code"]) == [
        {"_cid": 1, "_op": "insert", "_id": country_id_1, "code": 1},
    ]

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, model)
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / "manifest.csv"

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0].identifier == country_id_1
    assert keymap_after_sync[0].value == 1
    assert keymap_after_sync[0].redirect is None
    with keymap as km:
        assert km.decode(model, country_id_1) == 1

    country_id_2 = str(uuid.uuid4())
    obj = remote.app.post(f"{model}/{country_id_2}", json={"_op": Action.UPSERT.value, "code": 10}).json()
    country_id_2 = obj["_id"]
    assert country_id_1 != country_id_2

    assert send(remote.app, model, ":changes", select=["_cid", "_op", "_id", "code"]) == [
        {"_cid": 1, "_op": "insert", "_id": country_id_1, "code": 1},
        {"_cid": 2, "_op": "upsert", "_id": country_id_2, "code": 10},
    ]

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 2
    assert keymap_after_sync[0].identifier == country_id_1
    assert keymap_after_sync[0].value == 1
    assert keymap_after_sync[0].redirect is None
    with keymap as km:
        assert km.decode(model, country_id_1) == 1

    assert keymap_after_sync[1].identifier == country_id_2
    assert keymap_after_sync[1].value == 10
    assert keymap_after_sync[1].redirect is None
    with keymap as km:
        assert km.decode(model, country_id_2) == 10


def test_keymap_sync_upsert_update(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request, reset_keymap
):
    table = """
        d | r | b | m | property | type    | ref                             | source         | level | access
        syncdataset              |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | City         |         | id                              | cities         | 4     |
          |   |   |   | id       | integer |                                 | id             | 4     | open
          |   |   |   | name     | string  |                                 | name           | 2     | open
          |   |   |   | country  | ref     | /syncdataset/countries/Country  | country        | 4     | open
          |   |   |   |          |         |                                 |                |       |
        syncdataset/countries    |         |                                 |                |       |
          |   |   | Country      |         | code                            |                | 4     |
          |   |   |   | code     | integer |                                 |                | 4     | open
          |   |   |   | name     | string  |                                 |                | 2     | open
    """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    store = remote.app.context.get("store")
    manifest = store.manifest
    keymap = manifest.keymap
    request.addfinalizer(remote.app.context.wipe_all)

    model = "syncdataset/countries/Country"
    remote.app.authmodel(model, ["insert", "wipe", "changes", "upsert"])
    obj = remote.app.post(model, json={"code": 1, "name": "a"}).json()
    country_id_1 = obj["_id"]

    assert send(remote.app, model, ":changes", select=["_cid", "_op", "_id", "code"]) == [
        {"_cid": 1, "_op": "insert", "_id": country_id_1, "code": 1},
    ]

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, model)
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / "manifest.csv"

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0].identifier == country_id_1
    assert keymap_after_sync[0].value == 1
    assert keymap_after_sync[0].redirect is None
    with keymap as km:
        assert km.decode(model, country_id_1) == 1

    obj = remote.app.post(f"{model}/{country_id_1}", json={"_op": Action.UPSERT.value, "code": 10}).json()
    country_id_2 = obj["_id"]
    assert country_id_1 == country_id_2

    assert send(remote.app, model, ":changes", select=["_cid", "_op", "_id", "code"]) == [
        {"_cid": 1, "_op": "insert", "_id": country_id_1, "code": 1},
        {"_cid": 2, "_op": "upsert", "_id": country_id_2, "code": 10},
    ]

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0].identifier == country_id_2
    assert keymap_after_sync[0].value == 10
    assert keymap_after_sync[0].redirect is None
    with keymap as km:
        assert km.decode(model, country_id_2) == 10


def test_keymap_sync_move(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request, reset_keymap
):
    table = """
        d | r | b | m | property | type    | ref                             | source         | level | access
        syncdataset              |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | City         |         | id                              | cities         | 4     |
          |   |   |   | id       | integer |                                 | id             | 4     | open
          |   |   |   | name     | string  |                                 | name           | 2     | open
          |   |   |   | country  | ref     | /syncdataset/countries/Country  | country        | 4     | open
          |   |   |   |          |         |                                 |                |       |
        syncdataset/countries    |         |                                 |                |       |
          |   |   | Country      |         | code                            |                | 4     |
          |   |   |   | code     | integer |                                 |                | 4     | open
          |   |   |   | name     | string  |                                 |                | 2     | open
    """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    store = remote.app.context.get("store")
    manifest = store.manifest
    keymap = manifest.keymap
    request.addfinalizer(remote.app.context.wipe_all)

    model = "syncdataset/countries/Country"
    remote.app.authmodel(model, ["insert", "wipe", "changes", "upsert", "delete", "move"])
    remote.app.authorize(["spinta_set_meta_fields"])
    obj = remote.app.post(model, json={"code": 1, "name": "a"}).json()
    country_id_1 = obj["_id"]
    obj = remote.app.post(model, json={"code": 2, "name": "a"}).json()
    country_revision_2 = obj["_revision"]
    country_id_2 = obj["_id"]

    assert send(remote.app, model, ":changes", select=["_cid", "_op", "_id", "code"]) == [
        {"_cid": 1, "_op": "insert", "_id": country_id_1, "code": 1},
        {"_cid": 2, "_op": "insert", "_id": country_id_2, "code": 2},
    ]

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, model)
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / "manifest.csv"

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 2
    assert keymap_after_sync[0].identifier == country_id_1
    assert keymap_after_sync[0].value == 1
    assert keymap_after_sync[0].redirect is None
    assert keymap_after_sync[1].identifier == country_id_2
    assert keymap_after_sync[1].value == 2
    assert keymap_after_sync[1].redirect is None
    with keymap as km:
        assert km.decode(model, country_id_1) == 1
        assert km.decode(model, country_id_2) == 2

    remote.app.request(
        "DELETE", f"{model}/{country_id_2}/:move", json={"_revision": country_revision_2, "_id": country_id_1}
    )

    assert send(remote.app, model, ":changes", select=["_cid", "_op", "_id", "_same_as", "code"]) == [
        {"_cid": 1, "_op": "insert", "_id": country_id_1, "code": 1},
        {"_cid": 2, "_op": "insert", "_id": country_id_2, "code": 2},
        {"_cid": 3, "_op": "move", "_id": country_id_2, "_same_as": country_id_1},
    ]

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 2
    assert keymap_after_sync[0].identifier == country_id_1
    assert keymap_after_sync[0].value == 1
    assert keymap_after_sync[0].redirect is None
    assert keymap_after_sync[1].identifier == country_id_2
    assert keymap_after_sync[1].value == 2
    assert keymap_after_sync[1].redirect == country_id_1


def test_keymap_sync_invalid_changelog_validation(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request, reset_keymap
):
    table = """
        d | r | b | m | property | type    | ref                             | source         | level | access
        syncdataset              |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | City         |         | id                              | cities         | 4     |
          |   |   |   | id       | integer |                                 | id             | 4     | open
          |   |   |   | name     | string  |                                 | name           | 2     | open
          |   |   |   | country  | ref     | /syncdataset/countries/Country  | country        | 4     | open
          |   |   |   |          |         |                                 |                |       |
        syncdataset/countries    |         |                                 |                |       |
          |   |   | Country      |         | code                            |                | 4     |
          |   |   |   | code     | integer |                                 |                | 4     | open
          |   |   |   | name     | string  |                                 |                | 2     | open
    """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    store = remote.app.context.get("store")
    manifest = store.manifest
    keymap = manifest.keymap
    backend = manifest.backend
    request.addfinalizer(remote.app.context.wipe_all)
    with backend.begin() as conn:
        insp = sa.inspect(backend.engine)
        table_identifier = get_table_identifier("syncdataset/countries/Country")
        constraint_name = get_pg_constraint_name(table_identifier.pg_table_name, ["code"])
        for constraint in insp.get_unique_constraints(
            table_identifier.pg_table_name, schema=table_identifier.pg_schema_name
        ):
            if constraint["name"] == constraint_name:
                conn.execute(f'''
                    ALTER TABLE {table_identifier.pg_escaped_qualified_name} DROP CONSTRAINT "{constraint_name}";
                ''')

    model = "syncdataset/countries/Country"
    remote.app.authmodel(model, ["insert", "wipe", "changes", "upsert", "delete", "move"])
    remote.app.authorize(["spinta_set_meta_fields"])
    obj = remote.app.post(model, json={"code": 1, "name": "a"}).json()
    country_id_1 = obj["_id"]
    remote.app.delete(f"{model}/{country_id_1}")
    obj = remote.app.post(model, json={"code": 1, "name": "a"}).json()
    country_id_2 = obj["_id"]

    assert send(remote.app, model, ":changes", select=["_cid", "_op", "_id", "code"]) == [
        {"_cid": 1, "_op": "insert", "_id": country_id_1, "code": 1},
        {"_cid": 2, "_op": "delete", "_id": country_id_1},
        {"_cid": 3, "_op": "insert", "_id": country_id_2, "code": 1},
    ]

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, model)
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / "manifest.csv"

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
        fail=False,
    )
    assert result.exit_code == 1
    assert isinstance(result.exception, KeymapDuplicateMapping)

    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 2
    assert keymap_after_sync[0].identifier == country_id_1
    assert keymap_after_sync[0].value == 1
    assert keymap_after_sync[0].redirect is None
    assert keymap_after_sync[1].identifier == country_id_2
    assert keymap_after_sync[1].value == 1
    assert keymap_after_sync[1].redirect is None
    with keymap as km:
        assert km.decode(model, country_id_1) == 1
        assert km.decode(model, country_id_2) == 1


# TODO remove this, when models without primary key no longer can access _id features
def test_keymap_sync_invalid_changelog_validation_no_pkey(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request, reset_keymap
):
    table = """
        d | r | b | m | property | type    | ref                             | source         | level | access
        syncdataset              |         |                                 |                |       |
          | db                   | sql     |                                 |                |       |
          |   |   | City         |         | id                              | cities         | 4     |
          |   |   |   | id       | integer |                                 | id             | 4     | open
          |   |   |   | name     | string  |                                 | name           | 2     | open
          |   |   |   | country  | ref     | /syncdataset/countries/Country  | country        | 4     | open
          |   |   |   |          |         |                                 |                |       |
        syncdataset/countries    |         |                                 |                |       |
          |   |   | Country      |         |                                 |                | 4     |
          |   |   |   | code     | integer |                                 |                | 4     | open
          |   |   |   | name     | string  |                                 |                | 2     | open
    """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    store = remote.app.context.get("store")
    manifest = store.manifest
    keymap = manifest.keymap
    request.addfinalizer(remote.app.context.wipe_all)

    model = "syncdataset/countries/Country"
    remote.app.authmodel(model, ["insert", "wipe", "changes", "upsert", "delete", "move"])
    remote.app.authorize(["spinta_set_meta_fields"])
    obj = remote.app.post(model, json={"code": 1, "name": "a"}).json()
    country_id_1 = obj["_id"]
    remote.app.delete(f"{model}/{country_id_1}")
    obj = remote.app.post(model, json={"code": 1, "name": "a"}).json()
    country_id_2 = obj["_id"]

    assert send(remote.app, model, ":changes", select=["_cid", "_op", "_id", "code"]) == [
        {"_cid": 1, "_op": "insert", "_id": country_id_1, "code": 1},
        {"_cid": 2, "_op": "delete", "_id": country_id_1},
        {"_cid": 3, "_op": "insert", "_id": country_id_2, "code": 1},
    ]

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, model)
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / "manifest.csv"

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
        fail=False,
    )
    assert result.exit_code == 0

    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0].identifier == country_id_2
    assert keymap_after_sync[0].value == [1, "a"]
    assert keymap_after_sync[0].redirect is None
    with keymap as km:
        assert km.decode(model, country_id_2) == [1, "a"]


def test_keymap_sync_duplicate_warn_only_use_latest(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request, reset_keymap
):
    table = """
        d | r | b | m | property | type    | ref                             | source         | level | access
        syncdataset/countries    |         |                                 |                |       |
          |   |   | Country      |         | code                                |                | 4     |
          |   |   |   | code     | integer |                                 |                | 4     | open
          |   |   |   | name     | string  |                                 |                | 2     | open
    """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    store = remote.app.context.get("store")
    manifest = store.manifest
    keymap = manifest.keymap
    request.addfinalizer(remote.app.context.wipe_all)

    model = "syncdataset/countries/Country"
    remote.app.authmodel(model, ["insert", "wipe", "changes", "upsert", "delete", "move"])
    remote.app.authorize(["spinta_set_meta_fields"])
    obj = remote.app.post(model, json={"code": 1, "name": "a"}).json()
    country_id_1 = obj["_id"]
    remote.app.delete(f"{model}/{country_id_1}")
    obj = remote.app.post(model, json={"code": 1, "name": "a"}).json()
    country_id_2 = obj["_id"]

    assert send(remote.app, model, ":changes", select=["_cid", "_op", "_id", "code"]) == [
        {"_cid": 1, "_op": "insert", "_id": country_id_1, "code": 1},
        {"_cid": 2, "_op": "delete", "_id": country_id_1},
        {"_cid": 3, "_op": "insert", "_id": country_id_2, "code": 1},
    ]

    # Check keymap state before sync for Country
    keymap_before_sync = check_keymap_state(context, model)
    assert len(keymap_before_sync) == 0

    manifest = tmp_path / "manifest.csv"

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--check-all",
            "--credentials",
            remote.credsfile,
            "--no-progress-bar",
        ],
        fail=False,
    )
    assert result.exit_code == 1
    assert isinstance(result.exception, KeymapDuplicateMapping)

    localrc = localrc.fork({"keymaps": {"default": {"duplicate_warn_only": True}}})

    result = cli.invoke(
        localrc,
        [
            "keymap",
            "sync",
            manifest,
            "-i",
            remote.url,
            "--credentials",
            remote.credsfile,
            "--check-all",
            "--no-progress-bar",
        ],
        fail=False,
    )
    assert result.exit_code == 0

    keymap_after_sync = check_keymap_state(context, model)
    assert len(keymap_after_sync) == 2
    assert keymap_after_sync[0].identifier == country_id_1
    assert keymap_after_sync[0].value == 1
    assert keymap_after_sync[0].redirect is None
    with keymap as km:
        assert km.decode(model, country_id_1) == 1

    assert keymap_after_sync[1].identifier == country_id_2
    assert keymap_after_sync[1].value == 1
    assert keymap_after_sync[1].redirect is None
    with keymap as km:
        assert km.decode(model, country_id_2) == 1

    with keymap as km:
        assert km.encode(model, 1) == country_id_2
