import datetime
import json

import pytest
import sqlalchemy as sa

from spinta.cli.helpers.upgrade.components import Script
from spinta.core.config import RawConfig
from spinta.datasets.keymaps.sqlalchemy import SqlAlchemyKeyMap, _hash_value
from spinta.exceptions import KeymapMigrationRequired
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.client import create_rc, configure_remote_server
from spinta.testing.datasets import create_sqlite_db
from spinta.testing.tabular import create_tabular_manifest
from tests.cli.test_keymap import check_keymap_state


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


def test_upgrade_missing_initial_migration(
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

    keymap: SqlAlchemyKeyMap = remote.app.context.get("store").keymaps["default"]
    with keymap:
        migration_table = keymap.get_table(keymap.migration_table_name)
        keymap.conn.execute(migration_table.delete())

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
        fail=False,
    )
    assert result.exit_code == 1
    assert isinstance(result.exception, KeymapMigrationRequired)

    result = cli.invoke(localrc, ["upgrade"])
    assert result.exit_code == 0

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
    keymap_after_sync = check_keymap_state(context, "syncdataset/countries/Country")
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0].identifier == country_id_1
    assert keymap_after_sync[0].value == 2
    assert keymap_after_sync[0].redirect is None


def test_upgrade_missing_redirect_migration_entry(
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

    keymap: SqlAlchemyKeyMap = remote.app.context.get("store").keymaps["default"]
    with keymap:
        migration_table = keymap.get_table(keymap.migration_table_name)
        keymap.conn.execute(
            migration_table.delete().where(migration_table.c.migration == Script.SQL_KEYMAP_REDIRECT.value)
        )

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
        fail=False,
    )
    assert result.exit_code == 1
    assert isinstance(result.exception, KeymapMigrationRequired)

    result = cli.invoke(localrc, ["upgrade"])
    assert result.exit_code == 0

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
    keymap_after_sync = check_keymap_state(context, "syncdataset/countries/Country")
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0].identifier == country_id_1
    assert keymap_after_sync[0].value == 2
    assert keymap_after_sync[0].redirect is None


def test_upgrade_missing_modified_migration_entry(
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
    remote.app.authmodel("syncdataset/countries/Country", ["insert", "wipe", "changes"])
    resp = remote.app.post(
        "https://example.com/syncdataset/countries/Country",
        json={
            "code": 2,
        },
    )

    country_id_1 = resp.json()["_id"]
    modified_at = remote.app.get("https://example.com/syncdataset/countries/Country/:changes/-1").json()["_data"][0][
        "_created"
    ]
    modified_at = datetime.datetime.fromisoformat(modified_at)
    keymap: SqlAlchemyKeyMap = remote.app.context.get("store").keymaps["default"]
    with keymap:
        migration_table = keymap.get_table(keymap.migration_table_name)
        keymap.conn.execute(
            migration_table.delete().where(migration_table.c.migration == Script.SQL_KEYMAP_MODIFIED.value)
        )

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
        fail=False,
    )
    assert result.exit_code == 1
    assert isinstance(result.exception, KeymapMigrationRequired)

    result = cli.invoke(localrc, ["upgrade"])
    assert result.exit_code == 0

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
    keymap_after_sync = check_keymap_state(context, "syncdataset/countries/Country")
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0].identifier == country_id_1
    assert keymap_after_sync[0].value == 2
    assert keymap_after_sync[0].redirect is None
    assert keymap_after_sync[0].modified_at == modified_at


def test_upgrade_redirect_migration_from_old_version(
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

    keymap: SqlAlchemyKeyMap = remote.app.context.get("store").keymaps["default"]
    with keymap:
        migration_table = keymap.get_table(keymap.migration_table_name)
        keymap.conn.execute(
            migration_table.delete().where(migration_table.c.migration == Script.SQL_KEYMAP_REDIRECT.value)
        )

        old_table = sa.Table(
            "syncdataset/countries/Country",
            keymap.metadata,
            sa.Column("key", sa.Text, primary_key=True),
            sa.Column("hash", sa.Text, unique=True, index=True),
            sa.Column("value", sa.LargeBinary),
        )
        old_table.drop(checkfirst=True)
        old_table.create()

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
    assert isinstance(result.exception, KeymapMigrationRequired)

    result = cli.invoke(localrc, ["upgrade"])
    assert result.exit_code == 0

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
    keymap_after_sync = check_keymap_state(context, "syncdataset/countries/Country")
    assert len(keymap_after_sync) == 1
    assert keymap_after_sync[0].identifier == country_id_1
    assert keymap_after_sync[0].value == 2
    assert keymap_after_sync[0].redirect is None


def test_upgrade_redirect_migration_from_old_version_with_data(
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

    keymap: SqlAlchemyKeyMap = remote.app.context.get("store").keymaps["default"]
    with keymap:
        migration_table = keymap.get_table(keymap.migration_table_name)
        keymap.conn.execute(
            migration_table.delete().where(migration_table.c.migration == Script.SQL_KEYMAP_REDIRECT.value)
        )
        first_entry_id = keymap.encode(name="syncdataset/countries/Country", value=1)
        second_entry_id = keymap.encode(name="syncdataset/countries/Country", value=5)
        third_entry_id = keymap.encode(name="syncdataset/countries/Country", value=10)
        table_to_remove = keymap.get_table("syncdataset/countries/Country")
        keymap.conn.execute("""
            ALTER TABLE "syncdataset/countries/Country" RENAME TO "_correct_table";
        """)
        keymap.metadata.remove(table_to_remove)
        old_table = sa.Table(
            "syncdataset/countries/Country",
            keymap.metadata,
            sa.Column("key", sa.Text, primary_key=True),
            sa.Column("hash", sa.Text, unique=True, index=True),
            sa.Column("value", sa.LargeBinary),
        )
        old_table.create()
        for row in keymap.conn.execute("""
            SELECT * FROM "_correct_table"
        """):
            value_, hash_ = _hash_value(json.loads(row["value"]))
            keymap.conn.execute(old_table.insert().values(key=row["key"], value=value_, hash=hash_))
        keymap.conn.execute("""
            DROP TABLE "_correct_table"
        """)

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
    assert isinstance(result.exception, KeymapMigrationRequired)

    result = cli.invoke(localrc, ["upgrade"])
    assert result.exit_code == 0

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
    keymap_after_sync = check_keymap_state(context, "syncdataset/countries/Country")
    assert len(keymap_after_sync) == 4
    first_entry = keymap_after_sync[0]
    second_entry = keymap_after_sync[1]
    third_entry = keymap_after_sync[2]
    fourth_entry = keymap_after_sync[3]
    assert first_entry.identifier == first_entry_id
    assert first_entry.value == 1
    assert first_entry.redirect is None
    assert second_entry.identifier == second_entry_id
    assert second_entry.value == 5
    assert second_entry.redirect is None
    assert third_entry.identifier == third_entry_id
    assert third_entry.value == 10
    assert third_entry.redirect is None
    assert fourth_entry.identifier == country_id_1
    assert fourth_entry.value == 2
    assert fourth_entry.redirect is None


def test_upgrade_redirect_migration_from_old_version_with_multi_column_data(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request, reset_keymap
):
    table = """
            d | r | b | m | property | type    | ref                             | source         | level | access
            syncdataset/multi        |         |                                 |                |       |
              | db                   | sql     |                                 |                |       |
              |   |   | City         |         | id, name, country               | cities         | 4     |
              |   |   |   | id       | integer |                                 | id             | 4     | open
              |   |   |   | name     | string  |                                 | name           | 2     | open
              |   |   |   | country  | ref     | /syncdataset/countries/Country | country        | 4     | open
              |   |   |   |          |         |                                 |                |       |
            syncdataset/countries    |         |                                 |                |       |
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
    city_model = "syncdataset/multi/City"
    country_id_1 = resp.json()["_id"]

    keymap: SqlAlchemyKeyMap = remote.app.context.get("store").keymaps["default"]
    with keymap:
        migration_table = keymap.get_table(keymap.migration_table_name)
        keymap.conn.execute(
            migration_table.delete().where(migration_table.c.migration == Script.SQL_KEYMAP_REDIRECT.value)
        )
        first_entry_id = keymap.encode(name=city_model, value=[1, "Vilnius", country_id_1])
        second_entry_id = keymap.encode(name=city_model, value=[5, "Kaunas", country_id_1])
        third_entry_id = keymap.encode(name=city_model, value=[10, "Siauliai", country_id_1])
        table_to_remove = keymap.get_table(city_model)
        keymap.conn.execute(f'''
            ALTER TABLE "{city_model}" RENAME TO "_correct_table";
        ''')
        keymap.metadata.remove(table_to_remove)
        old_table = sa.Table(
            city_model,
            keymap.metadata,
            sa.Column("key", sa.Text, primary_key=True),
            sa.Column("hash", sa.Text, unique=True, index=True),
            sa.Column("value", sa.LargeBinary),
        )
        old_table.create()
        for row in keymap.conn.execute("""
            SELECT * FROM "_correct_table"
        """):
            value_, hash_ = _hash_value(json.loads(row["value"]))
            keymap.conn.execute(old_table.insert().values(key=row["key"], value=value_, hash=hash_))
        keymap.conn.execute("""
            DROP TABLE "_correct_table"
        """)

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
    assert isinstance(result.exception, KeymapMigrationRequired)

    result = cli.invoke(localrc, ["upgrade"])
    assert result.exit_code == 0

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
    keymap_after_sync = check_keymap_state(context, "syncdataset/countries/Country")
    assert len(keymap_after_sync) == 1
    first_entry = keymap_after_sync[0]
    assert first_entry.identifier == country_id_1
    assert first_entry.value == 2
    assert first_entry.redirect is None

    keymap_after_sync = check_keymap_state(context, city_model)
    assert len(keymap_after_sync) == 3
    first_entry = keymap_after_sync[0]
    second_entry = keymap_after_sync[1]
    third_entry = keymap_after_sync[2]
    assert first_entry.identifier == first_entry_id
    assert first_entry.value == [1, "Vilnius", country_id_1]
    assert first_entry.redirect is None
    assert second_entry.identifier == second_entry_id
    assert second_entry.value == [5, "Kaunas", country_id_1]
    assert second_entry.redirect is None
    assert third_entry.identifier == third_entry_id
    assert third_entry.value == [10, "Siauliai", country_id_1]
    assert third_entry.redirect is None


def test_upgrade_modified_from_old_version(
    context, postgresql, rc: RawConfig, cli: SpintaCliRunner, responses, tmp_path, geodb, request, reset_keymap
):
    table = """
            d | r | b | m | property | type    | ref                             | source         | level | access
            migrate/modified         |         |                                 |                |       |
              |   |   | Country      |         | code                            |                | 4     |
              |   |   |   | code     | integer |                                 |                | 4     | open
              |   |   |   | name     | string  |                                 |                | 2     | open
            """
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(table))
    localrc = create_rc(rc, tmp_path, geodb)
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses, remove_source=False)
    request.addfinalizer(remote.app.context.wipe_all)

    country_model = "migrate/modified/Country"

    assert remote.url == "https://example.com/"
    remote.app.authmodel(country_model, ["insert", "wipe", "changes"])
    resp = remote.app.post(
        country_model,
        json={
            "code": 2,
        },
    )

    country_id_1 = resp.json()["_id"]
    modified_at = remote.app.get(f"{country_model}/:changes/-1").json()["_data"][0]["_created"]
    modified_at = datetime.datetime.fromisoformat(modified_at)

    keymap: SqlAlchemyKeyMap = remote.app.context.get("store").keymaps["default"]

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
            "--check-all",
            "--no-progress-bar",
        ],
    )
    assert result.exit_code == 0

    with keymap:
        migration_table = keymap.get_table(keymap.migration_table_name)
        keymap.conn.execute(
            migration_table.delete().where(migration_table.c.migration == Script.SQL_KEYMAP_MODIFIED.value)
        )
        keymap.conn.execute(f"""
            DROP INDEX "ix_{country_model}_modified_at";
        """)
        keymap.conn.execute(f'''
            ALTER TABLE "{country_model}" DROP COLUMN "modified_at";
        ''')

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
    assert isinstance(result.exception, KeymapMigrationRequired)

    result = cli.invoke(localrc, ["upgrade"])
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, country_model)
    assert len(keymap_after_sync) == 1
    first_entry = keymap_after_sync[0]
    assert first_entry.identifier == country_id_1
    assert first_entry.value == 2
    assert first_entry.redirect is None
    assert first_entry.modified_at is None

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
    assert result.exit_code == 0
    keymap_after_sync = check_keymap_state(context, country_model)
    assert len(keymap_after_sync) == 1
    first_entry = keymap_after_sync[0]
    assert first_entry.identifier == country_id_1
    assert first_entry.value == 2
    assert first_entry.redirect is None

    assert first_entry.modified_at == modified_at
