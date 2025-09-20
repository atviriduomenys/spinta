from pathlib import Path

import sqlalchemy as sa
from _pytest.fixtures import FixtureRequest

from spinta.backends.constants import TableType
from spinta.backends.postgresql.helpers.name import get_pg_table_name, get_pg_constraint_name
from spinta.cli.helpers.admin.components import Script
from spinta.cli.helpers.upgrade.components import Script as UpgradeScript
from spinta.cli.helpers.script.components import ScriptStatus
from spinta.cli.helpers.script.helpers import script_check_status_message
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.tabular import create_tabular_manifest


def test_admin_deduplicate_missing_redirect(
    context: Context,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    cli: SpintaCliRunner,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(
            """
        d | r | b | m | property      | type    | ref     | prepare | access | level
        datasets/deduplicate/cli/req  |         |         |         |        |
          |   |   | Country           |         | id      |         |        |
          |   |   |   | id            | integer |         |         | open   |
          |   |   |   | name          | string  |         |         | open   |
          |   |   | City              |         | id      |         |        |
          |   |   |   | id            | integer |         |         | open   |
          |   |   |   | name          | string  |         |         | open   |
          |   |   |   | country       | ref     | Country |         | open   |
        datasets/deduplicate/rand/req |         |         |         |        |
          |   |   | Random            |         | id      |         |        |
          |   |   |   | id            | integer |         |         | open   |
          |   |   |   | name          | string  |         |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )

    store = context.get("store")
    backend = store.manifest.backend
    insp = sa.inspect(backend.engine)
    country_redirect = get_pg_table_name("datasets/deduplicate/cli/req/Country", TableType.REDIRECT)
    city_redirect = get_pg_table_name("datasets/deduplicate/cli/req/City", TableType.REDIRECT)
    random_redirect = get_pg_table_name("datasets/deduplicate/rand/req/Random", TableType.REDIRECT)

    with backend.begin() as conn:
        conn.execute(f'''
            DROP TABLE IF EXISTS "{country_redirect}";
            DROP TABLE IF EXISTS "{city_redirect}";
            DROP TABLE IF EXISTS "{random_redirect}";
        ''')

    assert not insp.has_table(country_redirect)
    assert not insp.has_table(city_redirect)
    assert not insp.has_table(random_redirect)
    result = cli.invoke(context.get("rc"), ["admin", Script.DEDUPLICATE.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.DEDUPLICATE.value, ScriptStatus.SKIPPED) in result.stdout

    assert not insp.has_table(country_redirect)
    assert not insp.has_table(city_redirect)
    assert not insp.has_table(random_redirect)

    result = cli.invoke(context.get("rc"), ["upgrade", UpgradeScript.REDIRECT.value])
    assert result.exit_code == 0
    assert script_check_status_message(UpgradeScript.REDIRECT.value, ScriptStatus.REQUIRED) in result.stdout

    assert insp.has_table(country_redirect)
    assert insp.has_table(city_redirect)
    assert insp.has_table(random_redirect)

    result = cli.invoke(context.get("rc"), ["admin", Script.DEDUPLICATE.value, "-c"])
    assert result.exit_code == 0
    assert script_check_status_message(Script.DEDUPLICATE.value, ScriptStatus.PASSED) in result.stdout


def test_admin_deduplicate_missing_constraint(
    context: Context,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    cli: SpintaCliRunner,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(
            """
        d | r | b | m | property  | type    | ref                           | prepare | access | level
        datasets/deduplicate/cli  |         |                               |         |        |
          |   |   | Country       |         | id                            |         |        |
          |   |   |   | id        | integer |                               |         | open   |
          |   |   |   | name      | string  |                               |         | open   |
          |   |   | City          |         | id                            |         |        |
          |   |   |   | id        | integer |                               |         | open   |
          |   |   |   | name      | string  |                               |         | open   |
          |   |   |   | country   | ref     | Country                       |         | open   |
        datasets/deduplicate/rand |         |                               |         |        |
          |   |   | Random        |         | id                            |         |        |
          |   |   |   | id        | integer |                               |         | open   |
          |   |   |   | name      | string  |                               |         | open   |
          |   |   |   | city      | ref     | datasets/deduplicate/cli/City |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )

    store = context.get("store")
    manifest = store.manifest
    backend = manifest.backend
    insp = sa.inspect(backend.engine)
    city_name = get_pg_table_name("datasets/deduplicate/cli/City")
    uq_city_constraint = get_pg_constraint_name("datasets/deduplicate/cli/City", ["id"])
    assert any(uq_city_constraint == constraint["name"] for constraint in insp.get_unique_constraints(city_name))

    with backend.begin() as conn:
        conn.execute(f'''
            ALTER TABLE "{city_name}" DROP CONSTRAINT "{uq_city_constraint}";
        ''')
    insp = sa.inspect(backend.engine)
    assert not any(uq_city_constraint == constraint["name"] for constraint in insp.get_unique_constraints(city_name))

    result = cli.invoke(context.get("rc"), ["admin", Script.DEDUPLICATE.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.DEDUPLICATE.value, ScriptStatus.REQUIRED) in result.stdout

    insp = sa.inspect(backend.engine)
    assert any(uq_city_constraint == constraint["name"] for constraint in insp.get_unique_constraints(city_name))


def test_admin_deduplicate_requires_destructive(
    context: Context,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    cli: SpintaCliRunner,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(
            """
        d | r | b | m | property  | type    | ref                           | prepare | access | level
        datasets/deduplicate/rand |         |                               |         |        |
          |   |   | Random        |         | id                            |         |        |
          |   |   |   | id        | integer |                               |         | open   |
          |   |   |   | name      | string  |                               |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )

    app = create_test_client(context)
    app.authorize(
        [
            "spinta_insert",
            "spinta_getone",
            "spinta_delete",
            "spinta_wipe",
            "spinta_search",
            "spinta_set_meta_fields",
            "spinta_move",
            "spinta_getall",
            "spinta_changes",
        ]
    )

    data = {
        "datasets/deduplicate/rand/Random": [
            {
                "_id": "4f5a99a4-eb5f-4792-a280-5914baa3ca49",
                "id": 0,
                "name": "rand_0",
            },
            {
                "_id": "58639700-37d4-4479-b800-5ab006b9c914",
                "id": 0,
                "name": "rand_0",
            },
            {
                "_id": "48b7cc6c-0bab-4ed8-bc8f-2da1c486c895",
                "id": 1,
                "name": "rand_1",
            },
            {
                "_id": "496effd6-1f1c-43ec-aefd-9abb50e04595",
                "id": 1,
                "name": "rand_1",
            },
            {
                "_id": "5fbc4498-9013-4905-9bc0-a2b94f12d4f2",
                "id": 2,
                "name": "rand_2",
            },
        ]
    }
    store = context.get("store")
    manifest = store.manifest
    backend = manifest.backend
    insp = sa.inspect(backend.engine)
    random_name = get_pg_table_name("datasets/deduplicate/rand/Random")
    uq_random_constraint = get_pg_constraint_name("datasets/deduplicate/rand/Random", ["id"])
    assert any(uq_random_constraint == constraint["name"] for constraint in insp.get_unique_constraints(random_name))

    with backend.begin() as conn:
        conn.execute(f'''
            ALTER TABLE "{random_name}" DROP CONSTRAINT "{uq_random_constraint}";
        ''')
    insp = sa.inspect(backend.engine)
    assert not any(
        uq_random_constraint == constraint["name"] for constraint in insp.get_unique_constraints(random_name)
    )

    # insert data
    for model, items in data.items():
        for item in items:
            resp = app.post(model, json=item)
            rev = resp.json()["_revision"]
            item["_revision"] = rev

    result = app.get("datasets/deduplicate/rand/Random")
    assert (
        listdata(result, "id", "name", "_id", "city", "_revision", full=True)
        == data["datasets/deduplicate/rand/Random"]
    )

    result = cli.invoke(
        context.get("rc"),
        [
            "admin",
            Script.DEDUPLICATE.value,
        ],
    )
    assert result.exit_code == 0
    assert script_check_status_message(Script.DEDUPLICATE.value, ScriptStatus.REQUIRED) in result.stdout
    assert (
        '"datasets/deduplicate/rand/Random" contains duplicate values, use --destructive to migrate them'
        in result.stdout
    )

    insp = sa.inspect(backend.engine)
    assert not any(
        uq_random_constraint == constraint["name"] for constraint in insp.get_unique_constraints(random_name)
    )

    result = cli.invoke(context.get("rc"), ["admin", Script.DEDUPLICATE.value, "-d"])
    assert result.exit_code == 0
    assert script_check_status_message(Script.DEDUPLICATE.value, ScriptStatus.REQUIRED) in result.stdout
    assert (
        '"datasets/deduplicate/rand/Random" contains duplicate values, use --destructive to migrate them'
        not in result.stdout
    )

    insp = sa.inspect(backend.engine)
    assert any(uq_random_constraint == constraint["name"] for constraint in insp.get_unique_constraints(random_name))

    result = app.get("datasets/deduplicate/rand/Random")
    assert listdata(result, "id", "name", "city", "_id", full=True) == [
        {
            "_id": "58639700-37d4-4479-b800-5ab006b9c914",
            "id": 0,
            "name": "rand_0",
        },
        {
            "_id": "496effd6-1f1c-43ec-aefd-9abb50e04595",
            "id": 1,
            "name": "rand_1",
        },
        {
            "_id": "5fbc4498-9013-4905-9bc0-a2b94f12d4f2",
            "id": 2,
            "name": "rand_2",
        },
    ]

    resp = app.get("/datasets/deduplicate/rand/Random/:changes/-2")
    assert resp.status_code == 200
    assert listdata(resp, "_cid", "_id", "_op", "_same_as", "id", "name", "country._id", full=True) == [
        {
            "_cid": 6,
            "_op": "move",
            "_id": "4f5a99a4-eb5f-4792-a280-5914baa3ca49",
            "_same_as": "58639700-37d4-4479-b800-5ab006b9c914",
        },
        {
            "_cid": 7,
            "_op": "move",
            "_id": "48b7cc6c-0bab-4ed8-bc8f-2da1c486c895",
            "_same_as": "496effd6-1f1c-43ec-aefd-9abb50e04595",
        },
    ]


def test_admin_deduplicate_simple(
    context: Context,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    cli: SpintaCliRunner,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(
            """
        d | r | b | m | property  | type    | ref                           | prepare | access | level
        datasets/deduplicate/cli  |         |                               |         |        |
          |   |   | Country       |         | id                            |         |        |
          |   |   |   | id        | integer |                               |         | open   |
          |   |   |   | name      | string  |                               |         | open   |
          |   |   | City          |         | id                            |         |        |
          |   |   |   | id        | integer |                               |         | open   |
          |   |   |   | name      | string  |                               |         | open   |
          |   |   |   | country   | ref     | Country                       |         | open   |
        datasets/deduplicate/rand |         |                               |         |        |
          |   |   | Random        |         | id                            |         |        |
          |   |   |   | id        | integer |                               |         | open   |
          |   |   |   | name      | string  |                               |         | open   |
          |   |   |   | city      | ref     | datasets/deduplicate/cli/City |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )

    app = create_test_client(context)
    app.authorize(
        [
            "spinta_insert",
            "spinta_getone",
            "spinta_delete",
            "spinta_wipe",
            "spinta_search",
            "spinta_set_meta_fields",
            "spinta_move",
            "spinta_getall",
            "spinta_changes",
        ]
    )

    data = {
        "datasets/deduplicate/cli/Country": [
            {"_id": "9b6442ed-ed75-4f78-bb2b-617d63ad837a", "id": 0, "name": "Lithuania"}
        ],
        "datasets/deduplicate/cli/City": [
            {
                "_id": "f55f9c6f-43ff-49e6-bfcf-3e9b33ffb987",
                "id": 0,
                "name": "Vilnius",
                "country": {"_id": "9b6442ed-ed75-4f78-bb2b-617d63ad837a"},
            },
            {
                "_id": "53d25cc5-d1cb-409e-883c-cb277057c654",
                "id": 1,
                "name": "Kaunas",
                "country": {"_id": "9b6442ed-ed75-4f78-bb2b-617d63ad837a"},
            },
        ],
        "datasets/deduplicate/rand/Random": [
            {
                "_id": "4f5a99a4-eb5f-4792-a280-5914baa3ca49",
                "id": 0,
                "name": "rand_0",
                "city": {"_id": "53d25cc5-d1cb-409e-883c-cb277057c654"},
            },
            {
                "_id": "58639700-37d4-4479-b800-5ab006b9c914",
                "id": 0,
                "name": "rand_0",
                "city": {"_id": "f55f9c6f-43ff-49e6-bfcf-3e9b33ffb987"},
            },
            {
                "_id": "48b7cc6c-0bab-4ed8-bc8f-2da1c486c895",
                "id": 1,
                "name": "rand_1",
                "city": {"_id": "f55f9c6f-43ff-49e6-bfcf-3e9b33ffb987"},
            },
            {
                "_id": "496effd6-1f1c-43ec-aefd-9abb50e04595",
                "id": 1,
                "name": "rand_1",
                "city": {"_id": "53d25cc5-d1cb-409e-883c-cb277057c654"},
            },
            {
                "_id": "5fbc4498-9013-4905-9bc0-a2b94f12d4f2",
                "id": 2,
                "name": "rand_2",
                "city": {"_id": "f55f9c6f-43ff-49e6-bfcf-3e9b33ffb987"},
            },
        ],
    }
    store = context.get("store")
    manifest = store.manifest
    backend = manifest.backend
    insp = sa.inspect(backend.engine)
    random_name = get_pg_table_name("datasets/deduplicate/rand/Random")
    uq_random_constraint = get_pg_constraint_name("datasets/deduplicate/rand/Random", ["id"])
    assert any(uq_random_constraint == constraint["name"] for constraint in insp.get_unique_constraints(random_name))

    with backend.begin() as conn:
        conn.execute(f'''
            ALTER TABLE "{random_name}" DROP CONSTRAINT "{uq_random_constraint}";
        ''')
    insp = sa.inspect(backend.engine)
    assert not any(
        uq_random_constraint == constraint["name"] for constraint in insp.get_unique_constraints(random_name)
    )

    # insert data
    for model, items in data.items():
        for item in items:
            resp = app.post(model, json=item)
            rev = resp.json()["_revision"]
            item["_revision"] = rev

    result = app.get("datasets/deduplicate/cli/Country")
    assert listdata(result, "id", "name", "_revision", "_id", full=True) == data["datasets/deduplicate/cli/Country"]

    result = app.get("datasets/deduplicate/cli/City")
    assert (
        listdata(result, "id", "name", "country", "_revision", "_id", full=True)
        == data["datasets/deduplicate/cli/City"]
    )

    result = app.get("datasets/deduplicate/rand/Random")
    assert (
        listdata(result, "id", "name", "_id", "city", "_revision", full=True)
        == data["datasets/deduplicate/rand/Random"]
    )

    result = cli.invoke(context.get("rc"), ["admin", Script.DEDUPLICATE.value, "-d"])
    assert result.exit_code == 0
    assert script_check_status_message(Script.DEDUPLICATE.value, ScriptStatus.REQUIRED) in result.stdout

    insp = sa.inspect(backend.engine)
    assert any(uq_random_constraint == constraint["name"] for constraint in insp.get_unique_constraints(random_name))

    result = app.get("datasets/deduplicate/cli/Country")
    assert listdata(result, "id", "name", "_id", "_revision", full=True) == data["datasets/deduplicate/cli/Country"]

    result = app.get("datasets/deduplicate/cli/City")
    assert (
        listdata(result, "id", "name", "country", "_id", "_revision", full=True)
        == data["datasets/deduplicate/cli/City"]
    )

    result = app.get("datasets/deduplicate/rand/Random")
    assert listdata(result, "id", "name", "city", "_id", full=True) == [
        {
            "_id": "58639700-37d4-4479-b800-5ab006b9c914",
            "id": 0,
            "name": "rand_0",
            "city": {"_id": "f55f9c6f-43ff-49e6-bfcf-3e9b33ffb987"},
        },
        {
            "_id": "496effd6-1f1c-43ec-aefd-9abb50e04595",
            "id": 1,
            "name": "rand_1",
            "city": {"_id": "53d25cc5-d1cb-409e-883c-cb277057c654"},
        },
        {
            "_id": "5fbc4498-9013-4905-9bc0-a2b94f12d4f2",
            "id": 2,
            "name": "rand_2",
            "city": {"_id": "f55f9c6f-43ff-49e6-bfcf-3e9b33ffb987"},
        },
    ]

    resp = app.get("/datasets/deduplicate/rand/Random/:changes/-2")
    assert resp.status_code == 200
    assert listdata(resp, "_cid", "_id", "_op", "_same_as", "id", "name", "country._id", full=True) == [
        {
            "_cid": 6,
            "_op": "move",
            "_id": "4f5a99a4-eb5f-4792-a280-5914baa3ca49",
            "_same_as": "58639700-37d4-4479-b800-5ab006b9c914",
        },
        {
            "_cid": 7,
            "_op": "move",
            "_id": "48b7cc6c-0bab-4ed8-bc8f-2da1c486c895",
            "_same_as": "496effd6-1f1c-43ec-aefd-9abb50e04595",
        },
    ]


def test_admin_deduplicate_referenced(
    context: Context,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    cli: SpintaCliRunner,
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(
            """
        d | r | b | m | property  | type    | ref                           | prepare | access | level
        datasets/deduplicate/cli  |         |                               |         |        |
          |   |   | Country       |         | id                            |         |        |
          |   |   |   | id        | integer |                               |         | open   |
          |   |   |   | name      | string  |                               |         | open   |
          |   |   | City          |         | id                            |         |        |
          |   |   |   | id        | integer |                               |         | open   |
          |   |   |   | name      | string  |                               |         | open   |
          |   |   |   | country   | ref     | Country                       |         | open   |
        datasets/deduplicate/rand |         |                               |         |        |
          |   |   | Random        |         | id                            |         |        |
          |   |   |   | id        | integer |                               |         | open   |
          |   |   |   | name      | string  |                               |         | open   |
          |   |   |   | city      | ref     | datasets/deduplicate/cli/City |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )

    app = create_test_client(context)
    app.authorize(
        [
            "spinta_insert",
            "spinta_getone",
            "spinta_delete",
            "spinta_wipe",
            "spinta_search",
            "spinta_set_meta_fields",
            "spinta_move",
            "spinta_getall",
            "spinta_changes",
        ]
    )

    data = {
        "datasets/deduplicate/cli/Country": [
            {"_id": "9b6442ed-ed75-4f78-bb2b-617d63ad837a", "id": 0, "name": "Lithuania"},
            {"_id": "d8785261-cc15-409f-800d-aeacf44162f2", "id": 0, "name": "Lithuania"},
            {"_id": "2582807c-f4d5-4026-8fa8-023088d747bb", "id": 1, "name": "Poland"},
        ],
        "datasets/deduplicate/cli/City": [
            {
                "_id": "f55f9c6f-43ff-49e6-bfcf-3e9b33ffb987",
                "id": 0,
                "name": "Vilnius",
                "country": {"_id": "9b6442ed-ed75-4f78-bb2b-617d63ad837a"},
            },
            {
                "_id": "f65f9c6f-43ff-49e6-bfcf-3e9b33ffb987",
                "id": 0,
                "name": "Vilnius",
                "country": {"_id": "9b6442ed-ed75-4f78-bb2b-617d63ad837a"},
            },
            {
                "_id": "53d25cc5-d1cb-409e-883c-cb277057c654",
                "id": 1,
                "name": "Kaunas",
                "country": {"_id": "d8785261-cc15-409f-800d-aeacf44162f2"},
            },
            {
                "_id": "e7a7f160-37d4-4e37-a248-b32f7ce003c1",
                "id": 2,
                "name": "Warsaw",
                "country": {"_id": "2582807c-f4d5-4026-8fa8-023088d747bb"},
            },
        ],
        "datasets/deduplicate/rand/Random": [
            {
                "_id": "4f5a99a4-eb5f-4792-a280-5914baa3ca49",
                "id": 0,
                "name": "rand_0",
                "city": {"_id": "53d25cc5-d1cb-409e-883c-cb277057c654"},
            },
            {
                "_id": "58639700-37d4-4479-b800-5ab006b9c914",
                "id": 0,
                "name": "rand_0",
                "city": {"_id": "f55f9c6f-43ff-49e6-bfcf-3e9b33ffb987"},
            },
            {
                "_id": "49b7cc6c-0bab-4ed8-bc8f-2da1c486c895",
                "id": 1,
                "name": "rand_1",
                "city": {"_id": "f65f9c6f-43ff-49e6-bfcf-3e9b33ffb987"},
            },
            {
                "_id": "586effd6-1f1c-43ec-aefd-9abb50e04595",
                "id": 1,
                "name": "rand_1",
                "city": {"_id": "f55f9c6f-43ff-49e6-bfcf-3e9b33ffb987"},
            },
            {
                "_id": "5fbc4498-9013-4905-9bc0-a2b94f12d4f2",
                "id": 2,
                "name": "rand_2",
                "city": {"_id": "f55f9c6f-43ff-49e6-bfcf-3e9b33ffb987"},
            },
        ],
    }
    store = context.get("store")
    manifest = store.manifest
    backend = manifest.backend
    insp = sa.inspect(backend.engine)
    random_name = get_pg_table_name("datasets/deduplicate/rand/Random")
    city_name = get_pg_table_name("datasets/deduplicate/cli/City")
    country_name = get_pg_table_name("datasets/deduplicate/cli/Country")
    uq_random_constraint = get_pg_constraint_name("datasets/deduplicate/rand/Random", ["id"])
    uq_city_constraint = get_pg_constraint_name("datasets/deduplicate/cli/City", ["id"])
    uq_country_constraint = get_pg_constraint_name("datasets/deduplicate/cli/Country", ["id"])
    assert any(uq_random_constraint == constraint["name"] for constraint in insp.get_unique_constraints(random_name))
    assert any(uq_city_constraint == constraint["name"] for constraint in insp.get_unique_constraints(city_name))
    assert any(uq_country_constraint == constraint["name"] for constraint in insp.get_unique_constraints(country_name))

    with backend.begin() as conn:
        conn.execute(f'''
            ALTER TABLE "{random_name}" DROP CONSTRAINT "{uq_random_constraint}";
            ALTER TABLE "{city_name}" DROP CONSTRAINT "{uq_city_constraint}";
            ALTER TABLE "{country_name}" DROP CONSTRAINT "{uq_country_constraint}";
        ''')
    insp = sa.inspect(backend.engine)
    assert not any(
        constraint["name"] in (uq_country_constraint, uq_city_constraint, uq_random_constraint)
        for constraint in insp.get_unique_constraints(random_name)
    )

    # insert data
    for model, items in data.items():
        for item in items:
            resp = app.post(model, json=item)
            rev = resp.json()["_revision"]
            item["_revision"] = rev

    result = app.get("datasets/deduplicate/cli/Country")
    assert listdata(result, "id", "name", "_id", "_revision", full=True) == data["datasets/deduplicate/cli/Country"]

    result = app.get("datasets/deduplicate/cli/City")
    assert (
        listdata(result, "id", "name", "_id", "country", "_revision", full=True)
        == data["datasets/deduplicate/cli/City"]
    )

    result = app.get("datasets/deduplicate/rand/Random")
    assert (
        listdata(result, "id", "name", "_id", "city", "_revision", full=True)
        == data["datasets/deduplicate/rand/Random"]
    )

    result = cli.invoke(context.get("rc"), ["admin", Script.DEDUPLICATE.value, "-d"])
    assert result.exit_code == 0
    assert script_check_status_message(Script.DEDUPLICATE.value, ScriptStatus.REQUIRED) in result.stdout

    insp = sa.inspect(backend.engine)
    assert any(uq_random_constraint == constraint["name"] for constraint in insp.get_unique_constraints(random_name))
    assert any(uq_city_constraint == constraint["name"] for constraint in insp.get_unique_constraints(city_name))
    assert any(uq_country_constraint == constraint["name"] for constraint in insp.get_unique_constraints(country_name))

    result = app.get("datasets/deduplicate/cli/Country")
    assert listdata(result, "id", "name", "_id", full=True) == [
        {"_id": "d8785261-cc15-409f-800d-aeacf44162f2", "id": 0, "name": "Lithuania"},
        {"_id": "2582807c-f4d5-4026-8fa8-023088d747bb", "id": 1, "name": "Poland"},
    ]

    result = app.get("datasets/deduplicate/cli/City")
    assert listdata(result, "id", "name", "country", "_id", full=True) == [
        {
            "_id": "f65f9c6f-43ff-49e6-bfcf-3e9b33ffb987",
            "id": 0,
            "name": "Vilnius",
            "country": {"_id": "d8785261-cc15-409f-800d-aeacf44162f2"},
        },
        {
            "_id": "53d25cc5-d1cb-409e-883c-cb277057c654",
            "id": 1,
            "name": "Kaunas",
            "country": {"_id": "d8785261-cc15-409f-800d-aeacf44162f2"},
        },
        {
            "_id": "e7a7f160-37d4-4e37-a248-b32f7ce003c1",
            "id": 2,
            "name": "Warsaw",
            "country": {"_id": "2582807c-f4d5-4026-8fa8-023088d747bb"},
        },
    ]

    result = app.get("datasets/deduplicate/rand/Random")
    assert listdata(result, "id", "name", "city", "_id", full=True) == [
        {
            "_id": "58639700-37d4-4479-b800-5ab006b9c914",
            "id": 0,
            "name": "rand_0",
            "city": {"_id": "f65f9c6f-43ff-49e6-bfcf-3e9b33ffb987"},
        },
        {
            "_id": "586effd6-1f1c-43ec-aefd-9abb50e04595",
            "id": 1,
            "name": "rand_1",
            "city": {"_id": "f65f9c6f-43ff-49e6-bfcf-3e9b33ffb987"},
        },
        {
            "_id": "5fbc4498-9013-4905-9bc0-a2b94f12d4f2",
            "id": 2,
            "name": "rand_2",
            "city": {"_id": "f65f9c6f-43ff-49e6-bfcf-3e9b33ffb987"},
        },
    ]

    resp = app.get("/datasets/deduplicate/cli/Country/:changes/-1")
    assert resp.status_code == 200
    assert listdata(resp, "_cid", "_id", "_op", "_same_as", "id", "name", full=True) == [
        {
            "_cid": 4,
            "_op": "move",
            "_id": "9b6442ed-ed75-4f78-bb2b-617d63ad837a",
            "_same_as": "d8785261-cc15-409f-800d-aeacf44162f2",
        },
    ]

    resp = app.get("/datasets/deduplicate/cli/City/:changes/-3")
    assert resp.status_code == 200
    assert listdata(resp, "_cid", "_id", "_op", "_same_as", "id", "name", "country._id", full=True) == [
        {
            "_cid": 5,
            "_op": "patch",
            "_id": "f55f9c6f-43ff-49e6-bfcf-3e9b33ffb987",
            "country._id": "d8785261-cc15-409f-800d-aeacf44162f2",
        },
        {
            "_cid": 6,
            "_op": "patch",
            "_id": "f65f9c6f-43ff-49e6-bfcf-3e9b33ffb987",
            "country._id": "d8785261-cc15-409f-800d-aeacf44162f2",
        },
        {
            "_cid": 7,
            "_op": "move",
            "_id": "f55f9c6f-43ff-49e6-bfcf-3e9b33ffb987",
            "_same_as": "f65f9c6f-43ff-49e6-bfcf-3e9b33ffb987",
        },
    ]

    resp = app.get("/datasets/deduplicate/rand/Random/:changes/-5")
    assert resp.status_code == 200
    assert listdata(resp, "_cid", "_id", "_op", "_same_as", "id", "name", "city._id", sort="_cid", full=True) == [
        {
            "_cid": 6,
            "_op": "patch",
            "_id": "58639700-37d4-4479-b800-5ab006b9c914",
            "city._id": "f65f9c6f-43ff-49e6-bfcf-3e9b33ffb987",
        },
        {
            "_cid": 7,
            "_op": "patch",
            "_id": "586effd6-1f1c-43ec-aefd-9abb50e04595",
            "city._id": "f65f9c6f-43ff-49e6-bfcf-3e9b33ffb987",
        },
        {
            "_cid": 8,
            "_op": "patch",
            "_id": "5fbc4498-9013-4905-9bc0-a2b94f12d4f2",
            "city._id": "f65f9c6f-43ff-49e6-bfcf-3e9b33ffb987",
        },
        {
            "_cid": 9,
            "_op": "move",
            "_id": "4f5a99a4-eb5f-4792-a280-5914baa3ca49",
            "_same_as": "58639700-37d4-4479-b800-5ab006b9c914",
        },
        {
            "_cid": 10,
            "_op": "move",
            "_id": "49b7cc6c-0bab-4ed8-bc8f-2da1c486c895",
            "_same_as": "586effd6-1f1c-43ec-aefd-9abb50e04595",
        },
    ]
