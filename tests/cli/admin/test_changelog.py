import uuid
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


def test_admin_changelog_requires_redirect(
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
    result = cli.invoke(context.get("rc"), ["admin", Script.CHANGELOG.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.CHANGELOG.value, ScriptStatus.SKIPPED) in result.stdout

    assert not insp.has_table(country_redirect)
    assert not insp.has_table(city_redirect)
    assert not insp.has_table(random_redirect)

    result = cli.invoke(context.get("rc"), ["upgrade", UpgradeScript.REDIRECT.value])
    assert result.exit_code == 0
    assert script_check_status_message(UpgradeScript.REDIRECT.value, ScriptStatus.REQUIRED) in result.stdout

    assert insp.has_table(country_redirect)
    assert insp.has_table(city_redirect)
    assert insp.has_table(random_redirect)

    with open(tmp_path / "modellist.txt", "w") as f:
        f.write("datasets/deduplicate/cli/req/Country\n")
        f.write("datasets/deduplicate/cli/req/City\n")
        f.write("datasets/deduplicate/rand/req/Random\n")

    result = cli.invoke(
        context.get("rc"), ["admin", Script.CHANGELOG.value, "-c", "--input", f"{tmp_path / 'modellist.txt'}"]
    )
    assert result.exit_code == 0
    assert script_check_status_message(Script.CHANGELOG.value, ScriptStatus.PASSED) in result.stdout


def test_admin_changelog_requires_deduplicate(
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

    result = cli.invoke(context.get("rc"), ["admin", Script.CHANGELOG.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.CHANGELOG.value, ScriptStatus.SKIPPED) in result.stdout

    result = cli.invoke(context.get("rc"), ["admin", Script.DEDUPLICATE.value])
    assert result.exit_code == 0
    assert script_check_status_message(Script.DEDUPLICATE.value, ScriptStatus.REQUIRED) in result.stdout

    insp = sa.inspect(backend.engine)
    assert any(uq_city_constraint == constraint["name"] for constraint in insp.get_unique_constraints(city_name))

    with open(tmp_path / "modellist.txt", "w") as f:
        f.write("datasets/deduplicate/cli/Country\n")
        f.write("datasets/deduplicate/cli/City\n")
        f.write("datasets/deduplicate/rand/Random\n")

    result = cli.invoke(
        context.get("rc"), ["admin", Script.CHANGELOG.value, "--input", f"{tmp_path / 'modellist.txt'}"]
    )
    assert result.exit_code == 0
    assert script_check_status_message(Script.CHANGELOG.value, ScriptStatus.PASSED) in result.stdout


def test_admin_changelog_old_deleted_entries(
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
        d | r | b | m | property       | type    | ref                           | prepare | access | level
        datasets/deduplicate/changelog |         |                               |         |        |
          |   |   | Country            |         | id                            |         |        |
          |   |   |   | id             | integer |                               |         | open   |
          |   |   |   | name           | string  |                               |         | open   |
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
    c_0 = str(uuid.uuid4())
    c_1 = str(uuid.uuid4())
    c_2 = str(uuid.uuid4())
    c_3 = str(uuid.uuid4())
    c_0_1 = str(uuid.uuid4())
    c_0_2 = str(uuid.uuid4())
    c_1_1 = str(uuid.uuid4())

    data = [
        {"_id": c_0, "id": 0, "name": "C0"},
        {"_id": c_1, "id": 1, "name": "C1"},
        {"_id": c_2, "id": 2, "name": "C2"},
        {"_id": c_3, "id": 3, "name": "C3"},
    ]

    """
        Recreate these actions:
            c_0 - inserted
            c_1 - inserted
            c_2 - inserted
            c_3 - inserted
            c_0 - deleted
            c_0_1 - inserted
            c_0_1 - deleted
            c_0_2 - inserted
            c_1 - deleted
            c_1_1 - inserted
            c_1_1 - deleted
            
        Output:
            c_0_2
            c_2
            c_3
    """

    for row in data:
        app.post("/datasets/deduplicate/changelog/Country", json=row)

    app.delete(f"/datasets/deduplicate/changelog/Country/{c_0}")
    app.post("/datasets/deduplicate/changelog/Country", json={"_id": c_0_1, "id": 0, "name": "C0"})
    app.delete(f"/datasets/deduplicate/changelog/Country/{c_0_1}")
    app.post("/datasets/deduplicate/changelog/Country", json={"_id": c_0_2, "id": 0, "name": "C0"})
    app.delete(f"/datasets/deduplicate/changelog/Country/{c_1}")
    app.post("/datasets/deduplicate/changelog/Country", json={"_id": c_1_1, "id": 1, "name": "C1"})
    app.delete(f"/datasets/deduplicate/changelog/Country/{c_1_1}")

    result = app.get("/datasets/deduplicate/changelog/Country")
    assert listdata(result, "_id", "id", "name", sort="id", full=True) == [
        {"_id": c_0_2, "id": 0, "name": "C0"},
        {"_id": c_2, "id": 2, "name": "C2"},
        {"_id": c_3, "id": 3, "name": "C3"},
    ]

    result = app.get("/datasets/deduplicate/changelog/Country/:changes")
    assert listdata(result, "_cid", "_op", "_id", "_same_as", "id", "name", sort="_cid", full=True) == [
        {"_cid": 1, "_op": "insert", "_id": c_0, "id": 0, "name": "C0"},
        {"_cid": 2, "_op": "insert", "_id": c_1, "id": 1, "name": "C1"},
        {"_cid": 3, "_op": "insert", "_id": c_2, "id": 2, "name": "C2"},
        {"_cid": 4, "_op": "insert", "_id": c_3, "id": 3, "name": "C3"},
        {"_cid": 5, "_op": "delete", "_id": c_0},
        {"_cid": 6, "_op": "insert", "_id": c_0_1, "id": 0, "name": "C0"},
        {"_cid": 7, "_op": "delete", "_id": c_0_1},
        {"_cid": 8, "_op": "insert", "_id": c_0_2, "id": 0, "name": "C0"},
        {"_cid": 9, "_op": "delete", "_id": c_1},
        {"_cid": 10, "_op": "insert", "_id": c_1_1, "id": 1, "name": "C1"},
        {"_cid": 11, "_op": "delete", "_id": c_1_1},
    ]

    with open(tmp_path / "modellist.txt", "w") as f:
        f.write("datasets/deduplicate/changelog/Country")

    result = cli.invoke(
        context.get("rc"), ["admin", Script.CHANGELOG.value, "--input", f"{tmp_path / 'modellist.txt'}"]
    )
    assert result.exit_code == 0
    assert script_check_status_message(Script.CHANGELOG.value, ScriptStatus.REQUIRED) in result.stdout

    result = app.get("/datasets/deduplicate/changelog/Country")
    assert listdata(result, "_id", "id", "name", sort="id", full=True) == [
        {"_id": c_0_2, "id": 0, "name": "C0"},
        {"_id": c_2, "id": 2, "name": "C2"},
        {"_id": c_3, "id": 3, "name": "C3"},
    ]

    result = app.get("/datasets/deduplicate/changelog/Country/:changes/-3")
    assert listdata(result, "_cid", "_op", "_id", "_same_as", "id", "name", sort="_cid", full=True) == [
        {"_cid": 12, "_op": "move", "_id": c_0_1, "_same_as": c_0_2},
        {"_cid": 13, "_op": "move", "_id": c_0, "_same_as": c_0_2},
        {"_cid": 14, "_op": "move", "_id": c_1, "_same_as": c_1_1},
    ]
