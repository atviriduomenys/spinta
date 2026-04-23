from pathlib import Path
from _pytest.fixtures import FixtureRequest

from spinta import commands
from spinta.cli.helpers.admin.components import Script
from spinta.cli.helpers.script.components import ScriptStatus
from spinta.cli.helpers.script.helpers import script_check_status_message
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner, message_in_result
from spinta.testing.client import create_test_client
from spinta.testing.csv import read_csv
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.tabular import create_tabular_manifest


def test_admin_enum_list_all_valid(
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
        datasets/enums/cli/req        |         |         |         |        |
          |   |   | Country           |         | id      |         |        |
          |   |   |   | id            | integer |         |         | open   |
          |   |   |   | name          | string  |         |         | open   |
          |   |   |   |               | enum    |         | 'Test1' | open   |
          |   |   |   |               |         |         | 'Test2' | open   |
          |   |   | City              |         |         |         |        |
          |   |   |   | id            | integer |         |         | open   |
          |   |   |   |               | enum    |         | 1       | open   |
          |   |   |   |               |         |         | 2       | open   |
          |   |   |   | name          | string  |         |         | open   |
          |   |   |   |               | enum    |         | 'Test1' | open   |
          |   |   |   |               |         |         | 'Test2' | open   |
          |   |   |   | country       | ref     | Country |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )
    app = create_test_client(context)
    app.authorize(["uapi:/:create"])

    res0 = app.post("/datasets/enums/cli/req/Country", json={"id": 0, "name": "Test1"})
    res1 = app.post("/datasets/enums/cli/req/Country", json={"id": 1, "name": "Test2"})
    app.post("/datasets/enums/cli/req/City", json={"id": 1, "name": "Test1", "country": res0.json()["_id"]})
    app.post("/datasets/enums/cli/req/City", json={"id": 2, "name": "Test2", "country": res1.json()["_id"]})

    result = cli.invoke(context.get("rc"), ["admin", Script.ENUM_LIST.value, "-o", tmp_path / "output.csv"])
    assert result.exit_code == 0
    assert message_in_result(result, script_check_status_message(Script.ENUM_LIST.value, ScriptStatus.REQUIRED))
    assert read_csv(tmp_path / "output.csv") == [["model", "property", "invalid_value"]]


def test_admin_enum_list_invalid_str(
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
        datasets/enums/cli/req        |         |         |         |        |
          |   |   | Country           |         | id      |         |        |
          |   |   |   | id            | integer |         |         | open   |
          |   |   |   | name          | string  |         |         | open   |
          |   |   |   |               | enum    |         | 'Test1' | open   |
          |   |   |   |               |         |         | 'Test2' | open   |
          |   |   | City              |         |         |         |        |
          |   |   |   | id            | integer |         |         | open   |
          |   |   |   |               | enum    |         | 1       | open   |
          |   |   |   |               |         |         | 2       | open   |
          |   |   |   | name          | string  |         |         | open   |
          |   |   |   |               | enum    |         | 'Test1' | open   |
          |   |   |   |               |         |         | 'Test2' | open   |
          |   |   |   | country       | ref     | Country |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )
    app = create_test_client(context)
    app.authorize(["uapi:/:create"])

    res0 = app.post("/datasets/enums/cli/req/Country", json={"id": 0, "name": "Test1"})
    res1 = app.post("/datasets/enums/cli/req/Country", json={"id": 1, "name": "Test2"})
    res2 = app.post("/datasets/enums/cli/req/Country", json={"id": 2, "name": "Test2"})
    app.post("/datasets/enums/cli/req/City", json={"id": 1, "name": "Test1", "country": res0.json()["_id"]})
    app.post("/datasets/enums/cli/req/City", json={"id": 2, "name": "Test2", "country": res1.json()["_id"]})

    store = context.get("store")
    manifest = store.manifest
    backend = manifest.backend
    model = commands.get_model(context, manifest, "datasets/enums/cli/req/Country")
    country_table = backend.get_table(model)

    with backend.begin() as conn:
        conn.execute(country_table.update().values(name="Test3").where(country_table.c._id == res0.json()["_id"]))
        conn.execute(country_table.update().values(name="Test4").where(country_table.c._id == res2.json()["_id"]))

    result = cli.invoke(context.get("rc"), ["admin", Script.ENUM_LIST.value, "-o", tmp_path / "output.csv"])
    assert result.exit_code == 0
    assert message_in_result(result, script_check_status_message(Script.ENUM_LIST.value, ScriptStatus.REQUIRED))
    assert read_csv(tmp_path / "output.csv") == [
        ["model", "property", "invalid_value"],
        ["datasets/enums/cli/req/Country", "name", "Test3"],
        ["", "", "Test4"],
    ]


def test_admin_enum_list_invalid_int(
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
        datasets/enums/cli/req        |         |         |         |        |
          |   |   | City              |         |         |         |        |
          |   |   |   | id            | integer |         |         | open   |
          |   |   |   |               | enum    |         | 1       | open   |
          |   |   |   |               |         |         | 2       | open   |
          |   |   |   | name          | string  |         |         | open   |
          |   |   |   |               | enum    |         | 'Test1' | open   |
          |   |   |   |               |         |         | 'Test2' | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )
    app = create_test_client(context)
    app.authorize(["uapi:/:create"])

    app.post("/datasets/enums/cli/req/City", json={"id": 1, "name": "Test1"})
    app.post("/datasets/enums/cli/req/City", json={"id": 2, "name": "Test2"})
    res0 = app.post("/datasets/enums/cli/req/City", json={"id": 2, "name": "Test2"})
    res1 = app.post("/datasets/enums/cli/req/City", json={"id": 2, "name": "Test2"})

    store = context.get("store")
    manifest = store.manifest
    backend = manifest.backend
    model = commands.get_model(context, manifest, "datasets/enums/cli/req/City")
    city_table = backend.get_table(model)

    with backend.begin() as conn:
        conn.execute(city_table.update().values(id=10).where(city_table.c._id == res0.json()["_id"]))
        conn.execute(city_table.update().values(id=20).where(city_table.c._id == res1.json()["_id"]))

    result = cli.invoke(context.get("rc"), ["admin", Script.ENUM_LIST.value, "-o", tmp_path / "output.csv"])
    assert result.exit_code == 0
    assert message_in_result(result, script_check_status_message(Script.ENUM_LIST.value, ScriptStatus.REQUIRED))
    assert read_csv(tmp_path / "output.csv") == [
        ["model", "property", "invalid_value"],
        ["datasets/enums/cli/req/City", "id", "10"],
        ["", "", "20"],
    ]


def test_admin_enum_list_invalid_multiple_properties(
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
        datasets/enums/cli/req        |         |         |         |        |
          |   |   | City              |         |         |         |        |
          |   |   |   | id            | integer |         |         | open   |
          |   |   |   |               | enum    |         | 1       | open   |
          |   |   |   |               |         |         | 2       | open   |
          |   |   |   | name          | string  |         |         | open   |
          |   |   |   |               | enum    |         | 'Test1' | open   |
          |   |   |   |               |         |         | 'Test2' | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )
    app = create_test_client(context)
    app.authorize(["uapi:/:create"])

    app.post("/datasets/enums/cli/req/City", json={"id": 1, "name": "Test1"})
    app.post("/datasets/enums/cli/req/City", json={"id": 2, "name": "Test2"})
    res0 = app.post("/datasets/enums/cli/req/City", json={"id": 2, "name": "Test2"})
    res1 = app.post("/datasets/enums/cli/req/City", json={"id": 2, "name": "Test2"})
    res2 = app.post("/datasets/enums/cli/req/City", json={"id": 2, "name": "Test2"})
    res3 = app.post("/datasets/enums/cli/req/City", json={"id": 2, "name": "Test2"})

    store = context.get("store")
    manifest = store.manifest
    backend = manifest.backend
    model = commands.get_model(context, manifest, "datasets/enums/cli/req/City")
    city_table = backend.get_table(model)

    with backend.begin() as conn:
        conn.execute(city_table.update().values(id=10).where(city_table.c._id == res0.json()["_id"]))
        conn.execute(city_table.update().values(id=20).where(city_table.c._id == res1.json()["_id"]))
        conn.execute(city_table.update().values(name="Test3").where(city_table.c._id == res2.json()["_id"]))
        conn.execute(city_table.update().values(name="Test4").where(city_table.c._id == res3.json()["_id"]))

    result = cli.invoke(context.get("rc"), ["admin", Script.ENUM_LIST.value, "-o", tmp_path / "output.csv"])
    assert result.exit_code == 0
    assert message_in_result(result, script_check_status_message(Script.ENUM_LIST.value, ScriptStatus.REQUIRED))
    assert read_csv(tmp_path / "output.csv") == [
        ["model", "property", "invalid_value"],
        ["datasets/enums/cli/req/City", "id", "10"],
        ["", "", "20"],
        ["", "name", "Test3"],
        ["", "", "Test4"],
    ]


def test_admin_enum_list_invalid_multiple_models(
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
        datasets/enums/cli/req        |         |         |         |        |
          |   |   | Country           |         | id      |         |        |
          |   |   |   | id            | integer |         |         | open   |
          |   |   |   | name          | string  |         |         | open   |
          |   |   |   |               | enum    |         | 'Test1' | open   |
          |   |   |   |               |         |         | 'Test2' | open   |
          |   |   | City              |         |         |         |        |
          |   |   |   | id            | integer |         |         | open   |
          |   |   |   |               | enum    |         | 1       | open   |
          |   |   |   |               |         |         | 2       | open   |
          |   |   |   | name          | string  |         |         | open   |
          |   |   |   |               | enum    |         | 'Test1' | open   |
          |   |   |   |               |         |         | 'Test2' | open   |
          |   |   |   | country       | ref     | Country |         | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )
    app = create_test_client(context)
    app.authorize(["uapi:/:create"])

    res_country_0 = app.post("/datasets/enums/cli/req/Country", json={"id": 1, "name": "Test1"})
    res_country_1 = app.post("/datasets/enums/cli/req/Country", json={"id": 2, "name": "Test2"})
    res_city_0 = app.post("/datasets/enums/cli/req/City", json={"id": 1, "name": "Test1"})
    res_city_1 = app.post("/datasets/enums/cli/req/City", json={"id": 2, "name": "Test2"})

    store = context.get("store")
    manifest = store.manifest
    backend = manifest.backend
    city_model = commands.get_model(context, manifest, "datasets/enums/cli/req/City")
    country_model = commands.get_model(context, manifest, "datasets/enums/cli/req/Country")
    city_table = backend.get_table(city_model)
    country_table = backend.get_table(country_model)

    with backend.begin() as conn:
        conn.execute(city_table.update().values(id=10).where(city_table.c._id == res_city_0.json()["_id"]))
        conn.execute(city_table.update().values(name="0 C").where(city_table.c._id == res_city_1.json()["_id"]))
        conn.execute(
            country_table.update().values(name="0 C").where(country_table.c._id == res_country_0.json()["_id"])
        )
        conn.execute(
            country_table.update().values(name="1 C").where(country_table.c._id == res_country_1.json()["_id"])
        )

    result = cli.invoke(context.get("rc"), ["admin", Script.ENUM_LIST.value, "-o", tmp_path / "output.csv"])
    assert result.exit_code == 0
    assert message_in_result(result, script_check_status_message(Script.ENUM_LIST.value, ScriptStatus.REQUIRED))
    assert read_csv(tmp_path / "output.csv") == [
        ["model", "property", "invalid_value"],
        ["datasets/enums/cli/req/City", "id", "10"],
        ["", "name", "0 C"],
        ["datasets/enums/cli/req/Country", "name", "1 C"],
        ["", "", "0 C"],
    ]


def test_admin_enum_list_swap(
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
        d | r | b | m | property      | type    | ref     | prepare                | access | level
        datasets/enums/cli/req        |         |         |                        |        |
          |   |   | Country           |         | id      |                        |        |
          |   |   |   | id            | integer |         |                        | open   |
          |   |   |   | name          | string  |         |                        | open   |
          |   |   |   |               | enum    |         | 'Test1'                | open   |
          |   |   |   |               |         |         | 'Test2'                | open   |
          |   |   |   |               |         |         | swap('Test3', 'Test2') | open   |
          |   |   |   |               |         |         | swap('Test4', 'BAD')   | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )
    app = create_test_client(context)
    app.authorize(["uapi:/:create"])

    res_country_0 = app.post("/datasets/enums/cli/req/Country", json={"id": 1, "name": "Test1"})
    res_country_1 = app.post("/datasets/enums/cli/req/Country", json={"id": 2, "name": "Test2"})

    store = context.get("store")
    manifest = store.manifest
    backend = manifest.backend
    country_model = commands.get_model(context, manifest, "datasets/enums/cli/req/Country")
    country_table = backend.get_table(country_model)

    with backend.begin() as conn:
        conn.execute(
            country_table.update().values(name="Test3").where(country_table.c._id == res_country_0.json()["_id"])
        )
        conn.execute(
            country_table.update().values(name="Test4").where(country_table.c._id == res_country_1.json()["_id"])
        )

    result = cli.invoke(context.get("rc"), ["admin", Script.ENUM_LIST.value, "-o", tmp_path / "output.csv"])
    assert result.exit_code == 0
    assert message_in_result(result, script_check_status_message(Script.ENUM_LIST.value, ScriptStatus.REQUIRED))
    assert read_csv(tmp_path / "output.csv") == [
        ["model", "property", "invalid_value"],
        ["datasets/enums/cli/req/Country", "name", "Test4"],
    ]


def test_admin_enum_list_source(
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
        d | r | b | m | property      | type    | ref     | source | prepare | access | level
        datasets/enums/cli/req        |         |         |        |         |        |
          |   |   | Country           |         |         |        |         |        |
          |   |   |   | id            | integer |         |        |         | open   |
          |   |   |   | name          | string  |         |        |         | open   |
          |   |   |   |               | enum    |         | t1     | 'Test1' | open   |
          |   |   |   |               |         |         | t2     | 'Test2' | open   |
        """
        ),
    )

    context = bootstrap_manifest(
        rc, tmp_path / "manifest.csv", backend=postgresql, tmp_path=tmp_path, request=request, full_load=True
    )
    app = create_test_client(context)
    app.authorize(["uapi:/:create"])

    res_country_0 = app.post("/datasets/enums/cli/req/Country", json={"id": 1, "name": "t1"})
    res_country_1 = app.post("/datasets/enums/cli/req/Country", json={"id": 2, "name": "t2"})
    app.post("/datasets/enums/cli/req/Country", json={"id": 2, "name": "t2"})

    store = context.get("store")
    manifest = store.manifest
    backend = manifest.backend
    country_model = commands.get_model(context, manifest, "datasets/enums/cli/req/Country")
    country_table = backend.get_table(country_model)

    with backend.begin() as conn:
        conn.execute(
            country_table.update().values(name="Test1").where(country_table.c._id == res_country_0.json()["_id"])
        )
        conn.execute(
            country_table.update().values(name="asd").where(country_table.c._id == res_country_1.json()["_id"])
        )

    result = cli.invoke(context.get("rc"), ["admin", Script.ENUM_LIST.value, "-o", tmp_path / "output.csv"])
    assert result.exit_code == 0
    assert message_in_result(result, script_check_status_message(Script.ENUM_LIST.value, ScriptStatus.REQUIRED))
    assert read_csv(tmp_path / "output.csv") == [
        ["model", "property", "invalid_value"],
        ["datasets/enums/cli/req/Country", "name", "Test1"],
        ["", "", "asd"],
    ]
