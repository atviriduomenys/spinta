import sqlalchemy as sa
from fsspec.implementations.memory import MemoryFileSystem
from pathlib import Path

import pytest

from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.client import create_client, create_test_client
from spinta.testing.data import listdata
from spinta.testing.datasets import Sqlite
from spinta.testing.manifest import prepare_manifest
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.utils import error


@pytest.fixture
def fs():
    fs = MemoryFileSystem()
    yield fs
    MemoryFileSystem.store.clear()


def test_undeclared_ref_run_does_not_fail_csv(rc: RawConfig, fs: MemoryFileSystem):
    """spinta run should not fail (manifest loads, app starts, data is served)
    when a ref points to a model not declared in the manifest."""
    fs.pipe("cities.csv", b"name,country_id\nVilnius,lt\nRiga,lv\n")

    context, manifest = prepare_manifest(
        rc,
        """
        d | r | b | m | property | type     | ref              | source                 | access
        example                  |          |                  |                        |
          | csv                  | dask/csv |                  | memory://cities.csv    |
          |   |   | City         |          |                  |                        |
          |   |   |   | name     | string   |                  | name                   | open
          |   |   |   | country  | ref      | example2/Country | country_id             | open
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/City", ["getall"])

    resp = app.get("/example/City")
    assert resp.status_code == 200
    assert listdata(resp, "name") == ["Riga", "Vilnius"]


def test_undeclared_ref_run_does_not_fail_sqlite(context: Context, rc: RawConfig, tmp_path: Path, sqlite: Sqlite):
    """spinta run should not fail with a SQL data source when a ref points
    to a model not declared in the manifest."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref              | source     | access
    example                  |        |                  |            |
      | resource             | sql    |                  |            |
      |   |   | City         |        |                  | CITY       |
      |   |   |   | name     | string |                  | NAME       | open
      |   |   |   | country  | ref    | example2/Country | COUNTRY_ID | open
    """),
    )

    sqlite.init(
        {
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY_ID", sa.Text),
            ],
        }
    )
    sqlite.write(
        "CITY",
        [
            {"NAME": "Vilnius", "COUNTRY_ID": "lt"},
            {"NAME": "Riga", "COUNTRY_ID": "lv"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel("example/City", ["getall"])

    resp = app.get("/example/City")
    assert resp.status_code == 200
    assert listdata(resp, "name") == ["Riga", "Vilnius"]


def test_undeclared_ref_run_does_not_fail_xml(rc: RawConfig, tmp_path: Path):
    """spinta run should not fail with an XML data source when a ref points
    to a model not declared in the manifest."""
    path = tmp_path / "cities.xml"
    path.write_text("""
        <cities>
            <city>
                <name>Vilnius</name>
                <country_id>lt</country_id>
            </city>
            <city>
                <name>Riga</name>
                <country_id>lv</country_id>
            </city>
        </cities>
    """)

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property | type     | ref              | source       | access
        example                  |          |                  |              |
          | xml                  | dask/xml |                  | {path}       |
          |   |   | City         |          |                  | /cities/city |
          |   |   |   | name     | string   |                  | name         | open
          |   |   |   | country  | ref      | example2/Country | country_id   | open
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/City", ["getall"])

    resp = app.get("/example/City")
    assert resp.status_code == 200
    assert listdata(resp, "name") == ["Riga", "Vilnius"]


def test_undeclared_base_run_does_not_fail_csv(rc: RawConfig, fs: MemoryFileSystem):
    """spinta run should not fail (manifest loads, app starts, data is served)
    when a model declares a base not present in the manifest."""
    fs.pipe("cities.csv", b"name\nVilnius\nRiga\n")
    context, manifest = prepare_manifest(
        rc,
        """
        d | r | b                              | m    | property | type     | ref | source              | access
        example                                |      |          |          |     |                     |
          | csv                                |      |          | dask/csv |     | memory://cities.csv |
          |   | dataset/gov/vssa/is/ds/Address |      |          |          |     |                     |
          |   |                                | City |          |          |     |                     |
          |   |                                |      | name     | string   |     | name                | open
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/City", ["getall"])
    resp = app.get("/example/City")
    assert resp.status_code == 200
    assert listdata(resp, "name") == ["Riga", "Vilnius"]


def test_undeclared_base_run_does_not_fail_sqlite(context: Context, rc: RawConfig, tmp_path: Path, sqlite: Sqlite):
    """spinta run should not fail with a SQL data source when a model declares
    a base not present in the manifest."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b                              | m    | property | type   | ref | source | access
    example                                |      |          |        |     |        |
      | resource                           |      |          | sql    |     |        |
      |   | dataset/gov/vssa/is/ds/Address |      |          |        |     |        |
      |   |                                | City |          |        |     | CITY   |
      |   |                                |      | name     | string |     | NAME   | open
    """),
    )
    sqlite.init(
        {
            "CITY": [
                sa.Column("NAME", sa.Text),
            ],
        }
    )
    sqlite.write(
        "CITY",
        [
            {"NAME": "Vilnius"},
            {"NAME": "Riga"},
        ],
    )
    app = create_client(rc, tmp_path, sqlite)
    app.authmodel("example/City", ["getall"])
    resp = app.get("/example/City")
    assert resp.status_code == 200
    assert listdata(resp, "name") == ["Riga", "Vilnius"]


def test_undeclared_base_run_does_not_fail_xml(rc: RawConfig, tmp_path: Path):
    """spinta run should not fail with an XML data source when a model declares
    a base not present in the manifest."""
    path = tmp_path / "cities.xml"
    path.write_text("""
        <cities>
            <city>
                <name>Vilnius</name>
            </city>
            <city>
                <name>Riga</name>
            </city>
        </cities>
    """)
    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b                              | m    | property | type     | ref | source       | access
        example                                |      |          |          |     |              |
          | xml                                |      |          | dask/xml |     | {path}       |
          |   | dataset/gov/vssa/is/ds/Address |      |          |          |     |              |
          |   |                                | City |          |          |     | /cities/city |
          |   |                                |      | name     | string   |     | name         | open
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/City", ["getall"])
    resp = app.get("/example/City")
    assert resp.status_code == 200
    assert listdata(resp, "name") == ["Riga", "Vilnius"]


def test_undeclared_base_with_ref_and_level_comment_prepare(rc: RawConfig):
    from spinta import commands as spinta_commands

    context, manifest = prepare_manifest(
        rc,
        """
        d | r | b                              | m       | property | type   | ref | level | access
        example                                |         |          |        |     |       |
                                               |         |          |        |     |       |
          |   | dataset/gov/vssa/is/ds/Address |         |          |        | id  | 4     |
          |   |                                | Country |          |        |     |       |
          |   |                                |         | name     | string |     |       | open
        """,
        mode=Mode.internal,
    )
    model = spinta_commands.get_models(context, manifest)["example/Country"]
    assert model.base is None, "undeclared base should be dropped"
    assert model.comments, "restore comment should be added"
    comment = model.comments[0]
    assert comment.prepare == 'insert(base:"dataset/gov/vssa/is/ds/Address", ref:"id", level:4)'


def test_custom_scope_caps_user_filter_sqlite(context: Context, rc: RawConfig, tmp_path: Path, sqlite: Sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property      | type   | ref | source       | prepare           | access
    example                       |        |     |              |                   |
      | resource                  | sql    |     |              |                   |
      |   |   | City              |        |     | CITY         |                   |
                                  | scope  | ltu |              | country_code='lt' |
      |   |   |   | name          | string |     | NAME         |                   | open
      |   |   |   | country_code  | string |     | COUNTRY_CODE |                   | open
    """),
    )

    sqlite.init(
        {
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY_CODE", sa.Text),
            ],
        }
    )
    sqlite.write(
        "CITY",
        [
            {"NAME": "Vilnius", "COUNTRY_CODE": "lt"},
            {"NAME": "Riga", "COUNTRY_CODE": "lv"},
            {"NAME": "Tallinn", "COUNTRY_CODE": "ee"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel("example/City", ["getall", "search"])

    # Scope alone: only the Lithuanian city is returned
    resp = app.get("/example/City/@ltu")
    assert resp.status_code == 200
    assert listdata(resp, "name") == ["Vilnius"]

    # Contradicting user filter: scope requires lt, user asks for lv → empty
    resp = app.get('/example/City/@ltu?country_code="lv"')
    assert resp.status_code == 200
    assert listdata(resp, "name") == []

    # Compatible user filter narrows within the scope
    resp = app.get('/example/City/@ltu?name="Vilnius"')
    assert resp.status_code == 200
    assert listdata(resp, "name") == ["Vilnius"]


def test_no_scope_returns_all_data(context: Context, rc: RawConfig, tmp_path: Path, sqlite: Sqlite):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property      | type   | ref | source       | prepare           | access
    example                       |        |     |              |                   |
      | resource                  | sql    |     |              |                   |
      |   |   | City              |        |     | CITY         |                   |
                                  | scope  | ltu |              | country_code='lt' |
      |   |   |   | name          | string |     | NAME         |                   | open
      |   |   |   | country_code  | string |     | COUNTRY_CODE |                   | open
    """),
    )

    sqlite.init(
        {
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY_CODE", sa.Text),
            ],
        }
    )
    sqlite.write(
        "CITY",
        [
            {"NAME": "Vilnius", "COUNTRY_CODE": "lt"},
            {"NAME": "Riga", "COUNTRY_CODE": "lv"},
            {"NAME": "Tallinn", "COUNTRY_CODE": "ee"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel("example/City", ["getall"])

    resp = app.get("/example/City")
    assert resp.status_code == 200
    assert sorted(listdata(resp, "name")) == ["Riga", "Tallinn", "Vilnius"]


def test_scope_gt_filter_sqlite(context: Context, rc: RawConfig, tmp_path: Path, sqlite: Sqlite):
    """Scope prepare with gt (>) restricts rows; user filter cannot reach outside the scope."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property  | type    | ref | source   | prepare         | access
    example                   |         |     |          |                 |
      | resource              | sql     |     |          |                 |
      |   |   | Country       |         |     | COUNTRY  |                 |
                              | scope   | big |          | area_km2>200000 |
      |   |   |   | name      | string  |     | NAME     |                 | open
      |   |   |   | area_km2  | integer |     | AREA_KM2 |                 | open
    """),
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
                sa.Column("AREA_KM2", sa.Integer),
            ],
        }
    )
    sqlite.write(
        "COUNTRY",
        [
            {"NAME": "Lithuania", "AREA_KM2": 65300},
            {"NAME": "Germany", "AREA_KM2": 357000},
            {"NAME": "India", "AREA_KM2": 3287000},
            {"NAME": "Rwanda", "AREA_KM2": 26338},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel("example/Country", ["getall", "search"])

    # Scope allows only area_km2 > 200000 — Lithuania and Rwanda excluded
    resp = app.get("/example/Country/@big")
    assert resp.status_code == 200
    assert sorted(listdata(resp, "name")) == ["Germany", "India"]

    # User tries to get small countries (area < 100000) — scope blocks them, result empty
    resp = app.get('/example/Country/@big?area_km2<"100000"')
    assert resp.status_code == 200
    assert listdata(resp, "name") == []


def test_scope_select_dotted_path_sqlite(context: Context, rc: RawConfig, tmp_path: Path, sqlite: Sqlite):
    """Scope select(name,country.name) blocks country.code even when user explicitly requests it."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref     | source     | prepare                   | access
    example                  |        |         |            |                           |
      | resource             | sql    |         |            |                           |
      |   |   | Country      |        | name    | COUNTRY    |                           |
      |   |   |   | name     | string |         | NAME       |                           | open
      |   |   |   | code     | string |         | CODE       |                           | open
      |   |   | City         |        | name    | CITY       |                           |
                             | scope  | public  |            | select(name,country.name) |
      |   |   |   | name     | string |         | NAME       |                           | open
      |   |   |   | country  | ref    | Country | COUNTRY_ID |                           | open
    """),
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("CODE", sa.Text),
                sa.Column("NAME", sa.Text),
            ],
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY_ID", sa.Text),
            ],
        }
    )
    sqlite.write("COUNTRY", [{"CODE": "lt", "NAME": "Lithuania"}])
    sqlite.write(
        "CITY",
        [
            {"NAME": "Vilnius", "COUNTRY_ID": "lt"},
            {"NAME": "Kaunas", "COUNTRY_ID": "lt"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel("example/City", ["getall", "search"])

    # User requests country.code which is outside scope — raises error
    resp = app.get("/example/City/@public?select(name,country.name,country.code)")
    assert resp.status_code == 404


def test_scope_caps_user_filter_csv(rc: RawConfig, fs: MemoryFileSystem):
    """Scope on CSV source; user filter that contradicts the scope returns empty rows."""
    fs.pipe(
        "cities.csv",
        b"name,country_code\nVilnius,lt\nRiga,lv\nTallinn,ee\n",
    )

    context, manifest = prepare_manifest(
        rc,
        """
        d | r | b | m | property      | type     | ref | source              | prepare           | access
        example                       |          |     |                     |                   |
          | csv                       | dask/csv |     | memory://cities.csv |                   |
          |   |   | City              |          |     |                     |                   |
                                      | scope    | ltu |                     | country_code='lt' |
          |   |   |   | name          | string   |     | name                |                   | open
          |   |   |   | country_code  | string   |     | country_code        |                   | open
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/City", ["getall", "search"])

    # Scope alone: only the Lithuanian city is returned
    resp = app.get("/example/City/@ltu")
    assert resp.status_code == 200
    assert listdata(resp, "name") == ["Vilnius"]

    # Contradicting user filter: scope requires lt, user asks for lv → empty
    resp = app.get('/example/City/@ltu?country_code="lv"')
    assert resp.status_code == 200
    assert listdata(resp, "name") == []

    # Compatible user filter still narrows within the scope
    resp = app.get('/example/City/@ltu?name="Vilnius"')
    assert resp.status_code == 200
    assert listdata(resp, "name") == ["Vilnius"]


def test_scope_caps_user_filter_xml(rc: RawConfig, tmp_path: Path):
    """Scope on XML source; user filter that contradicts the scope returns empty rows."""
    path = tmp_path / "cities.xml"
    path.write_text("""
        <cities>
            <city>
                <name>Vilnius</name>
                <country_code>lt</country_code>
            </city>
            <city>
                <name>Riga</name>
                <country_code>lv</country_code>
            </city>
            <city>
                <name>Tallinn</name>
                <country_code>ee</country_code>
            </city>
        </cities>
    """)

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property      | type     | ref | source       | prepare           | access
        example                       |          |     |              |                   |
          | xml                       | dask/xml |     | {path}       |                   |
          |   |   | City              |          |     | /cities/city |                   |
                                      | scope    | ltu |              | country_code='lt' |
          |   |   |   | name          | string   |     | name         |                   | open
          |   |   |   | country_code  | string   |     | country_code |                   | open
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/City", ["getall", "search"])

    # Scope alone: only the Lithuanian city is returned
    resp = app.get("/example/City/@ltu")
    assert resp.status_code == 200
    assert listdata(resp, "name") == ["Vilnius"]

    # Contradicting user filter: scope requires lt, user asks for ee → empty
    resp = app.get('/example/City/@ltu?country_code="ee"')
    assert resp.status_code == 200
    assert listdata(resp, "name") == []

    # Compatible user filter still narrows within the scope
    resp = app.get('/example/City/@ltu?name="Vilnius"')
    assert resp.status_code == 200
    assert listdata(resp, "name") == ["Vilnius"]


def test_scope_and_conditions_caps_filter_sqlite(context: Context, rc: RawConfig, tmp_path: Path, sqlite: Sqlite):
    """Scope with two AND conditions; contradicting either condition returns empty rows."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property      | type   | ref | source       | prepare                           | access
    example                       |        |     |              |                                   |
      | resource                  | sql    |     |              |                                   |
      |   |   | City              |        |     | CITY         |                                   |
                                  | scope  | ltu |              | country_code='lt'&status='active' |
      |   |   |   | name          | string |     | NAME         |                                   | open
      |   |   |   | country_code  | string |     | COUNTRY_CODE |                                   | open
      |   |   |   | status        | string |     | STATUS       |                                   | open
    """),
    )

    sqlite.init(
        {
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY_CODE", sa.Text),
                sa.Column("STATUS", sa.Text),
            ],
        }
    )
    sqlite.write(
        "CITY",
        [
            {"NAME": "Vilnius", "COUNTRY_CODE": "lt", "STATUS": "active"},
            {"NAME": "Kaunas", "COUNTRY_CODE": "lt", "STATUS": "inactive"},
            {"NAME": "Riga", "COUNTRY_CODE": "lv", "STATUS": "active"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel("example/City", ["getall", "search"])

    # Scope alone: only Vilnius (lt + active) passes both conditions
    resp = app.get("/example/City/@ltu")
    assert resp.status_code == 200
    assert listdata(resp, "name") == ["Vilnius"]

    # Contradicts country_code condition: scope requires lt, user asks for lv
    resp = app.get('/example/City/@ltu?country_code="lv"')
    assert resp.status_code == 200
    assert listdata(resp, "name") == []

    # Contradicts status condition: scope requires active, user asks for inactive
    resp = app.get('/example/City/@ltu?status="inactive"')
    assert resp.status_code == 200
    assert listdata(resp, "name") == []

    # Compatible user filter still narrows within both scope conditions
    resp = app.get('/example/City/@ltu?name="Vilnius"')
    assert resp.status_code == 200
    assert listdata(resp, "name") == ["Vilnius"]


def test_scope_ne_caps_filter_sqlite(context: Context, rc: RawConfig, tmp_path: Path, sqlite: Sqlite):
    """Scope using != (ne) operator; user filter requesting the excluded value returns empty rows."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property  | type   | ref | source   | prepare            | access
    example                   |        |     |          |                    |
      | resource              | sql    |     |          |                    |
      |   |   | City          |        |     | CITY     |                    |
                              | scope  | pub |          | status!='inactive' |
      |   |   |   | name      | string |     | NAME     |                    | open
      |   |   |   | status    | string |     | STATUS   |                    | open
    """),
    )

    sqlite.init(
        {
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("STATUS", sa.Text),
            ],
        }
    )
    sqlite.write(
        "CITY",
        [
            {"NAME": "Vilnius", "STATUS": "active"},
            {"NAME": "Kaunas", "STATUS": "inactive"},
            {"NAME": "Riga", "STATUS": "active"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel("example/City", ["getall", "search"])

    # Scope alone: excludes Kaunas (inactive)
    resp = app.get("/example/City/@pub")
    assert resp.status_code == 200
    assert sorted(listdata(resp, "name")) == ["Riga", "Vilnius"]

    # User requests the excluded value: scope says !=inactive, user says =inactive → empty
    resp = app.get('/example/City/@pub?status="inactive"')
    assert resp.status_code == 200
    assert listdata(resp, "name") == []

    # User narrows within scope (requests active only)
    resp = app.get('/example/City/@pub?status="active"')
    assert resp.status_code == 200
    assert sorted(listdata(resp, "name")) == ["Riga", "Vilnius"]


def test_multiple_scopes_caps_cross_scope_filter_sqlite(
    context: Context, rc: RawConfig, tmp_path: Path, sqlite: Sqlite
):
    """Two named scopes on the same model; using one scope while filtering for data
    exclusive to the other scope returns empty rows."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property      | type   | ref | source       | prepare           | access
    example                       |        |     |              |                   |
      | resource                  | sql    |     |              |                   |
      |   |   | City              |        |     | CITY         |                   |
                                  | scope  | ltu |              | country_code='lt' |
                                  | scope  | est |              | country_code='ee' |
      |   |   |   | name          | string |     | NAME         |                   | open
      |   |   |   | country_code  | string |     | COUNTRY_CODE |                   | open
    """),
    )

    sqlite.init(
        {
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY_CODE", sa.Text),
            ],
        }
    )
    sqlite.write(
        "CITY",
        [
            {"NAME": "Vilnius", "COUNTRY_CODE": "lt"},
            {"NAME": "Riga", "COUNTRY_CODE": "lv"},
            {"NAME": "Tallinn", "COUNTRY_CODE": "ee"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel("example/City", ["getall", "search"])

    # Each scope returns its own country's cities
    resp = app.get("/example/City/@ltu")
    assert resp.status_code == 200
    assert listdata(resp, "name") == ["Vilnius"]

    resp = app.get("/example/City/@est")
    assert resp.status_code == 200
    assert listdata(resp, "name") == ["Tallinn"]

    # Using ltu scope but filtering for ee data: scope requires lt, user asks ee → empty
    resp = app.get('/example/City/@ltu?country_code="ee"')
    assert resp.status_code == 200
    assert listdata(resp, "name") == []

    # Using est scope but filtering for lt data: scope requires ee, user asks lt → empty
    resp = app.get('/example/City/@est?country_code="lt"')
    assert resp.status_code == 200
    assert listdata(resp, "name") == []

    # Using either scope but filtering for lv (in neither scope) → empty
    resp = app.get('/example/City/@ltu?country_code="lv"')
    assert resp.status_code == 200
    assert listdata(resp, "name") == []


def test_scope_range_caps_filter_sqlite(context: Context, rc: RawConfig, tmp_path: Path, sqlite: Sqlite):
    """Scope with a numeric range (>= AND <= on same field); user filter outside the range returns empty rows."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property  | type    | ref | source   | prepare                           | access
    example                   |         |     |          |                                   |
      | resource              | sql     |     |          |                                   |
      |   |   | Country       |         |     | COUNTRY  |                                   |
                              | scope   | mid |          | area_km2>=50000&area_km2<=400000  |
      |   |   |   | name      | string  |     | NAME     |                                   | open
      |   |   |   | area_km2  | integer |     | AREA_KM2 |                                   | open
    """),
    )

    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
                sa.Column("AREA_KM2", sa.Integer),
            ],
        }
    )
    sqlite.write(
        "COUNTRY",
        [
            {"NAME": "Rwanda", "AREA_KM2": 26338},
            {"NAME": "Lithuania", "AREA_KM2": 65300},
            {"NAME": "Germany", "AREA_KM2": 357000},
            {"NAME": "India", "AREA_KM2": 3287000},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel("example/Country", ["getall", "search"])

    # Scope alone: only medium-sized countries pass both bounds
    resp = app.get("/example/Country/@mid")
    assert resp.status_code == 200
    assert sorted(listdata(resp, "name")) == ["Germany", "Lithuania"]

    # User filters below the lower bound → contradicts scope >= 50000 → empty
    resp = app.get('/example/Country/@mid?area_km2<"30000"')
    assert resp.status_code == 200
    assert listdata(resp, "name") == []

    # User filters above the upper bound → contradicts scope <= 400000 → empty
    resp = app.get('/example/Country/@mid?area_km2>"1000000"')
    assert resp.status_code == 200
    assert listdata(resp, "name") == []

    # User narrows within the range → compatible, returns matching row
    resp = app.get('/example/Country/@mid?name="Germany"')
    assert resp.status_code == 200
    assert listdata(resp, "name") == ["Germany"]


def test_scope_select_enforces_fields_without_user_select(
    context: Context, rc: RawConfig, tmp_path: Path, sqlite: Sqlite
):
    """Scope with select(name) enforces the field list when the user provides no select at all."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property      | type   | ref | source       | prepare       | access
    example                       |        |     |              |               |
      | resource                  | sql    |     |              |               |
      |   |   | City              |        |     | CITY         |               |
                                  | scope  | pub |              | select(name)  |
      |   |   |   | name          | string |     | NAME         |               | open
      |   |   |   | country_code  | string |     | COUNTRY_CODE |               | open
    """),
    )

    sqlite.init(
        {
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY_CODE", sa.Text),
            ],
        }
    )
    sqlite.write(
        "CITY",
        [
            {"NAME": "Vilnius", "COUNTRY_CODE": "lt"},
            {"NAME": "Riga", "COUNTRY_CODE": "lv"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel("example/City", ["getall", "search"])

    # No user select — scope enforces its own field list
    resp = app.get("/example/City/@pub")
    assert resp.status_code == 200
    data = resp.json()["_data"]
    assert sorted(row["name"] for row in data) == ["Riga", "Vilnius"]
    assert all("country_code" not in row for row in data)


def test_scope_select_fallback_when_no_overlap(context: Context, rc: RawConfig, tmp_path: Path, sqlite: Sqlite):
    """When user's select has no fields in common with the scope's select,
    the request is rejected with a 404 PropertyNotFound — from the user's
    perspective those fields simply do not exist."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property      | type   | ref | source       | prepare       | access
    example                       |        |     |              |               |
      | resource                  | sql    |     |              |               |
      |   |   | City              |        |     | CITY         |               |
                                  | scope  | pub |              | select(name)  |
      |   |   |   | name          | string |     | NAME         |               | open
      |   |   |   | country_code  | string |     | COUNTRY_CODE |               | open
    """),
    )

    sqlite.init(
        {
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY_CODE", sa.Text),
            ],
        }
    )
    sqlite.write(
        "CITY",
        [
            {"NAME": "Vilnius", "COUNTRY_CODE": "lt"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel("example/City", ["getall", "search"])

    # User asks for a field entirely outside the scope — looks like the property doesn't exist.
    resp = app.get("/example/City/@pub?select(country_code)")
    assert resp.status_code == 404
    assert error(resp, status=404) == "PropertyNotFound"


def test_scope_select_blocks_sort_by_hidden_field(context: Context, rc: RawConfig, tmp_path: Path, sqlite: Sqlite):
    """A select scope must reject sort expressions that reference fields outside
    the allowed set — otherwise field values could be inferred by ordering."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property      | type   | ref | source       | prepare       | access
    example                       |        |     |              |               |
      | resource                  | sql    |     |              |               |
      |   |   | City              |        |     | CITY         |               |
                                  | scope  | pub |              | select(name)  |
      |   |   |   | name          | string |     | NAME         |               | open
      |   |   |   | country_code  | string |     | COUNTRY_CODE |               | open
    """),
    )
    sqlite.init({"CITY": [sa.Column("NAME", sa.Text), sa.Column("COUNTRY_CODE", sa.Text)]})
    sqlite.write("CITY", [{"NAME": "Vilnius", "COUNTRY_CODE": "lt"}])
    app = create_client(rc, tmp_path, sqlite)
    app.authmodel("example/City", ["getall", "search"])

    # Sorting by an allowed field is fine
    resp = app.get("/example/City/@pub?sort(name)")
    assert resp.status_code == 200

    # Sorting by a hidden field must be rejected
    resp = app.get("/example/City/@pub?sort(country_code)")
    assert resp.status_code == 404
    assert error(resp, status=404) == "PropertiesNotFound"


def test_scope_select_blocks_filter_by_hidden_field(context: Context, rc: RawConfig, tmp_path: Path, sqlite: Sqlite):
    """A select scope must reject filter expressions that reference fields outside
    the allowed set — otherwise field values could be inferred by binary search."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property      | type   | ref | source       | prepare       | access
    example                       |        |     |              |               |
      | resource                  | sql    |     |              |               |
      |   |   | City              |        |     | CITY         |               |
                                  | scope  | pub |              | select(name)  |
      |   |   |   | name          | string |     | NAME         |               | open
      |   |   |   | country_code  | string |     | COUNTRY_CODE |               | open
    """),
    )
    sqlite.init({"CITY": [sa.Column("NAME", sa.Text), sa.Column("COUNTRY_CODE", sa.Text)]})
    sqlite.write(
        "CITY",
        [
            {"NAME": "Vilnius", "COUNTRY_CODE": "lt"},
            {"NAME": "Berlin", "COUNTRY_CODE": "de"},
        ],
    )
    app = create_client(rc, tmp_path, sqlite)
    app.authmodel("example/City", ["getall", "search"])

    # Filtering by an allowed field is fine
    resp = app.get("/example/City/@pub?name='Vilnius'")
    assert resp.status_code == 200

    # Filtering by a hidden field must be rejected
    resp = app.get("/example/City/@pub?country_code='lt'")
    assert resp.status_code == 404
    assert error(resp, status=404) == "PropertiesNotFound"


def test_scope_or_filter_blocks_outside_countries_sqlite(
    context: Context, rc: RawConfig, tmp_path: Path, sqlite: Sqlite
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property      | type   | ref | source       | prepare                                         | access
    example                       |        |     |              |                                                 |
      | resource                  | sql    |     |              |                                                 |
      |   |   | City              |        |     | CITY         |                                                 |
                                  | scope  | baltic |           | or(country_code='lt',country_code='lv')         |
      |   |   |   | name          | string |     | NAME         |                                                 | open
      |   |   |   | country_code  | string |     | COUNTRY_CODE |                                                 | open
    """),
    )

    sqlite.init(
        {
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY_CODE", sa.Text),
            ],
        }
    )
    sqlite.write(
        "CITY",
        [
            {"NAME": "Vilnius", "COUNTRY_CODE": "lt"},
            {"NAME": "Riga", "COUNTRY_CODE": "lv"},
            {"NAME": "Berlin", "COUNTRY_CODE": "de"},
        ],
    )

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel("example/City", ["getall", "search"])

    # Scope alone: Baltic cities only
    resp = app.get("/example/City/@baltic")
    assert resp.status_code == 200
    assert sorted(listdata(resp, "name")) == ["Riga", "Vilnius"]

    # User requests a non-Baltic country directly — scope blocks it
    resp = app.get('/example/City/@baltic?country_code="de"')
    assert resp.status_code == 200
    assert listdata(resp, "name") == []

    # User constructs an OR query mixing an allowed and a disallowed country —
    # scope AND-combines with the user OR, so only the allowed branch survives
    resp = app.get('/example/City/@baltic?or(country_code="lt",country_code="de")')
    assert resp.status_code == 200
    assert listdata(resp, "name") == ["Vilnius"]
