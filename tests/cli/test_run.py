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
