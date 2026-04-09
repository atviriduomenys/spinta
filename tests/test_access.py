from pathlib import Path

from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.testing.client import create_test_client
from spinta.testing.manifest import bootstrap_manifest, prepare_manifest
from spinta.testing.utils import error
from spinta.testing.data import listdata
import pytest


@pytest.mark.manifests("internal_sql", "csv")
def test_empty_manifest(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
):
    rc = rc.fork(
        {
            "default_auth_client": "default",
        }
    )
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | access
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        full_load=True,
    )

    app = create_test_client(context)
    resp = app.get("/")
    assert error(resp) == "AuthorizedClientsOnly"


@pytest.mark.manifests("internal_sql", "csv")
def test_manifest_without_open_properties(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
):
    rc = rc.fork(
        {
            "default_auth_client": "default",
        }
    )

    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | access
    datasets/gov/vpt/new     |        |
      | resource             |        |
      |   |   | Country      |        |
      |   |   |   | name     | string |
      |   |   | City         |        |
      |   |   |   | name     | string |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        full_load=True,
    )

    app = create_test_client(context)
    resp = app.get("/")
    assert error(resp) == "AuthorizedClientsOnly"


@pytest.mark.manifests("internal_sql", "csv")
def test_manifest_with_open_properties(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
):
    rc = rc.fork(
        {
            "default_auth_client": "default",
        }
    )

    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | access
    datasets/gov/vpt/new     |        |
      | resource             |        |
      |   |   | Country      |        |
      |   |   |   | name     | string |
      |   |   | City         |        |
      |   |   |   | name     | string | open
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        full_load=True,
    )

    app = create_test_client(context)
    resp = app.get("/")
    assert resp.json() == {
        "_data": [
            {
                "name": "datasets/:ns",
                "title": "",
                "description": "",
            },
        ]
    }


def test_config_access_visible_properties(rc: RawConfig, tmp_path: Path):
    rc = rc.fork({"access": "public"})
    xml = """
        <cities>
            <city>
                <country>lt</country>
                <name>Vilnius</name>
                <area>10</area>
                <gdp>20</gdp>
            </city>
        </cities>
    """
    path = tmp_path / "data.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property | type     | ref  | source                | access
        example                  |          |      |                       |
          | xml                  | dask/xml |      | {path}                |
          |   |   | City         |          | name | /cities/city          |
          |   |   |   | country  | string   |      | country               | open
          |   |   |   | name     | string   |      | name                  | public
          |   |   |   | area     | integer  |      | area                  | protected
          |   |   |   | gdp      | integer  |      | gdp                   | private
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authorize(["uapi:/example/City/:getall"])

    resp = app.get("/example/City")
    returned_data = listdata(resp, full=True)[0]
    assert "country" in returned_data
    assert "name" in returned_data
    assert "area" not in returned_data
    assert "gdp" not in returned_data


@pytest.mark.parametrize(
    "access, status_code",
    [
        ("open", 200),
        ("public", 404),
        ("protected", 404),
        ("private", 404),
    ],
)
def test_config_access_models(rc: RawConfig, tmp_path: Path, access: str, status_code: int):
    rc = rc.fork({"access": "open"})
    xml = """
        <cities>
            <city>
                <name>Vilnius</name>
            </city>
        </cities>
    """
    path = tmp_path / "data.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property | type     | ref  | source                | access
        example                  |          |      |                       |
          | xml                  | dask/xml |      | {path}                |
          |   |   | City         |          | name | /cities/city          | {access}
          |   |   |   | name     | string   |      | name                  | 
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authorize(["uapi:/example/City/:getall"])

    assert app.get("/example/City").status_code == status_code
