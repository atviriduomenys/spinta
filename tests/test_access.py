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
            "default_access_level": "protected",
            "access": "protected",
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
            "default_access_level": "protected",
            "access": "protected",
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


@pytest.mark.parametrize(
    "config_access, node_access, status_code",
    [
        ("open", "open", 200),
        ("open", "public", 404),
        ("open", "protected", 404),
        ("open", "private", 404),
        ("public", "open", 401),
        ("public", "public", 401),
        ("public", "protected", 404),
        ("public", "private", 404),
        ("protected", "open", 401),
        ("protected", "public", 401),
        ("protected", "protected", 401),
        ("protected", "private", 404),
        ("private", "open", 401),
        ("private", "public", 401),
        ("private", "protected", 401),
        ("private", "private", 401),
    ],
)
def test_config_access_unauthenticated(
    rc: RawConfig, tmp_path: Path, config_access: str, node_access: str, status_code: int
):
    rc = rc.fork(
        {
            "access": config_access,
            "default_auth_client": "default",
        }
    )
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
          |   |   | City         |          | name | /cities/city          | {node_access}
          |   |   |   | name     | string   |      | name                  | 
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)

    assert app.get("/example/City").status_code == status_code


@pytest.mark.parametrize(
    "config_access, node_access, has_scopes, status_code",
    [
        ("open", "open", False, 403),
        ("open", "public", False, 404),
        ("open", "protected", False, 404),
        ("open", "private", False, 404),
        ("public", "open", False, 403),
        ("public", "public", False, 403),
        ("public", "protected", False, 404),
        ("public", "private", False, 404),
        ("protected", "open", False, 403),
        ("protected", "public", False, 403),
        ("protected", "protected", False, 403),
        ("protected", "private", False, 404),
        ("private", "open", False, 403),
        ("private", "public", False, 403),
        ("private", "protected", False, 403),
        ("private", "private", False, 403),
        ("open", "open", True, 200),
        ("open", "public", True, 404),
        ("open", "protected", True, 404),
        ("open", "private", True, 404),
        ("public", "open", True, 200),
        ("public", "public", True, 200),
        ("public", "protected", True, 404),
        ("public", "private", True, 404),
        ("protected", "open", True, 200),
        ("protected", "public", True, 200),
        ("protected", "protected", True, 200),
        ("protected", "private", True, 404),
        ("private", "open", True, 200),
        ("private", "public", True, 200),
        ("private", "protected", True, 200),
        ("private", "private", True, 200),
    ],
)
def test_config_access_authenticated(
    rc: RawConfig, tmp_path: Path, config_access: str, node_access: str, has_scopes: bool, status_code: int
):
    rc = rc.fork({"access": config_access})
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
          |   |   | City         |          | name | /cities/city          | {node_access}
          |   |   |   | name     | string   |      | name                  | 
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    scopes = ["uapi:/example/City/:getall"] if has_scopes else [""]
    app.authorize(scopes)

    assert app.get("/example/City").status_code == status_code
