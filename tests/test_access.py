from pathlib import Path

from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.utils import error
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
