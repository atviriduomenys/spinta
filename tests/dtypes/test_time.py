from pathlib import Path

from pytest import FixtureRequest

from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest
import pytest


@pytest.mark.manifests("internal_sql", "csv")
def test_time(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property          | type   | ref
    backends/postgres/dtypes/time     |        |
      |   |   | City                  |        |
      |   |   |   | name              | string |
      |   |   |   | time              | time   |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authmodel(
        "backends/postgres/dtypes/time/City",
        [
            "insert",
            "getall",
        ],
    )

    # Write data
    resp = app.post(
        "/backends/postgres/dtypes/time/City",
        json={
            "name": "Vilnius",
            "time": "00:00:01",
        },
    )
    assert resp.status_code == 201

    # Read data
    resp = app.get("/backends/postgres/dtypes/time/City")
    assert listdata(resp, full=True) == [
        {
            "name": "Vilnius",
            "time": "00:00:01",
        }
    ]
