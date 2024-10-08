import pytest
from pathlib import Path
import uuid
from pytest import FixtureRequest
from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest


@pytest.mark.manifests('internal_sql', 'csv')
def test_uuid(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property      | type
    backends/postgres/dtypes/uuid |
      |   |   | Entity            |
      |   |   |   | id            | uuid
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )

    app = create_test_client(context)
    app.authmodel('backends/postgres/dtypes/uuid/Entity', [
        'insert',
        'getall',
        'search'
    ])

    # post uuid in string format
    entity_id = str(uuid.uuid4())
    resp = app.post('/backends/postgres/dtypes/uuid/Entity', json={
        'id': entity_id
    })
    assert resp.status_code == 201

    # Read data when uuid is written in hex format
    resp = app.get('/backends/postgres/dtypes/uuid/Entity?select(id)')
    assert listdata(resp, full=True) == [{'id': entity_id}]


    invalid_uuid = "invalid-uuid"
    resp = app.post('/backends/postgres/dtypes/uuid/Entity', json={
        'id': invalid_uuid
    })
    assert resp.status_code == 400


