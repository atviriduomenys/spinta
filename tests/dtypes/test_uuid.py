from pathlib import Path
import sqlalchemy as sa
import uuid

from pytest import FixtureRequest
import pytest

from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client, create_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.datasets import create_sqlite_db
from spinta.testing.datasets import Sqlite
from spinta.testing.tabular import create_tabular_manifest
from spinta.manifests.tabular.helpers import striptable
from spinta.utils.types import is_str_uuid


@pytest.fixture(scope='module')
def uuid_db():
    with create_sqlite_db({
        'test_uuid': [
            sa.Column('id', sa.String, primary_key=True, default=int),
            sa.Column('guid', sa.String, default=lambda: str(uuid.uuid4())),
        ],
    }) as db:
        db.write('test_uuid', [
                    {'id': 1, 'guid': str(uuid.uuid4())},
                    {'id': 2, 'guid': str(uuid.uuid4())},
                    {'id': 3, 'guid': str(uuid.uuid4())},
                ])
        yield db

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

    # Test insert
    entity_id = str(uuid.uuid4())
    resp = app.post('/backends/postgres/dtypes/uuid/Entity', json={
        'id': entity_id
    })
    assert resp.status_code == 201

    # Read data when uuid is written in hex format
    resp = app.get('/backends/postgres/dtypes/uuid/Entity?select(id)')
    assert listdata(resp, full=True) == [{'id': entity_id}]

    # Test filters ('eq', 'contains', 'sort')
    test_cases = [
        f'/backends/postgres/dtypes/uuid/Entity?id="{entity_id}"',
        f'/backends/postgres/dtypes/uuid/Entity?id.contains("-")',
        f'/backends/postgres/dtypes/uuid/Entity?sort(id)',
    ]
    assert all(app.get(url).status_code == 200 for url in test_cases)

    # Test formats ('csv', 'jsonl', 'ascii', 'rdf')
    format_test_cases = [
        f'/backends/postgres/dtypes/uuid/Entity/:format/csv',
        f'/backends/postgres/dtypes/uuid/Entity/:format/jsonl',
        f'/backends/postgres/dtypes/uuid/Entity/:format/ascii',
        f'/backends/postgres/dtypes/uuid/Entity/:format/rdf',
    ]
    assert all(app.get(url).status_code == 200 for url in format_test_cases)

    # Test invalid uuid
    invalid_uuid = "invalid-uuid"
    resp = app.post('/backends/postgres/dtypes/uuid/Entity', json={
        'id': invalid_uuid
    })
    assert resp.status_code == 400


def test_uuid_sql(context, rc: RawConfig, tmp_path: Path, uuid_db: Sqlite):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
id | d | r | b | m | property  | type    | ref | source    | prepare | level | access | uri | title | description
   | datasets/uuid/example     |         |     |           |         |       |        |     |       |
   |   | data                  | sql     |     |           |         |       |        |     |       |
   |                           |         |     |           |         |       |        |     |       |
   |   |   |   | TestUUID      |         | id  | test_uuid |         |       | open   |     |       |
   |   |   |   |   | id        | integer |     | id        |         |       |        |     |       |
   |   |   |   |   | guid      | uuid    |     | guid      |         |       |        |     |       |
    '''))

    app = create_client(rc, tmp_path, uuid_db)

    resp = app.get('/datasets/uuid/example/TestUUID')
    assert resp.status_code == 200
    data = resp.json()['_data']
    assert len(data) == 3
