from pathlib import Path

from _pytest.fixtures import FixtureRequest

from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.manifest import load_manifest
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.utils import get_error_codes
import pytest


@pytest.mark.manifests('internal_sql', 'csv')
def test_load(manifest_type, tmp_path: Path, rc: RawConfig):
    table = '''
    d | r | b | m | property   | type   | ref                  | source       | level | access
    dataset/one                |        |                      |              |       |
      | external               | sql    |                      | sqlite://    |       |
                               |        |                      |              |       |
      |   |   | Country        |        | code                 |              |       |
      |   |   |   | code       | string |                      |              |       | open
      |   |   |   | name       | string |                      |              |       | open
    dataset/two                |        |                      |              |       |
                               |        |                      |              |       |
      |   |   | City           |        |                      |              |       |
      |   |   |   | name       | string |                      |              |       | open
      |   |   |   | country    | ref    | /dataset/one/Country |              | 3     | open
    '''
    manifest = load_manifest(rc, table, manifest_type=manifest_type, tmp_path=tmp_path)
    assert manifest == table


@pytest.mark.manifests('internal_sql', 'csv')
def test_external_ref(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property | type   | ref                           | source       | level | access
    datasets/externalref     |        |                               |              |       |
      | external             | sql    |                               | sqlite://    |       |
      |   |   | Country      |        | code                          |              |       |
      |   |   |   | code     | string |                               |              |       | open
      |   |   |   | name     | string |                               |              |       | open
    datasets/internal        |        |                               |              |       |
      |   |   | City         |        |                               |              |       |
      |   |   |   | name     | string |                               |              |       | open
      |   |   |   | country  | ref    | /datasets/externalref/Country |              | 3     | open
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )

    app = create_test_client(context)
    app.authmodel('datasets/internal/City', [
        'insert',
        'getall',
        'search'
    ])

    resp = app.post('/datasets/internal/City', json={
        'country': {'code': "lt"},
        'name': 'Vilnius',
    })
    assert resp.status_code == 201

    resp = app.get('/datasets/internal/City')
    assert listdata(resp, full=True) == [
        {
            'country.code': 'lt',
            'name': "Vilnius"
        }
    ]

    resp = app.get('/datasets/internal/City?select(country.code)')
    assert listdata(resp, full=True) == [
        {
            'country.code': 'lt'
        }
    ]


@pytest.mark.manifests('internal_sql', 'csv')
def test_external_ref_without_primary_key(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property | type   | ref                           | source       | level | access
    datasets/externalref     |        |                               |              |       |
      | external             | sql    |                               | sqlite://    |       |
      |   |   | Country      |        |                               |              |       |
      |   |   |   | code     | string |                               |              |       | open
      |   |   |   | name     | string |                               |              |       | open
    datasets/internal/pk     |        |                               |              |       |
      |   |   | City         |        |                               |              |       |
      |   |   |   | name     | string |                               |              |       | open
      |   |   |   | country  | ref    | /datasets/externalref/Country |              | 3     | open
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )

    app = create_test_client(context)
    app.authmodel('datasets/internal/pk/City', [
        'insert',
        'getall',
        'search'
    ])

    _id = '4d741843-4e94-4890-81d9-5af7c5b5989a'
    resp = app.post('/datasets/internal/pk/City', json={
        'country': {'_id': _id},
        'name': 'Vilnius',
    })
    assert resp.status_code == 201

    resp = app.get('/datasets/internal/pk/City')
    assert listdata(resp, full=True) == [
        {
            'country._id': _id,
            'name': "Vilnius"
        }
    ]

    resp = app.get('/datasets/internal/pk/City?select(country)')
    assert listdata(resp, full=True) == [
        {
            'country._id': _id
        }
    ]


@pytest.mark.manifests('internal_sql', 'csv')
def test_external_ref_with_explicit_key(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property | type   | ref                               | source       | level | access
    datasets/external/ref    |        |                                   |              |       |
      | external             | sql    |                                   | sqlite://    |       |
      |   |   | Country      |        |                                   |              |       |
      |   |   |   | id       | integer|                                   |              |       | open
      |   |   |   | name     | string |                                   |              |       | open
    datasets/explicit/ref    |        |                                   |              |       |
      |   |   | City         |        |                                   |              |       |
      |   |   |   | name     | string |                                   |              |       | open
      |   |   |   | country  | ref    | /datasets/external/ref/Country[id]|              | 3     | open
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )

    app = create_test_client(context)
    app.authmodel('datasets/explicit/ref/City', [
        'insert',
        'getall',
        'search'
    ])

    resp = app.post('/datasets/explicit/ref/City', json={
        'country': {'_id': 1},
        'name': 'Vilnius',
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ['FieldNotInResource']

    resp = app.post('/datasets/explicit/ref/City', json={
        'country': {'id': 1},
        'name': 'Vilnius',
    })
    assert resp.status_code == 201

    resp = app.get('/datasets/explicit/ref/City')
    assert listdata(resp, full=True) == [
        {
            'country.id': 1,
            'name': "Vilnius"
        }
    ]

    resp = app.get('/datasets/explicit/ref/City?select(country)')
    assert listdata(resp, full=True) == [
        {
            'country.id': 1
        }
    ]
