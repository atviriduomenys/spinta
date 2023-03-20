from pathlib import Path

import pytest
from _pytest.fixtures import FixtureRequest

from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest, prepare_manifest, load_manifest
from spinta.testing.tabular import create_tabular_manifest


def test_load(tmp_path: Path, rc: RawConfig):
    table = '''
    d | r | b | m | property   | type   | ref                | source       | level | access
    dataset/1                  |        |                    |              |       |
      | external               | sql    |                    | sqlite://    |       |
                               |        |                    |              |       |
      |   |   | Country        |        | code               |              |       |
      |   |   |   | code       | string |                    |              |       | open
      |   |   |   | name       | string |                    |              |       | open
    dataset/2                  |        |                    |              |       |
                               |        |                    |              |       |
      |   |   | City           |        |                    |              |       |
      |   |   |   | name       | string |                    |              |       | open
      |   |   |   | country    | ref    | /dataset/1/Country |              | 3     | open
    '''
    create_tabular_manifest(tmp_path / 'manifest.csv', table)
    manifest = load_manifest(rc, tmp_path / 'manifest.csv')
    assert manifest == table


def test_external_ref(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, f'''
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
    ''', backend=postgresql, request=request)

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


