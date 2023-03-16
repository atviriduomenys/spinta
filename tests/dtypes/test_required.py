import uuid

from pytest import FixtureRequest

from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.utils import get_error_codes, get_error_context


def test_insert_with_required_property(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property          | type            | ref
    example                           |                 |
      |   |   | City                  |                 |
      |   |   |   | name              | string required |
      |   |   |   | description       | string          |
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authmodel('example/City', [
        'insert',
        'getall'
    ])

    resp = app.post('/example/City', json={
        'description': 'Test city',
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["RequiredProperty"]
    assert get_error_context(
        resp.json(),
        "RequiredProperty",
        ["property", "model"]
    ) == {
        'property': 'name',
        'model': 'example/City',
    }
    resp = app.post('/example/City', json={
        'name': 'City',
        'description': 'Test city',
    })
    assert resp.status_code == 201
    resp = app.get('/example/City')
    assert listdata(resp, full=True) == [
        {
            'name': "City",
            'description': 'Test city',
        }
    ]


def test_update_with_required_property(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property          | type            | ref
    example                           |                 |
      |   |   | City                  |                 |
      |   |   |   | name              | string required |
      |   |   |   | description       | string          |
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authmodel('example/City', [
        'insert',
        'update',
        'getall',
    ])

    resp = app.post('/example/City', json={
        'name': 'City',
        'description': 'Test city',
    })
    city = resp.json()

    resp = app.put(f'/example/City/{city["_id"]}', json={
        '_revision': city["_revision"],
        'description': 'Test city updated',
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["RequiredProperty"]
    assert get_error_context(
        resp.json(),
        "RequiredProperty",
        ["property", "model"]
    ) == {
        'property': 'name',
        'model': 'example/City',
    }
    resp = app.put(f'/example/City/{city["_id"]}', json={
        '_revision': city["_revision"],
        'name': 'City',
        'description': 'Test city updated',
    })
    assert resp.status_code == 200
    resp = app.get('/example/City')
    assert listdata(resp, full=True) == [
        {
            'name': "City",
            'description': 'Test city updated',
        }
    ]


def test_patch_with_required_property(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property          | type            | ref
    example                           |                 |
      |   |   | City                  |                 |
      |   |   |   | name              | string required |
      |   |   |   | description       | string          |
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authmodel('example/City', [
        'insert',
        'patch',
        'getall',
    ])

    resp = app.post('/example/City', json={
        'name': 'City',
        'description': 'Test city',
    })
    city = resp.json()

    resp = app.patch(f'/example/City/{city["_id"]}', json={
        '_revision': city["_revision"],
        'name': None,
        'description': 'Test city updated',
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["RequiredProperty"]
    assert get_error_context(
        resp.json(),
        "RequiredProperty",
        ["property", "model"]
    ) == {
        'property': 'name',
        'model': 'example/City',
    }

    resp = app.patch(f'/example/City/{city["_id"]}', json={
        '_revision': city["_revision"],
        'description': 'Test city updated',
    })
    assert resp.status_code == 200
    resp = app.get('/example/City')
    assert listdata(resp, full=True) == [
        {
            'name': "City",
            'description': 'Test city updated',
        }
    ]


def test_upsert_insert_with_required_property(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property          | type            | ref
    example                           |                 |
      |   |   | City                  |                 |
      |   |   |   | name              | string required |
      |   |   |   | description       | string          |
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authmodel('example/City', [
        'insert',
        'upsert',
        'getall'
    ])

    resp = app.post('/example/City', json={
        '_op': 'upsert',
        '_where': f"_id='{str(uuid.uuid4())}'",
        'description': 'Test city',
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["RequiredProperty"]
    assert get_error_context(
        resp.json(),
        "RequiredProperty",
        ["property", "model"]
    ) == {
        'property': 'name',
        'model': 'example/City',
    }
    resp = app.post('/example/City', json={
        '_op': 'upsert',
        '_where': f"_id='{str(uuid.uuid4())}'",
        'name': 'City',
        'description': 'Test city',
    })
    assert resp.status_code == 201
    resp = app.get('/example/City')
    assert listdata(resp, full=True) == [
        {
            'name': "City",
            'description': 'Test city',
        }
    ]


def test_upsert_patch_with_required_property(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property          | type            | ref
    example                           |                 |
      |   |   | City                  |                 |
      |   |   |   | name              | string required |
      |   |   |   | description       | string          |
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authmodel('example/City', [
        'insert',
        'upsert',
        'getall',
    ])

    resp = app.post('/example/City', json={
        'name': 'City',
        'description': 'Test city',
    })
    city = resp.json()

    resp = app.post('/example/City', json={
        '_op': 'upsert',
        '_where': f"_id='{city['_id']}'",
        'description': 'Test city',
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["RequiredProperty"]
    assert get_error_context(
        resp.json(),
        "RequiredProperty",
        ["property", "model"]
    ) == {
        'property': 'name',
        'model': 'example/City',
    }
    resp = app.post('/example/City', json={
        '_op': 'upsert',
        '_where': f"_id='{city['_id']}'",
        'name': 'City',
        'description': 'Test city updated',
    })
    assert resp.status_code == 200
    resp = app.get('/example/City')
    assert listdata(resp, full=True) == [
        {
            'name': "City",
            'description': 'Test city updated',
        }
    ]
