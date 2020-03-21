import json
import pathlib

import pytest

from spinta.testing.client import create_test_client
from spinta.testing.context import create_test_context


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_post_accesslog(model, app, context):
    app.authmodel(model, ['insert'])
    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201

    data = resp.json()
    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 1
    assert accesslog[-1] == {
        'http_method': 'POST',
        'url': f'https://testserver/{model}',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'client',
                'id': 'baa448a8-205c-4faa-a048-a10e4b32a136',
            },
        ],
        'fields': [
            'status',
        ],
        'resources': [
            {
                '_type': model,
                '_id': data['_id'],
                '_revision': data['_revision'],
            },
        ],
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_get_accesslog(app, model, context):
    app.authmodel(model, ['insert', 'getone'])
    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201

    data = resp.json()
    pk = data['_id']
    resp = app.get(f'/{model}/{pk}')
    assert resp.status_code == 200

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2
    accesslog[-1]['fields'].sort()
    assert accesslog[-1] == {
        'http_method': 'GET',
        'url': f'https://testserver/{model}/{pk}',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'client',
                'id': 'baa448a8-205c-4faa-a048-a10e4b32a136',
            },
        ],
        'fields': [
            '_id',
            '_revision',
            '_type',
            'count',
            'notes',
            'operating_licenses',
            'report_type',
            'status',
            'sync',
            'sync.sync_resources',
            'sync.sync_revision',
            'update_time',
            'valid_from_date',
        ],
        'resources': [
            {
                '_type': model,
                '_id': data['_id'],
                '_revision': data['_revision'],
            },
        ],
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_get_prop_accesslog(app, model, context):
    app.authmodel(model, ['insert', 'getone'])
    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201

    data = resp.json()
    pk = data['_id']
    resp = app.get(f'/{model}/{pk}/sync')
    assert resp.status_code == 200

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2
    accesslog[-1]['fields'].sort()
    assert accesslog[-1] == {
        'http_method': 'GET',
        'url': f'https://testserver/{model}/{pk}/sync',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'client',
                'id': 'baa448a8-205c-4faa-a048-a10e4b32a136',
            }
        ],
        'fields': [
            '_id',
            '_revision',
            '_type',
            'sync',
            'sync.sync_resources',
            'sync.sync_revision',
        ],
        'resources': [
            {
                '_type': f'{model}.sync',
                '_id': data['_id'],
                '_revision': data['_revision'],
            },
        ],
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_get_w_select_accesslog(app, model, context):
    app.authmodel(model, ['insert', 'getone'])
    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201

    data = resp.json()
    pk = data['_id']
    resp = app.get(f'/{model}/{pk}?select(status)')
    assert resp.status_code == 200

    accesslog = context.get('accesslog.stream')
    accesslog[-1]['fields'].sort()
    assert accesslog[-1] == {
        'http_method': 'GET',
        'url': f'https://testserver/{model}/{pk}?select(status)',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'client',
                'id': 'baa448a8-205c-4faa-a048-a10e4b32a136',
            },
        ],
        'fields': [
            '_id',
            '_revision',
            '_type',
            'status',
        ],
        'resources': [
            {
                '_type': model,
                '_id': data['_id'],
                '_revision': data['_revision'],
            },
        ],
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_getall_accesslog(app, model, context):
    app.authmodel(model, ['insert', 'getall'])
    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201

    data = resp.json()
    resp = app.get(f'/{model}')
    assert resp.status_code == 200

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2
    accesslog[-1]['fields'].sort()
    assert accesslog[-1] == {
        'http_method': 'GET',
        'url': f'https://testserver/{model}',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'client',
                'id': 'baa448a8-205c-4faa-a048-a10e4b32a136',
            },
        ],
        'fields': [
            '_id',
            '_revision',
            '_type',
            'count',
            'notes',
            'operating_licenses',
            'report_type',
            'status',
            'sync',
            'sync.sync_resources',
            'sync.sync_revision',
            'update_time',
            'valid_from_date',
        ],
        'resources': [
            {
                '_type': model,
                '_id': data['_id'],
                '_revision': data['_revision'],
            },
        ],
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_getall_w_select_accesslog(app, model, context):
    app.authmodel(model, ['insert', 'getall', 'search'])
    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201

    data = resp.json()
    resp = app.get(f'/{model}?select(status)')
    assert resp.status_code == 200

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2
    accesslog[-1]['fields'].sort()
    assert accesslog[-1] == {
        'http_method': 'GET',
        'url': f'https://testserver/{model}?select(status)',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'client',
                'id': 'baa448a8-205c-4faa-a048-a10e4b32a136',
            },
        ],
        'fields': [
            '_id',
            '_revision',
            '_type',
            'status',
        ],
        'resources': [
            {
                '_type': model,
                '_id': data['_id'],
                '_revision': data['_revision'],
            },
        ],
    }


@pytest.mark.models(
    'backends/postgres/report',
)
def test_accesslog_file(model, postgresql, rc, request, tmpdir):
    logfile = pathlib.Path(tmpdir / 'accesslog.log')

    rc = rc.fork({
        'accesslog': {
            'type': 'file',
            'file': str(logfile),
        },
    })

    context = create_test_context(rc)
    request.addfinalizer(context.wipe_all)
    app = create_test_client(context)

    app.authmodel(model, ['insert'])
    resp = app.post(f'/{model}', json={'status': '42'})
    data = resp.json()
    assert resp.status_code == 201

    accesslog = [json.loads(line) for line in logfile.read_text().splitlines()]
    assert len(accesslog) == 1
    accesslog[-1]['fields'].sort()
    assert accesslog[-1] == {
        'http_method': 'POST',
        'url': f'https://testserver/{model}',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'client',
                'id': 'baa448a8-205c-4faa-a048-a10e4b32a136',
            },
        ],
        'fields': [
            'status',
        ],
        'resources': [
            {
                '_type': model,
                '_id': data['_id'],
                '_revision': data['_revision'],
            },
        ],
    }


@pytest.mark.models(
    'backends/postgres/report',
)
def test_accesslog_file_dev_null(model, postgresql, rc, request):
    rc = rc.fork({
        'accesslog': {
            'type': 'file',
            'file': '/dev/null',
        },
    })

    context = create_test_context(rc)
    request.addfinalizer(context.wipe_all)
    app = create_test_client(context)

    app.authmodel(model, ['insert'])
    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201
