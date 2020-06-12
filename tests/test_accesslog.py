import json
import pathlib

import pytest

from spinta.testing.client import create_test_client
from spinta.testing.context import create_test_context


def _upload_pdf(model, app):
    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201
    data = resp.json()
    id_ = data['_id']
    revision = data['_revision']

    resp = app.put(f'/{model}/{id_}/pdf', data=b'BINARYDATA', headers={
        'revision': revision,
        'content-type': 'application/pdf',
        'content-disposition': 'attachment; filename="test.pdf"',
    })
    revision = resp.json()['_revision']
    assert resp.status_code == 200, resp.text
    return id_, revision, resp


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_post_accesslog(model, app, context):
    app.authmodel(model, ['insert'])

    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201, resp.json()

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
                'type': 'person',
                'id': 'test-client',
            },
            {
                'type': 'client',
                'id': 'test-client',
            },
        ],
        'fields': [],
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
def test_post_array_accesslog(model, app, context):
    app.authmodel(model, ['insert'])
    resp = app.post(f'/{model}', json={
        'status': '42',
        'notes': [{
            'note': 'foo',
        }],
    })
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
                'type': 'person',
                'id': 'test-client',
            },
            {
                'type': 'client',
                'id': 'test-client',
            },
        ],
        'fields': [],
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
def test_put_accesslog(model, app, context):
    app.authmodel(model, ['insert', 'update'])
    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201
    data = resp.json()
    id_ = data['_id']
    revision = data['_revision']

    resp = app.put(f'/{model}/{id_}', json={
        'status': '314',
        '_revision': revision,
    })
    assert resp.status_code == 200

    data = resp.json()
    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2
    assert accesslog[-1] == {
        'http_method': 'PUT',
        'url': f'https://testserver/{model}/{id_}',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'person',
                'id': 'test-client',
            },
            {
                'type': 'client',
                'id': 'test-client',
            },
        ],
        'fields': [],
        'resources': [
            {
                '_type': model,
                '_id': id_,
                '_revision': data['_revision'],
            },
        ],
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_pdf_put_accesslog(model, app, context):
    app.authmodel(model, ['insert', 'update', 'pdf_update'])
    id_, revision, resp = _upload_pdf(model, app)

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2  # 2 accesses overall: POST and PUT
    assert accesslog[-1] == {
        'http_method': 'PUT',
        'url': f'https://testserver/{model}/{id_}/pdf',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'person',
                'id': 'test-client',
            },
            {
                'type': 'client',
                'id': 'test-client',
            },
        ],
        'fields': [],
        'resources': [
            {
                '_type': f'{model}.pdf',
                '_id': id_,
                '_revision': revision,
            },
        ],
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_patch_accesslog(model, app, context):
    app.authmodel(model, ['insert', 'patch'])
    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201
    data = resp.json()
    id_ = data['_id']
    revision = data['_revision']

    resp = app.patch(f'/{model}/{id_}', json={
        '_revision': revision,
        'status': '13',
    })
    assert resp.status_code == 200

    data = resp.json()
    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2
    assert accesslog[-1] == {
        'http_method': 'PATCH',
        'url': f'https://testserver/{model}/{id_}',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'person',
                'id': 'test-client',
            },
            {
                'type': 'client',
                'id': 'test-client',
            },
        ],
        'fields': [
            '_revision',
            'status',
        ],
        'resources': [
            {
                '_type': model,
                '_id': id_,
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
    assert accesslog[-1] == {
        'http_method': 'GET',
        'url': f'https://testserver/{model}/{pk}',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'person',
                'id': 'test-client',
            },
            {
                'type': 'client',
                'id': 'test-client',
            },
        ],
        'fields': [],
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
def test_get_array_accesslog(model, app, context):
    app.authmodel(model, ['insert', 'getone'])
    resp = app.post(f'/{model}', json={
        'status': '42',
        'notes': [{
            'note': 'foo',
        }],
    })
    assert resp.status_code == 201
    data = resp.json()
    id_ = data['_id']

    resp = app.get(f'/{model}/{id_}')
    data = resp.json()
    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2
    assert accesslog[-1] == {
        'http_method': 'GET',
        'url': f'https://testserver/{model}/{id_}',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'person',
                'id': 'test-client',
            },
            {
                'type': 'client',
                'id': 'test-client',
            },
        ],
        'fields': [],
        'resources': [
            {
                '_type': model,
                '_id': id_,
                '_revision': data['_revision'],
            },
        ],
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_pdf_get_accesslog(model, app, context):
    app.authmodel(model, ['insert', 'update', 'pdf_update', 'pdf_getone'])
    id_, revision, resp = _upload_pdf(model, app)

    resp = app.get(f'/{model}/{id_}/pdf')
    assert resp.status_code == 200
    revision = resp.headers['revision']

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 3  # 3 accesses overall: POST, PUT, GET
    assert accesslog[-1] == {
        'http_method': 'GET',
        'url': f'https://testserver/{model}/{id_}/pdf',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'person',
                'id': 'test-client',
            },
            {
                'type': 'client',
                'id': 'test-client',
            },
        ],
        'fields': [],
        'resources': [
            {
                '_type': f'{model}.pdf',
                '_id': id_,
                '_revision': revision,  # XXX: revisions between report and report.pdf are not the same?
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
    assert accesslog[-1] == {
        'http_method': 'GET',
        'url': f'https://testserver/{model}/{pk}/sync',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'person',
                'id': 'test-client',
            },
            {
                'type': 'client',
                'id': 'test-client',
            }
        ],
        'fields': [],
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
    assert accesslog[-1] == {
        'http_method': 'GET',
        'url': f'https://testserver/{model}/{pk}?select(status)',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'person',
                'id': 'test-client',
            },
            {
                'type': 'client',
                'id': 'test-client',
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
def test_getall_accesslog(app, model, context):
    app.authmodel(model, ['insert', 'getall'])

    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201

    data = resp.json()
    resp = app.get(f'/{model}')
    assert resp.status_code == 200

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2
    assert accesslog[-1] == {
        'http_method': 'GET',
        'url': f'https://testserver/{model}',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'person',
                'id': 'test-client',
            },
            {
                'type': 'client',
                'id': 'test-client',
            },
        ],
        'fields': [],
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
    assert accesslog[-1] == {
        'http_method': 'GET',
        'url': f'https://testserver/{model}?select(status)',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'person',
                'id': 'test-client',
            },
            {
                'type': 'client',
                'id': 'test-client',
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
    assert accesslog[-1] == {
        'http_method': 'POST',
        'url': f'https://testserver/{model}',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'person',
                'id': 'test-client',
            },
            {
                'type': 'client',
                'id': 'test-client',
            },
        ],
        'fields': [],
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


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_delete_accesslog(model, app, context):
    app.authmodel(model, ['insert', 'delete'])
    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201
    data = resp.json()
    id_ = data['_id']

    resp = app.delete(f'/{model}/{id_}')
    assert resp.status_code == 204
    assert resp.content == b''

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2
    assert accesslog[-1] == {
        'http_method': 'DELETE',
        'url': f'https://testserver/{model}/{id_}',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'person',
                'id': 'test-client',
            },
            {
                'type': 'client',
                'id': 'test-client',
            },
        ],
        'fields': [],
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
def test_pdf_delete_accesslog(model, app, context):
    app.authmodel(model, ['insert', 'update', 'getone', 'pdf_getone', 'pdf_update', 'pdf_delete'])
    id_, revision, resp = _upload_pdf(model, app)

    resp = app.delete(f'/{model}/{id_}/pdf')
    assert resp.status_code == 204
    assert resp.content == b''

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 3  # 3 accesses overall: POST, PUT, DELETE
    assert accesslog[-1] == {
        'http_method': 'DELETE',
        'url': f'https://testserver/{model}/{id_}/pdf',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'person',
                'id': 'test-client',
            },
            {
                'type': 'client',
                'id': 'test-client',
            },
        ],
        'fields': [],
        'resources': [
            {
                '_type': f'{model}.pdf',
                '_id': id_,
                '_revision': revision,  # XXX: revisions between report and report.pdf are not the same?
            },
        ],
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_pdf_ref_update_accesslog(model, app, context, tmpdir):
    app.authmodel(model, ['insert', 'update', 'getone', 'pdf_getone', 'pdf_update', 'pdf_delete'])
    id_, revision, resp = _upload_pdf(model, app)

    image = pathlib.Path(tmpdir) / 'image.png'
    image.write_bytes(b'IMAGEDATA')

    resp = app.put(f'/{model}/{id_}/pdf:ref', json={
        '_id': 'image.png',
        '_revision': revision
    })
    assert resp.status_code == 200
    revision = resp.json()['_revision']

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 3  # 3 accesses overall: POST, PUT, PUT
    assert accesslog[-1] == {
        'http_method': 'PUT',
        'url': f'https://testserver/{model}/{id_}/pdf:ref',
        'reason': None,
        'transaction_id': accesslog[-1]['transaction_id'],
        'timestamp': accesslog[-1]['timestamp'],
        'accessors': [
            {
                'type': 'person',
                'id': 'test-client',
            },
            {
                'type': 'client',
                'id': 'test-client',
            },
        ],
        'fields': [],
        'resources': [
            {
                '_type': f'{model}.pdf',
                '_id': id_,
                '_revision': revision,
            },
        ],
    }
