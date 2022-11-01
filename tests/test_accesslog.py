import json
import pathlib

import pytest
from _pytest.capture import CaptureFixture

from spinta.accesslog.file import FileAccessLog
from spinta.components import Store
from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.context import create_test_context


def _upload_pdf(model, app):
    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201
    data = resp.json()
    id_ = data['_id']
    rev = data['_revision']

    resp = app.put(f'/{model}/{id_}/pdf', content=b'BINARYDATA', headers={
        'revision': rev,
        'content-type': 'application/pdf',
        'content-disposition': 'attachment; filename="test.pdf"',
    })
    assert resp.status_code == 200, resp.text
    return id_, rev, resp


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_post_accesslog(model, app, context):
    app.authmodel(model, ['insert'])

    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201, resp.json()

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'client': 'test-client',
            'method': 'POST',
            'url': f'https://testserver/{model}',
            'agent': 'testclient',
            'rctype': 'application/json',
            'format': 'json',
            'action': 'insert',
            'model': model,
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': 1,
        },
    ]


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

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'method': 'POST',
            'agent': 'testclient',
            'rctype': 'application/json',
            'format': 'json',
            'action': 'insert',
            'url': f'https://testserver/{model}',
            'client': 'test-client',
            'model': model,
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': 1,
        },
    ]


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
    rev = data['_revision']
    revision = data['_revision']

    resp = app.put(f'/{model}/{id_}', json={
        'status': '314',
        '_revision': revision,
    })
    assert resp.status_code == 200

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 4
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'method': 'PUT',
            'agent': 'testclient',
            'rctype': 'application/json',
            'format': 'json',
            'action': 'update',
            'url': f'https://testserver/{model}/{id_}',
            'client': 'test-client',
            'model': model,
            'id': id_,
            'rev': rev,
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': 1,
        },
    ]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_pdf_put_accesslog(model, app, context):
    app.authmodel(model, ['insert', 'update', 'pdf_update'])
    id_, rev, resp = _upload_pdf(model, app)

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 4  # 2 accesses overall: POST and PUT
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'action': 'update',
            'agent': 'testclient',
            'rctype': 'application/pdf',
            'format': 'json',
            'method': 'PUT',
            'url': f'https://testserver/{model}/{id_}/pdf',
            'client': 'test-client',
            'model': model,
            'prop': 'pdf',
            'id': id_,
            'rev': rev,
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': 1,
        },
    ]


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
    rev = data['_revision']

    resp = app.patch(f'/{model}/{id_}', json={
        '_revision': rev,
        'status': '13',
    })
    assert resp.status_code == 200

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 4
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'agent': 'testclient',
            'rctype': 'application/json',
            'format': 'json',
            'action': 'patch',
            'method': 'PATCH',
            'url': f'https://testserver/{model}/{id_}',
            'client': 'test-client',
            'model': model,
            'id': id_,
            'rev': rev,
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': 1,
        },
    ]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_get_accesslog(app, model, context):
    app.authmodel(model, ['insert', 'getone'])
    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201

    data = resp.json()
    id_ = data['_id']
    resp = app.get(f'/{model}/{id_}')
    assert resp.status_code == 200

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 4
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'agent': 'testclient',
            'format': 'json',
            'action': 'getone',
            'method': 'GET',
            'url': f'https://testserver/{model}/{id_}',
            'client': 'test-client',
            'model': model,
            'id': id_,
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': 1,
        },
    ]


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

    app.get(f'/{model}/{id_}')
    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 4
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'agent': 'testclient',
            'format': 'json',
            'action': 'getone',
            'method': 'GET',
            'url': f'https://testserver/{model}/{id_}',
            'client': 'test-client',
            'model': model,
            'id': id_,
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': 1,
        },
    ]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_pdf_get_accesslog(model, app, context):
    app.authmodel(model, ['insert', 'update', 'pdf_update', 'pdf_getone'])
    id_, revision, resp = _upload_pdf(model, app)

    app.get(f'/{model}/{id_}/pdf')
    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 6  # 3 accesses overall: POST, PUT, GET
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'format': 'json',
            'agent': 'testclient',
            'action': 'getone',
            'method': 'GET',
            'url': f'https://testserver/{model}/{id_}/pdf',
            'client': 'test-client',
            'model': model,
            'prop': 'pdf',
            'id': id_,
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': 1,
        },
    ]


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
    assert len(accesslog) == 4
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'agent': 'testclient',
            'format': 'json',
            'action': 'getone',
            'method': 'GET',
            'url': f'https://testserver/{model}/{pk}/sync',
            'client': 'test-client',
            'model': model,
            'prop': 'sync',
            'id': pk,
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': 1,
        },
    ]


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
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'agent': 'testclient',
            'format': 'json',
            'action': 'getone',
            'method': 'GET',
            'url': f'https://testserver/{model}/{pk}?select(status)',
            'client': 'test-client',
            'model': model,
            'id': pk,
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': 1,
        },
    ]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_getall_accesslog(app, model, context):
    app.authmodel(model, ['insert', 'getall'])

    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201

    resp = app.get(f'/{model}')
    assert resp.status_code == 200

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 4
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'agent': 'testclient',
            'format': 'json',
            'action': 'getall',
            'method': 'GET',
            'url': f'https://testserver/{model}',
            'client': 'test-client',
            'model': model,
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': 1,
        },
    ]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_getall_w_select_accesslog(app, model, context):
    app.authmodel(model, ['insert', 'getall', 'search'])
    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201

    resp = app.get(f'/{model}?select(status)')
    assert resp.status_code == 200

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 4
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'agent': 'testclient',
            'format': 'json',
            'action': 'search',
            'method': 'GET',
            'url': f'https://testserver/{model}?select(status)',
            'client': 'test-client',
            'model': model,
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': 1,
        },
    ]


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
    assert resp.status_code == 201

    accesslog = [json.loads(line) for line in logfile.read_text().splitlines()]
    assert len(accesslog) == 2
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'agent': 'testclient',
            'rctype': 'application/json',
            'format': 'json',
            'action': 'insert',
            'method': 'POST',
            'url': f'https://testserver/{model}',
            'client': 'test-client',
            'model': model,
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': 1,
        },
    ]


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

    store: Store = context.get('store')
    assert isinstance(store.accesslog, FileAccessLog)
    assert store.accesslog.file is None


@pytest.mark.models(
    'backends/postgres/report',
)
def test_accesslog_file_null(model, postgresql, rc, request):
    rc = rc.fork({
        'accesslog': {
            'type': 'file',
            'file': 'null',
        },
    })

    context = create_test_context(rc)
    request.addfinalizer(context.wipe_all)
    app = create_test_client(context)

    app.authmodel(model, ['insert'])
    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201

    store: Store = context.get('store')
    assert isinstance(store.accesslog, FileAccessLog)
    assert store.accesslog.file is None


@pytest.mark.models(
    'backends/postgres/report',
)
def test_accesslog_file_stdin(
    model: str,
    postgresql,
    rc: RawConfig,
    request,
    capsys: CaptureFixture,
):
    rc = rc.fork({
        'accesslog': {
            'type': 'file',
            'file': 'stdout',
        },
    })

    context = create_test_context(rc)
    request.addfinalizer(context.wipe_all)
    app = create_test_client(context)

    app.authmodel(model, ['insert'])
    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201

    store: Store = context.get('store')
    assert isinstance(store.accesslog, FileAccessLog)

    cap = capsys.readouterr()
    accesslog = [
        json.loads(line)
        for line in cap.out.splitlines()
        # Skip other lines from stdout that are not json
        if line.startswith('{')
    ]
    assert len(accesslog) == 2
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'action': 'insert',
            'agent': 'testclient',
            'client': 'test-client',
            'format': 'json',
            'method': 'POST',
            'model': 'backends/postgres/report',
            'rctype': 'application/json',
            'url': 'https://testserver/backends/postgres/report'
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': 1,
        },
    ]


@pytest.mark.models(
    'backends/postgres/report',
)
def test_accesslog_file_stderr(
    model: str,
    postgresql,
    rc: RawConfig,
    request,
    capsys: CaptureFixture,
):
    rc = rc.fork({
        'accesslog': {
            'type': 'file',
            'file': 'stderr',
        },
    })

    context = create_test_context(rc)
    request.addfinalizer(context.wipe_all)
    app = create_test_client(context)

    app.authmodel(model, ['insert'])
    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201

    store: Store = context.get('store')
    assert isinstance(store.accesslog, FileAccessLog)

    cap = capsys.readouterr()
    accesslog = [
        json.loads(line)
        for line in cap.err.splitlines()
        # Skip other lines from stdout that are not json
        if line.startswith('{')
    ]
    assert len(accesslog) == 2
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'action': 'insert',
            'agent': 'testclient',
            'client': 'test-client',
            'format': 'json',
            'method': 'POST',
            'model': 'backends/postgres/report',
            'rctype': 'application/json',
            'url': 'https://testserver/backends/postgres/report',
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': 1,
        },
    ]


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
    assert len(accesslog) == 4
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'agent': 'testclient',
            'format': 'json',
            'action': 'delete',
            'method': 'DELETE',
            'url': f'https://testserver/{model}/{id_}',
            'client': 'test-client',
            'model': model,
            'id': data['_id'],
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': 1,
        },
    ]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_pdf_delete_accesslog(model, app, context):
    app.authmodel(model, ['insert', 'update', 'getone', 'pdf_getone', 'pdf_update', 'pdf_delete'])
    id_, rev, resp = _upload_pdf(model, app)

    resp = app.delete(f'/{model}/{id_}/pdf')
    assert resp.status_code == 204
    assert resp.content == b''

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 6  # 3 accesses overall: POST, PUT, DELETE
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'agent': 'testclient',
            'format': 'json',
            'action': 'delete',
            'method': 'DELETE',
            'url': f'https://testserver/{model}/{id_}/pdf',
            'client': 'test-client',
            'model': model,
            'prop': 'pdf',
            'id': id_,
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': 1,
        },
    ]


def _get_object_rev(app, model: str, id_: str) -> str:
    resp = app.get(f'/{model}/{id_}')
    assert resp.status_code == 200
    data = resp.json()
    return data['_revision']


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_pdf_ref_update_accesslog(model, app, context, tmpdir):
    app.authmodel(model, ['insert', 'update', 'getone', 'pdf_getone', 'pdf_update', 'pdf_delete'])
    _id, rev, resp = _upload_pdf(model, app)

    image = pathlib.Path(tmpdir) / 'image.png'
    image.write_bytes(b'IMAGEDATA')

    rev = _get_object_rev(app, model, _id)
    resp = app.put(f'/{model}/{_id}/pdf:ref', json={
        '_id': 'image.png',
        '_revision': rev
    })
    assert resp.status_code == 200

    accesslog = context.get('accesslog.stream')
    srv = 'https://testserver'
    assert [(
        a['type'],
        a.get('method'),
        a.get('url'),
    ) for a in accesslog] == [
        ('request', 'POST', f'{srv}/{model}'),
        ('response', None, None),
        ('request', 'PUT', f'{srv}/{model}/{_id}/pdf'),
        ('response', None, None),
        ('request', 'GET', f'{srv}/{model}/{_id}'),
        ('response', None, None),
        ('request', 'PUT', f'{srv}/{model}/{_id}/pdf:ref'),
        ('response', None, None),
    ]
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'agent': 'testclient',
            'rctype': 'application/json',
            'format': 'json',
            'action': 'update',
            'method': 'PUT',
            'url': f'https://testserver/{model}/{_id}/pdf:ref',
            'client': 'test-client',
            'model': model,
            'prop': 'pdf',
            'id': _id,
            'rev': rev,
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': 1,
        },
    ]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_batch_write(model, app, context, tmpdir):
    ns = model[:-len('/report')]

    app.authmodel(ns, ['insert'])

    resp = app.post(f'/{ns}', json={
        '_data': [
            {
                '_op': 'insert',
                '_type': model,
                'status': 'ok',
            },
        ],
    })
    resp.raise_for_status()

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'agent': 'testclient',
            'rctype': 'application/json',
            'format': 'json',
            'action': 'insert',
            'method': 'POST',
            'url': f'https://testserver/{ns}',
            'client': 'test-client',
            'ns': ns,
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': 1,
        },
    ]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_stream_write(model, app, context, tmpdir):
    ns = model[:-len('/report')]

    app.authmodel(ns, ['insert'])

    headers = {'content-type': 'application/x-ndjson'}
    resp = app.post(f'/{ns}', headers=headers, content=json.dumps({
        '_op': 'insert',
        '_type': model,
        'status': 'ok',
    }))
    resp.raise_for_status()

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'agent': 'testclient',
            'rctype': 'application/x-ndjson',
            'format': 'json',
            'action': 'insert',
            'method': 'POST',
            'url': f'https://testserver/{ns}',
            'client': 'test-client',
            'ns': ns,
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': 1,
        },
    ]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_ns_read(model, app, context, tmpdir):
    ns = model[:-len('/report')]

    app.authmodel(ns, ['getall'])

    resp = app.get(f'/{ns}/:ns/:all')
    assert resp.status_code == 200, resp.json()

    objects = {
        'backends/mongo/report': 20,
        'backends/postgres/report': 21,
    }

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'agent': 'testclient',
            'format': 'json',
            'action': 'getall',
            'method': 'GET',
            'url': f'https://testserver/{ns}/:ns/:all',
            'client': 'test-client',
            'ns': ns,
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': objects[model],
        },
    ]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_ns_read_csv(model, app, context, tmpdir):
    ns = model[:-len('/report')]

    app.authmodel(ns, ['getall'])

    resp = app.get(f'/{ns}/:ns/:all/:format/csv')
    assert resp.status_code == 200

    objects = {
        'backends/mongo/report': 20,
        'backends/postgres/report': 21,
    }

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2
    assert accesslog[-2:] == [
        {
            'txn': accesslog[-2]['txn'],
            'type': 'request',
            'time': accesslog[-2]['time'],
            'agent': 'testclient',
            'format': 'csv',
            'action': 'getall',
            'method': 'GET',
            'url': f'https://testserver/{ns}/:ns/:all/:format/csv',
            'client': 'test-client',
            'ns': ns,
        },
        {
            'txn': accesslog[-2]['txn'],
            'type': 'response',
            'time': accesslog[-1]['time'],
            'delta': accesslog[-1]['delta'],
            'memory': accesslog[-1]['memory'],
            'objects': objects[model],
        },
    ]
