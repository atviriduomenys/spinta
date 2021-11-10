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

    resp = app.put(f'/{model}/{id_}/pdf', data=b'BINARYDATA', headers={
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
    assert len(accesslog) == 1
    assert accesslog[-1] == {
        'txn': accesslog[-1]['txn'],
        'time': accesslog[-1]['time'],
        'client': 'test-client',
        'method': 'POST',
        'url': f'https://testserver/{model}',
        'agent': 'testclient',
        'rctype': 'application/json',
        'format': 'json',
        'action': 'insert',
        'model': model,
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

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 1
    assert accesslog[-1] == {
        'method': 'POST',
        'agent': 'testclient',
        'rctype': 'application/json',
        'format': 'json',
        'action': 'insert',
        'url': f'https://testserver/{model}',
        'txn': accesslog[-1]['txn'],
        'time': accesslog[-1]['time'],
        'client': 'test-client',
        'model': model,
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
    rev = data['_revision']
    revision = data['_revision']

    resp = app.put(f'/{model}/{id_}', json={
        'status': '314',
        '_revision': revision,
    })
    assert resp.status_code == 200

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2
    assert accesslog[-1] == {
        'method': 'PUT',
        'agent': 'testclient',
        'rctype': 'application/json',
        'format': 'json',
        'action': 'update',
        'url': f'https://testserver/{model}/{id_}',
        'txn': accesslog[-1]['txn'],
        'time': accesslog[-1]['time'],
        'client':  'test-client',
        'model': model,
        'id': id_,
        'rev': rev,
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_pdf_put_accesslog(model, app, context):
    app.authmodel(model, ['insert', 'update', 'pdf_update'])
    id_, rev, resp = _upload_pdf(model, app)

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2  # 2 accesses overall: POST and PUT
    assert accesslog[-1] == {
        'action': 'update',
        'agent': 'testclient',
        'rctype': 'application/pdf',
        'format': 'json',
        'method': 'PUT',
        'url': f'https://testserver/{model}/{id_}/pdf',
        'txn': accesslog[-1]['txn'],
        'time': accesslog[-1]['time'],
        'client':  'test-client',
        'model': model,
        'prop': 'pdf',
        'id': id_,
        'rev': rev,
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
    rev = data['_revision']

    resp = app.patch(f'/{model}/{id_}', json={
        '_revision': rev,
        'status': '13',
    })
    assert resp.status_code == 200

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2
    assert accesslog[-1] == {
        'agent': 'testclient',
        'rctype': 'application/json',
        'format': 'json',
        'action': 'patch',
        'method': 'PATCH',
        'url': f'https://testserver/{model}/{id_}',
        'txn': accesslog[-1]['txn'],
        'time': accesslog[-1]['time'],
        'client': 'test-client',
        'model': model,
        'id': id_,
        'rev': rev,
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
    id_ = data['_id']
    resp = app.get(f'/{model}/{id_}')
    assert resp.status_code == 200

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2
    assert accesslog[-1] == {
        'agent': 'testclient',
        'format': 'json',
        'action': 'getone',
        'method': 'GET',
        'url': f'https://testserver/{model}/{id_}',
        'time': accesslog[-1]['time'],
        'client': 'test-client',
        'model': model,
        'id': id_,
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

    app.get(f'/{model}/{id_}')
    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 2
    assert accesslog[-1] == {
        'agent': 'testclient',
        'format': 'json',
        'action': 'getone',
        'method': 'GET',
        'url': f'https://testserver/{model}/{id_}',
        'time': accesslog[-1]['time'],
        'client': 'test-client',
        'model': model,
        'id': id_,
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_pdf_get_accesslog(model, app, context):
    app.authmodel(model, ['insert', 'update', 'pdf_update', 'pdf_getone'])
    id_, revision, resp = _upload_pdf(model, app)

    app.get(f'/{model}/{id_}/pdf')
    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 3  # 3 accesses overall: POST, PUT, GET
    assert accesslog[-1] == {
        'format': 'json',
        'agent': 'testclient',
        'action': 'getone',
        'method': 'GET',
        'url': f'https://testserver/{model}/{id_}/pdf',
        'time': accesslog[-1]['time'],
        'client': 'test-client',
        'model': model,
        'prop': 'pdf',
        'id': id_,
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
        'agent': 'testclient',
        'format': 'json',
        'action': 'getone',
        'method': 'GET',
        'url': f'https://testserver/{model}/{pk}/sync',
        'time': accesslog[-1]['time'],
        'client': 'test-client',
        'model': model,
        'prop': 'sync',
        'id': pk,
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
        'agent': 'testclient',
        'format': 'json',
        'action': 'getone',
        'method': 'GET',
        'url': f'https://testserver/{model}/{pk}?select(status)',
        'time': accesslog[-1]['time'],
        'client': 'test-client',
        'model': model,
        'id': pk,
    }


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
    assert len(accesslog) == 2
    assert accesslog[-1] == {
        'agent': 'testclient',
        'format': 'json',
        'action': 'getall',
        'method': 'GET',
        'url': f'https://testserver/{model}',
        'time': accesslog[-1]['time'],
        'client': 'test-client',
        'model': model,
    }


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
    assert len(accesslog) == 2
    assert accesslog[-1] == {
        'agent': 'testclient',
        'format': 'json',
        'action': 'search',
        'method': 'GET',
        'url': f'https://testserver/{model}?select(status)',
        'time': accesslog[-1]['time'],
        'client': 'test-client',
        'model': model,
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
    assert resp.status_code == 201

    accesslog = [json.loads(line) for line in logfile.read_text().splitlines()]
    assert len(accesslog) == 1
    assert accesslog[-1] == {
        'agent': 'testclient',
        'rctype': 'application/json',
        'format': 'json',
        'action': 'insert',
        'method': 'POST',
        'url': f'https://testserver/{model}',
        'txn': accesslog[-1]['txn'],
        'time': accesslog[-1]['time'],
        'client': 'test-client',
        'model': model,
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
    assert len(accesslog) == 1
    assert accesslog[-1] == {
        'action': 'insert',
        'agent': 'testclient',
        'client': 'test-client',
        'format': 'json',
        'method': 'POST',
        'model': 'backends/postgres/report',
        'rctype': 'application/json',
        'time': accesslog[-1]['time'],
        'txn': accesslog[-1]['txn'],
        'url': 'https://testserver/backends/postgres/report'
    }


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
    assert len(accesslog) == 1
    assert accesslog[-1] == {
        'action': 'insert',
        'agent': 'testclient',
        'client': 'test-client',
        'format': 'json',
        'method': 'POST',
        'model': 'backends/postgres/report',
        'rctype': 'application/json',
        'time': accesslog[-1]['time'],
        'txn': accesslog[-1]['txn'],
        'url': 'https://testserver/backends/postgres/report'
    }


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
        'agent': 'testclient',
        'format': 'json',
        'action': 'delete',
        'method': 'DELETE',
        'url': f'https://testserver/{model}/{id_}',
        'txn': accesslog[-1]['txn'],
        'time': accesslog[-1]['time'],
        'client': 'test-client',
        'model': model,
        'id': data['_id'],
    }


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
    assert len(accesslog) == 3  # 3 accesses overall: POST, PUT, DELETE
    assert accesslog[-1] == {
        'agent': 'testclient',
        'format': 'json',
        'action': 'delete',
        'method': 'DELETE',
        'url': f'https://testserver/{model}/{id_}/pdf',
        'txn': accesslog[-1]['txn'],
        'time': accesslog[-1]['time'],
        'client': 'test-client',
        'model': model,
        'prop': 'pdf',
        'id': id_,
    }


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
    id_, rev, resp = _upload_pdf(model, app)

    image = pathlib.Path(tmpdir) / 'image.png'
    image.write_bytes(b'IMAGEDATA')

    rev = _get_object_rev(app, model, id_)
    resp = app.put(f'/{model}/{id_}/pdf:ref', json={
        '_id': 'image.png',
        '_revision': rev
    })
    assert resp.status_code == 200

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 4  # 4 accesses overall: POST, PUT, GET, PUT
    assert accesslog[-1] == {
        'agent': 'testclient',
        'rctype': 'application/json',
        'format': 'json',
        'action': 'update',
        'method': 'PUT',
        'url': f'https://testserver/{model}/{id_}/pdf:ref',
        'txn': accesslog[-1]['txn'],
        'time': accesslog[-1]['time'],
        'client': 'test-client',
        'model': model,
        'prop': 'pdf',
        'id': id_,
        'rev': rev,
    }


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
    assert len(accesslog) == 1
    assert accesslog[-1] == {
        'agent': 'testclient',
        'rctype': 'application/json',
        'format': 'json',
        'action': 'insert',
        'method': 'POST',
        'url': f'https://testserver/{ns}',
        'txn': accesslog[-1]['txn'],
        'time': accesslog[-1]['time'],
        'client': 'test-client',
        'ns': ns,
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_stream_write(model, app, context, tmpdir):
    ns = model[:-len('/report')]

    app.authmodel(ns, ['insert'])

    headers = {'content-type': 'application/x-ndjson'}
    resp = app.post(f'/{ns}', headers=headers, data=json.dumps({
        '_op': 'insert',
        '_type': model,
        'status': 'ok',
    }))
    resp.raise_for_status()

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 1
    assert accesslog[-1] == {
        'agent': 'testclient',
        'rctype': 'application/x-ndjson',
        'format': 'json',
        'action': 'insert',
        'method': 'POST',
        'url': f'https://testserver/{ns}',
        'txn': accesslog[-1]['txn'],
        'time': accesslog[-1]['time'],
        'client': 'test-client',
        'ns': ns,
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_ns_read(model, app, context, tmpdir):
    ns = model[:-len('/report')]

    app.authmodel(ns, ['getall'])

    resp = app.get(f'/{ns}/:ns/:all')
    assert resp.status_code == 200, resp.json()

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 1
    assert accesslog[-1] == {
        'agent': 'testclient',
        'format': 'json',
        'action': 'getall',
        'method': 'GET',
        'url': f'https://testserver/{ns}/:ns/:all',
        'time': accesslog[-1]['time'],
        'client': 'test-client',
        'ns': ns,
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_ns_read(model, app, context, tmpdir):
    ns = model[:-len('/report')]

    app.authmodel(ns, ['getall'])

    resp = app.get(f'/{ns}/:ns/:all/:format/csv')
    assert resp.status_code == 200

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 1
    assert accesslog[-1] == {
        'agent': 'testclient',
        'format': 'csv',
        'action': 'getall',
        'method': 'GET',
        'url': f'https://testserver/{ns}/:ns/:all/:format/csv',
        'time': accesslog[-1]['time'],
        'client': 'test-client',
        'ns': ns,
    }
