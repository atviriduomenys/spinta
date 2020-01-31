import base64

import pytest


def _create_file(
    app, model: str, *,
    name: str = 'data.txt',
    ctype: str = 'text/plain',
    body: bytes = b'DATA',
):
    assert isinstance(body, bytes)
    resp = app.post(f'/{model}', json={
        'file': {
            '_id': name,
            '_content_type': ctype,
            '_content': base64.b64encode(body).decode(),
        },
    })
    assert resp.status_code == 201, resp.json()
    return resp


@pytest.mark.models(
    'backends/postgres/dtypes/file',
)
def test_insert(model, app):
    app.authmodel(model, ['insert', 'getone'])

    resp = _create_file(app, model)
    data = resp.json()

    pk = data['_id']
    rev = data['_revision']
    assert data == {
        '_type': 'backends/postgres/dtypes/file',
        '_id': pk,
        '_revision': rev,
        'file': {
            '_id': 'data.txt',
            '_content_type': 'text/plain',
        },
    }

    resp = app.get(f'{model}/{pk}/file')
    assert resp.status_code == 200, resp.json()
    assert resp.content == b'DATA'
    assert resp.headers['Revision'] == rev
    assert resp.headers['Content-Type'] == 'text/plain; charset=utf-8'
    assert resp.headers['Content-Disposition'] == 'attachment; filename="data.txt"'


@pytest.mark.models(
    'backends/postgres/dtypes/file',
)
def test_update(model, app, tmpdir):
    app.authmodel(model, ['insert', 'update', 'getone'])

    resp = _create_file(app, model)
    data = resp.json()

    pk = data['_id']
    rev = data['_revision']
    resp = app.put(f'/{model}/{pk}', json={
        '_revision': rev,
        'file': {
            '_id': 'data2.txt',
            '_content_type': 'text/plain',
            '_content': base64.b64encode(b'DATA2').decode(),
        },
    })
    data = resp.json()
    assert resp.status_code == 200, data
    assert data == {
        '_type': 'backends/postgres/dtypes/file',
        '_id': data['_id'],
        '_revision': data['_revision'],
        'file': {
            '_id': 'data2.txt',
            '_content_type': 'text/plain',
        },
    }
    assert data['_revision'] != rev

    resp = app.get(f'{model}/{pk}/file')
    assert resp.status_code == 200, resp.json()
    assert resp.content == b'DATA2'
    assert resp.headers['Content-Disposition'] == 'attachment; filename="data2.txt"'


@pytest.mark.models(
    'backends/postgres/dtypes/file',
)
def test_patch(model, app, tmpdir):
    app.authmodel(model, ['insert', 'patch', 'getone'])

    resp = _create_file(app, model)
    data = resp.json()

    pk = data['_id']
    rev = data['_revision']
    resp = app.patch(f'/{model}/{pk}', json={
        '_revision': rev,
        'file': {
            '_id': 'data2.txt',
            '_content': base64.b64encode(b'DATA2').decode(),
        },
    })
    data = resp.json()
    assert resp.status_code == 200, data
    assert data == {
        '_type': 'backends/postgres/dtypes/file',
        '_id': data['_id'],
        '_revision': data['_revision'],
        'file': {
            '_id': 'data2.txt',
        },
    }
    assert data['_revision'] != rev

    resp = app.get(f'{model}/{pk}/file')
    assert resp.status_code == 200, resp.json()
    assert resp.content == b'DATA2'
    assert resp.headers['Content-Disposition'] == 'attachment; filename="data2.txt"'


@pytest.mark.models(
    'backends/postgres/dtypes/file',
)
def test_subresource_update(model, app, tmpdir):
    app.authmodel(model, ['insert', 'update', 'getone'])

    resp = _create_file(app, model)
    data = resp.json()

    pk = data['_id']
    rev = data['_revision']
    resp = app.put(f'/{model}/{pk}/file', data=b'DATA4', headers={
        'Revision': rev,
        'Content-Type': 'text/plain',
        'Content-Disposition': 'attachment; filename="data.txt"',
    })
    data = resp.json()
    assert resp.status_code == 200, data
    assert data == {
        '_type': 'backends/postgres/dtypes/file.file',
        '_revision': data['_revision'],
        '_id': 'data.txt',
        '_content_type': 'text/plain',
    }
    assert data['_revision'] != rev

    resp = app.get(f'{model}/{pk}/file')
    assert resp.status_code == 200, resp.json()
    assert resp.content == b'DATA4'
    assert resp.headers['Content-Disposition'] == 'attachment; filename="data.txt"'


@pytest.mark.models(
    'backends/postgres/dtypes/file',
)
def test_subresource_update_ref(model, app, tmpdir):
    app.authmodel(model, ['insert', 'update', 'getone'])

    resp = _create_file(app, model)
    data = resp.json()

    pk = data['_id']
    rev = data['_revision']
    resp = app.put(f'/{model}/{pk}/file:ref', json={
        '_revision': data['_revision'],
        '_id': 'data.rst',
        '_content_type': 'text/x-rst',
    })
    data = resp.json()
    assert resp.status_code == 200, data
    assert data == {
        '_type': 'backends/postgres/dtypes/file.file',
        '_revision': data['_revision'],
        '_id': 'data.rst',
        '_content_type': 'text/x-rst',
    }
    assert data['_revision'] != rev

    resp = app.get(f'{model}/{pk}/file')
    assert resp.status_code == 200, resp.json()
    assert resp.content == b'DATA'
    assert resp.headers['Content-Type'] == 'text/x-rst; charset=utf-8'
    assert resp.headers['Content-Disposition'] == 'attachment; filename="data.rst"'


@pytest.mark.models(
    'backends/postgres/dtypes/file',
)
def test_subresource_patch_ref(model, app, tmpdir):
    app.authmodel(model, ['insert', 'patch', 'getone'])

    resp = _create_file(app, model)
    data = resp.json()

    pk = data['_id']
    rev = data['_revision']
    resp = app.patch(f'/{model}/{pk}/file:ref', json={
        '_revision': data['_revision'],
        '_id': 'data.rst',
    })
    data = resp.json()
    assert resp.status_code == 200, data
    assert data == {
        '_type': 'backends/postgres/dtypes/file.file',
        '_revision': data['_revision'],
        '_id': 'data.rst',
    }
    assert data['_revision'] != rev

    resp = app.get(f'{model}/{pk}/file')
    assert resp.status_code == 200, resp.json()
    assert resp.content == b'DATA'
    assert resp.headers['Content-Type'] == 'text/plain; charset=utf-8'
    assert resp.headers['Content-Disposition'] == 'attachment; filename="data.rst"'


@pytest.mark.models(
    'backends/postgres/dtypes/file',
)
def test_subresource_get_ref(model, app, tmpdir):
    app.authmodel(model, ['insert', 'getone'])

    resp = _create_file(app, model)
    data = resp.json()
    pk = data['_id']

    resp = app.get(f'{model}/{pk}/file:ref')
    data = resp.json()
    assert resp.status_code == 200, data
    assert data == {
        '_type': f'{model}.file',
        '_revision': data['_revision'],
        '_id': 'data.txt',
        '_content_type': 'text/plain',
    }


@pytest.mark.models(
    'backends/postgres/dtypes/file',
)
def test_subresource_delete(model, app, tmpdir):
    app.authmodel(model, ['insert', 'delete', 'getone'])

    resp = _create_file(app, model)
    data = resp.json()
    pk = data['_id']
    rev = data['_revision']

    resp = app.delete(f'/{model}/{pk}/file')
    assert resp.status_code == 204, resp.text
    data = resp.json()
    assert data == {
        '_type': f'{model}.file',
        '_revision': data['_revision'],
        '_id': None,
        '_content_type': None,
    }
    assert data['_revision'] != rev

    rev = data['_revision']
    resp = app.get(f'/{model}/{pk}/file:ref')
    assert resp.status_code == 200
    assert resp.json() == {
        '_type': f'{model}.file',
        '_revision': rev,
        '_id': None,
        '_content_type': None,
    }

    resp = app.get(f'/{model}/{pk}/file')
    assert resp.status_code == 404


@pytest.mark.models(
    'backends/postgres/dtypes/file',
)
def test_select(model, app, tmpdir):
    app.authmodel(model, ['insert', 'search'])
    _create_file(app, model)
    resp = app.get(f'/{model}?select(file._id)')
    assert resp.status_code == 200, resp.json()
    assert resp.json() == {
        '_data': [
            {
                'file': {
                    '_id': 'data.txt',
                },
            },
        ],
    }


@pytest.mark.models(
    'backends/postgres/dtypes/file',
)
def test_select_content(model, app, tmpdir):
    app.authmodel(model, ['insert', 'search'])
    _create_file(app, model)
    resp = app.get(f'/{model}?select(file._id,file._content)')
    assert resp.status_code == 200, resp.json()
    assert resp.json() == {
        '_data': [
            {
                'file': {
                    '_id': 'data.txt',
                    '_content': 'REFUQQ==',
                },
            },
        ],
    }


@pytest.mark.models(
    'backends/postgres/dtypes/file',
)
def test_select_all(model, app, tmpdir):
    app.authmodel(model, ['insert', 'search'])
    _create_file(app, model)
    resp = app.get(f'/{model}?select(file.*)')
    assert resp.status_code == 200, resp.json()
    assert resp.json() == {
        '_data': [
            {
                'file': {
                    '_id': 'data.txt',
                    '_content_type': 'text/plain',
                },
            },
        ],
    }
