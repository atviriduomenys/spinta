import pathlib

import pytest

from spinta.testing.utils import get_error_codes, get_error_context


@pytest.mark.models(
    'backends/mongo/photo',
    'backends/postgres/photo',
)
def test_crud(model, app):
    app.authmodel(model, [
        'insert',
        'update',
        'image_update',
        'patch',
        'delete',
        'getone',
        'image_getone',
        'image_delete',
    ])

    # Create a new photo resource.
    resp = app.post(f'/{model}', json={
        '_type': model,
        'name': 'myphoto',
    })
    assert resp.status_code == 201, resp.text
    data = resp.json()
    id_ = data['_id']
    revision = data['_revision']

    # PUT image to just create photo resource.
    resp = app.put(f'/{model}/{id_}/image', data=b'BINARYDATA', headers={
        'content-type': 'image/png',
        # TODO: with content-disposition header it is possible to specify file
        #       name directly, but there should be option, to use model id as a
        #       file name, but that is currently not implemented.
        'content-disposition': 'attachment; filename="myimg.png"',
    })
    assert resp.status_code == 200, resp.text

    resp = app.get(f'/{model}/{id_}')
    data = resp.json()
    assert data == {
        '_type': model,
        '_id': id_,
        '_revision': data['_revision'],
        'name': 'myphoto',
    }
    assert data['_revision'] != revision
    revision = data['_revision']

    resp = app.get(f'/{model}/{id_}/image:ref')
    assert resp.json() == {
        '_id': id_,
        '_revision': revision,
        'content_type': 'image/png',
        'filename': 'myimg.png',
    }

    resp = app.get(f'/{model}/{id_}/image')
    assert resp.content == b'BINARYDATA'

    resp = app.delete(f'/{model}/{id_}/image')
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data == {
        '_id': id_,
        '_revision': data['_revision'],
        'image': None
    }
    assert data['_revision'] != revision
    revision = data['_revision']

    resp = app.get(f'/{model}/{id_}/image:ref')
    assert resp.status_code == 200
    assert resp.json() == {
        '_id': id_,
        '_revision': revision,
    }

    resp = app.get(f'/{model}/{id_}/image')
    assert resp.status_code == 404
    assert get_error_codes(resp.json()) == ['ItemDoesNotExist']
    assert get_error_context(resp.json(), 'ItemDoesNotExist', ['model', 'property', 'id']) == {
        'model': model,
        'property': 'image',
        'id': id_,
    }

    resp = app.get(f'/{model}/{id_}')
    assert resp.json() == {
        '_type': model,
        '_id': id_,
        '_revision': revision,
        'name': 'myphoto',
    }


@pytest.mark.models(
    'backends/mongo/photo',
    'backends/postgres/photo',
)
def test_add_existing_file(model, app, tmpdir):
    app.authmodel(model, ['insert', 'image_getone', 'image_patch'])

    image = pathlib.Path(tmpdir) / 'image.png'
    image.write_bytes(b'IMAGEDATA')

    resp = app.post(f'/{model}', json={
        '_type': model,
        'name': 'myphoto',
        'image': {
            'content_type': 'image/png',
            'filename': str(image),
        },
    })
    assert resp.status_code == 201, resp.text
    id_ = resp.json()['_id']

    resp = app.patch(f'/{model}/{id_}/image:ref', json={
        'content_type': 'image/png',
        'filename': str(image),
    })
    assert resp.status_code == 200

    resp = app.get(f'/{model}/{id_}/image')
    assert resp.content == b'IMAGEDATA'


@pytest.mark.models(
    'backends/mongo/photo',
    'backends/postgres/photo',
)
def test_add_missing_file(model, app, tmpdir):
    app.authmodel(model, ['insert', 'getone'])

    image = pathlib.Path(tmpdir) / 'missing.png'

    resp = app.post(f'/{model}', json={
        '_type': model,
        'name': 'myphoto',
        'image': {
            'content_type': 'image/png',
            'filename': str(image),
        },
    })
    assert resp.status_code == 400, resp.text
    assert get_error_codes(resp.json()) == ['FileNotFound']
    assert get_error_context(resp.json(), 'FileNotFound', ['manifest', 'model', 'property', 'file']) == {
        'manifest': 'default',
        'model': model,
        'property': 'image',
        'file': str(image),
    }


@pytest.mark.models(
    'backends/mongo/photo',
    'backends/postgres/photo',
)
def test_add_missing_file_as_prop(model, app, tmpdir):
    app.authmodel(model, ['insert', 'getone', 'image_update'])

    resp = app.post(f'/{model}', json={
        '_type': model,
        'name': 'myphoto',
    })
    id_ = resp.json()['_id']
    revision_ = resp.json()['_revision']

    image = pathlib.Path(tmpdir) / 'missing.png'
    resp = app.put(f'/{model}/{id_}/image:ref', json={
        '_revision': revision_,
        'image': {
            'content_type': 'image/png',
            'filename': str(image),
        },
    })
    assert resp.status_code == 400, resp.text
    assert get_error_codes(resp.json()) == ['FileNotFound']
    assert get_error_context(resp.json(), 'FileNotFound', ['manifest', 'model', 'property', 'file']) == {
        'manifest': 'default',
        'model': model,
        'property': 'image',
        'file': str(image),
    }


@pytest.mark.models(
    'backends/mongo/photo',
    'backends/postgres/photo',
)
def test_id_as_filename(model, app, tmpdir):
    app.authmodel(model, ['insert', 'getone', 'image_update', 'image_getone'])

    resp = app.post(f'/{model}', json={
        '_type': model,
        'name': 'myphoto',
    })
    id_ = resp.json()['_id']

    resp = app.put(f'/{model}/{id_}/image', data=b'BINARYDATA', headers={
        'content-type': 'image/png',
    })
    assert resp.status_code == 200, resp.text

    resp = app.get(f'/{model}/{id_}/image:ref')
    assert resp.status_code == 200
    data = resp.json()
    assert resp.json() == {
        '_id': id_,
        '_revision': data['_revision'],
        'content_type': 'image/png',
        'filename': id_,
    }

    resp = app.get(f'/{model}/{id_}?select(name)')
    assert resp.status_code == 200
    assert resp.json() == {
        'name': 'myphoto',
    }
