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
        'revision': revision,
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
        'avatar': {
            '_id': None,
            '_content_type': None,
        },
        'name': 'myphoto',
    }
    assert data['_revision'] != revision
    revision = data['_revision']

    resp = app.get(f'/{model}/{id_}/image:ref')
    assert resp.json() == {
        '_id': 'myimg.png',
        '_revision': revision,
        '_type': f'{model}.image',
        '_content_type': 'image/png',
    }

    resp = app.get(f'/{model}/{id_}/image')
    assert resp.content == b'BINARYDATA'

    resp = app.delete(f'/{model}/{id_}/image')
    assert resp.status_code == 204, resp.text
    data = resp.json()
    assert data == {
        '_id': id_,
        '_revision': data['_revision'],
        '_type': f'{model}.image',
    }
    assert data['_revision'] != revision
    revision = data['_revision']

    resp = app.get(f'/{model}/{id_}/image:ref')
    assert resp.status_code == 200
    assert resp.json() == {
        '_id': None,
        '_revision': revision,
        '_type': f'{model}.image',
        '_content_type': None,
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
        'avatar': {
            '_id': None,
            '_content_type': None,
        },
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
    })
    assert resp.status_code == 201, resp.text
    id_ = resp.json()['_id']
    revision_ = resp.json()['_revision']

    resp = app.patch(f'/{model}/{id_}/image:ref', json={
        '_revision': revision_,
        '_content_type': 'image/png',
        '_id': str(image),
    })
    assert resp.status_code == 200
    revision_ = resp.json()['_revision']

    resp = app.patch(f'/{model}/{id_}/image:ref', json={
        '_revision': revision_,
        '_content_type': 'image/png',
        '_id': str(image),
    })
    assert resp.status_code == 200

    resp = app.get(f'/{model}/{id_}/image')
    assert resp.content == b'IMAGEDATA'


@pytest.mark.models(
    'backends/mongo/photo',
    'backends/postgres/photo',
)
def test_add_missing_file(model, app, tmpdir):
    app.authmodel(model, ['insert', 'getone', 'image_patch'])

    avatar = pathlib.Path(tmpdir) / 'missing.png'

    resp = app.post(f'/{model}', json={
        '_type': model,
        'name': 'myphoto',
        'avatar': {
            '_content_type': 'image/png',
            '_id': str(avatar),
        },
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ['FileNotFound']
    assert get_error_context(resp.json(), 'FileNotFound', ['manifest', 'model', 'property', 'file']) == {
        'manifest': 'default',
        'model': model,
        'property': 'avatar',
        'file': str(avatar),
    }


@pytest.mark.models(
    'backends/mongo/photo',
    'backends/postgres/photo',
)
def test_create_hidden_image_on_insert(model, app, tmpdir):
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
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ['FieldNotInResource']
    assert get_error_context(resp.json(), 'FieldNotInResource', ['manifest', 'model', 'property']) == {
        'manifest': 'default',
        'model': model,
        'property': 'image',
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
        '_content_type': 'image/png',
        '_id': str(image),
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
    revision = resp.json()['_revision']

    resp = app.put(f'/{model}/{id_}/image', data=b'BINARYDATA', headers={
        'revision': revision,
        'content-type': 'image/png',
    })
    assert resp.status_code == 200, resp.text
    revision_ = resp.json()['_revision']

    resp = app.get(f'/{model}/{id_}/image:ref')
    assert resp.status_code == 200
    assert resp.json() == {
        '_id': id_,
        '_revision': revision_,
        '_type': f'{model}.image',
        '_content_type': 'image/png',
    }

    resp = app.get(f'/{model}/{id_}?select(name)')
    assert resp.status_code == 200
    assert resp.json() == {
        'name': 'myphoto',
    }


@pytest.mark.models(
    'backends/mongo/photo',
    'backends/postgres/photo',
)
def test_check_revision_for_file(model, app):
    app.authmodel(model, [
        'insert',
        'image_update',
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

    # PUT image without revision
    resp = app.put(f'/{model}/{id_}/image', data=b'BINARYDATA', headers={
        'content-type': 'image/png',
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ['NoItemRevision']

    # PUT image with revision
    resp = app.put(f'/{model}/{id_}/image', data=b'BINARYDATA', headers={
        'revision': revision,
        'content-type': 'image/png',
    })
    assert resp.status_code == 200, resp.text
    old_revision = revision
    revision = resp.json()['_revision']
    assert old_revision != revision


@pytest.mark.models(
    'backends/mongo/photo',
    'backends/postgres/photo',
)
def test_check_revision_for_file_ref(model, app, tmpdir):
    app.authmodel(model, ['insert', 'image_patch'])

    image = pathlib.Path(tmpdir) / 'image.png'
    image.write_bytes(b'IMAGEDATA')

    resp = app.post(f'/{model}', json={
        '_type': model,
        'name': 'myphoto',
    })
    assert resp.status_code == 201, resp.text
    id_ = resp.json()['_id']
    revision = resp.json()['_revision']

    # PATCH file without revision
    resp = app.patch(f'/{model}/{id_}/image:ref', json={
        'content_type': 'image/png',
        'filename': str(image),
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ['NoItemRevision']

    # PATCH file with revision
    resp = app.patch(f'/{model}/{id_}/image:ref', json={
        '_revision': revision,
        '_content_type': 'image/png',
        '_id': str(image),
    })
    assert resp.status_code == 200
    old_revision = revision
    revision = resp.json()['_revision']
    assert old_revision != revision


@pytest.mark.models(
    'backends/mongo/photo',
    'backends/postgres/photo',
)
def test_put_file_multiple_times(model, app):
    app.authmodel(model, ['insert', 'image_update', 'image_getone'])

    # Create a new report resource.
    resp = app.post(f'/{model}', json={
        '_type': model,
        'name': 'myphoto',
    })
    assert resp.status_code == 201, resp.text
    id_ = resp.json()['_id']
    revision = resp.json()['_revision']

    # Upload a PDF file.
    resp = app.put(f'/{model}/{id_}/image', data=b'BINARYDATA', headers={
        'revision': revision,
        'content-type': 'application/pdf',
        'content-disposition': f'attachment; filename="{id_}.pdf"',
    })
    assert resp.status_code == 200, resp.text
    revision = resp.json()['_revision']

    # Upload a new PDF file second time.
    resp = app.put(f'/{model}/{id_}/image', data=b'BINARYDATA2', headers={
        'revision': revision,
        'content-type': 'application/pdf',
        'content-disposition': f'attachment; filename="{id_}.pdf"',
    })
    assert resp.status_code == 200, resp.text

    resp = app.get(f'/{model}/{id_}/image')
    assert resp.content == b'BINARYDATA2'
