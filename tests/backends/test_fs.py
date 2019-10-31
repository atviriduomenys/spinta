import pathlib

import pytest

from spinta.testing.utils import get_error_codes, get_error_context


def test_crud(app):
    app.authorize([
        'spinta_photo_insert',
        'spinta_photo_update',
        'spinta_photo_image_update',
        'spinta_photo_patch',
        'spinta_photo_delete',
        'spinta_photo_getone',
        'spinta_photo_image_getone',
        'spinta_photo_image_delete',
    ])

    # Create a new photo resource.
    resp = app.post('/photos', json={
        '_type': 'photo',
        'name': 'myphoto',
    })
    assert resp.status_code == 201, resp.text
    data = resp.json()
    id_ = data['_id']
    revision = data['_revision']

    # PUT image to just create photo resource.
    resp = app.put(f'/photos/{id_}/image', data=b'BINARYDATA', headers={
        'content-type': 'image/png',
        # TODO: with content-disposition header it is possible to specify file
        #       name directly, but there should be option, to use model id as a
        #       file name, but that is currently not implemented.
        'content-disposition': 'attachment; filename="myimg.png"',
    })
    assert resp.status_code == 200, resp.text

    resp = app.get(f'/photos/{id_}')
    data = resp.json()
    assert data == {
        '_type': 'photo',
        '_id': id_,
        '_revision': data['_revision'],
        'name': 'myphoto',
    }
    assert data['_revision'] != revision
    revision = data['_revision']

    resp = app.get(f'/photos/{id_}/image:ref')
    assert resp.json() == {
        '_id': id_,
        '_revision': revision,
        'image': {
            'content_type': 'image/png',
            'filename': 'myimg.png',
        }
    }

    resp = app.get(f'/photos/{id_}/image')
    assert resp.content == b'BINARYDATA'

    resp = app.delete(f'/photos/{id_}/image')
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data == {
        '_id': id_,
        '_revision': data['_revision'],
        'image': None,
    }
    assert data['_revision'] != revision
    revision = data['_revision']

    resp = app.get(f'/photos/{id_}/image:ref')
    assert resp.status_code == 200
    assert resp.json() == {
        '_id': id_,
        '_revision': revision,
        'image': None,
    }

    resp = app.get(f'/photos/{id_}/image')
    assert resp.status_code == 404
    assert get_error_codes(resp.json()) == ['ItemDoesNotExist']
    assert get_error_context(resp.json(), 'ItemDoesNotExist', ['model', 'property', 'id']) == {
        'model': 'photo',
        'property': 'image',
        'id': id_,
    }

    resp = app.get(f'/photos/{id_}')
    assert resp.json() == {
        '_type': 'photo',
        '_id': id_,
        '_revision': revision,
        'name': 'myphoto',
    }


def test_add_existing_file(app, tmpdir):
    app.authorize([
        'spinta_photo_insert',
        'spinta_photo_image_getone',
        'spinta_photo_image_patch',
    ])

    image = pathlib.Path(tmpdir) / 'image.png'
    image.write_bytes(b'IMAGEDATA')

    resp = app.post('/photos', json={
        '_type': 'photo',
        'name': 'myphoto',
        'image': {
            'content_type': 'image/png',
            'filename': str(image),
        },
    })
    assert resp.status_code == 201, resp.text
    id_ = resp.json()['_id']

    resp = app.patch(f'/photos/{id_}/image:ref', json={
        'content_type': 'image/png',
        'filename': str(image),
    })
    assert resp.status_code == 200

    resp = app.get(f'/photos/{id_}/image')
    assert resp.content == b'IMAGEDATA'


def test_add_missing_file(app, tmpdir):
    app.authorize([
        'spinta_photo_insert',
        'spinta_photo_getone',
    ])

    image = pathlib.Path(tmpdir) / 'missing.png'

    resp = app.post('/photos', json={
        '_type': 'photo',
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
        'model': 'photo',
        'property': 'image',
        'file': str(image),
    }


def test_add_missing_file_as_prop(app, tmpdir):
    app.authorize([
        'spinta_photo_insert',
        'spinta_photo_getone',
        'spinta_photo_image_update',
    ])

    resp = app.post('/photos', json={
        '_type': 'photo',
        'name': 'myphoto',
    })
    id_ = resp.json()['_id']

    image = pathlib.Path(tmpdir) / 'missing.png'
    resp = app.put(f'/photos/{id_}/image:ref', json={
        'image': {
            'content_type': 'image/png',
            'filename': str(image),
        },
    })
    assert resp.status_code == 400, resp.text
    assert get_error_codes(resp.json()) == ['FileNotFound']
    assert get_error_context(resp.json(), 'FileNotFound', ['manifest', 'model', 'property', 'file']) == {
        'manifest': 'default',
        'model': 'photo',
        'property': 'image',
        'file': str(image),
    }


def test_id_as_filename(app, tmpdir):
    app.authorize([
        'spinta_photo_insert',
        'spinta_photo_getone',
        'spinta_photo_image_update',
        'spinta_photo_image_getone',
    ])

    resp = app.post('/photos', json={
        '_type': 'photo',
        'name': 'myphoto',
    })
    id_ = resp.json()['_id']

    resp = app.put(f'/photos/{id_}/image', data=b'BINARYDATA', headers={
        'content-type': 'image/png',
    })
    assert resp.status_code == 200, resp.text

    resp = app.get(f'/photos/{id_}/image:ref')
    assert resp.status_code == 200
    data = resp.json()
    assert resp.json() == {
        '_id': id_,
        '_revision': data['_revision'],
        'image': {
            'content_type': 'image/png',
            'filename': id_,
        },
    }

    resp = app.get(f'/photos/{id_}?select(name)')
    assert resp.status_code == 200
    assert resp.json() == {
        'name': 'myphoto',
    }
