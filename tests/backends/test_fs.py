import pathlib

import pytest
import requests

from spinta.testing.utils import error, get_error_codes, get_error_context


@pytest.mark.models(
    'backends/mongo/photo',
    'backends/postgres/photo',
)
def test_crud(model, app, tmpdir):
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
    img = pathlib.Path(tmpdir) / 'myimg.png'
    assert img.is_file() is True

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
    assert resp.content == b''
    assert img.is_file() is True

    resp = app.get(f'/{model}/{id_}/image:ref')
    assert resp.status_code == 200
    revision = resp.json()['_revision']

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
        '_id': 'image.png',
    })
    assert resp.status_code == 200
    revision_ = resp.json()['_revision']

    resp = app.patch(f'/{model}/{id_}/image:ref', json={
        '_revision': revision_,
        '_content_type': 'image/png',
        '_id': 'image.png',
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
            '_id': str(avatar),
            '_content_type': 'image/png',
        },
    })
    assert resp.status_code == 400
    assert error(resp) == 'FileNotFound'
    assert error(resp, 'code', ['manifest', 'model', 'property', 'file']) == {
        'code': 'FileNotFound',
        'context': {
            'manifest': 'default',
            'model': model,
            'property': 'avatar',
            'file': str(avatar),
        },
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

    resp = app.put(f'/{model}/{id_}/image:ref', json={
        '_revision': revision_,
        '_content_type': 'image/png',
        '_id': 'missing.png',
    })
    assert resp.status_code == 400, resp.text
    assert get_error_codes(resp.json()) == ['FileNotFound']
    assert get_error_context(
        resp.json(),
        'FileNotFound',
        ['manifest', 'model', 'property', 'file']
    ) == {
        'manifest': 'default',
        'model': model,
        'property': 'image',
        'file': 'missing.png',
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
        '_content_type': 'image/png',
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ['NoItemRevision']

    # PATCH file with revision
    resp = app.patch(f'/{model}/{id_}/image:ref', json={
        '_revision': revision,
        '_content_type': 'image/png',
        '_id': 'image.png',
    })
    assert resp.status_code == 200
    old_revision = revision
    revision = resp.json()['_revision']
    assert old_revision != revision


@pytest.mark.models(
    'backends/mongo/photo',
    'backends/postgres/photo',
)
def test_check_extra_field(model, app, tmpdir):
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
        '_content_type': 'image/png',
        '_revision': revision,
        'asd': 'qwerty'
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ['FieldNotInResource']


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


@pytest.mark.models(
    'backends/mongo/photo',
    'backends/postgres/photo',
)
def test_file_get_headers(model, app):
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

    resp = app.get(f'/{model}/{id_}/image')
    assert resp.content == b'BINARYDATA'
    assert 'revision' in resp.headers
    assert resp.headers['revision'] == revision


@pytest.mark.models(
    'backends/mongo/photo',
    'backends/postgres/photo',
)
def test_rename_non_existing_file(model, app):
    app.authmodel(model, ['getone', 'insert', 'image_update', 'image_patch', 'image_delete'])

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

    # DELETE the file
    resp = app.delete(f'/{model}/{id_}/image')
    assert resp.status_code == 204, resp.text
    assert resp.content == b''

    # Try to change reference file name, when file does not exist
    resp = app.get(f'/{model}/{id_}')
    revision = resp.json()['_revision']

    resp = app.patch(f'/{model}/{id_}/image:ref', json={
        '_revision': revision,
        '_content_type': 'application/pdf',
        '_id': 'file_does_not_exist.pdf',
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ['FileNotFound']
    assert get_error_context(resp.json(), 'FileNotFound', ['manifest', 'model', 'property', 'file']) == {
        'manifest': 'default',
        'model': model,
        'property': 'image',
        'file': 'file_does_not_exist.pdf',
    }


@pytest.mark.models(
    'backends/mongo/photo',
    'backends/postgres/photo',
)
def test_put_file_no_content(context, model, app):
    app.authmodel(model, ['insert', 'image_update', 'image_getone'])

    # Create a new report resource.
    resp = app.post(f'/{model}', json={
        '_type': model,
        'name': 'myphoto',
    })
    assert resp.status_code == 201, resp.text
    id_ = resp.json()['_id']
    revision = resp.json()['_revision']

    # Prepare request without any file
    req = requests.Request(
        'PUT',
        f'{app.base_url}/{model}/{id_}/image',
        headers={
            'revision': revision,
            'content-type': 'image/png',
        }
    )
    prep_req = req.prepare()
    # Make sure content-length does not exist
    del prep_req.headers['content-length']
    # Use authorization headers from before
    prep_req.headers['authorization'] = app.headers['authorization']

    # Upload an image, but add no file.
    resp = app.send(prep_req)
    assert resp.status_code == 411


@pytest.mark.models(
    'backends/mongo/photo',
    'backends/postgres/photo',
)
def test_changelog(context, model, app):
    app.authmodel(model, ['insert', 'image_update', 'image_delete',
                          'changes'])

    # Create a new photo resource.
    resp = app.post(f'/{model}', json={
        '_type': model,
        'name': 'myphoto',
    })
    assert resp.status_code == 201, resp.text
    data = resp.json()
    id_ = data['_id']
    revision = data['_revision']

    resp = app.put(f'/{model}/{id_}/image', data=b'BINARYDATA', headers={
        'revision': revision,
        'content-type': 'image/png',
        'content-disposition': 'attachment; filename="myimg.png"',
    })
    assert resp.status_code == 200, resp.text

    resp = app.delete(f'/{model}/{id_}/image')
    assert resp.status_code == 204, resp.text

    # check changelog
    if model == 'backends/mongo/photo':
        table_name = f'{model}__changelog'
        store = context.get('store')
        table = store.backends['mongo'].db[table_name]
        q = table.find({"$and": [{"__id": id_},
                                 {"$or": [{"_op": "delete"},
                                          {"_op": "update"}]}]})
    else:
        resp = app.get(f'/{model}/{id_}/:changes')
        assert resp.status_code == 200, resp.json()
        q = resp.json()['_data']

    rows = []
    for row in q:
        r = dict(row)
        if r['_op'] in ('update', 'delete'):
            rows.append({'_op': r['_op'], 'image': r['image']})

    assert sorted(rows, key=lambda r: r['_op']) == [
        {'_op': 'delete', 'image': None},
        {'_op': 'update', 'image': {'_id': 'myimg.png',
                                    '_content_type': 'image/png'}}
    ]


@pytest.mark.models(
    'backends/mongo/photo',
    'backends/postgres/photo',
)
def test_wipe(tmpdir, model, app):
    app.authmodel(model, [
        'insert',
        'image_update',
        'getone',
        'wipe'
    ])
    # Create file which should not be deleted after wipe
    new_file = pathlib.Path(tmpdir) / 'new.file'
    new_file.write_bytes(b'DATA')

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
        'content-disposition': 'attachment; filename="myimg.png"',
    })
    assert resp.status_code == 200, resp.text
    img = pathlib.Path(tmpdir) / 'myimg.png'
    assert img.is_file() is True

    resp = app.delete(f'/{model}/:wipe')
    assert resp.status_code == 200
    assert img.is_file() is False
    assert new_file.is_file() is True

    resp = app.get(f'/{model}/{id_}')
    assert resp.status_code == 404
