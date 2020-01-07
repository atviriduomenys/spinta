import pathlib

import pytest

from spinta.testing.utils import get_error_context, get_error_codes


@pytest.mark.models(
    'backends/mongo/subitem',
    'backends/postgres/subitem',
)
def test_get_subresource(model, app):
    app.authmodel(model, ['insert', 'getone',
                          'hidden_subobj_update', 'hidden_subobj_getone'])

    resp = app.post(f'/{model}', json={
        '_op': 'insert',
        '_type': model,
        'scalar': '42',
        'subarray': [{
            'foo': 'foobarbaz',
        }],
        'subobj': {
            'foo': 'foobar123',
            'bar': 42,
        },
    })
    assert resp.status_code == 201, resp.json()
    id_ = resp.json()['_id']
    revision_ = resp.json()['_revision']

    resp = app.put(f'/{model}/{id_}/hidden_subobj', json={
        '_revision': revision_,
        'fooh': 'secret',
        'barh': 1337,
    })
    assert resp.status_code == 200
    revision_ = resp.json()['_revision']

    resp = app.get(f'/{model}/{id_}/subarray')
    assert resp.status_code == 400
    assert get_error_context(resp.json(), "UnavailableSubresource", ["prop", "prop_type"]) == {
        'prop': 'subarray',
        'prop_type': 'array',
    }

    resp = app.get(f'/{model}/{id_}/scalar')
    assert resp.status_code == 400
    assert get_error_context(resp.json(), "UnavailableSubresource", ["prop", "prop_type"]) == {
        'prop': 'scalar',
        'prop_type': 'string',
    }

    resp = app.get(f'/{model}/{id_}/subobj')
    assert resp.status_code == 200
    assert resp.json() == {
        '_id': id_,
        '_type': f'{model}.subobj',
        '_revision': revision_,
        'foo': 'foobar123',
        'bar': 42,
    }

    resp = app.get(f'/{model}/{id_}/hidden_subobj')
    assert resp.status_code == 200
    assert resp.json() == {
        '_id': id_,
        '_type': f'{model}.hidden_subobj',
        '_revision': revision_,
        'fooh': 'secret',
        'barh': 1337,
    }


@pytest.mark.models(
    'backends/mongo/subitem',
    'backends/postgres/subitem',
)
def test_put_subresource(model, app):
    app.authmodel(model, [
        'insert', 'getone', 'update', 'subarray_update', 'hidden_subobj_update'
    ])

    resp = app.post(f'/{model}', json={
        '_op': 'insert',
        '_type': model,
        'scalar': '42',
        'subarray': [{
            'foo': 'foobarbaz',
        }],
        'subobj': {
            'foo': 'foobar123',
            'bar': 42,
        },
    })
    assert resp.status_code == 201, resp.json()
    id_ = resp.json()['_id']
    revision_ = resp.json()['_revision']

    resp = app.put(f'/{model}/{id_}/hidden_subobj', json={
        '_revision': revision_,
        'fooh': 'secret',
        'barh': 1337,
    })
    assert resp.status_code == 200
    revision_ = resp.json()['_revision']

    # PUT with object property
    resp = app.put(f'/{model}/{id_}/subobj', json={
        '_revision': revision_,
        'foo': 'changed',
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data['_id'] == id_
    assert data['_type'] == f'{model}.subobj'
    assert data['_revision'] != revision_
    assert data['foo'] == 'changed'
    revision_ = data['_revision']

    resp = app.put(f'/{model}/{id_}/hidden_subobj', json={
        '_revision': revision_,
        'fooh': 'changed secret',
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data['_id'] == id_
    assert data['_type'] == f'{model}.hidden_subobj'
    assert data['_revision'] != revision_
    assert data['fooh'] == 'changed secret'
    revision_ = data['_revision']

    # GET full resource
    resp = app.get(f'/{model}/{id_}')
    assert resp.status_code == 200
    data = resp.json()
    assert data == {
        '_id': id_,
        '_type': model,
        '_revision': revision_,
        'scalar': '42',
        'subarray': [{'foo': 'foobarbaz'}],
        'subobj': {'bar': None, 'foo': 'changed'},
    }

    # PUT to non object or file property - should not be possible
    resp = app.put(f'/{model}/{id_}/subarray', json={
        '_revision': revision_,
        'foo': 'array',
    })
    assert resp.status_code == 400
    assert get_error_context(resp.json(), "InvalidValue", ["property", "type"]) == {
        'property': 'subarray',
        'type': 'array',
    }

    resp = app.put(f'/{model}/{id_}/scalar', json={
        'scalar': '314',
    })
    assert resp.status_code == 400
    assert get_error_context(resp.json(), "InvalidValue", ["property", "type"]) == {
        'property': 'scalar',
        'type': 'string',
    }

    # Test that revision is required in json data
    resp = app.put(f'/{model}/{id_}/subobj', json={
        'foo': 'changed',
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["NoItemRevision"]


@pytest.mark.models(
    'backends/mongo/subitem',
    'backends/postgres/subitem',
)
def test_patch_subresource(model, app):
    app.authmodel(model, [
        'insert', 'getone', 'patch', 'subobj_patch',
        'subarray_patch', 'hidden_subobj_patch', 'hidden_subobj_update'
    ])

    resp = app.post(f'/{model}', json={
        '_op': 'insert',
        '_type': model,
        'scalar': '42',
        'subarray': [{
            'foo': 'foobarbaz',
        }],
        'subobj': {
            'foo': 'foobar123',
            'bar': 42,
        },
    })
    assert resp.status_code == 201, resp.json()
    id_ = resp.json()['_id']
    revision_ = resp.json()['_revision']

    resp = app.put(f'/{model}/{id_}/hidden_subobj', json={
        '_revision': revision_,
        'fooh': 'secret',
        'barh': 1337,
    })
    assert resp.status_code == 200
    revision_ = resp.json()['_revision']

    # PATCH with object property
    resp = app.patch(f'/{model}/{id_}/subobj', json={
        '_revision': revision_,
        'foo': 'changed',
    })
    assert resp.status_code == 200
    data = resp.json()
    revision_ = data['_revision']
    assert data == {
        '_id': id_,
        '_revision': revision_,
        '_type': f'{model}.subobj',
        'foo': 'changed',
    }

    # PATCH with already existing values
    resp = app.patch(f'/{model}/{id_}/subobj', json={
        '_revision': revision_,
        'foo': 'changed',
    })
    assert resp.status_code == 200
    data = resp.json()
    revision_ = data['_revision']
    assert data == {
        '_id': id_,
        '_revision': revision_,
        '_type': f'{model}.subobj',
    }

    # PATCH with hidden object property
    resp = app.patch(f'/{model}/{id_}/hidden_subobj', json={
        '_revision': revision_,
        'fooh': 'changed secret',
    })
    assert resp.status_code == 200
    data = resp.json()
    revision_ = data['_revision']
    assert data == {
        '_id': id_,
        '_revision': revision_,
        '_type': f'{model}.hidden_subobj',
        'fooh': 'changed secret',
    }

    # GET full resource
    resp = app.get(f'/{model}/{id_}')
    assert resp.status_code == 200
    data = resp.json()
    assert data == {
        '_id': id_,
        '_type': model,
        '_revision': revision_,
        'scalar': '42',
        'subarray': [{'foo': 'foobarbaz'}],
        'subobj': {'bar': 42, 'foo': 'changed'},
    }

    # Test that revision is required in json data
    resp = app.patch(f'/{model}/{id_}/subobj', json={
        'foo': 'changed',
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["NoItemRevision"]

    # PATCH to non object or file property - should not be possible
    resp = app.patch(f'/{model}/{id_}/subarray', json={
        '_revision': revision_,
        'foo': 'array',
    })
    assert resp.status_code == 400
    assert get_error_context(resp.json(), "InvalidValue", ["property", "type"]) == {
        'property': 'subarray',
        'type': 'array',
    }

    resp = app.patch(f'/{model}/{id_}/scalar', json={
        '_revision': revision_,
        'scalar': '314',
    })
    assert resp.status_code == 400
    assert get_error_context(resp.json(), "InvalidValue", ["property", "type"]) == {
        'property': 'scalar',
        'type': 'string',
    }


@pytest.mark.models(
    'backends/mongo/subitem',
    'backends/postgres/subitem',
)
def test_subresource_scopes(model, app):
    app.authmodel(model, ['insert', 'hidden_subobj_update'])

    resp = app.post(f'/{model}', json={
        '_op': 'insert',
        '_type': model,
        'subobj': {
            'foo': 'foobar123',
            'bar': 42,
        },
    })
    assert resp.status_code == 201, resp.json()
    id_ = resp.json()['_id']
    revision_ = resp.json()['_revision']

    resp = app.put(f'/{model}/{id_}/hidden_subobj', json={
        '_revision': revision_,
        'fooh': 'secret',
        'barh': 1337,
    })
    assert resp.status_code == 200
    revision_ = resp.json()['_revision']

    # try to GET subresource without specific subresource or model scope
    resp = app.get(f'/{model}/{id_}/subobj')
    assert resp.status_code == 403

    # try to GET subresource without specific subresource scope,
    # but with model scope
    app._scopes = []
    app.authmodel(model, ['getone'])
    resp = app.get(f'/{model}/{id_}/subobj')
    assert resp.status_code == 200
    assert resp.json() == {
        '_id': id_,
        '_type': f'{model}.subobj',
        '_revision': revision_,
        'foo': 'foobar123',
        'bar': 42,
    }

    # try to GET subresource without model scope,
    # but with specific subresource scope
    app._scopes = []
    app.authmodel(model, ['subobj_getone'])
    resp = app.get(f'/{model}/{id_}/subobj')
    assert resp.status_code == 200
    assert resp.json() == {
        '_id': id_,
        '_type': f'{model}.subobj',
        '_revision': revision_,
        'foo': 'foobar123',
        'bar': 42,
    }

    # try to GET subresource without specific hidden subresource or model scope
    app._scopes = []
    resp = app.get(f'/{model}/{id_}/hidden_subobj')
    assert resp.status_code == 403

    # try to GET subresource without specific hidden subresource scope,
    # but with model scope
    app._scopes = []
    app.authmodel(model, ['getone'])
    resp = app.get(f'/{model}/{id_}/hidden_subobj')
    assert resp.status_code == 403

    # try to GET subresource without model scope,
    # but with specific hidden subresource scope
    app._scopes = []
    app.authmodel(model, ['hidden_subobj_getone'])
    resp = app.get(f'/{model}/{id_}/hidden_subobj')
    assert resp.status_code == 200
    assert resp.json() == {
        '_id': id_,
        '_type': f'{model}.hidden_subobj',
        '_revision': revision_,
        'fooh': 'secret',
        'barh': 1337,
    }


@pytest.mark.models(
    'backends/mongo/subitem',
    'backends/postgres/subitem',
)
def test_get_subresource_file(model, app, tmpdir):
    app.authmodel(model, ['insert', 'getone', 'hidden_subobj_update',
                          'pdf_update', 'pdf_getone'])

    resp = app.post(f'/{model}', json={
        '_op': 'insert',
        '_type': model,
    })
    assert resp.status_code == 201, resp.json()
    id_ = resp.json()['_id']
    revision_ = resp.json()['_revision']

    resp = app.put(f'/{model}/{id_}/hidden_subobj', json={
        '_revision': revision_,
        'fooh': 'secret',
        'barh': 1337,
    })
    assert resp.status_code == 200
    revision_ = resp.json()['_revision']

    pdf = pathlib.Path(tmpdir) / 'report.pdf'
    pdf.write_bytes(b'REPORTDATA')

    resp = app.put(f'/{model}/{id_}/pdf:ref', json={
        '_revision': revision_,
        '_content_type': 'application/pdf',
        '_id': str(pdf),
    })
    assert resp.status_code == 200

    resp = app.get(f'/{model}/{id_}/pdf')
    assert resp.status_code == 200
    assert resp.headers['content-type'] == 'application/pdf'
    assert resp.content == b'REPORTDATA'

    resp = app.get(f'/{model}/{id_}/pdf:ref')
    assert resp.status_code == 200
    assert resp.json() == {
        '_type': f'{model}.pdf',
        '_revision': resp.json()['_revision'],
        '_content_type': 'application/pdf',
        '_id': str(pdf),
    }


@pytest.mark.models(
    'backends/mongo/subitem',
    'backends/postgres/subitem',
)
def test_put_hidden_subresource_on_model(model, app):
    app.authmodel(model, ['insert', 'getone', 'update', 'hidden_subobj_update'])

    resp = app.post(f'/{model}', json={
        '_op': 'insert',
        '_type': model,
    })
    assert resp.status_code == 201, resp.json()
    id_ = resp.json()['_id']
    revision_ = resp.json()['_revision']

    resp = app.put(f'/{model}/{id_}/hidden_subobj', json={
        '_revision': revision_,
        'fooh': 'secret',
        'barh': 1337,
    })
    assert resp.status_code == 200
    revision_ = resp.json()['_revision']

    resp = app.put(f'/{model}/{id_}', json={
        '_revision': revision_,
        'hidden_subobj': {
            'fooh': 'change_secret',
        },
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["FieldNotInResource"]


@pytest.mark.models(
    'backends/mongo/subitem',
    'backends/postgres/subitem',
)
def test_patch_hidden_subresource_on_model(model, app):
    app.authmodel(model, ['insert', 'getone', 'patch', 'hidden_subobj_update'])

    resp = app.post(f'/{model}', json={
        '_op': 'insert',
        '_type': model,
    })
    assert resp.status_code == 201, resp.json()
    id_ = resp.json()['_id']
    revision_ = resp.json()['_revision']

    resp = app.put(f'/{model}/{id_}/hidden_subobj', json={
        '_revision': revision_,
        'fooh': 'secret',
        'barh': 1337,
    })
    assert resp.status_code == 200
    revision_ = resp.json()['_revision']

    resp = app.patch(f'/{model}/{id_}', json={
        '_revision': revision_,
        'hidden_subobj': {
            'fooh': 'change_secret',
        },
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["FieldNotInResource"]
