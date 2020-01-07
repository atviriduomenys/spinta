import pathlib

import pytest

from spinta.testing.utils import get_error_codes, get_error_context


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_insert_get(model, app):
    app.authmodel(model, ['insert', 'getone'])

    resp = app.post(f'/{model}', json={
        '_type': model,
        'status': '42',
    })
    assert resp.status_code == 201

    data = resp.json()
    assert data == {
        '_type': model,
        '_id': data['_id'],
        '_revision': data['_revision'],
        'status': '42',
        'notes': [],
        'count': None,
        'report_type': None,
        'update_time': None,
        'valid_from_date': None,
        'operating_licenses': [],
        'sync': {
            'sync_resources': [],
            'sync_revision': None,
        },
    }

    # Read those objects from database.
    id_ = data['_id']
    resp = app.get(f'/{model}/{id_}')
    assert resp.json() == {
        '_type': model,
        '_revision': data['_revision'],
        '_id': id_,
        'status': '42',
        'notes': [],
        'count': None,
        'report_type': None,
        'update_time': None,
        'valid_from_date': None,
        'operating_licenses': [],
        'sync': {
            'sync_resources': [],
            'sync_revision': None,
        },
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_update_get(model, app):
    app.authmodel(model, ['insert', 'update', 'getone', 'getall'])

    resp = app.post(f'/{model}', json={
        '_type': model,
        'status': '42',
    })
    assert resp.status_code == 201

    # change report status
    data = resp.json()
    id_ = data['_id']
    revision = data['_revision']
    resp = app.put(f'/{model}/{id_}', json={
        '_revision': revision,
        'status': '13',
    })
    assert resp.status_code == 200, resp.json()

    data = resp.json()
    assert data['_revision'] != revision

    revision = data['_revision']
    assert data == {
        '_type': model,
        '_id': id_,
        '_revision': data['_revision'],
        'status': '13',
        'notes': [],
        'count': None,
        'report_type': None,
        'update_time': None,
        'valid_from_date': None,
        'operating_licenses': [],
        'sync': {
            'sync_revision': None,
            'sync_resources': [],
        },
    }

    # Read those objects from database.
    resp = app.get(f'/{model}/{id_}')
    data = resp.json()
    assert data == {
        '_type': model,
        '_revision': revision,
        '_id': id_,
        'status': '13',
        'notes': [],
        'count': None,
        'report_type': None,
        'update_time': None,
        'valid_from_date': None,
        'operating_licenses': [],
        'sync': {
            'sync_revision': None,
            'sync_resources': [],
        },
    }

    # Get all objects from database.
    resp = app.get(f'/{model}')
    data = resp.json()
    assert data == {
        '_data': [
            {
                '_type': model,
                '_id': id_,
                '_revision': revision,
                'notes': [],
                'report_type': None,
                'status': '13',
                'update_time': None,
                'valid_from_date': None,
                'count': None,
                'operating_licenses': [],
                'sync': {
                    'sync_revision': None,
                    'sync_resources': [],
                },
            },
        ]
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_put_non_existant_resource(model, app):
    resp = app.get(f'/{model}/4e67-256f9a7388f88ccc502570f434f289e8-057553c2')
    assert resp.status_code == 404
    assert get_error_codes(resp.json()) == ["ModelNotFound"]

    resp = app.put(f'/{model}/4e67-256f9a7388f88ccc502570f434f289e8-057553c2',
                   json={})
    assert resp.status_code == 404
    assert get_error_codes(resp.json()) == ["ModelNotFound"]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_get_non_existant_subresource(model, context, app):
    app.authmodel(model, ['insert', 'getone'])

    resp = app.post(f'/{model}', json={
        '_type': 'report',
        'status': '42',
    })
    assert resp.status_code == 201
    id_ = resp.json()['_id']

    manifest = context.get('store').manifests['default']
    resp = app.get(f'/{model}/{id_}/foo')
    assert resp.status_code == 404
    # FIXME: Fix error message, here model and resource is found, but model
    #        preprety is not found.
    assert resp.json() == {"errors": [{
        'type': 'model',
        'code': 'PropertyNotFound',
        'template': 'Property {property!r} not found.',
        'message': "Property 'foo' not found.",
        'context': {
            'schema': f'{manifest.path}/{model}.yml',
            'manifest': 'default',
            'model': model,
            'property': 'foo',
        },
    }]}


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_delete(model, app, tmpdir):
    # FIXME: `spinta_report_pdf_delete` gives access to:
    # DELETE /report/ID/pdf
    # DELETE /report/pdf/ID
    app.authmodel(model, [
        'insert',
        'getall',
        'delete',
        'pdf_delete',
        'pdf_update',
        'pdf_getone',
    ])

    resp = app.post(f'/{model}', json={'_data': [
        {'_op': 'insert', '_type': model, 'status': '1'},
        {'_op': 'insert', '_type': model, 'status': '2'},
    ]})
    assert resp.status_code == 200, resp.json()
    ids = [x['_id'] for x in resp.json()['_data']]
    revisions = [x['_revision'] for x in resp.json()['_data']]

    pdf = pathlib.Path(tmpdir) / 'report.pdf'
    pdf.write_bytes(b'REPORTDATA')

    resp = app.put(f'/{model}/{ids[0]}/pdf:ref', json={
        '_revision': revisions[0],
        '_content_type': 'application/pdf',
        '_id': str(pdf),
    })
    assert resp.status_code == 200

    resp = app.get(f'/{model}').json()
    data = [x['_id'] for x in resp['_data']]
    assert ids[0] in data
    assert ids[1] in data

    resp = app.delete(f'/{model}/{ids[0]}')
    assert resp.status_code == 204

    # multiple deletes should just return HTTP/404
    resp = app.delete(f'/{model}/{ids[0]}')
    assert resp.status_code == 404
    assert get_error_codes(resp.json()) == ["ItemDoesNotExist"]
    assert get_error_context(
        resp.json(),
        "ItemDoesNotExist",
        ["model", "id"],
    ) == {
        "model": model,
        "id": ids[0],
    }

    # subresourses should be deleted
    resp = app.delete(f'/{model}/{ids[0]}/pdf')
    assert resp.status_code == 404
    assert get_error_codes(resp.json()) == ["ItemDoesNotExist"]
    assert get_error_context(
        resp.json(),
        "ItemDoesNotExist",
        ["model", "id"],
    ) == {
        "model": model,
        "id": ids[0],
    }

    # FIXME: https://jira.tilaajavastuu.fi/browse/SPLAT-131
    # assert pdf.is_file() is False

    resp = app.get(f'/{model}').json()
    data = [x['_id'] for x in resp['_data']]
    assert ids[0] not in data
    assert ids[1] in data


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_delete_with_list_value(model, app):
    app.authmodel(model, [
        'insert',
        'delete'
    ])
    # delete report with list value
    id_ = app.post(f'/{model}', json={
        'status': '1',
        'notes': [{'note': 'NOTE', 'note_type': 'note type'}]
    }).json()['_id']

    resp = app.delete(f'/{model}/{id_}')
    assert resp.status_code == 204


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_patch(model, app, context):
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel(model, ['insert', 'getone', 'patch'])

    report_data = app.post(f'/{model}', json={
        '_type': model,
        'status': '1',
    }).json()
    id_ = report_data['_id']

    resp = app.patch(f'/{model}/{id_}', json={
        '_revision': report_data['_revision'],
        'status': '42',
    })
    assert resp.status_code == 200
    assert resp.json()['status'] == '42'
    revision = resp.json()['_revision']

    # test that revision mismatch is checked
    resp = app.patch(f'/{model}/{id_}', json={
        '_revision': 'r3v1510n',
        'status': '42',
    })
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ["ConflictingValue"]
    assert get_error_context(resp.json(), "ConflictingValue", ["given", "expected", "model"]) == {
        'given': 'r3v1510n',
        'expected': revision,
        'model': model,
    }

    resp = app.patch(f'/{model}/{id_}', json={
        '_revision': '',
        'status': '42'
    })
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ["ConflictingValue"]
    assert get_error_context(resp.json(), "ConflictingValue", ["given", "expected", "model"]) == {
        'given': '',
        'expected': revision,
        'model': model,
    }

    resp = app.patch(f'/{model}/{id_}', json={
        '_revision': None,
        'status': '42',
    })
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ["ConflictingValue"]
    assert get_error_context(resp.json(), "ConflictingValue", ["given", "expected", "model"]) == {
        'given': None,
        'expected': revision,
        'model': model,
    }

    # test that type mismatch is checked
    resp = app.patch(f'/{model}/{id_}', json={
        '_type': 'country',
        '_revision': revision,
        'status': '42',
    })
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ["ConflictingValue"]
    assert get_error_context(resp.json(), "ConflictingValue", ["given", "expected", "model"]) == {
        'given': 'country',
        'expected': model,
        'model': model,
    }

    # test that id mismatch is checked
    resp = app.patch(f'/{model}/{id_}', json={
        '_id': '0007ddec-092b-44b5-9651-76884e6081b4',
        '_revision': revision,
        'status': '42',
    })
    assert resp.status_code == 200
    data = resp.json()
    assert revision != data['_revision']
    revision = data['_revision']
    assert data['_id'] == '0007ddec-092b-44b5-9651-76884e6081b4'
    assert data['_revision'] == revision
    assert data['_type'] == model
    id_ = data['_id']

    # test that protected fields (_id, _type, _revision) are accepted, but not PATCHED
    resp = app.patch(f'/{model}/{id_}', json={
        '_id': id_,
        '_type': model,
        '_revision': revision,
        'status': '42',
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data['_id'] == id_
    assert data['_revision'] == revision
    assert data['_type'] == model


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_escaping_chars(model, app):
    app.authmodel(model, ['insert', 'getone'])

    resp = app.post(f'/{model}', json={
        '_type': model,
        'status': 'application/json',
    })
    assert resp.status_code == 201
    data = resp.json()

    resp = app.get(f'/{model}/{data["_id"]}')
    assert resp.status_code == 200
    assert resp.json()['status'] == 'application/json'


@pytest.mark.models(
    'backends/mongo/subitem',
    'backends/postgres/subitem',
)
def test_update_same_scalar(model, app):
    app.authmodel(model, ['insert', 'getone', 'update'])

    resource_keys = {'_id', '_revision', '_type', 'scalar', 'subarray', 'subobj'}

    resp = app.post(f'/{model}', json={
        '_type': model,
        'scalar': 'ok',
    })
    assert resp.status_code == 201
    id_ = resp.json()['_id']
    revision = resp.json()['_revision']

    resp = app.put(f'/{model}/{id_}', json={
        '_revision': revision,
        'scalar': 'ok',
    })
    assert resp.status_code == 200
    data = resp.json()
    assert set(data) == resource_keys
    new_revision = data['_revision']
    assert data['scalar'] == 'ok'
    assert data['subarray'] == []
    assert data['subobj'] == {'bar': None, 'foo': None}
    assert new_revision != revision

    # sanity check
    resp = app.get(f'/{model}/{id_}')
    assert resp.status_code == 200
    assert resp.json()['scalar'] == 'ok'


@pytest.mark.models(
    'backends/mongo/subitem',
    'backends/postgres/subitem',
)
def test_update_same_obj(model, app):
    app.authmodel(model, ['insert', 'getone', 'update'])

    resource_keys = {'_id', '_revision', '_type', 'scalar', 'subarray', 'subobj'}

    resp = app.post(f'/{model}', json={
        '_type': model,
        'subobj': {'foo': 'bar'},
    })
    assert resp.status_code == 201
    id_ = resp.json()['_id']
    revision = resp.json()['_revision']

    resp = app.put(f'/{model}/{id_}', json={
        '_revision': revision,
        'subobj': {'foo': 'bar'},
    })
    assert resp.status_code == 200
    data = resp.json()
    assert set(data) == resource_keys
    new_revision = data['_revision']
    assert data['subobj'] == {
        'foo': 'bar',
        'bar': None,
    }
    assert data['scalar'] is None
    assert data['subarray'] == []
    assert new_revision != revision

    # sanity check
    resp = app.get(f'/{model}/{id_}')
    assert resp.status_code == 200
    assert resp.json()['subobj'] == {
        'foo': 'bar',
        'bar': None,
    }


@pytest.mark.models(
    'backends/mongo/subitem',
    'backends/postgres/subitem',
)
def test_update_same_subresource(model, app):
    app.authmodel(model, ['insert', 'getone', 'update', 'subobj_update', 'subobj_get'])

    resp = app.post(f'/{model}', json={
        '_type': model,
        'subobj': {'foo': 'bar'},
    })
    assert resp.status_code == 201
    id_ = resp.json()['_id']
    revision = resp.json()['_revision']

    resp = app.put(f'/{model}/{id_}/subobj', json={
        '_revision': revision,
        'foo': 'baz',
    })
    assert resp.status_code == 200
    data = resp.json()
    new_revision = data['_revision']
    assert data == {
        '_id': id_,
        '_revision': new_revision,
        '_type': f'{model}.subobj',
        'bar': None,
        'foo': 'baz',
    }
    assert new_revision != revision

    # sanity check
    resp = app.get(f'/{model}/{id_}/subobj')
    assert resp.status_code == 200
    assert resp.json() == {
        '_id': id_,
        '_revision': new_revision,
        '_type': f'{model}.subobj',
        'bar': None,
        'foo': 'baz',
    }


@pytest.mark.models(
    'backends/mongo/subitem',
    'backends/postgres/subitem',
)
def test_update_same_array(model, app):
    app.authmodel(model, ['insert', 'getone', 'update'])

    resource_keys = {'_id', '_revision', '_type', 'scalar', 'subarray', 'subobj'}

    resp = app.post(f'/{model}', json={
        '_type': model,
        'subarray': [{'foo': 'baz'}],
    })
    assert resp.status_code == 201
    id_ = resp.json()['_id']
    revision = resp.json()['_revision']

    resp = app.put(f'/{model}/{id_}', json={
        '_revision': revision,
        'subarray': [{'foo': 'bar'}],
    })
    assert resp.status_code == 200
    data = resp.json()
    assert set(data) == resource_keys
    new_revision = data['_revision']
    assert data['subarray'] == [{'foo': 'bar'}]
    assert data['subobj'] == {'bar': None, 'foo': None}
    assert data['scalar'] is None
    assert new_revision != revision

    # sanity check
    resp = app.get(f'/{model}/{id_}')
    assert resp.status_code == 200
    assert resp.json()['subarray'] == [{'foo': 'bar'}]
