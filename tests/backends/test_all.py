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
        'type': model,
        'status': '42',
    })
    assert resp.status_code == 201

    data = resp.json()
    assert data == {
        'type': model,
        'id': data['id'],
        'revision': data['revision'],
        'status': '42',
        'notes': [],
        'count': None,
        'report_type': None,
        'update_time': None,
        'valid_from_date': None,
        'operating_licenses': [],
    }

    # Read those objects from database.
    id_ = data['id']
    resp = app.get(f'/{model}/{id_}')
    assert resp.json() == {
        'type': model,
        'revision': data['revision'],
        'id': id_,
        'status': '42',
        'notes': [],
        'count': None,
        'report_type': None,
        'update_time': None,
        'valid_from_date': None,
        'operating_licenses': [],
    }


# FIXME: postgres PUT requests do not work
# @pytest.mark.models(
#     'backends/mongo/report',
#     'backends/postgres/report',
# )
@pytest.mark.models(
    'backends/mongo/report',
)
def test_update_get(model, app):
    app.authmodel(model, ['insert', 'update', 'getone', 'getall'])

    resp = app.post(f'/{model}', json={
        'type': model,
        'status': '42',
    })
    assert resp.status_code == 201

    # change report status
    data = resp.json()
    id_ = data['id']
    revision = data['revision']
    resp = app.put(f'/{model}/{id_}', json={
        'revision': revision,
        'status': '13',
    })
    assert resp.status_code == 200

    data = resp.json()
    assert data['revision'] != revision

    revision = data['revision']
    assert data == {
        'type': model,
        'id': id_,
        'revision': data['revision'],
        'status': '13',
        'notes': [],
        'count': None,
        'report_type': None,
        'update_time': None,
        'valid_from_date': None,
        'operating_licenses': [],
    }

    # Read those objects from database.
    resp = app.get(f'/{model}/{id_}')
    data = resp.json()
    assert data == {
        'type': model,
        'revision': revision,
        'id': id_,
        'status': '13',
        'notes': [],
        'count': None,
        'report_type': None,
        'update_time': None,
        'valid_from_date': None,
        'operating_licenses': [],
    }

    # Get all objects from database.
    resp = app.get(f'/{model}')
    data = resp.json()
    assert data == {
        'data': [
            {
                'type': model,
                'id': id_,
                'notes': [],
                'report_type': None,
                'revision': revision,
                'status': '13',
                'update_time': None,
                'valid_from_date': None,
                'count': None,
                'operating_licenses': [],
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
def test_get_non_existant_subresource(model, app):
    app.authmodel(model, ['insert', 'getone'])

    resp = app.post(f'/{model}', json={
        'type': 'report',
        'status': '42',
    })
    assert resp.status_code == 201
    id_ = resp.json()['id']

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
            'schema': f'tests/manifest/{model}.yml',
            'manifest': 'default',
            'model': model,
            'property': 'foo',
        },



    }]}


# FIXME: postgres throws 500 when cannot find resource instead of 404
@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_delete(model, context, app, tmpdir):
    result = context.push([
        {'type': model, 'status': '1'},
        {'type': model, 'status': '2'},
    ])
    ids = [x['id'] for x in result]

    # FIXME: `spinta_report_pdf_delete` gives access to:
    # DELETE /report/ID/pdf
    # DELETE /report/pdf/ID
    app.authmodel(model, [
        'getall',
        'delete',
        'pdf_delete',
        'pdf_update',
        'pdf_getone',
    ])

    pdf = pathlib.Path(tmpdir) / 'report.pdf'
    pdf.write_bytes(b'REPORTDATA')

    resp = app.put(f'/{model}/{ids[0]}/pdf:ref', json={
        'content_type': 'application/pdf',
        'filename': str(pdf),
    })
    assert resp.status_code == 200

    resp = app.get(f'/{model}').json()
    data = [x['id'] for x in resp['data']]
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
    data = [x['id'] for x in resp['data']]
    assert ids[0] not in data
    assert ids[1] in data


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_patch(model, app, context):
    app.authmodel(model, ['insert', 'getone', 'patch'])

    report_data = app.post(f'/{model}', json={
        'type': model,
        'status': '1',
    }).json()
    id_ = report_data['id']

    resp = app.patch(f'/{model}/{id_}',
                     json={'status': '42'})
    assert resp.status_code == 200
    assert resp.json()['status'] == '42'
    revision = resp.json()['revision']

    # test that revision mismatch is checked
    resp = app.patch(f'/{model}/{id_}',
                     json={'revision': 'r3v1510n', 'status': '42'})
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ["ConflictingValue"]
    assert get_error_context(
        resp.json(),
        "ConflictingValue",
        ["given", "expected", "model"],
    ) == {
        'given': 'r3v1510n',
        'expected': revision,
        'model': model,
    }

    resp = app.patch(f'/{model}/{id_}',
                     json={'revision': '', 'status': '42'})
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ["ConflictingValue"]
    assert get_error_context(
        resp.json(),
        "ConflictingValue",
        ["given", "expected", "model"],
    ) == {
        'given': '',
        'expected': revision,
        'model': model,
    }

    resp = app.patch(f'/{model}/{id_}',
                     json={'revision': None, 'status': '42'})
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ["ConflictingValue"]
    assert get_error_context(
        resp.json(),
        "ConflictingValue",
        ["given", "expected", "model"],
    ) == {
        'given': None,
        'expected': revision,
        'model': model,
    }

    # test that type mismatch is checked
    resp = app.patch(f'/{model}/{id_}',
                     json={'type': 'country', 'revision': revision, 'status': '42'})
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ["ConflictingValue"]
    assert get_error_context(
        resp.json(),
        "ConflictingValue",
        ["given", "expected", "model"],
    ) == {
        'given': 'country',
        'expected': model,
        'model': model,
    }

    # test that id mismatch is checked
    resp = app.patch(f'/{model}/{id_}',
                     json={'id': '0007ddec-092b-44b5-9651-76884e6081b4', 'revision': revision, 'status': '42'})
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ["ConflictingValue"]
    assert get_error_context(
        resp.json(),
        "ConflictingValue",
        ["given", "expected", "model"],
    ) == {
        'given': '0007ddec-092b-44b5-9651-76884e6081b4',
        'expected': id_,
        'model': model,
    }

    # test that protected fields (id, type, revision) are accepted, but not PATCHED
    resp = app.patch(f'/{model}/{id_}',
                     json={
                         'id': id_,
                         'type': model,
                         'revision': revision,
                         'status': '42',
                     })
    assert resp.status_code == 200
    resp_data = resp.json()

    assert resp_data['id'] == id_
    assert resp_data['type'] == model
    # new status patched
    assert resp_data['status'] == '42'
    # new revision created regardless of PATCH'ed JSON
    assert resp_data['revision'] != revision


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_escaping_chars(model, app):
    app.authmodel(model, ['insert', 'getone'])

    resp = app.post(f'/{model}', json={
        'type': model,
        'status': 'application/json',
    })
    assert resp.status_code == 201
    data = resp.json()

    resp = app.get(f'/{model}/{data["id"]}')
    assert resp.status_code == 200
    assert resp.json()['status'] == 'application/json'
