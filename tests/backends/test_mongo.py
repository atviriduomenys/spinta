import pathlib

from spinta.testing.utils import get_error_codes, get_error_context


def test_mongo_insert_get(app):
    app.authorize([
        'spinta_report_insert',
        'spinta_report_getone',
    ])

    resp = app.post('/reports', json={
        'type': 'report',
        'status': '42',
    })
    assert resp.status_code == 201

    data = resp.json()
    assert data == {
        'type': 'report',
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
    resp = app.get(f'/reports/{id_}')
    assert resp.json() == {
        'type': 'report',
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


def test_mongo_update_get(app):
    app.authorize([
        'spinta_report_insert',
        'spinta_report_update',
        'spinta_report_getone',
        'spinta_report_getall',
    ])

    resp = app.post('/reports', json={
        'type': 'report',
        'status': '42',
    })
    assert resp.status_code == 201

    # change report status
    data = resp.json()
    id_ = data['id']
    revision = data['revision']
    resp = app.put(f'/reports/{id_}', json={
        'revision': revision,
        'status': '13',
    })
    assert resp.status_code == 200

    data = resp.json()
    assert data['revision'] != revision

    revision = data['revision']
    assert data == {
        'type': 'report',
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
    resp = app.get(f'/reports/{id_}')
    data = resp.json()
    assert data == {
        'type': 'report',
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
    resp = app.get('/reports')
    data = resp.json()
    assert data == {
        'data': [
            {
                'type': 'report',
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


def test_put_non_existant_resource(app):
    resp = app.get('/reports/4e67-256f9a7388f88ccc502570f434f289e8-057553c2')
    assert resp.status_code == 404
    assert get_error_codes(resp.json()) == ["ModelNotFoundError"]

    resp = app.put('/reports/4e67-256f9a7388f88ccc502570f434f289e8-057553c2',
                   json={})
    assert resp.status_code == 404
    assert get_error_codes(resp.json()) == ["ModelNotFoundError"]


def test_get_non_existant_subresource(app):
    app.authorize([
        'spinta_report_insert',
        'spinta_report_getone',
    ])

    resp = app.post('/reports', json={
        'type': 'report',
        'status': '42',
    })
    assert resp.status_code == 201
    id_ = resp.json()['id']

    resp = app.get(f'/reports/{id_}/foo')
    assert resp.status_code == 404
    assert resp.json() == {"errors": [{
        "code": "HTTPException",
        "message": "Resource 'report' does not have property 'foo'.",
    }]}


def test_delete(context, app, tmpdir):
    result = context.push([
        {'type': 'report', 'status': '1'},
        {'type': 'report', 'status': '2'},
    ])
    ids = [x['id'] for x in result]

    # FIXME: `spinta_report_pdf_delete` gives access to:
    # DELETE /report/ID/pdf
    # DELETE /report/pdf/ID
    app.authorize([
        'spinta_report_getall',
        'spinta_report_delete',
        'spinta_report_pdf_delete',
        'spinta_report_pdf_update',
        'spinta_report_pdf_getone',
    ])

    pdf = pathlib.Path(tmpdir) / 'report.pdf'
    pdf.write_bytes(b'REPORTDATA')

    resp = app.put(f'/reports/{ids[0]}/pdf:ref', json={
        'content_type': 'application/pdf',
        'filename': str(pdf),
    })
    assert resp.status_code == 200

    resp = app.get('/report').json()
    data = [x['id'] for x in resp['data']]
    assert ids[0] in data
    assert ids[1] in data

    resp = app.delete(f'/report/{ids[0]}')
    assert resp.status_code == 204

    # multiple deletes should just return HTTP/404
    resp = app.delete(f'/report/{ids[0]}')
    assert resp.status_code == 404
    assert get_error_codes(resp.json()) == ["ResourceNotFoundError"]
    assert get_error_context(resp.json(), "ResourceNotFoundError") == {
        "id_": ids[0],
        "model": "report",
    }

    # subresourses should be deleted
    resp = app.delete(f'/report/{ids[0]}/pdf')
    assert resp.status_code == 404
    assert get_error_codes(resp.json()) == ["ResourceNotFoundError"]
    assert get_error_context(resp.json(), "ResourceNotFoundError") == {
        "id_": ids[0],
        "model": "report",
    }

    # FIXME: https://jira.tilaajavastuu.fi/browse/SPLAT-131
    # assert pdf.is_file() is False

    resp = app.get('/report').json()
    data = [x['id'] for x in resp['data']]
    assert ids[0] not in data
    assert ids[1] in data


def test_patch(app, context):
    app.authorize([
        'spinta_report_insert',
        'spinta_report_getone',
        'spinta_report_patch',
    ])

    report_data = app.post('/report', json={
            'type': 'report',
            'status': '1',
    }).json()
    id_ = report_data['id']

    resp = app.patch(f'/report/{id_}',
                     json={'status': '42'})
    assert resp.status_code == 200
    assert resp.json()['status'] == '42'
    revision = resp.json()['revision']

    # test that revision mismatch is checked
    resp = app.patch(f'/report/{id_}',
                     json={'revision': 'r3v1510n', 'status': '42'})
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ["ModelPropertyValueConflictError"]
    assert get_error_context(resp.json(), "ModelPropertyValueConflictError") == {
        "given_value": "r3v1510n",
        "existing_value": revision,
        "model": "report",
        "prop": "revision",
    }

    resp = app.patch(f'/report/{id_}',
                     json={'revision': '', 'status': '42'})
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ["ModelPropertyValueConflictError"]
    assert get_error_context(resp.json(), "ModelPropertyValueConflictError") == {
        "given_value": "",
        "existing_value": revision,
        "model": "report",
        "prop": "revision",
    }

    resp = app.patch(f'/report/{id_}',
                     json={'revision': None, 'status': '42'})
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ["ModelPropertyValueConflictError"]
    assert get_error_context(resp.json(), "ModelPropertyValueConflictError") == {
        "given_value": None,
        "existing_value": revision,
        "model": "report",
        "prop": "revision",
    }

    # test that type mismatch is checked
    resp = app.patch(f'/report/{id_}',
                     json={'type': 'country', 'revision': revision, 'status': '42'})
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ["ModelPropertyValueConflictError"]
    assert get_error_context(resp.json(), "ModelPropertyValueConflictError") == {
        "given_value": "country",
        "existing_value": "report",
        "model": "report",
        "prop": "type",
    }

    # test that id mismatch is checked
    resp = app.patch(f'/report/{id_}',
                     json={'id': '0007ddec-092b-44b5-9651-76884e6081b4', 'revision': revision, 'status': '42'})
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ["ModelPropertyValueConflictError"]
    assert get_error_context(resp.json(), "ModelPropertyValueConflictError") == {
        "given_value": "0007ddec-092b-44b5-9651-76884e6081b4",
        "existing_value": id_,
        "model": "report",
        "prop": "id",
    }

    # test that protected fields (id, type, revision) are accepted, but not PATCHED
    resp = app.patch(f'/report/{id_}',
                     json={
                         'id': id_,
                         'type': 'report',
                         'revision': revision,
                         'status': '42',
                     })
    assert resp.status_code == 200
    resp_data = resp.json()

    assert resp_data['id'] == id_
    assert resp_data['type'] == 'report'
    # new status patched
    assert resp_data['status'] == '42'
    # new revision created regardless of PATCH'ed JSON
    assert resp_data['revision'] != revision


def test_escaping_chars(app):
    app.authorize([
        'spinta_report_insert',
        'spinta_report_getone',
    ])

    resp = app.post('/report', json={
        'type': 'report',
        'status': 'application/json',
    })
    assert resp.status_code == 201
    data = resp.json()

    resp = app.get(f'/report/{data["id"]}')
    assert resp.status_code == 200
    assert resp.json()['status'] == 'application/json'
