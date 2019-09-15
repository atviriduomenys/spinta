from spinta.testing.utils import get_error_codes, get_error_context


def test_report(app):
    app.authorize([
        'spinta_report_insert',
        'spinta_report_getone',
    ])

    resp = app.post('/reports', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'count': 42,
        'valid_from_date': '2019-04-20',
        'update_time': '2019-04-20 03:14:15',
        'notes': [{
            'note': 'hello report',
            'note_type': 'test',
            'create_date': '2019-04-20',
        }],
        'operating_licenses': [],
    })
    assert resp.status_code == 201

    data = resp.json()
    id = data['id']
    resp = app.get(f'/reports/{id}')
    assert resp.status_code == 200

    data = resp.json()
    assert data == {
        'id': id,
        'revision': data['revision'],
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'count': 42,
        'valid_from_date': '2019-04-20',
        'update_time': '2019-04-20T03:14:15',
        'notes': [{
            'note': 'hello report',
            'note_type': 'test',
            'create_date': '2019-04-20',
        }],
        'operating_licenses': [],
    }


def test_invalid_report_int(app):
    app.authorize([
        'spinta_report_insert',
    ])

    resp = app.post('/reports', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'count': 'c0unt',  # invalid conversion to int
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]
    assert get_error_context(resp.json(), "InvalidValue") == {
        'schema': 'tests/manifest/models/report.yml',
        'manifest': 'default',
        'model': 'report',
        'property': 'count',
        'type': 'integer',
    }


def test_invalid_report_date(app):
    app.authorize([
        'spinta_report_insert',
    ])

    resp = app.post('/reports', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'valid_from_date': '2019-04',  # invalid conversion to date
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]


def test_non_string_report_date(app):
    app.authorize([
        'spinta_report_insert',
    ])

    resp = app.post('/reports', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'valid_from_date': 42,  # invalid conversion to date
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]


def test_invalid_report_datetime(app):
    app.authorize([
        'spinta_report_insert',
    ])

    resp = app.post('/reports', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'update_time': '2019-04',  # invalid conversion to datetime
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]


def test_non_string_report_datetime(app):
    app.authorize([
        'spinta_report_insert',
    ])

    resp = app.post('/reports', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'update_time': 42,  # invalid conversion to datetime
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]


def test_invalid_report_array(app):
    app.authorize([
        'spinta_report_insert',
    ])

    resp = app.post('/reports', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'notes': {'foo': 'bar'},  # invalid conversion to array
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]


def test_invalid_report_array_object(app):
    app.authorize([
        'spinta_report_insert',
    ])

    resp = app.post('/reports', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'notes': ['hello', 'world'],  # invalid array item type
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]


def test_invalid_nested_object_property(app):
    app.authorize([
        'spinta_nested_insert',
    ])

    resp = app.post('/nested', json={
        'some': [{
            'nested': 'object' # invalid object property type
        }]
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]


def test_missing_report_object_property(app):
    app.authorize([
        'spinta_report_insert',
        'spinta_report_getone',
    ])

    resp = app.post('/reports', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'count': 42,
        'valid_from_date': '2019-04-20',
        'update_time': '2019-04-20 03:14:15',
        'notes': [{
            'note': 'hello report',  # missing object properties
        }],
    })

    assert resp.status_code == 201


def test_unknown_report_property(app):
    app.authorize([
        'spinta_report_insert',
        'spinta_report_getone',
    ])

    resp = app.post('/reports', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'count': '42',
        'valid_from_date': '2019-04-20',
        'update_time': '2019-04-20 03:14:15',
        'notes': [{
            'note': 'hello report',
            'note_type': 'test',
            'create_date': '2014-20',
        }],
        'random_prop': 'foo',
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["UnknownProperty"]
    assert get_error_context(resp.json(), "UnknownProperty") == {
        'schema': 'tests/manifest/models/report.yml',
        'manifest': 'default',
        'model': 'report',
        'property': 'random_prop',
    }


def test_unknown_report_object_property(app):
    app.authorize([
        'spinta_report_insert',
        'spinta_report_getone',
    ])

    resp = app.post('/reports', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'count': 42,
        'valid_from_date': '2019-04-20',
        'update_time': '2019-04-20 03:14:15',
        'notes': [{
            'note': 'hello report',
            'rand_prop': 42,  # unknown object properties
        }],
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["UnknownProperty"]
    assert get_error_context(resp.json(), "UnknownProperty") == {
        'schema': 'tests/manifest/models/report.yml',
        'manifest': 'default',
        'model': 'report',
        'property': 'notes.rand_prop',
    }
