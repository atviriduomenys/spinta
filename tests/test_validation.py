def test_report(context, app):
    app.authorize([
        'spinta_report_insert',
        'spinta_report_getone',
    ])

    resp = app.post('/report', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'count': '42',
        'valid_from_date': '2019-04-20',
        'update_time': '2019-04-20 03:14:15',
        'notes': [{'note': 'hello report', 'note_type': 'test'}]
    })
    assert resp.status_code == 201

    data = resp.json()
    id = data['id']
    resp = app.get(f'/report/{id}')
    assert resp.status_code == 200

    # FIXME: should return date/datetime strings instead of unix timestamps?
    assert resp.json() == {
        'id': id,
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'count': 42,
        'valid_from_date': 1555718400,
        'update_time': 1555730055,
        'notes': [{'note': 'hello report', 'note_type': 'test'}],
    }


def test_invalid_report_int(context, app):
    app.authorize([
        'spinta_report_insert',
    ])

    resp = app.post('/report', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'count': 'c0unt',  # invalid conversion to int
    })

    assert resp.status_code == 400
    assert resp.json() == {
        "error": "invalid literal for int() with base 10: 'c0unt'",
    }


def test_invalid_report_date(context, app):
    app.authorize([
        'spinta_report_insert',
    ])

    resp = app.post('/report', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'valid_from_date': '2019-04',  # invalid conversion to date
    })

    assert resp.status_code == 400
    assert resp.json() == {
        "error": "Invalid isoformat string: '2019-04'",
    }


def test_non_string_report_date(context, app):
    app.authorize([
        'spinta_report_insert',
    ])

    resp = app.post('/report', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'valid_from_date': 42,  # invalid conversion to date
    })

    assert resp.status_code == 400
    assert resp.json() == {
        "error": "fromisoformat: argument must be str",
    }


def test_invalid_report_datetime(context, app):
    app.authorize([
        'spinta_report_insert',
    ])

    resp = app.post('/report', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'update_time': '2019-04',  # invalid conversion to datetime
    })

    assert resp.status_code == 400
    assert resp.json() == {
        "error": "Invalid isoformat string: '2019-04'",
    }


def test_non_string_report_datetime(context, app):
    app.authorize([
        'spinta_report_insert',
    ])

    resp = app.post('/report', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'update_time': 42,  # invalid conversion to datetime
    })

    assert resp.status_code == 400
    assert resp.json() == {
        "error": "fromisoformat: argument must be str",
    }


def test_invalid_report_array(context, app):
    app.authorize([
        'spinta_report_insert',
    ])

    resp = app.post('/report', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'notes': {'foo': 'bar'},  # invalid conversion to array
    })

    assert resp.status_code == 400
    assert resp.json() == {
        "error": "Invalid array type: <class 'dict'>",
    }


def test_invalid_report_array_object(context, app):
    app.authorize([
        'spinta_report_insert',
    ])

    resp = app.post('/report', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'notes': ['hello', 'world'],  # invalid array item type
    })

    assert resp.status_code == 400
    assert resp.json() == {
        "error": "Invalid object type: <class 'str'>",
    }


def test_invalid_nested_object_property(context, app):
    app.authorize([
        'spinta_nested_insert',
    ])

    resp = app.post('/nested', json={
        'some': [{
            'nested': 'object' # invalid object property type
        }]
    })

    assert resp.status_code == 400
    assert resp.json() == {
        "error": "Invalid object type: <class 'str'>",
    }
