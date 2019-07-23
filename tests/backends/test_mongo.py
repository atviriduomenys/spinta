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
            },
        ]
    }


def test_delete(context, app):
    result = context.push([
        {'type': 'report', 'status': '1'},
        {'type': 'report', 'status': '2'},
    ])
    ids = [x['id'] for x in result]

    app.authorize([
        'spinta_report_getall',
        'spinta_report_delete',
    ])

    resp = app.get('/report').json()
    data = [x['id'] for x in resp['data']]
    assert ids[0] in data
    assert ids[1] in data

    resp = app.delete(f'/report/{ids[0]}')
    assert resp.status_code == 204

    resp = app.get('/report').json()
    data = [x['id'] for x in resp['data']]
    assert ids[0] not in data
    assert ids[1] in data


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
