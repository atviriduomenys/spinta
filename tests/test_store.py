def test_schema_loader(context, app):
    country, = context.push([
        {
            'type': 'country',
            'code': 'lt',
            'title': 'Lithuania',
        },
    ])
    org, = context.push([
        {
            'type': 'org',
            'title': 'My Org',
            'govid': '0042',
            'country': country['id'],
        },
    ])

    assert country == {
        'id': country['id'],
        'type': 'country',
    }
    assert org == {
        'id': org['id'],
        'type': 'org',
    }

    app.authorize(['spinta_getone'])

    assert app.get(f'/org/{org["id"]}').json() == {
        'id': org['id'],
        'govid': '0042',
        'title': 'My Org',
        'country': country['id'],
        'type': 'org',
        # FIXME: revision should not be None.
        'revision': None,
    }

    assert app.get(f'/country/{country["id"]}').json() == {
        'id': country['id'],
        'code': 'lt',
        'title': 'Lithuania',
        'type': 'country',
        # FIXME: revision should not be None.
        'revision': None,
    }


def test_nested(app):
    app.authorize(['spinta_insert', 'spinta_getone'])

    resp = app.post('/nested', json={
        'type': 'nested',
        'some': [{'nested': {'structure': 'here'}}]
    })
    assert resp.status_code == 201
    data = resp.json()
    id_ = data['id']

    assert app.get(f'/nested/{id_}').json() == {
        'type': 'nested',
        'id': id_,
        # TODO: add nested structure support for PostgreSQL
        'some': [],
        'revision': None,
    }
