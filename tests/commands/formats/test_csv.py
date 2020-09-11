from spinta.testing.csv import parse_csv


def test_export_csv(app):
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('datasets/csv/country', [
        'insert',
        'patch',
        'getall',
        'search',
        'changes',
    ])

    resp = app.post('/datasets/csv/country', json={'_data': [
        {
            '_op': 'insert',
            '_type': 'datasets/csv/country',
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            '_op': 'insert',
            '_type': 'datasets/csv/country',
            'code': 'lv',
            'title': 'LATVIA',
        },
    ]})
    assert resp.status_code == 200, resp.json()
    data = resp.json()['_data']
    lv = data[1]
    resp = app.patch(f'/datasets/csv/country/{lv["_id"]}/', json={
        '_revision': lv['_revision'],
        'title': 'Latvia',
    })
    assert resp.status_code == 200, resp.json()

    assert app.get(
        '/datasets/csv/country/:format/csv?select(code,title)&sort(+code)'
    ).text == (
        'code,title\r\n'
        'lt,Lithuania\r\n'
        'lv,Latvia\r\n'
    )

    resp = app.get('/datasets/csv/country/:changes/:format/csv')
    assert resp.status_code == 200
    assert resp.headers['content-disposition'] == 'attachment; filename="country.csv"'
    header, *lines = resp.text.splitlines()
    assert header.split(',') == [
        '_id',
        '_revision',
        '_txn',
        '_rid',
        '_created',
        '_op',
        'code',
        'title',
    ]
    lines = [x.split(',')[-2:] for x in lines]
    assert lines == [
        ['lt', 'Lithuania'],
        ['lv', 'LATVIA'],
        ['', 'Latvia'],
    ]


def test_csv_limit(app):
    app.authmodel('country', ['insert', 'search', ])
    resp = app.post('/country', json={'_data': [
        {'_op': 'insert', '_type': 'country', 'code': 'lt', 'title': 'Lithuania'},
        {'_op': 'insert', '_type': 'country', 'code': 'lv', 'title': 'Latvia'},
        {'_op': 'insert', '_type': 'country', 'code': 'ee', 'title': 'Estonia'},
    ]})
    assert resp.status_code == 200, resp.json()

    resp = app.get('/country/:format/csv?select(code,title)&sort(code)&limit(1)')
    assert parse_csv(resp) == [
        ['code', 'title'],
        ['ee', 'Estonia'],
    ]

