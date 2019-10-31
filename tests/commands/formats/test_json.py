import datetime


def test_export_json(app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))

    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('country/:dataset/csv/:resource/countries', ['upsert', 'getall', 'search', 'getone', 'changes'])

    resp = app.post('/country/:dataset/csv/:resource/countries', json={'_data': [
        {
            '_op': 'upsert',
            '_type': 'country/:dataset/csv/:resource/countries',
            '_id': '69a33b149af7a7eeb25026c8cdc09187477ffe21',
            '_where': '_id=69a33b149af7a7eeb25026c8cdc09187477ffe21',
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            '_op': 'upsert',
            '_type': 'country/:dataset/csv/:resource/countries',
            '_id': '2',
            '_where': '_id=string:2',
            'code': 'lv',
            'title': 'LATVIA',
        },
        {
            '_op': 'upsert',
            '_type': 'country/:dataset/csv/:resource/countries',
            '_id': '2',
            '_where': '_id=string:2',
            'code': 'lv',
            'title': 'Latvia',
        },
    ]})
    assert resp.status_code == 200, resp.json()
    data = resp.json()
    revs = [d['_revision'] for d in data['_data']]

    assert app.get('/country/:dataset/csv/:resource/countries/:format/json?sort(+code)').json() == {
        '_data': [
            {
                '_type': 'country/:dataset/csv/:resource/countries',
                '_id': '69a33b149af7a7eeb25026c8cdc09187477ffe21',
                '_revision': revs[0],
                'code': 'lt',
                'title': 'Lithuania',
            },
            {
                '_type': 'country/:dataset/csv/:resource/countries',
                '_id': '2',
                '_revision': revs[2],
                'code': 'lv',
                'title': 'Latvia',
            },
        ],
    }

    assert app.get('/country/69a33b149af7a7eeb25026c8cdc09187477ffe21/:dataset/csv/:resource/countries/:format/json').json() == {
        '_type': 'country/:dataset/csv/:resource/countries',
        '_id': '69a33b149af7a7eeb25026c8cdc09187477ffe21',
        '_revision': revs[0],
        'title': 'Lithuania',
        'code': 'lt',
    }

    changes = app.get('/country/:dataset/csv/:resource/countries/:changes/:format/json').json()['_data']
    assert changes == [
        {
            '_change': changes[0]['_change'],
            '_revision': changes[0]['_revision'],
            '_transaction': changes[0]['_transaction'],
            '_created': '2019-03-06T16:15:00.816308',
            '_op': 'upsert',
            '_id': '69a33b149af7a7eeb25026c8cdc09187477ffe21',
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            '_change': changes[1]['_change'],
            '_revision': changes[1]['_revision'],
            '_transaction': changes[1]['_transaction'],
            '_created': '2019-03-06T16:15:00.816308',
            '_op': 'upsert',
            '_id': '2',
            'code': 'lv',
            'title': 'LATVIA',
        },
        {
            '_change': changes[2]['_change'],
            '_revision': changes[2]['_revision'],
            '_transaction': changes[2]['_transaction'],
            '_created': '2019-03-06T16:15:00.816308',
            '_op': 'upsert',
            '_id': '2',
            'title': 'Latvia',
        },
    ]

    assert app.get('country/:dataset/csv/:resource/countries/:changes/1000/:format/json').json() == {
        '_data': []
    }
