import datetime


def test_export_json(store, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))

    store.push([
        {
            'type': 'country/:source/csv',
            'id': 1,
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            'type': 'country/:source/csv',
            'id': 2,
            'code': 'lv',
            'title': 'LATVIA',
        },
        {
            'type': 'country/:source/csv',
            'id': 2,
            'code': 'lv',
            'title': 'Latvia',
        },
    ])

    assert app.get('country/:source/csv/:format/json').text == (
        '{"data":['
        '{"type":"country\\/:source\\/csv","id":"025685077bbcf6e434a95b65b9a6f5fcef046861","code":"lv","title":"Latvia"},'
        '{"type":"country\\/:source\\/csv","id":"69a33b149af7a7eeb25026c8cdc09187477ffe21","code":"lt","title":"Lithuania"}'
        ']}'
    )

    assert app.get('country/69a33b149af7a7eeb25026c8cdc09187477ffe21/:source/csv/:format/json').text == (
        '{"type":"country\\/:source\\/csv","id":"69a33b149af7a7eeb25026c8cdc09187477ffe21","code":"lt","title":"Lithuania"}'
    )

    changes = app.get('country/:source/csv/:changes/:format/json').json()['data']
    assert changes == [
        {
            'change_id': changes[0]['change_id'],
            'transaction_id': changes[0]['transaction_id'],
            'datetime': 1551888900,
            'action': 'insert',
            'id': '69a33b149af7a7eeb25026c8cdc09187477ffe21',
            'change': {'code': 'lt', 'title': 'Lithuania'},
        },
        {
            'change_id': changes[1]['change_id'],
            'transaction_id': changes[1]['transaction_id'],
            'datetime': 1551888900,
            'action': 'insert',
            'id': '025685077bbcf6e434a95b65b9a6f5fcef046861',
            'change': {'code': 'lv', 'title': 'LATVIA'},
        },
        {
            'change_id': changes[2]['change_id'],
            'transaction_id': changes[2]['transaction_id'],
            'datetime': 1551888900,
            'action': 'update',
            'id': '025685077bbcf6e434a95b65b9a6f5fcef046861',
            'change': {'title': 'Latvia'},
        },
    ]

    assert app.get('country/:source/csv/:changes/100/:format/json').json() == {
        'data': []
    }
