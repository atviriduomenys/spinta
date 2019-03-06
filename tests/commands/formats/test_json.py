def test_export_csv(store, app):
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
