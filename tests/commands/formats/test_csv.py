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

    assert app.get('country/:source/csv/:format/csv').text == (
        'type,id,code,title\r\n'
        'country/:source/csv,025685077bbcf6e434a95b65b9a6f5fcef046861,lv,Latvia\r\n'
        'country/:source/csv,69a33b149af7a7eeb25026c8cdc09187477ffe21,lt,Lithuania\r\n'
    )
