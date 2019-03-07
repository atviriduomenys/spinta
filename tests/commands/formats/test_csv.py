import datetime


def test_export_csv(store, app, mocker):
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

    assert app.get('country/:source/csv/:format/csv').text == (
        'type,id,code,title\r\n'
        'country/:source/csv,025685077bbcf6e434a95b65b9a6f5fcef046861,lv,Latvia\r\n'
        'country/:source/csv,69a33b149af7a7eeb25026c8cdc09187477ffe21,lt,Lithuania\r\n'
    )

    changes = list(store.changes({
        'path': 'country',
        'source': 'csv',
        'changes': None,
    }))
    ids = [c['change_id'] for c in changes]
    txn = [c['transaction_id'] for c in changes]
    assert app.get('country/:source/csv/:changes/:format/csv').text == (
        'change_id,transaction_id,id,datetime,action,change.code,change.title\r\n'
        f'{ids[0]},{txn[0]},69a33b149af7a7eeb25026c8cdc09187477ffe21,2019-03-06 16:15:00.816308,insert,lt,Lithuania\r\n'
        f'{ids[1]},{txn[1]},025685077bbcf6e434a95b65b9a6f5fcef046861,2019-03-06 16:15:00.816308,insert,lv,LATVIA\r\n'
        f'{ids[2]},{txn[2]},025685077bbcf6e434a95b65b9a6f5fcef046861,2019-03-06 16:15:00.816308,update,,Latvia\r\n'
    )
