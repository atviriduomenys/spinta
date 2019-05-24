import datetime

from spinta.utils.itertools import consume


def test_export_csv(context, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))

    consume(context.push([
        {
            'type': 'country/:ds/csv/:rs/countries',
            'id': 1,
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            'type': 'country/:ds/csv/:rs/countries',
            'id': 2,
            'code': 'lv',
            'title': 'LATVIA',
        },
        {
            'type': 'country/:ds/csv/:rs/countries',
            'id': 2,
            'code': 'lv',
            'title': 'Latvia',
        },
    ]))

    app.authorize([
        'spinta_country_ds_csv_rs_countries_getall',
        'spinta_country_ds_csv_rs_countries_changes',
    ])

    assert app.get('country/:ds/csv/:rs/countries/:format/csv').text == (
        'type,id,code,title\r\n'
        'country/:ds/csv/:rs/countries,025685077bbcf6e434a95b65b9a6f5fcef046861,lv,Latvia\r\n'
        'country/:ds/csv/:rs/countries,69a33b149af7a7eeb25026c8cdc09187477ffe21,lt,Lithuania\r\n'
    )

    changes = context.changes('country', dataset='csv', resource='countries')
    ids = [c['change_id'] for c in changes]
    txn = [c['transaction_id'] for c in changes]
    assert app.get('country/:ds/csv/:rs/countries/:changes/:format/csv').text == (
        'change_id,transaction_id,id,datetime,action,change.code,change.title\r\n'
        f'{ids[0]},{txn[0]},69a33b149af7a7eeb25026c8cdc09187477ffe21,2019-03-06 16:15:00.816308,insert,lt,Lithuania\r\n'
        f'{ids[1]},{txn[1]},025685077bbcf6e434a95b65b9a6f5fcef046861,2019-03-06 16:15:00.816308,insert,lv,LATVIA\r\n'
        f'{ids[2]},{txn[2]},025685077bbcf6e434a95b65b9a6f5fcef046861,2019-03-06 16:15:00.816308,update,,Latvia\r\n'
    )
