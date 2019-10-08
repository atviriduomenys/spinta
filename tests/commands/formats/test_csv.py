import datetime

from spinta.utils.itertools import consume


def test_export_csv(context, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))
    mocker.patch('spinta.backends.postgresql.dataset.get_new_id', return_value='REV')

    consume(context.push([
        {
            'type': 'country/:dataset/csv/:resource/countries',
            'id': '1',
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            'type': 'country/:dataset/csv/:resource/countries',
            'id': '2',
            'code': 'lv',
            'title': 'LATVIA',
        },
        {
            'type': 'country/:dataset/csv/:resource/countries',
            'id': '2',
            'code': 'lv',
            'title': 'Latvia',
        },
    ]))

    app.authorize([
        'spinta_country_dataset_csv_resource_countries_getall',
        'spinta_country_dataset_csv_resource_countries_search',
        'spinta_country_dataset_csv_resource_countries_changes',
    ])

    assert app.get('country/:dataset/csv/:resource/countries/:format/csv?sort(+code)').text == (
        'id,code,title,type,revision\r\n'
        '1,lt,Lithuania,country/:dataset/csv/:resource/countries,REV\r\n'
        '2,lv,Latvia,country/:dataset/csv/:resource/countries,REV\r\n'
    )

    changes = context.changes('country', dataset='csv', resource='countries')
    ids = [c['change_id'] for c in changes]
    txn = [c['transaction_id'] for c in changes]
    assert app.get('country/:dataset/csv/:resource/countries/:changes/:format/csv').text == (
        'change_id,transaction_id,id,datetime,action,change.code,change.title\r\n'
        f'{ids[0]},{txn[0]},1,2019-03-06T16:15:00.816308,insert,lt,Lithuania\r\n'
        f'{ids[1]},{txn[1]},2,2019-03-06T16:15:00.816308,insert,lv,LATVIA\r\n'
        f'{ids[2]},{txn[2]},2,2019-03-06T16:15:00.816308,patch,,Latvia\r\n'
    )
