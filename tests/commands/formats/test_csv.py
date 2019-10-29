import datetime


def test_export_csv(context, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))

    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('country/:dataset/csv/:resource/countries', ['upsert', 'getall', 'search', 'changes'])

    resp = app.post('/country/:dataset/csv/:resource/countries', json={'_data': [
        {
            '_op': 'upsert',
            '_type': 'country/:dataset/csv/:resource/countries',
            '_id': '1',
            '_where': '_id=string:1',
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

    data = app.get('/country/:dataset/csv/:resource/countries?sort(+code)').json()['_data']
    ids = [d['_id'] for d in data]
    rev = [d['_revision'] for d in data]
    assert app.get('/country/:dataset/csv/:resource/countries/:format/csv?sort(+code)').text == (
        '_type,_id,_revision,code,title\r\n'
        f'country/:dataset/csv/:resource/countries,{ids[0]},{rev[0]},lt,Lithuania\r\n'
        f'country/:dataset/csv/:resource/countries,{ids[1]},{rev[1]},lv,Latvia\r\n'
    )

    changes = app.get('/country/:dataset/csv/:resource/countries/:changes').json()['_data']
    ids = [c['_id'] for c in changes]
    cxn = [c['_change'] for c in changes]
    txn = [c['_transaction'] for c in changes]
    rev = [c['_revision'] for c in changes]
    assert app.get('/country/:dataset/csv/:resource/countries/:changes/:format/csv').text == (
        '_id,_change,_revision,_transaction,_created,_op,code,title\r\n'
        f'{ids[0]},{cxn[0]},{rev[0]},{txn[0]},2019-03-06T16:15:00.816308,upsert,lt,Lithuania\r\n'
        f'{ids[1]},{cxn[1]},{rev[1]},{txn[1]},2019-03-06T16:15:00.816308,upsert,lv,LATVIA\r\n'
        f'{ids[2]},{cxn[2]},{rev[2]},{txn[2]},2019-03-06T16:15:00.816308,upsert,,Latvia\r\n'
    )
