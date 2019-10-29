import re
import datetime

import pytest


def test_export_ascii(app, mocker):
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
    data = resp.json()
    rev1 = data['_data'][0]['_revision']
    rev2 = data['_data'][2]['_revision']

    assert app.get('/country/:dataset/csv/:resource/countries/:format/ascii?sort(+code)').text == (
        '\n\n'
        'Table: country/:dataset/csv/:resource/countries\n'
        '_id                _revision                 code     title  \n'
        '=============================================================\n'
        f'1     {rev1}   lt     Lithuania\n'
        f'2     {rev2}   lv     Latvia   '
    )

    resp = app.get('/country/:dataset/csv/:resource/countries/:changes')
    changes = resp.json()['_data']
    ids = [c['_change'] for c in changes]
    txn = [c['_transaction'] for c in changes]
    rev = [c['_revision'] for c in changes]
    res = app.get('country/:dataset/csv/:resource/countries/:changes/:format/ascii').text
    res = re.sub(f' +', '    ', res)
    assert res == (
        '_id    _change    _revision    _transaction    _created    _op    code    title    \n'
        '============================================================================================================================\n'
        f'1    {ids[0]}    {rev[0]}    {txn[0]}    2019-03-06T16:15:00.816308    upsert    lt    Lithuania\n'
        f'2    {ids[1]}    {rev[1]}    {txn[1]}    2019-03-06T16:15:00.816308    upsert    lv    LATVIA    \n'
        f'2    {ids[2]}    {rev[2]}    {txn[2]}    2019-03-06T16:15:00.816308    upsert    None    Latvia    '
    )


@pytest.mark.asyncio
async def test_export_multiple_types(context):
    rows = [
        {'_type': 'a', 'value': 1},
        {'_type': 'a', 'value': 2},
        {'_type': 'a', 'value': 3},
        {'_type': 'b', 'value': 1},
        {'_type': 'b', 'value': 2},
        {'_type': 'b', 'value': 3},
        {'_type': 'c', 'value': 1},
        {'_type': 'c', 'value': 2},
        {'_type': 'c', 'value': 3},
    ]

    config = context.get('config')
    exporter = config.exporters['ascii']
    assert ''.join(exporter(rows)) == (
        '\n\n'
        'Table: a\n'
        'value\n'
        '=====\n'
        '1    \n'
        '2    \n'
        '3    \n'
        '\n'
        'Table: b\n'
        'value\n'
        '=====\n'
        '1    \n'
        '2    \n'
        '3    \n'
        '\n'
        'Table: c\n'
        'value\n'
        '=====\n'
        '1    \n'
        '2    \n'
        '3    '
    )
