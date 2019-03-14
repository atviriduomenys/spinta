import datetime

from spinta.utils.itertools import consume


def test_export_asciitable(store, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))

    consume(store.push([
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
    ]))

    assert app.get('country/:source/csv/:format/asciitable').text == (
        '\n\n'
        'Table: country/:source/csv\n'
        '                   id                      code     title  \n'
        '===========================================================\n'
        '025685077bbcf6e434a95b65b9a6f5fcef046861   lv     Latvia   \n'
        '69a33b149af7a7eeb25026c8cdc09187477ffe21   lt     Lithuania'
    )

    changes = list(store.changes({
        'path': 'country',
        'source': 'csv',
        'changes': None,
    }))
    ids = [c['change_id'] for c in changes]
    txn = [c['transaction_id'] for c in changes]
    assert app.get('country/:source/csv/:changes/:format/asciitable').text == (
        'change_id   transaction_id                      id                               datetime            action   change.code   change.title\n'
        '========================================================================================================================================\n'
        f'{ids[0]:<3}         {txn[0]:<3}              69a33b149af7a7eeb25026c8cdc09187477ffe21   2019-03-06 16:15:00.816308   insert   lt            Lithuania   \n'
        f'{ids[1]:<3}         {txn[0]:<3}              025685077bbcf6e434a95b65b9a6f5fcef046861   2019-03-06 16:15:00.816308   insert   lv            LATVIA      \n'
        f'{ids[2]:<3}         {txn[0]:<3}              025685077bbcf6e434a95b65b9a6f5fcef046861   2019-03-06 16:15:00.816308   update   None          Latvia      '
    )


def test_export_multiple_types(store):
    rows = [
        {'type': 'a', 'value': 1},
        {'type': 'a', 'value': 2},
        {'type': 'a', 'value': 3},
        {'type': 'b', 'value': 1},
        {'type': 'b', 'value': 2},
        {'type': 'b', 'value': 3},
        {'type': 'c', 'value': 1},
        {'type': 'c', 'value': 2},
        {'type': 'c', 'value': 3},
    ]
    assert ''.join(store.export(rows, 'asciitable')) == (
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
