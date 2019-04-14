import datetime

from spinta.utils.itertools import consume


def test_export_json(context, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))

    consume(context.push([
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

    assert app.get('country/:source/csv/:format/jsonl').text == (
        '{"type":"country\\/:source\\/csv","id":"025685077bbcf6e434a95b65b9a6f5fcef046861","code":"lv","title":"Latvia"}\n'
        '{"type":"country\\/:source\\/csv","id":"69a33b149af7a7eeb25026c8cdc09187477ffe21","code":"lt","title":"Lithuania"}\n'
    )
