import sqlalchemy as sa

from unittest.mock import MagicMock

from spinta.utils.itertools import consume
from spinta.backends.postgresql import get_table_name


def test_get_table_name():
    ns = 'default'
    backend = MagicMock()
    backend.get.return_value = 42

    assert get_table_name(backend, 'internal', 'org') == 'org'
    assert get_table_name(backend, ns, 'org') == 'ORG_0042M'
    assert len(get_table_name(backend, ns, 'a' * 100)) == 63
    assert get_table_name(backend, ns, 'a' * 100)[-10:] == 'AAAA_0042M'
    assert get_table_name(backend, ns, 'some_/name/hėrę!') == 'SOME_NAME_HERE_0042M'


def test_changes(context):
    data, = list(context.push([{'type': 'country', 'code': 'lt', 'title': 'Lithuania'}]))
    consume(context.push([{'id': data['id'], 'type': 'country', 'title': "Lietuva"}]))
    consume(context.push([{'id': data['id'], 'type': 'country', 'code': 'lv', 'title': "Latvia"}]))

    backend = context.get('store').manifests['default'].backend
    txn = backend.tables['internal']['transaction'].main
    changes = backend.tables['default']['country'].changes
    with backend.transaction() as transaction:
        c = transaction.connection
        assert len(c.execute(sa.select([txn.c.id])).fetchall()) == 3
        assert list(map(dict, c.execute(
            sa.select([
                changes.c.id,
                changes.c.action,
                changes.c.change,
            ]).order_by(changes.c.transaction_id)
        ).fetchall())) == [
            {'id': int(data['id']), 'action': 'insert', 'change': {'code': 'lt', 'title': 'Lithuania'}},
            {'id': int(data['id']), 'action': 'update', 'change': {'title': 'Lietuva'}},
            {'id': int(data['id']), 'action': 'update', 'change': {'code': 'lv', 'title': 'Latvia'}},
        ]


def test_show_with_joins(context):
    consume(context.push([
        {
            'type': 'continent/:source/dependencies',
            'id': '1',
            'title': 'Europe',
        },
        {
            'type': 'country/:source/dependencies',
            'id': 1,
            'title': 'Lithuania',
            'continent': '1',
        },
        {
            'type': 'capital/:source/dependencies',
            'id': 1,
            'title': 'Vilnius',
            'country': '1',
        },
    ]))

    result = context.getall('capital', dataset='dependencies', show=[
        'id',
        'title',
        'country.title',
        'country.continent.title',
    ])

    assert result == [
        {
            'country.continent.title': 'Europe',
            'country.title': 'Lithuania',
            'title': 'Vilnius',
            'id': '69a33b149af7a7eeb25026c8cdc09187477ffe21',
        },
    ]
