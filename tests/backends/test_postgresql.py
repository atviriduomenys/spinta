import sqlalchemy as sa

from unittest.mock import MagicMock

from spinta.backends.postgresql import get_table_name
from spinta.testing.utils import get_error_codes, get_error_context


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
    context.push([{'id': data['id'], 'type': 'country', 'title': "Lietuva"}])
    context.push([{'id': data['id'], 'type': 'country', 'code': 'lv', 'title': "Latvia"}])

    backend = context.get('store').manifests['default'].backend
    txn = backend.tables['internal']['transaction'].main
    changes = backend.tables['default']['country'].changes
    with backend.transaction() as transaction:
        c = transaction.connection
        assert len(c.execute(sa.select([txn.c.id])).fetchall()) == 3
        result = list(map(dict, c.execute(
            sa.select([
                changes.c.id,
                changes.c.action,
                changes.c.change,
            ]).order_by(changes.c.transaction_id)
        ).fetchall()))
        assert result == [
            {'id': data['id'], 'action': 'insert', 'change': {'code': 'lt', 'revision': result[0]['change']['revision'], 'title': 'Lithuania'}},
            {'id': data['id'], 'action': 'patch', 'change': {'revision': result[1]['change']['revision'], 'title': 'Lietuva'}},
            {'id': data['id'], 'action': 'patch', 'change': {'code': 'lv', 'revision': result[2]['change']['revision'], 'title': 'Latvia'}},
        ]


def test_show_with_joins(context):
    context.push([
        {
            'type': 'continent/:dataset/dependencies/:resource/continents',
            'id': '1',
            'title': 'Europe',
        },
        {
            'type': 'country/:dataset/dependencies/:resource/continents',
            'id': '1',
            'title': 'Lithuania',
            'continent': '1',
        },
        {
            'type': 'capital/:dataset/dependencies/:resource/continents',
            'id': '1',
            'title': 'Vilnius',
            'country': '1',
        },
    ])

    result = context.getall('capital', dataset='dependencies', resource='continents', show=[
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
            'id': '1',
        },
    ]
