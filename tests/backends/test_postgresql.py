import sqlalchemy as sa

from unittest.mock import MagicMock

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


def test_changes(store):
    data, = store.push([{'type': 'country', 'code': 'lt', 'title': 'Lithuania'}])
    store.push([{**data, 'title': "Lietuva"}])
    store.push([{**data, 'code': 'lv', 'title': "Latvia"}])

    backend = store.config.backends['default']
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
            {'id': data['id'], 'action': 'insert', 'change': {'code': 'lt', 'title': 'Lithuania'}},
            {'id': data['id'], 'action': 'update', 'change': {'title': 'Lietuva'}},
            {'id': data['id'], 'action': 'update', 'change': {'code': 'lv', 'title': 'Latvia'}},
        ]
