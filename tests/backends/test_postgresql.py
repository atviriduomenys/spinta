import sqlalchemy as sa

from unittest.mock import Mock, MagicMock

from spinta.backends.postgresql import Prepare


def test_get_table_name():
    backend = MagicMock()
    backend.get.return_value = 42
    cmd = Prepare(None, None, None, None, backend=backend)
    model = Mock()

    model.name = 'org'
    assert cmd.get_table_name(model) == 'ORG_0042M'

    model.name = 'a' * 100
    assert len(cmd.get_table_name(model)) == 63
    assert cmd.get_table_name(model)[-10:] == 'AAAA_0042M'

    model.name = 'some_/name/hėrę!'
    assert cmd.get_table_name(model) == 'SOME_NAME_HERE_0042M'


def test_changes(store):
    data, = store.push([{'type': 'country', 'code': 'lt', 'title': 'Lithuania'}])
    store.push([{**data, 'title': "Lietuva"}])
    store.push([{**data, 'code': 'lv', 'title': "Latvia"}])

    backend = store.config.backends['default']
    txn = backend.tables['transaction'].main
    changes = backend.tables['country'].changes
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
