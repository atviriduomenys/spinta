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
    context.push([{'id': data['id'], 'type': 'country', 'title': "Lietuva"}])
    context.push([{'id': data['id'], 'type': 'country', 'code': 'lv', 'title': "Latvia"}])

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
            {'id': data['id'], 'action': 'insert', 'change': {'code': 'lt', 'title': 'Lithuania'}},
            {'id': data['id'], 'action': 'patch', 'change': {'title': 'Lietuva'}},
            {'id': data['id'], 'action': 'patch', 'change': {'code': 'lv', 'title': 'Latvia'}},
        ]


def test_show_with_joins(context):
    context.push([
        {
            'type': 'continent/:ds/dependencies/:rs/continents',
            'id': '1',
            'title': 'Europe',
        },
        {
            'type': 'country/:ds/dependencies/:rs/continents',
            'id': '1',
            'title': 'Lithuania',
            'continent': '1',
        },
        {
            'type': 'capital/:ds/dependencies/:rs/continents',
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


def test_delete(context, app):
    result = context.push([
        {'type': 'country', 'code': 'fi', 'title': 'Finland'},
        {'type': 'country', 'code': 'lt', 'title': 'Lithuania'},
    ])
    ids = [x['id'] for x in result]

    app.authorize([
        'spinta_country_getall',
        'spinta_country_delete',
    ])

    resp = app.get('/country').json()
    data = [x['id'] for x in resp['data']]
    assert ids[0] in data
    assert ids[1] in data

    resp = app.delete(f'/country/{ids[0]}').json()

    resp = app.get('/country').json()
    data = [x['id'] for x in resp['data']]
    assert ids[0] not in data
    assert ids[1] in data
