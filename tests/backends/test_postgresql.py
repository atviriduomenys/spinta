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

    resp = app.delete(f'/country/{ids[0]}')
    assert resp.status_code == 204

    # multiple deletes should just return HTTP/404
    resp = app.delete(f'/country/{ids[0]}')
    assert resp.status_code == 404
    assert get_error_codes(resp.json()) == ["ResourceNotFound"]
    assert get_error_context(
        resp.json(),
        "ResourceNotFound",
        ["manifest", "model", "id"],
    ) == {
        'manifest': 'default',
        'model': 'country',
        'id': ids[0],
    }

    resp = app.get('/country').json()
    data = [x['id'] for x in resp['data']]
    assert ids[0] not in data
    assert ids[1] in data


def test_patch(app, context):
    app.authorize([
        'spinta_country_insert',
        'spinta_country_getone',
        'spinta_org_insert',
        'spinta_org_getone',
        'spinta_org_patch',
    ])

    country_data = app.post('/country', json={
        'type': 'country',
        'code': 'lt',
        'title': 'Lithuania',
    }).json()
    org_data = app.post('/org', json={
            'type': 'org',
            'title': 'My Org',
            'govid': '0042',
            'country': country_data['id'],
    }).json()
    id_ = org_data['id']

    resp = app.patch(f'/org/{org_data["id"]}',
                     json={'title': 'foo org'})
    assert resp.status_code == 200
    assert resp.json()['title'] == 'foo org'
    revision = resp.json()['revision']

    # test that revision mismatch is checked
    resp = app.patch(f'/org/{org_data["id"]}',
                     json={'revision': 'r3v1510n', 'title': 'foo org'})
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ["ConflictingValue"]
    assert get_error_context(
        resp.json(),
        "ConflictingValue",
        ["given", "expected", "model"],
    ) == {
        'given': 'r3v1510n',
        'expected': revision,
        'model': 'org',
    }

    # test that type mismatch is checked
    resp = app.patch(f'/org/{org_data["id"]}',
                     json={'type': 'country', 'revision': org_data["revision"], 'title': 'foo org'})
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ["ConflictingValue"]
    assert get_error_context(
        resp.json(),
        "ConflictingValue",
        ["given", "expected", "model"],
    ) == {
        'given': 'country',
        'expected': 'org',
        'model': 'org',
    }

    # test that id mismatch is checked
    resp = app.patch(f'/org/{org_data["id"]}',
                     json={'id': '0007ddec-092b-44b5-9651-76884e6081b4', 'revision': org_data["revision"], 'title': 'foo org'})
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ["ConflictingValue"]
    assert get_error_context(
        resp.json(),
        "ConflictingValue",
        ["given", "expected", "model"],
    ) == {
        'given': '0007ddec-092b-44b5-9651-76884e6081b4',
        'expected': id_,
        'model': 'org',
    }

    # test that protected fields (id, type, revision) are accepted, but not PATCHED
    resp = app.patch(f'/org/{org_data["id"]}', json={
        'id': id_,
        'type': 'org',
        'revision': revision,
        'title': 'foo org',
    })
    assert resp.status_code == 200
    resp_data = resp.json()

    assert resp_data['id'] == id_
    assert resp_data['type'] == 'org'
    # new title patched
    assert resp_data['title'] == 'foo org'
    # new revision created regardless of PATCH'ed JSON
    assert resp_data['revision'] != revision
