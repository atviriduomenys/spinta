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


def test_changes(app):
    app.authmodel('country', ['insert', 'update', 'changes'])
    data = app.post('/country', json={'_type': 'country', 'code': 'lt', 'title': "Lithuania"}).json()
    app.put(f'/country/{data["_id"]}', json={'_type': 'country', '_id': data['_id'], 'title': "Lietuva"})
    app.put(f'/country/{data["_id"]}', json={'type': 'country', '_id': data['_id'], 'code': 'lv', 'title': "Latvia"})
    app.get(f'/country/{data["_id"]}/:changes').json() == {}


def test_show_with_joins(context, app):
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('continent/:dataset/dependencies/:resource/continents', ['insert'])
    app.authmodel('country/:dataset/dependencies/:resource/continents', ['insert'])
    app.authmodel('capital/:dataset/dependencies/:resource/continents', ['insert', 'search'])

    app.post('/', json={
        '_data': [
            {
                '_type': 'continent/:dataset/dependencies/:resource/continents',
                '_op': 'insert',
                '_id': '1',
                'title': 'Europe',
            },
            {
                '_type': 'country/:dataset/dependencies/:resource/continents',
                '_op': 'insert',
                '_id': '1',
                'title': 'Lithuania',
                'continent': '1',
            },
            {
                '_type': 'capital/:dataset/dependencies/:resource/continents',
                '_op': 'insert',
                '_id': '1',
                'title': 'Vilnius',
                'country': '1',
            },
        ]
    })

    # XXX: Maybe we should require `search` scope also for linked models? Now,
    #      we only have access to `continent`, but using foreign keys, we can
    #      also access country and continent.
    resp = app.get('/capital/:dataset/dependencies/:resource/continents?select(id,title,country.title,country.continent.title)')
    assert resp.json() == {
        '_data': [
            {
                'country.continent.title': 'Europe',
                'country.title': 'Lithuania',
                'title': 'Vilnius',
                'id': '1',
            },
        ],
    }


def test_delete(context, app):
    app.authmodel('country', ['insert', 'getall', 'delete'])

    resp = app.post('/', json={
        '_data': [
            {'_op': 'insert', '_type': 'country', 'code': 'fi', 'title': 'Finland'},
            {'_op': 'insert', '_type': 'country', 'code': 'lt', 'title': 'Lithuania'},
        ],
    })
    ids = [x['_id'] for x in resp.json()['_data']]
    revs = [x['_revision'] for x in resp.json()['_data']]

    resp = app.get('/country').json()
    data = [x['_id'] for x in resp['_data']]
    assert ids[0] in data
    assert ids[1] in data

    resp = app.delete(f'/country/{ids[0]}', json={
        '_revision': revs[0],
    })
    assert resp.status_code == 204

    # multiple deletes should just return HTTP/404
    resp = app.delete(f'/country/{ids[0]}')
    assert resp.status_code == 404
    assert get_error_codes(resp.json()) == ['ItemDoesNotExist']
    assert get_error_context(resp.json(), 'ItemDoesNotExist', ['manifest', 'model', 'id']) == {
        'manifest': 'default',
        'model': 'country',
        'id': ids[0],
    }

    resp = app.get('/country').json()
    data = [x['_id'] for x in resp['_data']]
    assert ids[0] not in data
    assert ids[1] in data


def test_patch(app):
    app.authorize([
        'spinta_set_meta_fields',
        'spinta_country_insert',
        'spinta_country_getone',
        'spinta_org_insert',
        'spinta_org_getone',
        'spinta_org_patch',
    ])

    country_data = app.post('/country', json={
        '_type': 'country',
        'code': 'lt',
        'title': 'Lithuania',
    }).json()
    org_data = app.post('/org', json={
        '_type': 'org',
        'title': 'My Org',
        'govid': '0042',
        'country': country_data['_id'],
    }).json()
    id_ = org_data['_id']

    resp = app.patch(f'/org/{org_data["_id"]}', json={
        '_revision': org_data['_revision'],
        'title': 'foo org',
    })
    assert resp.status_code == 200
    assert resp.json()['title'] == 'foo org'
    revision = resp.json()['_revision']
    assert org_data['_revision'] != revision

    # test that revision mismatch is checked
    resp = app.patch(f'/org/{org_data["_id"]}', json={
        '_revision': 'r3v1510n',
        'title': 'foo org',
    })
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ["ConflictingValue"]
    assert get_error_context(resp.json(), "ConflictingValue", ["given", "expected", "model"]) == {
        'given': 'r3v1510n',
        'expected': revision,
        'model': 'org',
    }

    # test that type mismatch is checked
    resp = app.patch(f'/org/{org_data["_id"]}', json={
        '_type': 'country',
        '_revision': org_data['_revision'],
        'title': 'foo org',
    })
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ['ConflictingValue']
    assert get_error_context(resp.json(), 'ConflictingValue', ['given', 'expected', 'model']) == {
        'given': 'country',
        'expected': 'org',
        'model': 'org',
    }

    # test that id mismatch is checked
    resp = app.patch(f'/org/{org_data["_id"]}', json={
        '_id': '0007ddec-092b-44b5-9651-76884e6081b4',
        '_revision': revision,
        'title': 'foo org',
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data['_revision'] != revision
    assert data == {
        '_type': 'org',
        '_id': '0007ddec-092b-44b5-9651-76884e6081b4',
        '_revision': data['_revision'],
    }
    id_ = data['_id']
    revision = data['_revision']

    # patch using same values as already stored in database
    resp = app.patch(f'/org/{id_}', json={
        '_id': id_,
        '_type': 'org',
        '_revision': revision,
        'title': 'foo org',
    })
    assert resp.status_code == 200
    resp_data = resp.json()

    assert resp_data['_id'] == id_
    assert resp_data['_type'] == 'org'
    # title have not changed, so should not be included in result
    assert 'title' not in resp_data
    # revision must be the same, since nothing has changed
    assert resp_data['_revision'] == revision
