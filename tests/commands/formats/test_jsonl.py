import datetime
import json
import operator


def test_export_json(app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))

    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('country/:dataset/csv/:resource/countries', ['upsert', 'getall'])

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
    data = resp.json()['_data']
    revs = [d['_revision'] for d in data]

    resp = app.get('country/:dataset/csv/:resource/countries/:format/jsonl')
    data = sorted([json.loads(line) for line in resp.text.splitlines()], key=operator.itemgetter('code'))
    assert data == [
        {
            'code': 'lt',
            '_id': '1',
            '_revision': revs[0],
            'title': 'Lithuania',
            '_type': 'country/:dataset/csv/:resource/countries'
        },
        {
            'code': 'lv',
            '_id': '2',
            '_revision': revs[2],
            'title': 'Latvia',
            '_type': 'country/:dataset/csv/:resource/countries',
        },
    ]


def test_export_jsonl_with_all(app):
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('continent/:dataset/dependencies/:resource/continents', ['insert', 'getall'])
    app.authmodel('country/:dataset/dependencies/:resource/continents', ['insert', 'getall'])
    app.authmodel('capital/:dataset/dependencies/:resource/continents', ['insert', 'getall'])

    resp = app.post('/', json={'_data': [
        {
            '_type': 'continent/:dataset/dependencies/:resource/continents',
            '_op': 'insert',
            '_id': '1',
            'title': 'Europe',
        },
        {
            '_type': 'country/:dataset/dependencies/:resource/continents',
            '_op': 'insert',
            '_id': '2',
            'title': 'Lithuania',
            'continent': '1',
        },
        {
            '_type': 'capital/:dataset/dependencies/:resource/continents',
            '_op': 'insert',
            '_id': '3',
            'title': 'Vilnius',
            'country': '2',
        },
    ]})
    assert resp.status_code == 200, resp.json()

    resp = app.get('/:all/:dataset/dependencies/:resource/continents?format(jsonl)')
    assert resp.status_code == 200, resp.json()
    data = [json.loads(d) for d in resp.text.splitlines()]
    assert sorted((d['_id'], d['title']) for d in data) == [
        ('1', 'Europe'),
        ('2', 'Lithuania'),
        ('3', 'Vilnius')
    ]
