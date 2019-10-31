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
