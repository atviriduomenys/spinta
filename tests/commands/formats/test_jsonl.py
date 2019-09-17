import datetime
import json
import operator


def test_export_json(context, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))
    mocker.patch('spinta.backends.postgresql.dataset.get_new_id', return_value='REV')

    context.push([
        {
            'type': 'country/:dataset/csv/:resource/countries',
            'id': '1',
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            'type': 'country/:dataset/csv/:resource/countries',
            'id': '2',
            'code': 'lv',
            'title': 'LATVIA',
        },
        {
            'type': 'country/:dataset/csv/:resource/countries',
            'id': '2',
            'code': 'lv',
            'title': 'Latvia',
        },
    ])

    app.authorize(['spinta_country_dataset_csv_resource_countries_getall'])

    resp = app.get('country/:dataset/csv/:resource/countries/:format/jsonl')
    data = sorted([json.loads(line) for line in resp.text.splitlines()], key=operator.itemgetter('code'))
    assert data == [
        {
            'code': 'lt',
            'id': '1',
            'revision': 'REV',
            'title': 'Lithuania',
            'type': 'country/:dataset/csv/:resource/countries'
        },
        {
            'code': 'lv',
            'id': '2',
            'revision': 'REV',
            'title': 'Latvia',
            'type': 'country/:dataset/csv/:resource/countries',
        },
    ]
