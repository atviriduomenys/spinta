import datetime
import json


def test_export_json(context, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))
    mocker.patch('spinta.backends.postgresql.dataset.get_new_id', return_value='REV')

    context.push([
        {
            'type': 'country/:ds/csv/:rs/countries',
            'id': '1',
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            'type': 'country/:ds/csv/:rs/countries',
            'id': '2',
            'code': 'lv',
            'title': 'LATVIA',
        },
        {
            'type': 'country/:ds/csv/:rs/countries',
            'id': '2',
            'code': 'lv',
            'title': 'Latvia',
        },
    ])

    app.authorize(['spinta_country_ds_csv_rs_countries_getall'])

    resp = app.get('country/:ds/csv/:rs/countries/:format/jsonl')
    data = [json.loads(line) for line in resp.text.splitlines()]
    assert data == [
        {
            'code': 'lt',
            'id': '1',
            'revision': 'REV',
            'title': 'Lithuania',
            'type': 'country/:ds/csv/:rs/countries'
        },
        {
            'code': 'lv',
            'id': '2',
            'revision': 'REV',
            'title': 'Latvia',
            'type': 'country/:ds/csv/:rs/countries',
        },
    ]
