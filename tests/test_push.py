import json

import pytest


@pytest.mark.models(
    'backends/postgres/report/:dataset/test',
)
def test_push(model, app):
    app.authmodel(model, ['insert'])
    data = [
        {'_op': 'insert', '_type': model, 'status': 'ok'},
        {'_op': 'insert', '_type': model, 'status': 'warning'},
        {'_op': 'insert', '_type': model, 'status': 'critical'},
        {'_op': 'insert', '_type': model, 'status': 'blocker'},
    ]
    headers = {'content-type': 'application/x-ndjson'}
    payload = (json.dumps(x) + '\n' for x in data)
    resp = app.post(f'/{model}', headers=headers, data=payload)
    resp = resp.json()
    assert resp == {
        'transaction': resp['transaction'],
        'status': 'ok',
        'stats': {
            'errors': 0,
            'insert': 4,
        },
    }
