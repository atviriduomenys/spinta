import base64

import pytest


@pytest.mark.models(
    'dtypes/binary/:dataset/dtypes/binary/:resource/resource/:origin/origin',
)
def test_binary(model, app):
    data = base64.b64encode(b'data').decode('ascii')
    app.authmodel(model, ['insert'])
    resp = app.post(f'/{model}', json={
        'blob': data,
    })
    assert resp.status_code == 201, resp.json()
    assert resp.json()['blob'] == data
