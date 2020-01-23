import pytest


@pytest.mark.models(
    'dtypes/binary/:dataset/dtypes/binary/:resource/resource/:origin/origin',
)
def test_binary(model, app):
    app.authmodel(model, ['insert'])
    resp = app.post(f'/{model}', json={
        'blob': 'test',
    })
    assert resp.status_code == 201, resp.json()
    assert resp.json()['blob'] == 'test'
    assert False
