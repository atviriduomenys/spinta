import base64

import pytest


@pytest.mark.models(
    'backends/postgres/dtypes/binary',
)
def test_insert(model, app):
    data = base64.b64encode(b'data').decode('ascii')
    app.authmodel(model, ['insert'])
    resp = app.post(f'/{model}', json={'blob': data})
    assert resp.status_code == 201, resp.json()
    assert resp.json()['blob'] == data


@pytest.mark.models(
    'backends/postgres/dtypes/binary',
)
def test_upsert(model, app):
    data = base64.b64encode(b'data').decode('ascii')
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel(model, ['upsert'])
    if ':dataset/' in model:
        pk = '844b08602aeffbf0d12dbfd5f2e861c7501ed2cb'
    else:
        pk = '9ea9cf88-68f6-4753-b9e6-ce3d40ba1861'
    resp = app.post(f'/{model}', json={
        '_op': 'upsert',
        '_where': f'_id="{pk}"',
        '_id': pk,
        'blob': data,
    })
    assert resp.status_code == 201, resp.json()
    assert resp.json()['_id'] == pk
    assert resp.json()['blob'] == data

    resp = app.post(f'/{model}', json={
        '_op': 'upsert',
        '_where': f'_id="{pk}"',
        '_id': pk,
        'blob': data,
    })
    assert resp.status_code == 200, resp.json()
    assert resp.json()['_id'] == pk
    assert 'blob' not in resp.json()


@pytest.mark.models(
    'datasets/dtypes/binary',
)
def test_getone(model, app):
    data = base64.b64encode(b'data').decode('ascii')
    app.authmodel(model, ['insert', 'getone'])
    resp = app.post(f'/{model}', json={'blob': data})
    assert resp.status_code == 201, resp.json()
    assert resp.json()['blob'] == data

    pk = resp.json()['_id']
    resp = app.get(f'/{model}/{pk}')
    assert resp.status_code == 200, resp.json()
    assert resp.json()['blob'] == data


@pytest.mark.models(
    'datasets/dtypes/binary',
)
def test_getall(model, app):
    data = base64.b64encode(b'data').decode('ascii')
    app.authmodel(model, ['insert', 'getall'])
    resp = app.post(f'/{model}', json={'blob': data})
    assert resp.status_code == 201, resp.json()
    assert resp.json()['blob'] == data

    resp = app.get(f'/{model}')
    assert resp.status_code == 200, resp.json()
    assert resp.json()['_data'][0]['blob'] == data
