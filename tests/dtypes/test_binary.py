import base64

import pytest


@pytest.mark.models(
    'dtypes/binary/:dataset/dtypes/binary/:resource/resource/:origin/origin',
)
def test_insert(model, app):
    data = base64.b64encode(b'data').decode('ascii')
    app.authmodel(model, ['insert'])
    resp = app.post(f'/{model}', json={'blob': data})
    assert resp.status_code == 201, resp.json()
    assert resp.json()['blob'] == data


@pytest.mark.models(
    'dtypes/binary/:dataset/dtypes/binary/:resource/resource/:origin/origin',
)
def test_upsert(model, app):
    data = base64.b64encode(b'data').decode('ascii')
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel(model, ['upsert'])
    pk = '844b08602aeffbf0d12dbfd5f2e861c7501ed2cb'
    resp = app.post(f'/{model}', json={
        '_op': 'upsert',
        '_where': f'_id={pk}',
        '_id': pk,
        'blob': data,
    })
    assert resp.status_code == 201, resp.json()
    assert resp.json()['_id'] == pk
    assert resp.json()['blob'] == data

    resp = app.post(f'/{model}', json={
        '_op': 'upsert',
        '_where': f'_id={pk}',
        '_id': pk,
        'blob': data,
    })
    assert resp.status_code == 200, resp.json()
    assert resp.json()['_id'] == pk
    assert resp.json()['blob'] == data


@pytest.mark.models(
    'dtypes/binary/:dataset/dtypes/binary/:resource/resource/:origin/origin',
)
def test_getone(model, app):
    data = base64.b64encode(b'data').decode('ascii')
    app.authmodel(model, ['insert', 'getone'])
    resp = app.post(f'/{model}', json={'blob': data})
    assert resp.status_code == 201, resp.json()
    assert resp.json()['blob'] == data

    pk = resp.json()['_id']
    resp = app.get(f'/{model}'.replace(':dataset/', f'{pk}/:dataset/'))
    assert resp.status_code == 200, resp.json()
    assert resp.json()['blob'] == data


@pytest.mark.models(
    'dtypes/binary/:dataset/dtypes/binary/:resource/resource/:origin/origin',
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
