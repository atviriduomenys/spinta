import pytest


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_update_object(model, app):
    app.authmodel(model, ['insert', 'patch', 'getone'])

    resp = app.post(f'/{model}', json={
        'status': 'ok',
        'sync': {
            'sync_revision': '1',
            'sync_resources': [
                {
                    'sync_id': '2',
                    'sync_source': 'report'
                }
            ]
        }
    })
    assert resp.status_code == 201, resp.json()
    id_ = resp.json()['_id']
    rev = resp.json()['_revision']

    resp = app.patch(f'/{model}/{id_}', json={
        '_revision': rev,
        'sync': {
            'sync_revision': '3'
        }
    })
    assert resp.status_code == 200, resp.json()
    rev = resp.json()['_revision']

    resp = app.get(f'/{model}/{id_}')
    assert resp.status_code == 200, resp.json()
    assert resp.json()['sync'] == {
        'sync_revision': '3',
        'sync_resources': [
            {
                'sync_id': '2',
                'sync_source': 'report'
            }
        ]
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_update_object_array(model, app):
    app.authmodel(model, ['insert', 'patch', 'getone'])

    resp = app.post(f'/{model}', json={
        'status': 'ok',
        'sync': {
            'sync_revision': '1',
            'sync_resources': [
                {
                    'sync_id': '2',
                    'sync_source': 'report'
                }
            ]
        }
    })
    assert resp.status_code == 201, resp.json()
    id_ = resp.json()['_id']
    rev = resp.json()['_revision']

    resp = app.patch(f'/{model}/{id_}', json={
        '_revision': rev,
        'sync': {
            'sync_resources': [{
                'sync_id': '3',
                'sync_source': 'troper'
            }],
        }
    })
    assert resp.status_code == 200, resp.json()
    rev = resp.json()['_revision']

    resp = app.get(f'/{model}/{id_}')
    assert resp.status_code == 200, resp.json()
    assert resp.json()['sync'] == {
        'sync_revision': '1',
        'sync_resources': [
            {
                'sync_id': '3',
                'sync_source': 'troper'
            }
        ]
    }
