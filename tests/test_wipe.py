import pytest

from spinta.testing.utils import get_error_codes


def test_wipe_everything(app):
    app.authorize(['spinta_insert', 'spinta_getall', 'spinta_wipe'])

    # Create some data in different models
    resp = app.post('/', json={'_data': [
        {'_op': 'insert', '_type': 'report', 'status': 'ok'},
        {'_op': 'insert', '_type': 'backends/mongo/report', 'status': 'ok'},
        {'_op': 'insert', '_type': 'backends/postgres/report', 'status': 'ok'},
    ]})
    assert resp.status_code == 200, resp.json()

    # Get data from all models
    resp = app.get('/:all')
    assert resp.status_code == 200, resp.json()

    # Check what data we got
    data = sorted([(r['_type'], r['status']) for r in resp.json()['_data']])
    assert data == [
        ('backends/mongo/report', 'ok'),
        ('backends/postgres/report', 'ok'),
        ('report', 'ok'),
    ]

    # Wipe all data
    resp = app.delete('/:wipe')
    assert resp.status_code == 200, resp.json()

    # Check what data again
    resp = app.get('/:all')
    assert resp.status_code == 200, resp.json()
    assert len(resp.json()['_data']) == 0


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_wipe_single_model(model, app):
    app.authorize(['spinta_insert', 'spinta_getall', 'spinta_wipe'])

    # Create some data in different models
    resp = app.post('/', json={'_data': [
        {'_op': 'insert', '_type': 'report', 'status': 'ok'},
        {'_op': 'insert', '_type': 'backends/mongo/report', 'status': 'ok'},
        {'_op': 'insert', '_type': 'backends/postgres/report', 'status': 'ok'},
    ]})
    assert resp.status_code == 200, resp.json()

    # Get data from all models
    resp = app.get('/:all')
    assert resp.status_code == 200, resp.json()

    # Check what data we got
    data = sorted([(r['_type'], r['status']) for r in resp.json()['_data']])
    assert data == [
        ('backends/mongo/report', 'ok'),
        ('backends/postgres/report', 'ok'),
        ('report', 'ok'),
    ]

    # Wipe all data
    resp = app.delete(f'/{model}/:wipe')
    assert resp.status_code == 200, resp.json()

    # Check what data again
    resp = app.get('/:all')
    assert resp.status_code == 200, resp.json()
    data = sorted([(r['_type'], r['status']) for r in resp.json()['_data']])
    expected = [
        ('backends/mongo/report', 'ok'),
        ('backends/postgres/report', 'ok'),
        ('report', 'ok'),
    ]
    expected = [r for r in expected if r[0] != model]
    assert data == expected


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_wipe_check_scope(model, app):
    app.authorize(['spinta_insert', 'spinta_getall', 'spinta_delete'])
    resp = app.delete(f'/{model}/:wipe')
    assert resp.status_code == 403


def test_wipe_check_ns_scope(app):
    app.authorize(['spinta_insert', 'spinta_getall', 'spinta_delete'])
    resp = app.delete(f'/:wipe')
    assert resp.status_code == 403


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_wipe_in_batch(model, app):
    app.authorize(['spinta_wipe'])
    resp = app.post(f'/', json={
        '_data': [
            {'_op': 'wipe', '_type': model}
        ]
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ['UnknownAction']
