import pytest


@pytest.mark.models(
    'backends/mongo/report',
    # TODO: 'backends/postgres/report',
    'backends/postgres/report/:dataset/test',
)
def test_sort(model, app):
    app.authmodel(model, ['insert', 'search'])
    resp = app.post('/', json={'_data': [
        {'_op': 'insert', '_type': model, 'count': 10},
        {'_op': 'insert', '_type': model, 'count': 9},
    ]})
    assert resp.status_code == 200, resp.json()

    resp = app.get(f'/{model}?select(count),sort(+count)')
    assert resp.json()['_data'] == [
        {'count': 9},
        {'count': 10},
    ]

    resp = app.get(f'/{model}?select(count),sort(-count)')
    assert resp.json()['_data'] == [
        {'count': 10},
        {'count': 9},
    ]
