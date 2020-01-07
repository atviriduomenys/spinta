import pytest


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
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

    resp = app.get(f'/{model}?select(_id),sort(+_id)')
    ids = [i["_id"] for i in resp.json()['_data']]
    assert sorted(ids) == ids


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_sort_with_nested_prop(model, app):
    app.authmodel(model, ['insert', 'search'])

    resp = app.post('/', json={'_data': [
        {'_op': 'insert', '_type': model, 'notes': [{'note': '01'}]},
        {'_op': 'insert', '_type': model, 'notes': [{'note': '02'}]},
    ]})
    assert resp.status_code == 200, resp.json()

    resp = app.get(f'/{model}?select(notes.note),sort(+notes.note)')
    assert resp.json()['_data'] == [
        {'notes': [{'note': '01'}]},
        {'notes': [{'note': '02'}]},
    ]

    resp = app.get(f'/{model}?select(notes.note),sort(-notes.note)')
    assert resp.json()['_data'] == [
        {'notes': [{'note': '02'}]},
        {'notes': [{'note': '01'}]},
    ]
