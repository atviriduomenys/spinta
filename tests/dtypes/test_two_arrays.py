import pytest


@pytest.mark.models(
    'backends/mongo/dtypes/array/two_arrays',
    'backends/postgres/dtypes/array/two_arrays',
)
def test_update_with_two_array(model, app):
    app.authmodel(model, ['insert', 'update', 'search'])
    resp = app.post(f'/{model}', json={'array2': [{'string': 'old'}]})
    assert resp.status_code == 201
    res = resp.json()
    pk = res['_id']
    rev = res['_revision']
    resp = app.put(f'/{model}/{pk}', json={'_revision': rev,
                                           'array2': [{'string': 'old'}],
                                           'array1': ['new']})
    assert resp.status_code == 200
    resp = app.get(f'{model}?select(_id)&array2.string="old"')
    assert resp.json() == {'_data': [{'_id': pk}]}
