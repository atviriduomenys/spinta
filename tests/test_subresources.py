import pytest

from spinta.testing.utils import get_error_context

@pytest.mark.models(
    'backends/mongo/subitem',
    'backends/postgres/subitem',
)
def test_get_subresource(model, app):
    # XXX: if there's non-hidden subresource, then we should not ask for scope
    app.authmodel(model, ['insert', 'getone', 'subobj_getone', 'hidden_subobj_getone'])

    resp = app.post(f'/{model}', json={'_data': [
        {
            '_op': 'insert',
            '_type': model,
            'scalar': '42',
            'subarray': [{
                'foo': 'foobarbaz',
            }],
            'subobj': {
                'subprop': 'foobar123',
            },
            'hidden_subobj': {
                'hidden': 'secret',
            }
        }
    ]})

    assert resp.status_code == 200, resp.json()
    id_ = resp.json()['_data'][0]['_id']

    resp = app.get(f'/{model}/{id_}/subarray')
    assert resp.status_code == 400
    assert get_error_context(resp.json(), "UnavailableSubresource", ["prop", "prop_type"]) == {
        'prop': 'subarray',
        'prop_type': 'array',
    }

    resp = app.get(f'/{model}/{id_}/subobj')
    assert resp.json() == {
        'subprop': 'foobar123',
    }

    resp = app.get(f'/{model}/{id_}/hidden_subobj')
    assert resp.json() == {
        'hidden': 'secret',
    }


@pytest.mark.skip('NotImplemented')
@pytest.mark.models(
    'backends/mongo/subitem',
    'backends/postgres/subitem',
)
def test_get_subresource_file(model, app):
    pass
