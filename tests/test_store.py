import pytest


@pytest.mark.models(
    'backends/mongo/{}',
    'backends/postgres/{}',
)
def test_schema_loader(model, app):
    model_org = model.format('org')
    model_country = model.format('country')

    app.authmodel(model_org, ['insert'])
    app.authmodel(model_country, ['insert'])

    country = app.post(f'/{model_country}', json={
        'code': 'lt',
        'title': 'Lithuania',
    }).json()
    org = app.post(f'/{model_org}', json={
        'title': 'My Org',
        'govid': '0042',
        'country': country['_id'],
    }).json()

    assert country == {
        '_id': country['_id'],
        '_type': model_country,
        '_revision': country['_revision'],
        'code': 'lt',
        'title': 'Lithuania',
    }
    assert org == {
        '_id': org['_id'],
        '_type': model_org,
        '_revision': org['_revision'],
        'country': country['_id'],
        'govid': '0042',
        'title': 'My Org',
    }

    app.authorize(['spinta_getone'])

    resp = app.get(f'/{model_org}/{org["_id"]}')
    data = resp.json()
    revision = data['_revision']
    assert data == {
        '_id': org['_id'],
        'govid': '0042',
        'title': 'My Org',
        'country': country['_id'],
        '_type': model_org,
        '_revision': revision,
    }

    resp = app.get(f'/{model_country}/{country["_id"]}')
    data = resp.json()
    revision = data['_revision']
    assert data == {
        '_id': country['_id'],
        'code': 'lt',
        'title': 'Lithuania',
        '_type': model_country,
        '_revision': revision,
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_nested(model, app):
    app.authmodel(model, ['insert', 'getone'])

    resp = app.post(f'/{model}', json={
        '_type': model,
        'notes': [{'note': 'foo'}]
    })
    assert resp.status_code == 201
    data = resp.json()
    id_ = data['_id']
    revision = data['_revision']

    data = app.get(f'/{model}/{id_}').json()
    assert data['_id'] == id_
    assert data['_type'] == model
    assert data['_revision'] == revision
    assert data['notes'] == [{
        'note': 'foo',
        'note_type': None,
        'create_date': None
    }]
