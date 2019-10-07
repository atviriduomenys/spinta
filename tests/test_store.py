import pytest


@pytest.mark.models(
    'backends/mongo/{}',
    'backends/postgres/{}',
)
def test_schema_loader(model, context, app):
    model_org = model.format('org')
    model_country = model.format('country')

    country, = context.push([
        {
            'type': model_country,
            'code': 'lt',
            'title': 'Lithuania',
        },
    ])
    org, = context.push([
        {
            'type': model_org,
            'title': 'My Org',
            'govid': '0042',
            'country': country['id'],
        },
    ])

    assert country == {
        'id': country['id'],
        'type': model_country,
    }
    assert org == {
        'id': org['id'],
        'type': model_org,
    }

    app.authorize(['spinta_getone'])

    resp = app.get(f'/{model_org}/{org["id"]}')
    data = resp.json()
    revision = data['revision']
    assert data == {
        'id': org['id'],
        'govid': '0042',
        'title': 'My Org',
        'country': country['id'],
        'type': model_org,
        'revision': revision,
    }

    resp = app.get(f'/{model_country}/{country["id"]}')
    data = resp.json()
    revision = data['revision']
    assert data == {
        'id': country['id'],
        'code': 'lt',
        'title': 'Lithuania',
        'type': model_country,
        'revision': revision,
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_nested(model, app):
    app.authmodel(model, ['insert', 'getone'])

    resp = app.post(f'/{model}', json={
        'type': model,
        'notes': [{'note': 'foo'}]
    })
    assert resp.status_code == 201
    data = resp.json()
    id_ = data['id']
    revision = data['revision']

    data = app.get(f'/{model}/{id_}').json()
    assert data['id'] == id_
    assert data['type'] == model
    assert data['revision'] == revision
    assert data['notes'] == [{
        'note': 'foo',
        'note_type': None,
        'create_date': None
    }]
