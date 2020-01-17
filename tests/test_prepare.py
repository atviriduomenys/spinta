import pytest

from responses import GET

from spinta import exceptions


def test_prepare_model(app, context, responses):
    responses.add(
        GET, 'http://example.com/orgs.csv',
        status=200, content_type='text/plain; charset=utf-8',
        body=(
            'govid,org,kodas,šalis\n'
            '1,Org1,foo,Lietuva\n'
        ),
    )
    with pytest.raises(exceptions.InvalidValue) as e:
        context.pull('denorm')
    assert e.value.context == {
        'schema': 'tests/manifest/datasets/denorm.yml',
        'manifest': 'default',
        'backend': 'default',
        'dataset': 'denorm',
        'model': 'country',
        'resource': 'orgs',
        'origin': '',
        'error': 'Data check error.',
    }


def test_prepare_prop(app, context, responses):
    responses.add(
        GET, 'http://example.com/countries.csv',
        status=200, stream=True, content_type='text/plain; charset=utf-8',
        body='kodas,šalis\nfoo,Lithuania\n',
    )
    with pytest.raises(exceptions.InvalidValue) as e:
        context.pull('csv')
    assert e.value.context == {
        'schema': 'tests/manifest/datasets/csv.yml',
        'manifest': 'default',
        'backend': 'default',
        'dataset': 'csv',
        'model': 'country',
        'resource': 'countries',
        'origin': '',
        'property': 'code',
        'type': 'string',
        'error': 'Data check error.',
    }
