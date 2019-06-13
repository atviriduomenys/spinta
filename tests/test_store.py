def test_schema_loader(context):
    country, = context.push([
        {
            'type': 'country',
            'code': 'lt',
            'title': 'Lithuania',
        },
    ])
    org, = context.push([
        {
            'type': 'org',
            'title': 'My Org',
            'govid': '0042',
            'country': country['id'],
        },
    ])

    assert country == {
        'id': country['id'],
        'code': 'lt',
        'title': 'Lithuania',
        'type': 'country',
    }
    assert org == {
        'id': org['id'],
        'country': country['id'],
        'govid': '0042',
        'title': 'My Org',
        'type': 'org',
    }

    assert context.getone('org', org['id']) == {
        'id': org['id'],
        'govid': '0042',
        'title': 'My Org',
        'country': country['id'],
        'type': 'org',
    }

    assert context.getone('country', country['id']) == {
        'id': country['id'],
        'code': 'lt',
        'title': 'Lithuania',
        'type': 'country',
    }


def test_nested(context):
    result = list(context.push([
        {
            'type': 'nested',
            'some': [{'nested': {'structure': 'here'}}]
        }
    ]))
    assert context.getone('nested', result[0]['id']) == {
        'type': 'nested',
        'id': result[0]['id'],
        # TODO: add nested structure support for PostgreSQL
        'some': [],
    }
