def test_schema_loader(context):
    result = context.push([
        {
            'type': 'country',
            '<id>': '1',
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            'type': 'org',
            '<id>': '1',
            'title': 'My Org',
            'govid': '0042',
            'country': {'type': 'country', '<id>': '1'},
        },
    ])

    # Add few objects to database.
    result = {x.pop('type'): x for x in result}
    assert result == {
        'country': {
            '<id>': '1',
            'id': result['country']['id'],
            'code': 'lt',
            'title': 'Lithuania',
        },
        'org': {
            '<id>': '1',
            'id': result['org']['id'],
            'country': result['country']['id'],
            'govid': '0042',
            'title': 'My Org',
        },
    }

    # Read those objects from database.
    assert context.getone('org', result['org']['id']) == {
        'id': result['org']['id'],
        'govid': '0042',
        'title': 'My Org',
        'country': int(result['country']['id']),
        'type': 'org',
    }

    assert context.getone('country', result['country']['id']) == {
        'id': result['country']['id'],
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
        'some': None,
    }
