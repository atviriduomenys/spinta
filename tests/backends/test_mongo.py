def test_schema_loader(context):
    # Add few objects to database.
    result = context.push([
        {
            'type': 'mongo',
            'value': '42',
        },
    ])

    result = {x.pop('type'): x for x in result}
    assert result == {
        'mongo': {
            'id': result['mongo']['id'],
            'value': '42',
        },
    }

    # Read those objects from database.
    assert context.getone('mongo', result['mongo']['id']) == {
        'type': 'mongo',
        'id': result['mongo']['id'],
        'value': '42',
    }
