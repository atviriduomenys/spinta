import pathlib

from spinta.store import Store


def test_schema_loader(postgresql):
    # Configure data store.
    config = {
        'backends': {
            'default': {
                'type': 'postgresql',
                'dsn': postgresql,
            },
        },
        'manifests': {
            'default': {
                'path': pathlib.Path(__file__).parent / 'manifest',
            },
        },
    }

    store = Store()
    store.add_types()
    store.add_commands()
    store.configure(config)

    assert store.serialize(limit=3) == {
        'internal': {
            'config': {
                'backends': None,
                'description': None,
                'manifests': None,
                'name': None,
                'properties': None,
                'title': None,
                'type': None,
            },
            'model': {
                'model': None,
            },
        },
        'default': {
            'model': {
                'country': None,
                'org': None,
            },
            'dataset': {
                'csv': None,
            },
        },
    }

    # Prepare database, run schema migrations.
    store.prepare(internal=True)
    store.migrate(internal=True)
    store.prepare()
    store.migrate()

    result = store.push([
        {
            'type': 'country',
            '<id>': 1,
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            'type': 'org',
            '<id>': 1,
            'title': 'My Org',
            'govid': '0042',
            'country': {'type': 'country', '<id>': 1},
        },
    ])

    # Add few objects to database.
    result = {x.pop('type'): x for x in result}
    assert result == {
        'country': {
            '<id>': 1,
            'id': result['country']['id'],
        },
        'org': {
            '<id>': 1,
            'id': result['org']['id'],
        },
    }

    # Read those objects from database.
    assert store.get('org', result['org']['id']) == {
        'id': result['org']['id'],
        'govid': '0042',
        'title': 'My Org',
        'country': result['country']['id'],
    }

    assert store.get('country', result['country']['id']) == {
        'id': result['country']['id'],
        'code': 'lt',
        'title': 'Lithuania',
    }
