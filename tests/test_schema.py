import pathlib

from spinta.store import Store


def test_schema_loader(postgresql):
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

    import pp; pp(store.serialize(level=3))

    assert store.serialize() == {}

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
