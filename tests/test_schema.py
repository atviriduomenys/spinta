import pathlib

import pp

from spinta.store import Store


def test_schema_loader():
    config = {
        'backends': {
            'default': {
                'type': 'postgresql',
                'dsn': 'postgresql:///spinta',
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

    pp(result)

    pp(store.objects)

    assert False
