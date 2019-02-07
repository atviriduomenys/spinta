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

    pp(store.objects)

    assert False
