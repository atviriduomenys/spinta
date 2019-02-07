import pathlib

import pkg_resources as pres
import pp

from spinta.store import Store


def test_schema_loader():
    # Initialize data store and discover available types, functions and backends.
    store = Store().discover()

    # Internal manifest
    # =================

    # Load manifest YAML files.
    internal = store.get_object('manifest')
    store.run(internal, {
        'manifest.load': {
            'type': 'manifest',
            'name': 'internal',
            'path': pres.resource_filename('spinta', 'manifest'),
        },
    }, ns='internal')
    store.run(internal, {'manifest.check': None}, ns='internal')

    # Initialize database tables and run schema migration.
    # Internal tables are only available on default backend connection.
    store.run(internal, {'backend.prepare.internal': None}, backend='default', ns='internal')
    store.run(internal, {'backend.migrate.internal': None}, backend='default', ns='internal')

    # User provided manifest
    # ======================

    # Load manifest YAML files.
    default = store.get_object('manifest')
    store.run(default, {
        'manifest.load': {
            'type': 'manifest',
            'name': 'default',
            'path': pathlib.Path(__file__).parent / 'manifest',
        },
    })
    store.run(default, {'manifest.check': None})

    # Initialize database tables and run schema migration.
    store.run(default, {'backend.prepare': None}, backend='default')
    store.run(default, {'backend.migrate': None}, backend='default')

    pp(store.objects)

    assert False
