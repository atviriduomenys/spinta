import pathlib

import pkg_resources as pres
import pp

from spinta.types.manifest import Manifest


def test_schema_loader():
    # Initialize manifest and discover available types, functions and backends.
    manifest = Manifest().discover()

    # Internal manifest
    # =================

    # Load manifest YAML files.
    manifest.run(manifest, {'manifest.load': pres.resource_filename('spinta', 'manifest')}, ns='internal')
    manifest.run(manifest, {'manifest.check': None}, ns='internal')

    # Initialize database tables and run schema migration.
    # Internal tables are only available on default backend.
    manifest.run(manifest, {'backend.prepare.internal': None}, backend='default', ns='internal')
    manifest.run(manifest, {'backend.migrate.internal': None}, backend='default', ns='internal')

    # User provided manifest
    # ======================

    # Load manifest YAML files.
    manifest.run(manifest, {'manifest.load': pathlib.Path(__file__).parent / 'manifest'})
    manifest.run(manifest, {'manifest.check': None})

    # Initialize database tables and run schema migration.
    manifest.run(manifest, {'backend.prepare': None}, backend='default')
    manifest.run(manifest, {'backend.migrate': None}, backend='default')

    pp(manifest.objects)

    for cmd in manifest.commands.values():
        print(cmd.metadata.name)

    assert False
