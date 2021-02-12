from pathlib import Path
from typing import List

import click

from spinta import commands
from spinta.components import Context
from spinta.components import Mode
from spinta.components import Store
from spinta.core.config import RawConfig


def load_store(context: Context) -> Store:
    config = context.get('config')
    commands.load(context, config)
    commands.check(context, config)
    store = context.get('store')
    commands.load(context, store)
    return store


def prepare_manifest(context: Context, *, verbose: bool = True) -> Store:
    store = load_store(context)
    if verbose:
        click.echo(f"Loading manifest {store.manifest.name}...")
    commands.load(context, store.manifest)
    commands.link(context, store.manifest)
    commands.check(context, store.manifest)
    commands.wait(context, store)
    commands.prepare(context, store.manifest)
    return store


def configure(
    context: Context,
    manifests: List[Path],
    *,
    mode: Mode = Mode.internal,
) -> Context:
    context = context.fork('cli')

    if manifests:
        config = {
            'backends.run': {'type': 'memory'},
            'keymaps.default': {
                'type': 'sqlalchemy',
                'dsn': 'sqlite:///keymaps.db',
            },
            'manifests': {
                'run': {
                    'type': 'backend',
                    'backend': 'run',
                    'keymap': 'default',
                    'mode': mode.value,
                    'sync': [],
                },
            },
            'manifest': 'run',
        }

        for i, manifest_path in enumerate(manifests):
            manifest_name = f'run{i}'
            config['manifests'][manifest_name] = {
                'type': 'tabular',
                'path': manifest_path,
                'backend': '',
            }
            config['manifests']['run']['sync'].append(manifest_name)

        # Add given manifest file to configuration
        rc: RawConfig = context.get('rc')
        context.set('rc', rc.fork(config))

    return context
