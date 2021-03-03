import csv
import pathlib
from typing import Union

from spinta import commands
from spinta.core.config import RawConfig
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.tabular.helpers import read_ascii_tabular_manifest
from spinta.manifests.tabular.helpers import read_ascii_tabular_rows
from spinta.manifests.tabular.helpers import read_tabular_manifest
from spinta.testing.context import create_test_context


def create_tabular_manifest(path: pathlib.Path, manifest: str):
    with path.open('w') as f:
        writer = csv.writer(f)
        rows = read_ascii_tabular_rows(manifest, strip=True)
        for row in rows:
            writer.writerow(row)


def load_tabular_manifest(
    rc: RawConfig,
    manifest: Union[pathlib.Path, str],
) -> Manifest:
    if isinstance(manifest, str) and '|' in manifest:
        path = ''
    else:
        path = manifest
        manifest = None

    rc = rc.fork({
        'manifest': 'default',
        'manifests': {
            'default': {
                'type': 'tabular',
                'path': str(path),
                'keymap': 'default',
            },
        },
    })

    context = create_test_context(rc)

    config = context.get('config')
    commands.load(context, config)

    store = context.get('store')
    commands.load(context, store)

    if path:
        schemas = read_tabular_manifest(path)
    else:
        schemas = read_ascii_tabular_manifest(manifest, strip=True)
    load_manifest_nodes(context, store.manifest, schemas)

    commands.link(context, store.manifest)

    return store.manifest
