from typing import Tuple

from spinta import commands
from spinta.cli.helpers.store import load_store
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.components import Manifest
from spinta.manifests.tabular.helpers import SHORT_NAMES
from spinta.manifests.tabular.helpers import load_ascii_tabular_manifest
from spinta.manifests.tabular.helpers import render_tabular_manifest
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.context import create_test_context


def compare_manifest(manifest: Manifest, expected: str) -> Tuple[str, str]:
    expected = striptable(expected)
    cols = [c.strip() for c in expected.splitlines()[0].split('|')]
    cols = [SHORT_NAMES.get(c, c) for c in cols]
    actual = render_tabular_manifest(manifest, cols)
    return actual, expected


def configure_manifest(rc: RawConfig, manifest: str) -> Context:
    rc = rc.fork({
        'backends': {
            'memory': {
                'type': 'memory',
            },
        },
        'manifests.test': {
            'type': 'tabular',
            'backend': 'memory',
            'keymap': '',
            'mode': 'external',
            'path': None,
        },
        'manifest': 'test',
    })
    context = create_test_context(rc)
    store = load_store(context)
    load_ascii_tabular_manifest(context, store.manifest, manifest, strip=True)
    commands.check(context, store.manifest)
    commands.prepare(context, store.manifest)
    context.loaded = True
    return context
