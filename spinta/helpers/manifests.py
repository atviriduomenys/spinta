import pathlib

import pkg_resources as pres

from spinta import commands
from spinta.components.core import Context, Store, Config
from spinta.components.node import Manifest
from spinta.helpers.component import create_component


def load_internal_manifest(
    context: Context,
    config: Config,
    store: Store,
    manifest: Manifest,
) -> None:
    internal = create_component(config, store, ctype='yaml', group='manifests')
    internal.name = 'internal'
    internal.path = pathlib.Path(pres.resource_filename('spinta', 'manifest'))

    for data, versions in internal.read(context):
        node = create_component(config, manifest, data, group='nodes')
        commands.load(context, node)
        manifest.objects[node.type][node.name] = node

    return internal
