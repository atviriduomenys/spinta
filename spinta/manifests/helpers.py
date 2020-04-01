from typing import List

import pathlib

import pkg_resources as pres

from spinta import commands
from spinta.nodes import get_node
from spinta.components import Context, Config, Store
from spinta.manifests.components import Manifest


def get_manifest(config: Config, name: str):
    type_ = config.rc.get('manifests', name, 'type', required=True)
    Manifest = config.components['manifests'][type_]
    manifest = Manifest()
    manifest.type = type_
    manifest.name = name
    manifest.load(config)
    return manifest


def load_manifest(context: Context, store: Store, config: Config, name: str, synced: List[str] = None):
    synced = synced or []
    if name in synced:
        synced = ', '.join(synced)
        raise Exception(
            f"Circular manifest sync is detected: {synced}. Check manifest "
            f"configuration with: `spinta config manifests`."
        )
    else:
        synced += [name]

    manifest = get_manifest(config, name)
    manifest.store = store
    manifest.backend = config.rc.get('manifests', name, 'backend', required=True)

    sync = config.rc.get('manifests', name, 'sync')
    if not isinstance(sync, list):
        sync = [sync] if sync else []
    # Do not fully load manifest, because loading whole manifest might be
    # expensive and we only need to do that if it is neceserry. For now, just
    # load empty manifest class.
    manifest.sync = [load_manifest(config, store, config, x, synced) for x in sync]

    # Add all supported node types.
    manifest.objects = {}
    for name in config.components['nodes'].keys():
        manifest.objects[name] = {}

    # Set some defaults.
    manifest.parent = None
    manifest.endpoints = {}

    return manifest


def get_internal_manifest(context: Context):
    config = context.get('config')
    Manifest = config.components['manifests']['yaml']
    internal = Manifest()
    internal.name = 'internal'
    internal.path = pathlib.Path(pres.resource_filename('spinta', 'manifest'))
    return internal


def load_manifest_node(
    context: Context,
    config: Config,
    manifest: Manifest,
    data: dict,
    *,
    # XXX: This is a temporary workaround and should be removed.
    #      `check is used to not check if node is already defined, we disable
    #      this check for freezed nodes.
    check=True,
):
    node = get_node(config, manifest, data, check=check)
    node.type = data['type']
    node.parent = manifest
    node.manifest = manifest
    return commands.load(context, node, data, manifest)
