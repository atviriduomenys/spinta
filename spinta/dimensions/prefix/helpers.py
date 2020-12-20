from typing import Any
from typing import Dict

from spinta import commands
from spinta.components import Component
from spinta.components import Context
from spinta.dimensions.prefix.components import UriPrefix
from spinta.manifests.components import Manifest
from spinta.nodes import get_node


def load_prefixes(
    context: Context,
    manifest: Manifest,
    parent: Component,
    prefixes: Dict[str, Dict[str, Any]],
) -> Dict[str, UriPrefix]:
    config = context.get('config')
    loaded = {}
    for name, data in prefixes.items():
        prefix: UriPrefix = get_node(
            config,
            manifest,
            data['eid'],
            data,
            group='dimensions',
            parent=parent,
        )
        prefix.eid = data['eid']
        prefix.type = data['type']
        prefix.parent = parent
        prefix.parent = manifest
        commands.load(context, prefix, data, parent=parent)
        loaded[name] = prefix
    return loaded


