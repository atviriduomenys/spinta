from typing import Any

from spinta import commands
from spinta.components import Context
from spinta.datasets.components import Dataset
from spinta.dimensions.prefix.components import UriPrefix
from spinta.manifests.components import Manifest
from spinta.nodes import get_node


def load_prefixes(
    context: Context,
    manifest: Manifest,
    node: Manifest | Dataset,
    prefixes: dict[str, dict[str, Any]],
) -> dict[str, UriPrefix]:
    config = context.get("config")
    loaded = {}
    for name, data in prefixes.items():
        prefix: UriPrefix = get_node(
            context,
            config,
            manifest,
            data["eid"],
            data,
            group="dimensions",
            parent=node,
        )
        prefix.eid = data["eid"]
        prefix.type = data["type"]
        prefix.parent = node
        commands.load(context, prefix, data, parent=node)
        loaded[name] = prefix
    return loaded
