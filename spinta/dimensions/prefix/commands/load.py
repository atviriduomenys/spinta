from typing import Any, Dict

from spinta import commands
from spinta.components import Context, Node
from spinta.dimensions.prefix.components import UriPrefix
from spinta.nodes import load_node


@commands.load.register(Context, UriPrefix, dict)
def load(
    context: Context,
    prefix: UriPrefix,
    data: Dict[str, Any],
    *,
    parent: Node,
):
    return load_node(context, prefix, data, parent=parent)
