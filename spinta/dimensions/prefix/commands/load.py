from typing import Any
from typing import Dict

from spinta import commands
from spinta.components import Context
from spinta.components import Node
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
