from typing import Dict

from spinta import commands
from spinta.components import Config
from spinta.components import Node
from spinta.manifests.components import Manifest


@commands.get_error_context.register(Config)
def get_error_context(config: Config, *, prefix='this') -> Dict[str, str]:
    return {
        'config': f'{prefix}.rc.get_source_names()',
    }


@commands.get_error_context.register(Manifest)
def get_error_context(manifest: Manifest, *, prefix='this') -> Dict[str, str]:
    return {
        'schema': f'{prefix}.path.__str__()',
        'manifest': f'{prefix}.name'
    }


@commands.get_error_context.register(type(None))
def get_error_context(node: type(None), *, prefix='this') -> Dict[str, str]:
    return {}


@commands.get_error_context.register(Node)
def get_error_context(node: Node, *, prefix='this') -> Dict[str, str]:
    depth = 0
    context = {}
    while node:
        name = prefix + ('.' if depth else '') + '.'.join(['parent'] * depth)
        if isinstance(node, Manifest):
            context['manifest'] = f'{name}.name'
            break
        elif node.type:
            context[node.type] = f'{name}.name'
        depth += 1
        node = node.parent
    return context
