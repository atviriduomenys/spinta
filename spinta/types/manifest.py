from typing import Dict

import collections

from spinta import commands
from spinta.core.context import Context
from spinta.components.manifests import Manifest, Node
from spinta.errors.loading import DuplicateEndpoints


@commands.check.register()
def check(context: Context, manifest: Manifest):
    # Check endpoints.
    endpoints = collections.Counter(m.endpoint for m in manifest.models)
    endpoints = {k for k, v in endpoints if v > 1}
    if endpoints:
        endpoints = ', '.join(sorted(endpoints))
        raise DuplicateEndpoints(manifest, (
            f"Found duplicate endpoint names: "
        ))


@commands.get_error_context.register()
def get_error_context(manifest: Manifest, *, prefix='this') -> Dict[str, str]:
    return {
        'schema': f'{prefix}.path.__str__()',
        'manifest': f'{prefix}.name'
    }


@commands.get_error_context.register()  # noqa
def get_error_context(node: Node, *, prefix='this') -> Dict[str, str]:  # noqa
    context = {
        'schema': f'{prefix}.path.__str__()',
    }
    depth = 0
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
