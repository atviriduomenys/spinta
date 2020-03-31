from typing import Dict

from ruamel.yaml import YAML

from spinta.commands import prepare, check
from spinta.components import Context, Manifest, Node
from spinta import commands

yaml = YAML(typ='safe')


@commands.link.register()
def link(context: Context, manifest: Manifest):
    manifest.backend = manifest.store.backends[manifest.backend]
    for nodes in manifest.objects.values():
        for node in nodes.values():
            commands.link(context, node)


@commands.link.register()
def link(context: Context, node: Node):
    pass


@prepare.register()
def prepare(context: Context, manifest: Manifest):
    store = context.get('store')
    for backend in store.backends.values():
        prepare(context, backend, manifest)


@check.register()
def check(context: Context, manifest: Manifest):
    for objects in manifest.objects.values():
        for obj in objects.values():
            check(context, obj)

    # Check endpoints.
    names = set(manifest.models)
    for model in manifest.models.values():
        if model.endpoint == model.name or model.endpoint in names:
            raise Exception(f"Endpoint name can't overshadow existing model names and {model.endpoint!r} is already a model name.")


@commands.get_error_context.register()
def get_error_context(manifest: Manifest, *, prefix='this') -> Dict[str, str]:
    return {
        'schema': f'{prefix}.path.__str__()',
        'manifest': f'{prefix}.name'
    }


@commands.get_error_context.register(type(None))  # noqa
def get_error_context(node: Node, *, prefix='this') -> Dict[str, str]:  # noqa
    return {}


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
