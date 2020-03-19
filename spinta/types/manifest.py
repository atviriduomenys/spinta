from typing import Dict

from ruamel.yaml import YAML

from spinta.commands import prepare, check
from spinta.components import Context, Manifest, Node
from spinta import commands

yaml = YAML(typ='safe')


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

    # Check dataset names in config.
    config = context.get('config')
    known_datasets = set(manifest.objects.get('dataset', {}).keys())
    datasets_in_config = set(config.rc.keys('datasets', manifest.name))
    unknown_datasets = datasets_in_config - known_datasets
    if unknown_datasets:
        raise Exception("Unknown datasets in configuration parameter 'datasets': %s." % (
            ', '.join([repr(x) for x in sorted(unknown_datasets)])
        ))

    # Check endpoints.
    names = {model.name for model in manifest.find_all_models()}
    for model in manifest.find_all_models():
        if model.endpoint == model.name or model.endpoint in names:
            raise Exception(f"Endpoint name can't overshadow existing model names and {model.endpoint!r} is already a model name.")


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
