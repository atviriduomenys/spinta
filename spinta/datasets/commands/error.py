from typing import Dict

from spinta import commands
from spinta.datasets.components import Resource, Entity, Attribute


@commands.get_error_context.register(Resource)
def get_error_context(resource: Resource, *, prefix='this') -> Dict[str, str]:
    context = commands.get_error_context(resource.dataset, prefix=f'{prefix}.dataset')
    context['resource'] = f'{prefix}.name'
    return context


@commands.get_error_context.register(Entity)
def get_error_context(external: Entity, *, prefix='this') -> Dict[str, str]:
    context = commands.get_error_context(external.model, prefix=f'{prefix}.model')
    context['external'] = f'{prefix}.name'
    return context


@commands.get_error_context.register(Attribute)
def get_error_context(external: Attribute, *, prefix='this') -> Dict[str, str]:
    context = commands.get_error_context(external.prop, prefix=f'{prefix}.prop')
    context['external'] = f'{prefix}.name'
    return context
