from typing import Any
from typing import Dict
from typing import List
from typing import overload

from spinta import commands
from spinta import spyna
from spinta.components import Model
from spinta.core.ufuncs import asttoexpr
from spinta.datasets.components import Attribute
from spinta.datasets.helpers import load_resource_backend
from spinta.dimensions.enum.helpers import load_enums
from spinta.dimensions.lang.helpers import load_lang_data
from spinta.dimensions.prefix.helpers import load_prefixes
from spinta.exceptions import MultipleErrors
from spinta.exceptions import PropertyNotFound
from spinta.nodes import get_node, load_node
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.datasets.components import Dataset, Resource, Entity
from spinta.core.access import load_access_param
from spinta.types.namespace import load_namespace_from_name
from spinta.utils.data import take
from spinta.utils.schema import NA
from spinta.dimensions.comments.helpers import load_comments


@overload
@commands.load.register(Context, Dataset, dict, Manifest)
def load(
    context: Context,
    dataset: Dataset,
    data: dict,
    manifest: Manifest,
    *,
    source: Manifest = None,
):
    config = context.get('config')

    ns = load_namespace_from_name(context, manifest, data['name'], drop=False)
    if ns.generated:
        ns.title = data.get('title', '')
        ns.description = data.get('description', '')
    if 'prefixes' in data:
        prefixes = load_prefixes(context, manifest, dataset, data.pop('prefixes'))
        dataset.prefixes.update(prefixes)
    if 'enums' in data:
        parents = list(ns.parents())
        ns.enums = load_enums(context, parents, data.pop('enums'))

    load_node(context, dataset, data, parent=manifest)
    load_access_param(dataset, data.get('access'), (manifest,))

    dataset.ns = ns

    dataset.lang = load_lang_data(context, dataset.lang)

    # Load resources
    dataset.resources = {}
    for name, params in (data.get('resources') or {}).items():
        resource = get_node(config, manifest, dataset.eid, data, parent=dataset, group='datasets', ctype='resource')
        resource.type = params.get('type')
        resource.name = name
        resource.dataset = dataset
        dataset.resources[name] = commands.load(
            context,
            resource,
            params,
            manifest,
        )

    return dataset


@overload
@commands.load.register(Context, Resource, dict, Manifest)
def load(context: Context, resource: Resource, data: dict, manifest: Manifest):
    load_node(context, resource, data, parent=resource.dataset)
    if resource.prepare:
        formula = resource.prepare
        if formula and isinstance(formula, str):
            # If formula is a string, then convert it to parse tree. Formula can
            # be given as string or parse tree.
            formula = spyna.parse(formula)
        resource.prepare = asttoexpr(formula)
    load_access_param(resource, data.get('access'), (resource.dataset,))
    resource.lang = load_lang_data(context, resource.lang)
    resource.comments = load_comments(resource, resource.comments)
    resource.backend = load_resource_backend(
        context,
        resource,
        # First backend is loaded as string and later becomes Backend.
        resource.backend,
    )

    # Models will be added on `link` command.
    resource.models = {}
    return resource


def _check_unknown_keys(
    model: Model,
    keys: List[str],
    data: Dict[str, Any],
) -> None:
    # XXX: Similar to spinta.types.helpers.check_no_extra_keys.
    unknown = set(keys) - set(data)
    if unknown:
        raise MultipleErrors(
            PropertyNotFound(model, property=name)
            for name in sorted(unknown)
        )


@overload
@commands.load.register(Context, Entity, dict, Manifest)
def load(context: Context, entity: Entity, data: dict, manifest: Manifest):
    # XXX: https://gitlab.com/atviriduomenys/spinta/-/issues/44
    pkeys: List[str] = entity.pkeys or []
    if pkeys:
        _check_unknown_keys(entity.model, pkeys, entity.model.properties)
        entity.pkeys = [entity.model.properties[k] for k in pkeys]
    else:
        entity.unknown_primary_key = True
        entity.pkeys = sorted(
            take(entity.model.properties).values(),
            key=lambda p: p.place,
        )

    if entity.prepare:
        entity.prepare = asttoexpr(entity.prepare)
    return entity


@overload
@commands.load.register(Context, Attribute, dict, Manifest)
def load(context: Context, attr: Attribute, data: dict, manifest: Manifest):
    if attr.prepare:
        attr.prepare = asttoexpr(attr.prepare)
    else:
        attr.prepare = NA
    return attr
