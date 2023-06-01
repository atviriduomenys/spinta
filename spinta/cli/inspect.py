from copy import copy
from typing import Any, List
from typing import Callable
from typing import Dict
from typing import Hashable
from typing import Iterator
from typing import Optional
from typing import Tuple
from typing import TypeVar

from typer import Argument
from typer import Context as TyperContext
from typer import Option
from typer import echo

from spinta import commands
from spinta.cli.helpers.store import load_manifest
from spinta.components import Context, Property, Node, Namespace
from spinta.components import Mode
from spinta.core.config import RawConfig
from spinta.core.config import ResourceTuple
from spinta.core.config import parse_resource_args
from spinta.core.context import configure_context
from spinta.datasets.components import Dataset, Resource, Attribute, Entity, ExternalBackend
from spinta.dimensions.prefix.components import UriPrefix
from spinta.manifests.components import Manifest
from spinta.manifests.components import ManifestPath
from spinta.manifests.helpers import get_manifest_from_type, init_manifest
from spinta.manifests.tabular.helpers import render_tabular_manifest
from spinta.manifests.tabular.helpers import write_tabular_manifest
from spinta.types.datatype import Ref, DataType, Array, Object, Denorm
from spinta.utils.naming import Deduplicator
from spinta.utils.schema import NA
from spinta.utils.schema import NotAvailable
from spinta.components import Model


def inspect(
    ctx: TyperContext,
    manifest: Optional[str] = Argument(None, help="Path to manifest."),
    resource: Optional[Tuple[str, str]] = Option(
        (None, None), '-r', '--resource',
        help=(
            "Resource type and source URI "
            "(-r sql sqlite:////tmp/db.sqlite)"
        ),
    ),
    formula: str = Option('', '-f', '--formula', help=(
        "Formula if needed, to prepare resource for reading"
    )),
    backend: Optional[str] = Option(None, '-b', '--backend', help=(
        "Backend connection string"
    )),
    output: Optional[str] = Option(None, '-o', '--output', help=(
        "Output tabular manifest in a specified file"
    )),
    auth: Optional[str] = Option(None, '-a', '--auth', help=(
        "Authorize as a client"
    )),
    priority: str = Option('manifest', '-p', '--priority', help=(
        "Merge property priority ('manifest' or 'external')"
    ))
):
    """Update manifest schema from an external data source"""
    if priority not in ['manifest', 'external']:
        echo(f"Priority \'{priority}\' does not exist, there can only be \'manifest\' or \'external\', it will be set to default 'manifest'.")
        priority = 'manifest'
    has_manifest_priority = priority == 'manifest'
    resources = parse_resource_args(*resource, formula)
    context = configure_context(
        ctx.obj,
        [manifest] if manifest else None,
        mode=Mode.external,
        backend=backend,
    )
    store = load_manifest(context, ensure_config_dir=True)
    old = store.manifest
    manifest = Manifest()
    init_manifest(context, manifest, 'inspect')
    commands.merge(context, manifest, manifest, old, has_manifest_priority)

    if not resources:
        resources = []
        for ds in old.datasets.values():
            for resource in ds.resources.values():
                external = resource.external
                if external == '' and resource.backend:
                    external = resource.backend.config['dsn']
                if not any(res.external == external for res in resources):
                    resources.append(ResourceTuple(type=resource.type, external=external, prepare=resource.prepare))

    if resources:
        for resource in resources:
            _merge(context, manifest, manifest, resource, has_manifest_priority)

    # Sort models for render
    sorted_models = {}
    for key, model in manifest.models.items():
        if key not in sorted_models.keys():
            if model.external and model.external.resource:
                resource = model.external.resource
                for resource_key, resource_model in resource.models.items():
                    if resource_key not in sorted_models.keys():
                        sorted_models[resource_key] = resource_model
            else:
                sorted_models[key] = model
    manifest.objects['model'] = sorted_models

    if output:
        write_tabular_manifest(output, manifest)
    else:
        echo(render_tabular_manifest(manifest))


def _merge(context: Context, manifest: Manifest, old: Manifest, resource: ResourceTuple, has_manifest_priority: bool):
    rc: RawConfig = context.get('rc')
    Manifest_ = get_manifest_from_type(rc, resource.type)
    path = ManifestPath(type=Manifest_.type, path=resource.external)
    context = configure_context(context, [path], mode=Mode.external)
    store = load_manifest(context)
    new = store.manifest
    commands.merge(context, manifest, old, new, has_manifest_priority)


@commands.merge.register(Context, Manifest, Manifest, Manifest, bool)
def merge(context: Context, manifest: Manifest, old: Manifest, new: Manifest, has_manifest_priority: bool) -> None:
    resource_list = []
    for ds in new.datasets.values():
        for res in ds.resources.values():
            resource_list.append(_resource_source_key(res))

    backends = zipitems(
        old.backends.values(),
        new.backends.values(),
        _backend_dsn
    )

    backend_deduplicator = Deduplicator("_{}")
    for backend in backends:
        for o, n in backend:
            if not (o and n):
                if o:
                    backend_deduplicator(o.name)
                if n:
                    name = backend_deduplicator(n.name)
                    n.name = name
            merge(context, manifest, o, n)
    datasets = zipitems(
        old.datasets.values(),
        new.datasets.values(),
        _dataset_resource_source_key,
    )

    for ds in datasets:
        for o, n in ds:
            commands.merge(context, manifest, o, n, has_manifest_priority)


@commands.merge.register(Context, Manifest, NotAvailable, ExternalBackend)
def merge(context: Context, manifest: Manifest, old: NotAvailable, new: ExternalBackend) -> None:
    manifest.backends[new.name] = new


@commands.merge.register(Context, Manifest, ExternalBackend, ExternalBackend)
def merge(context: Context, manifest: Manifest, old: ExternalBackend, new: ExternalBackend) -> None:
    manifest.backends[old.name] = old


@commands.merge.register(Context, Manifest, ExternalBackend, NotAvailable)
def merge(context: Context, manifest: Manifest, old: ExternalBackend, new: NotAvailable) -> None:
    manifest.backends[old.name] = old


@commands.merge.register(Context, Manifest, NotAvailable, Dataset, bool)
def merge(context: Context, manifest: Manifest, old: NotAvailable, new: Dataset, has_manifest_priority: bool) -> None:
    manifest.datasets[new.name] = new
    _merge_resources(context, manifest, old, new)

    dataset_models = _filter_models_for_dataset(new.manifest, new)
    deduplicator = Deduplicator()
    for model in dataset_models:
        model.name = deduplicator(model.name)
        merge(context, manifest, NA, model, has_manifest_priority)

    new.manifest = manifest


@commands.merge.register(Context, Manifest, Dataset, Dataset, bool)
def merge(context: Context, manifest: Manifest, old: Dataset, new: Dataset, has_manifest_priority: bool) -> None:
    old.id = coalesce(old.id, new.id)
    old.description = coalesce(old.description, new.description)
    old.lang = coalesce(old.lang, new.lang)
    old.name = coalesce(old.name, new.name)
    old.manifest = coalesce(old.manifest, new.manifest)
    old.website = coalesce(old.website, new.website)
    old.projects = coalesce(old.projects, new.projects)
    old.source = coalesce(old.source, new.source)
    old.owner = coalesce(old.owner, new.owner)
    old.given = coalesce(old.given, new.given)
    old.level = coalesce(old.level, new.level)
    old.access = coalesce(old.access, new.access)
    old.title = coalesce(old.title, new.title)
    old.resources = coalesce(old.resources, new.resources)
    old.prefixes = coalesce(old.prefixes, new.prefixes)
    if old.ns and new.ns and old.ns.name == new.ns.name:
        commands.merge(context, manifest, old.ns, new.ns)
    else:
        old.ns = coalesce(old.ns, new.ns)
    manifest.datasets[old.name] = old
    _merge_prefixes(context, manifest, old, new)
    _merge_resources(context, manifest, old, new)

    dataset_models = _filter_models_for_dataset(manifest, old)
    models = zipitems(
        dataset_models,
        new.manifest.models.values(),
        _model_source_key
    )
    resource_list = []
    for res in new.resources.values():
        resource_list.append(_resource_source_key(res))
    deduplicator = Deduplicator()
    for model in models:
        for om, nm in model:
            if om:
                deduplicator(om.name)
            if om and not nm:
                if om.external and om.external.resource:
                    if _resource_source_key(om.external.resource) in resource_list:
                        om.external.name = None
            if not om and nm:
                if nm.external:
                    nm.external.dataset = old
                    name = deduplicator(f"{nm.external.dataset.name}/{nm.basename}")
                    nm.name = name
            merge(context, manifest, om, nm, has_manifest_priority)


@commands.merge.register(Context, Manifest, Dataset, NotAvailable, bool)
def merge(context: Context, manifest: Manifest, old: Dataset, new: NotAvailable, has_manifest_priority: bool) -> None:
    return


@commands.merge.register(Context, Manifest, NotAvailable, UriPrefix)
def merge(context: Context, manifest: Manifest, old: NotAvailable, new: UriPrefix) -> None:
    dataset = new.parent
    manifest.datasets[dataset.name].prefixes[new.name] = new


@commands.merge.register(Context, Manifest, UriPrefix, UriPrefix)
def merge(context: Context, manifest: Manifest, old: UriPrefix, new: UriPrefix) -> None:
    old.type = coalesce(new.type, old.type)
    old.id = coalesce(new.id, old.id)
    old.uri = coalesce(new.uri, old.uri)
    old.title = coalesce(new.title, old.title)
    old.description = coalesce(new.description, old.description)

    old.name = coalesce(old.name, new.name)


@commands.merge.register(Context, Manifest, UriPrefix, NotAvailable)
def merge(context: Context, manifest: Manifest, old: UriPrefix, new: NotAvailable) -> None:
    return


@commands.merge.register(Context, Manifest, NotAvailable, Namespace)
def merge(context: Context, manifest: Manifest, old: NotAvailable, new: Namespace) -> None:
    manifest.namespaces[new.name] = new


@commands.merge.register(Context, Manifest, Namespace, Namespace)
def merge(context: Context, manifest: Manifest, old: Namespace, new: Namespace) -> None:
    old.keymap = coalesce(new.keymap, old.keymap)
    old.backend = coalesce(new.backend, old.backend)
    old.parent = coalesce(new.parent, old.parent)
    old.description = coalesce(new.description, old.description)
    old.generated = coalesce(new.generated, old.generated)
    old.lang = coalesce(new.lang, old.lang)
    old.enums = coalesce(new.enums, old.enums)

    old.name = coalesce(old.name, new.name)
    old.given = coalesce(old.given, new.given)
    old.title = coalesce(old.title, new.title)
    old.models = coalesce(old.models, new.models)
    old.access = coalesce(old.access, new.access)


@commands.merge.register(Context, Manifest, Namespace, NotAvailable)
def merge(context: Context, manifest: Manifest, old: Namespace, new: NotAvailable) -> None:
    return


@commands.merge.register(Context, Manifest, NotAvailable, Resource)
def merge(context: Context, manifest: Manifest, old: NotAvailable, new: Resource) -> None:
    manifest.datasets[new.dataset.name].resources[new.name] = new


@commands.merge.register(Context, Manifest, Resource, Resource)
def merge(context: Context, manifest: Manifest, old: Resource, new: Resource) -> None:
    old.type = coalesce(new.type, old.type)
    old.description = coalesce(new.description, old.description)
    old.comments = coalesce(new.comments, old.comments)
    old.lang = coalesce(new.lang, old.lang)

    old.backend = coalesce(old.backend, new.backend)
    old.dataset = coalesce(old.dataset, new.dataset)
    old.models = old.models
    old.eid = coalesce(old.eid, new.eid)
    old.given = coalesce(old.given, new.given)
    old.access = coalesce(old.access, new.access)
    old.title = coalesce(old.title, new.title)
    old.name = coalesce(old.name, new.name)
    old.level = coalesce(old.level, new.level)
    old.external = old.external
    old.prepare = coalesce(old.prepare, new.prepare)


@commands.merge.register(Context, Manifest, Resource, NotAvailable)
def merge(context: Context, manifest: Manifest, old: Resource, new: NotAvailable) -> None:
    return


@commands.merge.register(Context, Manifest, NotAvailable, Model, bool)
def merge(context: Context, manifest: Manifest, old: NotAvailable, new: Model, has_manifest_priority: bool) -> None:
    old = copy(new)
    old.external = copy(old.external)
    if old.external and old.external.resource:
        resources = zipitems(
            old.external.dataset.resources.values(),
            [old.external.resource],
            _resource_source_key
        )
        for res in resources:
            for old_res, new_res in res:
                if old_res and new_res:
                    old.external.resource = old_res
                    old_res.models[old.name] = old
    old.manifest = manifest
    manifest.models[old.name] = old

    _merge_model_properties(context, manifest, old, new, has_manifest_priority)


@commands.merge.register(Context, Manifest, Model, Model, bool)
def merge(context: Context, manifest: Manifest, old: Model, new: Model, has_manifest_priority: bool) -> None:
    old.description = coalesce(new.description, old.description)
    old.comments = coalesce(new.comments, old.comments)
    old.lang = coalesce(new.lang, old.lang)

    old.id = coalesce(old.id, new.id)
    old.eid = coalesce(old.eid, new.eid)
    old.base = coalesce(old.base, new.base)
    old.given = coalesce(old.given, new.given)
    old.name = coalesce(old.name, new.name)
    old.title = coalesce(old.title, new.title)
    old.access = coalesce(old.access, new.access)
    old.level = coalesce(old.level, new.level)
    old.external = coalesce(old.external, new.external)
    old.manifest = manifest

    manifest.models[old.name] = old
    _merge_model_properties(context, manifest, old, new, has_manifest_priority)

    if old.external and new.external:
        if not has_manifest_priority or (old.external.unknown_primary_key and not new.external.unknown_primary_key):
            keys = zipitems(
                old.properties.values(),
                new.external.pkeys,
                _property_source_key
            )
            new_keys = []
            for key in keys:
                for o, n in key:
                    if o and n:
                        new_keys.append(o)
            if new_keys:
                old.external.pkeys = new_keys
                old.external.unknown_primary_key = new.external.unknown_primary_key


@commands.merge.register(Context, Manifest, Model, NotAvailable, bool)
def merge(context: Context, manifest: Manifest, old: Model, new: NotAvailable, has_manifest_priority: bool) -> None:
    if old.external and not old.external.name:
        for prop in old.properties.values():
            if prop.external:
                prop.external.name = None


@commands.merge.register(Context, Manifest, NotAvailable, Property, bool)
def merge(context: Context, manifest: Manifest, old: NotAvailable, new: Property, has_manifest_priority: bool) -> None:
    if new.external:
        new.external.prop = new
    new.dtype.prop = new
    merge(context, manifest, new.dtype, new.dtype)
    new.model.properties[new.name] = new


@commands.merge.register(Context, Manifest, Property, Property, bool)
def merge(context: Context, manifest: Manifest, old: Property, new: Property, has_manifest_priority: bool) -> None:
    if not has_manifest_priority:
        merged = old
        merged.type = coalesce(new.type, old.type)
        merged.uri = coalesce(new.uri, old.uri)
        merged.description = coalesce(new.description, old.description)
        merged.enum = coalesce(new.enum, old.enum)
        merged.enums = coalesce(new.enums, old.enums)
        merged.unit = coalesce(new.unit, old.unit)
        merged.comments = coalesce(new.comments, old.comments)
        merged.lang = coalesce(new.lang, old.lang)

        merged.model = coalesce(old.model, new.model)
        merged.external = coalesce(old.external, new.external)
        merged.place = coalesce(old.place, new.place)
        merged.name = coalesce(old.name, new.name)
        merged.given = coalesce(old.given, new.given)
        merged.title = coalesce(old.title, new.title)
        merged.level = coalesce(old.level, new.level)
        merged.access = coalesce(old.access, new.access)
        merged.dtype.prop = merged

        old.model.properties[old.name] = merged

        merge(context, manifest, merged.dtype, new.dtype)


@commands.merge.register(Context, Manifest, Property, NotAvailable, bool)
def merge(context: Context, manifest: Manifest, old: Property, new: NotAvailable, has_manifest_priority: bool) -> None:
    if old.external:
        old.external.name = None
    manifest.models[old.model.name].properties[old.name] = old


@commands.merge.register(Context, Manifest, DataType, Array)
def merge(context: Context, manifest: Manifest, old: DataType, new: Array) -> None:
    merged = new
    merged.type_args = coalesce(new.type_args, old.type_args)
    merged.unique = coalesce(new.unique, old.unique)
    merged.nullable = coalesce(new.nullable, old.nullable)
    merged.required = coalesce(new.required, old.required)
    merged.default = coalesce(new.default, old.default)
    merged.name = coalesce(new.name, old.name)

    merged.backend = coalesce(old.backend, new.backend)
    merged.prop = coalesce(old.prop, new.prop)
    merged.choices = coalesce(old.choices, new.choices)
    merged.prepare = coalesce(old.prepare, new.prepare)
    models = zipitems(
        [merged.items.model],
        manifest.models.values(),
        _model_source_key
    )
    for model in models:
        for o, n in model:
            if o and n:
                properties = zipitems(
                    merged.items,
                    o.properties.values(),
                    _property_source_key
                )
                for prop in properties:
                    for po, pn in prop:
                        if po and pn:
                            merged.items = po
                            break
                    else:
                        continue
                    break
            break
        else:
            continue
        break

    old.prop.dtype = merged


@commands.merge.register(Context, Manifest, DataType, Object)
def merge(context: Context, manifest: Manifest, old: DataType, new: Object) -> None:
    merged = new
    merged.type_args = coalesce(new.type_args, old.type_args)
    merged.unique = coalesce(new.unique, old.unique)
    merged.nullable = coalesce(new.nullable, old.nullable)
    merged.required = coalesce(new.required, old.required)
    merged.default = coalesce(new.default, old.default)
    merged.name = coalesce(new.name, old.name)

    merged.backend = coalesce(old.backend, new.backend)
    merged.prop = coalesce(old.prop, new.prop)
    merged.choices = coalesce(old.choices, new.choices)
    merged.prepare = coalesce(old.prepare, new.prepare)

    new_properties = {}
    for key, value in merged.properties.items():
        new_key = key
        new_value = value
        models = zipitems(
            [value.model],
            manifest.models.values(),
            _model_source_key
        )
        for model in models:
            for o, n in model:
                if o and n:
                    properties = zipitems(
                        merged.items,
                        o.properties.values(),
                        _property_source_key
                    )
                    for prop in properties:
                        for po, pn in prop:
                            if po and pn:
                                new_value = po
                                new_key = po.name
                                break
                        else:
                            continue
                        break
                break
            else:
                continue
            break
        new_properties[new_key] = new_value
    merged.properties = new_properties

    old.prop.dtype = merged


@commands.merge.register(Context, Manifest, DataType, Ref)
def merge(context: Context, manifest: Manifest, old: DataType, new: Ref) -> None:
    merged = copy(new)
    merged.type_args = coalesce(new.type_args, old.type_args)
    merged.unique = coalesce(new.unique, old.unique)
    merged.nullable = coalesce(new.nullable, old.nullable)
    merged.required = coalesce(new.required, old.required)
    merged.default = coalesce(new.default, old.default)
    merged.name = coalesce(new.name, old.name)

    merged.backend = coalesce(old.backend, new.backend)
    merged.prop = coalesce(old.prop, new.prop)
    merged.choices = coalesce(old.choices, new.choices)
    merged.prepare = coalesce(old.prepare, new.prepare)

    models = zipitems(
        [merged.model],
        manifest.models.values(),
        _model_source_key
    )
    for model in models:
        for o, n in model:
            if o and n:
                if n.external and merged.prop.model.external:
                    if n.external.dataset is merged.prop.model.external.dataset:
                        merged.model = n
                        if not n.external.unknown_primary_key and n.external.pkeys:
                            merged.refprops = n.external.pkeys
                        else:
                            new_refprops = []
                            properties = zipitems(
                                merged.refprops,
                                n.properties.values(),
                                _property_source_key
                            )
                            for prop in properties:
                                for po, pn in prop:
                                    if po and pn:
                                        new_refprops.append(pn)

                            if not new_refprops:
                                new_refprops = merged.refprops
                            merged.refprops = new_refprops
                        break
        else:
            continue
        break
    old.prop.dtype = merged


@commands.merge.register(Context, Manifest, DataType, Denorm)
def merge(context: Context, manifest: Manifest, old: DataType, new: Denorm) -> None:
    merged = new
    merged.type_args = coalesce(new.type_args, old.type_args)
    merged.unique = coalesce(new.unique, old.unique)
    merged.nullable = coalesce(new.nullable, old.nullable)
    merged.required = coalesce(new.required, old.required)
    merged.default = coalesce(new.default, old.default)
    merged.name = coalesce(new.name, old.name)

    merged.backend = coalesce(old.backend, new.backend)
    merged.prop = coalesce(old.prop, new.prop)
    merged.choices = coalesce(old.choices, new.choices)
    merged.prepare = coalesce(old.prepare, new.prepare)

    models = zipitems(
        [merged.rel_prop.model],
        manifest.models.values(),
        _model_source_key
    )
    for model in models:
        for o, n in model:
            if o and n:
                properties = zipitems(
                    merged.items,
                    o.properties.values(),
                    _property_source_key
                )
                for prop in properties:
                    for po, pn in prop:
                        if po and pn:
                            merged.rel_prop = po
                            break
                    else:
                        continue
                    break
            break
        else:
            continue
        break

    old.prop.dtype = merged


@commands.merge.register(Context, Manifest, DataType, DataType)
def merge(context: Context, manifest: Manifest, old: DataType, new: DataType) -> None:
    merged = new
    merged.name = coalesce(new.name, old.name)
    merged.type_args = coalesce(new.type_args, old.type_args)
    merged.unique = coalesce(new.unique, old.unique)
    merged.nullable = coalesce(new.nullable, old.nullable)
    merged.required = coalesce(new.required, old.required)
    merged.default = coalesce(new.default, old.default)

    merged.backend = coalesce(old.backend, new.backend)
    merged.prop = coalesce(old.prop, new.prop)
    merged.choices = coalesce(old.choices, new.choices)
    merged.prepare = coalesce(old.prepare, new.prepare)
    old.prop.dtype = merged


def _filter_models_for_dataset(
    manifest: Manifest,
    dataset: Dataset
) -> List[Model]:
    models = []
    for model in manifest.models.values():
        if model.external:
            if model.external.dataset is dataset:
                models.append(model)
    return models


def _merge_model_properties(
    context: Context,
    manifest: Manifest,
    old: Model,
    new: Model,
    has_manifest_priority: bool
):
    properties = zipitems(
        old.properties.values(),
        new.properties.values(),
        _property_source_key,
    )
    deduplicator = Deduplicator("_{}")
    for prop in properties:
        for o, n in prop:
            if n:
                n = copy(n)
            if o:
                deduplicator(o.basename)
            if n and not o:
                name = deduplicator(n.basename)
                n.model = old
                n.name = name
                n.place = name
            commands.merge(context, manifest, o, n, has_manifest_priority)


def _merge_prefixes(
    context: Context,
    manifest: Manifest,
    old: Dataset,
    new: Dataset
):
    prefixes = zipitems(
        old.prefixes.values(),
        new.prefixes.values(),
        _name_key,
    )
    for prefix in prefixes:
        for o, n in prefix:
            commands.merge(context, manifest, o, n)


def _merge_resources(
    context: Context,
    manifest: Manifest,
    old: Dataset,
    new: Dataset
):
    old_resources = [] if old == NA else old.resources.values()
    new_resources = [] if new == NA else new.resources.values()
    resources = zipitems(
        old_resources,
        new_resources,
        _resource_source_key,
    )
    for res in resources:
        for o, n in res:
            commands.merge(context, manifest, o, n)


TItem = TypeVar('TItem')


def zipitems(
    a: TItem,
    b: TItem,
    key: Callable[[TItem], Hashable],
) -> Iterator[List[Tuple[TItem, TItem]]]:

    res: Dict[
        Hashable,  # key
        List[
            Tuple[
                Any,   # a
                Any,   # b
            ]
        ]
        ,
    ] = {}

    for v in a:
        k = key(v)
        if isinstance(k, Tuple):
            for keys in res.keys():
                if set(k) == set(keys):
                    res[keys].append([v, NA])
                    break
            else:
                res[k] = [[v, NA]]
        else:
            if k not in res.keys():
                res[k] = []
            res[k].append([v, NA])
    for v in b:
        k = key(v)
        if isinstance(k, Tuple):
            found = False
            for keys in res.keys():
                if set(k).issubset(set(keys)):
                    found = True
                    additional = []
                    for item in res[keys]:
                        if item[1] is NA:
                            item[1] = v
                        else:
                            additional.append([item[0], v])
                    res[keys] += additional
            if not found:
                res[k] = [[NA, v]]
        else:
            if k in res:
                additional = []
                for item in res[k]:
                    if item[1] is NA:
                        item[1] = v
                    else:
                        additional.append([item[0], v])
                res[k] += additional
            else:
                res[k] = [[NA, v]]
    yield from res.values()


def coalesce(*args: Any) -> Any:
    for arg in args:
        if arg:
            return arg
    return arg


def _name_key(node: Node) -> str:
    return node.name


def _dataset_resource_source_key(dataset: Dataset) -> Tuple:
    keys = []
    for resource in dataset.resources.values():
        keys.append(_resource_source_key(resource))
    return tuple(keys)


def _property_source_key(prop: Property) -> str:
    if prop.external and prop.external.name:
        return prop.external.name
    else:
        return prop.name


def _resource_source_key(resource: Resource) -> str:
    result = resource.name
    manifest = resource.dataset.manifest
    if resource.external:
        result = resource.external
    else:
        if manifest.backends:
            if resource.backend.name in manifest.backends:
                result = manifest.backends[resource.backend.name].config['dsn']
        elif manifest.store.backends:
            if resource.backend.name in manifest.store.backends:
                result = manifest.store.backends[resource.backend.name].config['dsn']
    if "@" in result:
        result = result.split("@")[1]
    return result


def _backend_dsn(backend: ExternalBackend) -> str:
    dsn = backend.config['dsn']
    if "@" in dsn:
        dsn = dsn.split("@")[1]
    return dsn


def _model_source_key(model: Model) -> str:
    if model.external and model.external.name:
        return f'{_resource_source_key(model.external.resource)}/{model.external.name}'
    else:
        return model.name
