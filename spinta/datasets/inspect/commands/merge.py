from copy import copy

from spinta import commands
from spinta.components import Context, Property, Namespace
from spinta.components import Model
from spinta.datasets.components import Dataset, Resource, ExternalBackend
from spinta.datasets.inspect.helpers import (
    zipitems,
    _backend_dsn,
    _dataset_key,
    _merge_resources,
    _filter_models_for_dataset,
    coalesce,
    _merge_prefixes,
    _model_key,
    _resource_key,
    _merge_model_properties,
    _property_key,
)
from spinta.dimensions.prefix.components import UriPrefix
from spinta.manifests.components import Manifest
from spinta.types.datatype import Ref, DataType, Array, Object, Denorm
from spinta.utils.naming import Deduplicator
from spinta.utils.schema import NA
from spinta.utils.schema import NotAvailable


@commands.merge.register(Context, Manifest, Manifest, Manifest, bool)
def merge(context: Context, manifest: Manifest, old: Manifest, new: Manifest, has_manifest_priority: bool) -> None:
    backends = zipitems(old.backends.values(), new.backends.values(), _backend_dsn)

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
        commands.get_datasets(context, old).values(),
        commands.get_datasets(context, new).values(),
        _dataset_key,
    )
    deduplicator = Deduplicator("{}")
    for ds in datasets:
        for o, n in ds:
            if o:
                deduplicator(o.name)
            elif n and not o:
                name = deduplicator(n.name)
                n.name = name
                n.ns.name = name
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
    commands.set_dataset(context, manifest, new.name, new)
    _merge_resources(context, manifest, old, new)

    dataset_models = _filter_models_for_dataset(context, new.manifest, new)
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
    old.source = coalesce(old.source, new.source)
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
    commands.set_dataset(context, manifest, old.name, old)
    _merge_prefixes(context, manifest, old, new)
    _merge_resources(context, manifest, old, new)

    dataset_models = _filter_models_for_dataset(context, manifest, old)
    models = zipitems(dataset_models, commands.get_models(context, new.manifest).values(), _model_key)
    resource_list = []
    for res in new.resources.values():
        resource_list.append(_resource_key(res))
    deduplicator = Deduplicator()
    for model in models:
        for om, nm in model:
            if om:
                deduplicator(om.name)
            if om and not nm:
                if om.external and om.external.resource:
                    if _resource_key(om.external.resource) in resource_list:
                        om.external.name = None
            if not om and nm:
                if nm.external:
                    nm.external.dataset = old
                    name = deduplicator(f"{nm.external.dataset.name}/{nm.basename}")
                    nm.name = name
                    nm.ns.name = nm.external.dataset.name
            merge(context, manifest, om, nm, has_manifest_priority)


@commands.merge.register(Context, Manifest, Dataset, NotAvailable, bool)
def merge(context: Context, manifest: Manifest, old: Dataset, new: NotAvailable, has_manifest_priority: bool) -> None:
    return


@commands.merge.register(Context, Manifest, NotAvailable, UriPrefix)
def merge(context: Context, manifest: Manifest, old: NotAvailable, new: UriPrefix) -> None:
    dataset = new.parent
    commands.get_dataset(context, manifest, dataset.name).prefixes[new.name] = new


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
    commands.set_namespace(context, manifest, new.name, new)


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
    commands.get_dataset(context, manifest, new.dataset.name).resources[new.name] = new


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
    old_name = old.name
    if f"{old.ns.name}/{old.basename}" != old.name and old.ns.name:
        name = f"{old.ns.name}/{old.basename}"
        old.name = name
        new.name = name
    if old.external and old.external.resource:
        resources = zipitems(old.external.dataset.resources.values(), [old.external.resource], _resource_key)
        for res in resources:
            for old_res, new_res in res:
                if old_res and new_res:
                    old.external.resource = old_res
                    if old_name != old.name:
                        if old_name in old_res.models:
                            del old_res.models[old_name]
                    old_res.models[old.name] = old
    old.manifest = manifest
    commands.set_model(context, manifest, old.name, old)
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

    commands.set_model(context, manifest, old.name, old)
    _merge_model_properties(context, manifest, old, new, has_manifest_priority)

    if old.external and new.external:
        if not has_manifest_priority or (old.external.unknown_primary_key and not new.external.unknown_primary_key):
            keys = zipitems(old.properties.values(), new.external.pkeys, _property_key)
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
    model = commands.get_model(context, manifest, old.model.name)
    model.properties[old.name] = old


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
    models = zipitems([merged.items.model], commands.get_models(context, manifest).values(), _model_key)
    for model in models:
        for o, n in model:
            if o and n:
                properties = zipitems(merged.items, o.properties.values(), _property_key)
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
        models = zipitems([value.model], commands.get_models(context, manifest).values(), _model_key)
        for model in models:
            for o, n in model:
                if o and n:
                    properties = zipitems(merged.items, o.properties.values(), _property_key)
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

    models = zipitems([merged.model], commands.get_models(context, manifest).values(), _model_key)
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
                            properties = zipitems(merged.refprops, n.properties.values(), _property_key)
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

    models = zipitems([merged.rel_prop.model], commands.get_models(context, manifest).values(), _model_key)
    for model in models:
        for o, n in model:
            if o and n:
                properties = zipitems(merged.items, o.properties.values(), _property_key)
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
