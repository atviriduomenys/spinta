from __future__ import annotations

from typing import List
from typing import Optional
from typing import Union
from typing import overload

import itertools
from typing import Any
from typing import Dict
from typing import TYPE_CHECKING
from typing import cast

from spinta import commands
from spinta import exceptions
from spinta.auth import authorized
from spinta.commands import authorize
from spinta.commands import check
from spinta.commands import load
from spinta.components import Action, Component
from spinta.components import Base
from spinta.components import Context
from spinta.components import Mode
from spinta.components import Model
from spinta.components import Property
from spinta.core.access import link_access_param
from spinta.core.access import load_access_param
from spinta.datasets.enums import Level
from spinta.dimensions.comments.helpers import load_comments
from spinta.dimensions.enum.components import EnumValue
from spinta.dimensions.enum.components import Enums
from spinta.dimensions.enum.helpers import link_enums
from spinta.dimensions.enum.helpers import load_enums
from spinta.dimensions.lang.helpers import load_lang_data
from spinta.exceptions import KeymapNotSet, InvalidLevel
from spinta.exceptions import UndefinedEnum
from spinta.exceptions import UnknownPropertyType
from spinta.manifests.components import Manifest
from spinta.manifests.tabular.components import PropertyRow
from spinta.nodes import get_node
from spinta.nodes import load_model_properties
from spinta.nodes import load_node
from spinta.types.namespace import load_namespace_from_name
from spinta.units.helpers import is_unit
from spinta.utils.enums import enum_by_value
from spinta.utils.schema import NA
from spinta.types.datatype import Ref

if TYPE_CHECKING:
    from spinta.datasets.components import Attribute


def _load_namespace_from_model(context: Context, manifest: Manifest, model: Model):
    ns = load_namespace_from_name(context, manifest, model.name)
    ns.models[model.model_type()] = model
    model.ns = ns


@load.register(Context, Model, dict, Manifest)
def load(
    context: Context,
    model: Model,
    data: dict,
    manifest: Manifest,
    *,
    source: Manifest = None,
) -> Model:
    model.parent = manifest
    model.manifest = manifest
    model.mode = manifest.mode  # TODO: mode should be inherited from namespace.
    load_node(context, model, data)
    model.lang = load_lang_data(context, model.lang)
    model.comments = load_comments(model, model.comments)

    if model.keymap:
        model.keymap = manifest.store.keymaps[model.keymap]
    else:
        model.keymap = manifest.keymap

    manifest.add_model_endpoint(model)
    _load_namespace_from_model(context, manifest, model)
    load_access_param(model, data.get('access'), itertools.chain(
        [model.ns],
        model.ns.parents(),
    ))
    load_level(model, model.level)
    load_model_properties(context, model, Property, data.get('properties'))

    # XXX: Maybe it is worth to leave possibility to override _id access?
    model.properties['_id'].access = model.access

    config = context.get('config')

    if model.base:
        base: dict = model.base
        model.base = get_node(
            config,
            manifest,
            model.eid,
            base,
            group='nodes',
            ctype='base',
            parent=model,
        )
        load_node(context, model.base, base)
        model.base.model = model
        commands.load(context, model.base, base, manifest)
    if model.unique:
        unique_properties = []
        for unique_set in model.unique:
            prop_set = []
            for prop_name in unique_set:
                if "." in prop_name:
                    prop_name = prop_name.split(".")[0]
                prop_set.append(model.properties[prop_name])
            if prop_set:
                unique_properties.append(prop_set)
        model.unique = unique_properties

    if model.external:
        external: dict = model.external
        model.external = get_node(
            config,
            manifest,
            model.eid,
            external,
            group='datasets',
            ctype='entity',
            parent=model,
        )
        model.external.model = model
        load_node(context, model.external, external, parent=model)
        commands.load(context, model.external, external, manifest)
        model.given.pkeys = external.get('pk', [])
    else:
        model.external = None
        model.given.pkeys = []

    return model


@load.register(Context, Base, dict, Manifest)
def load(context: Context, base: Base, data: dict, manifest: Manifest) -> None:
    pass


@overload
@commands.link.register(Context, Model)
def link(context: Context, model: Model):
    # Link external source.
    if model.external:
        commands.link(context, model.external)

        if model.keymap is None:
            raise KeymapNotSet(model)

    # Link model backend.
    if model.backend:
        if model.backend in model.manifest.backends:
            model.backend = model.manifest.backends[model.backend]
        else:
            model.backend = model.manifest.store.backends[model.backend]
    elif (
        model.mode == Mode.external and
        model.external and
        model.external.resource and
        model.external.resource.backend
    ):
        model.backend = model.external.resource.backend
    else:
        model.backend = model.manifest.backend

    if model.external and model.external.dataset:
        link_access_param(model, itertools.chain(
            [model.external.dataset],
            [model.ns],
            model.ns.parents(),
        ))
    else:
        link_access_param(model, itertools.chain(
            [model.ns],
            model.ns.parents(),
        ))

    # Link base
    if model.base:
        commands.link(context, model.base)

    # Link model properties.
    for prop in model.properties.values():
        commands.link(context, prop)


@overload
@commands.link.register(Context, Base)
def link(context: Context, base: Base):
    base.parent = base.model.manifest.models[base.parent]
    base.pk = [
        base.parent.properties[pk]
        for pk in base.pk
    ] if base.pk else []


@load.register(Context, Property, dict, Manifest)
def load(
    context: Context,
    prop: Property,
    data: PropertyRow,
    manifest: Manifest,
) -> Property:
    config = context.get('config')
    prop.type = 'property'
    prop, data = load_node(context, prop, data, mixed=True)
    prop = cast(Property, prop)
    parents = list(itertools.chain(
        [prop.model, prop.model.ns],
        prop.model.ns.parents(),
    ))
    load_access_param(prop, prop.access, parents)
    prop.enums = load_enums(context, [prop] + parents, prop.enums)
    prop.lang = load_lang_data(context, prop.lang)
    prop.comments = load_comments(prop, prop.comments)
    load_level(prop, prop.level)

    if data['type'] is None:
        raise UnknownPropertyType(prop, type=data['type'])
    if data['type'] == 'ref' and prop.level and prop.level < 4:
        data['type'] = '_external_ref'

    prop.dtype = get_node(
        config,
        manifest,
        prop.model.eid,
        data,
        group='types',
        parent=prop,
    )
    prop.dtype.type = 'type'
    prop.dtype.prop = prop
    load_node(context, prop.dtype, data)
    if prop.model.external:
        prop.external = _load_property_external(context, manifest, prop, prop.external)
    else:
        prop.external = NA
    commands.load(context, prop.dtype, data, manifest)
    if prop.model.unique:
        if isinstance(prop.dtype, Ref):
            if '.id' not in prop.name:
                prop.model.unique = [list(map(lambda val: val.replace(
                    prop.name, prop.name + '._id'), val)) for val in prop.model.unique]
    unit: Optional[str] = prop.enum
    if unit is None:
        prop.given.enum = None
        prop.given.unit = None
        prop.enum = None
        prop.unit = None
    elif is_unit(prop.dtype, unit):
        prop.given.enum = None
        prop.given.unit = unit
        prop.enum = None
        prop.unit = unit
    else:
        prop.given.enum = unit

    return prop


def load_level(
    component: Component,
    given_level: Union[Level, int, str],
):
    if given_level:
        if isinstance(given_level, Level):
            level = given_level
        else:
            if isinstance(given_level, str) and given_level.isdigit():
                given_level = int(given_level)
            if not isinstance(given_level, int):
                raise InvalidLevel(component, level=given_level)
            level = enum_by_value(component, 'level', Level, given_level)
    else:
        level = None
    component.level = level


def _link_prop_enum(
    prop: Property,
) -> Optional[EnumValue]:
    if prop.given.enum:
        enums: List[Enums] = (
            [prop.enums] +
            [prop.model.ns.enums] +
            [ns.enums for ns in prop.model.ns.parents()] +
            [prop.model.manifest.enums]
        )
        for enums_ in enums:
            if enums_ and prop.given.enum in enums_:
                return enums_[prop.given.enum]
        if not is_unit(prop.dtype, prop.given.enum):
            raise UndefinedEnum(prop, name=prop.given.enum)
    elif prop.enums:
        return prop.enums.get('')


@overload
@commands.link.register(Context, Property)
def link(context: Context, prop: Property):
    commands.link(context, prop.dtype)
    if prop.external:
        if isinstance(prop.external, list):
            for external in prop.external:
                commands.link(context, external)
        else:
            commands.link(context, prop.external)

    model = prop.model

    if prop.model.external and prop.model.external.dataset:
        parents = list(itertools.chain(
            [model],
            [model.external.dataset],
            [model.ns],
            model.ns.parents(),
        ))
    else:
        parents = list(itertools.chain(
            [model],
            [model.ns],
            model.ns.parents(),
        ))
    link_access_param(prop, parents, use_given=not prop.name.startswith('_'))
    link_enums([prop] + parents, prop.enums)
    prop.enum = _link_prop_enum(prop)


def _load_property_external(
    context: Context,
    manifest: Manifest,
    prop: Property,
    data: Any,  # external data
) -> Attribute:
    if not isinstance(data, dict):
        return _load_property_external(context, manifest, prop, {'name': data})

    config = context.get('config')
    external: Attribute = get_node(
        config,
        manifest,
        prop.model.eid,
        data,
        group='datasets',
        ctype='attribute',
        parent=prop,
    )
    load_node(context, external, data, parent=prop)
    commands.load(context, external, data, manifest)
    return external


@load.register(Context, Model, dict)
def load(context: Context, model: Model, data: dict) -> dict:
    # check that given data does not have more keys, than model's schema
    non_hidden_keys = []
    for key, prop in model.properties.items():
        if not prop.hidden:
            non_hidden_keys.append(key)

    unknown_props = set(data.keys()) - set(non_hidden_keys)
    if unknown_props:
        raise exceptions.MultipleErrors(
            exceptions.FieldNotInResource(model, property=prop)
            for prop in sorted(unknown_props)
        )

    result = {}
    for name, prop in model.properties.items():
        value = data.get(name, NA)
        value = load(context, prop.dtype, value)
        if value is not NA:
            result[name] = value
    return result


@load.register(Context, Property, object)
def load(context: Context, prop: Property, value: object) -> object:
    value = _prepare_prop_data(prop.name, value)
    value[prop.name] = load(context, prop.dtype, value[prop.name])
    return value


def _prepare_prop_data(name: str, data: dict):
    return {
        **{
            k: v
            for k, v in data.items()
            if k.startswith('_') and k not in ('_id', '_content_type')
        },
        name: {
            k: v
            for k, v in data.items()
            if not k.startswith('_') or k in ('_id', '_content_type')
        }
    }


@check.register(Context, Model)
def check(context: Context, model: Model):
    if '_id' not in model.properties:
        raise exceptions.MissingRequiredProperty(model, prop='_id')

    for prop in model.properties.values():
        commands.check(context, prop)


@check.register(Context, Property)
def check(context: Context, prop: Property):
    if prop.enum:
        for value, item in prop.enum.items():
            commands.check(context, item, prop.dtype, item.prepare)


@authorize.register(Context, Action, Model)
def authorize(context: Context, action: Action, model: Model):
    authorized(context, model, action, throw=True)


@authorize.register(Context, Action, Property)
def authorize(context: Context, action: Action, prop: Property):
    authorized(context, prop, action, throw=True)


@overload
@commands.get_error_context.register(Model)
def get_error_context(model: Model, *, prefix='this') -> Dict[str, str]:
    context = commands.get_error_context(model.manifest, prefix=f'{prefix}.manifest')
    context['schema'] = f'{prefix}.get_eid_for_error_context()'
    context['model'] = f'{prefix}.name'
    context['dataset'] = f'{prefix}.external.dataset.name'
    context['resource'] = f'{prefix}.external.resource.name'
    context['resource.backend'] = f'{prefix}.external.resource.backend.name'
    context['entity'] = f'{prefix}.external.name'
    return context


@overload
@commands.get_error_context.register(Property)
def get_error_context(prop: Property, *, prefix='this') -> Dict[str, str]:
    context = commands.get_error_context(prop.model, prefix=f'{prefix}.model')
    context['property'] = f'{prefix}.place'
    context['attribute'] = f'{prefix}.external.name'
    return context
