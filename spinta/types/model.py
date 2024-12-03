from __future__ import annotations

import itertools
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import TYPE_CHECKING
from typing import Union
from typing import cast
from typing import overload

from spinta import commands
from spinta import exceptions
from spinta.auth import authorized
from spinta.backends.constants import BackendFeatures
from spinta.backends.nobackend.components import NoBackend
from spinta.commands import authorize
from spinta.commands import check
from spinta.commands import load
from spinta.components import Action, Component, PageBy, Page, PageInfo, UrlParams, pagination_enabled
from spinta.components import Base
from spinta.components import Context
from spinta.components import Mode
from spinta.components import Model
from spinta.components import Property
from spinta.core.access import link_access_param
from spinta.core.access import load_access_param
from spinta.core.enums import Level
from spinta.datasets.components import ExternalBackend
from spinta.dimensions.comments.helpers import load_comments
from spinta.dimensions.enum.components import EnumValue
from spinta.dimensions.enum.components import Enums
from spinta.dimensions.enum.helpers import link_enums
from spinta.dimensions.enum.helpers import load_enums
from spinta.dimensions.lang.helpers import load_lang_data
from spinta.dimensions.param.helpers import load_params
from spinta.exceptions import KeymapNotSet, InvalidLevel
from spinta.exceptions import PropertyNotFound
from spinta.exceptions import UndefinedEnum
from spinta.exceptions import UnknownPropertyType
from spinta.hacks.urlparams import extract_params_sort_values
from spinta.manifests.components import Manifest
from spinta.manifests.tabular.components import PropertyRow
from spinta.nodes import get_node
from spinta.nodes import load_model_properties
from spinta.nodes import load_node
from spinta.types.helpers import check_model_name
from spinta.types.helpers import check_property_name
from spinta.types.namespace import load_namespace_from_name
from spinta.ufuncs.loadbuilder.components import LoadBuilder
from spinta.ufuncs.loadbuilder.helpers import page_contains_unsupported_keys, get_allowed_page_property_types
from spinta.units.helpers import is_unit
from spinta.utils.enums import enum_by_value
from spinta.utils.schema import NA

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
    model.given.name = data.get("given_name", None)
    if model.keymap:
        model.keymap = manifest.store.keymaps[model.keymap]
    else:
        model.keymap = manifest.keymap

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
            context,
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
                if prop_name not in model.properties:
                    raise PropertyNotFound(model, property=prop_name)
                prop_set.append(model.properties[prop_name])
            if prop_set:
                unique_properties.append(prop_set)
        model.unique = unique_properties

    if model.external:
        external: dict = model.external
        model.external = get_node(
            context,
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

    builder = LoadBuilder(context)
    builder.update(model=model)
    builder.load_page()

    model.params = load_params(context, manifest, model.params)

    if not model.name.startswith('_') and not model.basename[0].isupper():
        raise Exception(model.basename, "MODEL NAME NEEDS TO BE UPPER CASED")

    return model


@load.register(Context, Base, dict, Manifest)
def load(context: Context, base: Base, data: dict, manifest: Manifest) -> None:
    load_level(base, data['level'])


@overload
@commands.link.register(Context, Model)
def link(context: Context, model: Model):
    # Link external source.
    if model.external:
        commands.link(context, model.external)

        if model.keymap is None:
            raise KeymapNotSet(model)

    # Link model backend.
    if model.backend and isinstance(model.backend, str):
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
    elif model.mode == Mode.external:
        model.backend = NoBackend()
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

    _link_model_page(model)


def _disable_page_in_model(model: Model):
    model.page.enabled = False


def _link_model_page(model: Model):
    if not model.backend or not model.backend.supports(BackendFeatures.PAGINATION):
        _disable_page_in_model(model)
        return
    # Disable page if external backend and model.ref not given
    if isinstance(model.backend, ExternalBackend):
        if (model.external and not model.external.name) or not model.external:
            _disable_page_in_model(model)
            return

        if '_id' in model.page.keys:
            model.page.keys.pop('_id')
        if '-_id' in model.page.keys:
            model.page.keys.pop('-_id')
    else:
        # Force '_id' to be page key if other keys failed the checks
        if not model.page.enabled and page_contains_unsupported_keys(model.page):
            model.page.keys = {'_id': model.properties['_id']}
            model.page.enabled = True

        # Add _id to internal, if it's not added
        if '_id' not in model.page.keys and '-_id' not in model.page.keys:
            model.page.keys['_id'] = model.properties["_id"]

    if len(model.page.keys) == 0:
        _disable_page_in_model(model)


@overload
@commands.link.register(Context, Base)
def link(context: Context, base: Base):
    base.parent = commands.get_model(context, base.model.manifest, base.parent)
    base.pk = [
        base.parent.properties[pk]
        for pk in base.pk
    ] if base.pk else []
    if commands.identifiable(base):
        if base.pk and base.pk != base.parent.external.pkeys:
            base.parent.add_keymap_property_combination(base.pk)


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
    if prop.prepare_given:
        prop.given.prepare = prop.prepare_given
    load_level(prop, prop.level)

    if data['type'] is None:
        raise UnknownPropertyType(prop, type=data['type'])
    if data['type'] == 'ref' and not commands.identifiable(prop):
        data['type'] = '_external_ref'
    prop.dtype = get_node(
        context,
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
    # if prop.model.unique:
    #     if isinstance(prop.dtype, Ref):
    #         if '.id' not in prop.name:
    #             prop.model.unique = [list(map(lambda val: val.replace(
    #                 prop.name, prop.name + '._id'), val)) for val in prop.model.unique]
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
    prop.given.explicit = prop.explicitly_given if prop.explicitly_given is not None else True
    prop.given.name = prop.given_name
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
        context,
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
    # This load function should be called only with /Model/prop (subresource)
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
    check_model_name(context, model)
    if '_id' not in model.properties:
        raise exceptions.MissingRequiredProperty(model, prop='_id')

    for prop in model.properties.values():
        commands.check(context, prop)


@check.register(Context, Property)
def check(context: Context, prop: Property):
    check_property_name(context, prop)
    if prop.enum:
        for value, item in prop.enum.items():
            commands.check(context, item, prop.dtype, item.prepare)

    commands.check(context, prop.dtype)


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


@overload
@commands.get_error_context.register(Page)
def get_error_context(prop: Page, *, prefix='this') -> Dict[str, str]:
    context = commands.get_error_context(prop.model, prefix=f'{prefix}.model')
    context['page'] = f'{prefix}.get_repr_for_error()'
    return context


@commands.identifiable.register(Model)
def identifiable(model: Model):
    return model.level is None or model.level >= Level.identifiable


@commands.identifiable.register(Base)
def identifiable(base: Base):
    return base.level is None or base.level >= Level.identifiable


@commands.identifiable.register(Property)
def identifiable(prop: Property):
    return prop.level is None or prop.level >= Level.identifiable


@commands.create_page.register(PageInfo)
def create_page(page_info: PageInfo) -> Page:
    return Page(
        model=page_info.model,
        size=page_info.size,
        enabled=page_info.enabled,
        by={key: PageBy(prop) for key, prop in page_info.keys.items()}
    )


@commands.create_page.register(PageInfo, UrlParams)
def create_page(page_info: PageInfo, params: UrlParams) -> Page:
    params_page = params.page

    enabled = pagination_enabled(page_info.model, params)
    sort_values = extract_params_sort_values(
        page_info.model,
        params
    )
    if sort_values is None:
        enabled = False

    page_by = {key: PageBy(prop) for key, prop in page_info.keys.items()}
    if enabled and sort_values:
        new_order = {}
        allowed_types = get_allowed_page_property_types()
        for key, value in sort_values.items():
            if not isinstance(value.dtype, allowed_types):
                enabled = False
                break

            new_order[key] = PageBy(value)

        for key, value in page_by.items():
            reversed_key = key[1:] if key.startswith("-") else f'-{key}'
            if key not in new_order and reversed_key not in new_order:
                new_order[key] = value

        page_by = new_order

    page = Page(
        model=page_info.model,
        size=params_page and params_page.size or page_info.size,
        enabled=enabled,
        by=page_by
    )

    if enabled and params_page and params_page.values:
        page.update_values_from_list(params_page.values)

    return page


@commands.create_page.register(PageInfo, dict)
def create_page(page_info: PageInfo, data: dict) -> Page:
    page = commands.create_page(page_info)
    model = page_info.model
    for key, value in data.items():
        prop = model.properties.get(key)
        if prop:
            page.add_prop(key, prop, value)
    return page


@commands.create_page.register(PageInfo, (list, set, tuple))
def create_page(page_info: PageInfo, data: Union[list, set, tuple]) -> Page:
    page = commands.create_page(page_info)
    page.update_values_from_list(data)
    return page


@commands.create_page.register(PageInfo, object)
def create_page(page_info: PageInfo, data: Any) -> Page:
    return commands.create_page(page_info)
