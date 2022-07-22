from typing import Any
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypeVar
from typing import Union

from spinta import commands
from spinta import exceptions
from spinta import spyna
from spinta.auth import authorized
from spinta.backends import Backend
from spinta.backends.components import SelectTree
from spinta.backends.components import BackendOrigin
from spinta.components import Action
from spinta.components import Component
from spinta.components import Context
from spinta.components import Model
from spinta.components import Namespace
from spinta.components import Property
from spinta.types.datatype import DataType
from spinta.utils.data import take
from spinta.backends.constants import TableType


def load_backend(
    context: Context,
    component: Component,
    name: str,
    origin: BackendOrigin,
    data: Dict[str, str]
) -> Backend:
    config = context.get('config')
    type_ = data.get('type')
    if not type_:
        raise exceptions.RequiredConfigParam(
            component,
            name=f'backends.{name}.type',
        )
    if type_ not in config.components['backends']:
        raise exceptions.BackendNotFound(component, name=type_)
    Backend_ = config.components['backends'][type_]
    backend: Backend = Backend_()
    backend.type = type_
    backend.name = name
    backend.origin = origin
    backend.config = data
    commands.load(context, backend, data)
    return backend


def get_select_tree(
    context: Context,
    action: Action,
    select: Optional[List[str]],
) -> SelectTree:
    select = _apply_always_show_id(context, action, select)
    if select is None and action in (Action.GETALL, Action.SEARCH):
        # If select is not given, select everything.
        select = {'*': {}}
    return flat_select_to_nested(select)


def _apply_always_show_id(
    context: Context,
    action: Action,
    select: Optional[List[str]],
) -> Optional[List[str]]:
    if action in (Action.GETALL, Action.SEARCH):
        config = context.get('config')
        if config.always_show_id:
            if select is None:
                return ['_id']
            elif '_id' not in select:
                return ['_id'] + select
    return select


def get_select_prop_names(
    context: Context,
    node: Union[Model, Property, DataType],
    props: Dict[str, Property],
    action: Action,
    select: SelectTree,
    *,
    # If False, do not check if client has access to this property.
    auth: bool = True,
    # Allowed reserved property names.
    reserved: List[str] = None,
) -> List[str]:
    known = set(reserved or []) | set(take(props))
    check_unknown_props(node, select, known)

    if select is None or '*' in select:
        return [
            p.name
            for p in props.values() if (
                not p.name.startswith('_') and
                not p.hidden and
                (not auth or authorized(context, p, action))
            )
        ]
    else:
        return list(select)


def select_model_props(
    model: Model,
    prop_names: List[str],
    value: dict,
    select: SelectTree,
    reserved: List[str],
) -> Iterator[Tuple[
    Union[Property, str],
    Any,
    SelectTree,
]]:
    yield from select_props(
        model,
        reserved,
        model.properties,
        value,
        select,
    )
    yield from select_props(
        model,
        prop_names,
        model.properties,
        value,
        select,
        reserved=False,
    )


T = TypeVar('T')


def select_props(
    node: Union[Namespace, Model, Property],
    keys: Iterable[str],
    props: Dict[str, Property],
    value: Dict[str, T],
    select: SelectTree,
    *,
    reserved: bool = True,
) -> Iterator[Tuple[
    Union[Property, str],
    T,
    SelectTree,
]]:
    for key, val, sel in select_keys(keys, value, select, reserved=reserved):
        prop = _select_prop(key, props, node)
        if prop:
            yield prop, val, sel


def select_only_props(
    node: Union[Namespace, Model, Property],
    keys: Iterable[str],
    props: Dict[str, Property],
    select: SelectTree,
    *,
    reserved: bool = True,
) -> Iterator[Tuple[
    Union[Property, str],
    SelectTree,
]]:
    for key, sel in select_only_keys(keys, select, reserved=reserved):
        prop = _select_prop(key, props, node)
        if prop:
            yield prop, sel


def _select_prop(
    key: str,
    props: Dict[str, Property],
    node: Union[Namespace, Model, Property],
) -> Optional[Property]:
    if key not in props:
        # FIXME: We should check select list at the very beginning of
        #        request, not when returning results.
        raise exceptions.FieldNotInResource(node, property=key)
    prop = props[key]
    if not prop.hidden:
        return prop


def select_keys(
    keys: Iterable[str],
    value: Dict[str, T],
    select: SelectTree,
    *,
    reserved: bool = True,
) -> Iterator[Tuple[
    str,
    T,
    SelectTree,
]]:
    for key, sel in select_only_keys(keys, select, reserved=reserved):
        if select is None and key not in value:
            # Omit all keys if they are not present in value, this is a common
            # case in PATCH requests.
            continue

        if key in value:
            val = value[key]
        else:
            val = None

        yield key, val, sel


def select_only_keys(
    keys: Iterable[str],
    select: SelectTree,
    *,
    reserved: bool = True,
) -> Iterator[Tuple[
    str,
    SelectTree,
]]:
    for key in keys:
        if reserved is False and key.startswith('_'):
            continue

        if select is None:
            sel = None
        elif '*' in select:
            sel = select['*']
        elif key in select:
            sel = select[key]
        else:
            continue

        if sel is not None and sel == {}:
            sel = {'*': {}}

        yield key, sel


# FIXME: We should check select list at the very beginning of
#        request, not when returning results.
def check_unknown_props(
    node: Union[Model, Property, DataType],
    select: Optional[Iterable[str]],
    known: Iterable[str],
):
    unknown_properties = set(select or []) - set(known) - {'*'}
    if unknown_properties:
        raise exceptions.MultipleErrors(
            exceptions.FieldNotInResource(node, property=prop)
            for prop in sorted(unknown_properties)
        )


def flat_select_to_nested(select: Optional[List[str]]) -> SelectTree:
    """
    >>> flat_select_to_nested(None)

    >>> flat_select_to_nested(['foo.bar'])
    {'foo': {'bar': {}}}

    """
    if select is None:
        return None

    res = {}
    for v in select:
        if isinstance(v, dict):
            v = spyna.unparse(v)
        names = v.split('.')
        vref = res
        for name in names:
            if name not in vref:
                vref[name] = {}
            vref = vref[name]

    return res


def get_model_reserved_props(action: Action) -> List[str]:
    if action in (Action.GETALL, Action.SEARCH):
        return ['_type', '_id', '_revision']
    elif action == Action.CHANGES:
        return ['_cid', '_created', '_op', '_id', '_txn', '_revision']
    else:
        return ['_type', '_id', '_revision']


def get_ns_reserved_props(action: Action) -> List[str]:
    return []


def get_table_name(
    node: Union[Model, Property],
    ttype: TableType = TableType.MAIN,
) -> str:
    if isinstance(node, Model):
        model = node
    else:
        model = node.model
    if ttype in (TableType.LIST, TableType.FILE):
        name = model.model_type() + ttype.value + '/' + node.place
    else:
        name = model.model_type() + ttype.value
    return name
