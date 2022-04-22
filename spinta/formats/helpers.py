from typing import Iterator
from typing import List
from typing import Optional

from spinta.backends import SelectTree
from spinta.backends import get_model_reserved_props
from spinta.backends.helpers import get_ns_reserved_props
from spinta.backends.helpers import get_select_prop_names
from spinta.backends.helpers import get_select_tree
from spinta.backends.helpers import select_only_props
from spinta.components import Action
from spinta.components import Context
from spinta.components import Model
from spinta.components import UrlParams
from spinta.types.datatype import Array
from spinta.types.datatype import DataType
from spinta.types.datatype import Object
from spinta.types.datatype import File
from spinta.types.datatype import Ref
from spinta.utils.data import take


def _get_dtype_header(
    dtype: DataType,
    select: SelectTree,
    name: str,
) -> Iterator[str]:
    if isinstance(dtype, Object):
        for prop, sel in select_only_props(
            dtype.prop,
            take(dtype.properties).keys(),
            dtype.properties,
            select,
        ):
            name_ = name + '.' + prop.name
            yield from _get_dtype_header(prop.dtype, sel, name_)

    elif isinstance(dtype, Array):
        name_ = name + '[]'
        yield from _get_dtype_header(dtype.items.dtype, select, name_)

    elif isinstance(dtype, File):
        yield name + '._id'
        yield name + '._content_type'

    elif isinstance(dtype, Ref):
        if select == {'*': {}}:
            yield name + '._id'
        else:
            for prop, sel in select_only_props(
                dtype.prop,
                dtype.model.properties.keys(),
                dtype.model.properties,
                select,
            ):
                name_ = name + '.' + prop.name
                yield from _get_dtype_header(prop.dtype, sel, name_)

    else:
        yield name


def _get_model_header(
    model: Model,
    names: List[str],
    select: SelectTree,
    reserved: List[str],
) -> List[str]:
    if select is None or select == {'*': {}}:
        names_ = reserved + names
    else:
        names_ = names
    props = model.properties
    props_ = select_only_props(model, names_, props, select, reserved=True)
    for prop, sel in props_:
        yield from _get_dtype_header(prop.dtype, sel, prop.name)


def get_model_tabular_header(
    context: Context,
    model: Model,
    action: Action,
    params: UrlParams,
    *,
    reserved: Optional[List[str]] = None,
) -> List[str]:
    if params.count:
        header = ['count()']
    else:
        if reserved is None:
            if model.name == '_ns':
                reserved = get_ns_reserved_props(action)
            else:
                reserved = get_model_reserved_props(action)
        select = get_select_tree(context, action, params.select)
        if model.name == '_ns':
            names = get_select_prop_names(
                context,
                model,
                model.properties,
                action,
                select,
                auth=False,
            )
        else:
            names = get_select_prop_names(
                context,
                model,
                model.properties,
                action,
                select,
                reserved=reserved,
            )
        header = list(_get_model_header(model, names, select, reserved))
    return header
