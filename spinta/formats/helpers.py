import uuid
from typing import Iterator
from typing import List
from typing import Optional

from spinta import commands
from spinta.backends import SelectTree, get_property_base_model
from spinta.backends import get_model_reserved_props
from spinta.backends.helpers import get_ns_reserved_props
from spinta.backends.helpers import get_select_prop_names
from spinta.backends.helpers import get_select_tree
from spinta.backends.helpers import select_only_props
from spinta.components import Action, pagination_enabled, Node
from spinta.components import Context
from spinta.components import Model
from spinta.components import UrlParams
from spinta.formats.components import Format
from spinta.types.datatype import Array, ArrayBackRef, BackRef
from spinta.types.datatype import DataType
from spinta.types.datatype import ExternalRef
from spinta.types.datatype import File
from spinta.types.datatype import Inherit
from spinta.types.datatype import Object
from spinta.types.datatype import Ref
from spinta.types.text.components import Text
from spinta.ufuncs.basequerybuilder.ufuncs import Star
from spinta.utils.data import take
from spinta.utils.nestedstruct import get_separated_name


def _get_dtype_header(
    dtype: DataType,
    select: SelectTree,
    name: str,
    langs: List = None
) -> Iterator[str]:
    if isinstance(dtype, Object):
        for prop, sel in select_only_props(
            dtype.prop,
            take(dtype.properties).keys(),
            dtype.properties,
            select,
        ):
            name_ = get_separated_name(dtype, name, prop.name)
            yield from _get_dtype_header(prop.dtype, sel, name_, langs)

    elif isinstance(dtype, Array):
        name_ = name + '[]'
        yield from _get_dtype_header(dtype.items.dtype, select, name_, langs)

    elif isinstance(dtype, ArrayBackRef):
        name_ = name + '[]'
        yield from _get_dtype_header(dtype.refprop.dtype, select, name_, langs)

    elif isinstance(dtype, BackRef):
        yield from _get_dtype_header(dtype.refprop.dtype, select, name, langs)

    elif isinstance(dtype, File):
        yield get_separated_name(dtype, name, '_id')
        yield get_separated_name(dtype, name, '_content_type')

    elif isinstance(dtype, ExternalRef):
        if select is None or select == {'*': {}}:
            if dtype.model.given.pkeys or dtype.explicit:
                props = dtype.refprops
            else:
                props = [dtype.model.properties['_id']]
            processed_props = []
            for prop in props:
                processed_props.append(get_separated_name(dtype, name, prop.place))

            for key, prop in dtype.properties.items():
                for processed_name in _get_dtype_header(prop.dtype, select, get_separated_name(dtype, name, key), langs):
                    if processed_name not in processed_props:
                        processed_props.append(processed_name)
            yield from processed_props
        else:
            for prop, sel in select_only_props(
                dtype.prop,
                dtype.model.properties.keys(),
                dtype.model.properties,
                select,
            ):
                name_ = get_separated_name(dtype, name, prop.name)
                yield from _get_dtype_header(prop.dtype, sel, name_, langs)

    elif isinstance(dtype, Ref):
        if select is None or select == {'*': {}}:
            if not dtype.inherited:
                yield get_separated_name(dtype, name, '_id')
            for key, prop in dtype.properties.items():
                yield from _get_dtype_header(prop.dtype, select, get_separated_name(dtype, name, key), langs)
        else:
            for prop, sel in select_only_props(
                dtype.prop,
                dtype.model.properties.keys(),
                dtype.model.properties,
                select,
            ):
                name_ = get_separated_name(dtype, name, prop.name)
                yield from _get_dtype_header(prop.dtype, sel, name_, langs)
    elif isinstance(dtype, Text):
        if select is None or select == {'*': {}}:
            yield_text_count = 0
            if langs:
                if len(langs) == 1 and isinstance(langs[0], Star):
                    for lang in dtype.langs.keys():
                        yield_text_count += 1
                        yield get_separated_name(dtype, name, lang)
                elif len(langs) == 1:
                    yield_text_count += 1
                    yield name
                else:
                    for lang in langs:
                        if lang in dtype.langs:
                            yield_text_count += 1
                            yield get_separated_name(dtype, name, lang)
            else:
                yield_text_count += 1
                yield name

            if yield_text_count == 0:
                yield name
        else:
            for prop, sel in select_only_props(
                dtype.prop,
                dtype.langs.keys(),
                dtype.langs,
                select,
            ):
                name_ = get_separated_name(dtype, name, prop.name)
                yield from _get_dtype_header(prop.dtype, sel, name_, langs)
    elif isinstance(dtype, Inherit):
        if select and select != {'*': {}}:
            properties = {}
            for sel in select.keys():
                base_model = get_property_base_model(dtype.prop.model, sel)
                properties.update(base_model.properties)
            for prop, sel in select_only_props(
                dtype.prop,
                properties.keys(),
                properties,
                select,
            ):
                name_ = get_separated_name(dtype, name, prop.name)
                yield from _get_dtype_header(prop.dtype, sel, name_, langs)
        else:
            yield name
    else:
        yield name


def _get_model_header(
    model: Model,
    names: List[str],
    select: SelectTree,
    reserved: List[str],
    langs: List
) -> List[str]:
    if select is None or select == {'*': {}}:
        keys = reserved + names
    else:
        keys = names
    props = model.properties
    props_ = select_only_props(
        model,
        keys,
        props,
        select,
        reserved=True,
    )
    for prop, sel in props_:
        yield from _get_dtype_header(prop.dtype, sel, prop.name, langs)


def get_model_tabular_header(
    context: Context,
    model: Model,
    action: Action,
    params: UrlParams,
    *,
    reserved: Optional[List[str]] = None,
) -> List[str]:
    if reserved is None:
        if model.name == '_ns':
            reserved = get_ns_reserved_props(action)
        else:
            reserved = get_model_reserved_props(action, pagination_enabled(model, params))

    prop_select = params.select_props
    func_select = params.select_funcs
    select = get_select_tree(context, action, prop_select)

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

    langs = params.lang
    if params.changes:
        langs = [Star()]

    header = list(_get_model_header(model, names, select, reserved, langs))

    if func_select is not None:
        for key in func_select.keys():
            header.append(key)

    return header


def rename_page_col(rows):
    for row in rows:
        yield {'_page.next' if k == '_page' else k: v for k, v in row.items()}


@commands.gen_object_id.register(Context, Format, Node)
def gen_object_id(context: Context, fmt: Format, node: Node):
    return str(uuid.uuid4())


@commands.gen_object_id.register(Context, Format)
def gen_object_id(context: Context, fmt: Format):
    return str(uuid.uuid4())
