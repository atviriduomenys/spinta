from __future__ import annotations

from typing import Dict
from typing import List
from typing import Optional
from typing import TypeVar
from typing import Union
from typing import cast

from spinta.components import Context
from spinta.components import Model
from spinta.components import Namespace
from spinta.components import Property
from spinta.core.access import link_access_param
from spinta.core.access import load_access_param
from spinta.core.enums import load_level, load_status, load_visibility
from spinta.core.ufuncs import asttoexpr, Expr
from spinta.dimensions.enum.components import EnumFormula
from spinta.dimensions.enum.components import EnumItem
from spinta.dimensions.enum.components import EnumValue
from spinta.dimensions.enum.components import Enums
from spinta.exceptions import ValueNotInEnum, EnumPrepareMissing
from spinta.manifests.components import Manifest
from spinta.manifests.tabular.components import EnumRow
from spinta.nodes import load_node
from spinta.types.datatype import String
from spinta.ufuncs.resultbuilder.components import EnumResultBuilder
from spinta.utils.schema import NA


def _load_enum_item(
    context: Context,
    parents: List[Union[Property, Model, Namespace, Manifest]],
    data: EnumRow,
) -> EnumItem:
    item = EnumItem()
    parent = parents[0]
    item = load_node(context, item, data, parent=parent)
    item = cast(EnumItem, item)

    if item.prepare is NA:
        if hasattr(parent, "dtype") and not isinstance(parent.dtype, String):
            raise EnumPrepareMissing(enum=item.source)
    else:
        ast = item.prepare
        expr = asttoexpr(ast)
        env = EnumFormula(
            context,
            scope={
                "this": item.source,
                "node": parent,
            },
        )
        item.prepare = env.resolve(expr)

    load_access_param(item, data.get("access"), parents)
    load_level(context, item, data.get("level"))
    load_status(item, data.get("status"))
    load_visibility(item, data.get("visibility"))
    return item


def load_enums(
    context: Context,
    parents: List[Union[Property, Model, Namespace, Manifest]],
    enums: Optional[Dict[str, Dict[str, EnumRow]]],
) -> Optional[Enums]:
    if enums is None:
        return
    return {
        name: {source: _load_enum_item(context, parents, item) for source, item in enum.items()}
        for name, enum in enums.items()
    }


def link_enums(
    parents: List[Union[Property, Model, Namespace]],
    enums: Optional[Enums],
) -> None:
    if enums is None:
        return
    for enum in enums.values():
        for item in enum.values():
            link_access_param(item, parents)


def get_prop_enum(prop: Optional[Property]) -> EnumValue | None:
    if prop and prop.enum:
        return prop.enum
    return None


T = TypeVar("T")


def prepare_enum_value(prop: Property, value: T) -> Union[T, List[T]]:
    if enum := get_prop_enum(prop):
        source = [
            item.source for item in enum.values() if ((item.prepare is None and value is None) or item.prepare == value)
        ]
        if len(source) == 0:
            raise ValueNotInEnum(prop, value=value)
        elif len(source) == 1:
            return source[0]
        else:
            return source
    else:
        return value


def get_enum_value(
    context: Context,
    prop: Property,
    value: object,
    *,
    validate: bool = True,
) -> object:
    """
    Returns the expected enum value for a given source value.
    `validate` flag is used to raise an exception if the value is not in enum.
    """
    if (enum_options := get_prop_enum(prop)) is None:
        return value

    value_not_given = value is None or value is NA
    if not value_not_given and str(value) in enum_options:
        prepare_value = enum_options[str(value)].prepare
        if prepare_value is NA:
            return value
        return prepare_value

    check_value = value
    enum_contains_expr = any(isinstance(enum.prepare, Expr) for enum in enum_options.values())
    if enum_contains_expr:
        for enum in enum_options.values():
            if not isinstance(enum.prepare, Expr):
                continue

            env = EnumResultBuilder(context).init(value)
            val = env.resolve(enum.prepare)
            if env.has_value_changed:
                check_value = val
                break

    if str(check_value) in enum_options:
        prepare_value = enum_options[str(check_value)].prepare
        if prepare_value is NA:
            return check_value
        return prepare_value

    if validate:
        if value_not_given and not prop.dtype.required:
            return check_value
        raise ValueNotInEnum(prop, value=value)
    return check_value
