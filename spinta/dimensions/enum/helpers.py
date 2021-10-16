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
from spinta.core.ufuncs import asttoexpr
from spinta.dimensions.enum.components import EnumFormula
from spinta.dimensions.enum.components import EnumItem
from spinta.dimensions.enum.components import EnumValue
from spinta.dimensions.enum.components import Enums
from spinta.exceptions import ValueNotInEnum
from spinta.manifests.components import Manifest
from spinta.manifests.tabular.components import EnumRow
from spinta.nodes import load_node
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
    if item.prepare is not NA:
        ast = item.prepare
        expr = asttoexpr(ast)
        env = EnumFormula(context, scope={
            'this': item.source,
            'node': parent,
        })
        item.prepare = env.resolve(expr)

    load_access_param(item, data.get('access'), parents)
    return item


def load_enums(
    context: Context,
    parents: List[Union[Property, Model, Namespace, Manifest]],
    enums: Optional[Dict[str, Dict[str, EnumRow]]],
) -> Optional[Enums]:
    if enums is None:
        return
    return {
        name: {
            source: _load_enum_item(context, parents, item)
            for source, item in enum.items()
        }
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


def get_prop_enum(prop: Optional[Property]) -> EnumValue:
    if prop and prop.enum:
        return prop.enum


T = TypeVar('T')


def prepare_enum_value(prop: Property, value: T) -> Union[T, List[T]]:
    if enum := get_prop_enum(prop):
        source = [
            item.source
            for item in enum.values()
            if (
                (item.prepare is None and value is None) or
                item.prepare == value
            )
        ]
        if len(source) == 0:
            raise ValueNotInEnum(prop, value=value)
        elif len(source) == 1:
            return source[0]
        else:
            return source
    else:
        return value
