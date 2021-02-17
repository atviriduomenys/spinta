from typing import Dict
from typing import List
from typing import Optional
from typing import Union
from typing import cast

from spinta import spyna
from spinta.components import Context
from spinta.dimensions.enum.components import EnumItem
from spinta.components import Model
from spinta.components import Namespace
from spinta.components import Property
from spinta.core.access import load_access_param
from spinta.core.ufuncs import Expr
from spinta.core.ufuncs import asttoexpr
from spinta.dimensions.enum.components import EnumFormula
from spinta.exceptions import FormulaError
from spinta.manifests.tabular.components import EnumRow
from spinta.nodes import load_node
from spinta.utils.schema import NA


def _load_enum_item(
    context: Context,
    parents: List[Union[Property, Model, Namespace]],
    data: EnumRow,
) -> EnumItem:
    item = EnumItem()
    item = load_node(context, item, data, parent=parents[0])
    item = cast(EnumItem, item)
    if item.prepare is not NA:
        ast = item.prepare
        expr = asttoexpr(ast)
        env = EnumFormula(context, scope={
            'this': item.source,
            'node': parents[0],
        })
        item.prepare = env.resolve(expr)
        if isinstance(item.prepare, Expr):
            raise FormulaError(item, formula=spyna.unparse(ast), error=(
                "Formula must resolve to a literal value."
            ))

    load_access_param(item, data.get('access'), parents)
    return item


def load_enums(
    context: Context,
    parents: List[Union[Property, Model, Namespace]],
    enums: Optional[Dict[str, Dict[str, EnumRow]]],
) -> Optional[Dict[str, Dict[str, EnumItem]]]:
    if enums is None:
        return None
    return {
        name: {
            source: _load_enum_item(context, parents, item)
            for source, item in enum.items()
        }
        for name, enum in enums.items()
    }
