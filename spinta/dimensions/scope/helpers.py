from typing import Dict, List, Optional, cast

from spinta.components import Context, Model
from spinta.core.access import load_access_param
from spinta.dimensions.scope.components import Scope
from spinta.nodes import load_node


def _load_scope(
    context: Context,
    parents: List[Model],
    name: str,
    data: dict,
) -> Scope:
    scope = Scope()
    scope.name = name
    model = parents[0]
    scope.model = model
    scope = load_node(context, scope, data, parent=model)
    scope = cast(Scope, scope)
    load_access_param(scope, data.get("access"), parents)
    return scope


def load_scopes(
    context: Context,
    parents: List[Model],
    scopes: Optional[Dict[str, dict]],
) -> Dict[str, Scope]:
    if not scopes:
        return {}
    return {name: _load_scope(context, parents, name, data) for name, data in scopes.items()}
