from typing import cast

from spinta import commands
from spinta.components import Context, Model, UrlParams
from spinta.core.access import load_access_param
from spinta.core.enums import load_level, load_status, load_visibility
from spinta.core.ufuncs import Expr
from spinta.dimensions.scope.components import Scope, ScopeLoader
from spinta.manifests.components import Manifest
from spinta.nodes import load_node


def get_active_custom_scope(model: Model, params: UrlParams) -> Scope | None:
    if not params.custom_scope:
        return None
    return model.scopes.get(params.custom_scope)


def _load_scope(
    context: Context,
    parents: list[Model],
    name: str,
    data: dict,
) -> Scope:
    model = parents[0]

    scope = Scope()
    scope.name = name
    scope.model = model
    scope = load_node(context, scope, data, parent=model)
    scope = cast(Scope, scope)

    load_level(context, scope, data.get("level"))
    load_status(scope, data.get("status"))
    load_visibility(scope, data.get("visibility"))
    load_access_param(scope, data.get("access"), parents)

    return scope


def load_scopes(
    context: Context,
    parents: list[Model],
    scopes: dict[str, dict] | None,
) -> dict[str, Scope]:
    if not scopes:
        return {}
    return {name: _load_scope(context, parents, name, data) for name, data in scopes.items()}


def link_scopes(context: Context, model: Model, scopes: dict[str, Scope]) -> None:
    if not scopes:
        return
    loader = ScopeLoader(context)
    loader.update(model=model)
    loader.resolve(Expr("resolve_scope", list(scopes.values())))


def finalize_scope_link(context: Context, manifest: Manifest) -> None:
    for model in commands.get_models(context, manifest).values():
        if model.scopes:
            link_scopes(context, model, model.scopes)
