from spinta.components import Model
from spinta.core.ufuncs import ufunc, Expr, Bind, asttoexpr
from spinta.dimensions.scope.components import ScopeLoader, Scope
from spinta.exceptions import PropertyNotFound


@ufunc.resolver(ScopeLoader, list)
def resolve_scope(env: ScopeLoader, scopes: list):
    for scope in scopes:
        env.call("resolve_scope", scope)


@ufunc.resolver(ScopeLoader, Scope)
def resolve_scope(env: ScopeLoader, scope: Scope):
    prepare = scope.prepare
    if not prepare:
        return
    if isinstance(prepare, dict):
        prepare = asttoexpr(prepare)
    env.call("validate_prepare", scope.model, prepare)


@ufunc.resolver(ScopeLoader, Model, Expr)
def validate_prepare(env: ScopeLoader, model: Model, expr: Expr):
    if expr.name == "getattr":
        if expr.args:
            env.call("validate_prepare", model, expr.args[0])
        return
    if expr.name == "bind":
        if expr.args and isinstance(expr.args[0], str):
            env.call("validate_prepare", model, Bind(expr.args[0]))
        return
    for arg in expr.args:
        env.call("validate_prepare", model, arg)
    for value in expr.kwargs.values():
        env.call("validate_prepare", model, value)


@ufunc.resolver(ScopeLoader, Model, Bind)
def validate_prepare(env: ScopeLoader, model: Model, bind: Bind):
    if bind.name not in model.properties:
        raise PropertyNotFound(model, property=bind.name)


@ufunc.resolver(ScopeLoader, Model, object)
def validate_prepare(env: ScopeLoader, model: Model, value: object):
    return
