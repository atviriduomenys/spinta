from spinta.core.ufuncs import Bind, Expr, GetAttr, asttoexpr, ufunc
from spinta.dimensions.scope.components import Scope, ScopeLoader


@ufunc.resolver(ScopeLoader, Bind, Bind, name="getattr")
def getattr_(env: ScopeLoader, field: Bind, attr: Bind) -> GetAttr:
    return GetAttr(field.name, attr)


@ufunc.resolver(ScopeLoader, Bind, GetAttr, name="getattr")
def getattr_(env: ScopeLoader, obj: Bind, attr: GetAttr) -> GetAttr:
    return GetAttr(obj.name, attr)


@ufunc.resolver(ScopeLoader, GetAttr, Bind, name="getattr")
def getattr_(env: ScopeLoader, obj: GetAttr, attr: Bind) -> GetAttr:
    leaf = env.call("getattr", obj.name, attr)
    return GetAttr(obj.obj, leaf)


@ufunc.resolver(ScopeLoader, list)
def resolve_scope(env: ScopeLoader, scopes: list) -> None:
    for scope in scopes:
        env.call("resolve_scope", scope)


@ufunc.resolver(ScopeLoader, Scope)
def resolve_scope(env: ScopeLoader, scope: Scope) -> None:
    prepare = scope.prepare
    if not prepare:
        return
    if isinstance(prepare, dict):
        prepare = asttoexpr(prepare)
    env.call("validate_prepare", prepare)


@ufunc.resolver(ScopeLoader, Expr)
def validate_prepare(env: ScopeLoader, expr: Expr) -> None:
    if expr.name in ("bind", "getattr"):
        result = env.resolve(expr)
        env.resolve_property(result)
        return
    for arg in expr.args:
        env.call("validate_prepare", arg)
    for value in expr.kwargs.values():
        env.call("validate_prepare", value)


@ufunc.resolver(ScopeLoader, object)
def validate_prepare(env: ScopeLoader, value: object) -> None:
    return
