from spinta.components import PageInfo, Property
from spinta.core.ufuncs import Bind, Expr, Negative, Pair, Positive, ufunc
from spinta.exceptions import FieldNotInResource, InvalidArgumentInExpression
from spinta.ufuncs.loadbuilder.components import LoadBuilder
from spinta.utils.schema import NA


@ufunc.resolver(LoadBuilder, Expr, name="page")
def page_(env, expr):
    args = env.resolve(expr.args)
    page = PageInfo(env.model)
    if len(args) > 0:
        for item in args:
            res = env.resolve(item)
            res = env.call("page_item", res)
            if isinstance(res[1], Property):
                page.keys[res[0]] = res[1]
            else:
                if res[0] == "size" and isinstance(res[1], int):
                    page.size = res[1]
                else:
                    raise InvalidArgumentInExpression(arguments=res[0], expr="page")
    else:
        page.enabled = False
    return page


@ufunc.resolver(LoadBuilder, Pair, name="page_item")
def page_item(env, field):
    return field.name, field.value


@ufunc.resolver(LoadBuilder, Bind, name="page_item")
def page_item(env, field):
    if field.name in env.model.properties:
        prop = env.model.properties[field.name]
    else:
        raise FieldNotInResource(env.model, property=field.name)
    return prop.name, prop


@ufunc.resolver(LoadBuilder, Positive, name="page_item")
def page_item(env, field):
    return env.call("page_item", Bind(field.name))


@ufunc.resolver(LoadBuilder, Negative, name="page_item")
def page_item(env, field):
    resolved = env.call("page_item", Bind(field.name))
    return f"-{resolved[0]}", resolved[1]


@ufunc.resolver(LoadBuilder, Expr, name="and")
def and_(env, expr):
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    return args


@ufunc.resolver(LoadBuilder, Expr, name="or")
def or_(env, expr):
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    return args


@ufunc.resolver(LoadBuilder, Expr)
def cdata(env: LoadBuilder, expr: Expr) -> None:
    expr.resolve(env)
    env.param.soap_body_value_type = "cdata"


@ufunc.resolver(LoadBuilder, Expr, name="input")
def input_(env: LoadBuilder, expr: Expr) -> None:
    args, kwargs = expr.resolve(env)
    prep_value = args[0] if args else NA

    env.param.soap_body = {env.this: prep_value}


@ufunc.resolver(LoadBuilder, Expr)
def header(env: LoadBuilder, expr: Expr) -> None:
    args, kwargs = expr.resolve(env)
    if len(args) != 1:
        raise InvalidArgumentInExpression(arguments=args, expr="header")

    return env.call("header", env.this, args[0])


@ufunc.resolver(LoadBuilder, str, str)
def header(env: LoadBuilder, header_name: str, header_value: str) -> None:
    if env.resource is not None:
        env.resource.http.headers[header_name] = header_value


@ufunc.resolver(LoadBuilder, str, Expr)
def header(env: LoadBuilder, header_name: str, header_value: Expr) -> None:
    # Value that can't be resolved on load (e.g. `creds()`) is kept as `Expr`, resolved by the request builder.
    if env.resource is not None:
        env.resource.http.headers[header_name] = header_value


@ufunc.resolver(LoadBuilder, object)
def query(env: LoadBuilder, default: object) -> None:
    """`query('default')` - outgoing query parameter (`source`) with a default value.

    Stored on `resource.url.query`. A value given in the request URL for a source-less property linked with
    `param()` overrides the default.
    """
    if env.resource is not None and isinstance(env.this, str):
        env.param.query_name = env.this
        env.resource.url.query[env.this] = default


@ufunc.resolver(LoadBuilder)
def query(env: LoadBuilder) -> None:
    """`query()` - outgoing query parameter (`source`) without a default, value must come from the request URL."""
    if env.resource is not None and isinstance(env.this, str):
        env.param.query_name = env.this
        env.resource.url.query[env.this] = NA
