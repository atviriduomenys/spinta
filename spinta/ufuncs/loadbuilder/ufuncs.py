from spinta.components import Property, PageInfo
from spinta.core.ufuncs import ufunc, Expr, Pair, Bind, Positive, Negative
from spinta.exceptions import InvalidArgumentInExpression, FieldNotInResource
from spinta.ufuncs.loadbuilder.components import LoadBuilder


@ufunc.resolver(LoadBuilder, Expr, name='page')
def page_(env, expr):
    args = env.resolve(expr.args)
    page = PageInfo(env.model)
    if len(args) > 0:
        for item in args:
            res = env.resolve(item)
            res = env.call('page_item', res)
            if isinstance(res[1], Property):
                page.keys[res[0]] = res[1]
            else:
                if res[0] == 'size' and isinstance(res[1], int):
                    page.size = res[1]
                else:
                    raise InvalidArgumentInExpression(arguments=res[0], expr='page')
    else:
        page.enabled = False
    return page


@ufunc.resolver(LoadBuilder, Pair, name='page_item')
def page_item(env, field):
    return field.name, field.value


@ufunc.resolver(LoadBuilder, Bind, name='page_item')
def page_item(env, field):
    if field.name in env.model.properties:
        prop = env.model.properties[field.name]
    else:
        raise FieldNotInResource(env.model, property=field.name)
    return prop.name, prop


@ufunc.resolver(LoadBuilder, Positive, name='page_item')
def page_item(env, field):
    return env.call('page_item', Bind(field.name))


@ufunc.resolver(LoadBuilder, Negative, name='page_item')
def page_item(env, field):
    resolved = env.call('page_item', Bind(field.name))
    return f'-{resolved[0]}', resolved[1]


@ufunc.resolver(LoadBuilder, Expr, name='and')
def and_(env, expr):
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    return args


@ufunc.resolver(LoadBuilder, Expr, name='or')
def or_(env, expr):
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    return args
